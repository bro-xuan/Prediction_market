import os, time, json, datetime as dt
import signal
import random
from collections import defaultdict, deque
from decimal import Decimal
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

# --- Config ---
DATA_TRADES = "https://data-api.polymarket.com/trades"          # public Data-API (no auth)  # docs: https://docs.polymarket.com/developers/CLOB/trades/trades-data-api
MIDPOINT_URL = "https://clob.polymarket.com/midpoint"           # optional midpoint            # docs: https://docs.polymarket.com/developers/CLOB/prices-books/get-midpoint
TG_API = "https://api.telegram.org/bot{token}/sendMessage"      # Telegram Bot API             # docs: https://core.telegram.org/bots/api

EVENT_COOLDOWN_SEC = int(os.getenv("EVENT_COOLDOWN_SEC", "30")) 
_last_sent_by_event = {}  # cooldown key -> last_sent_epoch  (now per-asset or (event|outcome))
LAST_NEWEST_TS = 0
CLEANUP_COUNTER = 0  # Global counter for memory cleanup
_midpoint_cache = {}  # {asset_id: (value_or_None, expiry_epoch)}
MIDPOINT_TTL_SEC = int(os.getenv("MIDPOINT_TTL_SEC", "60"))
MIDPOINT_NEG_TTL_SEC = int(os.getenv("MIDPOINT_NEG_TTL_SEC", "300"))

def must_get(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

BOT_TOKEN = must_get("TELEGRAM_BOT_TOKEN")
CHAT_ID   = must_get("TELEGRAM_CHAT_ID")

MIN_CASH = Decimal(os.getenv("MIN_CASH_USD", "50000"))  # Updated default to match intended whale trades
SLEEP = float(os.getenv("POLL_INTERVAL_SEC", "3"))
TAIL_THRESHOLD_PCT = float(os.getenv("TAIL_THRESHOLD_PCT", "10"))  # Updated default to 10%

# New: data API knobs
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "300"))
DATA_API_MIN_FILL_USD = (os.getenv("DATA_API_MIN_FILL_USD", "") or "").strip()  # blank/0 => no server filter
TAKER_ONLY = os.getenv("TAKER_ONLY", "true").lower() == "true"

def validate_config():
    """Validate configuration values and provide helpful error messages."""
    errors = []
    
    # Validate MIN_CASH
    if MIN_CASH <= 0:
        errors.append(f"MIN_CASH_USD must be positive, got: {MIN_CASH}")
    elif MIN_CASH < 1000:
        errors.append(f"MIN_CASH_USD seems low ({MIN_CASH}), consider using at least $1000")
    
    # Validate SLEEP interval
    if SLEEP <= 0:
        errors.append(f"POLL_INTERVAL_SEC must be positive, got: {SLEEP}")
    elif SLEEP < 1:
        errors.append(f"POLL_INTERVAL_SEC is very low ({SLEEP}s), this may cause rate limiting")
    elif SLEEP > 60:
        errors.append(f"POLL_INTERVAL_SEC is high ({SLEEP}s), you might miss trades")
    
    # Validate TAIL_THRESHOLD_PCT
    if TAIL_THRESHOLD_PCT < 0:
        errors.append(f"TAIL_THRESHOLD_PCT must be non-negative, got: {TAIL_THRESHOLD_PCT}")
    elif TAIL_THRESHOLD_PCT > 50:
        errors.append(f"TAIL_THRESHOLD_PCT is very high ({TAIL_THRESHOLD_PCT}%), this may filter out most trades")
    
    # Validate Telegram credentials format
    if not BOT_TOKEN or len(BOT_TOKEN) < 20:
        errors.append("TELEGRAM_BOT_TOKEN appears invalid (too short)")
    
    if not CHAT_ID or not CHAT_ID.lstrip('-').isdigit():
        errors.append("TELEGRAM_CHAT_ID must be a numeric chat ID")
    
    if errors:
        print("Configuration validation errors:")
        for error in errors:
            print(f"  ❌ {error}")
        print("\nPlease fix these issues before running the bot.")
        return False
    
    print("✅ Configuration validation passed")
    print(f"   MIN_CASH_USD: ${MIN_CASH}")
    print(f"   POLL_INTERVAL_SEC: {SLEEP}s")
    print(f"   EVENT_COOLDOWN_SEC: {EVENT_COOLDOWN_SEC}s")
    print(f"   TAIL_THRESHOLD_PCT: {TAIL_THRESHOLD_PCT}%")
    print(f"   FETCH_LIMIT: {FETCH_LIMIT}")
    print(f"   DATA_API_MIN_FILL_USD: {DATA_API_MIN_FILL_USD or 'none'}")
    print(f"   TAKER_ONLY: {TAKER_ONLY}")
    return True

STOP = False

SEEN_FILE = "seen_tx.json"
SEEN_MAX = int(os.getenv("SEEN_MAX", "10000"))
SEEN_SET = set()       # composite keys: "tx|asset"
SEEN_ORDER = deque(maxlen=SEEN_MAX)
LEGACY_SEEN_TX = set() # tx-only keys loaded from old files

def make_seen_key(tx: str, asset: str) -> str:
    tx = tx or ""
    asset = asset or ""
    return f"{tx}|{asset}"

def cooldown_key_from_agg(agg: dict) -> Optional[str]:
    """
    Prefer per-asset cooldown. If no asset is present, fall back to (eventSlug|outcome),
    else to eventSlug alone. Returns None if no reasonable key exists.
    """
    asset = agg.get("asset")
    if asset:
        return str(asset)
    event = agg.get("eventSlug") or ""
    outcome = agg.get("outcome") or ""
    key = f"{event}|{outcome}".strip("|")
    return key or None

def add_seen(sk: str):
    """Add a composite seen key to deque+set, evicting the oldest if needed."""
    if not sk or sk in SEEN_SET:
        return
    # If deque is full, remember the oldest before appending
    evicted = SEEN_ORDER[0] if (SEEN_ORDER and len(SEEN_ORDER) == SEEN_ORDER.maxlen) else None
    SEEN_ORDER.append(sk)
    SEEN_SET.add(sk)
    if evicted is not None and evicted in SEEN_SET:
        SEEN_SET.remove(evicted)

def load_seen():
    try:
        with open(SEEN_FILE, "r") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return
        for entry in data:
            if isinstance(entry, str) and "|" in entry:
                if entry not in SEEN_SET:
                    SEEN_SET.add(entry)
                    SEEN_ORDER.append(entry)
            elif isinstance(entry, str):
                # legacy tx-only key
                LEGACY_SEEN_TX.add(entry)
    except Exception:
        pass

load_seen()

def save_seen():
    try:
        # Persist only composite keys in insertion order
        payload = list(SEEN_ORDER)
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, SEEN_FILE)  # atomic on POSIX
    except Exception as e:
        print("save_seen failed:", e)

def cleanup_memory():
    """Clean up growing data structures to prevent memory leaks."""
    # Clean up old cooldown entries (older than 1 hour)
    current_time = time.time()
    cutoff_time = current_time - 3600  # 1 hour ago
    
    # Remove old cooldown entries
    old_events = [event for event, timestamp in _last_sent_by_event.items() if timestamp < cutoff_time]
    for event in old_events:
        del _last_sent_by_event[event]
    
    if old_events:
        print(f"Cleaned up {len(old_events)} old cooldown entries")
    
    # SEEN_ORDER is automatically bounded by maxlen; no manual trimming required
    
    # Clean up midpoint cache (keep it reasonable size)
    MAX_CACHE_SIZE = 1000
    if len(_midpoint_cache) > MAX_CACHE_SIZE:
        # Keep only the most recent entries (simple FIFO)
        cache_items = list(_midpoint_cache.items())
        _midpoint_cache.clear()
        _midpoint_cache.update(cache_items[-MAX_CACHE_SIZE:])
        print(f"Trimmed midpoint cache from {len(cache_items)} to {len(_midpoint_cache)} entries")

def warm_start_if_needed():
    if os.path.exists(SEEN_FILE):
        return
    try:
        trades = fetch_whales(limit=100)
        for key, _ in aggregate_by_tx(trades):
            # key is (tx, asset)
            tx, asset = key
            sk = make_seen_key(tx, asset)
            add_seen(sk)
        save_seen()
        print(f"Warmed with {len(SEEN_ORDER)} existing tx-asset keys — waiting for new trades…")
    except Exception as e:
        print("Warm-start failed:", e)

def notify_started():
    try:
        ok = post_telegram(f"✅ Whale bot started (MIN_CASH_USD={int(MIN_CASH)}, poll={SLEEP}s)")
        if ok:
            print("Startup notify sent to Telegram", flush=True)
        else:
            print("Startup notify failed to send", flush=True)
    except Exception as e:
        print("Startup notify failed:", e)
    # Outcome already logged above.


def should_alert(agg):
    """Return (True, '') if alert should fire; otherwise (False, reason)."""
    # cooldown (per-asset, or fallback to event|outcome)
    key = cooldown_key_from_agg(agg)
    now_ts = time.time()
    last = _last_sent_by_event.get(key, 0)
    if key and now_ts - last < EVENT_COOLDOWN_SEC:
        return False, "cooldown"


    # require valid size to compute VWAP
    size = agg.get("size", 0)
    
    # Convert size to Decimal if it's a string, handle invalid types
    try:
        if isinstance(size, str):
            size = Decimal(size)
        elif not isinstance(size, (int, float, Decimal)):
            return False, "invalid_size_type"
        else:
            size = Decimal(str(size))
    except (ValueError, TypeError, Exception):
        return False, "invalid_size_format"
    
    if size <= 0:
        return False, "no_size"

    # Validate and convert weighted_price_num
    weighted_price_num = agg.get("weighted_price_num")
    if weighted_price_num is None:
        return False, "no_weighted_price"
    
    try:
        if isinstance(weighted_price_num, str):
            weighted_price_num = Decimal(weighted_price_num)
        elif not isinstance(weighted_price_num, (int, float, Decimal)):
            return False, "invalid_weighted_price_type"
        else:
            weighted_price_num = Decimal(str(weighted_price_num))
    except (ValueError, TypeError, Exception):
        return False, "invalid_weighted_price_format"
    
    if weighted_price_num < 0:
        return False, "negative_weighted_price"

    vwap_pct = float((weighted_price_num / size) * Decimal("100.0"))
    lower, upper = TAIL_THRESHOLD_PCT, 100.0 - TAIL_THRESHOLD_PCT
    # DROP tails: <=10% or >=90% by default
    if vwap_pct <= lower or vwap_pct >= upper:
        return False, f"tail({vwap_pct:.1f}%)"

    return True, ""

def mark_alert_sent(agg):
    key = cooldown_key_from_agg(agg)
    if key:
        _last_sent_by_event[key] = time.time()


def fmt_usd(x: Decimal) -> str:
    if x >= 1000:
        return f"${(x/Decimal(1000)):.1f}k"
    return f"${x:.0f}"

def epoch_to_utc(ts) -> str:
    try:
        if ts is None:
            return "Invalid timestamp"
        if isinstance(ts, str):
            ts = int(float(ts))
        else:
            ts = int(ts)
        # Convert ms -> s if it looks like milliseconds
        if ts > 10**10:
            ts //= 1000
        # Validate seconds range (Unix epoch..Jan 1, 2100)
        if ts <= 0 or ts > 4102444800:
            return "Invalid timestamp"
        return dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, TypeError, OSError, OverflowError):
        return "Invalid timestamp"

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=10))
def fetch_whales(limit: int = FETCH_LIMIT):
    params = {"limit": str(limit)}
    if TAKER_ONLY:
        params["takerOnly"] = "true"
    # Only apply per-fill server filter if explicitly configured
    if DATA_API_MIN_FILL_USD not in ("", "0"):
        params["filterType"] = "CASH"
        params["filterAmount"] = DATA_API_MIN_FILL_USD
    try:
        r = requests.get(DATA_TRADES, params=params, timeout=10)
        r.raise_for_status()
        trades = r.json()
        dbg = f"(min_fill_usd={DATA_API_MIN_FILL_USD or 'none'}, takerOnly={TAKER_ONLY}, limit={limit})"
        print(f"Fetched {len(trades)} trades from API {dbg}")
        return trades
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error fetching trades: {e}")
        raise

def current_mid(asset_id: str) -> Optional[str]:
    if not asset_id:
        return None
    # Check cache first with TTL
    now = time.time()
    cached = _midpoint_cache.get(asset_id)
    if cached is not None:
        value, expiry = cached
        if now < expiry:
            return value
    try:
        r = requests.get(MIDPOINT_URL, params={"token_id": asset_id}, timeout=5)
        if not r.ok:
            if r.status_code == 404:
                _midpoint_cache[asset_id] = (None, now + MIDPOINT_NEG_TTL_SEC)
                return None
            else:
                print(f"Midpoint API failed for asset {asset_id}: HTTP {r.status_code}")
                return None
        mid_raw = r.json().get("mid")
        if mid_raw is None:
            _midpoint_cache[asset_id] = (None, now + MIDPOINT_NEG_TTL_SEC)
            return None
        mid = Decimal(str(mid_raw))
        if mid > 0 and mid <= 1:
            result = f"{(mid*100):.1f}%"
        else:
            result = f"${mid:.3f}"
        _midpoint_cache[asset_id] = (result, now + MIDPOINT_TTL_SEC)
        return result
    except Exception as e:
        _midpoint_cache[asset_id] = (None, now + MIDPOINT_NEG_TTL_SEC)
        return None

def aggregate_by_tx(trades):
    """
    Combine multiple fills into a single alert per (transactionHash, asset).
    Only returns transactions that meet the minimum cash threshold after aggregation.
    De-duplicates fills so each tx-asset appears only once per fetch.
    """
    buckets = defaultdict(lambda: {
        "cash_usd": Decimal(0),
        "size": Decimal(0),
        "weighted_price_num": Decimal(0),
        "side": None,
        "title": None,
        "eventSlug": None,
        "outcome": None,
        "timestamp": None,
        "asset": None,
        "proxyWallet": None
    })
    order = []  # list of (tx, asset) in arrival order
    for tr in trades:
        tx = tr.get("transactionHash")
        asset = tr.get("asset")
        if not tx:
            continue
        key = (tx, asset)

        # add each tx-asset only once per API fetch
        if key not in buckets:
            order.append(key)

        try:
            price = Decimal(str(tr.get("price", "0")))
            size  = Decimal(str(tr.get("size", "0")))  # shares
            
            # Validate reasonable bounds for price and size
            MAX_REASONABLE_PRICE = Decimal("1.0")          # 0..1 for prediction tokens
            MAX_REASONABLE_SIZE  = Decimal("1000000000")   # 1B shares
            
            if price < 0 or price > MAX_REASONABLE_PRICE:
                print(f"Invalid price {price} in trade {tr.get('transactionHash', 'unknown')[:10]}...")
                continue
                
            if size < 0 or size > MAX_REASONABLE_SIZE:
                print(f"Invalid size {size} in trade {tr.get('transactionHash', 'unknown')[:10]}...")
                continue
                
        except (ValueError, TypeError, Exception) as e:
            print(f"Invalid price/size in trade {tr.get('transactionHash', 'unknown')[:10]}...: {e}")
            continue
            
        if size <= 0:
            continue

        b = buckets[key]
        usd_notional             = price * size
        b["cash_usd"]           += usd_notional       # total USD
        b["size"]               += size               # total shares
        b["weighted_price_num"] += price * size       # Σ(price*shares) for VWAP
        b["side"]                = tr.get("side", b["side"])
        b["title"]               = tr.get("title", b["title"])
        b["eventSlug"]           = tr.get("eventSlug", b["eventSlug"])
        b["outcome"]             = tr.get("outcome", b["outcome"])
        b["timestamp"]           = tr.get("timestamp", b["timestamp"])
        b["asset"]               = tr.get("asset", b["asset"])
        b["proxyWallet"]         = tr.get("proxyWallet", b["proxyWallet"])

    filtered_results = []
    for key in order:
        agg = buckets[key]
        if agg["cash_usd"] >= MIN_CASH:
            filtered_results.append((key, agg))
    return filtered_results


def format_address(addr: str) -> str:
    """Return a short masked version of a wallet address."""
    if not addr:
        return ""
    return f"{addr[:6]}...{addr[-4:]}"  # keep first 6 and last 4 characters

def polymarket_explorer_link(addr: str) -> str:
    """Return a clickable Polymarket user page link for the trader address."""
    if not addr:
        return ""
    return f"https://polymarket.com/profile/{addr}"

def build_message(agg):
    # Validate required fields with safe defaults
    cash_usd = agg.get("cash_usd", Decimal("0"))
    size = agg.get("size", Decimal("0"))
    weighted_price_num = agg.get("weighted_price_num", Decimal("0"))
    side = agg.get("side", "UNKNOWN")
    timestamp = agg.get("timestamp", 0)
    
    # Calculate VWAP safely (0..1) then percent
    vwap = None
    if size > 0 and weighted_price_num > 0:
        try:
            vwap = float((weighted_price_num / size) * Decimal("100.0"))
        except (ZeroDivisionError, TypeError, ValueError):
            vwap = None
    
    vwap_str = f" @ {vwap:.1f}%" if vwap is not None else ""
    now = current_mid(agg.get("asset")) if agg.get("asset") else None
    header = f"🐳 {fmt_usd(cash_usd)} {side}{vwap_str} • {agg.get('outcome','')} • {agg.get('title') or 'Untitled market'}"
    line2  = epoch_to_utc(timestamp)
    line3  = (f"Now ~{now} • " if now else "") + f"https://polymarket.com/event/{agg.get('eventSlug','')}"
    
    addr = agg.get("proxyWallet") or ""
    addr_short = format_address(addr)
    addr_link = polymarket_explorer_link(addr)
    line4 = f"Trader: {addr_short} • {addr_link}" if addr_short else ""

    return "\n".join([header, line2, line3, line4]) if line4 else "\n".join([header, line2, line3])


def post_telegram(text: str, max_retries: int = 5) -> bool:
    data = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}
    url = TG_API.format(token=BOT_TOKEN)
    attempts = 0
    
    while attempts <= max_retries:
        try:
            r = requests.post(url, json=data, timeout=10)
            
            # Handle rate limiting (429)
            if r.status_code == 429:
                retry_after = 1
                try:
                    retry_after = int(r.json().get("parameters", {}).get("retry_after", 1))
                except Exception:
                    pass
                print(f"Telegram 429 — sleeping {retry_after}s (attempt {attempts + 1}/{max_retries + 1})")
                time.sleep(retry_after + 1)
                attempts += 1
                continue
            
            # Handle other errors
            if not r.ok:
                if attempts < max_retries:
                    # Exponential backoff for other errors
                    sleep_time = min(2 ** attempts, 30)  # Cap at 30 seconds
                    print(f"Telegram error {r.status_code} — retrying in {sleep_time}s (attempt {attempts + 1}/{max_retries + 1})")
                    time.sleep(sleep_time)
                    attempts += 1
                    continue
                else:
                    print(f"Telegram error {r.status_code} — max retries exceeded: {r.text}")
                    return False
            
            # Success - only sleep if message was sent successfully
            time.sleep(1.2)
            return True
            
        except requests.exceptions.RequestException as e:
            if attempts < max_retries:
                sleep_time = min(2 ** attempts, 30)
                print(f"Telegram network error — retrying in {sleep_time}s (attempt {attempts + 1}/{max_retries + 1}): {e}")
                time.sleep(sleep_time)
                attempts += 1
                continue
            else:
                print(f"Telegram network error — max retries exceeded: {e}")
                return False
    
    return False



def _stop(*_):
    global STOP
    STOP = True

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

def loop():
    print("Whale bot running…")
    while not STOP:
        newest_ts = 0  # Initialize outside try block
        try:
            trades = fetch_whales()
            global LAST_NEWEST_TS
            for tr in trades:
                try:
                    ts = int(tr.get("timestamp", 0))
                    if ts > 10**12:
                        ts //= 1000
                    newest_ts = max(newest_ts, ts)
                except Exception:
                    pass

            # Always process; timestamp plateaus can still include unseen tx
            verbose = newest_ts > LAST_NEWEST_TS
            if verbose:
                print(f"Newest trade time: {epoch_to_utc(newest_ts)}", flush=True)
            else:
                print("No newer trades in API window (still processing for unseen tx)", flush=True)
            pending = []
            # newest-first from API; we'll still post oldest-first for readability
            for (tx, asset), agg in aggregate_by_tx(trades):  # API is newest-first
                sk = make_seen_key(tx, asset)
                if (sk not in SEEN_SET) and (tx not in LEGACY_SEEN_TX):
                    pending.append(((tx, asset), agg))

            eligible = sum(1 for _, a in pending if should_alert(a)[0])
            print(f"Found {len(pending)} new qualifying trades (>=${MIN_CASH}); eligible after filters: {eligible}")

            # cap how many we send in one pass (env overrideable)
            MAX_PER_PASS = int(os.getenv("MAX_PER_PASS", "3"))

            sent_this_pass = 0
            for (tx, asset), agg in reversed(pending):  # post oldest first for readable order
                if sent_this_pass >= MAX_PER_PASS:
                    print(f"Reached MAX_PER_PASS limit ({MAX_PER_PASS}), skipping remaining trades")
                    break
                ok_alert, reason = should_alert(agg)
                if not ok_alert:
                    if verbose:
                        print(f"Skipping tx-asset {tx[:10]}|{(asset or '')[:8]}... reason={reason}")
                    # Mark permanently-skipped reasons as seen so we don't reprocess them every loop
                    if reason.startswith("tail("):
                        sk = make_seen_key(tx, asset)
                        add_seen(sk)
                    # For cooldown, do NOT mark seen so it can post later
                    continue

                msg = build_message(agg)
                print(f"Posting trade (per market): {fmt_usd(agg['cash_usd'])} {agg['side']} - {agg.get('title', 'Unknown')} [{agg.get('outcome','')}]")
                ok = post_telegram(msg)
                if ok:
                    mark_alert_sent(agg)           # update per-event cooldown
                    sk = make_seen_key(tx, asset)
                    add_seen(sk)
                    sent_this_pass += 1
                    print(f"Successfully posted trade notification")
                else:
                    print(f"Failed to post trade notification")

            print(f"Loop summary: sent={sent_this_pass}, seen_total={len(SEEN_SET)} (composite)", flush=True)
            save_seen()
            
            # Periodic memory cleanup (every 100 loops)
            global CLEANUP_COUNTER
            CLEANUP_COUNTER += 1
            
            if CLEANUP_COUNTER % 100 == 0:
                cleanup_memory()
        except Exception as e:
            print(f"Fetch/post error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Always update LAST_NEWEST_TS to prevent race conditions
            LAST_NEWEST_TS = max(LAST_NEWEST_TS, newest_ts)
            time.sleep(SLEEP + random.uniform(0, 0.5))

    save_seen()
    print("Stopped. Seen set saved.")

if __name__ == "__main__":
    if not validate_config():
        exit(1)
    
    warm_start_if_needed()
    notify_started()
    loop()
