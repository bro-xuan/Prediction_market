# bot.py
import asyncio
import html
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv
from telegram import Bot, constants

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

API_BASE = "https://data-api.polymarket.com"
API_LIMIT = int(os.getenv("API_LIMIT", "100"))

# Server-side filtering
API_FILTER_TYPE = os.getenv("API_FILTER_TYPE", "CASH")  # CASH or TOKENS
API_FILTER_AMOUNT = float(os.getenv("API_FILTER_AMOUNT", "50000"))  # your note

# Client-side business rules
MIN_USD_TO_ALERT = float(os.getenv("MIN_USD_TO_ALERT", "10000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.10"))  # inclusive
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.90"))  # inclusive
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "8"))

SEEN_FILE = Path("seen_trades.json")  # persists across restarts

# ---- Helpers ----

def load_seen() -> Set[str]:
    if not SEEN_FILE.exists():
        return set()
    try:
        data = json.loads(SEEN_FILE.read_text())
        return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()

def save_seen(seen: Set[str]) -> None:
    try:
        SEEN_FILE.write_text(json.dumps(sorted(seen)[-5000:]))  # keep last 5k ids
    except Exception:
        pass

def normalize_price(p: float) -> float:
    """
    Polymarket prices are dollars per share in [0.00, 1.00]. If you ever
    see cents as integers (e.g., 55), normalize to 0.55 defensively.
    """
    return p / 100.0 if p > 1.0 else p

def trade_notional_usd(trade: Dict[str, Any]) -> float:
    """
    Approximates cash transacted for the taker: size * price.
    (For YES/NO tokens, price is the per-share dollar price.)
    """
    size = float(trade.get("size", 0))
    price = normalize_price(float(trade.get("price", 0)))
    return size * price

def within_price_band(trade: Dict[str, Any]) -> bool:
    price = normalize_price(float(trade.get("price", 0)))
    return MIN_PRICE <= price <= MAX_PRICE

def trade_url(trade: Dict[str, Any]) -> str:
    # Prefer market slug; fall back to event slug
    slug = trade.get("slug")
    event_slug = trade.get("eventSlug")
    if slug:
        return f"https://polymarket.com/market/{slug}"
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"
    return "https://polymarket.com/"

def format_trade_message(trade: Dict[str, Any]) -> str:
    title = html.escape(trade.get("title") or "Unknown market")
    outcome = html.escape(trade.get("outcome") or "?")
    side = html.escape(trade.get("side") or "?")
    price = normalize_price(float(trade.get("price", 0)))
    size = float(trade.get("size", 0))
    usd = trade_notional_usd(trade)
    ts = int(trade.get("timestamp", 0))
    when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    url = trade_url(trade)
    tx = trade.get("transactionHash", "")

    msg = (
        f"<b>🐋 Large Polymarket trade</b>\n"
        f"<b>Market:</b> {title}\n"
        f"<b>Outcome:</b> {outcome}\n"
        f"<b>Side:</b> {side}\n"
        f"<b>Price:</b> {price:.2%}\n"
        f"<b>Notional:</b> ${usd:,.0f}\n"
        f"<b>Time:</b> {when}\n"
        f"<b>Tx:</b> <code>{tx}</code>\n"
        f"🔗 <a href=\"{html.escape(url)}\">Open on Polymarket</a>"
    )
    return msg

# ---- API client ----

def build_query_params() -> Dict[str, Any]:
    """
    Query newest trades first (default). Keep takerOnly=true to avoid double-counting.
    Apply your requested server-side filters.
    """
    params = {
        "limit": API_LIMIT,
        "takerOnly": "true",
    }
    if API_FILTER_TYPE and API_FILTER_AMOUNT is not None:
        params["filterType"] = API_FILTER_TYPE  # CASH or TOKENS
        params["filterAmount"] = API_FILTER_AMOUNT
    return params

class TemporaryHTTPError(Exception):
    pass

@retry(
    retry=retry_if_exception_type(TemporaryHTTPError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(5),
)
async def fetch_trades(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/trades"
    params = build_query_params()
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        if resp.status >= 500:
            raise TemporaryHTTPError(f"Server error {resp.status}")
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"Fetch failed {resp.status}: {text}")
        return await resp.json()

async def post_message(bot: Bot, text: str) -> None:
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=text,
        parse_mode=constants.ParseMode.HTML,
        disable_web_page_preview=False,
    )

async def poll_and_alert():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    seen = load_seen()

    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                trades = await fetch_trades(session)
                # Trades are newest first. Process in reverse to post oldest-first within the batch.
                for tr in reversed(trades):
                    tx = tr.get("transactionHash")
                    if not tx or tx in seen:
                        continue

                    # Client-side filters
                    if not within_price_band(tr):
                        seen.add(tx); continue

                    usd = trade_notional_usd(tr)
                    if usd < MIN_USD_TO_ALERT:
                        seen.add(tx); continue

                    # Passed filters → post
                    msg = format_trade_message(tr)
                    await post_message(bot, msg)
                    seen.add(tx)

                save_seen(seen)
            except Exception as e:
                # Log and continue loop
                print(f"[WARN] {e}")

            await asyncio.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(poll_and_alert())
