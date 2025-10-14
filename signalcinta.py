# signalcinta.py
"""
Final signal scanner (single-candle EMA20/EMA100)
- TF: 30m, 1h, 2h
- Criteria (non-repainting):
  * Bullish: last CLOSED candle is green, EMA20 & EMA100 both lie inside candle body, and EMA20 < EMA100
  * Bearish: last CLOSED candle is red, EMA20 & EMA100 both lie inside candle body, and EMA20 > EMA100
- Targets: Binance USDⓈ-M (USDT perpetual)
- Persistence: sent_signals.json to avoid duplicate notifications
- Send Telegram via HTTP POST (fill TELEGRAM_TOKEN & TELEGRAM_CHAT_ID below)
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta

import aiohttp
import ccxt
import pandas as pd

# ---------------- CONFIG - isi manual di sini ----------------
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
TELEGRAM_CHAT_ID = "7183177114"

WAIT_AFTER_CLOSE = 120         # seconds buffer after candle close
RATE_DELAY = 0.12              # small pause between symbol processing
LOOP_SLEEP = 6                 # how often main loop wakes (seconds)
SENT_FILE = "sent_signals.json"  # persistence file

# ---------------- logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("signalcinta")

# ---------------- exchange ----------------
exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})

# ---------------- persistence helpers ----------------
def load_sent_signals():
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_sent_signals(data):
    try:
        with open(SENT_FILE, "w", encoding="utf8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed saving sent signals: {e}")

sent_signals = load_sent_signals()  # structure: { "BTC/USDT": {"30m": ts, "1h": ts, ...} }

# ---------------- networking (Telegram) ----------------
async def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token / chat id not set — skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=15) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Telegram returned {resp.status}: {body}")
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

# ---------------- ccxt wrappers (run sync calls in threads) ----------------
def _load_markets_sync():
    return exchange.load_markets()

async def load_markets_async():
    return await asyncio.to_thread(_load_markets_sync)

def _fetch_ohlcv_sync(symbol: str, timeframe: str, limit: int):
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

async def fetch_ohlcv_df(symbol: str, timeframe: str, limit: int = 500):
    """
    Returns DataFrame indexed by tz-aware UTC time, or None on failure.
    """
    try:
        data = await asyncio.to_thread(_fetch_ohlcv_sync, symbol, timeframe, limit)
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df = df.set_index("time")
        df = df.astype(float)
        return df
    except Exception as e:
        logger.debug(f"fetch_ohlcv_df error {symbol} {timeframe}: {e}")
        return None

# ---------------- EMA & detection ----------------
def compute_emas(df: pd.DataFrame):
    d = df.copy()
    d["ema20"] = d["close"].ewm(span=20, adjust=False).mean()
    d["ema100"] = d["close"].ewm(span=100, adjust=False).mean()
    return d

def detect_single_candle_signal(df: pd.DataFrame):
    """
    Evaluate the last CLOSED candle (df.iloc[-2]) and EMA values at that moment.
    Returns (signal_type, open, close, close_time, ema20, ema100) or (None, ...).
    """
    if df is None or len(df) < 120:   # need enough history for EMA100
        return None, None, None, None, None, None

    d = compute_emas(df)
    sig = d.iloc[-2]   # the candle that just closed
    open_p = float(sig["open"])
    close_p = float(sig["close"])
    ema20 = float(sig["ema20"])
    ema100 = float(sig["ema100"])
    close_time = sig.name  # tz-aware

    lo = min(open_p, close_p)
    hi = max(open_p, close_p)

    # both EMAs must be inside the candle body
    if not (lo <= ema20 <= hi and lo <= ema100 <= hi):
        return None, None, None, None, None, None

    # Bullish: green candle and ema20 < ema100
    if close_p > open_p and ema20 < ema100:
        return "bullish", open_p, close_p, close_time, ema20, ema100

    # Bearish: red candle and ema20 > ema100
    if close_p < open_p and ema20 > ema100:
        return "bearish", open_p, close_p, close_time, ema20, ema100

    return None, None, None, None, None, None

# ---------------- daily change ----------------
async def get_daily_change_pct(symbol: str):
    df = await fetch_ohlcv_df(symbol, "1d", limit=10)
    if df is None or len(df) < 2:
        return 0.0
    first_open = float(df.iloc[0]["open"])
    last_close = float(df.iloc[-1]["close"])
    change = (last_close - first_open) / first_open * 100
    return change

# ---------------- symbol filter ----------------
def filter_usdt_perpetual(markets_dict):
    """
    From ccxt exchange.load_markets() dictionary, return list of perpetual USDT futures symbols.
    We check meta info['info']['contractType'] == 'PERPETUAL' and symbol endswith '/USDT'.
    """
    syms = []
    for sym, meta in markets_dict.items():
        try:
            info = meta.get("info", {})
            if info.get("contractType") == "PERPETUAL" and sym.endswith("/USDT"):
                syms.append(sym)
        except Exception:
            continue
    return syms

# ---------------- process per symbol ----------------
async def process_symbol(symbol: str, tf: str):
    try:
        df = await fetch_ohlcv_df(symbol, tf, limit=300)
        await asyncio.sleep(RATE_DELAY)
        if df is None or len(df) < 120:
            return

        sig_type, open_p, close_p, close_time, ema20, ema100 = detect_single_candle_signal(df)
        if sig_type is None:
            return

        # daily filter: bullish skip if 1D drop >=5%; bearish skip if 1D rise >=5%
        dchg = await get_daily_change_pct(symbol)
        if sig_type == "bullish" and dchg < -5:
            return
        if sig_type == "bearish" and dchg > 5:
            return

        close_ts = int(pd.Timestamp(close_time).timestamp())
        rec = sent_signals.get(symbol, {})
        prev_ts = rec.get(tf)
        if prev_ts == close_ts:
            return  # already sent for this candle

        # prepare message
        header = "🟢 VALID BULLISH SIGNAL" if sig_type == "bullish" else "🔴 VALID BEARISH SIGNAL"
        msg = (
            f"{header}\n"
            f"TF detected : {tf}\n"
            f"Pair        : {symbol}\n"
            f"Open        : {open_p:.8f}\n"
            f"Close       : {close_p:.8f}\n"
            f"Candle UTC  : {close_time.isoformat()}\n"
            f"EMA20 ({tf}) = {ema20:.8f}\n"
            f"EMA100({tf}) = {ema100:.8f}\n"
            f"1D change   : {dchg:.2f}%"
        )

        await send_telegram(msg)

        # persist and log
        sent_signals.setdefault(symbol, {})[tf] = close_ts
        save_sent_signals(sent_signals)
        logger.info(f"Sent {sig_type} for {symbol} {tf} at {close_time.isoformat()}")

    except Exception as e:
        logger.debug(f"process_symbol error {symbol} {tf}: {e}")

# ---------------- orchestration ----------------
async def scan_symbols_for_tf(symbols, tf):
    logger.info(f"Scanning {len(symbols)} symbols for TF {tf} ...")
    for sym in symbols:
        await process_symbol(sym, tf)

def compute_close_dt(now: datetime, tf: str) -> datetime:
    """
    Compute the most recent closed candle datetime (UTC) for given timeframe.
    """
    if tf == "30m":
        minute = now.minute
        close_min = 30 * (minute // 30)
        close_dt = now.replace(minute=close_min, second=0, microsecond=0)
        if close_dt > now:
            close_dt -= timedelta(minutes=30)
        return close_dt
    if tf == "1h":
        close_dt = now.replace(minute=0, second=0, microsecond=0)
        if close_dt > now:
            close_dt -= timedelta(hours=1)
        return close_dt
    if tf == "2h":
        hour = now.hour - (now.hour % 2)
        close_dt = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if close_dt > now:
            close_dt -= timedelta(hours=2)
        return close_dt
    return now.replace(second=0, microsecond=0)

async def main_loop():
    logger.info(f"Scanner started — WAIT_AFTER_CLOSE={WAIT_AFTER_CLOSE}s")
    last_processed = {"30m": None, "1h": None, "2h": None}
    while True:
        now = datetime.now(timezone.utc)
        due = []

        for tf in ["30m", "1h", "2h"]:
            close_dt = compute_close_dt(now, tf)
            close_ts = int(close_dt.timestamp())
            if now >= (close_dt + timedelta(seconds=WAIT_AFTER_CLOSE)) and last_processed.get(tf) != close_ts:
                last_processed[tf] = close_ts
                due.append((tf, close_dt))

        if due:
            # load and filter markets once
            try:
                markets = await load_markets_async()
                symbols = filter_usdt_perpetual(markets)
                logger.info(f"Due TFs: {[t for t, _ in due]}; symbols to scan: {len(symbols)}")
                if len(symbols) == 0:
                    logger.warning("No symbols found to scan. Check load_markets/connectivity.")
            except Exception as e:
                logger.warning(f"Failed to load markets: {e}")
                symbols = []

            for tf, close_dt in due:
                logger.info(f"⏳ Scanning timeframe {tf} for close {close_dt.isoformat()} (UTC) ...")
                await scan_symbols_for_tf(symbols, tf)
                await asyncio.sleep(2)

        await asyncio.sleep(LOOP_SLEEP)

# ---------------- entrypoint ----------------
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
