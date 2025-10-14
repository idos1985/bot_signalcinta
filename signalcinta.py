# signalcinta.py
"""
Final scanner (single-candle EMA20 & EMA100)
- TF: 30m, 1h, 2h
- Criteria (non-repainting, uses closed candle df.iloc[-2]):
    * Bullish: closed green candle, EMA20 & EMA100 both inside the candle body, EMA20 < EMA100
    * Bearish: closed red candle, EMA20 & EMA100 both inside the candle body, EMA20 > EMA100
- Targets: Binance USDⓈ-M (USDT perpetual)
- Skip: bearish if 1D rise >= 5%; bullish skip if 1D drop <= -5% (optional, but per earlier logic)
- Persist sent signals to sent_signals.json to avoid duplicates across restarts
- Telegram token/chat filled directly below (no env required)
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta

import aiohttp
import ccxt
import pandas as pd

# ---------------- CONFIG (Isi manual di sini) ----------------
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
TELEGRAM_CHAT_ID = "7183177114"

# Buffer after candle close (seconds) to avoid API lag
WAIT_AFTER_CLOSE = 120

# small pauses to reduce rate pressure
RATE_DELAY = 0.12
TF_SLEEP = 2  # seconds pause between TF runs
LOOP_SLEEP = 6  # main loop sleep when nothing due

SENT_FILE = "sent_signals.json"  # persistence

# ---------------- logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("signalcinta")

# ---------------- exchange (ccxt) ----------------
exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})

# ---------------- persistence helpers ----------------
def load_sent_signals():
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed load sent signals: {e}")
    return {}

def save_sent_signals(data):
    try:
        with open(SENT_FILE, "w", encoding="utf8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed save sent signals: {e}")

sent_signals = load_sent_signals()  # { "BTC/USDT": { "30m": ts, ... } }

# ---------------- Telegram sender ----------------
async def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token/chat id not set. Skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=15) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    logger.warning(f"Telegram returned {resp.status}: {txt}")
    except Exception as e:
        logger.warning(f"Telegram send error: {e}")

# ---------------- ccxt wrappers (blocking calls in thread) ----------------
def _load_markets_sync():
    return exchange.load_markets()

async def load_markets_async():
    return await asyncio.to_thread(_load_markets_sync)

def _fetch_ohlcv_sync(symbol: str, timeframe: str, limit: int):
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

async def fetch_ohlcv_df(symbol: str, timeframe: str, limit: int = 500):
    try:
        data = await asyncio.to_thread(_fetch_ohlcv_sync, symbol, timeframe, limit)
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df = df.set_index("time")
        # ensure floats
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
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

def detect_single_candle(df: pd.DataFrame):
    """
    Use closed candle df.iloc[-2] (non-repainting).
    Return (signal_type, open, close, close_time, ema20, ema100) or (None,...)
    """
    if df is None or len(df) < 120:
        return None, None, None, None, None, None

    d = compute_emas(df)
    sig = d.iloc[-2]  # candle that just closed
    open_p = float(sig["open"])
    close_p = float(sig["close"])
    ema20 = float(sig["ema20"])
    ema100 = float(sig["ema100"])
    close_time = sig.name  # tz-aware Timestamp

    lo = min(open_p, close_p)
    hi = max(open_p, close_p)

    # both EMAs must be inside the candle body [lo, hi]
    if not (lo <= ema20 <= hi and lo <= ema100 <= hi):
        return None, None, None, None, None, None

    # Bullish: green candle and ema20 < ema100
    if close_p > open_p and ema20 < ema100:
        return "bullish", open_p, close_p, close_time, ema20, ema100

    # Bearish: red candle and ema20 > ema100
    if close_p < open_p and ema20 > ema100:
        return "bearish", open_p, close_p, close_time, ema20, ema100

    return None, None, None, None, None, None

# ---------------- daily change helper ----------------
async def daily_change_pct(symbol: str):
    df = await fetch_ohlcv_df(symbol, "1d", limit=10)
    if df is None or len(df) < 2:
        return 0.0
    first_open = float(df.iloc[0]["open"])
    last_close = float(df.iloc[-1]["close"])
    return (last_close - first_open) / first_open * 100

# ---------------- symbol filter ----------------
def filter_usdt_perpetual(markets_dict):
    syms = []
    for sym, meta in markets_dict.items():
        try:
            info = meta.get("info", {})
            if info.get("contractType") == "PERPETUAL" and sym.endswith("/USDT"):
                syms.append(sym)
        except Exception:
            continue
    return syms

# ---------------- per-symbol processing ----------------
async def process_symbol(symbol: str, tf: str):
    try:
        df = await fetch_ohlcv_df(symbol, tf, limit=300)
        await asyncio.sleep(RATE_DELAY)
        if df is None or len(df) < 120:
            return

        sig_type, open_p, close_p, close_time, ema20, ema100 = detect_single_candle(df)
        if sig_type is None:
            return

        # daily filter
        dchg = await daily_change_pct(symbol)
        # Skip bearish if 1D rise >= 5%
        if sig_type == "bearish" and dchg > 5:
            return
        # Skip bullish if 1D drop <= -5% (as earlier logic sometimes used)
        if sig_type == "bullish" and dchg < -5:
            return

        close_ts = int(pd.Timestamp(close_time).timestamp())
        rec = sent_signals.get(symbol, {})
        prev_ts = rec.get(tf)
        if prev_ts == close_ts:
            return  # already sent this candle tf

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

        # persist
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
    # now must be timezone-aware UTC
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
    last_processed_local = {"30m": None, "1h": None, "2h": None}
    while True:
        now = datetime.now(timezone.utc)
        due = []

        for tf in ["30m", "1h", "2h"]:
            close_dt = compute_close_dt(now, tf)
            close_ts = int(close_dt.timestamp())
            if now >= (close_dt + timedelta(seconds=WAIT_AFTER_CLOSE)) and last_processed_local.get(tf) != close_ts:
                last_processed_local[tf] = close_ts
                due.append((tf, close_dt))

        if due:
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
                await asyncio.sleep(TF_SLEEP)

        await asyncio.sleep(LOOP_SLEEP)

# ---------------- entrypoint ----------------
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
