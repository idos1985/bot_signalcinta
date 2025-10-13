# signalcinta.py
"""
Single-candle EMA20 & EMA100 scanner (final)
- TF: 30m, 1h, 2h (scan shortly AFTER candle close)
- Criteria:
  * Bullish: green candle (close>open), EMA20 & EMA100 both inside candle body, EMA20 < EMA100
  * Bearish: red candle (close<open), EMA20 & EMA100 both inside candle body, EMA20 > EMA100
- Non-repainting: use closed candle (df.iloc[-2]) and EMA values at that candle
- Skip: Bullish skip if 1D drop >=5%; Bearish skip if 1D rise >=5%
- Targets: Binance USDⓈ-M (USDT perpetual)
- Persist sent signals in sent_signals.json
"""

import os
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta

import ccxt
import pandas as pd
import aiohttp

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM", "")      # set in Railway env or fill here
TELEGRAM_CHAT_ID = os.getenv("7183177114", "")  # set in Railway env or fill here
WAIT_AFTER_CLOSE = int(os.getenv("WAIT_AFTER_CLOSE", "120"))  # seconds buffer after candle close
RATE_DELAY = float(os.getenv("RATE_DELAY", "0.12"))
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", "6"))
SENT_FILE = os.getenv("SENT_FILE", "sent_signals.json")
DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")

# CCXT Binance futures (USDT perpetual)
exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

# logging
level = logging.DEBUG if DEBUG else logging.INFO
logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("signalcinta")

# ---------------- persistence ----------------
def load_sent():
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed reading {SENT_FILE}: {e}")
    return {}

def save_sent(data):
    try:
        with open(SENT_FILE, "w", encoding="utf8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed saving {SENT_FILE}: {e}")

sent_signals = load_sent()  # { "BTC/USDT": { "30m": ts, ... } }
last_processed = {"30m": None, "1h": None, "2h": None}

# ---------------- Telegram ----------------
async def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token/chat id not configured. Skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=15) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    logger.warning(f"Telegram non-200 {resp.status}: {txt}")
    except Exception as e:
        logger.warning(f"Telegram send error: {e}")

# ---------------- ccxt wrappers ----------------
def _load_markets_sync():
    return exchange.load_markets()

async def load_markets_async():
    return await asyncio.to_thread(_load_markets_sync)

def _fetch_ohlcv_sync(symbol, timeframe, limit):
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

async def fetch_ohlcv_df(symbol: str, timeframe: str, limit: int = 500):
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

# ---------------- EMA & single-candle detection ----------------
def compute_emas(df: pd.DataFrame):
    d = df.copy()
    d["ema20"] = d["close"].ewm(span=20, adjust=False).mean()
    d["ema100"] = d["close"].ewm(span=100, adjust=False).mean()
    return d

def detect_single_candle(df: pd.DataFrame):
    """
    Use df where last row may be in-progress. Evaluate the last CLOSED candle at df.iloc[-2].
    Return (type, open, close, close_time, ema20, ema100) where type in {"bullish","bearish", None}
    """
    if df is None or len(df) < 120:  # need enough candles for EMA100
        return None, None, None, None, None, None

    d = compute_emas(df)
    sig = d.iloc[-2]    # closed candle
    open_p = float(sig["open"])
    close_p = float(sig["close"])
    ema20 = float(sig["ema20"])
    ema100 = float(sig["ema100"])
    close_time = sig.name
    lo = min(open_p, close_p)
    hi = max(open_p, close_p)

    # Both EMA must lie inside body [lo, hi]
    ema_inside = (lo <= ema20 <= hi) and (lo <= ema100 <= hi)
    if not ema_inside:
        return None, None, None, None, None, None

    if close_p > open_p and (ema20 < ema100):  # bullish
        return "bullish", open_p, close_p, close_time, ema20, ema100
    if close_p < open_p and (ema20 > ema100):  # bearish
        return "bearish", open_p, close_p, close_time, ema20, ema100

    return None, None, None, None, None, None

# ---------------- daily change ----------------
async def daily_change_pct(symbol: str):
    df = await fetch_ohlcv_df(symbol, "1d", limit=10)
    if df is None or len(df) < 2:
        return 0.0
    first_open = float(df.iloc[0]["open"])
    last_close = float(df.iloc[-1]["close"])
    change = (last_close - first_open) / first_open * 100
    return change

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

# ---------------- process one symbol ----------------
async def process_symbol(symbol: str, tf: str):
    try:
        df = await fetch_ohlcv_df(symbol, tf, limit=300)
        await asyncio.sleep(RATE_DELAY)
        if df is None or len(df) < 120:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: insufficient data ({None if df is None else len(df)})")
            return

        sig_type, open_p, close_p, close_time, ema20, ema100 = detect_single_candle(df)
        if sig_type is None:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: no signal on closed candle")
            return

        # daily filter
        dchg = await daily_change_pct(symbol)
        if sig_type == "bullish" and dchg < -5:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: skip bullish because 1D drop {dchg:.2f}%")
            return
        if sig_type == "bearish" and dchg > 5:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: skip bearish because 1D rise {dchg:.2f}%")
            return

        close_ts = int(pd.Timestamp(close_time).timestamp())
        sym_record = sent_signals.get(symbol, {})
        prev_ts = sym_record.get(tf)
        if prev_ts == close_ts:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: already sent for close_ts {close_ts}")
            return

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

        sent_signals.setdefault(symbol, {})[tf] = close_ts
        save_sent(sent_signals)
        logger.info(f"Sent {sig_type} signal for {symbol} {tf} at {close_time.isoformat()}")

    except Exception as e:
        logger.debug(f"process_symbol error {symbol} {tf}: {e}")

# ---------------- scan orchestration ----------------
async def scan_symbols_for_tf(symbols, tf):
    logger.info(f"Scanning {len(symbols)} symbols for TF {tf} ...")
    for sym in symbols:
        await process_symbol(sym, tf)

# compute last closed candle datetime for tf (UTC)
def compute_close_dt(now: datetime, tf: str) -> datetime:
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

# ---------------- main loop ----------------
async def main_loop():
    logger.info("Scanner started. WAIT_AFTER_CLOSE=%s sec", WAIT_AFTER_CLOSE)
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
            try:
                markets = await load_markets_async()
                symbols = filter_usdt_perpetual(markets)
                logger.info(f"Due TFs: {[t for t, _ in due]}; symbols to scan: {len(symbols)}")
                if len(symbols) == 0:
                    logger.warning("No symbols found to scan. Check exchange.load_markets() and connectivity.")
            except Exception as e:
                logger.warning(f"Failed to load markets: {e}")
                symbols = []

            for tf, close_dt in due:
                logger.info(f"⏳ Scanning timeframe {tf} for close {close_dt.isoformat()} (UTC) ...")
                await scan_symbols_for_tf(symbols, tf)
                await asyncio.sleep(2)

        await asyncio.sleep(LOOP_SLEEP)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
