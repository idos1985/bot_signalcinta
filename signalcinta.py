# signalcinta.py
"""
Final scanner — Bullish & Bearish Engulfing + EMA20 & EMA100 (non-repainting)
- Timeframes scanned: 30m, 1h, 2h
- Scan runs shortly AFTER candle closes (WAIT_AFTER_CLOSE seconds)
- Targets: Binance USDⓈ-M (USDT perpetual) pairs (from ccxt.load_markets)
- Persist sent signals to sent_signals.json to avoid duplicates
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
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"      # or fill "YOUR_TOKEN"
TELEGRAM_CHAT_ID = "7183177114"  # or fill "YOUR_CHAT_ID"
DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")

# Wait buffer after candle close to avoid missing data (2 minutes recommended)
WAIT_AFTER_CLOSE = int(os.getenv("WAIT_AFTER_CLOSE", "120"))

# Rate control
RATE_DELAY = float(os.getenv("RATE_DELAY", "0.12"))   # small sleep between symbol fetches
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", "6"))        # main loop wake frequency in seconds

# persistence
SENT_FILE = os.getenv("SENT_FILE", "sent_signals.json")

# CCXT exchange
exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

# logging
level = logging.DEBUG if DEBUG else logging.INFO
logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("signalcinta")

# ---------------- persistence helpers ----------------
def load_sent_signals():
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf8") as f:
                data = json.load(f)
                # keys: symbol -> { "30m": ts, "1h": ts, "2h": ts }
                return data
        except Exception as e:
            logger.warning(f"Failed to load sent_signals: {e}")
    return {}

def save_sent_signals(data):
    try:
        with open(SENT_FILE, "w", encoding="utf8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed to save sent_signals: {e}")

sent_signals = load_sent_signals()

# track last processed closes during runtime to avoid double processing
last_processed = {"30m": None, "1h": None, "2h": None}

# ---------------- helpers ----------------
async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token/chat id not set — skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=15) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(f"Telegram returned non-200 {resp.status}: {text}")
    except Exception as e:
        logger.warning(f"Telegram send error: {e}")

# CCXT blocking functions wrapped in asyncio.to_thread
def _load_markets_sync():
    return exchange.load_markets()

async def load_markets():
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

# ---------------- detection logic ----------------
def compute_emas(df: pd.DataFrame):
    d = df.copy()
    d["ema20"] = d["close"].ewm(span=20, adjust=False).mean()
    d["ema100"] = d["close"].ewm(span=100, adjust=False).mean()
    return d

def detect_signal(df: pd.DataFrame):
    """
    Non-repainting detection: evaluate the last CLOSED candle -> df.iloc[-2]
    Returns (signal_type, open, close, close_time, ema20, ema100)
    signal_type: "bullish" | "bearish" | None
    """
    if df is None or len(df) < 201:  # need enough candles for EMA100
        return None, None, None, None, None, None

    d = compute_emas(df)

    # previous candle (the one before signal candle)
    prev = d.iloc[-3]
    sig = d.iloc[-2]  # the candle that just closed

    open_p = float(sig["open"])
    close_p = float(sig["close"])
    ema20 = float(sig["ema20"])
    ema100 = float(sig["ema100"])
    close_time = sig.name  # tz-aware Timestamp
    lo = min(open_p, close_p)
    hi = max(open_p, close_p)

    # bullish engulfing: prev red, sig green, sig body engulfs prev body
    bullish = (prev["close"] < prev["open"]) and (close_p > open_p) and (close_p > float(prev["open"])) and (open_p < float(prev["close"]))
    # bearish engulfing: prev green, sig red, sig body engulfs prev body
    bearish = (prev["close"] > prev["open"]) and (close_p < open_p) and (close_p < float(prev["open"])) and (open_p > float(prev["close"]))

    bullish_ema_ok = (ema20 >= lo) and (ema100 <= hi) and (ema20 < ema100)
    bearish_ema_ok = (ema20 <= hi) and (ema100 >= lo) and (ema20 > ema100)

    if bullish and bullish_ema_ok:
        return "bullish", open_p, close_p, close_time, ema20, ema100
    if bearish and bearish_ema_ok:
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

# ---------------- symbol list helper ----------------
def filter_usdt_perpetual_markets(markets_dict):
    """
    markets_dict from exchange.load_markets() (ccxt)
    return list of symbol strings that represent USDT perpetuals (e.g. 'BTC/USDT')
    """
    syms = []
    for sym, meta in markets_dict.items():
        try:
            info = meta.get("info", {})
            if (info.get("contractType") == "PERPETUAL") and sym.endswith("/USDT"):
                syms.append(sym)
        except Exception:
            continue
    return syms

# ---------------- process single symbol ----------------
async def process_symbol(symbol: str, tf: str):
    try:
        # fetch klines
        df = await fetch_ohlcv_df(symbol, tf, limit=300)
        await asyncio.sleep(RATE_DELAY)
        if df is None or len(df) < 201:
            return

        sig_type, open_p, close_p, close_time, ema20, ema100 = detect_signal(df)
        if sig_type is None:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: no valid signal on closed candle.")
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
        # check persisted duplicates (exact timestamp)
        sym_record = sent_signals.get(symbol, {})
        prev_ts = sym_record.get(tf)
        if prev_ts == close_ts:
            if DEBUG:
                logger.debug(f"{symbol} {tf}: signal already sent for close_ts {close_ts}")
            return

        # send message
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
        logger.info(f"Sent {sig_type} signal for {symbol} {tf} at {close_time.isoformat()}")

    except Exception as e:
        logger.debug(f"process_symbol error {symbol} {tf}: {e}")

# ---------------- orchestration ----------------
async def scan_symbols_for_tf(symbols, tf):
    logger.info(f"Scanning {len(symbols)} symbols for TF {tf} ...")
    for sym in symbols:
        await process_symbol(sym, tf)

# compute last close datetime for a timeframe based on current UTC time
def compute_close_dt(now: datetime, tf: str) -> datetime:
    # now must be timezone-aware UTC
    if tf == "30m":
        # close at minute 0 or 30
        minute = now.minute
        close_min = 30 * (minute // 30)  # 0 or 30
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
    logger.info("Scanner started (waiting for scheduled closes)... WAIT_AFTER_CLOSE = %s sec", WAIT_AFTER_CLOSE)
    while True:
        now = datetime.now(timezone.utc)
        due = []

        # check each timeframe
        for tf in ["30m", "1h", "2h"]:
            close_dt = compute_close_dt(now, tf)
            close_ts = int(close_dt.timestamp())
            # process only if now is at least close_dt + WAIT_AFTER_CLOSE and not processed in this run
            if now >= (close_dt + timedelta(seconds=WAIT_AFTER_CLOSE)) and last_processed.get(tf) != close_ts:
                last_processed[tf] = close_ts
                due.append((tf, close_dt))

        if due:
            # load markets once
            try:
                markets = await load_markets()
                symbols = filter_usdt_perpetual_markets(markets)
                logger.info(f"Due TFs: {[t for t, _ in due]}; symbols to scan: {len(symbols)}")
                # quick check for specific known symbols
                if "4USDT" in symbols:
                    logger.info("4USDT present in symbols list.")
                else:
                    logger.debug("4USDT not found in symbols list.")
            except Exception as e:
                logger.warning(f"Failed to load markets: {e}")
                symbols = []

            # process each due TF sequentially (reduce API pressure)
            for tf, close_dt in due:
                logger.info(f"⏳ Scanning timeframe {tf} for close {close_dt.isoformat()} (UTC) ...")
                await scan_symbols_for_tf(symbols, tf)
                # pause a bit between tf runs
                await asyncio.sleep(2)

        await asyncio.sleep(LOOP_SLEEP)

# ---------------- entrypoint ----------------
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
