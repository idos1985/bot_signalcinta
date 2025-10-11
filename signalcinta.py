# signalcinta.py
"""
Final scanner — Bullish & Bearish Engulfing + EMA20 & EMA100 (non-repainting)
Timeframes scanned: 30m, 1h, 2h
Behavior:
 - Scan after candle close (wait WAIT_AFTER_CLOSE seconds)
 - EMA20 & EMA100 computed on same timeframe, and checked at the candle close moment
 - Bullish skip if 1D drop >= 5%
 - Bearish skip if 1D rise >= 5%
 - Persist sent signals to sent_signals.json to avoid duplicates across restarts
 - Target: Binance USDⓈ-M (USDT perpetual)
"""

import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone, timedelta

import ccxt
import pandas as pd
import aiohttp

# ---------------- CONFIG ----------------
# You can set these as environment variables in Railway, or fill here directly
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "7183177114")

# ccxt Binance futures (USDT perpetual)
exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

# persistence file for sent signals
SENT_FILE = "sent_signals.json"

# timing
WAIT_AFTER_CLOSE = 20      # seconds to wait after candle close before scanning (buffer)
RATE_DELAY = 0.12          # small sleep between fetches to reduce API pressure
LOOP_SLEEP = 6             # how often main loop wakes up to check schedule (seconds)

# logging: keep moderate
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# keep in-memory and persisted sent signals
def load_sent():
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_sent(data):
    try:
        with open(SENT_FILE, "w", encoding="utf8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Failed to save sent signals: {e}")

sent_signals = load_sent()  # structure: { "BTC/USDT": { "30m": ts, "1h": ts, "2h": ts } }

# track last processed close timestamps in-memory to avoid reprocessing within same run
last_processed = {"30m": None, "1h": None, "2h": None}

# ---------------- Networking / Telegram ----------------
async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram token/chat id not set. Skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=15) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    logging.warning(f"Telegram non-200: {resp.status} - {txt}")
    except Exception as e:
        logging.warning(f"Telegram send error: {e}")

# ---------------- CCXT helpers (blocking) wrapped in to_thread ----------------
def _fetch_ohlcv_sync(symbol: str, timeframe: str, limit: int = 500):
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

async def fetch_ohlcv_df(symbol: str, timeframe: str, limit: int = 500):
    """
    Fetch OHLCV via ccxt in a thread, return DataFrame with tz-aware UTC 'time' index.
    Returns None on failure.
    """
    try:
        data = await asyncio.to_thread(_fetch_ohlcv_sync, symbol, timeframe, limit)
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        # ensure numeric floats
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        df = df.set_index("time")
        return df
    except Exception as e:
        logging.debug(f"fetch_ohlcv_df error {symbol}-{timeframe}: {e}")
        return None

def _load_markets_sync():
    return exchange.load_markets()

async def load_markets_async():
    return await asyncio.to_thread(_load_markets_sync)

# ---------------- EMA & pattern logic ----------------
def compute_emas(df: pd.DataFrame):
    # compute EMA20 and EMA100 (named ema20, ema100)
    df = df.copy()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema100"] = df["close"].ewm(span=100, adjust=False).mean()
    return df

def detect_engulfing_and_ema(df: pd.DataFrame):
    """
    Use df where last row is most recent (may be in-progress).
    We will check the last CLOSED candle: that is df.iloc[-2].
    Return tuple (signal_type, open, close, close_time, ema20, ema100)
      signal_type in {"bullish","bearish", None}
    """
    if df is None or len(df) < 201:
        return None, None, None, None, None, None  # need enough for ema100

    df2 = compute_emas(df)
    # Use the last CLOSED candle (index -2). df.iloc[-1] may be the in-progress candle.
    c_prev = df2.iloc[-3]  # the candle before the signal candle
    c_sig = df2.iloc[-2]   # the candle that just closed (signal candle)
    # Values
    open_p = float(c_sig["open"])
    close_p = float(c_sig["close"])
    ema20 = float(c_sig["ema20"])
    ema100 = float(c_sig["ema100"])
    close_time = c_sig.name  # Timestamp (tz-aware)
    lo = min(open_p, close_p)
    hi = max(open_p, close_p)

    # Bullish Engulfing: previous two? we check immediate previous candle c_prev vs c_sig.
    bullish = (c_prev["close"] < c_prev["open"]) and (close_p > open_p) and (close_p > float(c_prev["open"])) and (open_p < float(c_prev["close"]))
    bearish = (c_prev["close"] > c_prev["open"]) and (close_p < open_p) and (close_p < float(c_prev["open"])) and (open_p > float(c_prev["close"]))

    # EMA conditions (same timeframe)
    bullish_ema_ok = (ema20 >= lo) and (ema100 <= hi) and (ema20 < ema100)
    bearish_ema_ok = (ema20 <= hi) and (ema100 >= lo) and (ema20 > ema100)

    if bullish and bullish_ema_ok:
        return "bullish", open_p, close_p, close_time, ema20, ema100
    if bearish and bearish_ema_ok:
        return "bearish", open_p, close_p, close_time, ema20, ema100
    return None, None, None, None, None, None

# ---------------- daily change helper ----------------
async def daily_change_pct(symbol: str):
    # fetch last 2 daily candles
    df = await fetch_ohlcv_df(symbol, "1d", limit=3)
    if df is None or len(df) < 2:
        return 0.0
    first = float(df.iloc[0]["open"])
    last = float(df.iloc[-1]["close"])
    change = (last - first) / first * 100
    return change

# ---------------- process single symbol ----------------
async def process_symbol_for_tf(symbol: str, tf: str):
    """
    Process one symbol for given timeframe tf ("30m","1h","2h").
    """
    try:
        # fetch recent klines (enough candles: 300)
        df = await fetch_ohlcv_df(symbol, tf, limit=300)
        await asyncio.sleep(RATE_DELAY)
        if df is None or len(df) < 201:
            return  # insufficient data for EMA100

        sig_type, o, c, close_time, ema20, ema100 = detect_engulfing_and_ema(df)
        if sig_type is None:
            return

        # daily filter: bullish skip if 1D drop >=5%; bearish skip if 1D rise >=5%
        dchg = await daily_change_pct(symbol)
        if sig_type == "bullish" and dchg < -5:
            # skip
            return
        if sig_type == "bearish" and dchg > 5:
            # skip
            return

        # create persistent record and avoid duplicate
        close_ts = int(pd.Timestamp(close_time).timestamp())
        sym_record = sent_signals.get(symbol, {})
        prev_ts = sym_record.get(tf)
        if prev_ts == close_ts:
            return  # already sent this candle

        # prepare message
        if sig_type == "bullish":
            header = "🟢 VALID BULLISH SIGNAL"
        else:
            header = "🔴 VALID BEARISH SIGNAL"

        msg = (
            f"{header}\n"
            f"TF detected : {tf}\n"
            f"Pair        : {symbol}\n"
            f"Open        : {o:.8f}\n"
            f"Close       : {c:.8f}\n"
            f"Candle UTC  : {close_time.isoformat()}\n"
            f"EMA20 ({tf}) = {ema20:.8f}\n"
            f"EMA100({tf}) = {ema100:.8f}\n"
            f"1D change   : {dchg:.2f}%"
        )

        # send and persist
        await send_telegram(msg)
        sent_signals.setdefault(symbol, {})[tf] = close_ts
        save_sent(sent_signals)
        logging.info(f"Signal sent {symbol} {tf} {sig_type} at {close_time}")

    except Exception as e:
        logging.debug(f"process_symbol_for_tf error {symbol} {tf}: {e}")

# ---------------- scanning orchestration ----------------
async def scan_pairs_for_tf(symbols, tf):
    """
    Run process_symbol_for_tf for all symbols sequentially to limit rate.
    """
    for sym in symbols:
        await process_symbol_for_tf(sym, tf)

# ---------------- schedule logic (wait until after candle close) ----------------
def compute_close_dt(now: datetime, tf: str):
    """
    Given current UTC datetime, compute the most recent candle close datetime for tf.
    Returns tz-aware datetime object (UTC) representing close time.
    """
    # expect now timezone-aware UTC
    if tf == "30m":
        # close at minute 0 or 30
        minute = now.minute
        close_min = 30 * (minute // 30)  # 0 or 30
        close_dt = now.replace(minute=close_min, second=0, microsecond=0)
        # if now is exactly at 00s and we want previous close, ensure close_dt < now
        if close_dt > now:
            close_dt -= timedelta(minutes=30)
        return close_dt
    if tf == "1h":
        close_dt = now.replace(minute=0, second=0, microsecond=0)
        if close_dt > now:
            close_dt -= timedelta(hours=1)
        return close_dt
    if tf == "2h":
        # close at hour even numbers with minute=0
        hour = now.hour - (now.hour % 2)
        close_dt = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if close_dt > now:
            close_dt -= timedelta(hours=2)
        return close_dt
    # fallback
    return now.replace(second=0, microsecond=0)

async def main_loop():
    logging.info("Scanner started, waiting for scheduled closes...")
    while True:
        now = datetime.now(timezone.utc)
        # check each TF
        due = []
        for tf in ["30m", "1h", "2h"]:
            close_dt = compute_close_dt(now, tf)
            # ensure we only process once per close (use timestamp integer as key)
            close_ts = int(close_dt.timestamp())
            # is it time? only if now >= close_dt + WAIT_AFTER_CLOSE and we haven't processed this close yet
            if now >= (close_dt + timedelta(seconds=WAIT_AFTER_CLOSE)) and last_processed.get(tf) != close_ts:
                last_processed[tf] = close_ts
                due.append((tf, close_dt))

        if due:
            # load markets once and filter USDⓈ-M perpetual symbols (ccxt symbol format: 'BTC/USDT')
            markets = await load_markets_async()
            symbols = [s for s, m in markets.items() if m.get("info", {}).get("contractType") == "PERPETUAL" and s.endswith("/USDT")]
            logging.info(f"Due TFs: {[t for t, _ in due]}; symbols to scan: {len(symbols)}")

            # process each tf sequentially (less API pressure)
            for tf, close_dt in due:
                logging.info(f"⏳ Scanning timeframe {tf} for close {close_dt.isoformat()} (UTC) ...")
                await scan_pairs_for_tf(symbols, tf)
                # small pause between TF runs
                await asyncio.sleep(2)

        await asyncio.sleep(LOOP_SLEEP)

# ---------------- entrypoint ----------------
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("Stopped by user")
