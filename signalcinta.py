# main.py
"""
Dual-direction scanner — Bullish & Bearish detection using EMA20 & EMA100 (same TF)
Timeframes: 30m, 1h, 2h
Scans all Binance USDⓈ-M (USDT perpetual) futures pairs.
Persists sent signals to sent_signals.json to avoid duplicate notifications.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
import ccxt
import pandas as pd
from telegram import Bot

# ---------------- CONFIG ----------------
BOT_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"   # <-- isi sendiri
CHAT_ID = "7183177114"       # <-- isi sendiri

# Binance futures (USDT perpetual)
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

bot = Bot(BOT_TOKEN)

# Persistence file for sent signals to avoid duplicates across restarts
SENT_FILE = "sent_signals.json"

# Rate & timing config
RATE_DELAY = 0.12           # seconds between fetches (reduce API pressure)
WAIT_AFTER_CLOSE = 20       # seconds to wait after candle close to ensure OHLCV finalized
SLEEP_GRANULARITY = 5       # loop sleep granularity in seconds

# Logging (minimal)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

# Track last processed close timestamps per TF to avoid double-processing in same run
last_processed = {"30m": None, "1h": None, "2h": None}


# ---------------- Persistence helpers ----------------
def load_sent_signals():
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_sent_signals(data):
    try:
        with open(SENT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Failed saving sent_signals: {e}")


# in-memory view of sent signals (load at startup)
sent_signals = load_sent_signals()  # dict: { "BTC/USDT": { "30m": ts, "1h": ts } }


# ---------------- Helpers ----------------
def now_utc():
    return datetime.now(timezone.utc)


async def safe_sleep(sec: float):
    await asyncio.sleep(sec)


async def send_telegram(msg: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int = 500):
    """
    Fetch OHLCV and return DataFrame indexed by tz-aware UTC 'time', or None on failure.
    """
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df = df.set_index("time")
        df = df.astype(float)
        return df
    except Exception as e:
        logging.debug(f"fetch_ohlcv error {symbol} {timeframe}: {e}")
        return None


def check_daily_drop(df1d):
    """Return True if daily drop >= 5% (from first open to last close)."""
    if df1d is None or len(df1d) < 2:
        return False
    first = df1d.iloc[0]["open"]
    last = df1d.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5


def detect_signal_from_tf(df_tf):
    """
    Given df for a timeframe (with index=time), check last closed candle vs EMA20 & EMA100 (same TF).
    Returns (signal_type, open, close, close_time, ema20, ema100) where signal_type in {"bullish","bearish", None}
    """
    if df_tf is None or len(df_tf) < 100:
        return None, None, None, None, None, None  # not enough data for EMA100

    d = df_tf.copy()
    # compute EMAs
    d["ema20"] = d["close"].ewm(span=20, adjust=False).mean()
    d["ema100"] = d["close"].ewm(span=100, adjust=False).mean()

    last = d.iloc[-1]
    open_p = float(last["open"])
    close_p = float(last["close"])
    ema20 = float(last["ema20"])
    ema100 = float(last["ema100"])
    close_time = last.name  # Timestamp (tz-aware UTC)

    lo = min(open_p, close_p)
    hi = max(open_p, close_p)

    # Bullish: candle green & EMA20 below both open & close & EMA100 above both open & close
    if (close_p > open_p) and (ema20 < open_p and ema20 < close_p) and (ema100 > open_p and ema100 > close_p):
        return "bullish", open_p, close_p, close_time, ema20, ema100

    # Bearish: candle red & EMA20 above both open & close & EMA100 below both open & close
    if (close_p < open_p) and (ema20 > open_p and ema20 > close_p) and (ema100 < open_p and ema100 < close_p):
        return "bearish", open_p, close_p, close_time, ema20, ema100

    return None, None, None, None, None, None


# ---------------- Processor for a timeframe ----------------
async def process_tf_for_exchange(exchange, name, tf_check, symbols):
    """
    Process scanning for tf_check (one of '30m','1h','2h').
    For each symbol: daily drop filter -> fetch tf_check -> detect bullish/bearish as defined -> send notif if new
    """
    errors = 0
    sent = 0
    for symbol in symbols:
        try:
            # daily filter (skip if already dropped >=5%)
            df1d = fetch_ohlcv_df(exchange, symbol, "1d", 10)
            await safe_sleep(RATE_DELAY)
            if df1d is None or check_daily_drop(df1d):
                continue

            # fetch tf_check candles (we need latest closed candle)
            df_check = fetch_ohlcv_df(exchange, symbol, tf_check, 260)
            await safe_sleep(RATE_DELAY)
            if df_check is None or len(df_check) < 100:
                continue

            signal_type, open_c, close_c, close_t, ema20_val, ema100_val = detect_signal_from_tf(df_check)
            if signal_type is None:
                continue

            close_ts = int(pd.Timestamp(close_t).timestamp())
            sig_key = f"{tf_check}-{symbol}-{close_ts}"

            # check persistence to avoid duplicates
            sym_record = sent_signals.get(symbol, {})
            prev_ts = sym_record.get(tf_check)
            if prev_ts == close_ts:
                continue  # already sent for this candle

            # build message
            if signal_type == "bullish":
                header = "✅ VALID BULLISH SIGNAL"
            else:
                header = "🔻 VALID BEARISH SIGNAL"

            msg = (
                f"{header}\n"
                f"TF detected : {tf_check}\n"
                f"Exchange    : {name}\n"
                f"Pair        : {symbol}\n"
                f"Open        : {open_c:.8f}\n"
                f"Close       : {close_c:.8f}\n"
                f"Candle UTC  : {close_t.isoformat()}\n"
                f"EMA20 ({tf_check}) = {ema20_val:.8f}\n"
                f"EMA100({tf_check}) = {ema100_val:.8f}\n"
            )

            # send and persist
            await send_telegram(msg)

            sent_signals.setdefault(symbol, {})[tf_check] = close_ts
            save_sent_signals(sent_signals)
            sent += 1

        except Exception:
            errors += 1

    if errors:
        logging.warning(f"{name} {tf_check}: {errors} symbols failed.")
    if sent:
        logging.warning(f"{name} {tf_check}: {sent} signals sent.")


# ---------------- Scheduler & Main Loop ----------------
async def main():
    # main loop: check every SLEEP_GRANULARITY seconds; trigger processing when candle close + WAIT_AFTER_CLOSE reached
    while True:
        utc = now_utc()
        minute = utc.minute
        second = utc.second
        due = []

        # only run a TF after WAIT_AFTER_CLOSE seconds since the close time
        if second >= WAIT_AFTER_CLOSE:
            # 30m: minute % 30 == 0
            if minute % 30 == 0:
                close_dt_30 = utc.replace(second=0, microsecond=0) - timedelta(minutes=(utc.minute % 30))
                close_ts = int(close_dt_30.timestamp())
                if last_processed["30m"] != close_ts:
                    due.append("30m")
                    last_processed["30m"] = close_ts

            # 1h: minute == 0
            if minute == 0:
                close_dt_1h = utc.replace(minute=0, second=0, microsecond=0)
                close_ts = int(close_dt_1h.timestamp())
                if last_processed["1h"] != close_ts:
                    due.append("1h")
                    last_processed["1h"] = close_ts

                # 2h: every even hour
                if utc.hour % 2 == 0:
                    close_dt_2h = close_dt_1h
                    close_ts = int(close_dt_2h.timestamp())
                    if last_processed["2h"] != close_ts:
                        due.append("2h")
                        last_processed["2h"] = close_ts

        # load markets once per loop (USDT perpetual symbols)
        try:
            markets = exchange_usdm.load_markets()
            # choose common perpetual symbols ending with '/USDT'
            symbols = [s for s in markets if s.endswith("/USDT")]
        except Exception as e:
            logging.warning(f"Failed loading markets: {e}")
            symbols = []

        # sequentially process due TFs (reduce API pressure)
        for tf in due:
            if tf == "30m":
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "30m", symbols)
            elif tf == "1h":
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "1h", symbols)
            elif tf == "2h":
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "2h", symbols)

        # small sleep before re-evaluating
        await asyncio.sleep(SLEEP_GRANULARITY)


if __name__ == "__main__":
    asyncio.run(main())
