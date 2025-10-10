# main.py
"""
Bot scanner — Bullish Engulfing + EMA20 bawah & EMA100 atas (same timeframe)
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

# use Binance futures (USDT perpetual)
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

bot = Bot(BOT_TOKEN)

# persistent file for sent signals
SENT_FILE = "sent_signals.json"

# rate and timing
RATE_DELAY = 0.12           # seconds between fetches (reduce API pressure)
WAIT_AFTER_CLOSE = 20       # seconds to wait after candle close to ensure OHLCV finalized
SLEEP_GRANULARITY = 5       # seconds loop sleep granularity

# logging (keep minimal to avoid Railway log limits)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

# track last processed close timestamps per TF to avoid double-processing in same run
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
sent_signals = load_sent_signals()  # dict: { "BTCUSDT": { "30m": timestamp, "1h": ts } }


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


def is_bullish_engulfing(df):
    """
    Detect bullish engulfing where last candle (c3) is green and engulfs previous 2 red candles.
    Return (True, open_c3, close_c3, close_time) or (False,...).
    """
    if df is None or len(df) < 3:
        return False, None, None, None
    try:
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
    except Exception:
        return False, None, None, None

    if (c1["close"] < c1["open"]) and (c2["close"] < c2["open"]) and (c3["close"] > c3["open"]):
        min_open = min(c1["open"], c2["open"])
        max_close = max(c1["close"], c2["close"])
        if (c3["open"] < min_open) and (c3["close"] > max_close):
            return True, float(c3["open"]), float(c3["close"]), c3.name
    return False, None, None, None


def check_daily_drop(df1d):
    """Return True if daily drop >= 5% (from first open to last close)."""
    if df1d is None or len(df1d) < 2:
        return False
    first = df1d.iloc[0]["open"]
    last = df1d.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5


def ema20_ema100_in_range_and_order(open_p, close_p, df_tf):
    """
    Compute EMA20 and EMA100 on df_tf (same TF).
    Check both lie inside [min(open,close), max(open,close)] and EMA20 < EMA100.
    Return (True, ema20_val, ema100_val) or (False, None, None)
    """
    if df_tf is None:
        return False, None, None
    if len(df_tf) < 100:  # need enough candles for EMA100
        return False, None, None

    d = df_tf.copy()
    d["ema20"] = d["close"].ewm(span=20, adjust=False).mean()
    d["ema100"] = d["close"].ewm(span=100, adjust=False).mean()
    ema20 = d.iloc[-1]["ema20"]
    ema100 = d.iloc[-1]["ema100"]
    if pd.isna(ema20) or pd.isna(ema100):
        return False, None, None

    lo = min(open_p, close_p)
    hi = max(open_p, close_p)
    if (lo <= ema20 <= hi) and (lo <= ema100 <= hi) and (ema20 < ema100):
        return True, float(ema20), float(ema100)
    return False, None, None


# ---------------- Processor for a timeframe ----------------
async def process_tf_for_exchange(exchange, name, tf_check, symbols):
    """
    Process scanning for tf_check (one of '30m','1h','2h').
    - fetch TF candles, detect engulfing, calculate EMA20 & EMA100 on same TF,
      verify condition and send Telegram.
    """
    errors = 0
    sent = 0
    for symbol in symbols:
        try:
            # daily filter
            df1d = fetch_ohlcv_df(exchange, symbol, "1d", 10)
            await safe_sleep(RATE_DELAY)
            if df1d is None or check_daily_drop(df1d):
                continue

            # fetch tf_check candles (want latest closed candle)
            df_check = fetch_ohlcv_df(exchange, symbol, tf_check, 250)
            await safe_sleep(RATE_DELAY)
            if df_check is None or len(df_check) < 3:
                continue

            engulf, open_c, close_c, close_t = is_bullish_engulfing(df_check)
            if not engulf:
                continue

            # fetch same TF for EMA calculation
            df_ma = fetch_ohlcv_df(exchange, symbol, tf_check, 500)
            await safe_sleep(RATE_DELAY)
            if df_ma is None:
                continue

            ok, ema20_val, ema100_val = ema20_ema100_in_range_and_order(open_c, close_c, df_ma)
            if not ok:
                continue

            # unique id: tf-exchange-symbol-close_ts
            close_ts = int(pd.Timestamp(close_t).timestamp())
            sig_id = f"{tf_check}-{symbol}-{close_ts}"
            # check persistent sent_signals mapping
            sym_record = sent_signals.get(symbol, {})
            prev_ts = sym_record.get(tf_check)
            if prev_ts == close_ts:
                # already sent this exact candle
                continue

            # send
            msg = (
                "✅ VALID SIGNAL\n"
                f"TF detected : {tf_check}\n"
                f"Exchange    : {name}\n"
                f"Pair        : {symbol}\n"
                f"Open        : {open_c:.8f}\n"
                f"Close       : {close_c:.8f}\n"
                f"Candle UTC  : {close_t.isoformat()}\n"
                f"EMA20 ({tf_check}) = {ema20_val:.8f}\n"
                f"EMA100({tf_check}) = {ema100_val:.8f}\n"
            )
            await send_telegram(msg)

            # persist sent signal
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

                # 2h: hour % 2 == 0 (every even hour)
                if utc.hour % 2 == 0:
                    close_dt_2h = close_dt_1h  # same time align
                    close_ts = int(close_dt_2h.timestamp())
                    if last_processed["2h"] != close_ts:
                        due.append("2h")
                        last_processed["2h"] = close_ts

        # load markets once per loop (USDT perpetual symbols)
        try:
            markets = exchange_usdm.load_markets()
            # pick symbols ending with /USDT (typical futures ticker)
            symbols = [s for s in markets if s.endswith("/USDT")]
        except Exception as e:
            logging.warning(f"Failed loading markets: {e}")
            symbols = []

        # sequentially process each due TF (reduce API pressure)
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
