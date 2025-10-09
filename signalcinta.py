# main.py
"""
Bot scanner — Bullish Engulfing + EMA20 bawah & EMA200 atas (same timeframe)
Timeframes: 15m (scan every 15m), 30m (every 30m), 1h (every 1h)
Scans all Binance futures pairs (USDⓈ-M and COIN-M).
"""

import asyncio
import logging
from datetime import datetime, timezone
import ccxt
import pandas as pd
from telegram import Bot

# ---------------- CONFIG ----------------
BOT_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"   # <-- isi sendiri
CHAT_ID = "7183177114"       # <-- isi sendiri

# Exchanges instances: futures and coin-margined (delivery)
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
exchange_coinm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'delivery'}})

bot = Bot(BOT_TOKEN)

# state for deduplication
sent_signals = set()

# small delay between fetches to reduce API pressure
RATE_DELAY = 0.12  # seconds

# wait after UTC candle close to ensure OHLCV finalized
WAIT_AFTER_CLOSE = 20  # seconds

# logging (railway-friendly)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

# keep track last processed candle timestamp for each TF to avoid double processing
last_processed = {"15m": None, "30m": None, "1h": None}


# ---------------- HELPERS ----------------
def now_utc():
    return datetime.now(timezone.utc)


async def safe_sleep(sec: float):
    await asyncio.sleep(sec)


async def send_telegram(msg: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int = 300):
    """
    Fetch OHLCV from exchange and return pandas DataFrame indexed by tz-aware UTC 'time'.
    Return None on failure.
    """
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df = df.set_index("time")
        # ensure numeric floats
        df = df.astype(float)
        return df
    except Exception as e:
        # debug-level to file logs if you enable file handler; avoid flooding console
        logging.debug(f"fetch_ohlcv error {symbol} {timeframe}: {e}")
        return None


def is_bullish_engulfing(df):
    """
    Detect bullish engulfing: last candle (c3) green and engulfs previous 2 red candles.
    Return (True, open_c3, close_c3, close_time) or (False, None, None, None).
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
    """
    Return True if daily drop >= 5% (from first open to last close in df1d).
    """
    if df1d is None or len(df1d) < 2:
        return False
    first = df1d.iloc[0]["open"]
    last = df1d.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5


def ema20_ema200_in_range_and_order(open_p, close_p, df_tf):
    """
    Compute EMA20 and EMA200 on df_tf (same TF).
    Check both lie inside [min(open,close), max(open,close)] and EMA20 < EMA200.
    Return (True, ema20_val, ema200_val) or (False, None, None)
    """
    if df_tf is None:
        return False, None, None
    # need at least 200 candles for EMA200 to be meaningful
    if len(df_tf) < 200:
        return False, None, None

    d = df_tf.copy()
    # exponential moving averages
    d["ema20"] = d["close"].ewm(span=20, adjust=False).mean()
    d["ema200"] = d["close"].ewm(span=200, adjust=False).mean()
    ema20 = d.iloc[-1]["ema20"]
    ema200 = d.iloc[-1]["ema200"]
    if pd.isna(ema20) or pd.isna(ema200):
        return False, None, None

    lo = min(open_p, close_p)
    hi = max(open_p, close_p)
    if (lo <= ema20 <= hi) and (lo <= ema200 <= hi) and (ema20 < ema200):
        return True, float(ema20), float(ema200)
    return False, None, None


# ---------------- PER-TIMEFRAME PROCESS ----------------
async def process_tf_for_exchange(exchange, name, tf_check, symbols):
    """
    Process scanning for timeframe tf_check.
    tf_check in {"15m","30m","1h"}.
    For tf_check:
      - TF15m -> MA calculated on 15m
      - TF30m -> MA calculated on 30m
      - TF1h  -> MA calculated on 1h
    """
    errors = 0
    sent = 0
    # determine required MA timeframe (same as tf_check)
    tf_ma = tf_check

    for symbol in symbols:
        try:
            # daily filter
            df1d = fetch_ohlcv_df(exchange, symbol, "1d", 10)
            await safe_sleep(RATE_DELAY)
            if df1d is None or check_daily_drop(df1d):
                continue

            # fetch tf_check candles (we want the latest closed one)
            df_check = fetch_ohlcv_df(exchange, symbol, tf_check, 250)
            await safe_sleep(RATE_DELAY)
            if df_check is None or len(df_check) < 3:
                continue

            engulf, open_c, close_c, close_t = is_bullish_engulfing(df_check)
            if not engulf:
                continue

            # fetch same timeframe for EMA calculation
            df_ma = fetch_ohlcv_df(exchange, symbol, tf_ma, 500)
            await safe_sleep(RATE_DELAY)
            if df_ma is None:
                continue

            ok, ema20_val, ema200_val = ema20_ema200_in_range_and_order(open_c, close_c, df_ma)
            if not ok:
                continue

            # unique id: tf-exchange-symbol-timestamp
            sig_id = f"{tf_check}-{name}-{symbol}-{int(pd.Timestamp(close_t).timestamp())}"
            if sig_id in sent_signals:
                continue

            sent_signals.add(sig_id)
            sent += 1

            # send telegram (rounded)
            msg = (
                f"✅ VALID SIGNAL\n"
                f"TF detected : {tf_check}\n"
                f"Exchange    : {name}\n"
                f"Pair        : {symbol}\n"
                f"Open        : {open_c:.8f}\n"
                f"Close       : {close_c:.8f}\n"
                f"Candle UTC  : {close_t.isoformat()}\n"
                f"EMA20 ({tf_ma}) = {ema20_val:.8f}\n"
                f"EMA200({tf_ma}) = {ema200_val:.8f}\n"
            )
            await send_telegram(msg)

        except Exception:
            errors += 1

    if errors:
        logging.warning(f"{name} {tf_check}: {errors} symbols failed.")
    if sent:
        logging.warning(f"{name} {tf_check}: {sent} signals sent.")


# ---------------- SCHEDULER & MAIN LOOP ----------------
async def main():
    # run forever
    while True:
        utc = now_utc()
        minute = utc.minute
        second = utc.second

        # Determine due timeframes (require WAIT_AFTER_CLOSE seconds passed since candle close)
        due = []

        if second >= WAIT_AFTER_CLOSE:
            if minute % 15 == 0:
                # 15m close
                close_ts_15 = int(utc.replace(second=0, microsecond=0).timestamp()) - (utc.minute % 15) * 60
                if last_processed["15m"] != close_ts_15:
                    due.append("15m")
                    last_processed["15m"] = close_ts_15

            if minute % 30 == 0:
                close_ts_30 = int(utc.replace(second=0, microsecond=0).timestamp()) - (utc.minute % 30) * 60
                if last_processed["30m"] != close_ts_30:
                    due.append("30m")
                    last_processed["30m"] = close_ts_30

            if minute == 0:
                close_ts_1h = int(utc.replace(minute=0, second=0, microsecond=0).timestamp())
                if last_processed["1h"] != close_ts_1h:
                    due.append("1h")
                    last_processed["1h"] = close_ts_1h

        # load markets once per loop for each exchange
        try:
            markets_usdm = exchange_usdm.load_markets()
            symbols_usdm = [s for s in markets_usdm if s.endswith(("USDT", "USD"))]
        except Exception as e:
            logging.warning(f"Failed loading USDM markets: {e}")
            symbols_usdm = []

        try:
            markets_coinm = exchange_coinm.load_markets()
            symbols_coinm = [s for s in markets_coinm if s.endswith(("USDT", "USD"))]
        except Exception as e:
            logging.warning(f"Failed loading COINM markets: {e}")
            symbols_coinm = []

        # process each due TF sequentially to reduce API pressure
        for tf in due:
            if tf == "15m":
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "15m", symbols_usdm)
                await process_tf_for_exchange(exchange_coinm, "COIN-M", "15m", symbols_coinm)
            elif tf == "30m":
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "30m", symbols_usdm)
                await process_tf_for_exchange(exchange_coinm, "COIN-M", "30m", symbols_coinm)
            elif tf == "1h":
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "1h", symbols_usdm)
                await process_tf_for_exchange(exchange_coinm, "COIN-M", "1h", symbols_coinm)

        # small sleep and re-evaluate
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
