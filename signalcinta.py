# main.py
"""
Bullish/Bearish Engulfing Detector (EMA20/EMA100) — Multi TF
- Exchange: Binance Futures (USDT-M) via ccxt
- Timeframes: 30m, 1h, 2h
- Scan intervals: 30m, 60m, 120m
- Criteria:
  * Engulfing of 2 previous candles (Bullish/Bearish)
  * EMA20 berada di antara open & close candle terakhir (dihitung sampai candle close)
  * EMA100 berada di atas (bullish) atau di bawah (bearish) candle terakhir
- Skip pair jika 1D change >= 5%
- Mengirim notif ke Telegram
"""

import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ===========================
# CONFIG - isi dengan milikmu
# ===========================
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"
bot = Bot(API_KEY)

# ===========================
# SETUP EXCHANGE
# ===========================
exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "future"}  # USDT-M futures
})

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Cache untuk mencegah duplikat dan memastikan hanya proses candle baru
sent_signals = set()
last_candle_time = {}  # key: f"{tf}|{symbol}" -> timestamp (pd.Timestamp)

# ===========================
# HELPERS
# ===========================
def get_ohlcv_df(symbol: str, timeframe: str, limit: int = 500):
    """Fetch OHLCV and return pandas DataFrame with UTC times."""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"fetch_ohlcv error {symbol} {timeframe}: {e}")
        return None

def daily_change_skip(df1d: pd.DataFrame):
    """Return True if abs daily change >= 5% (skip). df1d should have enough bars (prefer >=2)."""
    if df1d is None or len(df1d) < 2:
        return False
    change = (df1d.iloc[-1]["close"] - df1d.iloc[0]["open"]) / df1d.iloc[0]["open"] * 100
    return abs(change) >= 5

def ema(series: pd.Series, period: int):
    """Exponential moving average (EMA)."""
    return series.ewm(span=period, adjust=False).mean()

def engulfing_two_prev(df: pd.DataFrame):
    """
    Check last candle engulfs two previous candles.
    Returns "BULLISH" / "BEARISH" / None
    """
    if df is None or len(df) < 4:
        return None

    prev2 = df.iloc[-3]  # two candles ago
    prev1 = df.iloc[-2]  # previous candle
    last  = df.iloc[-1]  # last closed candle

    # bodies for prev1 & prev2
    body_low_prev = min(prev1["open"], prev1["close"], prev2["open"], prev2["close"])
    body_high_prev = max(prev1["open"], prev1["close"], prev2["open"], prev2["close"])

    open_last = last["open"]
    close_last = last["close"]

    # Bullish engulfing: prev1 & prev2 are red, last is green and body engulfs both
    if (prev1["close"] < prev1["open"]) and (prev2["close"] < prev2["open"]):
        if (close_last > open_last) and (open_last <= body_low_prev) and (close_last >= body_high_prev):
            return "BULLISH"

    # Bearish engulfing: prev1 & prev2 are green, last is red and body engulfs both
    if (prev1["close"] > prev1["open"]) and (prev2["close"] > prev2["open"]):
        if (close_last < open_last) and (open_last >= body_high_prev) and (close_last <= body_low_prev):
            return "BEARISH"

    return None

def detect_engulfing_with_ema(df: pd.DataFrame):
    """
    Combine engulfing condition and EMA20/EMA100 rules.
    EMAs are calculated up to the last closed candle.
    Returns "BULLISH" / "BEARISH" / None
    """
    if df is None or len(df) < 110:  # ensure enough data for EMA100
        return None

    last = df.iloc[-1]
    series_close = df["close"]

    ema20 = ema(series_close, 20).iloc[-1]
    ema100 = ema(series_close, 100).iloc[-1]

    if pd.isna(ema20) or pd.isna(ema100):
        return None

    open_last = last["open"]
    close_last = last["close"]
    low_body = min(open_last, close_last)
    high_body = max(open_last, close_last)

    engulf = engulfing_two_prev(df)
    if engulf == "BULLISH":
        # EMA20 harus di antara body, EMA100 di atas candle
        if (ema20 > low_body) and (ema20 < high_body) and (ema100 > high_body):
            return "BULLISH", ema20, ema100
    elif engulf == "BEARISH":
        # EMA20 di antara body, EMA100 di bawah candle
        if (ema20 > low_body) and (ema20 < high_body) and (ema100 < low_body):
            return "BEARISH", ema20, ema100

    return None

async def send_telegram(msg: str):
    """Send message to Telegram. Uses async send_message. If your telegram lib is sync, adapt here."""
    try:
        # If your Bot.send_message is synchronous, replace with: bot.send_message(chat_id=CHAT_ID, text=msg)
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except TypeError:
        # fallback for sync send_message (older python-telegram-bot)
        try:
            bot.send_message(chat_id=CHAT_ID, text=msg)
        except Exception as e:
            logging.error(f"Telegram send error (sync fallback): {e}")
    except Exception as e:
        logging.error(f"Telegram send error: {e}")

# ===========================
# SCANNER (per TF)
# ===========================
async def scan_timeframe(tf: str, interval_minutes: int):
    logging.info(f"Scanner started for TF {tf} (every {interval_minutes} min)")
    while True:
        try:
            markets = exchange.load_markets()
            symbols = [s for s in markets.keys() if s.endswith("USDT")]
        except Exception as e:
            logging.error(f"load_markets failed: {e}")
            symbols = []

        logging.info(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] Scanning {len(symbols)} pairs — TF {tf}")

        for symbol in symbols:
            try:
                df = get_ohlcv_df(symbol, tf, limit=500)
                if df is None or len(df) < 120:
                    continue

                # skip volatile pairs based on 1D
                df1d = get_ohlcv_df(symbol, "1d", limit=10)
                if df1d is not None and daily_change_skip(df1d):
                    continue

                # only process if last candle is new (just closed)
                last_time = df.iloc[-1]["time"]
                key = f"{tf}|{symbol}"
                prev_time = last_candle_time.get(key)
                if prev_time is not None and last_time <= prev_time:
                    # no new closed candle since last run
                    continue
                # update last seen time (we will process this candle now)
                last_candle_time[key] = last_time

                # detect pattern + ema
                res = detect_engulfing_with_ema(df)
                if not res:
                    continue
                signal, ema20_val, ema100_val = res

                # create unique id to avoid duplicate notifications across runs
                signal_id = f"{symbol}|{tf}|{last_time.isoformat()}|{signal}"
                if signal_id in sent_signals:
                    continue
                sent_signals.add(signal_id)

                # prepare message (include key values)
                open_last = df.iloc[-1]["open"]
                close_last = df.iloc[-1]["close"]
                last_time_str = last_time.strftime("%Y-%m-%d %H:%M UTC")
                if signal == "BULLISH":
                    note = "Bullish Engulfing (menelan 2 merah)\nEMA20 di antara open-close\nEMA100 di atas candle"
                else:
                    note = "Bearish Engulfing (menelan 2 hijau)\nEMA20 di antara open-close\nEMA100 di bawah candle"

                msg = (
                    f"📊 [{signal}] {symbol} (TF {tf})\n"
                    f"Time: {last_time_str}\n"
                    f"Open: {open_last:.8g} | Close: {close_last:.8g}\n"
                    f"EMA20: {ema20_val:.8g} | EMA100: {ema100_val:.8g}\n"
                    f"{note}"
                )

                await send_telegram(msg)
                logging.info(f"Sent {signal} for {symbol} {tf} @ {last_time_str}")

            except Exception as e:
                logging.error(f"Error processing {symbol} {tf}: {e}")

        logging.info(f"Sleeping {interval_minutes} minutes for TF {tf}...\n")
        await asyncio.sleep(interval_minutes * 60)

# ===========================
# MAIN
# ===========================
async def main():
    tasks = [
        asyncio.create_task(scan_timeframe("30m", 30)),
        asyncio.create_task(scan_timeframe("1h", 60)),
        asyncio.create_task(scan_timeframe("2h", 120)),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
