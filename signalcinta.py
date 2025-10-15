# main.py
"""
Bullish/Bearish Engulfing Detector (EMA20/EMA100) — Multi TF
Delay 1 menit agar candle & EMA sudah terbentuk sempurna.

- Exchange: Binance Futures (USDT-M)
- Timeframes: 30m, 1h, 2h
- Scan intervals: 31m, 61m, 121m (delay +1m)
- EMA20 / EMA100: exponential, dihitung sampai candle terakhir (baru close)
- Engulfing: candle terakhir menelan 2 candle sebelumnya
- Skip jika daily change >= 5%
- Kirim notifikasi ke Telegram
"""

import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ===========================
# 🔧 CONFIG TELEGRAM
# ===========================
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"
bot = Bot(API_KEY)

# ===========================
# 🔧 BINANCE FUTURES SETUP
# ===========================
exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "future"}  # USDT-M futures
})

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

sent_signals = set()
last_candle_time = {}

# ===========================
# 🔧 HELPER FUNCTIONS
# ===========================
def get_ohlcv_df(symbol: str, timeframe: str, limit: int = 500):
    """Fetch OHLCV and return pandas DataFrame."""
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
    """Return True if abs daily change >= 5% (skip pair)."""
    if df1d is None or len(df1d) < 2:
        return False
    change = (df1d.iloc[-1]["close"] - df1d.iloc[0]["open"]) / df1d.iloc[0]["open"] * 100
    return abs(change) >= 5


def ema(series: pd.Series, period: int):
    """Exponential moving average (EMA)."""
    return series.ewm(span=period, adjust=False).mean()


def engulfing_two_prev(df: pd.DataFrame):
    """Deteksi engulfing yang menelan 2 candle sebelumnya."""
    if df is None or len(df) < 4:
        return None

    prev2 = df.iloc[-3]
    prev1 = df.iloc[-2]
    last  = df.iloc[-1]

    body_low_prev = min(prev1["open"], prev1["close"], prev2["open"], prev2["close"])
    body_high_prev = max(prev1["open"], prev1["close"], prev2["open"], prev2["close"])

    open_last = last["open"]
    close_last = last["close"]

    # Bullish engulfing
    if (prev1["close"] < prev1["open"]) and (prev2["close"] < prev2["open"]):
        if (close_last > open_last) and (open_last <= body_low_prev) and (close_last >= body_high_prev):
            return "BULLISH"

    # Bearish engulfing
    if (prev1["close"] > prev1["open"]) and (prev2["close"] > prev2["open"]):
        if (close_last < open_last) and (open_last >= body_high_prev) and (close_last <= body_low_prev):
            return "BEARISH"

    return None


def detect_engulfing_with_ema(df: pd.DataFrame):
    """Deteksi pola engulfing + validasi EMA20 & EMA100."""
    if df is None or len(df) < 110:
        return None

    last = df.iloc[-1]
    ema20 = ema(df["close"], 20).iloc[-1]
    ema100 = ema(df["close"], 100).iloc[-1]

    if pd.isna(ema20) or pd.isna(ema100):
        return None

    open_last = last["open"]
    close_last = last["close"]
    low_body = min(open_last, close_last)
    high_body = max(open_last, close_last)

    engulf = engulfing_two_prev(df)
    if engulf == "BULLISH":
        if (ema20 > low_body) and (ema20 < high_body) and (ema100 > high_body):
            return "BULLISH", ema20, ema100
    elif engulf == "BEARISH":
        if (ema20 > low_body) and (ema20 < high_body) and (ema100 < low_body):
            return "BEARISH", ema20, ema100

    return None


async def send_telegram(msg: str):
    """Kirim pesan ke Telegram."""
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except TypeError:
        try:
            bot.send_message(chat_id=CHAT_ID, text=msg)
        except Exception as e:
            logging.error(f"Telegram send error (sync fallback): {e}")
    except Exception as e:
        logging.error(f"Telegram send error: {e}")

# ===========================
# 🔁 SCANNER LOOP
# ===========================
async def scan_timeframe(tf: str, interval_minutes: int):
    logging.info(f"Scanner started for TF {tf} (every {interval_minutes}+1 min)")
    while True:
        try:
            markets = exchange.load_markets()
            symbols = [s for s in markets.keys() if s.endswith("USDT")]
        except Exception as e:
            logging.error(f"load_markets failed: {e}")
            symbols = []

        logging.info(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] 🔍 Scanning {len(symbols)} pairs — TF {tf}")

        for symbol in symbols:
            try:
                df = get_ohlcv_df(symbol, tf, limit=500)
                if df is None or len(df) < 120:
                    continue

                # Skip jika volatilitas 1D terlalu tinggi
                df1d = get_ohlcv_df(symbol, "1d", limit=10)
                if df1d is not None and daily_change_skip(df1d):
                    continue

                last_time = df.iloc[-1]["time"]
                key = f"{tf}|{symbol}"
                prev_time = last_candle_time.get(key)

                # hanya proses jika candle baru close
                if prev_time is not None and last_time <= prev_time:
                    continue
                last_candle_time[key] = last_time

                res = detect_engulfing_with_ema(df)
                if not res:
                    continue
                signal, ema20_val, ema100_val = res

                signal_id = f"{symbol}|{tf}|{last_time.isoformat()}|{signal}"
                if signal_id in sent_signals:
                    continue
                sent_signals.add(signal_id)

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
                logging.info(f"✅ Sent {signal} for {symbol} {tf} @ {last_time_str}")

            except Exception as e:
                logging.error(f"Error processing {symbol} {tf}: {e}")

        # 🕒 Tambah delay 1 menit agar candle close benar-benar final
        logging.info(f"⏸ Selesai scan TF {tf}. Tidur {interval_minutes}+1 menit...\n")
        await asyncio.sleep(interval_minutes * 60 + 60)


# ===========================
# 🚀 MAIN
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
