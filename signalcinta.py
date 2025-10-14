"""
Signal Scanner — Bullish & Bearish Detection (EMA-based)
Timeframes: 30m / 1h / 2h
Kriteria valid:
✅ 1 candle hijau: EMA20 (bawah) & EMA100 (atas)
✅ 1 candle merah: EMA20 (atas) & EMA100 (bawah)
Skip daily ±5%
Scan all Binance USDⓈ-M futures
"""

import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ===========================
# 🔧 KONFIGURASI TELEGRAM
# ===========================
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

bot = Bot(API_KEY)

# ===========================
# 🔧 SETUP EXCHANGE
# ===========================
exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "future"}  # hanya USDT perpetual
})

sent_signals = set()
logging.basicConfig(level=logging.INFO)


# ===========================
# 🔧 HELPER FUNCTIONS
# ===========================
def get_ohlcv(symbol, tf, limit=200):
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["open"] = df["open"].astype(float)
        df["close"] = df["close"].astype(float)
        return df
    except Exception as e:
        logging.error(f"Error fetching {symbol}-{tf}: {e}")
        return None


def check_daily_change(df):
    """Skip jika harga 1D naik/turun >=5%"""
    if df is None or len(df) < 2:
        return True
    change = (df.iloc[-1]["close"] - df.iloc[0]["open"]) / df.iloc[0]["open"] * 100
    return abs(change) >= 5


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def is_valid_candle(df):
    """Cek candle terakhir: 1 candle valid dgn MA20 bawah MA100 atas (bullish) atau sebaliknya (bearish)"""
    last = df.iloc[-1]
    ema20 = ema(df["close"], 20).iloc[-1]
    ema100 = ema(df["close"], 100).iloc[-1]

    open_price = last["open"]
    close_price = last["close"]

    # Bullish candle
    if close_price > open_price and (ema20 > min(open_price, close_price)) and (ema100 < max(open_price, close_price)):
        if ema20 < ema100:  # posisi valid
            return "BULLISH"
    # Bearish candle
    elif close_price < open_price and (ema20 < max(open_price, close_price)) and (ema100 > min(open_price, close_price)):
        if ema20 > ema100:
            return "BEARISH"

    return None


async def send_telegram(msg):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


# ===========================
# 🔁 SCAN LOOP
# ===========================
async def scan_timeframe(tf, interval_minutes):
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith("USDT")]

    while True:
        logging.info(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Scanning {len(symbols)} pairs — TF {tf}")

        for symbol in symbols:
            try:
                df = get_ohlcv(symbol, tf, 200)
                if df is None:
                    continue

                # Skip jika daily sudah naik/turun 5%
                df1d = get_ohlcv(symbol, "1d", 10)
                if check_daily_change(df1d):
                    continue

                signal = is_valid_candle(df)
                if not signal:
                    continue

                # Unik ID supaya gak dobel notif
                last_time = df.iloc[-1]["time"]
                signal_id = f"{symbol}-{tf}-{last_time}-{signal}"
                if signal_id in sent_signals:
                    continue

                sent_signals.add(signal_id)

                msg = (
                    f"📊 {signal} Signal Detected\n"
                    f"Pair: {symbol}\n"
                    f"Timeframe: {tf}\n"
                    f"Time: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                    f"EMA20 / EMA100 in range body ✅"
                )
                await send_telegram(msg)
                logging.info(f"Sent: {msg}")

            except Exception as e:
                logging.error(f"Error on {symbol} {tf}: {e}")

        logging.info(f"Selesai scan TF {tf}. Tidur {interval_minutes} menit...\n")
        await asyncio.sleep(interval_minutes * 60)


# ===========================
# 🚀 MAIN PROGRAM
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
