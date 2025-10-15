# main.py
"""
🚀 Final Version — Bullish & Bearish Signal Detector (EMA-based)
==============================================================
Timeframes: 30m / 1h / 2h
Kriteria valid:
✅ Bullish: 1 candle hijau, EMA20 (bawah) & EMA100 (atas)
✅ Bearish: 1 candle merah, EMA20 (atas) & EMA100 (bawah)
EMA dihitung real-time hanya sampai close candle terakhir
Skip jika 1D naik/turun ≥ 5%
Scan semua Binance USDⓈ-M (perpetual) pairs
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
# 🔧 SETUP EXCHANGE BINANCE
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
    """Ambil data OHLCV"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"Error fetch {symbol}-{tf}: {e}")
        return None


def check_daily_change(df):
    """Skip jika harga 1D naik/turun ≥5%"""
    if df is None or len(df) < 2:
        return True
    change = (df.iloc[-1]["close"] - df.iloc[0]["open"]) / df.iloc[0]["open"] * 100
    return abs(change) >= 5


def ema(series, period):
    """Hitung EMA"""
    return series.ewm(span=period, adjust=False).mean()


def detect_signal(df):
    """
    Deteksi candle terakhir:
    Bullish: 1 candle hijau, EMA20 bawah, EMA100 atas
    Bearish: 1 candle merah, EMA20 atas, EMA100 bawah
    EMA dihitung real-time hanya sampai close terakhir
    """
    last = df.iloc[-1]

    # Hitung EMA saat close terakhir (real-time)
    ema20_now = ema(df["close"], 20).iloc[-1]
    ema100_now = ema(df["close"], 100).iloc[-1]

    open_price = last["open"]
    close_price = last["close"]

    # === BULLISH ===
    if close_price > open_price:
        if ema20_now > min(open_price, close_price) and ema100_now < max(open_price, close_price):
            if ema20_now < ema100_now:  # posisi valid
                return "BULLISH"

    # === BEARISH ===
    elif close_price < open_price:
        if ema20_now < max(open_price, close_price) and ema100_now > min(open_price, close_price):
            if ema20_now > ema100_now:
                return "BEARISH"

    return None


async def send_telegram(msg):
    """Kirim notif ke Telegram"""
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


# ===========================
# 🔁 SCANNER LOOP
# ===========================
async def scan_timeframe(tf, interval_minutes):
    """Scan tiap TF"""
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith("USDT")]

    while True:
        logging.info(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] 🔍 Scanning {len(symbols)} pairs — TF {tf}")

        for symbol in symbols:
            try:
                df = get_ohlcv(symbol, tf, 200)
                if df is None:
                    continue

                # Skip pair yang daily change ±5%
                df1d = get_ohlcv(symbol, "1d", 10)
                if check_daily_change(df1d):
                    continue

                signal = detect_signal(df)
                if not signal:
                    continue

                # Hindari notif duplikat
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
                    f"EMA20 bawah / EMA100 atas ✅" if signal == "BULLISH" else
                    f"EMA20 atas / EMA100 bawah ✅"
                )
                await send_telegram(msg)
                logging.info(f"✅ {signal} {symbol} {tf}")

            except Exception as e:
                logging.error(f"Error {symbol} {tf}: {e}")

        logging.info(f"⏸ Selesai scan TF {tf}. Tidur {interval_minutes} menit...\n")
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
