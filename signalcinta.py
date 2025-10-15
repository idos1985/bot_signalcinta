"""
🚀 FINAL VERSION — Bullish & Bearish Engulfing (EMA-based)
===========================================================
Timeframes: 30m / 1h / 2h
Kriteria valid:
✅ Bullish Engulfing: 1 candle hijau terakhir menelan 2 candle merah sebelumnya
   - EMA20 di bawah candle terakhir
   - EMA100 di atas candle terakhir
✅ Bearish Engulfing: 1 candle merah terakhir menelan 2 candle hijau sebelumnya
   - EMA20 di atas candle terakhir
   - EMA100 di bawah candle terakhir
EMA dihitung real-time sampai close candle terakhir
Scan semua pair Binance Futures (USDT-M)
Scan tiap 30m, 1h, dan 2h (dengan delay 1 menit setelah candle close)
"""

import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ===========================
# 🔧 KONFIG TELEGRAM
# ===========================
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"
bot = Bot(API_KEY)

# ===========================
# 🔧 SETUP BINANCE FUTURES
# ===========================
exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "future"}  # hanya USDT perpetual
})

# ===========================
# ⚙️ VARIABEL GLOBAL
# ===========================
sent_signals = set()
last_candle_time = {}
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ===========================
# 📊 HELPER FUNCTIONS
# ===========================
def get_ohlcv_df(symbol, tf, limit=200):
    """Ambil data OHLCV"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"Fetch error {symbol}-{tf}: {e}")
        return None


def ema(series, period):
    """Hitung EMA"""
    return series.ewm(span=period, adjust=False).mean()


def daily_change_skip(df):
    """Skip jika 1D naik/turun ≥5%"""
    if df is None or len(df) < 2:
        return False
    change = (df.iloc[-1]["close"] - df.iloc[0]["open"]) / df.iloc[0]["open"] * 100
    return abs(change) >= 5


def detect_engulfing_with_ema(df):
    """Deteksi Bullish/Bearish Engulfing yang valid terhadap EMA20 & EMA100"""
    if len(df) < 5:
        return None

    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    ema20_now = ema(df["close"], 20).iloc[-1]
    ema100_now = ema(df["close"], 100).iloc[-1]

    # === BULLISH ENGULFING ===
    if c3["close"] > c3["open"] and c1["close"] < c1["open"] and c2["close"] < c2["open"]:
        if c3["close"] > c1["open"] and c3["open"] < c1["close"]:
            # Validasi posisi EMA
            if ema20_now > c3["low"] and ema20_now < c3["high"] and ema100_now > c3["close"]:
                return "BULLISH", ema20_now, ema100_now

    # === BEARISH ENGULFING ===
    if c3["close"] < c3["open"] and c1["close"] > c1["open"] and c2["close"] > c2["open"]:
        if c3["open"] > c1["close"] and c3["close"] < c1["open"]:
            if ema20_now < c3["high"] and ema20_now > c3["low"] and ema100_now < c3["close"]:
                return "BEARISH", ema20_now, ema100_now

    return None


async def send_telegram(msg):
    """Kirim pesan ke Telegram"""
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


# ===========================
# 🔁 SCANNER LOOP
# ===========================
async def scan_timeframe(tf, interval_minutes):
    """Scan setiap timeframe"""
    logging.info(f"✅ Mulai scanner TF {tf} setiap {interval_minutes}+1 menit")

    while True:
        try:
            markets = exchange.load_markets()
            symbols = [s for s in markets if s.endswith("USDT")]
        except Exception as e:
            logging.error(f"Gagal load markets: {e}")
            symbols = []

        logging.info(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] 🔍 Scan {len(symbols)} pair — TF {tf}")

        for symbol in symbols:
            try:
                df = get_ohlcv_df(symbol, tf, limit=200)
                if df is None or len(df) < 120:
                    continue

                # Skip pair dengan perubahan 1D ekstrem
                df1d = get_ohlcv_df(symbol, "1d", limit=10)
                if df1d is not None and daily_change_skip(df1d):
                    continue

                last_time = df.iloc[-1]["time"]
                key = f"{tf}|{symbol}"
                prev_time = last_candle_time.get(key)

                # Hanya proses candle baru
                if prev_time is not None and last_time <= prev_time:
                    continue
                last_candle_time[key] = last_time

                result = detect_engulfing_with_ema(df)
                if not result:
                    continue

                signal, ema20_val, ema100_val = result
                signal_id = f"{symbol}|{tf}|{last_time.isoformat()}|{signal}"
                if signal_id in sent_signals:
                    continue
                sent_signals.add(signal_id)

                msg = (
                    f"📊 [{signal}] Engulfing Signal\n"
                    f"Pair: {symbol}\n"
                    f"Timeframe: {tf}\n"
                    f"Time: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                    f"EMA20: {ema20_val:.8f}\n"
                    f"EMA100: {ema100_val:.8f}\n"
                )
                await send_telegram(msg)
                logging.info(f"✅ {signal} {symbol} {tf} @ {last_time}")

            except Exception as e:
                logging.error(f"Error {symbol} {tf}: {e}")

        # Delay 1 menit agar candle benar-benar close sempurna
        logging.info(f"⏸ Selesai scan TF {tf}. Tidur {interval_minutes}+1 menit...\n")
        await asyncio.sleep(interval_minutes * 60 + 60)


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
