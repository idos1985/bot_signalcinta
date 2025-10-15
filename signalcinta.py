# ============================================================
# 🚀 Simple Engulfing Detector — MA20 & MA100 (EMA)
# ============================================================
# 🔹 Scan semua pair USDT di Binance Futures (USDT-M)
# 🔹 Timeframe: 30m / 1h / 2h
# 🔹 Bullish: Engulfing hijau menembus MA20(bawah) & MA100(atas)
# 🔹 Bearish: Engulfing merah menembus MA20(atas) & MA100(bawah)
# 🔹 Scan otomatis tiap 30m, 1h, 2h
# 🔹 Tunggu 1 menit setelah candle close agar MA terbaru sudah valid
# 🔹 Hanya kirim sinyal baru (1x per candle)
# ============================================================

import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ==========================
# 🔧 KONFIGURASI TELEGRAM
# ==========================
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"
bot = Bot(token=TELEGRAM_TOKEN)

# ==========================
# 🔧 BINANCE FUTURES
# ==========================
exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "future"}  # USDT-M
})

# ==========================
# 🧠 SETUP
# ==========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
sent = {}  # {(symbol, tf): last_candle_time}


# ==========================
# 🔹 FUNGSI BANTUAN
# ==========================
def get_ohlcv(symbol, tf, limit=200):
    """Ambil data OHLCV"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        return df
    except:
        return None


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def is_bullish_engulfing(df):
    prev, last = df.iloc[-2], df.iloc[-1]
    return (
        last["close"] > last["open"] and
        prev["close"] < prev["open"] and
        last["close"] > prev["open"] and
        last["open"] < prev["close"]
    )


def is_bearish_engulfing(df):
    prev, last = df.iloc[-2], df.iloc[-1]
    return (
        last["close"] < last["open"] and
        prev["close"] > prev["open"] and
        last["open"] > prev["close"] and
        last["close"] < prev["open"]
    )


# ==========================
# 🔁 PROSES SCAN
# ==========================
async def scan(tf, interval_min):
    """Scan timeframe tertentu"""
    await asyncio.sleep(60)  # tunggu 1 menit setelah candle close

    symbols = [s for s in exchange.load_markets() if s.endswith("USDT")]

    while True:
        logging.info(f"🔍 Scan {len(symbols)} pair — TF {tf}")

        for symbol in symbols:
            try:
                df = get_ohlcv(symbol, tf, 200)
                if df is None or len(df) < 2:
                    continue

                last_time = df.iloc[-1]["time"]
                if (symbol, tf) in sent and sent[(symbol, tf)] == last_time:
                    continue  # skip candle yang sudah dikirim

                ema20 = ema(df["close"], 20)
                ema100 = ema(df["close"], 100)
                ema20_now = ema20.iloc[-1]
                ema100_now = ema100.iloc[-1]
                last = df.iloc[-1]

                # === BULLISH ENGULFING ===
                if is_bullish_engulfing(df):
                    if last["open"] > ema20_now and last["close"] > ema100_now and ema20_now < ema100_now:
                        msg = (
                            f"🟢 [BULLISH ENGULFING]\n"
                            f"Pair: {symbol}\n"
                            f"Timeframe: {tf}\n"
                            f"Time: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                            f"MA20: {ema20_now:.4f} | MA100: {ema100_now:.4f}"
                        )
                        await bot.send_message(chat_id=CHAT_ID, text=msg)
                        sent[(symbol, tf)] = last_time
                        logging.info(f"BULLISH {symbol} {tf}")

                # === BEARISH ENGULFING ===
                elif is_bearish_engulfing(df):
                    if last["open"] < ema20_now and last["close"] < ema100_now and ema20_now > ema100_now:
                        msg = (
                            f"🔴 [BEARISH ENGULFING]\n"
                            f"Pair: {symbol}\n"
                            f"Timeframe: {tf}\n"
                            f"Time: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                            f"MA20: {ema20_now:.4f} | MA100: {ema100_now:.4f}"
                        )
                        await bot.send_message(chat_id=CHAT_ID, text=msg)
                        sent[(symbol, tf)] = last_time
                        logging.info(f"BEARISH {symbol} {tf}")

            except Exception as e:
                logging.error(f"{symbol} {tf} error: {e}")

        logging.info(f"⏸ Tunggu {interval_min} menit...\n")
        await asyncio.sleep(interval_min * 60)


# ==========================
# 🚀 MAIN
# ==========================
async def main():
    tasks = [
        asyncio.create_task(scan("30m", 30)),
        asyncio.create_task(scan("1h", 60)),
        asyncio.create_task(scan("2h", 120)),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
