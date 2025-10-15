# ============================================================
# 🚀 Engulfing Detector (Sinkron dengan Waktu Candle Close)
# ============================================================
# 🔹 Scan semua pair USDT di Binance Futures (USDT-M)
# 🔹 Timeframe: 30m / 1h / 2h
# 🔹 Bullish Engulfing: menembus MA20(bawah) & MA100(atas)
# 🔹 Bearish Engulfing: menembus MA20(atas) & MA100(bawah)
# 🔹 Kirim sinyal hanya saat candle baru benar-benar close
# ============================================================

import asyncio
import logging
from datetime import datetime, timezone, timedelta
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
    "options": {"defaultType": "future"}
})

# ==========================
# 🧠 SETUP
# ==========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
sent = {}  # {(symbol, tf): last_candle_time}


# ==========================
# 🔹 UTILITAS
# ==========================
def get_ohlcv(symbol, tf, limit=200):
    """Ambil data OHLCV"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"{symbol}-{tf} error fetch: {e}")
        return None


def ema(series, period):
    """Hitung EMA"""
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
# 🔁 SCANNER
# ==========================
async def scan_timeframe(tf, interval_minutes):
    """Scan timeframe tertentu sinkron dengan jam close candle"""
    symbols = [s for s in exchange.load_markets() if s.endswith("USDT")]
    logging.info(f"✅ Mulai scanner TF {tf} ({len(symbols)} pair)")

    # Loop tanpa henti
    while True:
        now = datetime.now(timezone.utc)

        # Hitung waktu candle berikutnya akan close
        minute = now.minute
        if tf == "30m":
            next_close_minute = 30 if minute < 30 else 60
            next_close = now.replace(minute=next_close_minute % 60, second=0, microsecond=0)
            if next_close_minute == 60:
                next_close += timedelta(hours=1)
        elif tf == "1h":
            next_close = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif tf == "2h":
            next_close_hour = (now.hour // 2 + 1) * 2
            next_close = now.replace(hour=next_close_hour % 24, minute=0, second=0, microsecond=0)
            if next_close_hour >= 24:
                next_close += timedelta(days=1)
        else:
            next_close = now + timedelta(minutes=interval_minutes)

        # Tunggu sampai candle benar-benar close + 1 menit (biar MA valid)
        wait_seconds = (next_close - now).total_seconds() + 60
        logging.info(f"⏳ {tf}: tunggu {int(wait_seconds/60)} menit sampai candle close...")
        await asyncio.sleep(wait_seconds)

        # Saatnya scan
        logging.info(f"🕒 {tf}: mulai scan candle baru...")
        for symbol in symbols:
            try:
                df = get_ohlcv(symbol, tf, 200)
                if df is None or len(df) < 2:
                    continue

                last_time = df.iloc[-1]["time"]
                if (symbol, tf) in sent and sent[(symbol, tf)] == last_time:
                    continue  # sudah dikirim untuk candle ini

                ema20 = ema(df["close"], 20).iloc[-1]
                ema100 = ema(df["close"], 100).iloc[-1]
                last = df.iloc[-1]

                # === BULLISH ENGULFING ===
                if is_bullish_engulfing(df):
                    if last["open"] > ema20 and last["close"] > ema100 and ema20 < ema100:
                        msg = (
                            f"🟢 [BULLISH ENGULFING]\n"
                            f"Pair: {symbol}\n"
                            f"Timeframe: {tf}\n"
                            f"Close: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                            f"EMA20: {ema20:.4f} | EMA100: {ema100:.4f}"
                        )
                        await bot.send_message(chat_id=CHAT_ID, text=msg)
                        sent[(symbol, tf)] = last_time
                        logging.info(f"BULLISH {symbol} {tf}")

                # === BEARISH ENGULFING ===
                elif is_bearish_engulfing(df):
                    if last["open"] < ema20 and last["close"] < ema100 and ema20 > ema100:
                        msg = (
                            f"🔴 [BEARISH ENGULFING]\n"
                            f"Pair: {symbol}\n"
                            f"Timeframe: {tf}\n"
                            f"Close: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                            f"EMA20: {ema20:.4f} | EMA100: {ema100:.4f}"
                        )
                        await bot.send_message(chat_id=CHAT_ID, text=msg)
                        sent[(symbol, tf)] = last_time
                        logging.info(f"BEARISH {symbol} {tf}")

            except Exception as e:
                logging.error(f"{symbol} {tf} error: {e}")

        logging.info(f"✅ TF {tf} selesai scan.\n")


# ==========================
# 🚀 MAIN
# ==========================
async def main():
    tasks = [
        asyncio.create_task(scan_timeframe("30m", 30)),
        asyncio.create_task(scan_timeframe("1h", 60)),
        asyncio.create_task(scan_timeframe("2h", 120)),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
