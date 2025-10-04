import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ===== KONFIGURASI TELEGRAM =====
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"   # isi token bot
CHAT_ID = "7183177114"          # isi chat id

bot = Bot(token=TELEGRAM_TOKEN)

logging.basicConfig(level=logging.INFO)

# ===== FUNGSI AMBIL DATA OHLCV =====
def fetch_ohlcv(symbol, timeframe="1h", limit=150):
    try:
        exchange = ccxt.binance()
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(
            ohlcv, columns=["time","open","high","low","close","volume"]
        )
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df = df.astype({"open":float,"high":float,"low":float,"close":float})
        return df
    except Exception as e:
        logging.error(f"Gagal fetch {symbol} {timeframe}: {e}")
        return None

# ===== HITUNG MA =====
def add_ma(df, period=100):
    df[f"MA{period}"] = df["close"].rolling(period).mean()
    return df

# ===== DETEKSI BULLISH ENGULFING VALID =====
def detect_bullish_engulfing(df1h, df2h):
    last3 = df1h.iloc[-3:]  # ambil 3 candle terakhir
    c1, c2, c3 = last3.iloc[0], last3.iloc[1], last3.iloc[2]

    # syarat: dua candle merah lalu engulfing bullish
    cond_red = c1["close"] < c1["open"] and c2["close"] < c2["open"]
    cond_green = c3["close"] > c3["open"]
    cond_engulf = c3["close"] > c1["open"] and c3["open"] < c1["close"]

    if not (cond_red and cond_green and cond_engulf):
        return None

    # ambil MA100 tf 1h
    ma100_1h = c3["MA100"]

    # cek apakah MA100 tf1h ada di antara open & close candle engulfing
    low_val, high_val = sorted([c3["open"], c3["close"]])
    if not (low_val <= ma100_1h <= high_val):
        return None

    # cek apakah MA100 tf2h juga ada di range itu
    last2h = df2h.iloc[-1]
    ma100_2h = last2h["MA100"]
    if not (low_val <= ma100_2h <= high_val):
        return None

    return {
        "time": str(c3["time"]),
        "open": c3["open"],
        "close": c3["close"],
        "ma100_1h": ma100_1h,
        "ma100_2h": ma100_2h,
    }

# ===== TELEGRAM =====
async def send_telegram(text: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Telegram error: {e}")

# ===== SCAN =====
async def scan():
    logging.info("=== Mulai Scan Bullish Engulfing ===")
    exchange = ccxt.binance()
    markets = exchange.load_markets()

    symbols = [
        s for s in markets if (s.endswith("/USDT") and (":USDT" not in s))
    ]

    for symbol in symbols:
        df1h = fetch_ohlcv(symbol, "1h", 200)
        df2h = fetch_ohlcv(symbol, "2h", 200)
        df1d = fetch_ohlcv(symbol, "1d", 10)

        if df1h is None or df2h is None or df1d is None:
            continue

        df1h = add_ma(df1h, 100)
        df2h = add_ma(df2h, 100)

        # hindari koin yang turun >5% di daily
        d_last = df1d.iloc[-1]
        if (d_last["close"] - d_last["open"]) / d_last["open"] <= -0.05:
            continue

        result = detect_bullish_engulfing(df1h, df2h)
        if result:
            msg = (
                f"✅ *Bullish Engulfing VALID*\n\n"
                f"Pair: `{symbol}`\n"
                f"🕒 {result['time']}\n"
                f"Open: {result['open']}\n"
                f"Close: {result['close']}\n"
                f"MA100 (1h): {result['ma100_1h']:.4f}\n"
                f"MA100 (2h): {result['ma100_2h']:.4f}"
            )
            await send_telegram(msg)

# ===== MAIN LOOP =====
async def main_loop():
    while True:
        now = datetime.now(timezone.utc)
        if now.minute % 30 == 0:  # tiap 30 menit
            await scan()
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main_loop())
