import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt
from telegram import Bot

# ===== KONFIGURASI TELEGRAM =====
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

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
def add_ma(df, period=50):
    df[f"MA{period}"] = df["close"].rolling(period).mean()
    return df

# ===== DETEKSI BULLISH ENGULFING =====
def detect_bullish_engulfing(df1h, df4h):
    last3 = df1h.iloc[-3:]  # ambil 3 candle terakhir
    c1, c2, c3 = last3.iloc[0], last3.iloc[1], last3.iloc[2]

    # dua candle merah
    cond_red = c1["close"] < c1["open"] and c2["close"] < c2["open"]
    # candle hijau terakhir
    cond_green = c3["close"] > c3["open"]
    # engulf menelan body 2 candle merah
    cond_engulf = (c3["close"] > max(c1["open"], c2["open"])) and (c3["open"] < min(c1["close"], c2["close"]))

    if not (cond_red and cond_green and cond_engulf):
        return None

    # MA50 tf 4h
    last4h = df4h.iloc[-1]
    ma50_4h = last4h["MA50"]

    # cek apakah MA50 tf4h berada di range open-close candle engulfing
    low_val, high_val = sorted([c3["open"], c3["close"]])
    if not (low_val <= ma50_4h <= high_val):
        return None

    return {
        "time": str(c3["time"]),
        "open": c3["open"],
        "close": c3["close"],
        "ma50_4h": ma50_4h,
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
        df4h = fetch_ohlcv(symbol, "4h", 200)
        df1d = fetch_ohlcv(symbol, "1d", 10)

        if df1h is None or df4h is None or df1d is None:
            continue

        df4h = add_ma(df4h, 50)  # pakai MA50 di 4h

        # hindari koin yang turun >5% di daily
        d_last = df1d.iloc[-1]
        if (d_last["close"] - d_last["open"]) / d_last["open"] <= -0.05:
            continue

        result = detect_bullish_engulfing(df1h, df4h)
        if result:
            msg = (
                f"✅ *Bullish Engulfing VALID*\n\n"
                f"Pair: `{symbol}`\n"
                f"🕒 {result['time']}\n"
                f"Open: {result['open']}\n"
                f"Close: {result['close']}\n"
                f"MA50 (4h): {result['ma50_4h']:.4f}"
            )
            await send_telegram(msg)

# ===== MAIN LOOP =====
async def main_loop():
    while True:
        now = datetime.now(timezone.utc)
        if now.minute % 20 == 0:  # tiap 20 menit
            await scan()
        await asyncio.sleep(60)
