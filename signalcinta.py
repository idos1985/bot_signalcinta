# final_signal_ma_inside_body.py
import asyncio
import ccxt
import pandas as pd
from datetime import datetime, timezone
import aiohttp
import logging

# ============== KONFIG ==============
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

TIMEFRAMES = {
    "30m": 1800 + 60,   # 30 menit + 1 menit delay
    "1h": 3600 + 60,    # 1 jam + 1 menit delay
    "2h": 7200 + 60     # 2 jam + 1 menit delay
}

exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "future"}  # Binance Futures USDT-M
})

sent_signals = set()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# ============== TELEGRAM ==============
async def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg}
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=data)
    except Exception as e:
        logging.error(f"Telegram Error: {e}")

# ============== FUNGSI DASAR ==============
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def fetch_df(symbol, tf, limit=200):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"{symbol}-{tf} fetch error: {e}")
        return None

# ============== CEK KONDISI ==============
def check_signal(df):
    last = df.iloc[-1]
    ema20 = ema(df["close"], 20).iloc[-1]
    ema100 = ema(df["close"], 100).iloc[-1]
    open_p, close_p = last["open"], last["close"]

    # === BULLISH ===
    if close_p > open_p:
        if open_p < ema20 < close_p and open_p < ema100 < close_p:
            return "bullish", ema20, ema100, last

    # === BEARISH ===
    if close_p < open_p:
        if close_p < ema20 < open_p and close_p < ema100 < open_p:
            return "bearish", ema20, ema100, last

    return None, ema20, ema100, last

# ============== PROSES SCAN ==============
async def scan(tf):
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith("USDT") and "DOWN" not in s and "UP" not in s]
    logging.info(f"Mulai scan TF {tf} ({len(symbols)} pair)")

    last_checked = {}

    while True:
        logging.info(f"⏳ Menunggu jadwal scan TF {tf}...")
        await asyncio.sleep(60)  # pastikan 1 menit delay setelah close candle

        for symbol in symbols:
            df = fetch_df(symbol, tf, 150)
            if df is None or len(df) < 100:
                continue

            last_time = df.iloc[-1]["time"]
            if symbol in last_checked and last_checked[symbol] == last_time:
                continue
            last_checked[symbol] = last_time

            signal, ema20, ema100, last = check_signal(df)
            if signal:
                key = f"{symbol}-{tf}-{last_time}"
                if key in sent_signals:
                    continue
                sent_signals.add(key)

                msg = (
                    f"{'🟢 Bullish' if signal == 'bullish' else '🔴 Bearish'} Signal\n"
                    f"Pair: {symbol}\n"
                    f"TF: {tf}\n"
                    f"Close Candle: {last_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
                    f"Open: {last.open:.4f} | Close: {last.close:.4f}\n"
                    f"EMA20: {ema20:.4f} | EMA100: {ema100:.4f}\n"
                    f"✅ MA berada di dalam body candle"
                )
                await send_telegram(msg)
                logging.info(f"✅ Sinyal {signal.upper()} {symbol} {tf} dikirim")

        logging.info(f"⏸ Selesai scan TF {tf}. Tunggu interval berikut...\n")
        await asyncio.sleep(TIMEFRAMES[tf])

# ============== MAIN LOOP ==============
async def main():
    tasks = [asyncio.create_task(scan(tf)) for tf in TIMEFRAMES.keys()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
