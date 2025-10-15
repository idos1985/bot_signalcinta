import asyncio
import logging
from datetime import datetime, timezone, timedelta
import ccxt
import pandas as pd
import telegram

# =============== KONFIGURASI ===============
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"
TIMEFRAMES = ["30m", "1h", "2h"]
SCAN_INTERVAL = {"30m": 30, "1h": 60, "2h": 120}  # menit
DELAY_AFTER_CLOSE = 1  # menit
# ===========================================

bot = telegram.Bot(token=TELEGRAM_TOKEN)
exchange = ccxt.binance({'options': {'defaultType': 'future'}})

sent_signals = set()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


async def fetch_ohlcv(pair, timeframe):
    try:
        bars = exchange.fetch_ohlcv(pair, timeframe=timeframe, limit=150)
        df = pd.DataFrame(bars, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df
    except Exception as e:
        logging.error(f"Gagal fetch {pair} {timeframe}: {e}")
        return None


def calculate_ema(df, period):
    return df["close"].ewm(span=period, adjust=False).mean()


def is_bullish_engulfing(df):
    if len(df) < 3:
        return False
    last = df.iloc[-1]
    prev1 = df.iloc[-2]
    prev2 = df.iloc[-3]
    merah_dua = (prev1["close"] < prev1["open"]) and (prev2["close"] < prev2["open"])
    hijau_menelan = (last["close"] > last["open"]) and (last["close"] > prev1["open"]) and (last["open"] < prev1["close"])
    return merah_dua and hijau_menelan


def is_bearish_engulfing(df):
    if len(df) < 3:
        return False
    last = df.iloc[-1]
    prev1 = df.iloc[-2]
    prev2 = df.iloc[-3]
    hijau_dua = (prev1["close"] > prev1["open"]) and (prev2["close"] > prev2["open"])
    merah_menelan = (last["close"] < last["open"]) and (last["close"] < prev1["open"]) and (last["open"] > prev1["close"])
    return hijau_dua and merah_menelan


async def check_signal(pair, timeframe):
    df = await fetch_ohlcv(pair, timeframe)
    if df is None or len(df) < 120:
        return

    df["EMA20"] = calculate_ema(df, 20)
    df["EMA100"] = calculate_ema(df, 100)
    last = df.iloc[-1]

    now = datetime.now(timezone.utc)
    candle_close_time = last["timestamp"]

    # Pastikan candle sudah close + delay 1 menit
    if (now - candle_close_time) < timedelta(minutes=DELAY_AFTER_CLOSE):
        return

    signal_id = f"{pair}_{timeframe}_{last['timestamp']}"
    if signal_id in sent_signals:
        return

    # Bullish Engulfing → close di atas EMA20 & EMA100
    if is_bullish_engulfing(df) and last["close"] > last["EMA20"] and last["close"] > last["EMA100"]:
        msg = (
            f"🟢 [BULLISH ENGULFING]\n"
            f"Pair: {pair}\n"
            f"Timeframe: {timeframe}\n"
            f"Close: {last['timestamp']:%Y-%m-%d %H:%M UTC}\n"
            f"Price: {last['close']:.6f}\n"
            f"EMA20: {last['EMA20']:.6f} | EMA100: {last['EMA100']:.6f}"
        )
        await bot.send_message(chat_id=CHAT_ID, text=msg)
        sent_signals.add(signal_id)
        logging.info(f"✅ Bullish signal: {pair} {timeframe}")

    # Bearish Engulfing → close di bawah EMA20 & EMA100
    elif is_bearish_engulfing(df) and last["close"] < last["EMA20"] and last["close"] < last["EMA100"]:
        msg = (
            f"🔴 [BEARISH ENGULFING]\n"
            f"Pair: {pair}\n"
            f"Timeframe: {timeframe}\n"
            f"Close: {last['timestamp']:%Y-%m-%d %H:%M UTC}\n"
            f"Price: {last['close']:.6f}\n"
            f"EMA20: {last['EMA20']:.6f} | EMA100: {last['EMA100']:.6f}"
        )
        await bot.send_message(chat_id=CHAT_ID, text=msg)
        sent_signals.add(signal_id)
        logging.info(f"✅ Bearish signal: {pair} {timeframe}")


async def get_futures_pairs():
    markets = exchange.load_markets()
    return [s for s in markets if s.endswith(":USDT") and "USDT" in s]


async def scan_timeframe(timeframe, pairs):
    while True:
        logging.info(f"Mulai scan timeframe {timeframe}")
        for pair in pairs:
            try:
                await check_signal(pair, timeframe)
                await asyncio.sleep(0.8)
            except Exception as e:
                logging.error(f"Error {pair} {timeframe}: {e}")
        await asyncio.sleep(SCAN_INTERVAL[timeframe] * 60)  # tunggu sesuai TF


async def main():
    pairs = await get_futures_pairs()
    logging.info(f"Total pair futures: {len(pairs)}")

    tasks = [scan_timeframe(tf, pairs) for tf in TIMEFRAMES]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
