# main.py
"""
Bot scanner — Bullish & Bearish Engulfing + EMA20 & EMA200 (non-repainting)
Timeframes: 30m (scan tiap 30m), 1h (tiap 1h), 2h (tiap 2h)
Scans all Binance USDⓈ-M perpetual futures pairs.
Filter:
- Bullish skip kalau 1D turun ≥5%
- Bearish skip kalau 1D naik ≥5%
"""

import asyncio
import logging
from datetime import datetime
import ccxt
import pandas as pd
from telegram import Bot

# === SETUP TELEGRAM ===
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

bot = Bot(API_KEY)
exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})

sent_signals = set()
logging.basicConfig(level=logging.INFO)


# === HELPER ===
def get_ohlcv(symbol, timeframe, limit=300):
    """Ambil data OHLCV + hitung EMA20 & EMA200"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        df["ema20"] = df["close"].ewm(span=20).mean()
        df["ema200"] = df["close"].ewm(span=200).mean()
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        return df
    except Exception as e:
        logging.error(f"OHLCV error {symbol}-{timeframe}: {e}")
        return None


def get_daily_change(symbol):
    """Hitung perubahan persen TF 1D"""
    try:
        df = get_ohlcv(symbol, "1d", 10)
        if df is None:
            return 0
        first, last = df.iloc[0]["open"], df.iloc[-1]["close"]
        change = (last - first) / first * 100
        return change
    except Exception as e:
        logging.error(f"Daily change check error {symbol}: {e}")
        return 0


# === DETEKSI POLA ===
def is_bullish_engulfing(c1, c2):
    """Bullish Engulfing"""
    return (c1["close"] < c1["open"]) and (c2["close"] > c2["open"]) and (c2["close"] > c1["open"]) and (c2["open"] < c1["close"])


def is_bearish_engulfing(c1, c2):
    """Bearish Engulfing"""
    return (c1["close"] > c1["open"]) and (c2["close"] < c2["open"]) and (c2["close"] < c1["open"]) and (c2["open"] > c1["close"])


# === CEK VALIDASI EMA ===
def is_valid_bullish(c2):
    """EMA20 bawah & EMA200 atas"""
    ema20, ema200 = c2["ema20"], c2["ema200"]
    low, high = min(c2["open"], c2["close"]), max(c2["open"], c2["close"])
    return ema20 >= low and ema200 <= high and ema20 < ema200


def is_valid_bearish(c2):
    """EMA20 atas & EMA200 bawah"""
    ema20, ema200 = c2["ema20"], c2["ema200"]
    low, high = min(c2["open"], c2["close"]), max(c2["open"], c2["close"])
    return ema20 <= high and ema200 >= low and ema20 > ema200


# === TELEGRAM ===
async def send_telegram(msg: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


# === SCAN ===
async def scan_tf(timeframe: str):
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith("/USDT")]

    for symbol in symbols:
        try:
            df = get_ohlcv(symbol, timeframe, 250)
            if df is None or len(df) < 3:
                continue

            daily_change = get_daily_change(symbol)
            c1, c2 = df.iloc[-3], df.iloc[-2]

            # === BULLISH ===
            if daily_change > -5:  # hanya skip kalau turun ≥5%
                if is_bullish_engulfing(c1, c2) and is_valid_bullish(c2):
                    signal_id = f"BULL-{symbol}-{timeframe}-{c2['time']}"
                    if signal_id not in sent_signals:
                        sent_signals.add(signal_id)
                        msg = (
                            f"🟢 **BULLISH SIGNAL {timeframe.upper()}**\n"
                            f"Pair: {symbol}\n"
                            f"Open: {c2['open']}\n"
                            f"Close: {c2['close']}\n"
                            f"EMA20: {c2['ema20']:.2f}\n"
                            f"EMA200: {c2['ema200']:.2f}\n"
                            f"Change 1D: {daily_change:.2f}%\n"
                            f"Time (close): {c2['time']}"
                        )
                        await send_telegram(msg)

            # === BEARISH ===
            if daily_change < 5:  # skip kalau naik ≥5%
                if is_bearish_engulfing(c1, c2) and is_valid_bearish(c2):
                    signal_id = f"BEAR-{symbol}-{timeframe}-{c2['time']}"
                    if signal_id not in sent_signals:
                        sent_signals.add(signal_id)
                        msg = (
                            f"🔴 **BEARISH SIGNAL {timeframe.upper()}**\n"
                            f"Pair: {symbol}\n"
                            f"Open: {c2['open']}\n"
                            f"Close: {c2['close']}\n"
                            f"EMA20: {c2['ema20']:.2f}\n"
                            f"EMA200: {c2['ema200']:.2f}\n"
                            f"Change 1D: {daily_change:.2f}%\n"
                            f"Time (close): {c2['time']}"
                        )
                        await send_telegram(msg)

        except Exception as e:
            logging.error(f"Error {symbol} ({timeframe}): {e}")


# === LOOP UTAMA ===
async def main():
    while True:
        now = datetime.utcnow()
        minute = now.minute
        hour = now.hour

        tasks = []

        if minute % 30 == 0:  # tiap 30 menit
            tasks.append(scan_tf("30m"))
        if minute == 0:  # tiap 1 jam
            tasks.append(scan_tf("1h"))
        if minute == 0 and hour % 2 == 0:  # tiap 2 jam
            tasks.append(scan_tf("2h"))

        if tasks:
            logging.info(f"Mulai scanning timeframe aktif...")
            await asyncio.gather(*tasks)

        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
