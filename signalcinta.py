import asyncio
import logging
from datetime import datetime
import ccxt
import pandas as pd
from telegram import Bot

# --- SETUP TELEGRAM ---
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

bot = Bot(API_KEY)

# Dua exchange instance (USDⓈ-M = future, COIN-M = delivery)
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
exchange_coinm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'delivery'}})

# Cache supaya tidak kirim notif berulang
sent_signals = set()

logging.basicConfig(level=logging.INFO)


# --- Helper Function ---
async def send_telegram(message: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


def get_ohlcv(exchange, symbol, timeframe, limit=200):
    """Ambil data OHLCV dari Binance"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["open"] = df["open"].astype(float)
        df["close"] = df["close"].astype(float)
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("Asia/Jakarta")
        return df
    except Exception as e:
        logging.error(f"OHLCV error {symbol}-{timeframe}: {e}")
        return None


def is_bullish_engulfing(df):
    """Deteksi Bullish Engulfing menelan 2 candle merah sebelumnya"""
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    # C1 & C2 merah
    if c1["close"] < c1["open"] and c2["close"] < c2["open"]:
        # C3 hijau
        if c3["close"] > c3["open"]:
            # Engulfing: body C3 lebih besar dari gabungan body C1 & C2
            min_open = min(c1["open"], c2["open"])
            max_close = max(c1["close"], c2["close"])
            if c3["open"] < min_open and c3["close"] > max_close:
                return True, c3["open"], c3["close"]
    return False, None, None


def check_daily_drop(df):
    """Cek apakah 1D turun ≥ 5%"""
    first = df.iloc[0]["open"]
    last = df.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5  # True kalau turun >= 5%


def check_ma50_inside(open_price, close_price, df4h):
    """Cek apakah MA50 TF 4H ada di range open-close engulfing"""
    df4h["ma50"] = df4h["close"].rolling(50).mean()
    last_ma50 = df4h.iloc[-1]["ma50"]
    return min(open_price, close_price) <= last_ma50 <= max(open_price, close_price)


# --- Main Scan ---
async def scan_exchange(exchange, name=""):
    markets = exchange.load_markets()
    # Ambil semua pair yang quote USDT atau USD
    symbols = [s for s in markets if s.endswith(("USDT", "USD"))]

    logging.info(f"{name}: Total {len(symbols)} pair futures ditemukan.")

    for symbol in symbols:
        try:
            # Daily filter
            df1d = get_ohlcv(exchange, symbol, "1d", 10)
            if df1d is None or check_daily_drop(df1d):
                continue

            # Check Engulfing 1H
            df1h = get_ohlcv(exchange, symbol, "1h", 100)
            if df1h is None:
                continue

            engulf, o, c = is_bullish_engulfing(df1h)
            if not engulf:
                continue

            # Validasi MA50 4H
            df4h = get_ohlcv(exchange, symbol, "4h", 100)
            if df4h is None:
                continue

            if check_ma50_inside(o, c, df4h):
                signal_id = f"{name}-{symbol}-{df1h.iloc[-1]['time']}"
                if signal_id not in sent_signals:
                    sent_signals.add(signal_id)
                    msg = (
                        f"✅ Bullish Engulfing VALID\n"
                        f"Exchange: {name}\n"
                        f"Pair: {symbol}\n"
                        f"Open: {o}\n"
                        f"Close: {c}\n"
                        f"Time: {df1h.iloc[-1]['time']}"
                    )
                    await send_telegram(msg)

        except Exception as e:
            logging.error(f"Error {symbol}: {e}")


async def main():
    while True:
        logging.info("Mulai scanning semua futures...")
        await scan_exchange(exchange_usdm, "USDⓈ-M")
        await scan_exchange(exchange_coinm, "COIN-M")
        logging.info("Scan selesai, tidur 20 menit...")
        await asyncio.sleep(20 * 60)


if __name__ == "__main__":
    asyncio.run(main())
