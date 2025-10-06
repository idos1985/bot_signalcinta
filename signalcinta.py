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

# Dua exchange instance
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
exchange_coinm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'delivery'}})

# Cache
sent_signals = set()
pending_1h = []   # simpan sinyal TF1H -> tunggu 4H close
pending_15m = []  # simpan sinyal TF15M -> tunggu 1H close

logging.basicConfig(level=logging.INFO)


# --- Helper Function ---
async def send_telegram(message: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


def get_ohlcv(exchange, symbol, timeframe, limit=200):
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
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    if c1["close"] < c1["open"] and c2["close"] < c2["open"]:
        if c3["close"] > c3["open"]:
            min_open = min(c1["open"], c2["open"])
            max_close = max(c1["close"], c2["close"])
            if c3["open"] < min_open and c3["close"] > max_close:
                return True, c3["open"], c3["close"]
    return False, None, None


def check_daily_drop(df):
    first = df.iloc[0]["open"]
    last = df.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5


def check_ma50_inside(open_price, close_price, df, period=50):
    df["ma50"] = df["close"].rolling(period).mean()
    last_ma50 = df.iloc[-1]["ma50"]
    return min(open_price, close_price) <= last_ma50 <= max(open_price, close_price)


def is_4h_close(now: datetime) -> bool:
    return now.minute == 0 and (now.hour % 4 == 0)


def is_1h_close(now: datetime) -> bool:
    return now.minute == 0  # tiap jam pas close


# --- Scan 1H Engulfing ---
async def scan_1h(exchange, name=""):
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith(("USDT", "USD"))]

    for symbol in symbols:
        try:
            df1d = get_ohlcv(exchange, symbol, "1d", 10)
            if df1d is None or check_daily_drop(df1d):
                continue

            df1h = get_ohlcv(exchange, symbol, "1h", 100)
            if df1h is None:
                continue

            engulf, o, c = is_bullish_engulfing(df1h)
            if not engulf:
                continue

            df4h = get_ohlcv(exchange, symbol, "4h", 100)
            if df4h is None:
                continue

            if check_ma50_inside(o, c, df4h):
                signal_id = f"1H-{name}-{symbol}-{df1h.iloc[-1]['time']}"
                if signal_id not in sent_signals:
                    sent_signals.add(signal_id)
                    pending_1h.append(
                        f"✅ [TF1H] Bullish Engulfing VALID\n"
                        f"Exchange: {name}\n"
                        f"Pair: {symbol}\n"
                        f"Open: {o}\n"
                        f"Close: {c}\n"
                        f"Time: {df1h.iloc[-1]['time']}"
                    )

        except Exception as e:
            logging.error(f"Error {symbol} (1H): {e}")


# --- Scan 15M Engulfing ---
async def scan_15m(exchange, name=""):
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith(("USDT", "USD"))]

    for symbol in symbols:
        try:
            df1d = get_ohlcv(exchange, symbol, "1d", 10)
            if df1d is None or check_daily_drop(df1d):
                continue

            df15m = get_ohlcv(exchange, symbol, "15m", 100)
            if df15m is None:
                continue

            engulf, o, c = is_bullish_engulfing(df15m)
            if not engulf:
                continue

            df1h = get_ohlcv(exchange, symbol, "1h", 100)
            if df1h is None:
                continue

            if check_ma50_inside(o, c, df1h):
                signal_id = f"15M-{name}-{symbol}-{df15m.iloc[-1]['time']}"
                if signal_id not in sent_signals:
                    sent_signals.add(signal_id)
                    pending_15m.append((
                        name, symbol, o, c, df15m.iloc[-1]['time']
                    ))

        except Exception as e:
            logging.error(f"Error {symbol} (15M): {e}")


# --- Main ---
async def main():
    global pending_1h, pending_15m
    while True:
        logging.info("Mulai scanning semua futures...")

        await scan_1h(exchange_usdm, "USDⓈ-M")
        await scan_1h(exchange_coinm, "COIN-M")

        await scan_15m(exchange_usdm, "USDⓈ-M")
        await scan_15m(exchange_coinm, "COIN-M")

        # cek apakah jam sekarang 4H close → kirim pending sinyal TF1H
        now = datetime.now().astimezone()
        if is_4h_close(now) and pending_1h:
            logging.info("Kirim pending sinyal TF1H (barengan 4H close).")
            for msg in pending_1h:
                await send_telegram(msg)
            pending_1h = []

        # cek apakah jam sekarang 1H close → kirim pending sinyal TF15M
        if is_1h_close(now) and pending_15m:
            logging.info("Kirim pending sinyal TF15M (barengan 1H close).")
            for (name, symbol, o, c, t) in pending_15m:
                msg = (
                    f"✅ [TF15M] Bullish Engulfing VALID\n"
                    f"Exchange: {name}\n"
                    f"Pair: {symbol}\n"
                    f"Open: {o}\n"
                    f"Close: {c}\n"
                    f"Time: {t}"
                )
                await send_telegram(msg)
            pending_15m = []

        logging.info("Tidur 15 menit...")
        await asyncio.sleep(15 * 60)


if __name__ == "__main__":
    asyncio.run(main())
