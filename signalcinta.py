# signalcinta.py
"""
Binance Futures Scanner (USDⓈ-M)
Bullish + Bearish Detection
----------------------------------
• Bullish valid: Candle hijau, EMA20 bawah & EMA100 atas
• Bearish valid: Candle merah, EMA20 atas & EMA100 bawah
• Bearish skip jika naik >5% di 1D
----------------------------------
TF Scan:
- 30m : tiap 30 menit
- 1h  : tiap 1 jam
- 2h  : tiap 2 jam
----------------------------------
Telegram notif otomatis
"""

import asyncio
import aiohttp
import pandas as pd
import logging
from datetime import datetime, timezone
import math
import os

# === KONFIGURASI ===
BINANCE_FUTURES_URL = "https://fapi.binance.com/fapi/v1"
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
TELEGRAM_CHAT_ID = "7183177114"

# logging Railway
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

sent_signals = set()  # untuk mencegah sinyal berulang


# === UTILITAS ===
async def fetch_json(session, url, params=None):
    for _ in range(3):
        try:
            async with session.get(url, params=params, timeout=10) as r:
                if r.status == 200:
                    return await r.json()
        except Exception as e:
            logging.warning(f"Retry fetch {url}: {e}")
        await asyncio.sleep(1)
    return None


def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def is_bullish(row):
    return row["close"] > row["open"]


def is_bearish(row):
    return row["close"] < row["open"]


# === TELEGRAM ===
async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
    async with aiohttp.ClientSession() as session:
        try:
            await session.post(url, data=payload)
        except Exception as e:
            logging.error(f"Telegram error: {e}")


# === GET SEMUA PAIR ===
async def get_all_usdt_pairs():
    async with aiohttp.ClientSession() as session:
        data = await fetch_json(session, f"{BINANCE_FUTURES_URL}/exchangeInfo")
        if not data:
            return []
        return [s["symbol"] for s in data["symbols"] if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT"]


# === GET DATA KLINE ===
async def get_klines(symbol, interval, limit=200):
    async with aiohttp.ClientSession() as session:
        data = await fetch_json(session, f"{BINANCE_FUTURES_URL}/klines",
                                params={"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            return None

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close", "volume",
        "_", "__", "___", "____", "_____", "______"
    ], dtype=float)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    return df[["time", "open", "high", "low", "close"]]


# === SCAN TF ===
async def scan_tf(tf):
    pairs = await get_all_usdt_pairs()
    logging.info(f"Scanning timeframe {tf} — total {len(pairs)} pairs")

    tasks = [process_symbol(symbol, tf) for symbol in pairs]
    await asyncio.gather(*tasks)


# === PROSES PER SYMBOL ===
async def process_symbol(symbol, tf):
    try:
        df = await get_klines(symbol, tf)
        if df is None or len(df) < 100:
            return

        df["ema20"] = calc_ema(df["close"], 20)
        df["ema100"] = calc_ema(df["close"], 100)

        c = df.iloc[-1]  # candle terbaru
        if math.isnan(c["ema20"]) or math.isnan(c["ema100"]):
            return

        # === BULLISH DETECTION ===
        if is_bullish(c) and (c["ema20"] > c["open"]) and (c["ema20"] < c["close"]) and (c["ema100"] > c["open"]) and (c["ema100"] < c["close"]) and (c["ema20"] < c["ema100"]):
            key = f"{symbol}_{tf}_bullish"
            if key not in sent_signals:
                sent_signals.add(key)
                msg = (
                    f"🟢 <b>BULLISH SIGNAL DETECTED</b>\n"
                    f"Symbol: <b>{symbol}</b>\n"
                    f"Timeframe: <b>{tf}</b>\n"
                    f"Open: {c['open']:.4f} | Close: {c['close']:.4f}\n"
                    f"EMA20: {c['ema20']:.4f} | EMA100: {c['ema100']:.4f}"
                )
                await send_telegram(msg)
                logging.info(f"{symbol} bullish signal sent ({tf})")

        # === BEARISH DETECTION ===
        elif is_bearish(c) and (c["ema20"] < c["open"]) and (c["ema20"] > c["close"]) and (c["ema100"] < c["open"]) and (c["ema100"] > c["close"]) and (c["ema20"] > c["ema100"]):
            # Cek 1D change > +5%
            df1d = await get_klines(symbol, "1d", limit=2)
            if df1d is not None and len(df1d) >= 2:
                pchg = (df1d.iloc[-1]["close"] - df1d.iloc[-2]["close"]) / df1d.iloc[-2]["close"] * 100
                if pchg > 5:
                    logging.info(f"{symbol} skipped bearish (daily +{pchg:.2f}%)")
                    return

            key = f"{symbol}_{tf}_bearish"
            if key not in sent_signals:
                sent_signals.add(key)
                msg = (
                    f"🔴 <b>BEARISH SIGNAL DETECTED</b>\n"
                    f"Symbol: <b>{symbol}</b>\n"
                    f"Timeframe: <b>{tf}</b>\n"
                    f"Open: {c['open']:.4f} | Close: {c['close']:.4f}\n"
                    f"EMA20: {c['ema20']:.4f} | EMA100: {c['ema100']:.4f}"
                )
                await send_telegram(msg)
                logging.info(f"{symbol} bearish signal sent ({tf})")

    except Exception as e:
        logging.warning(f"Error {symbol} {tf}: {e}")


# === LOOP UTAMA ===
async def main():
    while True:
        now = datetime.now(timezone.utc)
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
            logging.info("⏳ Mulai scanning timeframe aktif...")
            await asyncio.gather(*tasks)

        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
