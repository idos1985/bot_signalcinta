import ccxt
import pandas as pd
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

# === ISI MANUAL ===
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

# === KONFIGURASI ===
TIMEFRAMES = {
    "30m": 31 * 60,   # scan tiap 31 menit
    "1h": 61 * 60,    # scan tiap 61 menit
    "2h": 121 * 60    # scan tiap 121 menit
}

exchange = ccxt.binance({
    'options': {'defaultType': 'future'}
})

# === Fungsi kirim pesan ke Telegram ===
async def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    async with aiohttp.ClientSession() as session:
        await session.post(url, data=payload)

# === Ambil data candle & hitung EMA ===
def get_ema_signals(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=110)
        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        df['time'] = pd.to_datetime(df['timestamp'], unit='ms')

        # EMA20 & EMA100
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema100'] = df['close'].ewm(span=100, adjust=False).mean()

        last = df.iloc[-1]
        open_ = last['open']
        close_ = last['close']
        ema20 = last['ema20']
        ema100 = last['ema100']

        # === Bullish ===
        if close_ > open_ and ema20 > min(open_, close_) and ema20 < max(open_, close_) \
           and ema100 > min(open_, close_) and ema100 < max(open_, close_) \
           and ema20 < ema100:
            return f"📈 BULLISH: {symbol} ({timeframe})\nCandle menelan EMA20 & EMA100\nHarga: {close_:.4f}"

        # === Bearish ===
        if close_ < open_ and ema20 > min(open_, close_) and ema20 < max(open_, close_) \
           and ema100 > min(open_, close_) and ema100 < max(open_, close_) \
           and ema20 > ema100:
            return f"📉 BEARISH: {symbol} ({timeframe})\nCandle menelan EMA20 & EMA100\nHarga: {close_:.4f}"

    except Exception as e:
        print(f"Error {symbol} {timeframe}: {e}")
    return None

# === Fungsi utama untuk scan semua pair ===
async def scan_all_pairs():
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith('/USDT')]
    print(f"Memindai {len(symbols)} pair USDT di Binance Futures...")

    while True:
        for tf, delay in TIMEFRAMES.items():
            print(f"\n[SCAN] Timeframe: {tf} - {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
            for symbol in symbols:
                signal = get_ema_signals(symbol, tf)
                if signal:
                    print(signal)
                    await send_telegram_message(signal)

            # delay sesuai timeframe (sudah termasuk 1 menit tunggu candle close)
            await asyncio.sleep(delay)

# === Jalankan ===
async def main():
    print("Bot mulai jalan... Tunggu sinyal terbentuk.")
    await scan_all_pairs()

if __name__ == "__main__":
    asyncio.run(main())
