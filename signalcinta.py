import asyncio
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import ccxt
from telegram import Bot

# === KONFIGURASI TELEGRAM ===
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"
bot = Bot(API_KEY)

# === SETUP LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === EXCHANGE INSTANCE ===
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
exchange_coinm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'delivery'}})

# === CACHE SIGNAL ===
sent_signals = set()

# === RESAMPLE FUNCTION (Sudah aman, tanpa 'T') ===
def resample_ohlcv(df, tf_minutes):
    """Mengubah data OHLCV ke timeframe lain dengan aman (tanpa warning)."""
    try:
        rule = f"{tf_minutes}min"  # gunakan 'min' bukan 'T'
        agg = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }
        res = (
            df.resample(rule, label="right", closed="right")
            .agg(agg)
            .dropna(how="all")
        )
        return res
    except Exception as e:
        logging.error(f"Resample error: {e}")
        return df


# === TELEGRAM SEND ===
async def send_telegram(message: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        logging.error(f"Telegram error: {e}")


# === FETCH OHLCV ===
def get_ohlcv(exchange, symbol, timeframe, limit=200):
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.set_index("time", inplace=True)
        df = df.astype(float)
        return df
    except Exception as e:
        logging.error(f"Fetch error {symbol}-{timeframe}: {e}")
        return None


# === CHECK DAILY DROP ===
def check_daily_drop(df):
    """Cek apakah harga turun ≥ 5% di TF 1D."""
    first = df.iloc[0]["open"]
    last = df.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5


# === BULLISH ENGULFING DETECTOR ===
def is_bullish_engulfing(df):
    """Deteksi pola Bullish Engulfing menelan 2 candle merah sebelumnya."""
    if len(df) < 3:
        return False, None, None

    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    if c1["close"] < c1["open"] and c2["close"] < c2["open"] and c3["close"] > c3["open"]:
        min_open = min(c1["open"], c2["open"])
        max_close = max(c1["close"], c2["close"])
        if c3["open"] < min_open and c3["close"] > max_close:
            return True, c3["open"], c3["close"]

    return False, None, None


# === CHECK MA POSITION ===
def check_ma_inside(open_price, close_price, df, ma_period):
    """Cek apakah MA berada di antara open dan close candle."""
    df[f"ma{ma_period}"] = df["close"].rolling(ma_period).mean()
    last_ma = df.iloc[-1][f"ma{ma_period}"]
    return min(open_price, close_price) <= last_ma <= max(open_price, close_price)


# === MAIN SCAN PER TIMEFRAME ===
async def scan_timeframe(exchange, name, tf_main, tf_ma1, tf_ma2, tf_ma_extra, scan_interval, ma20_label, ma50_label):
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith(("USDT", "USD"))]

    logging.info(f"Mulai scan {tf_main} untuk {name}, total {len(symbols)} pair.")

    for symbol in symbols:
        try:
            # Skip jika turun 5% di TF 1D
            df1d = get_ohlcv(exchange, symbol, "1d", 10)
            if df1d is None or check_daily_drop(df1d):
                continue

            # Deteksi Engulfing di timeframe utama
            df_main = get_ohlcv(exchange, symbol, tf_main, 100)
            if df_main is None:
                continue

            engulf, o, c = is_bullish_engulfing(df_main)
            if not engulf:
                continue

            # MA validasi pertama (MA20 bawah, MA50 atas)
            df_ma1 = get_ohlcv(exchange, symbol, tf_ma1, 100)
            if df_ma1 is None:
                continue

            df_ma1["ma20"] = df_ma1["close"].rolling(20).mean()
            df_ma1["ma50"] = df_ma1["close"].rolling(50).mean()
            ma20 = df_ma1.iloc[-1]["ma20"]
            ma50 = df_ma1.iloc[-1]["ma50"]

            # Cek posisi MA20 & MA50 terhadap open/close candle
            if not (min(o, c) <= ma20 <= max(o, c) and min(o, c) <= ma50 <= max(o, c)):
                continue

            # MA validasi tambahan (MA20 dari timeframe lebih besar)
            df_extra = get_ohlcv(exchange, symbol, tf_ma_extra, 100)
            if df_extra is None:
                continue
            if not check_ma_inside(o, c, df_extra, 20):
                continue

            # Jika semua valid → kirim notif
            signal_id = f"{name}-{symbol}-{tf_main}-{df_main.index[-1]}"
            if signal_id not in sent_signals:
                sent_signals.add(signal_id)
                msg = (
                    f"✅ Bullish Engulfing VALID [{tf_main}]\n"
                    f"Exchange: {name}\n"
                    f"Pair: {symbol}\n"
                    f"Open: {o:.4f}\n"
                    f"Close: {c:.4f}\n"
                    f"MA20 ({tf_ma1}) & MA50 ({tf_ma1}) di antara range\n"
                    f"MA20 tambahan di TF {tf_ma_extra}\n"
                    f"Time: {df_main.index[-1].strftime('%Y-%m-%d %H:%M:%S UTC')}"
                )
                await send_telegram(msg)

        except Exception as e:
            logging.error(f"Error {symbol}: {e}")

    # Tunggu sampai close candle berikutnya (sinkron jam Binance)
    now_utc = datetime.now(timezone.utc)
    next_close = (now_utc + timedelta(minutes=scan_interval)).replace(second=20, microsecond=0)
    sleep_time = (next_close - now_utc).total_seconds()
    logging.info(f"Tidur {sleep_time/60:.1f} menit untuk TF {tf_main}.")
    await asyncio.sleep(sleep_time)


# === MAIN LOOP ===
async def main():
    while True:
        # Jalankan semua timeframe
        await asyncio.gather(
            scan_timeframe(exchange_usdm, "USDⓈ-M", "15m", "1h", "2h", "2h", 15, "MA20", "MA50"),
            scan_timeframe(exchange_usdm, "USDⓈ-M", "30m", "2h", "4h", "4h", 30, "MA20", "MA50"),
            scan_timeframe(exchange_usdm, "USDⓈ-M", "1h", "4h", "8h", "8h", 60, "MA20", "MA50"),
            scan_timeframe(exchange_usdm, "USDⓈ-M", "2h", "8h", "16h", "16h", 120, "MA20", "MA50"),
            scan_timeframe(exchange_coinm, "COIN-M", "15m", "1h", "2h", "2h", 15, "MA20", "MA50"),
            scan_timeframe(exchange_coinm, "COIN-M", "30m", "2h", "4h", "4h", 30, "MA20", "MA50"),
            scan_timeframe(exchange_coinm, "COIN-M", "1h", "4h", "8h", "8h", 60, "MA20", "MA50"),
            scan_timeframe(exchange_coinm, "COIN-M", "2h", "8h", "16h", "16h", 120, "MA20", "MA50"),
        )

if __name__ == "__main__":
    asyncio.run(main())
