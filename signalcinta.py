import asyncio
import logging
from datetime import datetime, timezone, timedelta
import math
import ccxt
import pandas as pd
from telegram import Bot

# ---------------- CONFIG ----------------
API_KEY = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

# Exchanges: USDⓈ-M (future) and COIN-M (delivery)
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
exchange_coinm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'delivery'}})

bot = Bot(API_KEY)

# state
sent_signals = set()   # avoid duplicate signals: ids like "15m-USDⓈ-M-BTCUSDT-<ts>"
last_processed = { "15m": None, "30m": None, "1h": None, "2h": None }

# Logging (railway-friendly)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# small delay between fetches to reduce pressure
RATE_DELAY = 0.12  # seconds

# ---------------- Helpers ----------------
def now_utc():
    return datetime.now(timezone.utc)

async def safe_sleep(seconds: float):
    # small wrapper to use asyncio.sleep
    await asyncio.sleep(seconds)

async def send_telegram(msg: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")

def get_ohlcv(exchange, symbol, timeframe, limit=200):
    """Fetch OHLCV and return DataFrame with tz-aware UTC 'time' column or None."""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["open"] = df["open"].astype(float)
        df["close"] = df["close"].astype(float)
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        return df
    except Exception as e:
        logging.debug(f"OHLCV error {symbol} {timeframe}: {e}")
        return None

def is_bullish_engulfing(df):
    """Return (True, open, close, close_time) if last candle is bullish engulfing of previous 2 red candles."""
    try:
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    except Exception:
        return False, None, None, None
    if (c1["close"] < c1["open"]) and (c2["close"] < c2["open"]) and (c3["close"] > c3["open"]):
        min_open = min(c1["open"], c2["open"])
        max_close = max(c1["close"], c2["close"])
        if (c3["open"] < min_open) and (c3["close"] > max_close):
            return True, float(c3["open"]), float(c3["close"]), c3["time"]
    return False, None, None, None

def check_daily_drop(df1d):
    """Return True if daily drop >=5% (from first open in df to last close)."""
    if df1d is None or len(df1d) < 2:
        return False
    first = df1d.iloc[0]["open"]
    last = df1d.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5

def ma_in_range_and_order(open_price, close_price, df_higher, ma20_period=20, ma50_period=50):
    """
    Check that MA20 and MA50 exists on df_higher, both within [min(open,close), max(open,close)]
    and MA20 < MA50 (MA20 bawah, MA50 atas).
    Return (True, ma20_val, ma50_val) or (False, None, None).
    """
    if df_higher is None:
        return False, None, None
    needed = max(ma20_period, ma50_period)
    if len(df_higher) < needed:
        return False, None, None
    df = df_higher.copy()
    df[f"ma{ma20_period}"] = df["close"].rolling(ma20_period).mean()
    df[f"ma{ma50_period}"] = df["close"].rolling(ma50_period).mean()
    ma20 = df.iloc[-1][f"ma{ma20_period}"]
    ma50 = df.iloc[-1][f"ma{ma50_period}"]
    if pd.isna(ma20) or pd.isna(ma50):
        return False, None, None
    lo = min(open_price, close_price)
    hi = max(open_price, close_price)
    if (lo <= ma20 <= hi) and (lo <= ma50 <= hi) and (ma20 < ma50):
        return True, float(ma20), float(ma50)
    return False, None, None

def floor_to_interval(dt: datetime, minutes_interval: int) -> datetime:
    """Return datetime floored to nearest interval in minutes (UTC tz-aware)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    total_minutes = dt.hour * 60 + dt.minute
    floored = (total_minutes // minutes_interval) * minutes_interval
    hour = floored // 60
    minute = floored % 60
    return dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

# ---------------- Scanners per TF ----------------
async def process_timeframe_once(tf_check: str, tf_higher: str, ma20_p: int, ma50_p: int,
                                 exchange, name, symbols):
    """
    Process scanning for one TF (tf_check) and its higher TF (tf_higher).
    This function loops symbols and sends signal immediately when found (after candle close).
    """
    error_count = 0
    found_count = 0
    for symbol in symbols:
        try:
            # daily filter
            df1d = get_ohlcv(exchange, symbol, "1d", 10)
            await safe_sleep(RATE_DELAY)
            if df1d is None or check_daily_drop(df1d):
                continue

            df_check = get_ohlcv(exchange, symbol, tf_check, 200)
            await safe_sleep(RATE_DELAY)
            if df_check is None or len(df_check) < 3:
                continue

            engulf, o, c, t_close = is_bullish_engulfing(df_check)
            if not engulf:
                continue

            # fetch higher timeframe data for MA checks
            needed = max(ma20_p, ma50_p) + 20
            df_higher = get_ohlcv(exchange, symbol, tf_higher, needed)
            await safe_sleep(RATE_DELAY)
            if df_higher is None:
                continue

            ok, ma20_val, ma50_val = ma_in_range_and_order(o, c, df_higher, ma20_p, ma50_p)
            if not ok:
                continue

            # Use candle close timestamp as unique id for dedup
            sig_id = f"{tf_check}-{name}-{symbol}-{int(t_close.timestamp())}"
            if sig_id in sent_signals:
                continue
            sent_signals.add(sig_id)
            found_count += 1

            msg = (
                f"✅ VALID SIGNAL\n"
                f"TF detected : {tf_check}\n"
                f"Exchange    : {name}\n"
                f"Pair        : {symbol}\n"
                f"Open        : {o}\n"
                f"Close       : {c}\n"
                f"Candle UTC  : {t_close.isoformat()}\n"
                f"Higher TF   : {tf_higher}\n"
                f"MA{ma20_p} {tf_higher} = {ma20_val}\n"
                f"MA{ma50_p} {tf_higher} = {ma50_val}\n"
            )
            await send_telegram(msg)

        except Exception:
            error_count += 1

    if error_count:
        logging.warning(f"{name} {tf_check}: {error_count} pair gagal diproses.")
    if found_count:
        logging.warning(f"{name} {tf_check}: {found_count} sinyal terkirim.")

# ---------------- Main loop & timing ----------------
async def main():
    # load markets once per loop to build symbol lists
    while True:
        utc = now_utc()
        minute = utc.minute
        second = utc.second
        hour = utc.hour

        # determine which TFs are due right now (we process only once per close using last_processed)
        # We wait ~20 seconds after exact UTC close, so only trigger when second >= 20
        # and the minute/hour matches TF close rules.
        due_tfs = []

        if second >= 20:
            if minute % 15 == 0:
                # 15m close
                # ensure not double-processed: use floored close timestamp
                close_dt_15 = floor_to_interval(utc, 15)
                if last_processed["15m"] != int(close_dt_15.timestamp()):
                    due_tfs.append("15m")
                    last_processed["15m"] = int(close_dt_15.timestamp())

            if minute % 30 == 0:
                close_dt_30 = floor_to_interval(utc, 30)
                if last_processed["30m"] != int(close_dt_30.timestamp()):
                    due_tfs.append("30m")
                    last_processed["30m"] = int(close_dt_30.timestamp())

            if minute == 0:
                close_dt_1h = floor_to_interval(utc, 60)
                if last_processed["1h"] != int(close_dt_1h.timestamp()):
                    due_tfs.append("1h")
                    last_processed["1h"] = int(close_dt_1h.timestamp())

                if hour % 2 == 0:
                    close_dt_2h = floor_to_interval(utc, 120)
                    if last_processed["2h"] != int(close_dt_2h.timestamp()):
                        due_tfs.append("2h")
                        last_processed["2h"] = int(close_dt_2h.timestamp())

        # load markets once per loop
        try:
            markets_usdm = exchange_usdm.load_markets()
            symbols_usdm = [s for s, m in markets_usdm.items() if s.endswith(("USDT", "USD"))]
        except Exception as e:
            logging.warning(f"Failed loading USDM markets: {e}")
            symbols_usdm = []

        try:
            markets_coinm = exchange_coinm.load_markets()
            symbols_coinm = [s for s, m in markets_coinm.items() if s.endswith(("USDT", "USD"))]
        except Exception as e:
            logging.warning(f"Failed loading COINM markets: {e}")
            symbols_coinm = []

        # For each due TF, run the appropriate scanner
        for tf in due_tfs:
            if tf == "15m":
                # check MA20 & MA50 on 1h
                await process_timeframe_once("15m", "1h", 20, 50, exchange_usdm, "USDⓈ-M", symbols_usdm)
                await process_timeframe_once("15m", "1h", 20, 50, exchange_coinm, "COIN-M", symbols_coinm)
            elif tf == "30m":
                # check on 2h
                await process_timeframe_once("30m", "2h", 20, 50, exchange_usdm, "USDⓈ-M", symbols_usdm)
                await process_timeframe_once("30m", "2h", 20, 50, exchange_coinm, "COIN-M", symbols_coinm)
            elif tf == "1h":
                # check on 4h
                await process_timeframe_once("1h", "4h", 20, 50, exchange_usdm, "USDⓈ-M", symbols_usdm)
                await process_timeframe_once("1h", "4h", 20, 50, exchange_coinm, "COIN-M", symbols_coinm)
            elif tf == "2h":
                # check on 8h
                await process_timeframe_once("2h", "8h", 20, 50, exchange_usdm, "USDⓈ-M", symbols_usdm)
                await process_timeframe_once("2h", "8h", 20, 50, exchange_coinm, "COIN-M", symbols_coinm)

        # Sleep small amount and re-evaluate; use 5-second granularity to catch close+20s condition reliably
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
