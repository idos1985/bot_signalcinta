# main.py
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import ccxt
import pandas as pd
from telegram import Bot

# ------------- CONFIG -------------
BOT_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"   # ganti sendiri
CHAT_ID = "7183177114"       # ganti sendiri

# Exchanges (Binance futures)
exchange_usdm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
exchange_coinm = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'delivery'}})

bot = Bot(BOT_TOKEN)

# state: hindari duplikat (persist jika mau di masa depan)
sent_signals = set()

# small delays to reduce API pressure
RATE_DELAY = 0.12  # seconds between fetches
WAIT_AFTER_CLOSE = 20  # seconds to wait after candle close to ensure OHLCV final

# logging -- keep light for Railway
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")


# ------------- HELPERS -------------
def now_utc():
    return datetime.now(timezone.utc)


async def safe_sleep(sec: float):
    await asyncio.sleep(sec)


async def send_telegram(msg: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int = 200):
    """
    Fetch OHLCV and return pandas DataFrame indexed by timezone-aware UTC 'time'.
    Returns None on failure.
    """
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        # convert time & set index
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.set_index("time", inplace=True)
        # ensure numeric
        df = df.astype(float)
        return df
    except Exception as e:
        logging.debug(f"fetch_ohlcv error {symbol} {timeframe}: {e}")
        return None


def is_bullish_engulfing(df):
    """
    Detect bullish engulfing where last candle (c3) is green and engulfs previous two red candles.
    Return (True, open_c3, close_c3, close_time) or (False, None, None, None).
    """
    if df is None or len(df) < 3:
        return False, None, None, None
    try:
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
    except Exception:
        return False, None, None, None

    if (c1["close"] < c1["open"]) and (c2["close"] < c2["open"]) and (c3["close"] > c3["open"]):
        min_open = min(c1["open"], c2["open"])
        max_close = max(c1["close"], c2["close"])
        if (c3["open"] < min_open) and (c3["close"] > max_close):
            return True, float(c3["open"]), float(c3["close"]), c3.name  # c3.name is Timestamp (UTC)
    return False, None, None, None


def check_daily_drop(df1d):
    """
    Return True if drop >= 5% from first open to last close in df1d.
    """
    if df1d is None or len(df1d) < 2:
        return False
    first_open = df1d.iloc[0]["open"]
    last_close = df1d.iloc[-1]["close"]
    drop_pct = (last_close - first_open) / first_open * 100
    return drop_pct < -5


def ma20_ma200_in_range_and_order(open_p, close_p, df_ma):
    """
    Check MA20 and MA200 are present in df_ma and:
      - both lie inside [min(open,close), max(open,close)]
      - MA20 < MA200 (MA20 bawah, MA200 atas)
    Returns (True, ma20_val, ma200_val) or (False, None, None)
    """
    if df_ma is None:
        return False, None, None
    # need at least 200 candles for MA200
    if len(df_ma) < 200:
        return False, None, None

    d = df_ma.copy()
    d["ma20"] = d["close"].rolling(20).mean()
    d["ma200"] = d["close"].rolling(200).mean()

    ma20 = d.iloc[-1]["ma20"]
    ma200 = d.iloc[-1]["ma200"]
    if pd.isna(ma20) or pd.isna(ma200):
        return False, None, None

    lo = min(open_p, close_p)
    hi = max(open_p, close_p)
    # check they are within the candle range and order is ma20 < ma200
    if (lo <= ma20 <= hi) and (lo <= ma200 <= hi) and (ma20 < ma200):
        return True, float(ma20), float(ma200)
    return False, None, None


# ------------- PER-TIMEFRAME PROCESSORS -------------
async def process_tf_for_exchange(exchange, name, tf_check, tf_ma_check, symbols):
    """
    Process scanning for given timeframe tf_check:
      - tf_check: timeframe to detect engulfing ("15m", "30m", "1h")
      - tf_ma_check: timeframe to compute MA20 & MA200 ("1h" for 15m, "2h" for 30m, "1h" for 1h)
    """
    errors = 0
    sent = 0
    for symbol in symbols:
        try:
            # daily filter
            df1d = fetch_ohlcv_df(exchange, symbol, "1d", 10)
            await safe_sleep(RATE_DELAY)
            if df1d is None or check_daily_drop(df1d):
                continue

            # fetch df for tf_check (we want the latest closed candle)
            df_check = fetch_ohlcv_df(exchange, symbol, tf_check, 10)
            await safe_sleep(RATE_DELAY)
            if df_check is None or len(df_check) < 3:
                continue

            engulf, open_c, close_c, close_t = is_bullish_engulfing(df_check)
            if not engulf:
                continue

            # fetch timeframe for MA check
            df_ma = fetch_ohlcv_df(exchange, symbol, tf_ma_check, 400)
            await safe_sleep(RATE_DELAY)
            if df_ma is None:
                continue

            ok, ma20_val, ma200_val = ma20_ma200_in_range_and_order(open_c, close_c, df_ma)
            if not ok:
                # explicit invalid if ma20 > ma200 inside range OR did not satisfy condition
                continue

            # unique id: tf-sym-timestamp
            sig_id = f"{tf_check}-{name}-{symbol}-{int(pd.Timestamp(close_t).timestamp())}"
            if sig_id in sent_signals:
                continue

            sent_signals.add(sig_id)
            sent += 1

            # prepare message (round numbers sensibly)
            msg = (
                f"✅ VALID SIGNAL\n"
                f"TF detected : {tf_check}\n"
                f"Exchange    : {name}\n"
                f"Pair        : {symbol}\n"
                f"Open        : {open_c:.8f}\n"
                f"Close       : {close_c:.8f}\n"
                f"Candle UTC  : {close_t.isoformat()}\n"
                f"MA20 ({tf_ma_check}) = {ma20_val:.8f}\n"
                f"MA200({tf_ma_check}) = {ma200_val:.8f}\n"
            )
            await send_telegram(msg)

        except Exception:
            errors += 1

    if errors:
        logging.warning(f"{name} {tf_check}: {errors} symbols failed.")
    if sent:
        logging.warning(f"{name} {tf_check}: {sent} signals sent.")


# ------------- SCHEDULER & MAIN LOOP -------------
async def main():
    # prepare: load markets once per loop per exchange
    while True:
        utc = now_utc()
        minute = utc.minute
        second = utc.second

        # decide which TFs are due: require second >= WAIT_AFTER_CLOSE
        due = []

        if second >= WAIT_AFTER_CLOSE:
            if minute % 15 == 0:
                due.append("15m")
            if minute % 30 == 0:
                due.append("30m")
            if minute == 0:
                due.append("1h")

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

        # run due scanners sequentially (reduce concurrent API load)
        for tf in due:
            if tf == "15m":
                # 15m -> check MA on 1h
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "15m", "1h", symbols_usdm)
                await process_tf_for_exchange(exchange_coinm, "COIN-M", "15m", "1h", symbols_coinm)
            elif tf == "30m":
                # 30m -> check MA on 2h
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "30m", "2h", symbols_usdm)
                await process_tf_for_exchange(exchange_coinm, "COIN-M", "30m", "2h", symbols_coinm)
            elif tf == "1h":
                # 1h -> check MA on 1h (no heavy higher TF check)
                await process_tf_for_exchange(exchange_usdm, "USDⓈ-M", "1h", "1h", symbols_usdm)
                await process_tf_for_exchange(exchange_coinm, "COIN-M", "1h", "1h", symbols_coinm)

        # sleep 5 seconds and re-evaluate (we use second >= WAIT_AFTER_CLOSE guard)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
