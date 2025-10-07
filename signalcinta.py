# main.py
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
last_processed = {"15m": None, "30m": None, "1h": None, "2h": None}

# small delay between fetches to reduce pressure
RATE_DELAY = 0.12  # seconds

# wait-after-close before fetching to let exchange finalize candle
WAIT_AFTER_CLOSE_SECONDS = 20

# Logging (railway-friendly)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ---------------- Helpers ----------------
def now_utc():
    return datetime.now(timezone.utc)

async def safe_sleep(seconds: float):
    await asyncio.sleep(seconds)

async def send_telegram(msg: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")

def _parse_timeframe_minutes(tf: str):
    """
    Parse timeframe string like '15m', '1h', '8h' into minutes integer.
    Return minutes or None if unknown format.
    """
    tf = tf.lower().strip()
    if tf.endswith('m'):
        return int(tf[:-1])
    if tf.endswith('h'):
        return int(tf[:-1]) * 60
    if tf.endswith('d'):
        return int(tf[:-1]) * 60 * 24
    return None

def _resample_ohlcv(df_base: pd.DataFrame, base_tf_min: int, target_tf_min: int):
    """
    Resample base timeframe OHLCV (base_tf_min minutes) into target timeframe (target_tf_min minutes).
    - df_base must have columns ['time','open','high','low','close','volume'] with tz-aware 'time' (UTC).
    """
    if df_base is None or len(df_base) == 0:
        return None
    # set datetime index
    df = df_base.copy()
    df = df.set_index(pd.DatetimeIndex(df["time"]))
    rule = f'{target_tf_min}T'  # pandas offset alias in minutes, e.g., '240T' for 4h
    agg = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    res = df.resample(rule, label='right', closed='right').agg(agg).dropna(how='all')
    # Reset index into 'time' column (tz-aware)
    res = res.reset_index().rename(columns={'index': 'time'})
    res = res[['time', 'open', 'high', 'low', 'close', 'volume']]
    # ensure time column tz-aware UTC
    res['time'] = pd.to_datetime(res['time']).dt.tz_convert('UTC')
    return res

def get_ohlcv_with_resample(exchange, symbol, timeframe, limit=200):
    """
    Attempt to fetch ohlcv for 'timeframe'. If exchange supports timeframe directly -> use it.
    Otherwise try to resample from base timeframe ('1m' for minute-based, '1h' for hour-based).
    Returns DataFrame with tz-aware UTC times or None on failure.
    """
    tf = timeframe.lower()
    # direct fetch if supported
    try:
        if hasattr(exchange, 'timeframes') and tf in exchange.timeframes:
            data = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
            df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
            df["open"] = df["open"].astype(float)
            df["close"] = df["close"].astype(float)
            df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
            return df
    except Exception as e:
        logging.debug(f"Direct fetch error {symbol} {timeframe}: {e}")

    # resample path
    minutes = _parse_timeframe_minutes(tf)
    if minutes is None:
        return None

    # choose base timeframe for resampling
    # prefer '1h' if minutes >= 60 else '1m'
    if minutes >= 60:
        base_tf = '1h'
        base_minutes = 60
    else:
        base_tf = '1m'
        base_minutes = 1

    multiplier = minutes // base_minutes
    # ensure integer multiplier
    if multiplier < 1:
        multiplier = 1

    # need more base candles to generate 'limit' target candles
    needed_base = limit * multiplier + 10  # small safety margin
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=base_tf, limit=needed_base)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["open"] = df["open"].astype(float)
        df["close"] = df["close"].astype(float)
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        # resample to target minutes
        res = _resample_ohlcv(df, base_minutes, minutes)
        # take last 'limit' rows
        if res is None or len(res) == 0:
            return None
        return res.tail(limit).reset_index(drop=True)
    except Exception as e:
        logging.debug(f"Resample fetch error {symbol} {timeframe} from {base_tf}: {e}")
        return None

def is_bullish_engulfing_df(df):
    """Return (True, open, close, close_time) if last candle is bullish engulfing over previous 2 red candles."""
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
    """Return True if daily drop >=5% (from first open to last close)."""
    if df1d is None or len(df1d) < 2:
        return False
    first = df1d.iloc[0]["open"]
    last = df1d.iloc[-1]["close"]
    drop = (last - first) / first * 100
    return drop < -5

def ma20_ma50_and_extra_check(open_price, close_price, df_higher_main, df_higher_extra,
                              ma20_main=20, ma50_main=50, ma20_extra=20):
    """
    Validate:
      (A) MA20_main and MA50_main on df_higher_main both lie in [open,close] and MA20_main < MA50_main
      (B) MA20_extra on df_higher_extra lies in [open,close]
    Returns (True, ma20_main_val, ma50_main_val, ma20_extra_val) or (False, None, None, None)
    """
    # check main ma20 & ma50
    if df_higher_main is None or len(df_higher_main) < max(ma20_main, ma50_main):
        return False, None, None, None
    dfm = df_higher_main.copy()
    dfm[f"ma{ma20_main}"] = dfm["close"].rolling(ma20_main).mean()
    dfm[f"ma{ma50_main}"] = dfm["close"].rolling(ma50_main).mean()
    ma20_main_val = dfm.iloc[-1][f"ma{ma20_main}"]
    ma50_main_val = dfm.iloc[-1][f"ma{ma50_main}"]
    if pd.isna(ma20_main_val) or pd.isna(ma50_main_val):
        return False, None, None, None

    lo = min(open_price, close_price)
    hi = max(open_price, close_price)
    cond_main = (lo <= ma20_main_val <= hi) and (lo <= ma50_main_val <= hi) and (ma20_main_val < ma50_main_val)
    if not cond_main:
        return False, None, None, None

    # check extra MA20 on df_higher_extra
    if df_higher_extra is None or len(df_higher_extra) < ma20_extra:
        return False, None, None, None
    dfe = df_higher_extra.copy()
    dfe[f"ma{ma20_extra}"] = dfe["close"].rolling(ma20_extra).mean()
    ma20_extra_val = dfe.iloc[-1][f"ma{ma20_extra}"]
    if pd.isna(ma20_extra_val):
        return False, None, None, None
    cond_extra = (lo <= ma20_extra_val <= hi)
    if not cond_extra:
        return False, None, None, None

    return True, float(ma20_main_val), float(ma50_main_val), float(ma20_extra_val)

# ---------------- Scanners per TF ----------------
async def process_tf_once(tf_check, tf_higher_main, tf_higher_extra, exchange, name, symbols,
                          ma20_main=20, ma50_main=50, ma20_extra=20):
    """
    Process scanning for one TF:
    - tf_check: the timeframe to detect engulfing (ex: '15m')
    - tf_higher_main: timeframe to compute MA20 & MA50 (ex: '1h')
    - tf_higher_extra: timeframe to compute extra MA20 (ex: '2h')
    """
    error_count = 0
    sent_count = 0
    for symbol in symbols:
        try:
            # daily filter
            df1d = get_ohlcv_with_resample(exchange, symbol, "1d", 10)
            await safe_sleep(RATE_DELAY)
            if df1d is None or check_daily_drop(df1d):
                continue

            df_check = get_ohlcv_with_resample(exchange, symbol, tf_check, 300)
            await safe_sleep(RATE_DELAY)
            if df_check is None or len(df_check) < 3:
                continue

            engulf, o, c, t_close = is_bullish_engulfing_df(df_check)
            if not engulf:
                continue

            # fetch main higher (for MA20 & MA50) and extra higher (for MA20)
            df_main = get_ohlcv_with_resample(exchange, symbol, tf_higher_main, 300)
            await safe_sleep(RATE_DELAY)
            df_extra = get_ohlcv_with_resample(exchange, symbol, tf_higher_extra, 300)
            await safe_sleep(RATE_DELAY)
            if df_main is None or df_extra is None:
                continue

            ok, ma20_main_val, ma50_main_val, ma20_extra_val = ma20_ma50_and_extra_check(
                o, c, df_main, df_extra, ma20_main, ma50_main, ma20_extra
            )
            if not ok:
                continue

            # unique id per candle close timestamp
            sig_id = f"{tf_check}-{name}-{symbol}-{int(pd.Timestamp(t_close).timestamp())}"
            if sig_id in sent_signals:
                continue
            sent_signals.add(sig_id)
            sent_count += 1

            msg = (
                f"✅ VALID SIGNAL\n"
                f"TF detected : {tf_check}\n"
                f"Exchange    : {name}\n"
                f"Pair        : {symbol}\n"
                f"Open        : {o}\n"
                f"Close       : {c}\n"
                f"Candle UTC  : {t_close.isoformat()}\n"
                f"Higher main : {tf_higher_main} | MA20 = {ma20_main_val:.8f} | MA50 = {ma50_main_val:.8f}\n"
                f"Higher extra: {tf_higher_extra} | MA20 = {ma20_extra_val:.8f}\n"
            )
            await send_telegram(msg)

        except Exception:
            error_count += 1

    if error_count:
        logging.warning(f"{name} {tf_check}: {error_count} pair gagal diproses.")
    if sent_count:
        logging.warning(f"{name} {tf_check}: {sent_count} sinyal terkirim.")

# ---------------- Main loop & timing ----------------
async def main():
    while True:
        utc = now_utc()
        minute = utc.minute
        second = utc.second
        hour = utc.hour

        # determine due TFs: we wait until >= WAIT_AFTER_CLOSE_SECONDS after exact close
        due_tfs = []

        if second >= WAIT_AFTER_CLOSE_SECONDS:
            if minute % 15 == 0:
                close_dt_15 = utc.replace(second=0, microsecond=0) - timedelta(minutes=utc.minute % 15)
                close_ts = int(close_dt_15.timestamp())
                if last_processed["15m"] != close_ts:
                    due_tfs.append("15m")
                    last_processed["15m"] = close_ts

            if minute % 30 == 0:
                close_dt_30 = utc.replace(second=0, microsecond=0) - timedelta(minutes=utc.minute % 30)
                close_ts = int(close_dt_30.timestamp())
                if last_processed["30m"] != close_ts:
                    due_tfs.append("30m")
                    last_processed["30m"] = close_ts

            if minute == 0:
                close_dt_1h = utc.replace(minute=0, second=0, microsecond=0)
                close_ts = int(close_dt_1h.timestamp())
                if last_processed["1h"] != close_ts:
                    due_tfs.append("1h")
                    last_processed["1h"] = close_ts

                if hour % 2 == 0:
                    close_dt_2h = utc.replace(minute=0, second=0, microsecond=0)
                    close_ts = int(close_dt_2h.timestamp())
                    if last_processed["2h"] != close_ts:
                        due_tfs.append("2h")
                        last_processed["2h"] = close_ts

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

        # Run scanners for due TFs (sequential to reduce concurrent API load)
        for tf in due_tfs:
            if tf == "15m":
                # 15m -> check main 1h (MA20/MA50) and extra 2h (MA20)
                await process_tf_once("15m", "1h", "2h", exchange_usdm, "USDⓈ-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_usdm)
                await process_tf_once("15m", "1h", "2h", exchange_coinm, "COIN-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_coinm)
            elif tf == "30m":
                # 30m -> main 2h, extra 4h
                await process_tf_once("30m", "2h", "4h", exchange_usdm, "USDⓈ-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_usdm)
                await process_tf_once("30m", "2h", "4h", exchange_coinm, "COIN-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_coinm)
            elif tf == "1h":
                # 1h -> main 4h, extra 8h
                await process_tf_once("1h", "4h", "8h", exchange_usdm, "USDⓈ-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_usdm)
                await process_tf_once("1h", "4h", "8h", exchange_coinm, "COIN-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_coinm)
            elif tf == "2h":
                # 2h -> main 8h, extra 16h (resampled if needed)
                await process_tf_once("2h", "8h", "16h", exchange_usdm, "USDⓈ-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_usdm)
                await process_tf_once("2h", "8h", "16h", exchange_coinm, "COIN-M",
                                      ma20_main=20, ma50_main=50, ma20_extra=20, symbols=symbols_coinm)

        # sleep short interval and re-evaluate (5s granularity)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
