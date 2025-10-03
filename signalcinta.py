import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta, timezone

# === Konfigurasi ===
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
TELEGRAM_CHAT_ID = "7183177114"

BASE_URL = "https://fapi.binance.com"
SYMBOLS_URL = BASE_URL + "/fapi/v1/exchangeInfo"
KLINES_URL = BASE_URL + "/fapi/v1/klines"

TF_LIST = {
    "5m": "TWS_OP_5m.txt",
    "15m": "TWS_OP_15m.txt",
    "30m": "TWS_OP_30m.txt"
}
MA_WINDOWS = {"ma50": 50, "ma100": 100, "ma200": 200}

# === Utils ===
def send_telegram(msg: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except Exception as e:
        print("Telegram error:", e)

def fetch_klines(symbol, interval, limit=300):
    url = f"{KLINES_URL}?symbol={symbol}&interval={interval}&limit={limit}"
    r = requests.get(url, timeout=10)
    data = r.json()
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "qav","num_trades","taker_base_vol","taker_quote_vol","ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    return df

def compute_ma(series, window):
    return series.rolling(window=window).mean()

def is_bullish(candle):
    return candle["close"] > candle["open"]

def touched(value, low, high, tol=0.001):
    return (low - tol) <= value <= (high + tol)

# === Logika TWS ===
def check_three_white_soldiers(df, tf_label):
    if df is None or len(df) < max(MA_WINDOWS.values()) + 5:
        return None

    close = df["close"]
    ma50 = compute_ma(close, MA_WINDOWS["ma50"])
    ma100 = compute_ma(close, MA_WINDOWS["ma100"])
    ma200 = compute_ma(close, MA_WINDOWS["ma200"])

    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]

    if not (is_bullish(c1) and is_bullish(c2) and is_bullish(c3)):
        return None
    if not (c2["close"] > c1["close"] and c3["close"] > c2["close"]):
        return None

    ma200_val = ma200.iloc[-3]
    ma100_val = ma100.iloc[-2]
    ma50_val  = ma50.iloc[-1]

    # C1 hanya sentuh MA200
    if not touched(ma200_val, c1["low"], c1["high"]): return None
    if touched(ma100_val, c1["low"], c1["high"]) or touched(ma50_val, c1["low"], c1["high"]): return None

    # C2 hanya sentuh MA100
    if not touched(ma100_val, c2["low"], c2["high"]): return None
    if touched(ma200_val, c2["low"], c2["high"]) or touched(ma50_val, c2["low"], c2["high"]): return None

    # C3 hanya sentuh MA50
    if not touched(ma50_val, c3["low"], c3["high"]): return None
    if touched(ma200_val, c3["low"], c3["high"]) or touched(ma100_val, c3["low"], c3["high"]): return None

    return {
        "tf": tf_label,
        "timestamp": c3["close_time"],
        "price_at_ma50": float(ma50_val),
        "last_close": float(c3["close"])
    }

# === Helper file ===
def save_signal(tf, symbol, signal):
    filename = TF_LIST[tf]
    with open(filename, "w") as f:
        f.write(f"{symbol},{signal['timestamp']},{signal['price_at_ma50']},{signal['last_close']}\n")

def load_signal(tf):
    filename = TF_LIST[tf]
    if not os.path.exists(filename):
        return None
    with open(filename, "r") as f:
        line = f.readline().strip()
        if not line:
            return None
        parts = line.split(",")
        return {
            "symbol": parts[0],
            "timestamp": datetime.fromisoformat(parts[1]),
            "price_at_ma50": float(parts[2]),
            "last_close": float(parts[3])
        }

# === Scan ===
def scan_symbol(symbol):
    try:
        # Skip kalau daily drop >=7%
        daily = fetch_klines(symbol, "1d", limit=2)
        if daily.empty:
            return
        prev_close = daily.iloc[-2]["close"]
        last_close = daily.iloc[-1]["close"]
        drop = (last_close - prev_close) / prev_close * 100
        if drop <= -7:
            return

        for tf in TF_LIST:
            df = fetch_klines(symbol, tf, limit=250)
            signal = check_three_white_soldiers(df, tf)
            if signal:
                now = datetime.now(timezone.utc)
                if now - signal["timestamp"] > timedelta(hours=6):
                    continue
                save_signal(tf, symbol, signal)
                msg = (f"✅ TWS {tf} VALID\n"
                       f"Pair: {symbol}\n"
                       f"Waktu: {signal['timestamp']}\n"
                       f"Harga MA50: {signal['price_at_ma50']}\n"
                       f"Close: {signal['last_close']}")
                send_telegram(msg)
    except Exception as e:
        print("Error scan", symbol, e)

def scan_all_pairs():
    try:
        data = requests.get(SYMBOLS_URL, timeout=10).json()
        symbols = [s["symbol"] for s in data["symbols"] if s["contractType"] in ("PERPETUAL")]
        for sym in symbols:
            if sym.endswith("USDT") or sym.endswith("USD"):
                scan_symbol(sym)
    except Exception as e:
        print("Error ambil symbols:", e)

# === Main Loop ===
if __name__ == "__main__":
    while True:
        print(f"Scan dimulai... {datetime.now()}")
        scan_all_pairs()
        time.sleep(60 * 20)  # tunggu 20 menit
