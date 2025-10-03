import pandas as pd
import requests
import time
import os
import json
from datetime import datetime, timezone, timedelta

# === KONFIGURASI TELEGRAM ===
TELEGRAM_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
TELEGRAM_CHAT_ID = "7183177114"

# === KONFIGURASI ===
MA_WINDOWS = {"ma50": 50, "ma100": 100, "ma200": 200}
INTERVALS = {"5m": "5m", "15m": "15m", "30m": "30m"}
BASE_URLS = {"um": "https://fapi.binance.com", "cm": "https://dapi.binance.com"}
PCT_SKIP = -7.0  # skip pair kalau TF1D turun >= 7%
OUTPUT_FILES = {"5m": "TWS_OP_5m.txt", "15m": "TWS_OP_15m.txt", "30m": "TWS_OP_30m.txt"}
STATE_FILE = "notified.json"
MAX_AGE_HOURS = 6  # skip sinyal kalau sudah lebih dari 6 jam

# === UTILITIES ===
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
        if r.status_code != 200:
            print(f"Gagal kirim notif: {r.text}")
    except Exception as e:
        print(f"Error Telegram notif: {e}")

def fetch_klines(symbol: str, interval: str, limit=210, base="um"):
    try:
        url = f"{BASE_URLS[base]}/fapi/v1/klines" if base == "um" else f"{BASE_URLS[base]}/dapi/v1/klines"
        r = requests.get(url, params={"symbol": symbol, "interval": interval, "limit": limit})
        data = r.json()
        cols = ["open_time","open","high","low","close","volume","close_time","qav","trades","tbbav","tbqav","ignore"]
        df = pd.DataFrame(data, columns=cols)
        for col in ["open","high","low","close","volume"]:
            df[col] = df[col].astype(float)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        return df
    except Exception as e:
        print(f"Error fetch klines {symbol} {interval}: {e}")
        return None

def compute_ma(series, window):
    return series.rolling(window=window).mean()

def is_bullish(candle):
    return candle['close'] > candle['open']

def touched(ma_val, low, high):
    return low <= ma_val <= high

def check_three_white_soldiers(df, tf_label):
    if df is None or len(df) < max(MA_WINDOWS.values()) + 5:
        return None

    close = df['close']
    ma50 = compute_ma(close, MA_WINDOWS['ma50'])
    ma100 = compute_ma(close, MA_WINDOWS['ma100'])
    ma200 = compute_ma(close, MA_WINDOWS['ma200'])

    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]

    if not (is_bullish(c1) and is_bullish(c2) and is_bullish(c3)):
        return None
    if not (c2['close'] > c1['close'] and c3['close'] > c2['close']):
        return None

    ma200_val = ma200.iloc[-3]
    ma100_val = ma100.iloc[-2]
    ma50_val  = ma50.iloc[-1]

    # Candle pertama harus sentuh MA200 saja
    if not touched(ma200_val, c1['low'], c1['high']):
        return None
    if touched(ma100_val, c1['low'], c1['high']) or touched(ma50_val, c1['low'], c1['high']):
        return None

    # Candle kedua harus sentuh MA100 saja
    if not touched(ma100_val, c2['low'], c2['high']):
        return None
    if touched(ma200_val, c2['low'], c2['high']) or touched(ma50_val, c2['low'], c2['high']):
        return None

    # Candle ketiga harus sentuh MA50 saja
    if not touched(ma50_val, c3['low'], c3['high']):
        return None
    if touched(ma200_val, c3['low'], c3['high']) or touched(ma100_val, c3['low'], c3['high']):
        return None

    return {
        'tf': tf_label,
        'timestamp': c3['close_time'].isoformat(),
        'price_at_ma50': float(ma50_val),
        'last_close': float(c3['close'])
    }

# === NOTIFY STATE ===
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

# === SCAN LOOP ===
def main():
    state = load_state()
    while True:
        print("Scan dimulai...", datetime.now())
        results = {"5m": [], "15m": [], "30m": []}

        try:
            um_info = requests.get(BASE_URLS["um"] + "/fapi/v1/exchangeInfo").json()
            um_pairs = [s['symbol'] for s in um_info['symbols'] if s['contractType'] == 'PERPETUAL']
        except Exception as e:
            print(f"Gagal fetch um pairs: {e}")
            um_pairs = []

        try:
            cm_info = requests.get(BASE_URLS["cm"] + "/dapi/v1/exchangeInfo").json()
            cm_pairs = [s['symbol'] for s in cm_info['symbols'] if s['contractType'] == 'PERPETUAL']
        except Exception as e:
            print(f"Gagal fetch cm pairs: {e}")
            cm_pairs = []

        all_pairs = [(sym, "um") for sym in um_pairs] + [(sym, "cm") for sym in cm_pairs]

        for sym, base in all_pairs:
            df1d = fetch_klines(sym, "1d", 2, base)
            if df1d is None or len(df1d) < 2:
                continue
            prev = df1d.iloc[-2]['close']
            last = df1d.iloc[-1]['close']
            chg = (last - prev) / prev * 100
            if chg <= PCT_SKIP:
                continue

            for tf in INTERVALS:
                df = fetch_klines(sym, INTERVALS[tf], 210, base)
                if df is None:
                    continue
                res = check_three_white_soldiers(df, tf)
                if res:
                    # Skip kalau sudah lebih dari 6 jam
                    ts = datetime.fromisoformat(res['timestamp'].replace("Z", "+00:00"))
                    if datetime.now(timezone.utc) - ts > timedelta(hours=MAX_AGE_HOURS):
                        continue

                    entry = f"{sym} {tf} {res['timestamp']} harga {res['last_close']} MA50 {res['price_at_ma50']}"
                    results[tf].append(entry)

                    key = f"{sym}_{tf}_{res['timestamp']}"
                    if key not in state:
                        msg = f"🚨 TWS OP {tf}\nPair: {sym}\nClose: {res['last_close']}\nMA50: {res['price_at_ma50']}\nTime: {res['timestamp']}"
                        send_telegram(msg)
                        state[key] = True
                        save_state(state)

        for tf, file in OUTPUT_FILES.items():
            with open(file, 'w') as f:
                f.write("\n".join(results[tf]))

        print("Scan selesai, tidur 30 menit...")
        time.sleep(1800)

if __name__ == "__main__":
    main()
