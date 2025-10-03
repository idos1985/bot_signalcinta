import pandas as pd
import requests
import time
import os
from datetime import datetime, UTC

# ========== CONFIG ==========
BOT_TOKEN = "8309387013:AAHHMBhUcsmBPOX2j5aEJatNmiN6VnhI2CM"
CHAT_ID = "7183177114"

# toleransi untuk "menyentuh MA"
TOL = 0.002   # 0.2%
# minimal body ratio (body / range) untuk menghindari doji
MIN_BODY_RATIO = 0.5
# scan interval (detik)
SCAN_INTERVAL = 1800  # 30 menit
# ============================


def send_telegram(msg):
    """Kirim pesan ke Telegram"""
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ BOT_TOKEN/CHAT_ID tidak terdeteksi (cek Railway Variables).")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print("Telegram error:", r.status_code, r.text)
    except Exception as e:
        print("Fail send Telegram:", e)


# --- ambil kline (Futures USD-M via fapi, COIN-M via dapi) ---
def get_data(symbol, interval, limit=300, coin_m=False):
    base = "https://dapi.binance.com/dapi/v1/klines" if coin_m else "https://fapi.binance.com/fapi/v1/klines"
    url = f"{base}?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if isinstance(data, dict) and data.get("code"):
            return None
        df = pd.DataFrame(data, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","qav","num_trades","taker_base","taker_quote","ignore"
        ])
        # numeric
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        return df
    except Exception:
        return None


# --- list semua futures pairs ---
def get_all_pairs_usdt(coin_m=False):
    url = "https://dapi.binance.com/dapi/v1/exchangeInfo" if coin_m else "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        r = requests.get(url, timeout=10).json()
        syms = []
        for s in r.get("symbols", []):
            if not coin_m:
                if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
                    syms.append(s["symbol"])
            else:
                if s.get("status") == "TRADING":
                    syms.append(s["symbol"])
        return syms
    except Exception as e:
        print("get_all_pairs error", e)
        return []


# --- skip jika hari ini turun >= 7% (1d) ---
def skip_if_drop(symbol, coin_m=False):
    df = get_data(symbol, "1d", limit=2, coin_m=coin_m)
    if df is None or len(df) < 2:
        return True
    prev = df.iloc[-2]["close"]
    last = df.iloc[-1]["close"]
    drop_pct = (last - prev) / prev * 100
    return drop_pct <= -7.0


# --- bantu: is bullish body & body ratio ---
def body_ratio_ok(row):
    high, low, o, c = row["high"], row["low"], row["open"], row["close"]
    rng = high - low
    if rng == 0:
        return False
    body = abs(c - o)
    return (body / rng) >= MIN_BODY_RATIO


# === deteksi Three White Soldiers OP pada DF (last 3 closed candles) ===
def detect_3ws_op(df):
    """Return ma50_value (float) if pattern valid and last close near MA50, else None"""
    if df is None or len(df) < 210:  # butuh history utk MA200
        return None
    df = df.copy()
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA100"] = df["close"].rolling(window=100).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()

    last3 = df.iloc[-3:]

    # bullish checks
    if not ((last3.iloc[0]["close"] > last3.iloc[0]["open"]) and
            (last3.iloc[1]["close"] > last3.iloc[1]["open"]) and
            (last3.iloc[2]["close"] > last3.iloc[2]["open"])):
        return None

    # body ratio checks
    if not (body_ratio_ok(last3.iloc[0]) and body_ratio_ok(last3.iloc[1]) and body_ratio_ok(last3.iloc[2])):
        return None

    # closes strictly increasing
    if not (last3.iloc[0]["close"] < last3.iloc[1]["close"] < last3.iloc[2]["close"]):
        return None

    # MA ordering
    last = last3.iloc[-1]
    ma50, ma100, ma200 = last["MA50"], last["MA100"], last["MA200"]
    if pd.isna(ma50) or pd.isna(ma100) or pd.isna(ma200):
        return None
    if not (ma200 < ma100 < ma50):
        return None

    # check last close near/above MA50
    close = last["close"]
    if ma50 == 0:
        return None
    if close >= ma50 or abs(close - ma50) / ma50 <= TOL:
        return ma50
    return None


# --- file helpers (pakai file untuk menyimpan 1 signal terakhir) ---
def save_signal_file(fname, symbol, tf, ma50):
    try:
        with open(fname, "w") as f:
            f.write(f"{symbol},{tf},{ma50:.8f},{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    except Exception as e:
        print("save_signal_file error", e)


def read_signal_file(fname):
    try:
        with open(fname, "r") as f:
            line = f.read().strip()
            if not line:
                return None
            parts = line.split(",")
            if len(parts) < 3:
                return None
            symbol = parts[0]
            tf = parts[1]
            ma50 = float(parts[2])
            ts = parts[3] if len(parts) >= 4 else ""
            return {"symbol": symbol, "tf": tf, "ma50": ma50, "ts": ts}
    except FileNotFoundError:
        return None
    except Exception:
        return None


def clear_signal_file(fname):
    try:
        open(fname, "w").close()
    except Exception as e:
        print("clear file error", e)


# --- check current price touches MA50 ---
def check_and_notify_touch(fname, coin_m=False):
    entry = read_signal_file(fname)
    if not entry:
        return
    symbol, tf, ma50 = entry["symbol"], entry["tf"], entry["ma50"]
    df = get_data(symbol, tf, limit=3, coin_m=coin_m)
    if df is None or len(df) == 0:
        return
    last_close = df.iloc[-1]["close"]
    if ma50 == 0:
        return
    touched = (abs(last_close - ma50) / ma50 <= TOL) or (last_close >= ma50)
    if touched:
        msg = (f"📣 *TWS OP {('COIN-M' if coin_m else 'USDⓈ-M')} {tf}*\n"
               f"Pair: {symbol}\n"
               f"Harga sentuh MA50: {ma50:.8f}\n"
               f"Harga sekarang: {last_close:.8f}\n"
               f"Waktu candle: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        send_telegram(msg)
        print("Notif touch sent:", symbol, tf, ma50)
        clear_signal_file(fname)


# --- main scan ---
def scan_tf_for_market(tf, fname, coin_m=False):
    market_name = "COIN-M" if coin_m else "USDⓈ-M"
    print(f"[{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}] Start scan {market_name} {tf}")

    pairs = get_all_pairs_usdt(coin_m=coin_m)
    if not pairs:
        print("No pairs found for", market_name)
        return

    latest_found = None
    for sym in pairs:
        try:
            if skip_if_drop(sym, coin_m=coin_m):
                continue
            df = get_data(sym, tf, limit=300, coin_m=coin_m)
            ma50 = detect_3ws_op(df)
            if ma50:
                latest_found = (sym, tf, ma50)
            time.sleep(0.05)
        except Exception:
            continue

    if latest_found:
        sym, tf_, ma50_ = latest_found
        save_signal_file(fname, sym, tf_, ma50_)
        print("Saved signal:", fname, latest_found)
    else:
        clear_signal_file(fname)
        print("No signal found for", fname)

    check_and_notify_touch(fname, coin_m=coin_m)


# === MAIN LOOP ===
if __name__ == "__main__":
    files = [
        ("5m", "TWS_OP_5m_usdt.txt", False),
        ("15m", "TWS_OP_15m_usdt.txt", False),
        ("30m", "TWS_OP_30m_usdt.txt", False),
        ("5m", "TWS_OP_5m_coin.txt", True),
        ("15m", "TWS_OP_15m_coin.txt", True),
        ("30m", "TWS_OP_30m_coin.txt", True),
    ]

    print("signalcinta_op starting - press Ctrl+C to stop")

    while True:
        cycle_start = datetime.now(UTC)
        print("\n== New cycle:", cycle_start.strftime("%Y-%m-%d %H:%M:%S UTC"), "==")
        send_telegram(f"🚀 Mulai scan TWS OP (5m/15m/30m) - {cycle_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        for tf, fname, coin_m_flag in files:
            scan_tf_for_market(tf, fname, coin_m=coin_m_flag)

        send_telegram(f"✅ Selesai scan cycle - {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("Cycle finished. Sleeping", SCAN_INTERVAL, "seconds.")
        time.sleep(SCAN_INTERVAL)
