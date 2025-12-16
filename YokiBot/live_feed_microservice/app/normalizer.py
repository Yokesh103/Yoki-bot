# app/normalizer.py
import time

def normalize(raw_tick, instrument):
    return {
        "symbol": instrument["symbol"],
        "expiry": instrument["expiry"],
        "strike": instrument["strike"],
        "option_type": instrument["option_type"],
        "ltp": raw_tick["ltp"],
        "oi": raw_tick.get("oi"),
        "volume": raw_tick.get("volume"),
        "timestamp": int(time.time())
    }
