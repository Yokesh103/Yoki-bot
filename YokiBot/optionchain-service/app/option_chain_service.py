from app.service import refresh_chain_to_redis
from app.db import get_key
from typing import Dict, Any, List

def build_option_chain(
    underlying: str,
    expiry: str,
    instruments: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    spot: float
):
    data = snapshot.get("data", {})

    rows = []

    for inst in instruments:
        key = inst["instrument_key"]
        md = data.get(key, {}).get("market_data", {})

        row = {
            "instrument_key": key,                # ✅ REQUIRED
            "strike": inst["strike"],
            "opt_type": inst["opt_type"],        # ✅ correct name
            "expiry": expiry,                    # ✅ REQUIRED
            "ltp": md.get("last_traded_price"),
            "oi": md.get("oi", 0)
        }
        rows.append(row)

    return {
        "underlying": underlying,
        "expiry": expiry,
        "spot": spot,                            # ✅ REQUIRED
        "instruments": rows,                     # ✅ NOT "rows"
        "indicators": {                         # ✅ CONTRACT BLOCK
            "adx14": 0,
            "rsi14": 0,
            "atr14": 0,
            "ivrank": 0,
            "vix": 0
        }
    }
