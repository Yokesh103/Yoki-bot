from typing import Dict, Any, List


def build_option_chain(
    underlying: str,
    expiry: str,
    instruments: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    spot: float,
) -> Dict[str, Any]:
    """
    Build normalized option chain snapshot.

    PURE FUNCTION:
    - No Redis
    - No DB writes
    - No WebSocket calls
    - Deterministic output for same input
    """

    rows: List[Dict[str, Any]] = []
    total_call_oi = 0
    total_put_oi = 0

    # Snapshot format contract:
    # snapshot = { "data": { instrument_key: { "market_data": {...} } } }
    data = snapshot.get("data", {})

    for inst in instruments:
        instrument_key = inst["instrument_key"]

        md = data.get(instrument_key, {}).get("market_data", {})

        strike = float(inst["strike"])
        opt_type = inst["opt_type"]  # "CE" or "PE"

        ltp = md.get("last_traded_price")
        oi = md.get("oi", 0) or 0

        if opt_type == "CE":
            total_call_oi += oi
        elif opt_type == "PE":
            total_put_oi += oi

        rows.append({
            "instrument_key": instrument_key,
            "strike": strike,
            "opt_type": opt_type,
            "ltp": ltp,
            "oi": oi,
        })

    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0.0

    return {
        "underlying": underlying,
        "expiry": expiry,
        "spot": spot,
        "pcr": pcr,
        "instruments": rows,
    }
