from app.service import refresh_chain_to_redis
from app.db import get_key
from typing import Dict, Any, List
import requests
from app.models import OptionChain, StrikeRow, OptionLeg

LIVE_FEED_URL = "http://127.0.0.1:8300/live"


def build_option_chain(
    underlying: str,
    expiry: str,
    instruments: List[Dict[str, Any]],
    snapshot: Dict[str, Any],  # kept for compatibility but NOT used now
) -> OptionChain:

    tree: Dict[float, Dict[str, OptionLeg]] = {}

    for inst in instruments:
        instrument_key = inst["instrument_key"]

        # ✅ Fetch LIVE data from Live Feed Microservice
        try:
            tick = requests.get(f"{LIVE_FEED_URL}/{instrument_key}", timeout=2).json()
        except Exception:
            tick = {}

        ltp = tick.get("ltp")
        oi = tick.get("oi", 0)

        strike = float(inst["strike"])
        opt_type = inst["opt_type"]  # "CE" / "PE"

        tree.setdefault(strike, {})
        tree[strike][opt_type] = OptionLeg(
            strike=strike,
            opt_type=opt_type,
            ltp=ltp,
            oi=oi,
        )

    rows: List[StrikeRow] = []
    total_call_oi = 0
    total_put_oi = 0

    for strike in sorted(tree.keys()):
        ce = tree[strike].get("CE")
        pe = tree[strike].get("PE")

        if ce and ce.oi:
            total_call_oi += ce.oi
        if pe and pe.oi:
            total_put_oi += pe.oi

        rows.append(StrikeRow(strike=strike, call=ce, put=pe))

    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0

    return OptionChain(
        underlying=underlying,
        expiry=expiry,
        pcr=pcr,
        rows=rows,
    )
