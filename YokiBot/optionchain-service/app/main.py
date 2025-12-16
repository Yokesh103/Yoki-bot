from fastapi import FastAPI
from typing import Literal
from datetime import date, datetime

from app.db import (
    init_db,
    get_instruments_from_db,
    get_expiries_for_underlying,
)
from app.data_source import RestMarketDataSource
from app.option_chain_service import build_option_chain

app = FastAPI(title="Option Chain Service")

# ---- INIT ----
init_db()
data_source = RestMarketDataSource()


# -------------------------
# HEALTH CHECK
# -------------------------
@app.get("/health")
def health():
    return {"status": "OK"}


# -------------------------
# EXPIRIES API
# -------------------------
@app.get("/expiries/{underlying}")
def get_expiries(underlying: Literal["NIFTY", "BANKNIFTY"]):
    return get_expiries_for_underlying(underlying)


# -------------------------
# AUTO OPTION CHAIN
# -------------------------
@app.get("/option-chain/{underlying}/auto")
def get_option_chain_auto(underlying: Literal["NIFTY", "BANKNIFTY"]):

    expiries = get_expiries_for_underlying(underlying)
    if not expiries:
        return {"error": "No expiries found in DB"}

    today = date.today().isoformat()
    valid_expiries = [e for e in expiries if e >= today]
    if not valid_expiries:
        return {"error": "No valid upcoming expiries"}

    expiry = valid_expiries[0]

    instruments = get_instruments_from_db(underlying, expiry)
    if not instruments:
        return {"error": "No instruments found"}

    strikes = sorted({inst["strike"] for inst in instruments})
    if not strikes:
        return {"error": "No strikes available"}

    spot = float(strikes[len(strikes) // 2])

    snapshot = data_source.get_snapshot(
        [inst["instrument_key"] for inst in instruments]
    )

    chain = build_option_chain(
        underlying=underlying,
        expiry=expiry,
        instruments=instruments,
        snapshot=snapshot,
        spot=spot,
    )

    return {
        "underlying": underlying,
        "spot": spot,
        "expiry": expiry,
        "data": chain,
    }


# =====================================================
# SNAPSHOT ENDPOINT (FOR SIGNAL-ENGINE)
# =====================================================
@app.get("/snapshot/{underlying}")
def snapshot(underlying: Literal["NIFTY", "BANKNIFTY"]):
    """
    Stable snapshot contract for signal-engine
    """

    expiries = get_expiries_for_underlying(underlying)
    if not expiries:
        return {"error": "No expiries found"}

    today = date.today().isoformat()
    valid_expiries = [e for e in expiries if e >= today]
    if not valid_expiries:
        return {"error": "No valid expiry"}

    expiry = valid_expiries[0]

    instruments = get_instruments_from_db(underlying, expiry)
    if not instruments:
        return {"error": "No instruments"}

    strikes = sorted({inst["strike"] for inst in instruments})
    if not strikes:
        return {"error": "No strikes"}

    spot = float(strikes[len(strikes) // 2])

    snapshot_data = data_source.get_snapshot(
        [inst["instrument_key"] for inst in instruments]
    )

    chain = build_option_chain(
        underlying=underlying,
        expiry=expiry,
        instruments=instruments,
        snapshot=snapshot_data,
        spot=spot,
    )

    return {
        "underlying": chain["underlying"],
        "expiry": chain["expiry"],
        "spot": chain["spot"],
        "timestamp": datetime.utcnow().isoformat(),
        "instruments": chain["instruments"],
    }
