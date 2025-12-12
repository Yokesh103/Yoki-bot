from fastapi import FastAPI
from typing import Literal
from datetime import date

from app.db import init_db
from app.db_instruments import get_instruments_from_db, get_expiries_for_underlying
from app.data_source import RestMarketDataSource
from app.option_chain_service import build_option_chain

app = FastAPI(title="Option Chain Service")

init_db()
data_source = RestMarketDataSource()


@app.get("/health")
def health():
    return {"status": "OK"}


@app.get("/expiries/{underlying}")
def get_expiries(underlying: Literal["NIFTY", "BANKNIFTY"]):
    return get_expiries_for_underlying(underlying)


@app.get("/option-chain/{underlying}/auto")
def get_option_chain_auto(underlying: Literal["NIFTY", "BANKNIFTY"]):

    expiries = get_expiries_for_underlying(underlying)
    if not expiries:
        return {"error": "No expiries found in DB"}

    from datetime import date

    today = date.today().isoformat()
    valid_expiries = [e for e in expiries if e >= today]

    if not valid_expiries:
        return {"error": "No valid upcoming expiries"}

    expiry = valid_expiries[0]

    instruments = get_instruments_from_db(underlying, expiry)
    if not instruments:
        return {"error": "No instruments found for given underlying & expiry"}

    strikes = sorted({inst["strike"] for inst in instruments})
    if not strikes:
        return {"error": "No strikes available"}

    mid = len(strikes) // 2
    spot = float(strikes[mid])
    atm = spot

    snapshot = data_source.get_snapshot(
        [inst["instrument_key"] for inst in instruments]
    )

    chain = build_option_chain(
        underlying=underlying,
        expiry=expiry,
        instruments=instruments,
        snapshot=snapshot,
        spot=spot
    )

    return {
        "underlying": underlying,
        "spot": spot,
        "atm": atm,
        "expiry": expiry,
        "data": chain
    }
