# app/clients/optionchain_client.py
import os
import requests
from typing import Any, Dict

from app.engine.models import DecideRequest, Instrument
from app.config import OPTIONCHAIN_SERVICE_URL

TIMEOUT = 3  # seconds
DEFAULT_UNDERLYING = os.getenv("DEFAULT_UNDERLYING", "NIFTY")


def fetch_optionchain(underlying: str | None = None) -> DecideRequest:
    """
    Fetches normalized snapshot from optionchain-service and converts it into
    a DecideRequest for the signal engine.

    - Calls: {OPTIONCHAIN_SERVICE_URL}/snapshot/{underlying}
    - Raises HTTP/requests exceptions on network error or non-2xx.
    - Raises ValueError on malformed response.
    """
    u = (underlying or DEFAULT_UNDERLYING).upper()
    url = f"{OPTIONCHAIN_SERVICE_URL.rstrip('/')}/snapshot/{u}"

    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    data: Dict[str, Any] = resp.json()

    # Basic validation of the snapshot contract
    if not isinstance(data, dict):
        raise ValueError("Optionchain snapshot not a JSON object")

    if "instruments" not in data or not isinstance(data["instruments"], list):
        raise ValueError("Invalid snapshot: missing instruments list")

    if "spot" not in data:
        raise ValueError("Invalid snapshot: missing spot")

    # Build Instrument list expected by Evaluate function
    instruments = []
    for i in data["instruments"]:
        # Basic defensive extraction - will raise if required keys missing
        strike = i.get("strike")
        opt_type = i.get("opt_type")
        ltp = i.get("ltp")
        oi = i.get("oi", 0)

        if strike is None or opt_type is None or ltp is None:
            # If the snapshot contains incomplete rows, fail-fast
            raise ValueError(f"Incomplete instrument data in snapshot: {i}")

        instruments.append(
            Instrument(
                strike=float(strike),
                opt_type=str(opt_type),
                ltp=float(ltp),
                oi=float(oi) if oi is not None else 0.0
            )
        )

    return DecideRequest(
        underlying=data.get("underlying", u),
        expiry=data.get("expiry", ""),   # optional; can be empty string
        spot=float(data["spot"]),
        instruments=instruments
    )
