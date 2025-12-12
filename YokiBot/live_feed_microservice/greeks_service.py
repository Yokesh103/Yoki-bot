# greeks_service.py
"""
Pure-Python Greeks microservice (no SciPy).
- Black-Scholes pricing (European)
- Normal PDF/CDF using math.erf
- Implied volatility via bisection root finding
- Computes delta, gamma, theta, vega, rho
- Optional Kafka consumer to receive marketfeed messages (if KAFKA_BOOTSTRAP set)
- FastAPI endpoints: /iv, /greeks, /surface
Compatible with Python 3.10+ (including 3.12)
"""

import os
import math
import time
import json
import asyncio
from typing import Dict, Any, Tuple, Optional
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query

# Optional Kafka consumer. If you don't use Kafka, set KAFKA_BOOTSTRAP empty.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "marketfeed")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "greeks_group")

try:
    from aiokafka import AIOKafkaConsumer
except Exception:
    AIOKafkaConsumer = None  # kafka optional

app = FastAPI(title="Greeks Service (pure-Python)")

# in-memory caches / placeholders
_latest_quotes: Dict[int, Dict[str, Any]] = {}         # security_id -> latest payload
_surface_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}   # (underlying, expiry) -> surface info
_debounce_tasks: Dict[str, asyncio.Task] = {}
_DEBOUNCE_MS = 500  # debounce window

# -------------------------
# Normal distribution utils
# -------------------------
SQRT2 = math.sqrt(2.0)
SQRT2PI = math.sqrt(2.0 * math.pi)


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT2PI


def norm_cdf(x: float) -> float:
    # use error function
    return 0.5 * (1.0 + math.erf(x / SQRT2))


# -------------------------
# Black-Scholes functions
# -------------------------
def bs_price(S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "CE") -> float:
    """Return Black-Scholes price for European call/put."""
    if T <= 0:
        # immediate intrinsic
        return max(0.0, (S - K) if opt_type == "CE" else (K - S))
    if sigma <= 0:
        return max(0.0, (S - K) if opt_type == "CE" else (K - S)) * math.exp(-r * T)
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    if opt_type == "CE":
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)


def compute_greeks(S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "CE") -> Dict[str, Optional[float]]:
    """Return delta,gamma,theta,vega,rho. Theta is per-day (approx) and vega per 1 vol point."""
    if sigma is None or sigma <= 0 or T <= 0:
        return {"delta": None, "gamma": None, "theta": None, "vega": None, "rho": None}
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    pdf_d1 = norm_pdf(d1)
    if opt_type == "CE":
        delta = norm_cdf(d1)
        theta = -(S * pdf_d1 * sigma) / (2 * sqrtT) - r * K * math.exp(-r * T) * norm_cdf(d2)
        rho = K * T * math.exp(-r * T) * norm_cdf(d2)
    else:
        delta = norm_cdf(d1) - 1.0
        theta = -(S * pdf_d1 * sigma) / (2 * sqrtT) + r * K * math.exp(-r * T) * norm_cdf(-d2)
        rho = -K * T * math.exp(-r * T) * norm_cdf(-d2)
    gamma = pdf_d1 / (S * sigma * sqrtT)
    vega = S * pdf_d1 * sqrtT
    # standardize theta to per-day
    theta_per_day = theta / 365.0
    return {
        "delta": float(delta),
        "gamma": float(gamma),
        "theta": float(theta_per_day),
        "vega": float(vega),
        "rho": float(rho)
    }


# -------------------------
# Implied volatility via bisection
# -------------------------
def implied_vol_bisect(mid_price: float, S: float, K: float, T: float, r: float = 0.06, opt_type: str = "CE",
                       tol: float = 1e-6, max_iter: int = 100) -> Optional[float]:
    """
    Compute implied volatility using bisection root finder.
    mid_price : target option mid price (market price)
    Returns vol or None if not found.
    """
    if mid_price <= 0 or S <= 0 or K <= 0 or T < 0:
        return None

    # Payoff bounds: min price (intrinsic discounted) and max price (S)
    # Lower bound sigma ~ 1e-8
    low = 1e-6
    high = 5.0  # 500% vol upper bound

    f_low = bs_price(S, K, T, r, low, opt_type) - mid_price
    f_high = bs_price(S, K, T, r, high, opt_type) - mid_price

    # If pricing at low/high already brackets zero, proceed
    if f_low * f_high > 0:
        # no bracket; check if mid_price is below intrinsic (impossible) or above theoretical high
        # If mid_price < intrinsic -> return 0.0 vol
        intrinsic = max(0.0, (S - K) if opt_type == "CE" else (K - S))
        if mid_price <= intrinsic:
            return 0.0
        # else try expanding high
        for attempt in range(5):
            high *= 2
            f_high = bs_price(S, K, T, r, high, opt_type) - mid_price
            if f_low * f_high <= 0:
                break
        else:
            return None

    for i in range(max_iter):
        mid = 0.5 * (low + high)
        price_mid = bs_price(S, K, T, r, mid, opt_type)
        diff = price_mid - mid_price
        if abs(diff) < tol:
            return float(mid)
        # decide side
        if diff > 0:
            high = mid
        else:
            low = mid
    return float(0.5 * (low + high))


# -------------------------
# Debounce + (placeholder) recompute
# -------------------------
def schedule_recompute(key: str):
    # cancel existing and schedule a new one
    loop = asyncio.get_event_loop()
    if key in _debounce_tasks:
        t = _debounce_tasks[key]
        if not t.done():
            t.cancel()
    _debounce_tasks[key] = loop.create_task(_debounced_recompute(key))


async def _debounced_recompute(key: str):
    try:
        await asyncio.sleep(_DEBOUNCE_MS / 1000.0)
    except asyncio.CancelledError:
        return
    # placeholder: build a trivial surface entry with timestamp
    _surface_cache[(key, "latest")] = {"updated_ts": int(time.time() * 1000), "notes": "recomputed (placeholder)"}


# -------------------------
# Optional Kafka consumer
# -------------------------
consumer = None


async def start_kafka_consumer():
    global consumer
    if not KAFKA_BOOTSTRAP or AIOKafkaConsumer is None:
        return
    consumer = AIOKafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id=KAFKA_GROUP)
    await consumer.start()
    asyncio.create_task(kafka_loop())


async def kafka_loop():
    global consumer
    try:
        async for msg in consumer:
            # decode
            try:
                payload = json.loads(msg.value.decode())
            except Exception:
                continue
            header = payload.get("header", {})
            sec_id = header.get("sec_id") or header.get("security_id")
            if sec_id:
                try:
                    sec_id = int(sec_id)
                except Exception:
                    pass
            _latest_quotes[sec_id] = payload
            # trigger recompute keyed by sec_id (or map to underlying later)
            schedule_recompute(f"SEC:{sec_id}")
    finally:
        if consumer:
            await consumer.stop()


# -------------------------
# FastAPI endpoints
# -------------------------
@app.get("/surface")
async def get_surface(underlying: str, expiry: Optional[str] = None):
    key = (underlying, expiry or "latest")
    val = _surface_cache.get(key)
    if not val:
        raise HTTPException(status_code=404, detail="surface not available")
    return val


@app.get("/iv")
async def endpoint_iv(
    spot: float = Query(..., description="Spot price"),
    strike: float = Query(..., description="Strike price"),
    expiry: str = Query(..., description="Expiry date in YYYY-MM-DD"),
    opt_type: str = Query("CE", regex="^(CE|PE)$"),
    mid: float = Query(..., description="Market mid price"),
    r: float = Query(0.06, description="Risk-free rate (annual)"),
):
    """
    Compute implied vol using bisection. Requires spot and mid.
    expiry must be parseable to YYYY-MM-DD.
    """
    try:
        from datetime import datetime
        exp_dt = datetime.strptime(expiry, "%Y-%m-%d")
        T = max(0.0, (exp_dt - datetime.utcnow()).total_seconds() / (365.0 * 24 * 3600))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"expiry parse error: {e}")

    iv = implied_vol_bisect(mid_price=mid, S=spot, K=strike, T=T, r=r, opt_type=opt_type)
    if iv is None:
        raise HTTPException(status_code=500, detail="IV not found")
    return {"iv": iv, "T": T}


@app.get("/greeks")
async def endpoint_greeks(
    spot: float = Query(...),
    strike: float = Query(...),
    expiry: str = Query(...),
    iv: float = Query(...),
    opt_type: str = Query("CE", regex="^(CE|PE)$"),
    r: float = Query(0.06),
):
    try:
        from datetime import datetime
        exp_dt = datetime.strptime(expiry, "%Y-%m-%d")
        T = max(0.0, (exp_dt - datetime.utcnow()).total_seconds() / (365.0 * 24 * 3600))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"expiry parse error: {e}")
    g = compute_greeks(spot, strike, T, r, iv, opt_type)
    return {"greeks": g, "T": T}


# -------------------------
# Startup / Shutdown
# -------------------------
@app.on_event("startup")
async def startup():
    # start kafka consumer if available
    if AIOKafkaConsumer and KAFKA_BOOTSTRAP:
        asyncio.create_task(start_kafka_consumer())


@app.on_event("shutdown")
async def shutdown():
    global consumer
    if consumer:
        await consumer.stop()
