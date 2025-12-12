# main.py - Greeks microservice (final)
# Requirements: fastapi, uvicorn, pydantic, redis, python-dotenv, httpx
# Run: uvicorn main:app --host 0.0.0.0 --port 8400 --reload

import os
import time
import math
import json
from datetime import datetime, timezone
print(datetime.now())
from typing import Optional, List

from pydantic import BaseModel, Field, condecimal
from fastapi import FastAPI, HTTPException
import redis
from dotenv import load_dotenv

load_dotenv()

# -------------------------
# Config
# -------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
CACHE_TTL = int(os.getenv("GREeks_CACHE_TTL", "30"))  # seconds

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="greeks-service", version="1.0")

# -------------------------
# Models
# -------------------------
class GreeksRequest(BaseModel):
    symbol: str
    underlying: float
    strike: float
    expiry: str  # "YYYY-MM-DD" or ISO
    option_type: str = Field(..., pattern="^(CE|PE)$")
    iv: Optional[float] = None  # annualized implied vol (decimal, e.g. 0.25)
    option_price: Optional[float] = None  # market option price (if iv not provided)
    r: Optional[float] = 0.06  # risk-free rate (annual decimal)
    q: Optional[float] = 0.0   # dividend yield (annual decimal)

class BatchRequest(BaseModel):
    requests: List[GreeksRequest]

# -------------------------
# Math helpers (normal pdf/cdf)
# -------------------------
SQRT2 = math.sqrt(2.0)
SQRT2PI = math.sqrt(2 * math.pi)


def norm_cdf(x: float) -> float:
    """Standard normal CDF"""
    return 0.5 * (1.0 + math.erf(x / SQRT2))


def norm_pdf(x: float) -> float:
    """Standard normal PDF"""
    return math.exp(-0.5 * x * x) / SQRT2PI


# -------------------------
# Expiry / time helper
# -------------------------
def parse_expiry_to_years(expiry_str: str) -> float:
    """
    Accepts YYYY-MM-DD or ISO-like strings. Returns time to expiry in years (float).
    If expiry is in the past or zero, returns 0.
    """
    try:
        # try ISO first
        dt = datetime.fromisoformat(expiry_str)
    except Exception:
        try:
            dt = datetime.strptime(expiry_str, "%Y-%m-%d")
        except Exception:
            raise ValueError("expiry must be YYYY-MM-DD or ISO datetime")

    # ensure timezone aware as UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    seconds = (dt - now).total_seconds()
    if seconds <= 0:
        return 0.0
    return seconds / (365.0 * 24.0 * 3600.0)


# -------------------------
# Black-Scholes calculator
# -------------------------
def bs_price_and_greeks(S: float, K: float, t: float, sigma: float, r: float, q: float, opt_type: str):
    """
    Compute Black-Scholes theoretical price and Greeks.
    t = time to expiry in YEARS (float)
    sigma = volatility (annual, decimal)
    r = risk-free rate
    q = dividend yield
    opt_type = "CE" or "PE"
    Returns dict with: price, delta, gamma, vega, theta, rho, d1, d2
    """
    if t <= 0 or sigma <= 0:
        # immediate expiry or zero vol: approximate payoff
        if opt_type == "CE":
            price = max(S - K, 0.0)
            delta = 1.0 if S > K else 0.0
        else:
            price = max(K - S, 0.0)
            delta = -1.0 if S < K else 0.0
        return {
            "price": price,
            "delta": delta,
            "gamma": 0.0,
            "vega": 0.0,
            "theta": 0.0,
            "rho": 0.0,
            "d1": None,
            "d2": None,
        }

    sqrt_t = math.sqrt(t)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    Nd1 = norm_cdf(d1)
    Nd2 = norm_cdf(d2)
    npd1 = norm_pdf(d1)

    # Discount factors
    disc_r = math.exp(-r * t)
    disc_q = math.exp(-q * t)

    if opt_type == "CE":
        price = S * disc_q * Nd1 - K * disc_r * Nd2
        delta = disc_q * Nd1
        rho = K * t * disc_r * Nd2
    else:
        # Put
        Nmd1 = norm_cdf(-d1)
        Nmd2 = norm_cdf(-d2)
        price = K * disc_r * Nmd2 - S * disc_q * Nmd1
        delta = -disc_q * Nmd1
        rho = -K * t * disc_r * Nmd2

    gamma = (disc_q * npd1) / (S * sigma * sqrt_t)
    vega = S * disc_q * npd1 * sqrt_t  # per 1 vol decimal
    # Theta: per year; convert to per day if needed. We'll return per year.
    # Full theta formula (annualized):
    theta = (-S * disc_q * npd1 * sigma) / (2 * sqrt_t) - r * K * disc_r * (Nd2 if opt_type == "CE" else -norm_cdf(-d2)) + q * S * disc_q * (Nd1 if opt_type == "CE" else -norm_cdf(-d1))
    # The above theta is annualized. Keep consistent.

    return {
        "price": price,
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
        "rho": rho,
        "d1": d1,
        "d2": d2,
    }


# -------------------------
# Implied volatility via bisection
# -------------------------
def implied_vol_bisect(market_price: float, S: float, K: float, t: float, r: float, q: float, opt_type: str, tol=1e-6, max_iter=60):
    """
    Solve for implied volatility using bisection between low and high bounds.
    Returns sigma (decimal) or raises ValueError if not found.
    """
    if t <= 0:
        raise ValueError("expiry already passed or zero time to expiry")

    # lower and upper bounds
    lo = 1e-6
    hi = 5.0  # 500% vol upper bound
    # Ensure sign at ends
    def price_at(sigma):
        return bs_price_and_greeks(S, K, t, sigma, r, q, opt_type)["price"]

    price_lo = price_at(lo)
    price_hi = price_at(hi)

    # If market price is outside [price_lo, price_hi], bisection will fail
    # widen hi until price_hi >= market_price or hi grows too large
    iter_expand = 0
    while price_hi < market_price and iter_expand < 10:
        hi *= 2
        price_hi = price_at(hi)
        iter_expand += 1

    # If still price_hi < market_price, can't find implied vol
    if price_hi < market_price:
        raise ValueError("market price too large for reasonable vol bounds")

    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        p_mid = price_at(mid)
        # difference
        if abs(p_mid - market_price) < tol:
            return mid
        # choose side
        if p_mid < market_price:
            lo = mid
        else:
            hi = mid
    # final mid
    return 0.5 * (lo + hi)


# -------------------------
# Caching helpers
# -------------------------
def greeks_cache_key(req: GreeksRequest) -> str:
    # Round numeric fields to stable strings for cache key
    expiry_norm = req.expiry.strip()
    return f"greeks:{req.symbol}:{expiry_norm}:{req.option_type}:{int(req.strike)}:{int(round(req.underlying))}"


# -------------------------
# Core compute function
# -------------------------
def compute_greeks_from_request(req: GreeksRequest) -> dict:
    # parse expiry -> time to expiry in years
    t = parse_expiry_to_years(req.expiry)
    if t <= 0:
        # already expired: return immediate payoff / zeros
        if req.option_type == "CE":
            price = max(req.underlying - req.strike, 0.0)
            delta = 1.0 if req.underlying > req.strike else 0.0
        else:
            price = max(req.strike - req.underlying, 0.0)
            delta = -1.0 if req.underlying < req.strike else 0.0
        return {
            "symbol": req.symbol,
            "strike": req.strike,
            "expiry": req.expiry,
            "option_type": req.option_type,
            "underlying": req.underlying,
            "iv": 0.0,
            "theoretical_price": price,
            "delta": delta,
            "gamma": 0.0,
            "vega": 0.0,
            "theta": 0.0,
            "rho": 0.0,
            "d1": None,
            "d2": None,
            "timestamp": time.time(),
        }

    sigma = req.iv
    # If no iv provided, try to infer from option_price if present
    if sigma is None:
        if req.option_price is None:
            raise ValueError("Either iv or option_price must be provided")
        # compute implied vol
        try:
            sigma = implied_vol_bisect(req.option_price, req.underlying, req.strike, t, req.r, req.q, req.option_type)
        except Exception as e:
            # if cannot find iv, return an error-like payload with exec_result showing failure
            raise ValueError(f"implied vol solve failed: {e}")

    # Now compute blacks cholesky
    res = bs_price_and_greeks(req.underlying, req.strike, t, sigma, req.r, req.q, req.option_type)

    return {
        "symbol": req.symbol,
        "strike": req.strike,
        "expiry": req.expiry,
        "option_type": req.option_type,
        "underlying": req.underlying,
        "iv": sigma,
        "theoretical_price": res["price"],
        "delta": res["delta"],
        "gamma": res["gamma"],
        "vega": res["vega"],
        "theta": res["theta"],
        "rho": res["rho"],
        "d1": res["d1"],
        "d2": res["d2"],
        "timestamp": time.time(),
    }


# -------------------------
# Endpoints
# -------------------------
@app.get("/health")
def health():
    try:
        redis_client.ping()
        return {"status": "ok", "redis": True}
    except Exception:
        return {"status": "ok", "redis": False}


@app.post("/compute")
def compute(req: GreeksRequest):
    key = greeks_cache_key(req)
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)

    try:
        out = compute_greeks_from_request(req)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    redis_client.set(key, json.dumps(out))
    redis_client.expire(key, CACHE_TTL)
    return out


@app.post("/batch")
def batch(req: BatchRequest):
    results = []
    for r in req.requests:
        key = greeks_cache_key(r)
        cached = redis_client.get(key)
        if cached:
            results.append(json.loads(cached))
            continue
        try:
            out = compute_greeks_from_request(r)
        except Exception as e:
            # include error per-request
            results.append({"error": str(e), "symbol": r.symbol, "strike": r.strike, "expiry": r.expiry})
            continue
        redis_client.set(key, json.dumps(out))
        redis_client.expire(key, CACHE_TTL)
        results.append(out)

    return {"count": len(results), "results": results}


# -------------------------
# Simple utility endpoint to compute many strikes from stored chain
# This helps integration with option-chain service: it expects the chain already in Redis.
# -------------------------
@app.get("/compute_chain/{symbol}/{expiry}")
def compute_chain(symbol: str, expiry: str, window: int = 0):
    """
    Read option chain stored under key chain:{symbol}:{expiry} and compute greeks batch for relevant strikes.
    window: +/- number of strikes around ATM to calculate (0 = only ATM if present)
    Expects chain JSON produced by option-chain-service in Redis under chain:{symbol}:{expiry}.
    """
    key_chain = f"chain:{symbol}:{expiry}"
    raw = redis_client.get(key_chain)
    if not raw:
        raise HTTPException(status_code=404, detail="chain not found in redis")

    chain = json.loads(raw)
    underlying = chain.get("underlying_ltp") or chain.get("underlying") or chain.get("underlyingPrice") or chain.get("underlyingValue")
    if underlying is None:
        raise HTTPException(status_code=400, detail="underlying LTP not found in chain")

    calls = chain.get("calls", [])
    puts = chain.get("puts", [])
    strikes = sorted(list({int(x.get("strikePrice") or x.get("strike") or 0) for x in calls + puts}))
    # find ATM index
    atm = chain.get("atm")
    if atm is None:
        # fallback to nearest
        atm = min(strikes, key=lambda s: abs(s - underlying))

    # choose strikes around atm depending on window
    if window <= 0:
        chosen = [atm]
    else:
        idx = strikes.index(int(atm)) if int(atm) in strikes else None
        if idx is None:
            # pick nearest slice
            strikes_sorted = strikes
            # find nearest index
            idx = min(range(len(strikes_sorted)), key=lambda i: abs(strikes_sorted[i] - atm))
        start = max(0, idx - window)
        end = min(len(strikes), idx + window + 1)
        chosen = strikes[start:end]

    # build batch requests
    batch_reqs = []
    for s in chosen:
        # find option market price if present
        ce_price = None
        pe_price = None
        for c in calls:
            if int(c.get("strikePrice", 0)) == int(s):
                ce_price = c.get("last_price") or c.get("lastPrice") or c.get("ltp")
                break
        for p in puts:
            if int(p.get("strikePrice", 0)) == int(s):
                pe_price = p.get("last_price") or p.get("lastPrice") or p.get("ltp")
                break

        if ce_price is not None:
            batch_reqs.append(GreeksRequest(
                symbol=symbol, underlying=underlying, strike=s, expiry=expiry, option_type="CE", option_price=float(ce_price)
            ))
        if pe_price is not None:
            batch_reqs.append(GreeksRequest(
                symbol=symbol, underlying=underlying, strike=s, expiry=expiry, option_type="PE", option_price=float(pe_price)
            ))

    # call batch compute
    batch_request = BatchRequest(requests=batch_reqs)
    return batch(batch_request)


# -------------------------
# End file
# -------------------------
