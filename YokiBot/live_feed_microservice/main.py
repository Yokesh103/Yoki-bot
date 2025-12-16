# app.py
"""
Core Dhan microservice (HYBRID SAFE):
- REST is the source of truth
- WS is only a trigger (via Redis)

Endpoints:
- /profile
- /ltp
- /quote
- /option_chain
- /debug_scrip_master
- /debug_underlyings
- /equity_lookup
- /live_status   <-- NEW

Compatible: Python 3.12, pandas >=2.x, fastapi, redis
"""
LATEST_TICKS = {}
import os
import time
from io import StringIO
from typing import Dict, Any, List


import httpx
import pandas as pd
import redis
from fastapi import FastAPI, HTTPException, Query
from pydantic import RootModel

# -----------------------
# DHAN CONFIG
# -----------------------
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
API_BASE = "https://api.dhan.co/v2"

SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
LOCAL_SCRIP = os.getenv("SCRIP_MASTER_PATH", r"D:\live_feed_microservice\api-scrip-master-detailed.csv")

# -----------------------
# REDIS (HYBRID TRIGGER)
# -----------------------
REDIS = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

# -----------------------
# FASTAPI
# -----------------------
app = FastAPI(title="DhanHQ Core API (Hybrid Safe)")

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
}

# -----------------------
# CACHE
# -----------------------
_scrip_cache = {"timestamp": 0.0, "df": None, "ttl": 3600.0}


class InstrumentsPayload(RootModel):
    root: Dict[str, Any]


def chunk_list(lst: List[int], size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def load_scrip_master(force: bool = False) -> pd.DataFrame:
    now = time.time()
    if not force and _scrip_cache["df"] is not None:
        if now - _scrip_cache["timestamp"] < _scrip_cache["ttl"]:
            return _scrip_cache["df"]

    if os.path.exists(LOCAL_SCRIP):
        df = pd.read_csv(LOCAL_SCRIP, low_memory=False)
    else:
        r = httpx.get(SCRIP_MASTER_URL, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text), low_memory=False)

    _scrip_cache["df"] = df
    _scrip_cache["timestamp"] = now
    return df


# =========================
# HEALTH / LIVE STATUS
# =========================
@app.get("/health")
def health():
    return {"status": "OK"}


@app.get("/live_status")
def live_status():
    """
    Hybrid heartbeat.
    WS service updates Redis key:
      live:last_packet_ts
    """
    return {
        "last_ws_packet_ts": REDIS.get("live:last_packet_ts"),
        "now": time.time(),
    }


# =========================
# DEBUG
# =========================
@app.get("/debug_scrip_master")
def debug_scrip_master():
    df = load_scrip_master(force=True)
    df = df.replace([float("inf"), float("-inf")], "").fillna("")
    return {
        "columns": df.columns.tolist(),
        "sample": df.head(5).to_dict(orient="records"),
    }


@app.get("/debug_underlyings")
def debug_underlyings():
    df = load_scrip_master()
    df.columns = [c.upper() for c in df.columns]
    if "UNDERLYING_SYMBOL" not in df.columns:
        return {"error": "UNDERLYING_SYMBOL missing"}
    return {
        "values": sorted(
            df["UNDERLYING_SYMBOL"].astype(str).str.upper().unique().tolist()
        )
    }


# =========================
# DHAN PASSTHROUGH
# =========================
@app.get("/profile")
async def profile():
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{API_BASE}/profile", headers=headers)
        r.raise_for_status()
        return r.json()


@app.post("/ltp")
async def ltp(payload: InstrumentsPayload):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(
            f"{API_BASE}/marketfeed/ltp", json=payload.root, headers=headers
        )
        r.raise_for_status()
        return r.json()


@app.post("/quote")
async def quote(payload: InstrumentsPayload):
    async with httpx.AsyncClient(timeout=25) as c:
        r = await c.post(
            f"{API_BASE}/marketfeed/quote", json=payload.root, headers=headers
        )
        r.raise_for_status()
        return r.json()


# =========================
# OPTION CHAIN (REST TRUTH)
# =========================
@app.get("/option_chain")
async def option_chain(symbol: str, expiry: str | None = None, limit: int = 200):
    df = load_scrip_master()
    df.columns = [c.upper() for c in df.columns]

    if "UNDERLYING_SYMBOL" not in df.columns:
        df["UNDERLYING_SYMBOL"] = df.get("SYMBOL_NAME", "")

    df_u = df[df["UNDERLYING_SYMBOL"].astype(str).str.upper() == symbol.upper()]
    if df_u.empty:
        return {"error": f"No contracts for {symbol}"}

    if expiry:
        df_u = df_u[df_u["SM_EXPIRY_DATE"].astype(str).str.contains(expiry, na=False)]

    df_u = df_u[df_u.get("OPTION_TYPE", "").isin(["CE", "PE"])]
    if df_u.empty:
        return {"error": "No option contracts"}

    df_u["STRIKE_PRICE"] = pd.to_numeric(df_u["STRIKE_PRICE"], errors="coerce")
    df_u = df_u.sort_values(["SM_EXPIRY_DATE", "STRIKE_PRICE", "OPTION_TYPE"]).head(limit)

    grouped: Dict[str, List[int]] = {}
    for seg, sub in df_u.groupby("SEGMENT"):
        ids = sub["SECURITY_ID"].dropna().astype(int).tolist()
        if ids:
            grouped: Dict[str, List[int]] = {}

    quotes: Dict[str, Any] = {}
    async with httpx.AsyncClient(timeout=40) as c:
        for seg, ids in grouped.items():
            seg_data = {}
            for chunk in chunk_list(ids, 800):
                r = await c.post(
                    f"{API_BASE}/marketfeed/quote",
                    json={seg: chunk},
                    headers=headers,
                )
                if r.status_code == 200:
                    seg_data.update(r.json().get("data", {}).get(seg, {}))
            quotes[seg] = seg_data

    return {
        "symbol": symbol,
        "expiry_filter": expiry,
        "count": len(df_u),
        "contracts": df_u.to_dict(orient="records"),
        "quotes": quotes,
    }
