# app.py
"""
Core Dhan microservice:
- /profile
- /ltp
- /quote
- /option_chain
- /debug_scrip_master
- /debug_underlyings
- /equity_lookup

Uses local copy of api-scrip-master-detailed.csv if present; otherwise downloads from images.dhan.co.
Compatible: Python 3.12, pandas >=2.x, pydantic >=2.x, fastapi.
"""

import os
import time
from io import StringIO
from typing import Dict, Any, List

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import RootModel

ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY1NDc2MDUwLCJpYXQiOjE3NjUzODk2NTAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5NDA1Mjc5In0.-TLbl7e5ndqVhpRDnNkFgQdfSehvmXdR9-MBUdCopwdX-OFNk_TUnMW885ixBCXvPL2YkSNiOvpFncvqhHpXBQ")  # put your token here
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1109405279")        # put your client id here
API_BASE = "https://api.dhan.co/v2"
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
LOCAL_SCRIP = os.getenv("SCRIP_MASTER_PATH", r"D:\live_feed_microservice\api-scrip-master-detailed.csv")

app = FastAPI(title="DhanHQ Core API")

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID
}

_scrip_cache = {"timestamp": 0.0, "df": None, "ttl": 3600.0}


class InstrumentsPayload(RootModel):
    root: Dict[str, Any]


def chunk_list(lst: List[int], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def load_scrip_master(force: bool = False) -> pd.DataFrame:
    now = time.time()
    if not force and _scrip_cache["df"] is not None and now - _scrip_cache["timestamp"] < _scrip_cache["ttl"]:
        return _scrip_cache["df"]

    # Prefer local file if it exists
    if os.path.exists(LOCAL_SCRIP):
        df = pd.read_csv(LOCAL_SCRIP, low_memory=False)
    else:
        resp = httpx.get(SCRIP_MASTER_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text), low_memory=False)

    # store
    _scrip_cache["df"] = df
    _scrip_cache["timestamp"] = now
    return df


@app.get("/debug_scrip_master")
async def debug_scrip_master():
    try:
        df = load_scrip_master(force=True)
        df_safe = df.replace([float("inf"), float("-inf")], "")
        df_safe = df_safe.fillna("")
        return {"columns": df_safe.columns.tolist(), "sample_rows": df_safe.head(5).to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/debug_underlyings")
async def debug_underlyings():
    df = load_scrip_master()
    cols = [c.upper() for c in df.columns]
    df.columns = cols
    if "UNDERLYING_SYMBOL" not in df.columns:
        return {"error": "UNDERLYING_SYMBOL not found in CSV"}
    unique = df["UNDERLYING_SYMBOL"].fillna("").astype(str).str.upper().unique().tolist()
    return {"found_column": "UNDERLYING_SYMBOL", "sample_values": sorted(unique)}


@app.get("/profile")
async def profile():
    if not ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="DHAN_ACCESS_TOKEN not set in environment")
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{API_BASE}/profile", headers={"access-token": ACCESS_TOKEN})
            r.raise_for_status()
            return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/ltp")
async def ltp(payload: InstrumentsPayload):
    body = payload.root
    # forward to Dhan
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(f"{API_BASE}/marketfeed/ltp", json=body, headers=headers)
            r.raise_for_status()
            return r.json()
    except httpx.HTTPStatusError as e:
        # return Dhan error body if present
        try:
            return e.response.json()
        except Exception:
            raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/quote")
async def quote(payload: InstrumentsPayload):
    body = payload.root
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(f"{API_BASE}/marketfeed/quote", json=body, headers=headers)
            r.raise_for_status()
            return r.json()
    except httpx.HTTPStatusError as e:
        try:
            return e.response.json()
        except Exception:
            raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/equity_lookup")
async def equity_lookup(symbol: str = Query(..., description="Symbol like TCS")):
    """
    Find NSE Equity security id(s) for a symbol (case-insensitive).
    Returns list of matches (security id, display name, segment).
    """
    try:
        df = load_scrip_master()
        df.columns = [c.upper() for c in df.columns]
        if "SYMBOL_NAME" not in df.columns and "UNDERLYING_SYMBOL" not in df.columns:
            raise HTTPException(status_code=500, detail="Scrip master malformed")

        # For equities, instrument type is EQUITY or segment E
        df_eq = df.copy()
        # Use SYMBOL_NAME or UNDERLYING_SYMBOL for matching
        match_mask = (
            df_eq.get("SYMBOL_NAME", "").astype(str).str.contains(symbol, case=False, na=False)
            | df_eq.get("UNDERLYING_SYMBOL", "").astype(str).str.contains(symbol, case=False, na=False)
            | df_eq.get("DISPLAY_NAME", "").astype(str).str.contains(symbol, case=False, na=False)
        )
        df_match = df_eq[match_mask]
        # restrict to equity segment or instrument
        df_match = df_match[
            (df_match.get("SEGMENT", "").astype(str).str.upper() == "E")
            | (df_match.get("INSTRUMENT", "").astype(str).str.upper() == "EQUITY")
            | (df_match.get("EXCH_ID", "").astype(str).str.upper() == "NSE")
        ]
        if df_match.empty:
            return {"error": f"No NSE equity found for {symbol}"}
        out = []
        for _, r in df_match.iterrows():
            out.append({
                "security_id": int(r["SECURITY_ID"]) if pd.notna(r["SECURITY_ID"]) else None,
                "symbol_name": r.get("SYMBOL_NAME", ""),
                "display_name": r.get("DISPLAY_NAME", ""),
                "segment": r.get("SEGMENT", ""),
                "exch_id": r.get("EXCH_ID", "")
            })
        return {"matches": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/option_chain")
async def option_chain(symbol: str, expiry: str = None, limit: int = 200):
    """
    Build option chain for given underlying symbol (e.g., RELIANCE).
    expiry filter accepts YYYY-MM-DD or year fragment like '2025' or '24-02-2026' as in CSV.
    Returns contracts (scrip master rows) + quotes (from /marketfeed/quote).
    """
    try:
        df = load_scrip_master()
        df.columns = [c.upper() for c in df.columns]

        # Standardize underlying symbol column
        if "UNDERLYING_SYMBOL" not in df.columns and "SYMBOL_NAME" in df.columns:
            df["UNDERLYING_SYMBOL"] = df["SYMBOL_NAME"]

        # Filter underlying
        mask = df["UNDERLYING_SYMBOL"].astype(str).str.upper() == symbol.upper()
        df_u = df[mask]
        if df_u.empty:
            return {"error": f"No contracts found for symbol {symbol}"}

        # Filter expiry loosely if provided
        if expiry:
            # try multiple expiry formats
            df_u = df_u[df_u.get("SM_EXPIRY_DATE", "").astype(str).str.contains(expiry, na=False)]

        if df_u.empty:
            return {"error": f"No contracts found for {symbol} with expiry {expiry}"}

        # Keep ONLY option-type records (CE/PE)
        if "OPTION_TYPE" in df_u.columns:
            df_u = df_u[df_u["OPTION_TYPE"].isin(["CE", "PE"])]
        else:
            # try SEM_OPTION_TYPE fallback
            if "SEM_OPTION_TYPE" in df_u.columns:
                df_u = df_u[df_u["SEM_OPTION_TYPE"].isin(["CE", "PE"])]

        if df_u.empty:
            return {"error": "No option-type contracts (CE/PE) for symbol"}

        # ensure numeric strike
        if "STRIKE_PRICE" in df_u.columns:
            df_u["STRIKE_PRICE"] = pd.to_numeric(df_u["STRIKE_PRICE"], errors="coerce")

        df_u = df_u.sort_values(["SM_EXPIRY_DATE", "STRIKE_PRICE", "OPTION_TYPE"])
        df_u = df_u.head(limit)

        # Build grouped payload for quote API: segment -> [ids]
        grouped = {}
        for seg, sub in df_u.groupby("SEGMENT"):
            ids = sub["SECURITY_ID"].dropna().astype(int).tolist()
            if ids:
                grouped[seg] = ids

        results = {}
        async with httpx.AsyncClient(timeout=40) as c:
            for seg, ids in grouped.items():
                seg_results = {}
                for chunk in chunk_list(ids, 800):
                    payload = {seg: chunk}
                    r = await c.post(f"{API_BASE}/marketfeed/quote", json=payload, headers=headers)
                    if r.status_code == 200:
                        data = r.json().get("data", {})
                        if seg in data:
                            seg_results.update(data[seg])
                results[seg] = seg_results

        return {
            "symbol": symbol,
            "expiry_filter": expiry,
            "count": len(df_u),
            "contracts": df_u.to_dict(orient="records"),
            "quotes": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
