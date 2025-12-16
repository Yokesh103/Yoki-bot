import asyncio
import os
import time
from dotenv import load_dotenv
from fastapi import FastAPI
from app.redis_client import redis_client

# 1. LOAD ENV FIRST
load_dotenv()

# 2. INTERNAL IMPORTS
from app.instrument_map import load_instruments
from app.dhan_ws import dhan_ws_worker

# 3. DEFINE APP INSTANCE (Must be before routes)
app = FastAPI(title="DhanHQ Core API (Hybrid Safe)")
CSV_PATH = "api-scrip-master-detailed.csv"

# 4. STARTUP EVENT
@app.on_event("startup")
async def startup_event():
    # Prove env is loaded
    print(f"[LIVE_FEED] DHAN_CLIENT_ID loaded: {os.getenv('DHAN_CLIENT_ID')}")

    # Load instrument map once
    try:
        load_instruments(CSV_PATH)
    except Exception as e:
        print(f"[ERROR] Failed to load CSV: {e}")

    # Start WS worker in background
    asyncio.create_task(dhan_ws_worker())

# 5. ROUTES
@app.get("/live_status")
def live_status():
    last_ts = redis_client.get("live:last_packet_ts")
    return {
        "status": "connected" if last_ts else "waiting",
        "last_ws_packet_ts": last_ts,
        "now": time.time(),
    }

@app.get("/health")
def health():
    return {"status": "OK"}