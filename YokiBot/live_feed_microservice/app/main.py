import asyncio
import os
import time
from dotenv import load_dotenv
from fastapi import FastAPI
import redis.asyncio as redis  # ✅ ASYNC REDIS

# --------------------------------------------------
# 1. LOAD ENV FIRST
# --------------------------------------------------
load_dotenv()

# --------------------------------------------------
# 2. REDIS SETUP (LOCAL, NO CROSS-IMPORTS)
# --------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

REDIS = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

# --------------------------------------------------
# 3. INTERNAL IMPORTS (AFTER ENV)
# --------------------------------------------------
from app.instrument_map import load_instruments
from app.dhan_ws import dhan_ws_worker

# --------------------------------------------------
# 4. APP INSTANCE
# --------------------------------------------------
app = FastAPI(title="DhanHQ Live Feed Microservice")
CSV_PATH = "api-scrip-master-detailed.csv"

# --------------------------------------------------
# 5. STARTUP EVENT
# --------------------------------------------------
@app.on_event("startup")
async def startup_event():
    print(f"[LIVE_FEED] DHAN_CLIENT_ID loaded: {os.getenv('DHAN_CLIENT_ID')}")

    try:
        load_instruments(CSV_PATH)
    except Exception as e:
        print(f"[ERROR] Failed to load CSV: {e}")

    asyncio.create_task(dhan_ws_worker())

# --------------------------------------------------
# 6. ROUTES
# --------------------------------------------------
@app.get("/live_status")
async def live_status():
    last_packet = await REDIS.get("live:last_packet_ts")
    return {
        "last_ws_packet_ts": last_packet,
        "now": time.time(),
    }

@app.get("/health")
def health():
    return {"status": "OK"}
