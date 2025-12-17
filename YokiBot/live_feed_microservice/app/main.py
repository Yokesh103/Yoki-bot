import asyncio
import os
import time
import logging
import redis.asyncio as redis
from fastapi import FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# We will create dhan_feed.py in the next step
from app.dhan_feed import DhanFeed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LIVE_FEED")
load_dotenv()

REDIS_URL = f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', 6379)}"
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

redis_client = None
feed_task = None

def load_instruments():
    """
    Standard Config for NIFTY 50 & BANK NIFTY (NSE)
    """
    # 1 = NSE Equity Segment
    # 13 = Nifty 50 Index
    # 25 = Nifty Bank Index
    instruments = [
        (1, "13"),  
        (1, "25")
    ]
    
    logger.info(f"Loaded {len(instruments)} instruments to track (NIFTY/BANKNIFTY).")
    return instruments

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, feed_task
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    instruments = load_instruments()
    
    # Start the WebSocket Feed
    feed = DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, redis_client)
    feed_task = asyncio.create_task(feed.run_forever())
    yield
    if feed_task: feed_task.cancel()
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.get("/live_status")
async def live_status():
    ts = await redis_client.get("live:last_packet_ts")
    return {"status": "ONLINE", "last_ws_packet_ts": ts, "now": time.time()}

@app.get("/health")
def health():
    return {"status": "OK"}