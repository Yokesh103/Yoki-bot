# main.py (debug-enhanced)
import os
import uuid
import time
import json
import asyncio
import traceback
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import redis
import httpx
from dotenv import load_dotenv

load_dotenv()

# CONFIG
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
ORDERS_HASH_PREFIX = os.getenv("ORDERS_HASH_PREFIX", "orders:")
ORDERS_QUEUE = os.getenv("ORDERS_QUEUE", "orders:queue")
PAPER_EXEC_URL = os.getenv("PAPER_EXEC_URL", "http://127.0.0.1:8301/execute")
ROUTE_TIMEOUT = float(os.getenv("ROUTE_TIMEOUT", "5"))

redis_client = redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="order-manager")

# Models
class Leg(BaseModel):
    symbol: str
    side: str = Field(..., pattern="^(BUY|SELL)$")
    exch: Optional[str] = "NSE"
    qty: Optional[int] = None
    price: Optional[float] = None

class OrderReq(BaseModel):
    symbol: str
    strategy: str
    legs: List[Leg]
    qty: Optional[int] = 25
    limit: Optional[float] = None
    meta: Optional[dict] = None

class OrderObj(BaseModel):
    id: str
    symbol: str
    strategy: str
    legs: List[Leg]
    qty: int
    limit: Optional[float]
    status: str
    submitted_at: float
    routed_at: Optional[float] = None
    executed_at: Optional[float] = None
    exec_result: Optional[dict] = None
    meta: Optional[dict] = None

# Helpers
def order_key(order_id: str) -> str:
    return f"{ORDERS_HASH_PREFIX}{order_id}"

def save_order_to_redis(order: dict):
    key = order_key(order["id"])
    redis_client.set(key, json.dumps(order))
    redis_client.lpush("orders:list", key)
    redis_client.ltrim("orders:list", 0, 9999)

def get_order_from_redis(order_id: str) -> Optional[dict]:
    raw = redis_client.get(order_key(order_id))
    return json.loads(raw) if raw else None

def enqueue_order(order: dict):
    redis_client.rpush(ORDERS_QUEUE, order["id"])

# Routing with robust logging
async def call_paper_exec(order: dict):
    print("\n================ ROUTER ==================")
    print(">>> ENTER call_paper_exec")
    print(">>> PAPER_EXEC_URL =", PAPER_EXEC_URL)
    print(">>> ORDER ID =", order["id"])

    order["status"] = "routing"
    order["routed_at"] = time.time()
    save_order_to_redis(order)

    payload = {
        "order_id": order["id"],
        "symbol": order["symbol"],
        "legs": order["legs"],
        "qty": order["qty"],
        "limit": order.get("limit"),
        "meta": order.get("meta"),
    }
    print(">>> PAYLOAD =", payload)

    async with httpx.AsyncClient(timeout=ROUTE_TIMEOUT) as client:
        try:
            print(">>> SENDING POST →", PAPER_EXEC_URL)
            resp = await client.post(PAPER_EXEC_URL, json=payload)
            print(">>> GOT RESPONSE STATUS =", resp.status_code)
            try:
                resp_text = resp.text
            except Exception as e_text:
                resp_text = f"<could not read resp.text: {repr(e_text)}>"

            print(">>> RESPONSE TEXT START\n", resp_text, "\n>>> RESPONSE TEXT END")

            data = None
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception as je:
                    print(">>> JSON PARSE ERROR:", repr(je))
                    print(traceback.format_exc())
                    # fall through to create a useful exec_result
                    order["status"] = "route_failed"
                    order["exec_result"] = {
                        "error": "invalid_json_response",
                        "resp_text": resp_text
                    }
                    save_order_to_redis(order)
                    return

                # success path
                print(">>> PARSED JSON:", data)
                order["status"] = data.get("status", "executed")
                order["executed_at"] = time.time()
                order["exec_result"] = data
            else:
                print(">>> NON-200 RESPONSE:", resp.status_code)
                order["status"] = "route_failed"
                order["exec_result"] = {"status_code": resp.status_code, "resp_text": resp_text}

        except Exception as e:
            print(">>> EXCEPTION CAUGHT:", repr(e))
            print(traceback.format_exc())
            order["status"] = "route_error"
            order["exec_result"] = {"error": str(e)}
        finally:
            save_order_to_redis(order)
            print(">>> FINAL ORDER SAVED")
            print("===========================================\n")

# Background worker
async def background_router_loop():
    print(">>> BACKGROUND ROUTER STARTED")
    while True:
        try:
            item = redis_client.blpop(ORDERS_QUEUE, timeout=5)
            if not item:
                await asyncio.sleep(0.5)
                continue
            _, order_id = item
            order = get_order_from_redis(order_id)
            if not order:
                continue
            if order.get("status") in ("executed", "routing"):
                continue
            await call_paper_exec(order)
        except Exception as e:
            print(">>> BACKGROUND LOOP ERROR:", repr(e))
            print(traceback.format_exc())
            await asyncio.sleep(1)

# Startup (windows-safe)
@app.on_event("startup")
async def startup_event():
    try:
        redis_client.ping()
        print(">>> REDIS CONNECTED")
    except Exception as e:
        print(">>> REDIS ERROR:", e)

    async def delayed_start():
        await asyncio.sleep(0.1)
        print(">>> STARTING BACKGROUND ROUTER (WINDOWS SAFE MODE)")
        asyncio.create_task(background_router_loop())

    asyncio.create_task(delayed_start())

# Endpoints
@app.post("/createOrder")
async def create_order(req: OrderReq, background_tasks: BackgroundTasks):
    oid = str(uuid.uuid4())
    order = OrderObj(
        id=oid,
        symbol=req.symbol,
        strategy=req.strategy,
        legs=req.legs,
        qty=req.qty or 0,
        limit=req.limit,
        status="submitted",
        submitted_at=time.time(),
        meta=req.meta or {},
    ).dict()
    save_order_to_redis(order)
    enqueue_order(order)
    background_tasks.add_task(call_paper_exec, order)
    return JSONResponse(status_code=202, content={"status": "accepted", "order_id": oid})

@app.get("/order/{order_id}")
def get_order(order_id: str):
    order = get_order_from_redis(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    return order

@app.get("/orders")
def list_orders(limit: int = 50):
    keys = redis_client.lrange("orders:list", 0, limit - 1)
    return {"orders": [json.loads(redis_client.get(k)) for k in keys if redis_client.get(k)]}
