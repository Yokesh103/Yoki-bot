# live_feed_service.py
"""
Dhan Live Feed Microservice (cleaned)
- Raw binary packets -> Kafka (AIOKafkaProducer)
- FastAPI endpoints for status / subscribe / data
- WS worker listens to Dhan, stores raw hex and minimal parsed fields
- Use environment variables to configure token, kafka bootstrap, instruments
"""

import os
import asyncio
import json
import logging
import struct
import time
from typing import Dict, Any, List, Optional

import websockets
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from urllib.parse import urlencode
from aiokafka import AIOKafkaProducer

# ---------------------------- CONFIG & LOGGING -----------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("live_feed_service")

# Dhan / WS config
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1109405279")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
WS_VERSION = os.getenv("WS_VERSION", "2")
AUTH_TYPE = os.getenv("AUTH_TYPE", "2")
BASE_WS = os.getenv("BASE_WS", "wss://api-feed.dhan.co")
WS_MAX_SIZE = int(os.getenv("WS_MAX_SIZE", "10000000"))

# Instruments env: e.g. "NSE_EQ:1333,NSE_FNO:52296"
INSTRUMENTS_ENV = os.getenv("INSTRUMENTS", "NSE_EQ:1333")

# Kafka config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dhan_ticks")

# Behavior
MAX_STORE = int(os.getenv("MAX_STORE", "50"))
INITIAL_BACKOFF = float(os.getenv("INITIAL_BACKOFF", "1"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "30"))

# ------------------------------- STATE -----------------------------------

app = FastAPI(title="Dhan Live Feed Microservice")

WS_STATE = {
    "connected": False,
    "last_error": None,
    "last_connect_time": None,
    "packets_received": 0,
}

# store recent parsed packets keyed by security id
PACKETS: Dict[str, List[Dict[str, Any]]] = {}
SUBSCRIBE_INSTRUMENTS: List[Dict[str, str]] = []

# Async primitives for inter-task comms
kafka_producer: Optional[AIOKafkaProducer] = None
ws_command_queue: "asyncio.Queue[dict]" = asyncio.Queue()  # commands to WS worker

# --------------------------- Helper functions -----------------------------

def parse_instruments(env: str) -> List[Dict[str, str]]:
    out = []
    for part in env.split(","):
        p = part.strip()
        if not p:
            continue
        if ":" not in p:
            logger.warning("Ignoring invalid instrument format: %s", p)
            continue
        seg, sid = p.split(":", 1)
        out.append({"ExchangeSegment": seg.strip(), "SecurityId": sid.strip()})
    return out

def ws_url(token: str, client_id: str) -> str:
    params = {"version": WS_VERSION, "token": token, "clientId": client_id, "authType": AUTH_TYPE}
    return f"{BASE_WS}?{urlencode(params)}"

def safe_float(buf: bytes, offset: int):
    try:
        return float(struct.unpack_from("<f", buf, offset)[0])
    except Exception:
        return None

def safe_uint32(buf: bytes, offset: int):
    try:
        return int(struct.unpack_from("<I", buf, offset)[0])
    except Exception:
        return None

def store_packet(secid: str, raw: bytes, parsed: dict):
    rec = {
        "received_at": time.time(),
        "raw_hex": raw.hex(),
        "length": len(raw),
        "parsed": parsed,
    }
    lst = PACKETS.setdefault(secid, [])
    lst.insert(0, rec)
    if len(lst) > MAX_STORE:
        del lst[MAX_STORE:]

# -------------------------- Kafka (AIO) functions -------------------------

async def start_kafka():
    """Start AIOKafkaProducer and set global variable."""
    global kafka_producer
    if kafka_producer is not None:
        return

    logger.info("Starting AIOKafkaProducer -> %s", KAFKA_BOOTSTRAP)
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        await kafka_producer.start()
        logger.info("Kafka producer READY.")
    except Exception as e:
        logger.exception("Failed to start Kafka producer: %s", e)
        kafka_producer = None

async def stop_kafka():
    global kafka_producer
    if kafka_producer:
        try:
            await kafka_producer.stop()
            logger.info("Kafka producer stopped.")
        except Exception:
            logger.exception("Error stopping kafka producer")
        finally:
            kafka_producer = None

async def publish_raw_bytes(secid: str, raw: bytes):
    """
    Publish the raw bytes to Kafka.
    Key: secid (utf-8 bytes) - helpful for partitioning by security
    Value: raw (bytes)
    """
    global kafka_producer
    if kafka_producer is None:
        logger.debug("Kafka producer not ready; skipping publish")
        return
    try:
        # no serializer; pass bytes directly
        await kafka_producer.send_and_wait(KAFKA_TOPIC, raw, key=secid.encode("utf-8"))
    except Exception as e:
        logger.error("Kafka publish error: %s", e)

# ---------------------------- WebSocket worker ----------------------------

async def dhan_ws_worker():
    """
    Persistent worker connecting to Dhan WS, subscribing to instruments,
    storing packets and publishing raw bytes to Kafka.
    It also listens for commands on ws_command_queue:
      - {"cmd": "resubscribe", "instruments": [...]}
    """
    global SUBSCRIBE_INSTRUMENTS, WS_STATE

    if not DHAN_ACCESS_TOKEN:
        logger.error("❌ DHAN_ACCESS_TOKEN is not set. Set DHAN_ACCESS_TOKEN env var.")
        return

    SUBSCRIBE_INSTRUMENTS = parse_instruments(INSTRUMENTS_ENV)
    if not SUBSCRIBE_INSTRUMENTS:
        logger.error("No instruments to subscribe. Set INSTRUMENTS env var.")
        return

    url = ws_url(DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID)
    backoff = INITIAL_BACKOFF

    while True:
        try:
            logger.info("Connecting to Dhan WS -> %s", url)
            async with websockets.connect(url, max_size=WS_MAX_SIZE) as ws:
                WS_STATE["connected"] = True
                WS_STATE["last_connect_time"] = time.time()
                WS_STATE["last_error"] = None
                logger.info("✅ Dhan WS connected.")

                # send initial subscription (Full Packet: 21)
                async def send_subscribe(instruments: List[Dict[str,str]]):
                    payload = {"RequestCode": 21, "InstrumentCount": len(instruments), "InstrumentList": instruments}
                    await ws.send(json.dumps(payload))
                    logger.info("📤 Sent subscription (count=%d)", len(instruments))

                await send_subscribe(SUBSCRIBE_INSTRUMENTS)
                backoff = INITIAL_BACKOFF

                # create a background task to drain commands (resubscribe)
                async def cmd_drain():
                    while True:
                        cmd = await ws_command_queue.get()
                        try:
                            if isinstance(cmd, dict) and cmd.get("cmd") == "resubscribe":
                                instruments = cmd.get("instruments") or SUBSCRIBE_INSTRUMENTS
                                SUBSCRIBE_INSTRUMENTS = instruments
                                # clear stored packets to avoid mixing old data
                                PACKETS.clear()
                                await send_subscribe(SUBSCRIBE_INSTRUMENTS)
                                WS_STATE["last_error"] = "manual_resubscribe_trigger"
                                logger.info("🔁 Resubscribe command processed.")
                        except Exception:
                            logger.exception("Error processing ws command")
                        finally:
                            ws_command_queue.task_done()

                cmd_task = asyncio.create_task(cmd_drain())

                # read loop
                ids_set = {int(i["SecurityId"]) for i in SUBSCRIBE_INSTRUMENTS}
                async for msg in ws:
                    # handle text handshake/status messages
                    if isinstance(msg, str):
                        # optional: you can parse JSON handshake or ignore
                        # keep connected state, increase packet count if wanted
                        continue

                    raw = bytes(msg)
                    parsed = {"length": len(raw)}

                    # best-effort ltp and ts extraction
                    ltp = safe_float(raw, 8)
                    if ltp is not None:
                        parsed["ltp"] = ltp

                    ts = safe_uint32(raw, 12)
                    if ts and ts > 1_000_000_000:
                        parsed["timestamp"] = ts

                    # find security id inside first 80 bytes (heuristic)
                    secid: Optional[str] = None
                    for off in range(0, min(80, len(raw) - 4)):
                        v = safe_uint32(raw, off)
                        if v in ids_set:
                            secid = str(v)
                            parsed["secid"] = secid
                            break

                    if not secid:
                        # fallback to first instrument id
                        secid = SUBSCRIBE_INSTRUMENTS[0]["SecurityId"]

                    # store & counters
                    store_packet(secid, raw, parsed)
                    WS_STATE["packets_received"] += 1

                    # publish raw bytes to kafka
                    await publish_raw_bytes(secid, raw)

                # if loop exits, cancel cmd_task
                cmd_task.cancel()
                try:
                    await cmd_task
                except asyncio.CancelledError:
                    pass

        except websockets.exceptions.ConnectionClosed as e:
            WS_STATE["connected"] = False
            WS_STATE["last_error"] = f"connection_closed:{e.code}"
            logger.warning("WS closed: %s", e)
        except Exception as e:
            WS_STATE["connected"] = False
            WS_STATE["last_error"] = str(e)
            logger.exception("WS worker error: %s", e)

        # backoff before reconnect
        logger.info("Reconnecting WS in %.1f seconds...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(MAX_BACKOFF, backoff * 2)

# ----------------------------- FastAPI endpoints --------------------------

class SubscribeRequest(BaseModel):
    instruments: List[Dict[str, str]]

@app.on_event("startup")
async def startup():
    # start kafka and ws worker tasks
    # we intentionally create tasks, not await them, so uvicorn continues startup
    asyncio.create_task(start_kafka())
    asyncio.create_task(dhan_ws_worker())
    logger.info("Service startup complete. Background tasks created.")

@app.on_event("shutdown")
async def shutdown():
    # attempt clean shutdown
    await stop_kafka()
    logger.info("Service shutdown complete.")

@app.get("/health")
async def health():
    return {
        "ok": True,
        "connected": WS_STATE["connected"],
        "kafka_ready": kafka_producer is not None,
    }

@app.get("/status")
async def status():
    return {
        "connected": WS_STATE["connected"],
        "last_error": WS_STATE["last_error"],
        "last_connect_time": WS_STATE["last_connect_time"],
        "packets_received": WS_STATE["packets_received"],
        "subscribed_instruments": SUBSCRIBE_INSTRUMENTS,
    }

@app.get("/data/{security_id}")
async def get_data(security_id: str):
    if security_id not in PACKETS:
        raise HTTPException(status_code=404, detail="No data for this security id")
    return PACKETS[security_id][0]

@app.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    """
    Update subscription list. This enqueues a resubscribe command processed by the WS worker.
    """
    if not req.instruments:
        raise HTTPException(status_code=400, detail="Empty instruments list")

    # validate instruments format quickly
    for idx, itm in enumerate(req.instruments):
        if "ExchangeSegment" not in itm or "SecurityId" not in itm:
            raise HTTPException(status_code=400, detail=f"Invalid instrument at index {idx}")

    # enqueue resubscribe command
    await ws_command_queue.put({"cmd": "resubscribe", "instruments": req.instruments})
    logger.info("Enqueued resubscribe command (count=%d).", len(req.instruments))

    return {"ok": True, "subscribed": req.instruments}

# ------------------------------- End file ---------------------------------
