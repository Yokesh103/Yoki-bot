# live_feed_ws_publisher.py
"""
Connect to Dhan WebSocket (tick-by-tick), parse binary packets (skeleton),
publish normalized JSON messages to Kafka topic 'marketfeed'.

If environment variable USE_REST_FALLBACK=1 or ACCESS_TOKEN missing, it runs safe REST poller (rate-limited).
"""

import os
import asyncio
import json
import struct
import time
import httpx
import websockets
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "marketfeed")
CORE_API = os.getenv("CORE_API", "http://127.0.0.1:8000")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
USE_REST_FALLBACK = os.getenv("USE_REST_FALLBACK", "0") == "1"
SUBSCRIPTIONS = os.getenv("SUBSCRIPTIONS", "")  # comma-separated security IDs for REST fallback

WS_ENDPOINT_TEMPLATE = "wss://api-feed.dhan.co?version=2&token={token}&clientId={clientId}&authType=2"

producer = None

async def start_producer():
    global producer
    producer = None

async def start_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()

    await producer.start()

async def stop_producer():
    global producer
    if producer:
        await producer.stop()


# ---- Binary parsing helpers (skeleton based on provided spec) ----
def parse_response_header(header_bytes: bytes):
    # header 8 bytes: [feed_code(1)][msg_len(2)][exchange_seg(1)][security_id(4)]
    if len(header_bytes) < 8:
        return None
    feed_code = header_bytes[0]
    msg_len = int.from_bytes(header_bytes[1:3], 'little')
    exch_seg = header_bytes[3]
    sec_id = int.from_bytes(header_bytes[4:8], 'little')
    return {"feed_code": feed_code, "msg_len": msg_len, "exch_seg": exch_seg, "sec_id": sec_id}


def decode_ticker_payload(payload: bytes):
    # offset 0: float32 LTP; 4: int32 epoch time
    try:
        last_price = struct.unpack_from('<f', payload, 0)[0]
        last_trade_time = int.from_bytes(payload[4:8], 'little')
        return {"last_price": last_price, "last_trade_time": last_trade_time}
    except Exception:
        return {"raw_hex": payload.hex()}


def decode_quote_payload(payload: bytes):
    # Best-effort decode some fields (LTP, last_qty, ltt, ATP, volume)
    try:
        last_price = struct.unpack_from('<f', payload, 0)[0]
        last_qty = int.from_bytes(payload[4:6], 'little')
        ltt = int.from_bytes(payload[6:10], 'little')
        atp = struct.unpack_from('<f', payload, 10)[0] if len(payload) >= 14 else None
        volume = int.from_bytes(payload[14:18], 'little') if len(payload) >= 18 else None
        return {"last_price": last_price, "last_qty": last_qty, "last_trade_time": ltt, "atp": atp, "volume": volume}
    except Exception:
        return {"raw_hex": payload.hex()}


async def handle_ws_messages(ws_uri):
    async with websockets.connect(ws_uri, max_size=None) as ws:
        print("Connected to Dhan WS:", ws_uri)
        while True:
            data = await ws.recv()
            if isinstance(data, bytes):
                if len(data) < 8:
                    continue
                header = parse_response_header(data[:8])
                if header and header["msg_len"] <= len(data):
    payload = data[8:header["msg_len"]]
else:
    payload = data[8:]

                msg = {"header": header, "recv_ts": int(time.time() * 1000)}
                if header:
                    fc = header["feed_code"]
                    if fc == 2:
                        msg["payload"] = decode_ticker_payload(payload)
                    elif fc in (4, 8):
                        msg["payload"] = decode_quote_payload(payload)
                    elif fc == 50:
                        # disconnect
                        code = int.from_bytes(payload[:2], 'little') if payload and len(payload) >= 2 else None
                        msg["payload"] = {"disconnect_code": code}
                    else:
                        msg["payload"] = {"raw_hex": payload.hex() if payload else None, "feed_code": fc}
                else:
                    msg["payload"] = {"raw_hex": payload.hex() if payload else None}
                await producer.send_and_wait(KAFKA_TOPIC, json.dumps(msg).encode("utf-8"))
            else:
                # text message
                try:
                    j = json.loads(data)
                    await producer.send_and_wait(KAFKA_TOPIC, json.dumps({"text": j, "recv_ts": int(time.time()*1000)}).encode("utf-8"))
                except Exception:
                    await producer.send_and_wait(KAFKA_TOPIC, json.dumps({"text": str(data), "recv_ts": int(time.time()*1000)}).encode("utf-8"))


# ---- REST poller fallback (safe, rate-limited) ----
MAX_BATCH = 800
SAFE_WAIT = 2.2  # seconds

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

async def safe_rest_poller():
    if not SUBSCRIPTIONS:
        print("SUBSCRIPTIONS env var empty. No fallback polling will run.")
        return
    sec_ids = [int(x.strip()) for x in SUBSCRIPTIONS.split(",") if x.strip()]
    if not sec_ids:
        print("No valid security ids in SUBSCRIPTIONS.")
        return
    async with httpx.AsyncClient(timeout=25) as c:
        batches = list(chunk(sec_ids, MAX_BATCH))
        while True:
            for b in batches:
                payload = {"root": {"NSE_EQ": b}}
                try:
                    r = await c.post(f"{CORE_API}/ltp", json=payload, timeout=20)
                    r.raise_for_status()
                    msg = {"ts": int(time.time()*1000), "batch": b, "raw": r.json()}
                    await producer.send_and_wait(KAFKA_TOPIC, json.dumps(msg).encode("utf-8"))
                except Exception as e:
                    print("poll error:", e)
                await asyncio.sleep(SAFE_WAIT)


async def main():
    await start_producer()
    try:
        if USE_REST_FALLBACK or not ACCESS_TOKEN or not CLIENT_ID:
            print("Running safe REST poller (USE_REST_FALLBACK set or missing token).")
            await safe_rest_poller()
            return
        ws_uri = WS_ENDPOINT_TEMPLATE.format(token=ACCESS_TOKEN, clientId=CLIENT_ID)
        await handle_ws_messages(ws_uri)
    finally:
        await stop_producer()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("exiting")
