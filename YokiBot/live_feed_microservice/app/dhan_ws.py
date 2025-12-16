# app/dhan_ws.py
"""
Dhan WebSocket Feed Worker (Redis-backed, production-safe)
- Safe reconnect with exponential backoff
- No WS hammering (prevents HTTP 429)
- WS is trigger-only, REST remains source of truth
"""

import os
import asyncio
import logging
import time
import json
import websockets
from urllib.parse import urlencode

from app.decoder import parse_packet
from app.instrument_map import resolve
from app.normalizer import normalize
from app.chain_builder import update_chain
from app.redis_client import redis_client

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("dhan_ws")

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

BASE_WS = "wss://api-feed.dhan.co"
WS_VERSION = "2"
AUTH_TYPE = "2"

INSTRUMENTS_ENV = os.getenv("INSTRUMENTS", "")
PING_INTERVAL = 20

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def build_ws_url() -> str:
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("Missing DHAN credentials in environment variables")

    params = {
        "version": WS_VERSION,
        "token": DHAN_ACCESS_TOKEN,
        "clientId": DHAN_CLIENT_ID,
        "authType": AUTH_TYPE,
    }
    return f"{BASE_WS}?{urlencode(params)}"


def parse_instruments():
    if not INSTRUMENTS_ENV:
        return []

    out = []
    for part in INSTRUMENTS_ENV.split(","):
        if ":" not in part:
            continue
        seg, sid = part.split(":", 1)
        out.append({"ExchangeSegment": seg, "SecurityId": sid})
    return out


def json_dumps(obj) -> str:
    """Compact JSON (Dhan WS prefers minimal payload)"""
    return json.dumps(obj, separators=(",", ":"))

# ------------------------------------------------------------------
# WS Worker
# ------------------------------------------------------------------

async def dhan_ws_worker():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("Missing DHAN credentials")

    instruments = parse_instruments()
    if not instruments:
        raise RuntimeError("INSTRUMENTS env var empty")

    url = build_ws_url()

    backoff = 30          # start safe
    max_backoff = 300     # 5 minutes cap

    logger.info("Starting Dhan WS worker")

    while True:
        try:
            logger.info(
                "Connecting to Dhan WS... (Subscribing to %d symbols)",
                len(instruments)
            )

            async with websockets.connect(
                url,
                ping_interval=PING_INTERVAL,
                max_size=10_000_000,
            ) as ws:

                # ---------------------------
                # SUBSCRIBE (QUOTE MODE)
                # ---------------------------
                await ws.send(
                    json_dumps({
                        "RequestCode": 2,   # Quote feed (stable)
                        "InstrumentCount": len(instruments),
                        "InstrumentList": instruments,
                    })
                )

                logger.info("Subscription packet sent. Listening for ticks...")
                backoff = 30  # reset after successful connect

                async for message in ws:
                    if not isinstance(message, (bytes, bytearray)):
                        continue

                    decoded = parse_packet(bytes(message))
                    secid = decoded.get("security_id")
                    if not secid:
                        continue

                    instrument = resolve(str(secid))
                    if not instrument:
                        continue

                    tick = normalize(decoded, instrument)
                    await update_chain(tick)

                    # heartbeat (used by REST / monitoring)
                    await redis_client.set(
                        "live:last_packet_ts",
                        int(time.time())
                    )

        except asyncio.CancelledError:
            logger.warning("WS worker cancelled")
            break

        except Exception as e:
            logger.warning(
                "WS error: %s | Cooling down for %ds",
                e,
                backoff
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
