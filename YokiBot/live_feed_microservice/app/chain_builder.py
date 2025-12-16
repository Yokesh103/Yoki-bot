# app/chain_builder.py
import json
import time
from collections import defaultdict
from app.redis_client import redis_client
from app.settings import SYMBOL, EXPIRY, SNAPSHOT_TTL

_chain = defaultdict(lambda: {"CE": None, "PE": None})

async def update_chain(tick: dict):
    strike = str(tick["strike"])
    _chain[strike][tick["option_type"]] = { # pyright: ignore[reportArgumentType]
        "ltp": tick["ltp"],
        "oi": tick["oi"],
        "volume": tick["volume"]
    }

    if not _is_valid_chain():
        return

    snapshot = {
        "symbol": SYMBOL,
        "expiry": EXPIRY,
        "timestamp": int(time.time()),
        "strikes": _chain
    }

    key = f"optionchain:{SYMBOL}:{EXPIRY}"
    last_good = f"optionchain:last_good:{SYMBOL}:{EXPIRY}"

    await redis_client.setex(key, SNAPSHOT_TTL, json.dumps(snapshot))
    await redis_client.set(last_good, json.dumps(snapshot))


def _is_valid_chain():
    complete = [
        s for s in _chain.values()
        if s["CE"] and s["PE"]
    ]
    return len(complete) >= 10  # minimum depth
