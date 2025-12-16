import requests
import random

# Mocking a decision coming from Signal Engine -> to Paper Bot
# Normally Signal Engine calls this, but we are manually triggering it.

URL_PAPER = "http://127.0.0.1:8400"

payload = {
    "symbol": "NIFTY 24100 CE",
    "qty": 50,
    "side": "BUY",
    "price": random.uniform(145.0, 155.0),
    "tag": "MANUAL_TEST"
}

print(f"🚀 Sending Order: {payload['symbol']} @ {payload['price']:.2f}")

try:
    r = requests.post(f"{URL_PAPER}/place_order", json=payload)
    print("Response:", r.json())
except Exception as e:
    print("Failed:", e)