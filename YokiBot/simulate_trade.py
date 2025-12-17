import requests
import random
import sys

# URL of your Paper Execution Service (Port 8400)
URL_PAPER = "http://127.0.0.1:8400"

print(f"--- 🚀 STARTING TRADE SIMULATION ---")

# 1. Generate random trade details
price = round(random.uniform(145.0, 155.0), 2)
payload = {
    "symbol": "NIFTY 24100 CE",
    "qty": 50,
    "side": "BUY",
    "price": price,
    "tag": "MANUAL_SIMULATION"
}

print(f"📡 Sending Order: {payload['symbol']} | Qty: {payload['qty']} | Price: {payload['price']}")

# 2. Send to Paper Bot
try:
    response = requests.post(f"{URL_PAPER}/place_order", json=payload, timeout=2)
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ SUCCESS! Order Filled.")
        print(f"   🆔 Order ID: {data.get('order_id')}")
        print(f"   💸 Charges Deducted: ₹ {data.get('charges_deducted')}")
        print(f"   📉 Filled Price: {data.get('filled_price')}")
    else:
        print(f"\n⚠️ FAILED. Server returned status: {response.status_code}")
        print(f"   Response: {response.text}")

except requests.exceptions.ConnectionError:
    print(f"\n❌ CONNECTION REFUSED")
    print(f"   The Paper Bot is NOT running on {URL_PAPER}")
    print(f"   👉 Run this in a new terminal: cd paper-exec && uvicorn main:app --port 8400")

except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")

print(f"------------------------------------")