import os

# --- 1. CLEAN CODE FOR simulate_trade.py ---
sim_code = r'''import requests
import random
import time

URL_PAPER = "http://127.0.0.1:8400"

# Generate a random trade for testing
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
    if r.status_code == 200:
        print("✅ SUCCESS! Response:", r.json())
    else:
        print(f"⚠️ FAILED (Status {r.status_code}):", r.text)
except Exception as e:
    print(f"❌ CONNECTION ERROR: Could not connect to {URL_PAPER}")
    print(f"Make sure the Paper Bot is running on port 8400!")
    print(f"Error details: {e}")
'''

# --- 2. CLEAN CODE FOR dashboard_ui.py ---
dash_code = r'''import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(page_title="YokiBot Dashboard", layout="wide")
st.title("🤖 YokiBot Live Dashboard")

# CONFIG - API URLs
URLS = {
    "Broker": "http://127.0.0.1:8000",
    "Chain": "http://127.0.0.1:8100",
    "Greeks": "http://127.0.0.1:8200",
    "Signal": "http://127.0.0.1:9000",
    "Paper": "http://127.0.0.1:8400"
}

# --- SIDEBAR STATUS ---
st.sidebar.title("System Status")
for name, url in URLS.items():
    status_icon = "🔴"
    try:
        # Try health endpoint, fallback to live_status
        try:
            r = requests.get(f"{url}/health", timeout=0.5)
        except:
            r = requests.get(f"{url}/live_status", timeout=0.5)
            
        if r.status_code == 200:
            status_icon = "🟢"
    except:
        pass
    st.sidebar.write(f"{status_icon} {name}")

# --- MAIN METRICS ---
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("💰 Paper Ledger")
    try:
        r = requests.get(f"{URLS['Paper']}/positions", timeout=1)
        data = r.json()
        ledger = data.get("ledger", {})
        st.metric("Balance", f"₹ {ledger.get('balance', 0):,.2f}")
        st.metric("Total Charges", f"₹ {ledger.get('charges', 0):,.2f}", delta_color="inverse")
    except:
        st.error("Paper Bot Offline (Port 8400)")

with col2:
    st.subheader("🧠 Latest Signal")
    try:
        r = requests.get(f"{URLS['Signal']}/latest_decision", timeout=1)
        st.json(r.json())
    except:
        st.info("Signal Engine Offline")

with col3:
    st.subheader("📡 Feed Status")
    try:
        r = requests.get(f"{URLS['Broker']}/live_status", timeout=1)
        data = r.json()
        st.write(f"Last Tick: {data.get('last_ws_packet_ts')}")
    except:
        st.write("Broker Offline")

# --- POSITIONS TABLE ---
st.divider()
st.subheader("📋 Active Trades")
try:
    r = requests.get(f"{URLS['Paper']}/positions", timeout=1)
    pos = r.json().get("open_positions", {})
    if pos:
        df = pd.DataFrame.from_dict(pos, orient='index')
        st.dataframe(df)
    else:
        st.info("No open positions.")
except:
    st.write("Cannot fetch positions.")

# Auto-refresh every 2 seconds
time.sleep(2)
st.rerun()
'''

print("Repairing files...")
with open("simulate_trade.py", "w", encoding="utf-8") as f:
    f.write(sim_code)
print(" - simulate_trade.py [FIXED]")

with open("dashboard_ui.py", "w", encoding="utf-8") as f:
    f.write(dash_code)
print(" - dashboard_ui.py [FIXED]")

print("\n✅ DONE! You can now run the commands below.")
