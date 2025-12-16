# YokiBot/dashboard_ui.py
import streamlit as st
import requests
import pandas as pd
import time

# --- CONFIG ---
st.set_page_config(page_title="YokiBot Command Center", layout="wide")
st.title("🤖 YokiBot Live Command Center")

# API ENDPOINTS
URL_LIVE = "http://127.0.0.1:8000"
URL_CHAIN = "http://127.0.0.1:8100"
URL_GREEKS = "http://127.0.0.1:8200"
URL_SIGNAL = "http://127.0.0.1:9000"
URL_PAPER = "http://127.0.0.1:8400"

# --- SIDEBAR (System Health) ---
st.sidebar.header("System Health")

def check_health(url, name):
    try:
        r = requests.get(f"{url}/health", timeout=1)
        if r.status_code == 200:
            st.sidebar.success(f"✅ {name}")
            return True
        else:
            st.sidebar.warning(f"⚠️ {name} (Error)")
            return False
    except:
        st.sidebar.error(f"❌ {name} (Offline)")
        return False

# Check all services
s1 = check_health(URL_LIVE, "Broker Gateway") # Note: /live_status is the endpoint, health might not exist, usually /live_status
s2 = check_health(URL_CHAIN, "Option Chain")
s3 = check_health(URL_GREEKS, "Greeks Engine")
s4 = check_health(URL_SIGNAL, "Signal Brain")
s5 = check_health(URL_PAPER, "Paper Execution")

# --- MAIN DASHBOARD ---

# Row 1: Market & Ledger
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("📡 Market Feed")
    try:
        r = requests.get(f"{URL_LIVE}/live_status")
        data = r.json()
        st.metric("Last Packet TS", data.get("last_ws_packet_ts", "Waiting..."))
        st.metric("System Time", data.get("now"))
    except:
        st.write("Waiting for Feed...")

with col2:
    st.subheader("💰 Paper Ledger")
    try:
        r = requests.get(f"{URL_PAPER}/positions")
        ledger = r.json().get("ledger", {})
        st.metric("Balance", f"₹ {ledger.get('balance',0):,.2f}")
        st.metric("Total Charges", f"₹ {ledger.get('charges',0):,.2f}", delta_color="inverse")
    except:
        st.write("Ledger Offline")

with col3:
    st.subheader("🧠 Latest Decision")
    try:
        r = requests.get(f"{URL_SIGNAL}/latest_decision")
        decision = r.json()
        if decision:
             st.json(decision)
        else:
             st.info("No Signals Generated Yet")
    except:
        st.write("Brain Offline")

# Row 2: Active Positions
st.divider()
st.subheader("📋 Live Positions")
try:
    r = requests.get(f"{URL_PAPER}/positions")
    positions = r.json().get("open_positions", {})
    if positions:
        df = pd.DataFrame.from_dict(positions, orient='index')
        st.dataframe(df)
    else:
        st.info("No Open Trades")
except:
    st.write("Could not fetch positions")

# --- AUTO REFRESH ---
time.sleep(2)
st.rerun()