import streamlit as st
import requests
import pandas as pd
import time
import random

# --- PAGE CONFIG ---
st.set_page_config(page_title="YokiBot Control", layout="wide")
st.title("🤖 YokiBot Production Dashboard")

# --- API CONFIG ---
URLS = {
    "Broker": "http://127.0.0.1:8000",
    "Chain": "http://127.0.0.1:8100",
    "Greeks": "http://127.0.0.1:8200",
    "Signal": "http://127.0.0.1:9000",
    "Paper": "http://127.0.0.1:8400"
}

# --- SIDEBAR: HEALTH CHECK ---
st.sidebar.header("System Health")

def check_service(name, url, endpoint="/health"):
    try:
        if "Broker" in name: endpoint = "/live_status" # Broker uses /live_status
        r = requests.get(f"{url}{endpoint}", timeout=0.5)
        if r.status_code == 200:
            st.sidebar.success(f"🟢 {name}")
            return True
        else:
            st.sidebar.warning(f"🟠 {name} (Error {r.status_code})")
            return False
    except:
        st.sidebar.error(f"🔴 {name} (Offline)")
        return False

for name, url in URLS.items():
    check_service(name, url)

# --- FAKE LTP GENERATOR (for market closed) ---
# This simulates price movement around the entry price.
# In live market, this would come from the Broker feed.
def get_simulated_ltp(symbol, entry_price):
    # Use symbol as seed for consistent but slightly random changes
    random.seed(symbol + str(int(time.time() / 10))) # Change every 10 seconds
    deviation = random.uniform(-0.02, 0.02) * entry_price # +/- 2% deviation
    return round(entry_price + deviation, 2)


# --- MAIN LAYOUT ---
col1, col2, col3 = st.columns(3)

# 1. ACCOUNT / LEDGER
with col1:
    st.subheader("💰 Paper Account")
    try:
        r = requests.get(f"{URLS['Paper']}/positions", timeout=1)
        if r.status_code == 200:
            data = r.json().get("ledger", {})
            st.metric("Available Balance", f"₹ {data.get('balance', 0):,.2f}")
            st.metric("Total Tax/Charges", f"₹ {data.get('charges', 0):,.2f}", delta_color="inverse")
            st.metric("Realized PnL", f"₹ {data.get('realized_pnl', 0):,.2f}")
        else:
            st.error("Service Error")
    except:
        st.write("Connecting to Paper Bot...")

# 2. SIGNAL BRAIN (Condensed)
with col2:
    st.subheader("🧠 Latest Logic")
    try:
        r = requests.get(f"{URLS['Signal']}/latest_decision", timeout=1)
        if r.status_code == 200:
            decision = r.json()
            if decision.get("decision_id") and decision.get("decision_id") != "NA":
                st.markdown(f"**Action:** `{decision.get('action')}`")
                st.markdown(f"**Strategy:** `{decision.get('strategy')}`")
                st.markdown(f"**Reason:** `{decision.get('reason')}`")
                st.markdown(f"**ID:** `{decision.get('decision_id')}`")
            else:
                st.info("No Active Signal")
        else:
            st.write("No Signal Data")
    except:
        st.write("Connecting to Signal Engine...")

# 3. MARKET FEED (Condensed)
with col3:
    st.subheader("📡 Market Status")
    try:
        r = requests.get(f"{URLS['Broker']}/live_status", timeout=1)
        if r.status_code == 200:
            data = r.json()
            ts = data.get('last_ws_packet_ts')
            st.write(f"**Last Tick (WS):** `{ts if ts else 'Waiting...'}`")
            st.write(f"**Server Time (REST):** `{data.get('now')}`")
        else:
            st.write("Broker Error")
    except:
        st.write("Connecting to Broker Gateway...")

# --- POSITIONS TABLE ---
st.divider()
st.subheader("📋 Open Positions")

try:
    r = requests.get(f"{URLS['Paper']}/positions", timeout=1)
    if r.status_code == 200:
        positions_raw = r.json().get("open_positions", {})
        
        if positions_raw:
            # Process each position for display
            display_data = []
            for order_id, pos in positions_raw.items():
                instrument = pos.get("Instrument", "N/A")
                entry_price = pos.get("Entry Price", 0.0)
                qty = pos.get("Qty", 0)
                charges = pos.get("Charges", 0.0)
                
                ltp = get_simulated_ltp(instrument, entry_price)
                
                entry_value = entry_price * qty
                current_value = ltp * qty
                unrealized_pnl = current_value - entry_value - charges

                display_data.append({
                    "Order ID": order_id,
                    "Instrument": instrument,
                    "Side": pos.get("Side", ""),
                    "Qty": qty,
                    "Entry Price": f"₹ {entry_price:,.2f}",
                    "LTP": f"₹ {ltp:,.2f}",
                    "Current Value": f"₹ {current_value:,.2f}",
                    "Unrealized P&L": f"₹ {unrealized_pnl:,.2f}",
                    "Entry Time": pos.get("Entry Time", ""),
                    "Charges": f"₹ {charges:,.2f}"
                })
            
            df_display = pd.DataFrame(display_data)
            
            # Use Streamlit's data_editor for better presentation and potential future interaction
            st.data_editor(
                df_display,
                column_config={
                    "Unrealized P&L": st.column_config.NumberColumn(
                        format="%.2f",
                        help="Unrealized Profit/Loss",
                        # You can add a conditional style here if needed for positive/negative P&L
                    )
                },
                hide_index=True,
                use_container_width=True,
                disabled=True # Not editable
            )

        else:
            st.info("No Active Trades. Waiting for signals...")
    else:
        st.error("Could not fetch positions.")
except Exception as e:
    st.error(f"Error fetching positions: {e}")

# --- AUTO REFRESH ---
time.sleep(1) # Refresh every 1 second
st.rerun()