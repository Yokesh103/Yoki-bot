import pandas as pd
import requests
import json

# ============================================================
# 1. HARD-CODED CREDENTIALS (DEBUG ONLY)
# ============================================================
CLIENT_ID = "1109405279"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY1OTk2ODk4LCJpYXQiOjE3NjU5MTA0OTgsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5NDA1Mjc5In0.du2Jv7m7m3idES-jL1oj3tkYmxMtwTG-7ZJ5NrwDuVEJUpmDXD1l2xqwOYlvKjxziGjI6swRcFIus_VyJqGH-Q"

print("🔹 Using HARD-CODED credentials")
print(f"   Client ID: {CLIENT_ID}")
print(f"   Token: {ACCESS_TOKEN[:10]}... (hidden)")

# ============================================================
# 2. LOAD CSV & FIND NIFTY INDEX
# ============================================================
csv_path = "live_feed_microservice/api-scrip-master-detailed.csv"
print(f"\n🔹 Loading Scrip Master: {csv_path}")

try:
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = df.columns.str.strip()
except Exception as e:
    print(f"❌ CSV LOAD FAILED: {e}")
    exit(1)

index_df = df[
    (df["SEM_EXM_EXCH_ID"] == "NSE") &
    (df["SEM_SEGMENT"] == "I")  # I = INDEX
]

print(f"Found {len(index_df)} NSE INDEX instruments")

candidates = index_df[
    index_df["SEM_TRADING_SYMBOL"].str.contains("NIFTY", case=False, na=False)
]

if candidates.empty:
    print("❌ NIFTY NOT FOUND IN CSV")
    print(index_df[["SEM_TRADING_SYMBOL", "SEM_SMST_SECURITY_ID"]].head(10))
    security_id = "13"  # known fallback for NIFTY index
    print("⚠️ Using fallback Security ID: 13")
else:
    row = candidates.iloc[0]
    security_id = str(row["SEM_SMST_SECURITY_ID"])
    print("✅ NIFTY FOUND")
    print(f"   Symbol     : {row['SEM_TRADING_SYMBOL']}")
    print(f"   Security ID: {security_id}")

# ============================================================
# 3. VERIFY TOKEN VIA RELIANCE (REST – RELIABLE)
# ============================================================
print("\n🔹 Verifying token using RELIANCE EQ (REST)")

url = "https://api.dhan.co/v2/marketfeed/ltp"
headers = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "content-type": "application/json",
    "accept": "application/json",
}

payload = {
    "NSE": ["2885"]  # RELIANCE EQ
}

try:
    r = requests.post(url, headers=headers, json=payload, timeout=5)
except Exception as e:
    print(f"❌ NETWORK ERROR: {e}")
    exit(1)

print(f"HTTP STATUS: {r.status_code}")
print(json.dumps(r.json(), indent=2))

if r.status_code == 200:
    print("\n✅ Token is VALID (HTTP 200)")
    if not r.json().get("data"):
        print("⚠️ REST returned empty data (EXPECTED for INDEX)")
        print("👉 Use WebSocket feed for NIFTY index")
else:
    print("\n❌ Token INVALID or EXPIRED")

