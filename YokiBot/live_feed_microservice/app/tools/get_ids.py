# tools/get_ids.py
import csv
import sys
from datetime import datetime

if len(sys.argv) < 3:
    print("Usage: python tools/get_ids.py <SYMBOL> <SPOT_PRICE>")
    sys.exit(1)

UNDERLYING = sys.argv[1].upper()
SPOT = float(sys.argv[2])

CSV_FILE = "api-scrip-master-detailed.csv"

print(f"🔍 Searching {UNDERLYING} near spot {SPOT}")

rows = []

try:
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if (
                r.get("UNDERLYING_SYMBOL", "").upper() == UNDERLYING
                and r.get("OPTION_TYPE") in ("CE", "PE")
                and r.get("STRIKE_PRICE")
            ):
                rows.append(r)
except FileNotFoundError:
    print("❌ CSV not found. Run setup_daily.py first.")
    sys.exit(1)

if not rows:
    print("❌ No option rows found.")
    sys.exit(1)

# --- expiry logic ---
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
expiries = sorted({
    datetime.strptime(r["SM_EXPIRY_DATE"], "%Y-%m-%d")
    for r in rows
    if datetime.strptime(r["SM_EXPIRY_DATE"], "%Y-%m-%d") >= today
})

if not expiries:
    print("❌ No future expiries found.")
    sys.exit(1)

expiry = expiries[0].strftime("%Y-%m-%d")
print(f"📅 Selected Expiry: {expiry}")

expiry_rows = [r for r in rows if r["SM_EXPIRY_DATE"] == expiry]

strikes = sorted({float(r["STRIKE_PRICE"]) for r in expiry_rows})
atm = min(strikes, key=lambda s: abs(s - SPOT))
print(f"🎯 ATM Strike: {atm}")

ce = next(
    r["SECURITY_ID"] for r in expiry_rows
    if float(r["STRIKE_PRICE"]) == atm and r["OPTION_TYPE"] == "CE"
)

pe = next(
    r["SECURITY_ID"] for r in expiry_rows
    if float(r["STRIKE_PRICE"]) == atm and r["OPTION_TYPE"] == "PE"
)

print("\n✅ COPY THIS:")
print("------------------------------------------------")
print(f'INSTRUMENTS="NSE_FNO:{ce},NSE_FNO:{pe}"')
print("------------------------------------------------")
