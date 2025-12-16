import csv
import sys
from collections import defaultdict
from datetime import datetime

CSV_PATH = "api-scrip-master-detailed.csv"

UNDERLYING = sys.argv[1] if len(sys.argv) > 1 else "BANKNIFTY"
SPOT = float(sys.argv[2]) if len(sys.argv) > 2 else None

if SPOT is None:
    print("ERROR: Spot price required")
    sys.exit(1)

rows = []
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        if (
            r.get("UNDERLYING_SYMBOL") == UNDERLYING
            and r.get("OPTION_TYPE") in ("CE", "PE")
            and r.get("STRIKE_PRICE")
        ):
            rows.append(r)

if not rows:
    print("ERROR: No option rows found")
    sys.exit(1)

# ---- nearest expiry ----
def parse_date(x):
    try:
        return datetime.strptime(x, "%Y-%m-%d")
    except Exception:
        return None

today = datetime.utcnow()
expiries = sorted(
    {parse_date(r["SM_EXPIRY_DATE"]) for r in rows if parse_date(r["SM_EXPIRY_DATE"])},
    key=lambda d: abs((d - today).days)
)

expiry = expiries[0].strftime("%Y-%m-%d") 

# ---- ATM strike ----
strikes = sorted(
    {float(r["STRIKE_PRICE"]) for r in rows if r["SM_EXPIRY_DATE"] == expiry}
)
atm = min(strikes, key=lambda s: abs(s - SPOT)) 

# ---- CE / PE IDs ----
ce = pe = None
for r in rows:
    if (
        r["SM_EXPIRY_DATE"] == expiry
        and float(r["STRIKE_PRICE"]) == atm
    ):
        if r["OPTION_TYPE"] == "CE":
            ce = r["SECURITY_ID"]
        elif r["OPTION_TYPE"] == "PE":
            pe = r["SECURITY_ID"]

if not ce or not pe:
    print("ERROR: CE/PE pair not found")
    sys.exit(1)

print(f"UNDERLYING={UNDERLYING}")
print(f"EXPIRY={expiry}")
print(f"ATM_STRIKE={atm}")
print(f"INSTRUMENTS=NSE_FNO:{ce},NSE_FNO:{pe}")
