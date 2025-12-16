# tools/get_ids.py
import csv
import sys
from datetime import datetime

if len(sys.argv) < 3:
    print("Usage: python tools/get_ids.py <SYMBOL> <SPOT_PRICE>")
    sys.exit(1)

SYMBOL = sys.argv[1].upper()
SPOT = float(sys.argv[2])

CSV_FILE = "api-scrip-master-detailed.csv"

rows = []
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        if (
            row.get("UNDERLYING_SYMBOL") == SYMBOL
            and row.get("OPTION_TYPE") in ("CE", "PE")
            and row.get("STRIKE_PRICE")
        ):
            rows.append(row)

if not rows:
    print("❌ No options found")
    sys.exit(1)

today = datetime.utcnow().date()
expiries = sorted({
    datetime.strptime(r["SM_EXPIRY_DATE"], "%Y-%m-%d").date()
    for r in rows
    if datetime.strptime(r["SM_EXPIRY_DATE"], "%Y-%m-%d").date() >= today
})

expiry = expiries[0]
expiry_rows = [r for r in rows if r["SM_EXPIRY_DATE"] == expiry.strftime("%Y-%m-%d")]

strikes = sorted({float(r["STRIKE_PRICE"]) for r in expiry_rows})
atm = min(strikes, key=lambda x: abs(x - SPOT))

ce = pe = None
for r in expiry_rows:
    if float(r["STRIKE_PRICE"]) == atm:
        if r["OPTION_TYPE"] == "CE":
            ce = r["SECURITY_ID"]
        elif r["OPTION_TYPE"] == "PE":
            pe = r["SECURITY_ID"]

if not ce or not pe:
    print("❌ CE/PE pair not found")
    sys.exit(1)

print("\n✅ COPY BELOW:")
print(f'INSTRUMENTS="NSE_FNO:{ce},NSE_FNO:{pe}"')
# tools/get_ids.py
import csv
import sys
from datetime import datetime

if len(sys.argv) < 3:
    print("Usage: python tools/get_ids.py <SYMBOL> <SPOT_PRICE>")
    sys.exit(1)

SYMBOL = sys.argv[1].upper()
SPOT = float(sys.argv[2])

CSV_FILE = "api-scrip-master-detailed.csv"

rows = []
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        if (
            row.get("UNDERLYING_SYMBOL") == SYMBOL
            and row.get("OPTION_TYPE") in ("CE", "PE")
            and row.get("STRIKE_PRICE")
        ):
            rows.append(row)

if not rows:
    print("❌ No options found")
    sys.exit(1)

today = datetime.utcnow().date()
expiries = sorted({
    datetime.strptime(r["SM_EXPIRY_DATE"], "%Y-%m-%d").date()
    for r in rows
    if datetime.strptime(r["SM_EXPIRY_DATE"], "%Y-%m-%d").date() >= today
})

expiry = expiries[0]
expiry_rows = [r for r in rows if r["SM_EXPIRY_DATE"] == expiry.strftime("%Y-%m-%d")]

strikes = sorted({float(r["STRIKE_PRICE"]) for r in expiry_rows})
atm = min(strikes, key=lambda x: abs(x - SPOT))

ce = pe = None
for r in expiry_rows:
    if float(r["STRIKE_PRICE"]) == atm:
        if r["OPTION_TYPE"] == "CE":
            ce = r["SECURITY_ID"]
        elif r["OPTION_TYPE"] == "PE":
            pe = r["SECURITY_ID"]

if not ce or not pe:
    print("❌ CE/PE pair not found")
    sys.exit(1)

print("\n✅ COPY BELOW:")
print(f'INSTRUMENTS="NSE_FNO:{ce},NSE_FNO:{pe}"')
