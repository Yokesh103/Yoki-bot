# app/instrument_map.py
import csv

INSTRUMENTS = {}

def load_instruments(csv_path: str):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            opt_type = row.get("OPTION_TYPE") or row.get("SEM_OPTION_TYPE")
            if opt_type not in ("CE", "PE"):
                continue

            security_id = row.get("SECURITY_ID")
            if not security_id:
                continue

            INSTRUMENTS[str(security_id)] = {
                "symbol": row.get("UNDERLYING_SYMBOL") or row.get("SYMBOL_NAME"),
                "expiry": row.get("SM_EXPIRY_DATE"),
                "strike": int(float(row.get("STRIKE_PRICE", 0))),
                "option_type": opt_type,
            }

def resolve(security_id: str):
    return INSTRUMENTS.get(str(security_id))
