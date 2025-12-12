import sqlite3
import csv
import gzip
import requests
from pathlib import Path

DB_PATH = Path("data/options.db")
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"


def get_conn():
    DB_PATH.parent.mkdir(exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_instruments_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS instruments (
        instrument_key TEXT PRIMARY KEY,
        underlying TEXT,
        expiry TEXT,
        strike REAL,
        opt_type TEXT
    )
    """)
    conn.commit()
    conn.close()


def download_and_parse_csv():
    print("Downloading Upstox instrument master...")
    resp = requests.get(INSTRUMENTS_URL, timeout=60)
    resp.raise_for_status()

    gz_path = Path("data/instruments.csv.gz")

    with open(gz_path, "wb") as f:
        f.write(resp.content)

    print("Parsing CSV...")

    rows = []

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row.get("exchange") != "NSE_FO":
                continue

            if row.get("instrument_type") != "OPTIDX":
                continue

            if row.get("name") not in ("NIFTY", "BANKNIFTY"):
                continue

            rows.append(row)

    return rows



def load_instruments():
    init_instruments_table()
    rows = download_and_parse_csv()

    conn = get_conn()
    cur = conn.cursor()

    inserted = 0

    for r in rows:
        instrument_key = r["instrument_key"]
        underlying = r["name"]
        expiry = r["expiry"]
        strike = float(r["strike"])
        opt_type = r["option_type"]

        cur.execute("""
            INSERT OR IGNORE INTO instruments
            (instrument_key, underlying, expiry, strike, opt_type)
            VALUES (?, ?, ?, ?, ?)
        """, (
            instrument_key,
            underlying,
            expiry,
            strike,
            opt_type
        ))
        inserted += 1

    conn.commit()
    conn.close()

    print("--------------------------------------------------")
    print(f"✅ INSERTED {inserted} REAL OPTION INSTRUMENTS")
    print("--------------------------------------------------")


if __name__ == "__main__":
    load_instruments()
