import sqlite3
from pathlib import Path

DB_PATH = Path("data/options.db")
DB_PATH.parent.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    cur = conn.cursor()

    # ✅ Instrument Master
    cur.execute("""
    CREATE TABLE IF NOT EXISTS instruments (
        instrument_key TEXT PRIMARY KEY,
        underlying TEXT NOT NULL,
        segment TEXT,
        instrument_type TEXT,
        strike REAL,
        opt_type TEXT,
        expiry TEXT NOT NULL
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_inst_underlying ON instruments(underlying)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inst_expiry ON instruments(expiry)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inst_underlying_expiry ON instruments(underlying, expiry)")

    # ✅ Market Snapshots
    cur.execute("""
    CREATE TABLE IF NOT EXISTS option_snapshots (
        ts TEXT,
        instrument_key TEXT,
        underlying TEXT,
        expiry TEXT,
        strike REAL,
        opt_type TEXT,
        ltp REAL,
        oi INTEGER
    )
    """)

    conn.commit()
