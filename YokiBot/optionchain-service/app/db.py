import sqlite3
from pathlib import Path
from typing import List, Dict, Any

# -------------------------------------------------
# DATABASE PATH
# -------------------------------------------------
DB_PATH = Path("data/options.db")
DB_PATH.parent.mkdir(exist_ok=True)

# -------------------------------------------------
# CONNECTION FACTORY
# -------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# -------------------------------------------------
# INIT DB
# -------------------------------------------------
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Instrument master
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

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_underlying ON instruments(underlying)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_expiry ON instruments(expiry)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_inst_underlying_expiry ON instruments(underlying, expiry)"
    )

    # Market snapshots (optional, future use)
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
    conn.close()


# -------------------------------------------------
# QUERIES
# -------------------------------------------------
def get_instruments_from_db(underlying: str, expiry: str) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM instruments
        WHERE underlying = ?
          AND expiry = ?
        """,
        (underlying, expiry),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_expiries_for_underlying(underlying: str) -> List[str]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DISTINCT expiry
        FROM instruments
        WHERE underlying = ?
        ORDER BY expiry ASC
        """,
        (underlying,),
    )

    expiries = [row["expiry"] for row in cur.fetchall()]
    conn.close()
    return expiries
