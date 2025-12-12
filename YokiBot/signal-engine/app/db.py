import sqlite3
from pathlib import Path
import json

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "signal_engine.db"
DB_PATH.parent.mkdir(exist_ok=True, parents=True)

_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_cur = _conn.cursor()

# ensure table exists
_cur.execute("""
CREATE TABLE IF NOT EXISTS trade_decision_logs (
    id TEXT PRIMARY KEY,
    timestamp TEXT,
    underlying TEXT,
    expiry TEXT,
    market_state TEXT,
    filter_data TEXT,
    strategy_selected TEXT,
    rejection_reason TEXT,
    strikes_chosen TEXT,
    confidence_score INTEGER,
    execution_status TEXT,
    premium_after_charges REAL,
    max_risk REAL,
    pnl_after_charges REAL,
    raw_payload TEXT
)
""")
_conn.commit()

def get_conn():
    return _conn

def init_db():
    """
    Compatibility shim so other modules can call init_db() at startup.
    Table creation already ran on import; calling this is safe and idempotent.
    """
    # Optionally re-check or migrate tables here in future.
    return True

def close_db():
    """
    Close DB on shutdown if needed.
    """
    try:
        _conn.close()
    except Exception:
        pass
def init_db():
    return _conn
