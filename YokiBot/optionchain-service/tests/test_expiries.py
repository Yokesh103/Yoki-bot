from app.db import init_db, get_expiries_for_underlying
import os

def test_empty_expiries(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)

    init_db()

    expiries = get_expiries_for_underlying("NIFTY")
    assert expiries == []
