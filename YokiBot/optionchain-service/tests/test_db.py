from app.db import init_db
import os

def test_db_init_creates_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # override DB path via cwd
    os.makedirs("data", exist_ok=True)

    init_db()

    assert os.path.exists("data/options.db")
