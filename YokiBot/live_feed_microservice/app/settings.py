# app/settings.py
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

SYMBOL = os.getenv("SYMBOL", "NIFTY")
EXPIRY = os.getenv("EXPIRY", "2025-01-30")

ATM_RANGE = int(os.getenv("ATM_RANGE", 10))  # ± strikes
SNAPSHOT_TTL = int(os.getenv("SNAPSHOT_TTL", 15))
