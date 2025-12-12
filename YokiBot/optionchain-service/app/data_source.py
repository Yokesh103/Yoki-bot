import os
import requests
from typing import List, Dict, Any

UPSTOX_BASE = "https://api.upstox.com/v2"
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")


class RestMarketDataSource:

    def __init__(self):
        if not ACCESS_TOKEN:
            raise RuntimeError("UPSTOX_ACCESS_TOKEN environment variable not set")

    def get_snapshot(self, instrument_keys: List[str]) -> Dict[str, Any]:
        url = f"{UPSTOX_BASE}/market-quote/quotes"
        params = {"instrument_key": ",".join(instrument_keys)}

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {ACCESS_TOKEN}"
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
