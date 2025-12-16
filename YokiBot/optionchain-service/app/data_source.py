from typing import Dict, Any, List
import requests


class RestMarketDataSource:
    """
    Fetches market data from local live_feed_microservice (Dhan),
    NOT from broker REST API.
    """

    LIVE_FEED_BASE = "http://127.0.0.1:8300/live"

    def get_snapshot(self, instrument_keys: List[str]) -> Dict[str, Any]:
        """
        Fetch snapshot for multiple instruments from live feed service.
        Fail-safe: missing instruments return empty data.
        """

        data: Dict[str, Any] = {}

        for key in instrument_keys:
            try:
                r = requests.get(f"{self.LIVE_FEED_BASE}/{key}", timeout=1)
                if r.status_code == 200:
                    data[key] = {
                        "market_data": r.json()
                    }
            except Exception:
                continue

        return {"data": data}
