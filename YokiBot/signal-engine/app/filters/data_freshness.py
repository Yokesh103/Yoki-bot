import time
from app.redis_client import redis_client
from .filter_result import FilterResult


def data_freshness_filter() -> FilterResult:
    """
    Reject trades if live feed data is stale.
    """

    try:
        ts = redis_client.get("live:last_packet_ts")
        if ts is None:
            return FilterResult(
                allowed=False,
                reason="NO_LIVE_FEED_TIMESTAMP",
            )

        age = time.time() - float(ts)
        if age > 3:
            return FilterResult(
                allowed=False,
                reason="STALE_LIVE_FEED",
            )

        return FilterResult(allowed=True)

    except Exception:
        return FilterResult(
            allowed=False,
            reason="LIVE_FEED_CHECK_FAILED",
        )
