from app.redis_client import redis_client
from .filter_result import FilterResult

MAX_TRADES_PER_DAY = 2


def trade_count_filter() -> FilterResult:
    count = redis_client.get("trades:today") or 0
    count = int(count)

    if count >= MAX_TRADES_PER_DAY:
        return FilterResult(
            allowed=False,
            reason="MAX_TRADES_REACHED",
        )

    return FilterResult(allowed=True)
