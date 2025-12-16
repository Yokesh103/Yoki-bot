from app.redis_client import redis_client
from .filter_result import FilterResult

MAX_DAILY_LOSS = 1000  # ₹


def daily_loss_filter() -> FilterResult:
    pnl = redis_client.get("pnl:today") or 0.0
    pnl = float(pnl)

    if pnl <= -MAX_DAILY_LOSS:
        return FilterResult(
            allowed=False,
            reason="DAILY_LOSS_LIMIT_REACHED",
        )

    return FilterResult(allowed=True)
