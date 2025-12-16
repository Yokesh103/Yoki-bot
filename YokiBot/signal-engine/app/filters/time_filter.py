from datetime import datetime
from .filter_result import FilterResult


def time_filter() -> FilterResult:
    now = datetime.now().time()

    # No trades before market stabilizes
    if now < datetime.strptime("09:30", "%H:%M").time():
        return FilterResult(
            allowed=False,
            reason="BEFORE_0930",
        )

    # No new trades late in the day
    if now > datetime.strptime("14:30", "%H:%M").time():
        return FilterResult(
            allowed=False,
            reason="AFTER_1430",
        )

    return FilterResult(allowed=True)
