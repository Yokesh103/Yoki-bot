from .data_freshness import data_freshness_filter
from .time_filter import time_filter
from .daily_loss import daily_loss_filter
from .trade_count import trade_count_filter
from .filter_result import FilterResult

def run_filters():
    for f in (
        data_freshness_filter,
        time_filter,
        daily_loss_filter,
        trade_count_filter,
    ):
        result = f()
        if result and not result.allowed:
            return result
    return None
