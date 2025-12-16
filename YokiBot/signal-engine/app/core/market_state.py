from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MarketState:
    spot: float
    atm: int
    vwap: Optional[float]
    rsi: Optional[float]
    trend_strength: Optional[float]
    time_block: str
    is_expiry: bool
    data_age_seconds: float
