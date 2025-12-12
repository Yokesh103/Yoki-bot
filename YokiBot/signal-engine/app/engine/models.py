from typing import List, Optional
from pydantic import BaseModel


class Instrument(BaseModel):
    strike: float
    opt_type: str
    ltp: float
    oi: Optional[float] = None


class DecideRequest(BaseModel):
    underlying: str
    expiry: str
    spot: float
    instruments: List[Instrument]


class DecisionResult(BaseModel):
    action: str
    strategy: str
    reason: Optional[str] = None
    trade_payload: Optional[dict] = None
    decision_id: str
