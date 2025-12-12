from uuid import uuid4
from app.engine.models import DecideRequest, DecisionResult
from app.config import SIMULATED_CHARGES, PREMIUM_THRESHOLD

MIN_DISTANCE = 150
MAX_DISTANCE = 200
HEDGE_GAP = 200

def evaluate_credit_spread(req: DecideRequest) -> DecisionResult:
    dec_id = str(uuid4())
    insts = [i.dict() for i in req.instruments]

    pes = [i for i in insts if i.get("opt_type") == "PE"]
    available_strikes = sorted([i["strike"] for i in pes])

    candidates = [
        i for i in pes
        if MIN_DISTANCE <= (req.spot - i["strike"]) <= MAX_DISTANCE
    ]

    if not candidates:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="NO_STRIKE_IN_RANGE",
            trade_payload={
                "spot": req.spot,
                "available_pe_strikes": available_strikes,
                "required_distance": [MIN_DISTANCE, MAX_DISTANCE]
            },
            decision_id=dec_id
        )

    short_leg = max(candidates, key=lambda x: x.get("oi", 0))

    hedge_candidates = [
        i for i in pes
        if i["strike"] == short_leg["strike"] - HEDGE_GAP
    ]

    if not hedge_candidates:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="NO_HEDGE_STRIKE",
            trade_payload={
                "short_strike": short_leg["strike"],
                "required_hedge": short_leg["strike"] - HEDGE_GAP,
                "available_pe_strikes": available_strikes
            },
            decision_id=dec_id
        )

    hedge_leg = hedge_candidates[0]
    short_prem = short_leg["ltp"]
    hedge_prem = hedge_leg["ltp"]

    gross_premium = short_prem - hedge_prem
    net_premium = gross_premium - SIMULATED_CHARGES
    max_risk = (short_leg["strike"] - hedge_leg["strike"]) * 50 - gross_premium * 50

    if net_premium < PREMIUM_THRESHOLD:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="PREMIUM_TOO_LOW",
            trade_payload={
                "gross_premium": gross_premium,
                "net_premium": net_premium,
                "threshold": PREMIUM_THRESHOLD
            },
            decision_id=dec_id
        )

    trade_payload = {
        "underlying": req.underlying,
        "expiry": req.expiry,
        "type": "PE_CREDIT_SPREAD",
        "short_strike": short_leg["strike"],
        "hedge_strike": hedge_leg["strike"],
        "short_premium": short_prem,
        "hedge_premium": hedge_prem,
        "gross_premium": gross_premium,
        "net_premium": net_premium,
        "max_risk": max_risk,
    }

    return DecisionResult(
        action="TRADE",
        strategy="CREDIT_SPREAD",
        reason=None,
        trade_payload=trade_payload,
        decision_id=dec_id
    )
