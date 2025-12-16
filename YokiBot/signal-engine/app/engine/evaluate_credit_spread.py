from uuid import uuid4
from datetime import datetime
from app.engine.models import DecideRequest, DecisionResult
from app.engine.risk_guard import passes_risk_guard

# ============================
# INDEX-SPECIFIC RULES
# ============================

INDEX_RULES = {
    "NIFTY": {
        "lot_size": 50,
        "min_distance": (150, 250),
        "hedge_gap": 200,
        "min_net_premium": 35,
        "zerodha_charges": 120,  # round-trip, 1 lot
    },
    "BANKNIFTY": {
        "lot_size": 15,
        "min_distance": (300, 500),
        "hedge_gap": 300,
        "min_net_premium": 70,
        "zerodha_charges": 180,
    },
}

# ============================
# HELPERS
# ============================

def is_monthly_expiry(expiry: str) -> bool:
    """
    NSE monthly expiry = last Thursday of month
    (rough check is enough for strategy logic)
    """
    try:
        d = datetime.strptime(expiry, "%Y-%m-%d")
        return d.weekday() == 3 and 22 <= d.day <= 31
    except Exception:
        return False


# ============================
# CORE STRATEGY
# ============================

def evaluate_credit_spread(req: DecideRequest) -> DecisionResult:
    dec_id = str(uuid4())

    # ---------------------------
    # BASIC SANITY CHECK
    # ---------------------------
    if not req.instruments or req.spot <= 0:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="INVALID_INPUT",
            decision_id=dec_id,
        )

    rules = INDEX_RULES.get(req.underlying.upper())
    if not rules:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="UNSUPPORTED_INDEX",
            decision_id=dec_id,
        )

    lot_size = rules["lot_size"]
    MIN_DISTANCE, MAX_DISTANCE = rules["min_distance"]
    HEDGE_GAP = rules["hedge_gap"]
    ZERODHA_CHARGES = rules["zerodha_charges"]
    MIN_NET_PREMIUM = rules["min_net_premium"]

    # Monthly expiry = stricter
    if is_monthly_expiry(req.expiry):
        MIN_DISTANCE += 100
        ZERODHA_CHARGES += 30

    insts = [i.model_dump() for i in req.instruments]
    pes = [i for i in insts if i["opt_type"] == "PE"]

    if not pes:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="NO_PE_INSTRUMENTS",
            decision_id=dec_id,
        )

    available_strikes = sorted(i["strike"] for i in pes)

    # ---------------------------
    # DISTANCE FILTER
    # ---------------------------
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
                "required_distance": [MIN_DISTANCE, MAX_DISTANCE],
            },
            decision_id=dec_id,
        )

    # ---------------------------
    # SHORT LEG (MAX OI)
    # ---------------------------
    short_leg = max(candidates, key=lambda x: x.get("oi", 0))

    # ---------------------------
    # HEDGE LEG
    # ---------------------------
    hedge_strike = short_leg["strike"] - HEDGE_GAP
    hedge_candidates = [i for i in pes if i["strike"] == hedge_strike]

    if not hedge_candidates:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="NO_HEDGE_STRIKE",
            trade_payload={
                "short_strike": short_leg["strike"],
                "required_hedge": hedge_strike,
                "available_pe_strikes": available_strikes,
            },
            decision_id=dec_id,
        )

    hedge_leg = hedge_candidates[0]

    # ---------------------------
    # PREMIUM CHECK
    # ---------------------------
    short_prem = short_leg["ltp"]
    hedge_prem = hedge_leg["ltp"]

    if short_prem <= 0 or hedge_prem <= 0:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="INVALID_PREMIUM_DATA",
            decision_id=dec_id,
        )

    if hedge_prem < 5:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="ILLQUID_HEDGE",
            trade_payload={"hedge_premium": hedge_prem},
            decision_id=dec_id,
        )

    gross_premium = short_prem - hedge_prem
    net_premium = gross_premium - ZERODHA_CHARGES

    if net_premium < MIN_NET_PREMIUM:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason="PREMIUM_TOO_LOW",
            trade_payload={
                "gross_premium": gross_premium,
                "net_premium": net_premium,
                "threshold": MIN_NET_PREMIUM,
            },
            decision_id=dec_id,
        )

    # ---------------------------
    # MAX RISK
    # ---------------------------
    spread_width = short_leg["strike"] - hedge_leg["strike"]
    max_risk = (spread_width * lot_size) - (gross_premium * lot_size)

    ok, risk_reason = passes_risk_guard(max_risk)
    if not ok:
        return DecisionResult(
            action="NO_TRADE",
            strategy="CREDIT_SPREAD",
            reason=risk_reason,
            trade_payload={"max_risk": max_risk},
            decision_id=dec_id,
        )

    # ---------------------------
    # FINAL TRADE
    # ---------------------------
    return DecisionResult(
        action="TRADE",
        strategy="CREDIT_SPREAD",
        reason=None,
        trade_payload={
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
            "lot_size": lot_size,
        },
        decision_id=dec_id,
    )
