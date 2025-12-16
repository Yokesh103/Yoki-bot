# app/engine/evaluate_iron_condor.py

from uuid import uuid4
from datetime import datetime
from app.engine.models import DecideRequest, DecisionResult
from app.engine.risk_guard import passes_risk_guard

LOT_SIZE = 50
PE_DISTANCE = (200, 350)
CE_DISTANCE = (200, 350)
HEDGE_GAP = 200
MIN_NET_PREMIUM = 80
ZERODHA_CHARGES = 200
MAX_RISK_ALLOWED = 4000


def evaluate_iron_condor(req: DecideRequest) -> DecisionResult:
    dec_id = str(uuid4())

    insts = [i.model_dump() for i in req.instruments]
    pes = [i for i in insts if i["opt_type"] == "PE"]
    ces = [i for i in insts if i["opt_type"] == "CE"]

    if not pes or not ces:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="MISSING_LEGS",
            decision_id=dec_id,
        )

    # -------- PE side --------
    pe_candidates = [
        i for i in pes
        if PE_DISTANCE[0] <= (req.spot - i["strike"]) <= PE_DISTANCE[1]
    ]
    if not pe_candidates:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="NO_PE_RANGE",
            decision_id=dec_id,
        )

    short_pe = max(pe_candidates, key=lambda x: x.get("oi", 0))
    hedge_pe = next(
        (i for i in pes if i["strike"] == short_pe["strike"] - HEDGE_GAP),
        None,
    )

    if not hedge_pe:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="NO_PE_HEDGE",
            decision_id=dec_id,
        )

    # -------- CE side --------
    ce_candidates = [
        i for i in ces
        if CE_DISTANCE[0] <= (i["strike"] - req.spot) <= CE_DISTANCE[1]
    ]
    if not ce_candidates:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="NO_CE_RANGE",
            decision_id=dec_id,
        )

    short_ce = max(ce_candidates, key=lambda x: x.get("oi", 0))
    hedge_ce = next(
        (i for i in ces if i["strike"] == short_ce["strike"] + HEDGE_GAP),
        None,
    )

    if not hedge_ce:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="NO_CE_HEDGE",
            decision_id=dec_id,
        )

    gross_premium = (
        short_pe["ltp"] + short_ce["ltp"]
        - hedge_pe["ltp"] - hedge_ce["ltp"]
    )

    net_premium = gross_premium - ZERODHA_CHARGES

    if net_premium < MIN_NET_PREMIUM:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="PREMIUM_TOO_LOW",
            decision_id=dec_id,
        )

    max_risk = HEDGE_GAP * LOT_SIZE - (gross_premium * LOT_SIZE)

    if max_risk > MAX_RISK_ALLOWED:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason="RISK_TOO_HIGH",
            trade_payload={"max_risk": max_risk},
            decision_id=dec_id,
        )

    ok, reason = passes_risk_guard(max_risk)
    if not ok:
        return DecisionResult(
            action="NO_TRADE",
            strategy="IRON_CONDOR",
            reason=reason,
            decision_id=dec_id,
        )

    return DecisionResult(
        action="TRADE",
        strategy="IRON_CONDOR",
        decision_id=dec_id,
        trade_payload={
            "short_pe": short_pe["strike"],
            "hedge_pe": hedge_pe["strike"],
            "short_ce": short_ce["strike"],
            "hedge_ce": hedge_ce["strike"],
            "net_premium": net_premium,
            "max_risk": max_risk,
        },
    )
