from app.config import MAX_RISK_PER_TRADE, MONTHLY_LOSS_LIMIT, WORKING_CAPITAL

# Later: Wire DB-based PnL tracking
DUMMY_MONTHLY_LOSS = 0  # placeholder until DB is connected

def passes_risk_guard(max_risk: float) -> (bool, str):
    """
    Validates whether the trade respects:
    - Max risk per trade
    - Monthly loss cap
    """

    # Rule 1: Per-trade risk cap
    if max_risk > MAX_RISK_PER_TRADE:
        return False, f"RISK_LIMIT_EXCEEDED_{max_risk}"

    # Rule 2: Monthly loss cap (to be replaced with real DB tracking)
    if DUMMY_MONTHLY_LOSS >= MONTHLY_LOSS_LIMIT:
        return False, "MONTHLY_LOSS_LIMIT_REACHED"

    return True, "OK"
