from app.config import MAX_RISK_PER_TRADE, MONTHLY_LOSS_LIMIT

# Placeholder until real PnL DB exists
DUMMY_MONTHLY_LOSS = 0

def passes_risk_guard(max_risk: float) -> (bool, str):
    """
    Hard risk constraints.
    """

    if max_risk > MAX_RISK_PER_TRADE:
        return False, "RISK_LIMIT_EXCEEDED"

    if DUMMY_MONTHLY_LOSS >= MONTHLY_LOSS_LIMIT:
        return False, "MONTHLY_LOSS_LIMIT_REACHED"

    return True, "OK"
