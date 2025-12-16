from app.engine.evaluate_credit_spread import evaluate_credit_spread
from app.engine.evaluate_iron_condor import evaluate_iron_condor
from app.engine.models import DecideRequest, DecisionResult


def route_strategy(req: DecideRequest) -> DecisionResult:
    """
    Temporary rule-based router.
    Will be replaced later by MarketState-based eligibility.
    """

    # Temporary range-bound heuristic
    if abs(req.spot % 100) < 40:
        return evaluate_iron_condor(req)

    return evaluate_credit_spread(req)
