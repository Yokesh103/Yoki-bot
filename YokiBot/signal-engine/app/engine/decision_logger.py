import json
from pathlib import Path

from app.engine.models import DecideRequest, DecisionResult
from app.engine.evaluate_credit_spread import evaluate_credit_spread


def load_mock_request() -> DecideRequest:
    file_path = Path(__file__).resolve().parents[1] / "data" / "mock_request.json"
    with open(file_path) as f:
        data = json.load(f)
    return DecideRequest(**data)


def generate_decision() -> DecisionResult:
    req = load_mock_request()
    decision = evaluate_credit_spread(req)
    return decision
