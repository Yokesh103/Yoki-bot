from datetime import datetime, timezone

_latest_decision = {
    "decision": None,
    "last_updated": None,
}


def save_decision(decision: dict):
    _latest_decision["decision"] = decision
    _latest_decision["last_updated"] = datetime.now(timezone.utc).isoformat()


def get_latest_decision():
    return _latest_decision
