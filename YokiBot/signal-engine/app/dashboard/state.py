from app.engine.decision_logger import get_latest_decision


def get_dashboard_state():
    data = get_latest_decision()
    decision = data.get("decision")

    if not decision:
        return {
            "state": "INIT",
            "last_updated": None,
        }

    return {
        "strategy": decision["strategy"],
        "action": decision["action"],
        "reason": decision["reason"],
        "last_updated": data["last_updated"],
    }
