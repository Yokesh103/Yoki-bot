from datetime import datetime
from typing import Optional, Dict

_latest_decision: Optional[Dict] = None
_last_updated: Optional[str] = None


def save_decision(decision: Dict):
    global _latest_decision, _last_updated
    _latest_decision = decision
    _last_updated = datetime.utcnow().isoformat()


def get_latest_decision():
    return {
        "decision": _latest_decision,
        "last_updated": _last_updated,
    }
