import requests
from app.config import ALERT_WEBHOOK

def send_alert(payload: dict):
    try:
        requests.post(ALERT_WEBHOOK, json=payload, timeout=3)
    except Exception:
        # do not crash signal engine; ideally log to file
        pass
