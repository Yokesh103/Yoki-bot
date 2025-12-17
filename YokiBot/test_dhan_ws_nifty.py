import json
import websocket

CLIENT_ID = "1109405279"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY1OTk2ODk4LCJpYXQiOjE3NjU5MTA0OTgsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5NDA1Mjc5In0.du2Jv7m7m3idES-jL1oj3tkYmxMtwTG-7ZJ5NrwDuVEJUpmDXD1l2xqwOYlvKjxziGjI6swRcFIus_VyJqGH-Q"

# NSE Equity
EXCHANGE_SEGMENT = 1  # NSE_EQ

INSTRUMENTS = [
    "2885",   # RELIANCE
    "1594",   # INFY
    "11536",  # TCS
]

WS_URL = (
    f"wss://api-feed.dhan.co/ws"
    f"?clientId={CLIENT_ID}"
    f"&accessToken={ACCESS_TOKEN}"
)

def on_open(ws):
    print("✅ WebSocket connected")

    subscribe_payload = {
        "RequestCode": 15,
        "Mode": 1,  # 🔑 LTP ONLY (RETAIL SAFE)
        "InstrumentCount": len(INSTRUMENTS),
        "InstrumentList": [
            {
                "ExchangeSegment": EXCHANGE_SEGMENT,
                "SecurityId": sec_id
            }
            for sec_id in INSTRUMENTS
        ],
    }

    ws.send(json.dumps(subscribe_payload))
    print("📡 Subscribed to EQUITY LTP feed")


def on_message(ws, message):
    print("📥 TICK:", message)


def on_error(ws, error):
    print("❌ WS ERROR:", error)


def on_close(ws, code, reason):
    print(f"🔌 WebSocket closed ({code}) {reason}")


if __name__ == "__main__":
    print("🚀 Connecting to Dhan WebSocket (EQUITY LTP MODE)...")

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(ping_interval=10, ping_timeout=5)
