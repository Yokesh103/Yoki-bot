# paper-exec/main.py
from fastapi import FastAPI
from pydantic import BaseModel
import time, uuid

app = FastAPI(title="paper-exec")

class ExecReq(BaseModel):
    order_id: str
    legs: list
    qty: int = 25
    slippage_pct: float = 0.2

@app.post("/exec")
async def call_paper_exec(order: dict):
    """Robust call to paper-exec with retries, backoff and detailed logging."""
    print("\n================ ROUTER ==================")
    print(">>> ENTER call_paper_exec")
    print(">>> PAPER_EXEC_URL =", PAPER_EXEC_URL)
    print(">>> ORDER ID =", order["id"])

    order["status"] = "routing"
    order["routed_at"] = time.time()
    save_order_to_redis(order)

    payload = {
        "order_id": order["id"],
        "symbol": order["symbol"],
        "legs": order["legs"],
        "qty": order["qty"],
        "limit": order.get("limit"),
        "meta": order.get("meta"),
    }
    print(">>> PAYLOAD =", payload)

    max_retries = 3
    backoff = 0.5
    last_error = None

    async with httpx.AsyncClient(timeout=ROUTE_TIMEOUT) as client:
        for attempt in range(1, max_retries + 1):
            try:
                print(f">>> TRY {attempt} - POST -> {PAPER_EXEC_URL}")
                resp = await client.post(PAPER_EXEC_URL, json=payload)
                print(">>> GOT STATUS =", resp.status_code)
                if resp.status_code == 200:
                    # successful
                    try:
                        data = resp.json()
                    except Exception as e:
                        data = {"error": "invalid_json_response", "raw_text": resp.text}
                        print(">>> JSON PARSE ERROR:", e, resp.text)
                    order["status"] = data.get("status", "executed")
                    order["executed_at"] = time.time()
                    order["exec_result"] = {"attempts": attempt, "response": data}
                    break
                else:
                    # non-200, record text
                    txt = resp.text
                    order["status"] = "route_failed"
                    order["exec_result"] = {"attempts": attempt, "status_code": resp.status_code, "text": txt}
                    last_error = f"status:{resp.status_code}"
                    # do not break — you may want to retry non-200 depending on your policy
            except Exception as e:
                last_error = str(e)
                print(f">>> EXCEPTION on attempt {attempt}:", e)
                order["exec_result"] = {"attempts": attempt, "error": last_error}
                # backoff before next retry
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
        else:
            # exhausted retries
            order["status"] = "route_error"
            order["exec_result"] = {"error": last_error, "attempts": max_retries}

    # final save
    save_order_to_redis(order)
    print(">>> FINAL ORDER SAVED:", order["id"], "status=", order["status"])
    print("===========================================\n")
