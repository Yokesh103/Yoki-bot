from fastapi import FastAPI, HTTPException
from app.engine.models import DecideRequest, DecisionResult
from app.engine.strategy_router import route_strategy
from app.engine.decision_logger import save_decision, get_latest_decision
from app.filters import run_filters

app = FastAPI(title="Signal Engine")

# =====================
# HEALTH
# =====================
@app.get("/health")
def health():
    return {"status": "SIGNAL ENGINE LIVE"}

# =====================
# CORE DECISION
# =====================
@app.post("/decide", response_model=DecisionResult)
def decide(req: DecideRequest):
    try:
        # 1️⃣ RUN FILTERS FIRST
        ok, reason = run_filters(req)
        if not ok:
            decision = DecisionResult(
                action="NO_TRADE",
                strategy="SYSTEM",
                reason=reason,
                decision_id="NA",
                legs={}
            )
            # Log it but don't crash
            save_decision(decision.model_dump())
            return decision

        # 2️⃣ ROUTE STRATEGY
        decision = route_strategy(req)

        # 3️⃣ STORE DECISION
        save_decision(decision.model_dump())

        return decision

    except Exception as e:
        # RETURN ERROR AS JSON, DO NOT CRASH SERVER
        print(f"[CRITICAL ERROR] {str(e)}")
        return DecisionResult(
            action="ERROR",
            strategy="CRASH",
            reason=str(e),
            decision_id="ERR",
            legs={}
        )

# =====================
# DASHBOARD
# =====================
@app.get("/latest_decision")
def latest_decision():
    return get_latest_decision()