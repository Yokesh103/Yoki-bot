from fastapi import FastAPI
from app.engine.decision_logger import generate_decision

app = FastAPI(title="Signal Engine")


@app.get("/health")
def health():
    return {"status": "SIGNAL ENGINE LIVE"}


@app.get("/signal")
def signal():
    decision = generate_decision()
    return decision.model_dump()
