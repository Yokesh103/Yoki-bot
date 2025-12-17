import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import time

app = FastAPI(title="Paper Execution & Ledger")

# --- CONFIGURATION ---
# 1. SET STARTING CAPITAL HERE
STARTING_CAPITAL = 20000.0 

# --- DATABASE (RAM) ---
positions = {}
ledger = {
    "balance": STARTING_CAPITAL,
    "realized_pnl": 0.0,
    "charges": 0.0
}

class OrderRequest(BaseModel):
    symbol: str
    qty: int
    side: str
    price: float
    tag: str = "SIGNAL_ENGINE"

# --- TAX CALCULATOR (NSE OPTIONS) ---
def calculate_taxes(price, qty, side):
    turnover = price * qty
    brokerage = 20.0
    stt = 0.00125 * turnover if side == "SELL" else 0.0
    exch = 0.0005 * turnover
    gst = 0.18 * (brokerage + exch)
    stamp = 0.00003 * turnover if side == "BUY" else 0.0
    return round(brokerage + stt + exch + gst + stamp, 2)

@app.get("/health")
def health():
    return {"status": "OK", "capital": ledger["balance"]}

@app.get("/positions")
def get_positions():
    return {"open_positions": positions, "ledger": ledger}

@app.post("/place_order")
def place_order(order: OrderRequest):
    # Check if we have enough money
    cost = order.qty * order.price
    if order.side == "BUY" and cost > ledger["balance"]:
        return {"status": "REJECTED", "reason": "Insufficient Funds"}

    tax = calculate_taxes(order.price, order.qty, order.side)
    order_id = str(uuid.uuid4())[:8]
    
    # Deduct charges immediately
    ledger["charges"] += tax
    ledger["balance"] -= tax 
    
    # Update Balance (For BUY orders, we usually block margin, but for paper we just track cash flow)
    # For simplicity in this view, we won't deduct the position cost from balance until we close it,
    # BUT we will deduct taxes.
    
    # Record Position
    positions[order_id] = {
        "Instrument": order.symbol,  # Renamed for Dashboard
        "Side": order.side,
        "Qty": order.qty,
        "Entry Price": order.price,
        "Entry Time": time.strftime("%H:%M:%S"),
        "Charges": tax
    }
    
    return {
        "status": "FILLED",
        "order_id": order_id,
        "filled_price": order.price,
        "charges_deducted": tax
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8400)