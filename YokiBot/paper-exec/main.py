# YokiBot/paper-exec/main.py
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid
import time

app = FastAPI(title="Paper Execution & Ledger")

# --- DATABASE (In-Memory for now) ---
positions = {}
ledger = {
    "balance": 100000.0,  # Starting Capital
    "realized_pnl": 0.0,
    "charges": 0.0
}

class OrderRequest(BaseModel):
    symbol: str
    qty: int
    side: str  # BUY or SELL
    price: float
    tag: str = "SIGNAL_ENGINE"

# --- INDIAN F&O CHARGES CALCULATOR ---
def calculate_charges(price, qty, side):
    turnover = price * qty
    brokerage = 20.0  # Flat 20 Rs per order
    
    # Exchange Txn Charge (NSE Options ~0.05%)
    exch_txn = 0.0005 * turnover
    
    # STT (0.125% on SELL only for Options)
    stt = 0.00125 * turnover if side == "SELL" else 0.0
    
    # SEBI Charges (0.0001%)
    sebi = 0.000001 * turnover
    
    # GST (18% on Brokerage + Txn + SEBI)
    gst = 0.18 * (brokerage + exch_txn + sebi)
    
    # Stamp Duty (0.003% on BUY only)
    stamp = 0.00003 * turnover if side == "BUY" else 0.0
    
    total_tax = brokerage + exch_txn + stt + sebi + gst + stamp
    return round(total_tax, 2)

@app.get("/health")
def health():
    return {"status": "PAPER BOT ONLINE", "capital": ledger["balance"]}

@app.get("/positions")
def get_positions():
    return {
        "open_positions": positions,
        "ledger": ledger
    }

@app.post("/place_order")
def place_order(order: OrderRequest):
    order_id = str(uuid.uuid4())[:8]
    tax = calculate_charges(order.price, order.qty, order.side)
    
    # Update Ledger
    ledger["charges"] += tax
    ledger["balance"] -= tax # Deduct charges immediately
    
    # Logic for Opening/Closing positions
    # (Simplified: Just adding to list for visualization)
    positions[order_id] = {
        "symbol": order.symbol,
        "side": order.side,
        "qty": order.qty,
        "entry_price": order.price,
        "tax": tax,
        "time": time.strftime("%H:%M:%S")
    }
    
    return {
        "status": "FILLED",
        "order_id": order_id,
        "filled_price": order.price,
        "charges_deducted": tax
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8400)