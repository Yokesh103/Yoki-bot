from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/execute")
async def execute(req: Request):
    data = await req.json()
    return {
        "status": "executed",
        "order_id": data.get("order_id"),
        "fill_price": 123.45
    }
