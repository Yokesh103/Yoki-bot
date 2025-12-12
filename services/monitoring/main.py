
import os
import asyncio
import json
import logging
import time
import sqlite3
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitoring")

DB_PATH = os.getenv("MONITOR_DB", "monitor.db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
SIGNAL_TOPIC = os.getenv("SIGNAL_TOPIC", "signals")
TICKS_TOPIC = os.getenv("TICKS_TOPIC", "market.ticks")

app = FastAPI(title="YokiBot Monitoring")

subscribers = set()

def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT,
        symbol TEXT,
        timestamp INTEGER,
        payload TEXT
    )
    """)
    conn.commit()
    return conn

db = init_db()

def save_event(event_type: str, symbol: str, payload):
    ts = int(time.time() * 1000)
    cur = db.cursor()
    cur.execute("INSERT INTO events (type, symbol, timestamp, payload) VALUES (?, ?, ?, ?)", 
                (event_type, symbol, ts, json.dumps(payload)))
    db.commit()
    return cur.lastrowid

@app.on_event('startup')
async def startup_bg():
    if KAFKA_BOOTSTRAP:
        app.state.kafka_task = asyncio.create_task(kafka_consumer_loop())
        logger.info('Kafka consumer started')
    else:
        logger.info('Kafka bootstrap not configured, running in HTTP-ingest-only mode')

@app.on_event('shutdown')
async def shutdown_bg():
    task = app.state.__dict__.get('kafka_task')
    if task:
        task.cancel()
        try:
            await task
        except Exception:
            pass

async def kafka_consumer_loop():
    try:
        from aiokafka import AIOKafkaConsumer
    except Exception as e:
        logger.warning('aiokafka not available, kafka consumer disabled: %s', e)
        return
    consumer = AIOKafkaConsumer(SIGNAL_TOPIC, TICKS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id='yokibot-monitor')
    await consumer.start()
    try:
        async for msg in consumer:
            topic = msg.topic
            try:
                payload = json.loads(msg.value.decode())
            except Exception:
                payload = { 'raw': msg.value.hex() }
            symbol = payload.get('symbol', 'UNKNOWN')
            save_event(topic, symbol, payload)
            data = json.dumps({'topic': topic, 'symbol': symbol, 'payload': payload, 'ts': int(time.time()*1000)})
            for q in list(subscribers):
                await q.put(data)
    finally:
        await consumer.stop()

@app.post('/ingest')
async def ingest_event(request: Request):
    body = await request.json()
    ev_type = body.get('type')
    symbol = body.get('symbol','UNKNOWN')
    payload = body.get('payload', {})
    if ev_type not in ('signal','tick','log','alert'):
        raise HTTPException(400, 'invalid type')
    save_event(ev_type, symbol, payload)
    data = json.dumps({'topic': ev_type, 'symbol': symbol, 'payload': payload, 'ts': int(time.time()*1000)})
    for q in list(subscribers):
        await q.put(data)
    return JSONResponse({'ok': True})

@app.get('/health')
async def health():
    return JSONResponse({'service': 'monitoring', 'ok': True})

@app.get('/events/stream')
async def events_stream():
    async def generator():
        q = asyncio.Queue()
        subscribers.add(q)
        try:
            cur = db.cursor()
            cur.execute('SELECT id,type,symbol,timestamp,payload FROM events ORDER BY id DESC LIMIT 20')
            rows = cur.fetchall()
            for r in reversed(rows):
                yield f"data: {json.dumps({'id':r[0],'type':r[1],'symbol':r[2],'ts':r[3],'payload':json.loads(r[4])})}\n\n"
            while True:
                data = await q.get()
                yield f'data: {data}\n\n'
        finally:
            subscribers.discard(q)
    return StreamingResponse(generator(), media_type='text/event-stream')

@app.get('/dashboard', response_class=HTMLResponse)
async def dashboard():
    html = open(os.path.join(os.path.dirname(__file__), 'static', 'index.html')).read()
    return HTMLResponse(html)

@app.get('/events/latest')
async def latest(n: int = 50):
    cur = db.cursor()
    cur.execute('SELECT id,type,symbol,timestamp,payload FROM events ORDER BY id DESC LIMIT ?', (n,))
    rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({'id': r[0], 'type': r[1], 'symbol': r[2], 'ts': r[3], 'payload': json.loads(r[4])})
    return JSONResponse(list(reversed(out)))
