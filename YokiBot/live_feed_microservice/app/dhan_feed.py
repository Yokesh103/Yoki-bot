import asyncio
import json
import struct
import time
import logging
import websockets

logger = logging.getLogger("dhan_feed")

class DhanFeed:
    # Dhan V2 URL
    WSS_URL = "wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"

    def __init__(self, client_id, access_token, instruments, redis_client):
        self.client_id = client_id
        self.access_token = access_token
        self.instruments = instruments
        self.redis = redis_client
        self.running = True

    async def run_forever(self):
        while self.running:
            try:
                url = self.WSS_URL.format(token=self.access_token, client_id=self.client_id)
                # Ping interval prevents client-side timeouts
                async with websockets.connect(url, ping_interval=30) as ws:
                    
                    # Subscribe (RequestCode 15 = Ticker Data, usually more stable)
                    payload = {
                        "RequestCode": 15, 
                        "InstrumentCount": len(self.instruments),
                        "InstrumentList": [{"ExchangeSegment": "MCX_COMM", "SecurityId": str(i[1])} for i in self.instruments]
                    }
                    await ws.send(json.dumps(payload))
                    logger.info("✅ Subscribed to MCX (Ticker Mode)")

                    while self.running:
                        try:
                            # Wait for data
                            data = await ws.recv()
                            
                            # Parse Binary
                            if len(data) > 1:
                                msg_type = struct.unpack('<B', data[0:1])[0]
                                
                                # Type 2 = Ticker Packet (16 bytes)
                                if msg_type == 2:
                                    header = struct.unpack('<BHBIfI', data[0:16])
                                    ltp = header[4]
                                    await self.redis.set("live:last_packet_ts", time.time())
                                    logger.info(f"⚡ Tick: {ltp:.2f}")

                        except websockets.exceptions.ConnectionClosed:
                            logger.error("❌ Disconnected by Server. Retrying...")
                            break
            except Exception as e:
                logger.error(f"Connection Error: {e}")
                await asyncio.sleep(5)