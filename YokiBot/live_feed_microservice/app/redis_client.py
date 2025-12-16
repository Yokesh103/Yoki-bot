# app/redis_client.py
import redis.asyncio as redis
from app.settings import REDIS_URL

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True
)
