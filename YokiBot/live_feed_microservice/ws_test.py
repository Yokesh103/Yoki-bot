from kafka import KafkaProducer
import json
import time
from datetime import datetime

# 1. Connect to Kafka (exactly as your microservice does)
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("✅ Connected to Kafka Broker on localhost:9092")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit()

# 2. Simulate 5 fake tick packets
topic = "dhan_ticks"
print(f"🚀 Sending 5 fake ticks to topic: '{topic}'...")

for i in range(1, 6):
    fake_tick = {
        "id": i,
        "symbol": "TEST-EQ",
        "price": 100 + i,
        "timestamp": datetime.now().isoformat()
    }
    
    # Send data
    future = producer.send(topic, value=fake_tick)
    result = future.get(timeout=10) # Wait for confirmation
    
    print(f"   Sent packet #{i} | Offset: {result.offset} | Partition: {result.partition}")
    time.sleep(1)

print("\n✅ Test Complete. If you see Offsets above, Kafka is working.")