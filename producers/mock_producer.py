import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SYMBOLS = ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN"]
BASE_PRICES = {"AAPL": 175, "GOOGL": 140, "TSLA": 250, "MSFT": 380, "AMZN": 185}

print("[MOCK PRODUCER] Streaming simulated ticks. Press Ctrl+C to stop.")

while True:
    for symbol in SYMBOLS:
        price = BASE_PRICES[symbol] * (1 + random.uniform(-0.02, 0.02))
        message = {
            "symbol": symbol,
            "open": round(price * 0.999, 2),
            "high": round(price * 1.005, 2),
            "low": round(price * 0.995, 2),
            "close": round(price, 2),
            "volume": random.randint(1000, 50000),
            "timestamp": datetime.utcnow().isoformat(),
            "ingested_at": datetime.utcnow().isoformat()
        }
        producer.send("raw_stock_ticks", value=message)
        print(f"[MOCK] {symbol} @ {message['close']}")
    time.sleep(2)