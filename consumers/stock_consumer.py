import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "raw_stock_ticks",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="stock-consumer-group"
)

print("[CONSUMER] Listening on raw_stock_ticks...")
for message in consumer:
    tick = message.value
    print(f"[CONSUMER] {tick['symbol']} | close={tick['close']} | vol={tick['volume']} | ts={tick['timestamp']}")