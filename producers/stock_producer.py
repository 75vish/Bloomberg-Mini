import json
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from alpaca_trade_api.stream import Stream
from kafka import KafkaProducer

load_dotenv()

ALPACA_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY")
SYMBOLS = ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def on_bar(bar):
    message = {
        "symbol": bar.symbol,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "timestamp": bar.timestamp.isoformat(),
        "ingested_at": datetime.utcnow().isoformat()
    }
    producer.send("raw_stock_ticks", value=message)
    print(f"[PRODUCER] Sent: {message['symbol']} @ {message['close']}")

stream = Stream(ALPACA_KEY, ALPACA_SECRET, data_feed="iex")
stream.subscribe_bars(on_bar, *SYMBOLS)

print("[PRODUCER] Starting stream...")
stream.run()