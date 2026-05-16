import os
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Define the exact schema you expect from Kafka
TICK_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

ALLOWED_SYMBOLS = ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN"]

spark = SparkSession.builder \
    .appName("StockBronzeWriter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_stock_ticks") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka gives you bytes — parse the JSON value
parsed = raw_stream.select(
    from_json(col("value").cast("string"), TICK_SCHEMA).alias("data")
).select("data.*")

# Split into good and bad rows
good_rows = parsed.filter(
    col("symbol").isin(ALLOWED_SYMBOLS) &
    col("close").isNotNull() &
    col("volume").isNotNull() &
    (col("close") > 0) &
    (col("volume") > 0)
).withColumn("date", to_date(current_timestamp()))

bad_rows = parsed.filter(
    ~col("symbol").isin(ALLOWED_SYMBOLS) |
    col("close").isNull() |
    col("volume").isNull() |
    (col("close") <= 0) |
    (col("volume") <= 0)
).withColumn("date", to_date(current_timestamp()))

# Write good rows to Bronze as Parquet
bronze_query = good_rows.writeStream \
    .format("parquet") \
    .option("path", "data/bronze/") \
    .option("checkpointLocation", "data/checkpoints/bronze/") \
    .partitionBy("date", "symbol") \
    .outputMode("append") \
    .start()

# Write bad rows to dead_letter
dead_letter_query = bad_rows.writeStream \
    .format("parquet") \
    .option("path", "data/dead_letter/") \
    .option("checkpointLocation", "data/checkpoints/dead_letter/") \
    .outputMode("append") \
    .start()

print("[BRONZE WRITER] Streaming started. Waiting for ticks...")
spark.streams.awaitAnyTermination()