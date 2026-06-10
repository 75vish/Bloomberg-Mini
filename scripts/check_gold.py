import duckdb

conn = duckdb.connect("data/gold.duckdb")

print("\n=== VWAP (last 5 rows) ===")
print(conn.execute("SELECT * FROM mart_vwap ORDER BY window_start DESC LIMIT 5").df())

print("\n=== ANOMALIES ===")
print(conn.execute("SELECT * FROM mart_anomalies LIMIT 5").df())

print("\n=== MOVING AVERAGES ===")
print(conn.execute("SELECT * FROM mart_moving_avg ORDER BY window_start DESC LIMIT 5").df())