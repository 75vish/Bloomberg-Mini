-- Reads directly from Bronze Parquet files
-- Deduplicates, normalizes timestamps, casts types

WITH raw AS (
    SELECT *
    FROM read_parquet('../data/bronze/**/*.parquet')
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, timestamp
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM raw
),

cleaned AS (
    SELECT
        symbol,
        CAST(open AS DOUBLE)    AS open,
        CAST(high AS DOUBLE)    AS high,
        CAST(low AS DOUBLE)     AS low,
        CAST(close AS DOUBLE)   AS close,
        CAST(volume AS BIGINT)  AS volume,
        CAST(timestamp AS TIMESTAMP) AS tick_time,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at,
        DATE(CAST(timestamp AS TIMESTAMP)) AS tick_date
    FROM deduplicated
    WHERE row_num = 1  -- keep only latest duplicate
      AND close > 0
      AND volume > 0
      AND symbol IS NOT NULL
)

SELECT * FROM cleaned