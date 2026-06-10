-- Volume Weighted Average Price per symbol per 5-minute window
-- VWAP = sum(price * volume) / sum(volume)

WITH ticks AS (
    SELECT * FROM {{ ref('stg_stock_ticks') }}
),

windowed AS (
    SELECT
        symbol,
        tick_date,
        -- Create 5-minute buckets
        TIME_BUCKET(INTERVAL '5 minutes', tick_time) AS window_start,
        SUM(close * volume) / SUM(volume)            AS vwap,
        AVG(close)                                   AS avg_price,
        MAX(high)                                    AS period_high,
        MIN(low)                                     AS period_low,
        SUM(volume)                                  AS total_volume,
        COUNT(*)                                     AS tick_count
    FROM ticks
    GROUP BY symbol, tick_date, TIME_BUCKET(INTERVAL '5 minutes', tick_time)
)

SELECT * FROM windowed
ORDER BY symbol, window_start