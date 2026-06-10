-- Flag ticks where price deviates >2% from VWAP in that window

WITH ticks AS (
    SELECT * FROM {{ ref('stg_stock_ticks') }}
),

vwap AS (
    SELECT * FROM {{ ref('mart_vwap') }}
),

joined AS (
    SELECT
        t.symbol,
        t.tick_time,
        t.tick_date,
        t.close,
        t.volume,
        v.vwap,
        ABS(t.close - v.vwap) / v.vwap * 100 AS pct_deviation,
        CASE
            WHEN ABS(t.close - v.vwap) / v.vwap * 100 > 2 THEN TRUE
            ELSE FALSE
        END AS is_anomaly,
        CASE
            WHEN t.close > v.vwap * 1.02 THEN 'PRICE_SPIKE'
            WHEN t.close < v.vwap * 0.98 THEN 'PRICE_DROP'
            ELSE 'NORMAL'
        END AS anomaly_type
    FROM ticks t
    LEFT JOIN vwap v
        ON t.symbol = v.symbol
        AND TIME_BUCKET(INTERVAL '5 minutes', t.tick_time) = v.window_start
)

SELECT * FROM joined
WHERE is_anomaly = TRUE
ORDER BY tick_time DESC