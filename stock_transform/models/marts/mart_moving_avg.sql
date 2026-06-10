-- 5-period and 20-period moving averages per symbol

WITH vwap AS (
    SELECT * FROM {{ ref('mart_vwap') }}
),

with_moving_avgs AS (
    SELECT
        symbol,
        tick_date,
        window_start,
        vwap,
        total_volume,
        AVG(vwap) OVER (
            PARTITION BY symbol
            ORDER BY window_start
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5,
        AVG(vwap) OVER (
            PARTITION BY symbol
            ORDER BY window_start
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        -- Golden cross signal: MA5 crosses above MA20
        CASE
            WHEN AVG(vwap) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
               > AVG(vwap) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
            THEN 'BULLISH'
            ELSE 'BEARISH'
        END AS signal
    FROM vwap
)

SELECT * FROM with_moving_avgs
ORDER BY symbol, window_start