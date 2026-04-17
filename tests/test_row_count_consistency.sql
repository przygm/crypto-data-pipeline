WITH raw_counts AS (
    SELECT
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Warsaw', ingestion_time)) AS day,
        COUNT(*) AS cnt
    FROM {{ source('raw', 'raw_crypto') }}
    GROUP BY 1
)

SELECT
    cp.day,
    cp.row_count,
    rc.cnt
FROM {{ ref('crypto_prices') }} cp
JOIN raw_counts rc
    ON cp.day = rc.day
WHERE cp.row_count != rc.cnt