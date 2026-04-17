SELECT
    data:bitcoin:usd::float AS btc_price,
    data:ethereum:usd::float AS eth_price,
    ingestion_time,
    CONVERT_TIMEZONE('UTC', 'Europe/Warsaw', ingestion_time) AS ingestion_time_pl
FROM {{ source('raw', 'raw_crypto') }}
WHERE data:bitcoin:usd IS NOT NULL