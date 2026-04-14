SELECT
    data:bitcoin:usd::float AS btc_price,
    data:ethereum:usd::float AS eth_price,
    ingestion_time
FROM {{ source('raw', 'raw_crypto') }}
WHERE data:bitcoin:usd IS NOT NULL