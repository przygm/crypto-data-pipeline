{{ config(
    materialized='incremental',
    unique_key='day',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('stg_crypto') }}

    {% if is_incremental() %}
    WHERE DATE(ingestion_time) >= ( SELECT MAX(day) FROM {{ this }} )
    {% endif %}

)

SELECT
    DATE(ingestion_time) AS day,
    COUNT(*) AS row_count,
    AVG(btc_price) AS avg_btc_price,
    MAX(btc_price) AS max_btc_price,
    MIN(btc_price) AS min_btc_price
FROM source_data
GROUP BY 1