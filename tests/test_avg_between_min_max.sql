SELECT *
FROM {{ ref('crypto_prices') }}
WHERE avg_btc_price < min_btc_price
   OR avg_btc_price > max_btc_price