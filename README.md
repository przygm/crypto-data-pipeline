# Crypto Data Pipeline (Snowflake)

## Overview
End-to-end data pipeline that ingests cryptocurrency data from API, stores it in Snowflake, transforms it using dbt, and visualizes results in a dashboard.

The project demonstrates a full data engineering workflow: ingestion, storage, transformation, orchestration, and visualization.

## Tech stack
- Python (API ingestion, retry logic, logging)
- Snowflake (data warehouse, RAW + ANALYTICS layers)
- dbt (transformations, tests)
- GitHub Actions (pipeline automation)
- Streamlit (dashboard)

## Architecture
Data flow:
- CoinGecko API
- Python ingestion script
- Snowflake stage (@%raw_crypto)
- RAW table: CRYPTO_RAW.PUBLIC.RAW_CRYPTO
- dbt models: stg_crypto -> crypto_prices
- ANALYTICS table: CRYPTO_ANALYTICS.DBT_PZYGMUNT.CRYPTO_PRICES
- Streamlit dashboard

## Dashboard
Live dashboard:
https://crypto-data-pipeline-dashboard.streamlit.app/

## Features
- Resilient API ingestion (retry logic, error handling)
- JSON data loading to Snowflake (PUT + COPY INTO)
- Separation of RAW and ANALYTICS layers
- Incremental transformations in dbt
- Data quality tests (dbt test)
- Logging (Python + GitHub logs)
- Automated pipeline execution
- Interactive dashboard (BTC min / avg / max price)

## Local setup
Install dependencies:
pip install -r requirements.txt

Create `.env` file in the root directory:

```env
SNOWFLAKE_USER=twoj_user
SNOWFLAKE_PASSWORD=twoje_haslo
SNOWFLAKE_ACCOUNT=twoj_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_RAW_DATABASE=CRYPTO_RAW
SNOWFLAKE_RAW_SCHEMA=PUBLIC
SNOWFLAKE_ANALYTICS_DATABASE=CRYPTO_ANALYTICS
SNOWFLAKE_ANALYTICS_SCHEMA=DBT_PZYGMUNT

Example .env file is provided as .env.example

Run pipeline locally:
python ingest_crypto.py
dbt run
dbt test

Run dashboard:
streamlit run dashboard.py

## Windows scheduling
Pipeline can be executed locally using a .bat file and Windows Task Scheduler.

Run:
run_pipeline.bat

## Automation (GitHub Actions)
- Pipeline is automated using GitHub Actions
- Uses GitHub Secrets for sensitive credentials (user, password, account, warehouse)
- Runs ingestion, dbt models and tests
- Scheduled execution via cron (best-effort timing, not guaranteed exact hourly execution)

## Notes
- ingestion_time is stored in UTC (best practice for pipelines)
- Timezone adjustments are applied in dbt layer
- RAW and ANALYTICS layers are separated for clarity and scalability