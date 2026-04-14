# Crypto Data Pipeline (Snowflake)

## Overview
This project implements an end-to-end data pipeline:
- Data ingestion from CoinGecko API
- Storage in Snowflake
- Incremental processing (planned with dbt)

## Tech stack
- Python
- Snowflake
- dbt (planned)

## Features
- API ingestion
- JSON data handling
- Snowflake loading

## Setup
1. Install dependencies:
   pip install -r requirements.txt

2. Create .env file with Snowflake credentials.

3. Run ingestion script:
   python ingest_crypto.py