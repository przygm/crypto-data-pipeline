import requests
import json
from datetime import datetime, UTC
import snowflake.connector
import os
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# load .env (only for 'local' call from windows)
if os.path.exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

logging.info("Starting ingestion")

url = "https://api.coingecko.com/api/v3/simple/price"
params = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "usd",
    "include_last_updated_at": "true"
}

MAX_RETRIES = 3
data = None

for attempt in range(MAX_RETRIES):
    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            logging.info("API request successful")
            break
        else:
            logging.warning(f"API error: {response.status_code}")

    except Exception as e:
        logging.error(f"Request failed: {e}")

    if attempt < MAX_RETRIES - 1:
        time.sleep(5)
else:
    logging.error("API failed after retries")
    raise Exception("API failed after retries")

if not data or not data.get("bitcoin") or not data.get("ethereum"):
    logging.error("Missing crypto data in API response")
    raise Exception("Missing crypto data")

# add timestamp ingestion 
record = {
    "ingestion_time": datetime.now(UTC).isoformat(),
    "data": data
}

# save to file (into raw folder)
os.makedirs("raw", exist_ok=True)

filename = f"raw/crypto_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
with open(filename, "w") as f:
    json.dump(record, f)

logging.info(f"File saved: {filename}")

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_RAW_DATABASE"),
    schema=os.getenv("SNOWFLAKE_RAW_SCHEMA")
)

cs = None
try:
    cs = conn.cursor()

    logging.info("Uploading file to Snowflake stage")
    #cs.execute(f"PUT file://{os.path.abspath(filename)} @{os.getenv('SNOWFLAKE_RAW_DATABASE')}.{os.getenv('SNOWFLAKE_RAW_SCHEMA')}.%raw_crypto")
    cs.execute(f"PUT file://{os.path.abspath(filename)} @%raw_crypto")

    logging.info("Copying data into raw table")
    cs.execute("""
    COPY INTO raw_crypto
    FROM (
        SELECT
            $1:data,
            $1:ingestion_time::timestamp
        FROM @%raw_crypto
    )
    FILE_FORMAT = (TYPE = 'JSON')
    """)

    logging.info("Data successfully loaded to Snowflake")

except Exception as e:
    logging.error(f"Snowflake error: {e}")
    raise

finally:
    if cs:
        cs.close()
    conn.close()
    logging.info("Connection closed")