import requests
import json
from datetime import datetime, UTC
import snowflake.connector
import os

# load .env tylko lokalnie (GitHub tego nie potrzebuje)
if os.path.exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

url = "https://api.coingecko.com/api/v3/simple/price"
params = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "usd",
    "include_last_updated_at": "true"
}

response = requests.get(url, params=params)
if response.status_code != 200:
    raise Exception(f"API error: {response.status_code}")

data = response.json()
if "error" in data:
    raise Exception(f"API returned error: {data}")

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

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_RAW_DATABASE"),
    schema=os.getenv("SNOWFLAKE_RAW_SCHEMA")
)

cs = conn.cursor()

cs.execute(f"PUT file://{os.path.abspath(filename)} @{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.%raw_crypto")

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

cs.close()
conn.close()