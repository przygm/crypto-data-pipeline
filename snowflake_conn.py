import snowflake.connector
import os

def get_connection(layer):
    if layer == "raw":
        database = os.getenv("SNOWFLAKE_RAW_DATABASE")
        schema = os.getenv("SNOWFLAKE_RAW_SCHEMA")
    elif layer == "analytics":
        database = os.getenv("SNOWFLAKE_ANALYTICS_DATABASE")
        schema = os.getenv("SNOWFLAKE_ANALYTICS_SCHEMA")
    else:
        raise ValueError("Invalid layer")
    
    if not database or not schema:
        raise ValueError(f"Missing config for layer: {layer}")

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=database,
        schema=schema
    )