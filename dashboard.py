# --- Standard Library Imports ---
import os
import sys
import subprocess
import time
import warnings

# --- Third-Party Library Imports ---
import pandas as pd
import altair as alt
import streamlit as st
from streamlit_javascript import st_javascript

# --- Local Module Imports ---
from snowflake_conn import get_connection

#------------------------------------------------------------------------------------- 
# --- CONFIGURATION & WARNINGS ---

warnings.filterwarnings(
    "ignore", 
    category=UserWarning, 
    message=".*pandas only supports SQLAlchemy connectable.*"
)

st.set_page_config(layout="wide", page_title="Crypto Dashboard")

APP_MODE = "PROD"   #only "PROD" will trigger real scripts
#------------------------------------------------------------------------------------- 
# --- ENVIRONMENT & METADATA ---

# Load .env (locally)
if os.path.exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

# Get Client IP for monitoring
client_ip = st_javascript("await fetch('https://api.ipify.org?format=json').then(res => res.json()).then(res => res.ip)")

# Auto-refresh meta tag
st.markdown(
    '<meta http-equiv="refresh" content="3600">', 
    unsafe_allow_html=True
)

#------------------------------------------------------------------------------------- 
# --- DATABASE FUNCTIONS ---

def get_last_update():
    conn = get_connection("analytics")
    query = "SELECT MAX(day) as last_day FROM crypto_prices"
    df = pd.read_sql(query, conn)
    conn.close()
    return df["LAST_DAY"].iloc[0]

#------------------------------------------------------------------------------------- 

@st.cache_data
def load_data(last_update):
    conn = get_connection("analytics")
    query = """
    SELECT day, row_count as ingest_records_count, avg_btc_price, min_btc_price, max_btc_price
    FROM crypto_prices
    ORDER BY day desc
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#------------------------------------------------------------------------------------- 
# --- DIALOGS & PIPELINE LOGIC ---

@st.dialog("Data Pipeline Execution")
def run_pipeline_dialog(ip):
    st.write(f"Executing pipeline...")
    
    with st.status("Running pipeline steps...", expanded=True) as status:
        try:
            st.write("📥 Requesting data from CoinGecko API...")

            if APP_MODE == "PROD":
                ingest_proc = subprocess.run([sys.executable, "ingest_crypto.py", ip], capture_output=True, text=True)
            else:
                time.sleep(2) 
                class MockIngest: returncode = 0; stderr = "API Rate Limit 429"; stdout = "Success"
                ingest_proc = MockIngest()

            if ingest_proc.returncode != 0:
                st.error("❌ Ingestion Stage Failed!")
                if "429" in ingest_proc.stderr:
                    st.warning("Rate limit exceeded (HTTP 429). Please wait or use a VPN.")
                st.code(ingest_proc.stderr, language="bash")
                status.update(label="Pipeline Failed at Ingestion", state="error")
                return

            st.write("✅ Ingestion successful.")

            st.write("⚙️ Installing dbt dependencies...")
            
            
            if APP_MODE == "PROD":
                deps_proc = subprocess.run("dbt deps --profiles-dir .", shell=True, capture_output=True, text=True)
            else:    
                time.sleep(2)

            st.write("✅ dbt dependencies installed.")

            st.write("🏗️ Running dbt transformations in Snowflake...")
            
            if APP_MODE == "PROD":
                dbt_proc = subprocess.run("dbt run --profiles-dir .", shell=True, capture_output=True, text=True)
            else:
                time.sleep(2)
                class MockDbt: returncode = 0; stderr = ""; stdout = "All models passed"
                dbt_proc = MockDbt()

            if dbt_proc.returncode != 0:
                st.error("❌ dbt Transformation Failed!")
                st.code(dbt_proc.stderr, language="bash")
                status.update(label="Pipeline Failed at dbt", state="error")
                return

            # --- SUCCESS ---
            st.write(dbt_proc.stdout)
            status.update(
                label="🚀 Pipeline finished successfully!\n\n You can close this window and click **🔄 Refresh**.", 
                state="complete", 
                expanded=False
            )
            st.success("Data updated in Snowflake (Simulation).")

        except Exception as e:
            st.error(f"⚠️ An unexpected error occurred: {str(e)}")
            status.update(label="Critical System Error", state="error")
#------------------------------------------------------------------------------------- 
# --- DATA PREPARATION ---

last_update = get_last_update()
df = load_data(last_update)
df_display = df.head(20).reset_index(drop=True)   
df_chart = df.sort_values("DAY")

df_melted = df_chart.melt(
    id_vars=["DAY"], 
    value_vars=["MIN_BTC_PRICE", "AVG_BTC_PRICE", "MAX_BTC_PRICE"],
    var_name="Price Type", 
    value_name="Price"
)

#------------------------------------------------------------------------------------- 
# --- MAIN DASHBOARD UI ---

col_title, col_refresh, col_ingest = st.columns([5, 1, 1]) 

with col_title:
    st.title("Bitcoin Price History (Min/Avg/Max)")
    st.caption("Data source: Snowflake Analytics Layer | Updated hourly")

with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True) 
    if st.button("🔄 Refresh", width='stretch'):
        load_data.clear()
        st.rerun()

with col_ingest:
    st.markdown("<br>", unsafe_allow_html=True) 
    if st.button("🚀 Run pipeline", width='stretch'):
        if client_ip:
            run_pipeline_dialog(client_ip)
        else:
            st.warning("Detecting IP... please wait.")

# --- Key Metrics ---
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(label="Latest Avg Price", value=f"{df['AVG_BTC_PRICE'].iloc[0]:,.2f} USD")

with col2:
    if len(df) > 1:
        delta = df['AVG_BTC_PRICE'].iloc[0] - df['AVG_BTC_PRICE'].iloc[1]
        st.metric(label="24h Change", value=f"{delta:,.2f} USD", delta=f"{delta:,.2f}")

with col3:
    st.metric(label="Day Max", value=f"{df['MAX_BTC_PRICE'].iloc[0]:,.2f} USD")

#------------------------------------------------------------------------------------- 
# --- VISUALIZATIONS ---

chart = alt.Chart(df_melted).mark_line(point=True).encode(
    x=alt.X("DAY:T", title="Date", axis=alt.Axis(values=df["DAY"].tolist(), format="%Y-%m-%d", labelAngle=-45)),
    y=alt.Y("Price:Q", title="BTC Price (USD)", scale=alt.Scale(zero=False)),
    color=alt.Color("Price Type:N", scale=alt.Scale(domain=["MIN_BTC_PRICE", "AVG_BTC_PRICE", "MAX_BTC_PRICE"], range=["#1f77b4", "#FFA500", "#d62728"])),
    tooltip=["DAY:T", "Price Type:N", "Price:Q"]
).interactive()

st.altair_chart(chart, width='stretch')

st.subheader("Raw data")
st.dataframe(df_display, width='stretch', hide_index=True)