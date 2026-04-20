import os

import streamlit as st
import pandas as pd
import altair as alt

from snowflake_conn import get_connection

st.set_page_config(layout="wide", page_title="Crypto Dashboard")

# load .env (lokalnie)
if os.path.exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

@st.cache_data(ttl=3600)
def load_data():
    conn = get_connection("analytics")

    query = """
    SELECT
        day,
        avg_btc_price,
        min_btc_price,
        max_btc_price
    FROM crypto_prices
    ORDER BY day
    """

    df = pd.read_sql(query, conn)
    conn.close()

    return df


df = load_data()

# ======================
# DASHBOARD
# ======================

col_title, col_refresh = st.columns([5, 1]) 

with col_title:
    st.title("Bitcoin Price History (Min/Avg/Max)")
    st.caption("Data source: Snowflake Analytics Layer | Automatically updated every 1 hour")

with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True) 
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Latest Avg Price", 
        value=f"{df['AVG_BTC_PRICE'].iloc[-1]:,.2f} USD"
    )

with col2:
    if len(df) > 1:
        delta = df['AVG_BTC_PRICE'].iloc[-1] - df['AVG_BTC_PRICE'].iloc[-2]
        st.metric(label="24h Change", value=f"{delta:,.2f} USD", delta=f"{delta:,.2f}")

with col3:
    st.metric(
        label="Day Max", 
        value=f"{df['MAX_BTC_PRICE'].iloc[-1]:,.2f} USD"
    )



df_melted = df.melt(
    id_vars=["DAY"], 
    value_vars=["MIN_BTC_PRICE", "AVG_BTC_PRICE", "MAX_BTC_PRICE"],
    var_name="Price Type", 
    value_name="Price"
)

chart = alt.Chart(df_melted).mark_line(point=True).encode(
    x=alt.X(
        "DAY:T", 
        title="Date",
        # Wymuszamy pokazywanie tylko tych dni, które są w danych
        axis=alt.Axis(values=df["DAY"].tolist(), format="%Y-%m-%d", labelAngle=-45)
    ),
    y=alt.Y(
        "Price:Q", 
        title="BTC Price (USD)",
        scale=alt.Scale(zero=False)
    ),
    color=alt.Color(
        "Price Type:N", 
        legend=alt.Legend(title="Price Types"),
        scale=alt.Scale(
            domain=["MIN_BTC_PRICE", "AVG_BTC_PRICE", "MAX_BTC_PRICE"],
            range=["#1f77b4", "#FFA500", "#d62728"] # Niebieski, Pomarańczowy, Czerwony
        )
    ),
    tooltip=["DAY:T", "Price Type:N", "Price:Q"]
).interactive()

st.altair_chart(chart, use_container_width=True)

# table
st.subheader("Raw data")
st.dataframe(df.tail(20))