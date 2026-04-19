import streamlit as st
import snowflake.connector
import pandas as pd
import os
import altair as alt
from snowflake_conn import get_connection

# load .env (lokalnie)
if os.path.exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

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

# ======================
# DASHBOARD
# ======================

st.title("Bitcoin Price History (Min/Avg/Max)")

# 1. Przygotowanie danych do legendy (zamiana kolumn na wiersze)
# Dzięki temu Altair automatycznie stworzy legendę z kolorami
df_melted = df.melt(
    id_vars=["DAY"], 
    value_vars=["MIN_BTC_PRICE", "AVG_BTC_PRICE", "MAX_BTC_PRICE"],
    var_name="Price Type", 
    value_name="Price"
)

# 2. Tworzenie wykresu
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