import streamlit as st
import pandas as pd
from connectors.prometheus_connector import fetch_prometheus_metrics
import logging

logging.basicConfig(level=logging.INFO)

st.title("Telemetry Dashboard")

def display_metrics(metrics_df, source):
    if metrics_df.empty:
        st.warning(f"No data found for {source}.")
    else:
        st.write(metrics_df)

try:
    logging.info("Fetching Supabase metrics...")
    supabase_metrics = fetch_prometheus_metrics('pg_stat_user_tables')  
    st.subheader("Supabase Metrics")
    display_metrics(supabase_metrics, "Supabase")
except Exception as e:
    logging.error(f"Error fetching Supabase metrics: {e}")
    st.error(f"Error fetching Supabase metrics: {e}")

try:
    logging.info("Fetching ClickHouse metrics...")
    clickhouse_metrics = fetch_prometheus_metrics('clickhouse_table_size_bytes')  
    st.subheader("ClickHouse Metrics")
    display_metrics(clickhouse_metrics, "ClickHouse")
except Exception as e:
    logging.error(f"Error fetching ClickHouse metrics: {e}")
    st.error(f"Error fetching ClickHouse metrics: {e}")