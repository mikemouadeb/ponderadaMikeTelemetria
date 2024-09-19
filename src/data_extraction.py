import logging
import pandas as pd
from connectors.supabase_connector import fetch_supabase_metrics
from connectors.clickhouse_connector import fetch_clickhouse_metrics

def fetch_all_metrics():
    """
    Fetches and combines metrics from Supabase and ClickHouse.
    Returns a combined DataFrame with metrics from both databases.
    """
    try:
        
        logging.info("Fetching metrics from Supabase...")
        supabase_metrics = fetch_supabase_metrics()

        
        logging.info("Fetching metrics from ClickHouse...")
        clickhouse_metrics = fetch_clickhouse_metrics()

      
        supabase_metrics['source'] = 'Supabase'
        clickhouse_metrics['source'] = 'ClickHouse'

        
        logging.info("Combining Supabase and ClickHouse metrics...")
        combined_metrics = pd.concat([supabase_metrics, clickhouse_metrics], ignore_index=True)

        logging.info("Metrics successfully combined.")
        return combined_metrics

    except Exception as e:
        logging.error(f"Error in fetching or combining metrics: {e}")
        raise

if __name__ == "__main__":
    metrics_df = fetch_all_metrics()
    print(metrics_df)