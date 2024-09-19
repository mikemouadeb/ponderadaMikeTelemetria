import logging
import clickhouse_connect
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.config.config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB

def connect_to_clickhouse():
    """Connect to ClickHouse."""
    logging.info("Connecting to ClickHouse...")
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
            secure=True,
            verify=False
        )
        logging.info("Connection to ClickHouse established.")
        return client
    except Exception as e:
        logging.error(f"Error connecting to ClickHouse: {e}")
        raise

def fetch_clickhouse_metrics():
    """Fetch metrics from ClickHouse."""
    client = connect_to_clickhouse()
    query = "SELECT * FROM grupox"  
    try:
        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)  
        return df
    except Exception as e:
        logging.error(f"Error fetching data from ClickHouse: {e}")
        raise