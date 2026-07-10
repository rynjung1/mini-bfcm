import time
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mini_bfcm.duckdb"


def read_query(sql: str, retries: int = 5, backoff_seconds: float = 0.15) -> pd.DataFrame:
    """Run a read-only query, retrying briefly if the consumer currently
    holds the file lock for a flush. DuckDB only allows one process to hold
    the file at a time (see consumer/store.py notes) -- flushes are short,
    so a retry almost always succeeds within a couple of attempts.
    """
    last_error = None
    for attempt in range(retries):
        try:
            conn = duckdb.connect(str(DB_PATH), read_only=True)
            try:
                return conn.execute(sql).fetch_df()
            finally:
                conn.close()
        except duckdb.IOException as e:
            last_error = e
            time.sleep(backoff_seconds * (attempt + 1))
    raise last_error


def get_window_stats() -> pd.DataFrame:
    return read_query(
        "SELECT window_start, order_count, revenue, unique_customers "
        "FROM window_stats ORDER BY window_start"
    )


def get_lag_series() -> pd.DataFrame:
    return read_query(
        "SELECT recorded_at, partition, lag FROM consumer_lag ORDER BY recorded_at"
    )
