"""
DuckDB Storage Layer

Owns the schema and the idempotent upsert logic that turns a stream of
individual (possibly redelivered) orders into windowed aggregates.

How idempotency works:
Kafka's default delivery guarantee is at-least-once, so the same
message can be redelivered (e.g. a consumer restart between processing
a message and committing its offset). processed_orders has order_id as
its PRIMARY KEY, so re-inserting an already-seen order_id is a no-op
(ON CONFLICT DO NOTHING), and the RETURNING clause coming back empty is
exactly how a duplicate is detected -- window_stats and
window_customers are simply never touched for it. That makes replaying
a window safe: aggregates reflect each order_id exactly once no matter
how many times it's redelivered.

DuckDB only allows one process to hold the database file at a time,
even for reads, so connections here are deliberately short-lived: open,
do one batch's worth of work in a single transaction, close. That's
what leaves the dashboard's read-only connection room to get in
between flushes.
"""

import time
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mini_bfcm.duckdb"


def get_connection(db_path: Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def get_connection_with_retry(
    db_path: Path = DEFAULT_DB_PATH, retries: int = 5, backoff_seconds: float = 0.15
) -> duckdb.DuckDBPyConnection:
    """Open a writer connection, retrying briefly if the dashboard currently
    holds the file's read-only lock. DuckDB allows only one process to touch
    the file at a time (see module docstring); the dashboard's connections are
    always short-lived, so a retry almost always succeeds within a couple of
    attempts -- the same approach dashboard/db.py uses on the read side.
    """
    last_error = None
    for attempt in range(retries):
        try:
            return get_connection(db_path)
        except duckdb.IOException as e:
            last_error = e
            time.sleep(backoff_seconds * (attempt + 1))
    raise last_error


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    # Dedup table: the primary key on order_id is what makes a Kafka
    # redelivery a no-op instead of a double count.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_orders (
            order_id VARCHAR PRIMARY KEY,
            window_start BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS window_stats (
            window_start BIGINT PRIMARY KEY,
            order_count BIGINT NOT NULL DEFAULT 0,
            revenue DOUBLE NOT NULL DEFAULT 0,
            unique_customers BIGINT NOT NULL DEFAULT 0
        )
        """
    )
    # One row per (window, customer): existence, not a counter, is what
    # lets us tell "seen this customer in this window before" from "new".
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS window_customers (
            window_start BIGINT NOT NULL,
            customer_id VARCHAR NOT NULL,
            PRIMARY KEY (window_start, customer_id)
        )
        """
    )
    # Time series, not a single current value: the dashboard needs to plot
    # lag climbing during a spike and draining afterward, not just a snapshot.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS consumer_lag (
            recorded_at DOUBLE NOT NULL,
            partition INTEGER NOT NULL,
            lag BIGINT NOT NULL
        )
        """
    )


def _apply_order(conn: duckdb.DuckDBPyConnection, order: dict, window_start: int) -> dict | None:
    """Fold one order into its window's aggregates. Caller owns the transaction.

    Returns the window's updated stats, or None if `order_id` was already
    processed before (a Kafka redelivery), in which case nothing changed.
    """
    inserted = conn.execute(
        """
        INSERT INTO processed_orders (order_id, window_start) VALUES (?, ?)
        ON CONFLICT (order_id) DO NOTHING
        RETURNING order_id
        """,
        [order["order_id"], window_start],
    ).fetchone()
    if inserted is None:
        return None

    conn.execute(
        """
        INSERT INTO window_stats (window_start, order_count, revenue, unique_customers)
        VALUES (?, 1, ?, 0)
        ON CONFLICT (window_start) DO UPDATE SET
            order_count = window_stats.order_count + 1,
            revenue = window_stats.revenue + excluded.revenue
        """,
        [window_start, order["total"]],
    )

    conn.execute(
        "INSERT INTO window_customers (window_start, customer_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
        [window_start, order["customer_id"]],
    )
    unique_customers = conn.execute(
        "SELECT COUNT(*) FROM window_customers WHERE window_start = ?", [window_start]
    ).fetchone()[0]
    conn.execute(
        "UPDATE window_stats SET unique_customers = ? WHERE window_start = ?",
        [unique_customers, window_start],
    )

    row = conn.execute(
        "SELECT order_count, revenue, unique_customers FROM window_stats WHERE window_start = ?",
        [window_start],
    ).fetchone()
    return {"order_count": row[0], "revenue": row[1], "unique_customers": row[2]}


def upsert_order(conn: duckdb.DuckDBPyConnection, order: dict, window_start: int) -> dict | None:
    """Idempotently fold a single order into its window's aggregates in its own transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
        result = _apply_order(conn, order, window_start)
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


def upsert_batch(conn: duckdb.DuckDBPyConnection, batch: list[tuple[dict, int]]) -> tuple[dict[int, dict], int]:
    """Idempotently fold a batch of (order, window_start) pairs into aggregates
    in a single transaction, so the file lock is held for one flush instead of
    once per message.

    Returns (latest stats per window_start that changed, count of orders in
    this batch that were already-processed duplicates). Note results has at
    most one entry per distinct window_start touched, even if many orders in
    the batch landed in the same window -- that's normal batch collapsing,
    not a sign of duplicates.
    """
    results: dict[int, dict] = {}
    duplicate_count = 0
    conn.execute("BEGIN TRANSACTION")
    try:
        for order, window_start in batch:
            stats = _apply_order(conn, order, window_start)
            if stats is not None:
                results[window_start] = stats
            else:
                duplicate_count += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return results, duplicate_count


def prune_processed_orders(conn: duckdb.DuckDBPyConnection, older_than_window_start: int) -> None:
    """Delete dedup records for windows older than `older_than_window_start`.

    processed_orders exists only to make a Kafka redelivery a no-op; a
    redelivery can only reference a window recent enough that the consumer
    might still replay it after a restart, so entries far older than that
    carry no useful information and would otherwise grow the table forever.
    """
    conn.execute("DELETE FROM processed_orders WHERE window_start < ?", [older_than_window_start])


def prune_consumer_lag(conn: duckdb.DuckDBPyConnection, older_than: float) -> None:
    """Delete lag readings recorded before `older_than` (unix timestamp).

    consumer_lag is pure observability telemetry, recorded every flush
    forever -- unlike window_stats (the actual aggregate output this
    project produces), there's no reason to keep it indefinitely. Left
    unpruned, it's the fastest-growing table (one row per partition per
    flush interval), and the dashboard does a full unfiltered read of it
    on every poll (see dashboard/db.py) -- so letting it grow forever
    would make the dashboard progressively slower the longer the pipeline
    stays up.
    """
    conn.execute("DELETE FROM consumer_lag WHERE recorded_at < ?", [older_than])


def record_lag(conn: duckdb.DuckDBPyConnection, recorded_at: float, partition_lags: dict[int, int]) -> None:
    if not partition_lags:
        return  # no partition assignment yet (e.g. still joining the consumer group)
    conn.executemany(
        "INSERT INTO consumer_lag (recorded_at, partition, lag) VALUES (?, ?, ?)",
        [(recorded_at, partition, lag) for partition, lag in partition_lags.items()],
    )
