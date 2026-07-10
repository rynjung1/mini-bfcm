from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mini_bfcm.duckdb"


def get_connection(db_path: Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


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
    try:
        conn.execute(
            "INSERT INTO processed_orders (order_id, window_start) VALUES (?, ?)",
            [order["order_id"], window_start],
        )
    except duckdb.ConstraintException:
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


def record_lag(conn: duckdb.DuckDBPyConnection, recorded_at: float, partition_lags: dict[int, int]) -> None:
    if not partition_lags:
        return  # no partition assignment yet (e.g. still joining the consumer group)
    conn.executemany(
        "INSERT INTO consumer_lag (recorded_at, partition, lag) VALUES (?, ?, ?)",
        [(recorded_at, partition, lag) for partition, lag in partition_lags.items()],
    )
