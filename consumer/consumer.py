"""
Mini BFCM Order Consumer

Reads order events off the Kafka topic, folds them into 10-second
tumbling window aggregates in DuckDB, and tracks consumer lag -- how
far behind the consumer is from the latest message Kafka has for each
partition.

How one iteration of the poll loop works:
1. Poll Kafka for the next message (0.2s timeout). If one arrives,
   parse it, work out which 10s window it belongs to
   (window_start_for), and append it to an in-memory buffer -- nothing
   touches the database yet.
2. Once FLUSH_INTERVAL_SECONDS has elapsed, flush(): open a DuckDB
   connection, upsert the whole buffered batch in a single transaction
   (idempotent by order_id -- see store.py), record the current
   per-partition lag, then close the connection.
3. Commit Kafka offsets only for the messages that just made it into
   that batch, and only after the DuckDB write succeeded. Then repeat.

Offsets are committed manually (enable.auto.commit=False) rather than
on Kafka's own timer, because auto-commit can mark a message "done"
before this consumer has actually durably written it -- a crash
between those two points would silently lose that message instead of
it being redelivered.

Flushing happens even when the buffer is empty, so the lag time series
stays continuous (e.g. sitting at 0 while idle) instead of having gaps
whenever there's no traffic.
"""

import argparse
import json
import time

import duckdb
from confluent_kafka import Consumer, KafkaException, TopicPartition

import store
from windowing import window_start_for

FLUSH_INTERVAL_SECONDS = 1.0
# How often to sweep old dedup records out of processed_orders, and how far
# back a redelivery could plausibly still land -- see store.prune_processed_orders.
PRUNE_INTERVAL_SECONDS = 300.0
DEDUP_RETENTION_SECONDS = 3600.0
# consumer_lag is pure observability telemetry (recorded every flush,
# forever), not aggregate output worth keeping indefinitely like
# window_stats. Pruned on the same timer as the dedup table so the
# dashboard's read query doesn't get slower every day the pipeline stays up.
LAG_RETENTION_SECONDS = 86400.0


def make_consumer(bootstrap_servers: str, group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            # We commit manually, after a message's effects are durably
            # applied, rather than on a timer. See CLAUDE.md / consumer
            # design notes for why: auto-commit can mark a message "done"
            # before we've actually processed it.
            "enable.auto.commit": False,
        }
    )


def measure_lag(consumer: Consumer) -> dict[int, int]:
    """Lag per partition: how many produced messages the consumer hasn't read yet.

    high_watermark is the offset of the newest message Kafka has for the
    partition; position is how far this consumer has actually read. The gap
    between them is lag -- it climbs when the producer outpaces the consumer
    and drains once the consumer catches back up.
    """
    lags = {}
    for tp in consumer.assignment():
        low, high = consumer.get_watermark_offsets(tp, timeout=1.0, cached=False)
        position = consumer.position([tp])[0].offset
        if position < 0:  # no messages read from this partition yet this run
            position = low
        lags[tp.partition] = max(high - position, 0)
    return lags


def flush(
    db_path,
    buffer: list,
    consumer: Consumer,
    prune_before_window_start: int | None = None,
    lag_prune_before: float | None = None,
) -> None:
    """Open a short-lived DuckDB connection, write the buffered batch and the
    current lag reading, then close it.

    DuckDB only allows one process to hold the database file at a time, even
    for reads (see store.py / concurrency notes) -- so the consumer only ever
    holds the connection for the duration of a flush, not for its whole
    lifetime. That's what leaves the dashboard room to read in between. The
    connection is opened with retry (get_connection_with_retry) since the
    dashboard's own short-lived read connection can occasionally win the race
    for the file lock first.
    """
    partition_lags = measure_lag(consumer)

    db = store.get_connection_with_retry(db_path)
    try:
        if buffer:
            results, duplicate_count = store.upsert_batch(db, buffer)
        else:
            results, duplicate_count = {}, 0
        store.record_lag(db, time.time(), partition_lags)
        if prune_before_window_start is not None:
            store.prune_processed_orders(db, prune_before_window_start)
        if lag_prune_before is not None:
            store.prune_consumer_lag(db, lag_prune_before)
    finally:
        db.close()

    for window_start, stats in sorted(results.items()):
        print(
            f"[window {window_start}] "
            f"orders={stats['order_count']} revenue=${stats['revenue']:.2f} "
            f"unique_customers={stats['unique_customers']}"
        )
    if duplicate_count:
        print(f"({duplicate_count} duplicate deliveries skipped in this batch)")
    if partition_lags:
        lag_str = ", ".join(f"p{p}={lag}" for p, lag in sorted(partition_lags.items()))
        print(f"lag: {lag_str}")


def run(bootstrap_servers: str, topic: str, group_id: str, from_beginning: bool, process_delay_ms: float) -> None:
    consumer = make_consumer(bootstrap_servers, group_id, from_beginning)
    consumer.subscribe([topic])

    # get_connection_with_retry, not get_connection: the dashboard could
    # already be holding the file's read lock the instant this starts up,
    # same as any other connection this process opens (see flush()).
    db = store.get_connection_with_retry()
    store.init_schema(db)
    db.close()

    if from_beginning:
        print(
            f"note: --from-beginning replays the whole topic, but duplicate-order "
            f"detection (processed_orders) only retains the last "
            f"{DEDUP_RETENTION_SECONDS / 3600:.0f}h (see store.prune_processed_orders) "
            f"-- if this DB already has data older than that, replayed orders from "
            f"those windows will double-count into window_stats"
        )

    print(f"consuming from '{topic}' as group '{group_id}', flushing every {FLUSH_INTERVAL_SECONDS}s (Ctrl+C to stop)")
    received = 0
    skipped = 0
    buffer: list[tuple[dict, int]] = []
    last_msg_per_partition: dict[int, object] = {}
    last_flush = time.monotonic()
    last_prune = time.monotonic()

    def do_flush():
        nonlocal buffer, last_flush, last_prune
        # Always flush, even with an empty buffer: lag needs to be recorded
        # continuously (e.g. sitting at 0 while idle), not just when there's
        # new data to write, or the dashboard's lag chart would have gaps.
        prune_before = None
        lag_prune_before = None
        if time.monotonic() - last_prune >= PRUNE_INTERVAL_SECONDS:
            prune_before = window_start_for(time.time() - DEDUP_RETENTION_SECONDS)
            lag_prune_before = time.time() - LAG_RETENTION_SECONDS
            last_prune = time.monotonic()
        flush(
            store.DEFAULT_DB_PATH,
            buffer,
            consumer,
            prune_before_window_start=prune_before,
            lag_prune_before=lag_prune_before,
        )
        if buffer:
            offsets = [
                TopicPartition(topic, partition, msg.offset() + 1)
                for partition, msg in last_msg_per_partition.items()
            ]
            consumer.commit(offsets=offsets, asynchronous=False)
            buffer = []
        last_flush = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=0.2)
            if msg is not None:
                if msg.error():
                    print(f"consumer error: {msg.error()}")
                else:
                    if process_delay_ms:
                        # Simulates realistic per-message processing cost
                        # (e.g. an enrichment call, writing to multiple
                        # sinks) that this toy buffer-append doesn't have on
                        # its own -- without it, a local consumer processes
                        # even a 100x spike faster than one flush interval
                        # and lag never has a chance to show anything.
                        time.sleep(process_delay_ms / 1000)
                    # Parsing/validating is isolated from everything else in
                    # this iteration: a single malformed message (bad JSON,
                    # or missing/wrong-typed fields) must not crash the whole
                    # consumer. Before this guard existed, an unreadable
                    # message here would raise uncaught, kill the process,
                    # and -- since offsets are only committed after a
                    # successful flush -- get redelivered and crash it again
                    # on every restart. Log and move on instead.
                    try:
                        order = json.loads(msg.value())
                        window_start = window_start_for(order["timestamp"])
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        skipped += 1
                        print(f"skipping unreadable message (partition {msg.partition()} offset {msg.offset()}): {e}")
                    else:
                        received += 1
                        buffer.append((order, window_start))
                    # Advance the commit point for this partition either way
                    # -- once logged, a skipped message shouldn't be
                    # redelivered forever.
                    last_msg_per_partition[msg.partition()] = msg

            if time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                try:
                    do_flush()
                except (duckdb.IOException, KafkaException) as e:
                    # duckdb.IOException: get_connection_with_retry already
                    # absorbs brief lock contention with the dashboard; this
                    # only fires if the file was locked longer than that
                    # retry budget. KafkaException: e.g. commit() failing
                    # mid-rebalance. Both are transient -- log and try again
                    # next cycle instead of taking the whole consumer down.
                    # Safe to retry: upsert_batch is idempotent, and the
                    # buffer isn't cleared until commit() actually succeeds.
                    print(f"flush skipped, will retry next cycle: {e}")
    except KeyboardInterrupt:
        print("stopping...")
        try:
            do_flush()
        except (duckdb.IOException, KafkaException) as e:
            print(f"final flush skipped: {e}")
    finally:
        consumer.close()
        summary = f"done, received {received} orders total"
        if skipped:
            summary += f", skipped {skipped} unreadable"
        print(summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mini BFCM order consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--group-id", default="mini-bfcm-consumer")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="start from the earliest offset instead of only new messages",
    )
    parser.add_argument(
        "--process-delay-ms",
        type=float,
        default=0.0,
        help="artificial per-message processing delay, to simulate realistic downstream cost so lag is visible under a spike",
    )
    args = parser.parse_args()

    run(args.bootstrap_servers, args.topic, args.group_id, args.from_beginning, args.process_delay_ms)
