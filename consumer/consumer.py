import argparse
import json
import time

from confluent_kafka import Consumer, TopicPartition

import store
from windowing import window_start_for

FLUSH_INTERVAL_SECONDS = 1.0


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


def flush(db_path, buffer: list, consumer: Consumer) -> None:
    """Open a short-lived DuckDB connection, write the buffered batch and the
    current lag reading, then close it.

    DuckDB only allows one process to hold the database file at a time, even
    for reads (see store.py / concurrency notes) -- so the consumer only ever
    holds the connection for the duration of a flush, not for its whole
    lifetime. That's what leaves the dashboard room to read in between.
    """
    partition_lags = measure_lag(consumer)

    db = store.get_connection(db_path)
    try:
        if buffer:
            results, duplicate_count = store.upsert_batch(db, buffer)
        else:
            results, duplicate_count = {}, 0
        store.record_lag(db, time.time(), partition_lags)
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

    db = store.get_connection()
    store.init_schema(db)
    db.close()

    print(f"consuming from '{topic}' as group '{group_id}', flushing every {FLUSH_INTERVAL_SECONDS}s (Ctrl+C to stop)")
    received = 0
    buffer: list[tuple[dict, int]] = []
    last_msg_per_partition: dict[int, object] = {}
    last_flush = time.monotonic()

    def do_flush():
        nonlocal buffer, last_flush
        # Always flush, even with an empty buffer: lag needs to be recorded
        # continuously (e.g. sitting at 0 while idle), not just when there's
        # new data to write, or the dashboard's lag chart would have gaps.
        flush(store.DEFAULT_DB_PATH, buffer, consumer)
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
                    order = json.loads(msg.value())
                    received += 1
                    window_start = window_start_for(order["timestamp"])
                    buffer.append((order, window_start))
                    last_msg_per_partition[msg.partition()] = msg

            if time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                do_flush()
    except KeyboardInterrupt:
        print("stopping...")
        do_flush()
    finally:
        consumer.close()
        print(f"done, received {received} orders total")


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
