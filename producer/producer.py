import argparse
import json
import time

from confluent_kafka import Producer

from order_generator import generate_order
from spike import SpikeProfile


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers})


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def run(bootstrap_servers: str, topic: str, profile: SpikeProfile) -> None:
    producer = make_producer(bootstrap_servers)

    print(f"producing to '{topic}' (Ctrl+C to stop)")
    sent = 0
    start = time.monotonic()
    last_phase = None
    try:
        while True:
            elapsed = time.monotonic() - start
            rate = profile.rate_at(elapsed)
            phase = profile.phase_at(elapsed)
            if phase != last_phase:
                print(f"[{elapsed:5.1f}s] phase -> {phase} ({rate:.1f} orders/sec)")
                last_phase = phase

            order = generate_order()
            producer.produce(
                topic,
                key=order["customer_id"],
                value=json.dumps(order),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if sent % 50 == 0:
                print(f"sent {sent} orders (current rate {rate:.1f}/sec)")
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        producer.flush()
        print(f"done, sent {sent} orders total")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mini BFCM order producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--rate", type=float, default=5.0, help="baseline orders/sec")
    parser.add_argument("--spike", action="store_true", help="enable flash-sale spike mode")
    parser.add_argument("--spike-multiplier", type=float, default=75.0)
    parser.add_argument("--spike-delay", type=float, default=10.0, help="seconds of baseline before the spike starts")
    parser.add_argument("--spike-ramp", type=float, default=5.0)
    parser.add_argument("--spike-hold", type=float, default=10.0)
    parser.add_argument("--spike-decay", type=float, default=5.0)
    args = parser.parse_args()

    if args.spike:
        profile = SpikeProfile(
            baseline_rate=args.rate,
            multiplier=args.spike_multiplier,
            delay_seconds=args.spike_delay,
            ramp_seconds=args.spike_ramp,
            hold_seconds=args.spike_hold,
            decay_seconds=args.spike_decay,
        )
    else:
        # multiplier=1 means rate_at() always returns the baseline rate,
        # so spike mode "off" is just a spike that never ramps up.
        profile = SpikeProfile(
            baseline_rate=args.rate,
            multiplier=1.0,
            delay_seconds=0.0,
            ramp_seconds=1.0,
            hold_seconds=0.0,
            decay_seconds=1.0,
        )

    run(args.bootstrap_servers, args.topic, profile)
