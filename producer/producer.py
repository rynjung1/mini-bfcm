"""
Mini BFCM Order Producer

Publishes synthetic Shopify-shaped order events to a Kafka topic at a
configurable baseline rate, with an optional flash-sale spike mode.

How spike mode works (ramp -> hold -> decay):
1. Baseline: orders are produced at a steady rate (--rate, orders/sec).
2. Ramp: over --spike-ramp seconds, the rate climbs linearly from
   baseline to baseline * --spike-multiplier (e.g. 75x), modeling a
   flash sale suddenly going live.
3. Hold: the rate stays pinned at the peak for --spike-hold seconds,
   the busiest part of the sale.
4. Decay: over --spike-decay seconds, the rate falls back linearly to
   baseline, modeling the sale tailing off.

Each loop iteration asks the SpikeProfile what the target rate is right
now (profile.rate_at(elapsed)), generates one order, hands it to Kafka
asynchronously (producer.produce + poll(0) to service delivery
callbacks without blocking), then sleeps 1/rate seconds -- so
throughput tracks the ramp/hold/decay curve without a separate
scheduler.
"""

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
            payload = json.dumps(order)
            buffer_full_retries = 0
            while True:
                try:
                    producer.produce(topic, key=order["customer_id"], value=payload, callback=delivery_report)
                    break
                except BufferError:
                    # librdkafka's local send queue is full -- the broker
                    # can't drain it as fast as we're producing, most likely
                    # right at a spike's peak. Poll to service pending
                    # deliveries and free up room, then retry, instead of
                    # crashing at the exact moment the demo is supposed to
                    # show the pipeline under load. No retry cap here on
                    # purpose -- during a legitimate 75x spike hold this can
                    # take a few seconds to drain and that's fine. But if the
                    # broker is actually down, this would otherwise retry
                    # forever in total silence, which just looks like the
                    # producer quietly stalled. So: keep retrying, but say
                    # something if it's been stuck a while.
                    buffer_full_retries += 1
                    if buffer_full_retries % 30 == 0:
                        print(
                            f"warning: send queue still full after {buffer_full_retries * 0.1:.0f}s "
                            f"straight -- broker may be down or can't keep up"
                        )
                    producer.poll(0.1)
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

    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if args.spike:
        if args.spike_ramp <= 0:
            parser.error("--spike-ramp must be greater than 0")
        if args.spike_decay <= 0:
            parser.error("--spike-decay must be greater than 0")
        if args.spike_multiplier <= 0:
            # A multiplier <= 0 drives rate_at() to 0 or negative during the
            # hold phase, and 1.0 / rate a few lines later in run() would
            # then divide by zero (or sleep a negative amount).
            parser.error("--spike-multiplier must be greater than 0")

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
