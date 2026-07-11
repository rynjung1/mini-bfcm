# Mini BFCM

A local streaming data pipeline that simulates a flash sale traffic
spike on an e-commerce order stream and shows the effect live on a
dashboard: synthetic orders flow through real, self-hosted Apache Kafka
into windowed DuckDB aggregates, with consumer lag tracked the whole way
so you can watch the pipeline either keep up or fall behind under load.

Most portfolio ETL projects show a steady-rate pipeline. This one
demonstrates a harder, more realistic problem: what happens to a
pipeline under a sudden load spike, and how do you observe and reason
about that instead of it silently falling over.

## Architecture

```
Producer (synthetic orders) --normal rate--> Apache Kafka topic (self-hosted, local Docker)
                              --spike mode (50-100x)-->
                                     |
                                     v
                          Consumer (windowed aggregation,
                          idempotent upserts, lag tracking)
                                     |
                                     v
                              DuckDB (local file)
                                     |
                                     v
                     Streamlit dashboard (local; tunneled to a
                     public URL on demand via ngrok/Cloudflare Tunnel)
```

Everything runs on one machine: Kafka, the producer, the consumer,
DuckDB, and the dashboard. There is no cloud deployment of the pipeline
itself. For demos, the dashboard's local port can be tunneled to a
public URL on demand.

## How the pipeline works, end to end

1. **Produce.** `producer/producer.py` generates a synthetic order every
   `1/rate` seconds and publishes it to the `orders` Kafka topic. With
   `--spike` enabled, the rate follows a ramp, hold, and decay curve
   (`producer/spike.py`) that climbs to 50 to 100x baseline over a few
   seconds, holds at peak, then decays back down, modeling a flash sale
   going live and tailing off.
2. **Buffer.** `consumer/consumer.py` polls Kafka in a loop. Each message
   that arrives gets parsed, assigned to its 10 second tumbling window
   (`consumer/windowing.py`), and appended to an in memory buffer.
   Nothing touches the database yet.
3. **Flush.** Once a second, the consumer opens a DuckDB connection,
   upserts the entire buffered batch into windowed aggregates in a
   single transaction (`consumer/store.py`), records the current
   consumer lag, and closes the connection. Offsets are committed only
   after that write succeeds, so a crash mid batch results in a
   redelivery rather than lost data.
4. **Dedupe.** Kafka's default delivery guarantee is at least once, so
   redeliveries happen. The upsert is keyed by `order_id`, so replaying
   an already processed order is a no op instead of a double count.
5. **Read.** `dashboard/app.py` polls the same DuckDB file every 2
   seconds through a read only connection (`dashboard/db.py`) and
   renders orders per minute, revenue per minute, and consumer lag as
   live charts. It only ever reads a handful of aggregated rows, so the
   dashboard stays cheap to query no matter how large the spike is.

## Components

- **Producer** (`producer/`): generates synthetic e-commerce order
  events at a configurable baseline rate, with an optional spike mode
  for simulating a flash sale surge.
- **Broker**: real Apache Kafka, self hosted via Docker Compose, running
  in KRaft mode so no separate Zookeeper container is needed. See
  `docker-compose.yml`.
- **Consumer** (`consumer/`): reads the order stream, computes 10 second
  tumbling window aggregates (order count, revenue, unique customers),
  tracks per partition consumer lag, and upserts idempotently so
  replays never double count.
- **Storage**: a single local DuckDB file (`data/mini_bfcm.duckdb`).
  Since every component runs on the same machine, there is no need for
  a hosted or shared database.
- **Dashboard** (`dashboard/`): a Streamlit app that polls the DuckDB
  file and shows live charts for orders/min, revenue/min, and consumer
  lag, the number that should visibly climb during a spike and recover
  afterward.

## Concepts this project is built to demonstrate

- **Windowed aggregation over raw event streaming to the dashboard.**
  Keeps dashboard queries cheap and constant cost regardless of event
  volume, even during a 100x spike.
- **Idempotency under at least once delivery.** Kafka can redeliver a
  message. Upserting by `order_id` (rather than incrementing a counter)
  makes replays safe.
- **Consumer lag as an observability signal.** Throughput alone can look
  fine while a pipeline quietly falls behind. Lag is the metric that
  actually reveals it.
- **Streaming over batch.** A batch job would only reveal a flash sale
  spike hours later, after the fact. Streaming reacts to it in near real
  time, which is the point of the demo.
- **Tumbling windows over sliding or session windows.** The dashboard
  only needs to answer "what happened in this fixed slice of time," so
  tumbling windows are the simplest correct choice.
- **Self hosted Kafka in KRaft mode instead of a managed service.** KRaft
  mode removes Kafka's historical Zookeeper dependency, so a single
  broker container is enough for local dev. It is also the direction
  the Kafka project itself has moved, not just a cost saving shortcut.
- **Local plus tunnel instead of a cloud deployment.** A free tier cloud
  VM would give an always on public URL, but real users have reported
  unexpected billing even on Oracle's "Always Free" tier, an
  unacceptable risk for a $0 budget project. Running locally and
  tunneling on demand gives a genuine public URL with zero billing
  surface, at the cost of the link only being live while the pipeline is
  running. For a project demoed live in interviews, that tradeoff is the
  right one: uptime isn't what's being evaluated, the pipeline's
  behavior under load is.

## Running it locally

1. Start Kafka (KRaft mode, single broker):
   ```
   docker compose up -d
   ```
2. Install dependencies:
   ```
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Start the consumer (it creates the DuckDB schema on first run):
   ```
   cd consumer && python consumer.py
   ```
4. In another terminal, start the producer. Baseline rate only:
   ```
   cd producer && python producer.py
   ```
   Or with a flash sale spike:
   ```
   cd producer && python producer.py --spike
   ```
5. In another terminal, start the dashboard:
   ```
   cd dashboard && streamlit run app.py
   ```
6. Optional, for a public demo link, tunnel the dashboard's local port
   with `ngrok` or `cloudflared`. The link only works while the local
   pipeline is running.

## Tech stack

- Python 3.11+
- `confluent-kafka` as the Kafka client library
- Apache Kafka in KRaft mode, self hosted locally via Docker Compose
- DuckDB for storage
- Streamlit and Plotly for the dashboard
- `ngrok` or `cloudflared` (Cloudflare Tunnel) for on demand public demo
  links
