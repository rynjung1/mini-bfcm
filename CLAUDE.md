# Mini BFCM — project brief for Claude Code

## Who's building this and how

Ryan is a 2nd-year CS student at Waterloo, currently a FinOps co-op at
StatCan, targeting a **Data Engineer internship at Shopify**. This project
is a portfolio piece for that application.

**Working style — read this first, every session:**
- This is a **vibe-coded project**: Claude Code writes the actual code.
  Ryan directs, reviews, and tweaks/adjusts what gets built, he is not
  typing every line himself.
- That said, explanation is still mandatory, not optional. Before writing
  any new piece of code, explain in plain English what it does and why
  it's built that way. The goal is that Ryan can explain every design
  decision fluently in a Shopify interview, even though Claude Code wrote
  the implementation. Code Ryan can't explain is worse than no code.
- When introducing a new concept (consumer lag, idempotency, windowing,
  etc.), explain it the way you'd explain it out loud to an interviewer,
  before showing the code that implements it.
- Prefer building incrementally: one component working end-to-end before
  moving to the next, rather than generating the whole repo in one shot.
- If Ryan asks "why did we do X instead of Y," always answer with the
  tradeoff, not just the definition, that's the actual signal Shopify's
  interviews screen for (see below).
- After building each piece, briefly check that Ryan understands it
  (e.g. "does that make sense / want me to explain any part again?")
  before moving to the next step.

## The project

**Mini BFCM**: a streaming data pipeline that simulates a Shopify-style
flash-sale traffic spike (mirroring Shopify's own BFCM load testing) and
shows the effect live on a public dashboard.

**Why this project, specifically**: most portfolio ETL projects show a
steady-rate pipeline. This one demonstrates a harder, more
interview-relevant problem: what happens to a pipeline under a sudden
load spike, and how do you observe/reason about it instead of it silently
falling over. Research into Shopify's actual DE interview process found
the #1 rejection reason is weak ownership signal, candidates describing
a pipeline without explaining tradeoffs (batch vs. micro-batch, how they
handled volume spikes, why a given partitioning strategy). This project
is built specifically to have good answers to those questions.

Shopify's own engineering blog was the direct inspiration: they run
bimonthly fire drills simulating 150% of last year's BFCM load, and their
public "Live Globe" BFCM dashboard runs on Apache FlinkSQL pipelines
processing real-time order events. This project is a small-scale version
of that same idea.

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

Everything (Kafka, producer, consumer, DuckDB, dashboard) runs on one
machine, all the time, exactly as in local dev. There is no cloud
deployment of the pipeline itself. For demos (interviews, recruiters
clicking the GitHub link), the dashboard's local port is exposed
publicly through a tunnel, started only when needed.

### Components

- **Producer**: generates synthetic Shopify-shaped order events (order id,
  customer id, line items, total, timestamp) at a configurable baseline
  rate. Has a `--spike` mode that ramps the rate up 50-100x over ~5
  seconds, holds, then decays back down, modeling a flash-sale surge.
- **Broker**: real Apache Kafka, self-hosted, not a Kafka-compatible
  substitute. Runs in **KRaft mode** (Kafka's modern mode that removes
  the old Zookeeper dependency, so a single Kafka container is enough for
  local dev, no separate coordination service to run), via Docker
  Compose. Runs locally only, no cloud VM. A free-tier cloud VM (e.g.
  Oracle Cloud "Always Free") was considered and deliberately rejected:
  there are enough real reports of people getting billed unexpectedly on
  Oracle's free tier that the risk isn't worth it for a portfolio
  project. Chosen deliberately over Redpanda/Confluent Cloud/Upstash
  regardless: "Apache Kafka" is the name recruiters and interviewers
  actually recognize, and self-hosting locally means genuinely $0 cost
  with no card on file anywhere.
- **Consumer**: reads the order stream, computes 10-second tumbling
  windows (order count, revenue, unique customers), and tracks consumer
  lag (how far behind the consumer is from the latest produced message).
  Idempotent by design: aggregates are upserted keyed by `window_start`,
  so replaying a window after a consumer restart doesn't double-count.
- **Storage**: DuckDB, a single local file. All components run on one
  machine, so there's no need for a hosted/shared DB like MotherDuck,
  that would only matter if producer/consumer/dashboard were split
  across separate deployed services, which they aren't.
- **Dashboard**: Streamlit, polls the DB every ~2s, shows orders/min and
  revenue/min as live charts, plus a lag indicator, the number that
  should visibly climb during the spike and recover afterward. This is
  the actual "does this pipeline keep up under load" story. For a public
  demo URL, expose it with `ngrok` or `cloudflared` (Cloudflare Tunnel),
  neither requires a card on file. The link only works while the local
  pipeline is running, that's an accepted tradeoff, see the concepts
  section below.

## Concepts Ryan needs to be able to explain in an interview

For each of these, Claude should make sure Ryan understands the concept
well enough to explain it unprompted, not just that the code implements
it:

1. **Why windowed aggregation instead of streaming raw events to the
   dashboard?** Keeps the dashboard cheap to query regardless of event
   volume, it only ever reads a handful of aggregated rows, even during
   a 100x spike.
2. **Idempotency and at-least-once delivery.** Kafka's default delivery
   guarantee can redeliver the same message. Upserting by `window_start`
   (rather than incrementing counters) makes replays safe.
3. **Consumer lag as an observability signal.** Lag is the metric that
   actually reveals whether a pipeline is keeping up or silently falling
   behind, throughput alone can look fine while lag quietly grows.
4. **Batch vs. streaming tradeoffs**, and why this project intentionally
   uses streaming (near-real-time reaction to a spike) rather than batch
   (which would only show the spike's aftermath, hours later).
5. **Tumbling windows** vs. other windowing strategies (sliding, session),
   and why tumbling is the simple, correct choice here.
6. **Why self-hosted Kafka in KRaft mode instead of a managed service.**
   Kafka historically needed Zookeeper for coordination; KRaft mode
   (Kafka Raft) removes that dependency, so Kafka manages its own
   metadata internally. This is also a genuinely current fact worth
   knowing, not just a cost-saving hack, KRaft is now the direction the
   whole Kafka project has moved.
7. **Why local + tunnel instead of a real cloud deployment.** The
   tradeoff: a free-tier cloud VM would give an always-on public URL,
   but real users have reported unexpected billing on Oracle's "Always
   Free" tier even without upgrading, an unacceptable risk for a project
   with $0 budget. Running locally and tunneling (ngrok/Cloudflare
   Tunnel) on demand gives a genuine public URL with zero billing
   surface, at the cost of the link only being live while Ryan's machine
   is running the pipeline. For a portfolio project demoed live in
   interviews (not something needing 24/7 uptime for strangers), that's
   the right tradeoff, uptime isn't the thing being evaluated, the
   pipeline's behavior under load is.

## Tech stack

- Python 3.11+
- `confluent-kafka` (Kafka client library, the name refers to the client,
  not the hosting, works against any real Kafka broker)
- **Apache Kafka in KRaft mode**, local only, via Docker Compose — no
  Redpanda, no Confluent Cloud, no Upstash, no cloud VM. Real Kafka end
  to end, self-hosted, $0 cost, no card required anywhere.
- DuckDB (single local file, all components on one machine)
- Streamlit + Plotly (dashboard)
- `ngrok` or `cloudflared` (Cloudflare Tunnel) to expose the local
  Streamlit dashboard with a public URL on demand, for demos

## Current status

Nothing has been built in this VS Code project yet, this is a fresh
start. A rough scaffold was drafted in an earlier chat session (producer,
consumer, dashboard, docker-compose, Render config) as a reference for
what "done" roughly looks like, but that draft used Redpanda and
Upstash, both superseded by the self-hosted Apache Kafka decision above.
Treat the architecture in this file as the current target design, not
the earlier draft. Claude Code should generate the actual implementation
(this is a vibe-coded project), but must explain each piece as it's
built, see Working Style above.

## Suggested build order

1. Get Apache Kafka (KRaft mode) running locally via Docker Compose,
   confirm you can produce/consume a test message manually (e.g. via
   `kafka-console-producer`/`kafka-console-consumer` inside the
   container), before writing any Python. Explain what KRaft mode is and
   what Docker Compose is doing for us here first.
2. Build the producer: start with a fixed baseline rate (no spike mode
   yet), confirm messages land in the topic.
3. Add spike mode to the producer once baseline works.
4. Build the consumer: start with simple per-message logging, then add
   windowing, then add idempotent DuckDB upserts, then add lag tracking,
   as separate incremental steps, explain each addition before writing it.
5. Build the dashboard against the local DuckDB file.
6. Set up `ngrok` or `cloudflared` to tunnel the local Streamlit
   dashboard to a public URL on demand, for demos. No cloud deployment
   of Kafka, the producer, or the consumer, they stay local permanently.

Even though Claude Code is writing the code (vibe-coded), keep moving
through these steps one at a time rather than generating everything at
once, that pacing is what makes the explanations land.

## Roadmap / stretch goals

- Second consumer group, to demonstrate horizontal scaling under load
- Feed spike-period infra cost estimates back in from Ryan's existing
  [finops-platform](https://github.com/rynjung1/finops-platform) project
- Data quality tests (e.g. dbt or Great Expectations) on the windowed
  output
