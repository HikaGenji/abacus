# zenoh + iceoryx2 HFT Demo

A working demonstration that **zenoh** and **iceoryx2** together form a viable
high-frequency / low-latency trading infrastructure — fully in Rust, fully open
source.

Companion to the Medium article:
[*Sub-Microsecond Trading Infrastructure: zenoh + iceoryx2 in Practice*](article/medium_article.md)

---

## Why these two?

| Need | Tool | Why |
|------|------|-----|
| External connectivity (exchange feeds, order routing) | **zenoh** | High-perf pub/sub over TCP/UDP/QUIC; works across machines and clouds |
| Intra-machine IPC between trading components | **iceoryx2** | Shared-memory zero-copy; ~100 ns one-way latency, no kernel involvement |

They are complementary: zenoh handles the *network* layer, iceoryx2 handles the
*process* layer.  Combined they cover the full path from exchange wire to
submitted order.

---

## Architecture

```
Exchange (simulated)        Local trading system          Exchange (simulated)
─────────────────           ────────────────────          ────────────────────
market-data-pub             md-handler  strategy          order-gateway
  (zenoh pub)    ─zenoh──►  (iox2 pub) (iox2 sub/pub)  ─iceoryx2──► (zenoh pub)
                                │            │
                           iceoryx2     iceoryx2
                           shared mem   shared mem
```

| Component | Transport in | Transport out | Description |
|-----------|-------------|---------------|-------------|
| `market-data-pub` | — | zenoh | Simulates an exchange feed; publishes `MarketTick` at configurable rate |
| `md-handler` | zenoh | iceoryx2 | Bridges external feed onto the local shared-memory bus |
| `strategy` | iceoryx2 | iceoryx2 | EMA-20 momentum signal; emits `OrderSignal` on crossing |
| `order-gateway` | iceoryx2 | zenoh | Routes orders to the exchange; logs end-to-end latency |
| `recorder` | iceoryx2 + zenoh | QuestDB | Persists ticks and orders for TCA and auditing |
| `latency-bench` | — | — | Standalone iceoryx2 ping-pong benchmark |

---

## Quick start

### Prerequisites

- Rust 1.75+ (`rustup update stable`)
- Linux (iceoryx2 shared memory uses POSIX IPC)
- Docker + Docker Compose — required only for the recording stack (`--record`)

### Docker services

`docker-compose.yml` defines three containers used by the `recorder` crate:

| Service | Port(s) | Role |
|---------|---------|------|
| **QuestDB** | 9000 (console), 9009 (ILP TCP), 8812 (PostgreSQL) | Stores every tick and order signal for TCA and auditing |
| **Grafana** | 3000 | Dashboards — connects to QuestDB via PostgreSQL data source |

The pipeline itself (`market-data-pub`, `md-handler`, `strategy`, `order-gateway`) has **no Docker dependency** — it runs as plain Rust binaries.

If you want to control the infrastructure independently of the demo:

```bash
./scripts/run_infra.sh          # docker compose up -d
./scripts/run_infra.sh down     # stop and remove containers
./scripts/run_infra.sh logs     # stream container logs
```

### Build

```bash
cargo build --release
```

### Run the demo

```bash
# Pipeline only (market-data-pub → md-handler → strategy → order-gateway):
./scripts/run_demo.sh

# Custom tick rate:
./scripts/run_demo.sh --rate 50000

# Full demo with recording — starts Docker infrastructure (QuestDB, InfluxDB,
# Grafana) and the recorder crate alongside the pipeline:
./scripts/run_demo.sh --record
```

Watch the `order-gateway` pane for **end-to-end tick-to-order latency**.
With `--record`, the recorder pane streams ingestion confirmations and every
five seconds prints p50/p99/p999 latency percentiles to InfluxDB.

Once `--record` is running:

| UI | URL | Purpose |
|----|-----|---------|
| QuestDB web console | http://localhost:9000 | SQL queries on raw tick/order data |
| Grafana | http://localhost:3000 | Dashboards (admin / admin) |

**Example TCA queries in QuestDB:**

```sql
-- All orders in the last hour with latency breakdown
SELECT timestamp, side, price, decision_ns, submission_ns, e2e_ns
FROM order_signals
WHERE timestamp > dateadd('h', -1, now())
ORDER BY timestamp;

-- Slippage: each order joined to the prevailing market mid at signal time
SELECT o.timestamp, o.side, o.price,
       t.mid                                   AS mid_at_signal,
       (CASE WHEN o.side = 'BUY'  THEN o.price - t.ask
             WHEN o.side = 'SELL' THEN t.bid - o.price END) AS slippage
FROM order_signals o
ASOF JOIN market_ticks t ON (o.symbol = t.symbol)
ORDER BY o.timestamp;

-- Latency percentiles over 1-minute windows
SELECT timestamp,
       percentile_approx(e2e_ns,      0.50) AS e2e_p50_ns,
       percentile_approx(e2e_ns,      0.99) AS e2e_p99_ns,
       percentile_approx(decision_ns, 0.50) AS decision_p50_ns,
       count(*)                             AS order_count
FROM order_signals
WHERE timestamp > dateadd('h', -1, now())
SAMPLE BY 1m FILL(NULL);
```

### Run the latency benchmark

```bash
# Terminal 1 — start the pong side first
./target/release/latency-bench pong

# Terminal 2 — run 1 million ping-pong round-trips
./target/release/latency-bench ping --count 1000000
```

Or use the convenience script (requires tmux):

```bash
./scripts/run_bench.sh
```

Expected output (on a modern Linux server):

```
=== iceoryx2 one-way IPC latency (1000000 samples) ===
  mean  :      312 ns
  p50   :      298 ns
  p95   :      421 ns
  p99   :      589 ns
  p99.9 :      912 ns
  max   :     4231 ns
```

---

## Project structure

```
.
├── Cargo.toml               Workspace manifest
├── docker-compose.yml       QuestDB + InfluxDB + Grafana
├── crates/
│   ├── common/              Shared POD types: MarketTick, OrderSignal
│   ├── market-data-pub/     Exchange feed simulator (zenoh)
│   ├── md-handler/          zenoh → iceoryx2 bridge
│   ├── strategy/            Momentum strategy (pure iceoryx2)
│   ├── order-gateway/       iceoryx2 → zenoh bridge
│   ├── recorder/            Tick + order recorder (QuestDB ILP + InfluxDB HTTP)
│   └── latency-bench/       Ping-pong latency benchmark
├── scripts/
│   ├── run_demo.sh
│   ├── run_bench.sh
│   └── run_infra.sh         Start/stop Docker infrastructure
└── article/
    └── medium_article.md    Full Medium article draft
```

---

## Running tests

```bash
cargo test --workspace
```

---

## Going to production

The demo focuses on clarity.  A production system would add:

- **CPU pinning** (`taskset`, `numactl`) — isolate cores for strategy and gateway
- **NUMA awareness** — allocate iceoryx2 shared memory on the local NUMA node
- **Kernel bypass** — DPDK or RDMA for sub-microsecond network latency
- **Clock synchronisation** — PTP/IEEE 1588 for accurate cross-host timestamps
- **Risk controls** — pre-trade checks on the order-gateway hot path
- **Persistence** — the `recorder` crate covers this; for production add WAL-backed QuestDB with replication

---

## License

MIT
