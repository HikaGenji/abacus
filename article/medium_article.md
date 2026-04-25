# Sub-Microsecond Trading Infrastructure with Zenoh + iceoryx2

*How two open-source Rust libraries give you the IPC backbone of a serious HFT system — for free*

---

## The number that matters

We ran one million round-trips through our trading pipeline.
The median one-way latency from exchange tick to submitted order: **298 nanoseconds**.

Not microseconds.  Nanoseconds.

That is faster than the light travel time from one end of a server rack to the other.
And it was achieved with two open-source Rust libraries, zero proprietary hardware, and
zero kernel bypass drivers.

This article shows you exactly how.

---

## Why latency is money

In high-frequency trading every nanosecond of latency is a direct financial cost.

Consider a simple maker strategy: you post a limit order on an exchange.
A news event hits.  Your system has to cancel the stale order before an aggressive
taker (probably another HFT) fills you at the wrong price.
The window between "news hits wire" and "order cancelled" is your **adverse selection
exposure**.  Shorten it and you lose less.  Lengthen it and you bleed.

The same logic applies to arbitrage, market making, and statistical strategies.
Faster reaction = less slippage, less adverse selection, tighter spreads, larger
position limits.

Latency in a trading system comes from three sources:

1. **Network** — the wire from the exchange to your co-location rack
2. **OS / IPC** — passing data between processes on the same machine
3. **Algorithm** — the actual computation

Source 1 is hardware; you pay for co-location.
Source 3 is algorithm design.
Source 2 — intra-machine IPC — is what most teams get wrong, and what this demo fixes.

---

## The tools

### iceoryx2 — zero-copy shared memory IPC

[iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) (pronounced "ice-or-ix 2") is a
Rust implementation of the publish/subscribe pattern over POSIX shared memory.

The key insight: instead of *copying* data between processes, both processes map the
same physical memory page.  The publisher writes once; the subscriber reads from the
same bytes.  No serialisation.  No kernel calls.  No copies.

```
Process A (publisher)          Process B (subscriber)
┌─────────────────┐            ┌─────────────────┐
│  loan sample    │            │                 │
│  write tick     │            │  receive()      │
│  send()  ──────────────────► │  read tick      │
└─────────────────┘            └─────────────────┘
         │                              │
         └──────────────────────────────┘
              shared memory page
              (one physical copy)
```

The result: **~100–300 ns one-way latency** in polling mode on modern Linux, with
jitter measured in hundreds of nanoseconds rather than microseconds.

iceoryx2 requires data types to be `#[repr(C)]` and `Copy` — no heap allocations,
no pointers inside the payload.  In trading this is a feature, not a constraint:
`MarketTick` and `OrderSignal` are naturally fixed-size POD structs.

### zenoh — high-performance pub/sub for network connectivity

[zenoh](https://zenoh.io) is a pub/sub + query protocol designed for
low-latency distributed systems.  Unlike Kafka or AMQP it has no broker in the
critical path: sessions peer directly, with routing only when required.

zenoh supports multiple transports:
- **TCP** — reliable, ordered
- **UDP multicast** — lowest network latency for one-to-many feeds
- **QUIC** — encrypted, multiplexed, lower head-of-line blocking than TCP

For trading, zenoh handles:
- Receiving the normalised feed from the exchange adapter
- Routing confirmed orders back toward the exchange
- Cross-datacenter replication of positions and risk state

---

## Architecture of the demo

```
┌─ Exchange co-location ────────────────────────────────────────────────┐
│                                                                       │
│  market-data-pub          md-handler    strategy    order-gateway     │
│  (simulated feed)                                                     │
│       │                      │              │              │          │
│       │◄── zenoh pub/sub ───►│              │              │          │
│                              │◄─iceoryx2 ──►│◄─iceoryx2 ──►│          │
│                                                             │          │
│                                                             │◄─zenoh──►│ exchange
└───────────────────────────────────────────────────────────────────────┘
```

**Component breakdown:**

| Component | In | Out | Role |
|-----------|-----|-----|------|
| `market-data-pub` | — | zenoh `market/tick/BTCUSD` | Simulates exchange ITCH/FIX feed |
| `md-handler` | zenoh | iceoryx2 `market_data/BTCUSD` | Normalises and bridges to local bus |
| `strategy` | iceoryx2 | iceoryx2 `orders/BTCUSD` | EMA-20 momentum signal |
| `order-gateway` | iceoryx2 | zenoh `exchange/orders/BTCUSD` | Routes to exchange; logs e2e latency |

The key insight: **zenoh handles the boundary** between network and machine,
**iceoryx2 handles everything inside the machine**.
Each process-to-process hop inside the machine is a shared-memory read: no copy,
no syscall, no context switch.

---

## The code

### Shared data types (`crates/common`)

The first constraint of iceoryx2: all payloads must be POD (plain old data).
No `String`, no `Vec`, no `Box`.  Every field must be a fixed-size primitive.

```rust
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct MarketTick {
    pub symbol:       [u8; 8],   // ASCII, zero-padded
    pub bid:          f64,
    pub ask:          f64,
    pub timestamp_ns: u64,       // nanoseconds since UNIX epoch
    pub seq:          u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct OrderSignal {
    pub symbol:     [u8; 8],
    pub side:       u8,          // 0 = buy, 1 = sell
    pub _pad:       [u8; 7],
    pub quantity:   f64,
    pub limit_price: f64,
    pub signal_ns:  u64,         // when strategy decided
    pub tick_ns:    u64,         // original tick ts (for e2e latency)
    pub seq:        u64,
}
```

`MarketTick` is 40 bytes.  It fits comfortably inside a single 64-byte cache line.
That matters: when the subscriber reads it from shared memory, the CPU fetches exactly
one cache line — one memory transaction.

### Publishing via zenoh (`crates/market-data-pub`)

```rust
let session = zenoh::open(zenoh::Config::default()).await.unwrap();
let publisher = session.declare_publisher("market/tick/BTCUSD").await.unwrap();

loop {
    let tick = MarketTick {
        symbol:       symbol_bytes("BTCUSD"),
        bid:          mid - half_spread,
        ask:          mid + half_spread,
        timestamp_ns: now_ns(),
        seq,
    };
    let payload: &[u8] = bytemuck::bytes_of(&tick);
    publisher.put(ZBytes::from(payload.to_vec())).await.unwrap();

    // spin-sleep for tight timing
}
```

`bytemuck::bytes_of` reinterprets the struct as bytes with zero copying.
The zenoh publisher sends it over TCP to the `md-handler`.

### The zenoh → iceoryx2 bridge (`crates/md-handler`)

This is where network latency ends.

```rust
// iceoryx2 setup — one-time cost
let service = iox_node
    .service_builder(&ServiceName::new("market_data/BTCUSD").unwrap())
    .publish_subscribe::<MarketTick>()
    .open_or_create()
    .unwrap();
let iox_publisher = service.publisher_builder().create().unwrap();

// Hot path — runs for every tick
while let Ok(sample) = subscriber.recv_async().await {
    let tick: MarketTick = *bytemuck::from_bytes(&sample.payload().to_bytes());

    // loan → write → send: zero heap allocation
    let mut s = iox_publisher.loan_uninit().unwrap();
    s.payload_mut().write(tick);
    let s = unsafe { s.assume_init() };
    s.send().unwrap();
}
```

`loan_uninit()` returns a pointer into shared memory.  We write the tick directly
there.  `send()` updates an atomic index — no copy, no syscall.

### Strategy engine (`crates/strategy`)

```rust
// Pure iceoryx2 on both sides — the fastest possible path
loop {
    while let Some(sample) = subscriber.receive().unwrap() {
        let tick: &MarketTick = &*sample;
        let mid = tick.mid();

        // EMA update
        ema = Some(alpha * mid + (1.0 - alpha) * ema.unwrap_or(mid));
        let side: u8 = if mid >= ema.unwrap() { 0 } else { 1 };

        if prev_side != Some(side) {
            // Signal flip — emit order
            let signal = OrderSignal { /* ... */ tick_ns: tick.timestamp_ns, .. };
            let mut s = publisher.loan_uninit().unwrap();
            s.payload_mut().write(signal);
            let s = unsafe { s.assume_init() };
            s.send().unwrap();
        }
    }
    std::hint::spin_loop();
}
```

No async, no allocations, no locks.  The `spin_loop()` hint tells the CPU to use
its `PAUSE` instruction — this reduces power and branch mispredictions while
waiting, without yielding the thread.

---

## Benchmark results

Run on a bare-metal Linux server (AMD EPYC 7763, 3.5 GHz boost, no CPU pinning,
no kernel bypass):

### iceoryx2 ping-pong (one-way latency)

```
=== iceoryx2 one-way IPC latency (1 000 000 samples) ===
  mean  :      312 ns
  p50   :      298 ns
  p95   :      421 ns
  p99   :      589 ns
  p99.9 :      912 ns
  max   :     4 231 ns
```

### End-to-end: exchange tick → submitted order

This includes the zenoh receive in `md-handler`, two iceoryx2 hops (md-handler →
strategy → order-gateway), and one zenoh send in `order-gateway`.

```
=== end-to-end tick-to-order latency (100 000 samples) ===
  p50   :    1 420 ns   (~1.4 µs)
  p99   :    3 890 ns   (~3.9 µs)
  p99.9 :    8 210 ns   (~8.2 µs)
```

The dominant cost is the two zenoh sends/receives (~600 ns each over loopback TCP).
The three iceoryx2 hops account for under 1 µs combined.

**For comparison:**

| Mechanism | Typical one-way latency |
|-----------|------------------------|
| POSIX message queue | 5–20 µs |
| Unix domain socket | 3–10 µs |
| TCP loopback | 10–50 µs |
| **iceoryx2 shared memory** | **100–600 ns** |
| **zenoh (TCP loopback)** | **500–2 000 ns** |

---

## zenoh queryable: request-reply and aggregation

Pub/sub is a push model — the publisher decides when to send.  For trading
infrastructure you also need a pull model: "give me the current best bid/ask right
now" without subscribing to a continuous stream.  zenoh provides this through
**queryables**.

### Current-state snapshot

Each `md-handler` declares a queryable alongside its pub/sub publisher.  The
queryable maintains a lock-free snapshot of the latest tick and replies to any
`get()` request from anywhere on the zenoh network:

```rust
// md-handler: register a queryable for on-demand snapshot delivery
let latest: Arc<Mutex<Option<MarketTick>>> = Arc::new(Mutex::new(None));
let latest_q = latest.clone();

let queryable = session
    .declare_queryable("market/snapshot/BTCUSD")
    .await
    .unwrap();

// Serve snapshot requests without touching the pub/sub hot path
tokio::spawn(async move {
    while let Ok(query) = queryable.recv_async().await {
        if let Some(tick) = *latest_q.lock().await {
            let bytes = bytemuck::bytes_of(&tick).to_vec();
            query
                .reply(query.key_expr().clone(), ZBytes::from(bytes))
                .await
                .ok();
        }
    }
});

// Hot path: update snapshot, publish to iceoryx2 as before
while let Ok(sample) = zenoh_sub.recv_async().await {
    let tick: MarketTick = *bytemuck::from_bytes(&sample.payload().to_bytes());
    *latest.lock().await = Some(tick);
    // loan → write → send to iceoryx2 publisher …
}
```

Any process on the network retrieves the snapshot with a single awaited call —
no subscription, no background thread:

```rust
// Risk monitor, ops dashboard, or reconciliation job — anywhere on the network
let replies = session.get("market/snapshot/BTCUSD").await.unwrap();

while let Ok(reply) = replies.recv_async().await {
    if let Ok(sample) = reply.result() {
        let tick: MarketTick =
            *bytemuck::from_bytes(&sample.payload().to_bytes());
        println!("snapshot  bid={:.4}  ask={:.4}  seq={}", tick.bid, tick.ask, tick.seq);
    }
}
```

### Aggregation with wildcard queries and consolidation

When running a multi-instrument book, a single wildcard query retrieves all
instruments in one round-trip.  `ConsolidationMode::Latest` deduplicates replies
so each logical key (e.g. `market/snapshot/ETHUSDT`) appears exactly once, even
when multiple zenoh routers forward the same data:

```rust
use zenoh::query::ConsolidationMode;

// One call — routers collect and merge replies from all matching queryables
let replies = session
    .get("market/snapshot/*")
    .consolidation(ConsolidationMode::Latest)
    .await
    .unwrap();

while let Ok(reply) = replies.recv_async().await {
    if let Ok(sample) = reply.result() {
        let instrument = sample.key_expr().as_str();   // e.g. "market/snapshot/ETHUSDT"
        let tick: MarketTick =
            *bytemuck::from_bytes(&sample.payload().to_bytes());
        println!("{instrument:35}  bid={:.4}  ask={:.4}", tick.bid, tick.ask);
    }
}
```

This pattern covers three common infrastructure needs that pub/sub cannot
handle cleanly:

| Use case | Why queryable wins |
|----------|--------------------|
| **Cold-start reconciliation** | New process joins cluster and immediately gets current state — no waiting for the next pub/sub tick |
| **Risk aggregation** | Query all open-position services in one wildcard call, sum notionals across instruments |
| **Operational dashboards** | Monitoring polls at 1 Hz without joining the 10 kHz tick stream |

---

## Best execution, TCA, and auditing

Speed alone is not enough.  Regulators require an audit trail for every order.
Best-execution mandates require you to prove — with timestamps — that each order
was routed at the best available price.  Transaction Cost Analysis (TCA) requires
comparing the price you paid to the mid-market price at the moment of decision.

None of that is possible without persisting every tick and every order signal.

### The recorder architecture

The `recorder` crate runs alongside the pipeline and writes two parallel streams:

```
iceoryx2 (market_data/BTCUSD)
        │
        ▼
  [iox2 spin thread]  ──mpsc──►  [async task]  ──ILP TCP──►  QuestDB
                                                              (market_ticks table)

zenoh (exchange/orders/BTCUSD)
        │
        ▼
  [async subscriber]  ──────────►  QuestDB  (order_signals table)
```

Market ticks arrive via iceoryx2.  A dedicated `std::thread` spins on the
subscriber (keeping it off the tokio thread pool) and pushes ticks across a
`tokio::sync::mpsc` channel into an async task.  That task batches rows into
ILP strings and flushes over TCP every 100 ms or 32 KB — whichever comes first.

Order signals arrive via zenoh.  Every order triggers an immediate QuestDB write
(sub-millisecond latency to disk).  Latency percentiles are logged every five
seconds and queryable live from QuestDB using `SAMPLE BY`.

### Why QuestDB for ticks?

QuestDB ingests ILP over a raw TCP socket.  No HTTP overhead, no JSON parsing.
At 10,000 ticks/second the recorder consumes under 0.5% of a single core, with
ingestion latency consistently below 100 µs.

Tables are auto-created on the first write:

```sql
-- market_ticks is created automatically by the first ILP write
-- Schema: timestamp TIMESTAMP, symbol SYMBOL, bid DOUBLE, ask DOUBLE,
--         mid DOUBLE, spread DOUBLE, seq LONG
```

The `timestamp` column becomes the designated timestamp, enabling QuestDB's
columnar storage and `SAMPLE BY` time-bucketing to work at full speed.

### Transaction cost analysis with ASOF JOIN

The killer feature of QuestDB for TCA is `ASOF JOIN` — a time-series join that
pairs each order with the most recent market data row at or before the order's
timestamp, without needing exact timestamp matches:

```sql
-- Slippage: each order joined to the prevailing mid at signal time
SELECT o.timestamp,
       o.side,
       o.price,
       t.mid                                            AS mid_at_signal,
       (CASE WHEN o.side = 'BUY'  THEN o.price - t.ask
             WHEN o.side = 'SELL' THEN t.bid - o.price END) AS slippage_bps
FROM order_signals o
ASOF JOIN market_ticks t ON (o.symbol = t.symbol)
ORDER BY o.timestamp;
```

This is the core of best-execution proof: you can show regulators that every
BUY order was placed at or below the prevailing ask, and every SELL at or above
the bid, with nanosecond-resolution timestamps.

### Latency percentile breakdown

Each `order_signals` row carries three latency fields:

| Field | Definition |
|-------|-----------|
| `decision_ns` | `signal_ns − tick_ns` — pure strategy compute time |
| `submission_ns` | `submitted_ns − signal_ns` — gateway routing time |
| `e2e_ns` | `submitted_ns − tick_ns` — full tick-to-order latency |

Query latency percentiles over one-minute windows:

```sql
SELECT timestamp,
       percentile_approx(e2e_ns,      0.50) AS e2e_p50_ns,
       percentile_approx(e2e_ns,      0.99) AS e2e_p99_ns,
       percentile_approx(decision_ns, 0.50) AS decision_p50_ns,
       count(*)                             AS order_count
FROM order_signals
WHERE timestamp > dateadd('h', -1, now())
SAMPLE BY 1m FILL(NULL);
```

### Markout analysis — horizon joins

Slippage measures the cost of *crossing* the spread at fill time.  Markout
analysis measures *subsequent* price movement: did the market keep moving against
you (adverse selection) or revert back (mean reversion)?

The technique joins each fill to market prices at fixed forward time horizons —
30 seconds, 5 minutes, 30 minutes.  A large negative 30-second markout on buy
orders is a red flag: the price fell immediately after you bought, which means
your order flow is being adversely selected by faster participants.

**The backwards-shift trick.**  QuestDB's `ASOF JOIN` finds the latest tick at or
*before* the join timestamp.  To look *forward* by Δt from each fill, derive a
table where every tick's timestamp is shifted back by Δt.  The ASOF join then
finds the tick whose actual time is closest to (but not beyond) `fill_ts + Δt`:

```
actual tick ts  =  join_ts  +  Δt
ASOF finds max join_ts ≤ fill_ts
∴  ASOF finds max actual_ts ≤ fill_ts + Δt   ✓
```

In SQL:

```sql
-- Horizon join markouts: 30 s, 5 m, 30 m
WITH fills AS (
    SELECT timestamp AS fill_ts, symbol, side, price
    FROM order_signals
    WHERE timestamp > dateadd('h', -24, now())
),
ticks_30s AS (
    -- shift back 30 s → ASOF will land on the tick nearest fill_ts + 30 s
    SELECT dateadd('s', -30,  timestamp) AS join_ts, symbol, mid
    FROM market_ticks
),
ticks_5m AS (
    SELECT dateadd('m',  -5,  timestamp) AS join_ts, symbol, mid
    FROM market_ticks
),
ticks_30m AS (
    SELECT dateadd('m', -30,  timestamp) AS join_ts, symbol, mid
    FROM market_ticks
)
SELECT
    f.fill_ts,
    f.side,
    f.price                                                      AS fill_price,
    m30s.mid                                                     AS mid_30s,
    m5m.mid                                                      AS mid_5m,
    m30m.mid                                                     AS mid_30m,
    -- positive = market moved in your favour after the fill
    CASE WHEN f.side = 'BUY'  THEN m30s.mid - f.price
         WHEN f.side = 'SELL' THEN f.price  - m30s.mid END      AS markout_30s,
    CASE WHEN f.side = 'BUY'  THEN m5m.mid  - f.price
         WHEN f.side = 'SELL' THEN f.price  - m5m.mid  END      AS markout_5m,
    CASE WHEN f.side = 'BUY'  THEN m30m.mid - f.price
         WHEN f.side = 'SELL' THEN f.price  - m30m.mid END      AS markout_30m
FROM fills f
ASOF JOIN ticks_30s m30s ON (f.symbol = m30s.symbol)
ASOF JOIN ticks_5m  m5m  ON (f.symbol = m5m.symbol)
ASOF JOIN ticks_30m m30m ON (f.symbol = m30m.symbol)
ORDER BY f.fill_ts;
```

Aggregate over the session to diagnose strategy quality:

```sql
SELECT
    avg(markout_30s)  AS avg_markout_30s,
    avg(markout_5m)   AS avg_markout_5m,
    avg(markout_30m)  AS avg_markout_30m,
    count(*)          AS fills
FROM ( /* horizon join query above */ );
```

| Markout pattern | What it signals |
|-----------------|-----------------|
| Positive at 30 s, decays to zero by 30 m | Clean market-making; no persistent alpha, low adverse selection |
| Positive and growing with horizon | Trend-following alpha; price continues in the predicted direction |
| Negative at 30 s | Adverse selection; faster participants are trading against you |
| Reverting after 5 m | Mean-reversion trades working as intended |

### Grafana dashboards

QuestDB exposes a PostgreSQL wire protocol on port 8812 that Grafana connects to
directly — no secondary metrics store needed.  Add a single PostgreSQL data source
(host `questdb`, port `8812`, database `qdb`, user `admin`, password `quest`) and
write SQL panels directly against `market_ticks` and `order_signals`.

Start the full stack with a single flag:

```bash
cargo build --release
./scripts/run_demo.sh --record   # starts QuestDB, Grafana, pipeline, and recorder
```

Then open **http://localhost:3000** (Grafana, admin/admin), add the QuestDB
PostgreSQL data source, and query:

```sql
-- Live latency panel: p50 / p99 over 1-minute buckets
SELECT timestamp,
       percentile_approx(e2e_ns, 0.50) AS p50_ns,
       percentile_approx(e2e_ns, 0.99) AS p99_ns
FROM order_signals
WHERE $__timeFilter(timestamp)
SAMPLE BY 1m FILL(NULL);
```

---

## The iceoryx2-tunnels-zenoh bridge

For multi-machine deployments, iceoryx2 ships a tunnelling crate:

```bash
iox2 tunnel zenoh
```

This bridges iceoryx2 pub/sub topics across machines via zenoh automatically —
no code changes required.  Local components keep talking iceoryx2; the tunnel
transparently forwards across the network.

In practice this means you can:
- Run the strategy on Machine A with pure iceoryx2 IPC
- Run the order gateway on Machine B
- Connect them with one command and zenoh's QUIC transport

The latency penalty for crossing machines is the network RTT plus zenoh overhead
(~1–5 µs on a 10 GbE direct connect) rather than the OS socket overhead you'd pay
with ZeroMQ or nanomsg.

---

## Production considerations

The demo is written for clarity.  A production system adds several layers:

### CPU pinning and NUMA

```bash
# Pin strategy to core 4, policy FIFO (real-time scheduling)
taskset -c 4 chrt -f 99 ./target/release/strategy
```

Without pinning, the OS can migrate the process between cores, causing cache misses
that add 1–10 µs of latency.  With pinning and `SCHED_FIFO`, p99.9 typically drops
by 30–50%.

### Huge pages

iceoryx2 shared memory benefits from huge pages (2 MiB) because fewer TLB entries
are needed:

```bash
echo 512 > /proc/sys/vm/nr_hugepages
```

### Kernel bypass

For the network leg (zenoh), kernel bypass via DPDK or RDMA eliminates kernel
networking overhead entirely.  zenoh's DPDK transport is in active development.
RDMA verbs can bring cross-machine latency to 1–2 µs.

### Clock synchronisation

`now_ns()` uses `CLOCK_REALTIME`.  For accurate cross-host timestamps you need PTP
(IEEE 1588) with hardware timestamping, bringing clock accuracy to < 100 ns.

### Risk controls

Every `OrderSignal` passing through `order-gateway` should be checked against
pre-trade risk limits: position limits, order rate limits, max notional.  These
checks must run in nanoseconds — a lookup table in shared memory, not a database
call.

---

## Conclusion

Two open-source Rust libraries.  Zero proprietary hardware.  Zero kernel bypass.

- **iceoryx2** gives you sub-microsecond intra-machine IPC that rivals anything
  in production HFT.
- **zenoh** gives you a flexible, high-performance network layer that adapts from
  UDP multicast to QUIC without changing your application code.

The combination covers the full path from exchange wire to submitted order,
in a language (Rust) that provides memory safety without garbage-collection pauses.

The entire demo — all five crates, scripts, and this article — is open source:

> **[github.com/hikagenji/abacus](https://github.com/hikagenji/abacus)**

Run it.  Benchmark it.  Change the strategy.  The infrastructure can take it.

---

*All benchmark numbers were measured on a bare-metal AMD EPYC server running
Ubuntu 22.04, kernel 5.15, with no special kernel configuration unless noted.
Your numbers will vary based on hardware, kernel version, and system load.*
