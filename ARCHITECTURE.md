# CEX-RS: System Architecture & Technical Specification

Welcome to the definitive architectural specification and engineering guide for **`cex-rs`** — a high-throughput, low-latency Centralized Cryptocurrency Exchange (CEX) backend built in **Rust** using **Tokio**, **Actix-web**, **Redis Streams**, and **PostgreSQL**.

---

## 📑 Table of Contents

1. [High-Level Architecture & Topography](#1-high-level-architecture--topography)
2. [The Core Architectural Philosophy (LMAX & CQRS)](#2-the-core-architectural-philosophy-lmax--cqrs)
3. [Component Breakdown](#3-component-breakdown)
   - [3.1 rs-shared: The Universal Contract](#31-rs-shared-the-universal-contract)
   - [3.2 rs-gateway: Public API & Security Layer](#32-rs-gateway-public-api--security-layer)
   - [3.3 rs-engine: The Matching Core](#33-rs-engine-the-matching-core)
   - [3.4 Async Cold-Path Batch Worker](#34-async-cold-path-batch-worker)
4. [The Write-Ahead Log (WAL) & Redis Streams](#4-the-write-ahead-log-wal--redis-streams)
5. [In-Memory Orderbook Mechanics](#5-in-memory-orderbook-mechanics)
6. [Crash Recovery: Snapshots + Fast-Forward Replay](#6-crash-recovery-snapshots--fast-forward-replay)
7. [The Double-Entry Financial Ledger](#7-the-double-entry-financial-ledger)
8. [Database Schema & Data Model](#8-database-schema--data-model)
9. [Request-Response Demuxing (Async over Queue)](#9-request-response-demuxing-async-over-queue)
10. [Security & Production Hardening](#10-security--production-hardening)
11. [Performance Benchmarks & Verified Metrics](#11-performance-benchmarks--verified-metrics)

---

## 1. High-Level Architecture & Topography

```
                                  [ HTTP / REST Clients ]
                                             │
                                             ▼
                     ┌───────────────────────────────────────────────┐
                     │                  rs-gateway                   │
                     │           (Actix-web API Gateway)             │
                     │  - Argon2id Password Hashing & JWT Auth       │
                     │  - Token Bucket Rate Limiting & DTO Validate  │
                     │  - Response Demuxer (oneshot channels)        │
                     └───────┬───────────────────────────────▲───────┘
                             │                               │
            XADD (WAL)       │                               │ LPUSH (ephemeral reply)
      "engine-events"        │                               │ "response-queue.{id}"
                             ▼                               │
                     ┌───────────────────────────────────────┴───────┐
                     │               Redis 7 Cluster                 │
                     │  - "engine-events": Append-only WAL Stream    │
                     │  - "engine-executions": Async Worker Stream   │
                     │  - "response-queue.*": Ephemeral Reply Queues │
                     └───────┬───────────────────────────────▲───────┘
                             │                               │
        XREAD BLOCK          │                               │ Emit Executions
     (Continuous Stream)     │                               │ & Replies
                             ▼                               │
                     ┌───────────────────────────────────────┴───────┐
                     │                   rs-engine                   │
                     │            (In-Memory Matching Core)          │
                     │  - Deterministic Single-Writer Event Loop     │
                     │  - Price-Time-Priority BTreeMap Orderbook     │
                     │  - RAM Balance Ledger (Lock-First)            │
                     └───────┬───────────────────────────────────────┘
                             │
     Periodic JSON Snapshot  │
     (Every K Events / Boot) │
                             ▼
                     ┌───────────────────────────────────────────────┐
                     │                 PostgreSQL 16                 │
                     │  - users (Auth & Credentials)                 │
                     │  - engine_snapshots (State Recovery)          │
                     │  - trades (Fill History)                      │
                     │  - orders (Order Lifecycle)                   │
                     │  - ledger_entries (Double-Entry Audit)        │
                     │  - execution_checkpoints (Stream Offsets)     │
                     └───────────────────────────────────────────────┘
                             ▲
                             │ Bulk Batch INSERT
                             │ (Up to 100 events / tx)
                     ┌───────┴───────────────────────────────────────┐
                     │         Async Cold-Path Batch Worker          │
                     │   (Drains "engine-executions" into Postgres)  │
                     └───────────────────────────────────────────────┘
```

---

## 2. The Core Architectural Philosophy (LMAX & CQRS)

In traditional web applications, every request queries and writes directly to an SQL database (e.g. `BEGIN TRANSACTION; UPDATE balances ...; INSERT INTO orders ...; COMMIT;`).
In a financial exchange, this naive approach collapses under load:
- **Disk I/O Latency**: Writing to disk with ACID fsync takes 1–10 milliseconds, capping throughput at a few hundred orders per second.
- **Database Lock Contention**: Two traders matching against the same order cause database row locks, leading to deadlocks and latency spikes.

`cex-rs` solves this using two tier-1 financial engineering patterns:

### A. The LMAX Architecture (Memory-First Single Writer)
All matching logic, order books, and balance locks run **entirely in memory (RAM)** on a single deterministic event loop.
- **Latency**: Sub-microsecond order matching ($< 5\,\mu\text{s}$).
- **Determinism**: Every event processed in sequence results in the exact same state every single time.
- **Durability**: Guaranteed via an append-only **Write-Ahead Log (WAL)** in Redis Streams.

### B. CQRS (Command Query Responsibility Segregation)
We strictly separate the **Write (Hot Path)** from the **Read (Cold Path)**:
- **Hot Path (Writes)**: `rs-engine` matches orders and locks balances in RAM, emitting lightweight events to Redis. It **never** waits for PostgreSQL.
- **Cold Path (Reads & Historical Storage)**: An asynchronous background batch worker reads execution events from Redis and bulk-inserts them into PostgreSQL. Client queries for trade history (`GET /trades/my`) or statements (`GET /ledger`) are answered by indexed PostgreSQL queries without touching the matching engine.

---

## 3. Component Breakdown

The codebase is structured as a multi-crate Cargo workspace (`resolver = "3"`, Rust 2024 edition):

### 3.1 `rs-shared`
The contract layer shared by both the gateway and the engine:
- **Channels** (`channels.rs`): Redis stream keys (`engine-events`, `engine-executions`) and channel names.
- **Protocol Models** (`models/`): Strongly typed message envelopes (`OrderMsg`, `CancelMsg`, `BalanceQueryMsg`, `SignupMsg`, `OnrampMsg`, `DepositMsg`).
- **Execution Models** (`models/execution.rs`): `ExecutionEvent` enum representing engine state emissions (`OrderCreated`, `TradeExecuted`, `OrderCancelled`, `FundingExecuted`).

### 3.2 `rs-gateway`
The user-facing HTTP gateway built on Actix-web:
- Authenticates users with JWT tokens (`AuthUser`).
- Hashes passwords using **Argon2id** with cryptographically secure random salts.
- Validates request payloads early via the `Validate` trait.
- Enforces per-IP and per-user rate limits via **Token Bucket** middleware.
- Serializes client commands and appends them to Redis Stream `engine-events` via `send_and_wait`.

### 3.3 `rs-engine`
The isolated matching core:
- Contains the in-memory orderbook and balance ledger (`EngineState`).
- Consumes events sequentially from `engine-events`.
- Operates in **Dual Execution Mode**:
  - **Live Mode**: Mutates RAM state, publishes replies back to client ephemeral response queues, and emits execution events to `engine-executions`.
  - **Replay Mode**: When recovering from a crash, mutates RAM state without publishing duplicate replies to historical clients.

### 3.4 Async Cold-Path Batch Worker
Located in `rs-gateway/src/ledger/worker.rs`:
- Runs continuously in the background using `XREAD BLOCK 1000 COUNT 100 STREAMS engine-executions <checkpoint>`.
- Batches up to 100 execution events into a **single PostgreSQL transaction**.
- Updates order records, logs trades, writes double-entry ledger rows, and updates the checkpoint atomically.

---

## 4. The Write-Ahead Log (WAL) & Redis Streams

### Why Redis Streams (`XADD` / `XREAD`) Instead of Lists (`LPUSH` / `BRPOP`)?

| Feature | Redis List (`LPUSH` / `BRPOP`) | Redis Stream (`XADD` / `XREAD`) |
|---|---|---|
| **Storage Nature** | Destructive queue (pop deletes message) | Immutable append-only log (WAL) |
| **Crash Recovery** | Impossible (popped data lost if engine dies) | Deterministic (replay from offset via `XRANGE`) |
| **Ordering** | Simple list position | Millisecond monotonic IDs (`<timestamp>-<seq>`) |
| **Multi-Consumer** | Only one consumer receives each item | Multiple independent reader offsets |

### How `send_and_wait` Appends to the WAL:
```rust
// Gateway appends event with auto-generated monotonic stream ID
let mut cmd = redis::cmd("XADD");
cmd.arg(STREAM_EVENTS) // "engine-events"
   .arg("*")           // Monotonic ID generated by Redis
   .arg("channel").arg(channel)
   .arg("data").arg(&json_str);
```

Every command accepted by the gateway is etched into the log *before* the engine processes it.

---

## 5. In-Memory Orderbook Mechanics

The orderbook (`rs-engine/src/orderbook/book.rs`) achieves high-frequency performance using three complementary data structures:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Orderbook Data Structure                        │
├──────────────────────────┬─────────────────────────┬───────────────────┤
│    bids: BTreeMap        │     asks: BTreeMap      │  orders: HashMap  │
│  (Sorted Price Levels)   │  (Sorted Price Levels)  │ (O(1) Direct Map) │
├──────────────────────────┼─────────────────────────┼───────────────────┤
│ $101 -> [Ord 1, Ord 2]   │ $102 -> [Ord 4]         │ 1 -> Order #1     │
│ $100 -> [Ord 3]          │ $103 -> [Ord 5, Ord 6]  │ 2 -> Order #2     │
└──────────────────────────┴─────────────────────────┴───────────────────┘
```

1. **`BTreeMap<Price, PriceLevel>`**:
   - `bids.last_key_value()` retrieves the **Best Bid** in $O(1)$ amortized time.
   - `asks.first_key_value()` retrieves the **Best Ask** in $O(1)$ amortized time.
2. **`PriceLevel` with `VecDeque<OrderId>`**:
   - Stores only 64-bit integer IDs (`OrderId`), maintaining minimal cache footprint.
   - Enforces **FIFO Time Priority**: orders matching at the same price are matched in the order they arrived (`pop_front()`).
3. **`HashMap<OrderId, Order>`**:
   - Gives $O(1)$ direct lookup and cancellation without scanning price levels.

### The Lock-First Accounting Invariant
To prevent negative balances and race conditions, funds are locked **before** matching:
- **Bid (Buy)**: Locks `price * qty` USD upfront from `available` into `locked`.
- **Ask (Sell)**: Locks `qty` SOL upfront from `available` into `locked`.
- **When Trade Fills**: Fills deduct from `locked` and credit `available`. No complex maker/taker balance heuristics needed.
- **When Order is Cancelled**: Unfilled remainder is refunded from `locked` back to `available`.

---

## 6. Crash Recovery: Snapshots + Fast-Forward Replay

If the matching engine process terminates abruptly (power loss, SIGKILL), its state is reconstructed with zero data loss:

```
[ Engine Boots Up with Empty RAM ]
                │
                ▼
1. Query PostgreSQL: SELECT * FROM engine_snapshots ORDER BY created_at DESC LIMIT 1;
                │
                ├─► Snapshot Found: Deserialize state into RAM. Set last_id = snapshot.last_stream_id
                └─► No Snapshot: State is empty. Set last_id = "0-0"
                │
                ▼
2. Fast-Forward Replay via Redis:
   XRANGE engine-events (last_id +
                │
                ▼
   For each event: dispatch_event(state, publisher = None, ...)
   (RAM mutates to current reality, but NO duplicate client replies emitted)
                │
                ▼
3. Switch to Live Trading:
   XREAD BLOCK 1000 STREAMS engine-events <last_id>
   (Now listening live with publisher = Some(conn))
```

---

## 7. The Double-Entry Financial Ledger

In accordance with GAAP and fintech regulatory standards, `cex-rs` implements a **mathematically closed, zero-sum double-entry general ledger**. Money cannot be created or destroyed.

### The Double-Entry Invariant:
$$\sum_{\text{all entries}} \text{amount} = 0 \quad (\text{for every currency})$$

### 1. Trade Execution Entry Example
Trader Alice buys 3 SOL from Trader Bob @ \$100 (\$300 total value):

| Account | Currency | Balance Type | Amount | Description |
|---|---|---|---|---|
| Alice (Buyer) | USD | `locked` | **-300** | Debit USD payment |
| Alice (Buyer) | SOL | `available` | **+3** | Credit purchased SOL |
| Bob (Seller) | USD | `available` | **+300** | Credit USD proceeds |
| Bob (Seller) | SOL | `locked` | **-3** | Debit sold SOL |
| **Net USD** | | | **0** | Balanced |
| **Net SOL** | | | **0** | Balanced |

### 2. Fiat Onramp / Crypto Deposit Example
Trader Alice onramps \$1,000 USD:

| Account | Currency | Balance Type | Amount | Description |
|---|---|---|---|---|
| Alice (User) | USD | `available` | **+1,000** | Credit user account |
| Treasury / Clearing (User ID 0) | USD | `clearing` | **-1,000** | Offset against external reserve |
| **Net USD** | | | **0** | Balanced |

This invariant is verified via automated integration tests directly querying PostgreSQL:
```sql
SELECT COALESCE(SUM(amount), 0)::BIGINT FROM ledger_entries WHERE currency = 'USD'; -- Must equal 0
```

---

## 8. Database Schema & Data Model

PostgreSQL 16 migrations are located in `rs-gateway/migrations/`:

```
0001_create_users_table.sql
0002_create_engine_snapshots_table.sql
0003_create_ledger_and_trade_history.sql
```

### Table Definitions

#### `users`
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `BIGSERIAL` | `PRIMARY KEY` | Unique User ID |
| `username` | `VARCHAR(32)` | `NOT NULL UNIQUE` | Alphanumeric username |
| `password_hash` | `VARCHAR(255)` | `NOT NULL` | Argon2id password hash |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Registration timestamp |

#### `engine_snapshots`
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `BIGSERIAL` | `PRIMARY KEY` | Snapshot ID |
| `last_stream_id` | `VARCHAR(64)` | `NOT NULL` | Redis stream offset of snapshot |
| `state_data` | `JSONB` | `NOT NULL` | Serialized `EngineState` |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Snapshot timestamp |

#### `trades`
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `BIGSERIAL` | `PRIMARY KEY` | Trade execution ID |
| `maker_order_id` | `BIGINT` | `NOT NULL` | Resting maker order ID |
| `taker_order_id` | `BIGINT` | `NOT NULL` | Aggressing taker order ID |
| `buyer_id` | `BIGINT` | `NOT NULL` | Buyer user ID |
| `seller_id` | `BIGINT` | `NOT NULL` | Seller user ID |
| `market` | `VARCHAR(16)` | `NOT NULL` | Trading pair (e.g. `SOL_USD`) |
| `price` | `BIGINT` | `NOT NULL` | Execution price |
| `qty` | `BIGINT` | `NOT NULL` | Filled quantity |
| `quote_amount` | `BIGINT` | `NOT NULL` | `price * qty` |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Fill timestamp |

#### `orders`
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `BIGINT` | `PRIMARY KEY` | Matching engine order ID |
| `user_id` | `BIGINT` | `NOT NULL` | Owner user ID |
| `market` | `VARCHAR(16)` | `NOT NULL` | Market symbol |
| `side` | `VARCHAR(8)` | `NOT NULL` | `bid` or `ask` |
| `price` | `BIGINT` | `NOT NULL` | Order price |
| `original_qty` | `BIGINT` | `NOT NULL` | Initial order quantity |
| `remaining_qty` | `BIGINT` | `NOT NULL` | Current unfilled quantity |
| `status` | `VARCHAR(16)` | `NOT NULL` | `open`, `partially_filled`, `filled`, `cancelled` |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Submission timestamp |
| `updated_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Last modification timestamp |

#### `ledger_entries`
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `BIGSERIAL` | `PRIMARY KEY` | Entry ID |
| `user_id` | `BIGINT` | `NOT NULL` | Account owner (0 = clearing) |
| `currency` | `VARCHAR(16)` | `NOT NULL` | Currency code (`USD`, `SOL`) |
| `amount` | `BIGINT` | `NOT NULL` | Signed change in balance |
| `balance_type` | `VARCHAR(16)` | `NOT NULL` | `available`, `locked`, `clearing` |
| `operation_type`| `VARCHAR(32)` | `NOT NULL` | `trade_fill`, `onramp`, `deposit` |
| `reference_id` | `VARCHAR(64)` | `NOT NULL` | Transaction / fill reference |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Ledger timestamp |

#### `execution_checkpoints`
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `INT` | `PRIMARY KEY (1)` | Singleton ID |
| `last_stream_id` | `VARCHAR(64)` | `NOT NULL` | Last processed execution stream ID |
| `updated_at` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Last batch commit timestamp |

---

## 9. Request-Response Demuxing (Async over Queue)

HTTP is synchronous request-response, but our matching engine processes events asynchronously over Redis. The gateway bridges this with a **zero-polling correlation demuxer**:

```
Client HTTP Request
       │
       ▼
1. Create tokio::sync::oneshot::channel() -> (tx, rx)
2. Generate UUID identifier (e.g. "req-abc-123")
3. Insert into AppState.pending: HashMap<identifier, tx>
4. XADD to "engine-events" with { queue_id: "gw-1", identifier: "req-abc-123", ... }
5. rx.await (Parks Tokio async task — 0% CPU usage)
       │
       ├─────────────────────────────────────────┐
       │ (Engine processes order)               │
       ▼                                         │
6. Engine LPUSH to "response-queue.gw-1"         │
       │                                         │
       ▼                                         │
7. Gateway Background Response Listener          │
   - BRPOP from "response-queue.gw-1"            │
   - Extracts identifier "req-abc-123"           │
   - Removes tx from AppState.pending            │
   - Calls tx.send(reply_payload) ───────────────┘
       │
       ▼
8. rx.await wakes up instantly!
9. HTTP handler returns 200 OK / 201 Created to client
```

---

## 10. Security & Production Hardening

1. **Argon2id Password Hashing**:
   - Industry-standard memory-hard password derivation (`argon2` crate).
   - Generates unique cryptographically random salts per user.
   - Constant-time verification to prevent timing side-channel attacks.
2. **Token Bucket Rate Limiting**:
   - Actix middleware (`rs-gateway/src/rate_limit/`) enforcing traffic smoothing.
   - Separate buckets: strict limits on public authentication endpoints (`/signup`, `/signin`) to prevent brute force; higher burst allowances for trading endpoints.
3. **DTO Input Validation**:
   - Mandatory validation before messages touch Redis.
   - Checks username length and alphanumeric characters, minimum password length, and positive price/quantity checks.
4. **Signal Handling & Graceful Termination**:
   - Intercepts `SIGINT` / `SIGTERM` on both engine and gateway.
   - Matching engine saves a final snapshot before terminating.
   - Gateway flushes in-flight HTTP connections before closing.

---

## 11. Performance Benchmarks & Verified Metrics

Benchmarking in `cex-rs` is separated into two tiers:

### Tier 1: In-Memory Matching Microbenchmarks (Criterion)
Executed via `cargo bench -p rs-engine` measuring pure algorithmic throughput:

| Benchmark Operation | Throughput | Latency per Operation | Sample Size |
|---|---|---|---|
| **Order Matching (`match_1000_fills`)** | **14.09 million fills / sec** | **70.9 nanoseconds** | 100 samples (50k iterations) |
| **Resting Ingestion (`place_1000_resting_orders`)** | **9.54 million orders / sec** | **104.7 nanoseconds** | 100 samples (50k iterations) |
| **Order Cancellation (`cancel_1000_orders`)** | **9.85 million cancels / sec** | **101.5 nanoseconds** | 100 samples (50k iterations) |

### Tier 2: End-to-End Full-Stack Benchmark (Release Mode)
Executed via `cargo test -p rs-gateway --test benchmark_e2e --release -- --ignored --nocapture`.
Measures full round-trip: HTTP Client $\to$ Actix Gateway $\to$ Redis Stream `XADD` $\to$ Matching Engine $\to$ Balance Lock $\to$ Redis `LPUSH` $\to$ Gateway Demuxer $\to$ HTTP 201 Created:

| Metric | Result |
|---|---|
| **Total Orders Tested** | 1,000 HTTP Limit Orders |
| **Total Elapsed Time** | 0.977 seconds |
| **End-to-End Throughput** | **1,023 requests / second** (sequential single client) |
| **Minimum Latency** | 590 µs (0.59 ms) |
| **P50 (Median) Latency** | **933 µs (0.93 ms)** |
| **Average Latency** | **975 µs (0.97 ms)** |
| **P90 Latency** | 1.22 ms |
| **P95 Latency** | 1.31 ms |
| **P99 Latency** | 1.60 ms |
| **Max Latency** | 10.77 ms |

