# cex-rs

A high-performance, purely asynchronous Centralized Cryptocurrency Exchange (CEX) backend built in **Rust** using **Tokio**, **Actix-web**, **Redis Streams**, and **PostgreSQL**.

Built with an **LMAX-style memory-first architecture**, an append-only **Write-Ahead Log (WAL)**, periodic snapshotting with fast-forward replay recovery, and a mathematically closed **double-entry financial ledger**.

---

## 🏗️ Architecture Overview

For a comprehensive technical deep-dive, see [**`ARCHITECTURE.md`**](./ARCHITECTURE.md).

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

## 📦 Workspace Structure

```
cex-rs/
├── Cargo.toml               # Workspace manifest (Rust 2024 edition, resolver 3)
├── docker-compose.yml       # PostgreSQL 16 & Redis 7 development services
├── ARCHITECTURE.md          # Comprehensive architectural & technical specification
├── rs-shared/               # Universal domain contracts & models
│   └── src/
│       ├── channels.rs      # Redis stream keys & queue prefixes
│       └── models/          # Execution events, DTOs, requests & responses
├── rs-engine/               # Matching Engine & In-Memory State Machine
│   └── src/
│       ├── engine.rs        # Stream event loop runner & deterministic replay
│       ├── snapshot.rs      # PostgreSQL snapshot load / save logic
│       ├── state.rs         # In-memory balance ledger & market state
│       ├── orderbook/       # BTreeMap + VecDeque price-time priority book
│       └── handlers/        # Channel message processors & execution emitters
└── rs-gateway/              # HTTP API, Security & Cold-Path Persistence
    ├── migrations/          # SQL database migrations (users, snapshots, ledger)
    └── src/
        ├── auth/            # Argon2id password hashing & JWT extractor
        ├── dto/             # Request & response Data Transfer Objects
        ├── ledger/          # Async cold-path batch worker (PostgreSQL sink)
        ├── rate_limit/      # Token Bucket anti-spam middleware
        ├── redis/           # WAL stream producer & response demuxer
        └── routes/          # Actix route handlers (auth, balance, orders, trades, ledger)
```

---

## 🚀 Quick Start

### 1. Prerequisites
* **Rust**: `rustc` / `cargo` (1.80+)
* **Docker & Docker Compose**: For local PostgreSQL and Redis

### 2. Start Infrastructure Services
Boot PostgreSQL 16 and Redis 7 in Docker:
```bash
docker compose up -d
```
* **PostgreSQL**: `localhost:5432` (`cex`, user: `postgres`, password: `postgrespassword`)
* **Redis**: `localhost:6379`

### 3. Run All Services
Run both the matching engine and gateway concurrently:
```bash
make dev
# or
./dev.sh
```
The gateway starts on `http://127.0.0.1:3000`.

### 4. Run Services Individually
Terminal 1 (Matching Engine):
```bash
cargo run -p rs-engine
```

Terminal 2 (HTTP Gateway):
```bash
cargo run -p rs-gateway
```

---

## 🧪 Testing

The repository features 100% pure Rust unit and integration tests (zero external Node.js dependencies):

```bash
cargo test --workspace
```

### Test Coverage Highlights:
- **`rs-engine` (17 unit tests)**: Price-time priority, FIFO queue order at same price, partial fills, cancellations, state serialization roundtrip.
- **`rs-gateway` (9 unit tests)**: Argon2id password hashing and constant-time verification, DTO validation, Token Bucket rate limiting.
- **`tests/integration.rs` (5 integration tests)**:
  - `test_validation_rejects_invalid_signup`: Input validation defense.
  - `test_unauthorized_endpoints_without_jwt`: JWT authentication protection.
  - `test_e2e_trading_lifecycle`: Complete trading flow (onramp, order matching, balance locks, order cancellations).
  - `test_engine_crash_snapshot_and_replay_recovery`: Simulates an abrupt engine process crash, recovers state from PostgreSQL snapshots, replays subsequent Redis Stream WAL events, and validates that balances and orderbook depth are 100% identical.
  - `test_double_entry_ledger_and_trade_history`: Exercises async execution batching, queries `/trades/my`, `/trades/sol`, and `/ledger`, and verifies mathematical zero-sum double-entry ledger conservation in PostgreSQL (`SUM(amount) == 0`).

---

## ⚡ Performance Benchmarks

### 1. In-Memory Matching Core (Criterion Microbenchmarks)
Run with `cargo bench -p rs-engine`:
* **Order Matching (`match_1000_fills`)**: **14.09 million fills / sec** (~70.9 ns / match)
* **Resting Order Ingestion**: **9.54 million orders / sec** (~104.7 ns / insert)
* **Order Cancellation**: **9.85 million cancels / sec** (~101.5 ns / cancel)

### 2. End-to-End Full-Stack Throughput & Latency
Run with `cargo test -p rs-gateway --test benchmark_e2e --release -- --ignored --nocapture`:
Measures round-trip: HTTP Client $\to$ Actix Gateway $\to$ Redis Stream WAL $\to$ In-Memory Engine $\to$ Balance Lock $\to$ Redis Reply $\to$ Demuxer $\to$ HTTP 201 Created:
* **Throughput**: **1,023 requests / sec** (single client loop)
* **P50 (Median) Latency**: **933 µs (0.93 ms)**
* **Average Latency**: **975 µs (0.97 ms)**
* **P99 Latency**: **1.60 ms**

---

## 📡 API Endpoints

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| `POST` | `/signup` | Public | Register user & save Argon2id hash in PostgreSQL |
| `POST` | `/signin` | Public | Authenticate user credentials & return JWT Bearer token |
| `GET` | `/balance` | Bearer | Get user's available and locked USD and SOL balances |
| `POST` | `/onramp` | Bearer | Add USD to user's available balance |
| `POST` | `/deposit/{asset}` | Bearer | Add asset (e.g. `sol`) to user's available balance |
| `GET` | `/ledger` | Bearer | Fetch authenticated user's double-entry financial ledger statement |
| `POST` | `/order` | Bearer | Submit a limit order (`asset`, `side`, `price`, `qty`) |
| `DELETE` | `/order/{order_id}` | Bearer | Cancel an open resting order & refund locked funds |
| `GET` | `/orders/open` | Bearer | Fetch open resting orders for the authenticated user |
| `GET` | `/trades/my` | Bearer | Fetch authenticated user's executed trade history |
| `GET` | `/trades/{asset}` | Public | Fetch recent public market trades (e.g. `/trades/sol`) |
| `GET` | `/orderbook/{asset}` | Public | Get current orderbook depth (sorted bids and asks) |
| `POST` | `/reset` | Public | Clear all tables and streams (for automated test suites) |

---

## 📖 Further Reading

* [**`ARCHITECTURE.md`**](./ARCHITECTURE.md): Comprehensive architectural specification, matching algorithms, WAL mechanics, and disaster recovery.
* [**`notes/ROADMAP.md`**](./notes/ROADMAP.md): Engineering roadmap, completed milestones, and upcoming features (Market Orders, WebSockets, Fees).
