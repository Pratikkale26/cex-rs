# cex-rs

A high-performance, asynchronous Centralized Exchange (CEX) backend built in **Rust** using **Tokio**, **Actix-web**, and **Redis**.

---

## 🏗️ Architecture

The workspace is organized as a Cargo multi-crate workspace (`resolver = "3"`, edition 2024):

```
cex-rs/
├── Cargo.toml               # Workspace manifest
├── rs-shared/               # Common data contracts & Redis channel names
├── rs-engine/               # In-memory Matching Engine & Balance Ledger
└── rs-gateway/              # Actix-web HTTP API & JWT Authentication
```

### Message Flow

```
[ HTTP Client ]
      │
      ▼  (1. JSON Request)
┌─────────────┐       (2. LPUSH request)       ┌─────────────┐
│  rs-gateway │ ─────────────────────────────► │  rs-engine  │
│ (Actix-web) │                                │  (Matching) │
│             │ ◄───────────────────────────── │             │
└─────────────┘       (3. LPUSH reply)         └─────────────┘
      │
      ▼  (4. JSON Response)
[ HTTP Client ]
```

1. **`rs-gateway`**: Receives incoming HTTP requests, validates auth/input, assigns a unique `identifier`, and pushes a message to a Redis channel via `LPUSH`.
2. **`rs-engine`**: Continuously listens using `BRPOP` in a single-threaded async event loop, locks funds upfront, matches orders in an in-memory limit orderbook, updates balance ledgers, and pushes the reply back to the gateway's dedicated response queue.
3. **Response routing**: `rs-gateway` polls its instance-specific response queue in a background Tokio task and resolves the waiting HTTP handler using an in-memory `tokio::sync::oneshot` channel.

---

## 🚀 Quick Start

### 1. Prerequisites
* **Rust**: `rustc` / `cargo` (1.80+)
* **Redis**: Running on `127.0.0.1:6379` (e.g. via Docker `docker run -d -p 6379:6379 redis:7-alpine`)

### 2. Run the Engine
In terminal 1:
```bash
cd /home/pratik/projects/cex-rs
cargo run -p rs-engine
```

### 3. Run the HTTP Gateway
In terminal 2:
```bash
cd /home/pratik/projects/cex-rs
cargo run -p rs-gateway
```
The gateway starts on `http://127.0.0.1:3000`.

---

## 🧪 Testing

### Unit Tests (Matching Engine)
Verifies orderbook price-time priority, partial fills, FIFO ordering, and order cancellations:
```bash
cargo test -p rs-engine
```

### End-to-End Integration Tests
Run the test suite against the running Rust servers:
```bash
cd /home/pratik/projects/ts-rs-cex/ts-cex
bun test
```
Result: **25 pass, 0 fail (172 assertions in ~360ms)**.

---

## 📡 API Endpoints

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| `POST` | `/signup` | Public | Register user & initialize USD/SOL balances |
| `POST` | `/signin` | Public | Authenticate user & return JWT Bearer token |
| `GET` | `/balance` | Bearer | Get user's USD and stock balances (available vs locked) |
| `POST` | `/onramp` | Bearer | Add USD to user's available balance |
| `POST` | `/deposit/{asset}` | Bearer | Add asset (e.g. `sol`) to user's available balance |
| `POST` | `/order` | Bearer | Place a limit order (`asset`, `side`, `price`, `qty`) |
| `DELETE` | `/order/{order_id}` | Bearer | Cancel open resting order & refund locked funds |
| `GET` | `/orderbook/{asset}` | Public | Get current orderbook depth (sorted bids and asks) |
| `GET` | `/orders/open` | Bearer | Fetch open resting orders for the authenticated user |
| `POST` | `/reset` | Public | Reset engine state & gateway user store (for testing) |

---

## 📖 In-Depth Documentation

* [`ROADMAP.md`](./ROADMAP.md): Production hardening, database persistence, and advanced order types.
* [`notes/`](./notes/): Technical deep-dive on Perpetual Futures (Perps), liquidations, funding rates, and Web3 Perp DEX architectures.
