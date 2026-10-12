#!/usr/bin/env bash
set -e

# Clean up any lingering background processes or ports
fuser -k 3000/tcp 2>/dev/null || true
killall -9 rs-engine 2>/dev/null || true
killall -9 rs-gateway 2>/dev/null || true

# Trap SIGINT, SIGTERM, and EXIT to gracefully terminate child processes on Ctrl+C
trap 'echo -e "\nShutting down cex-rs servers..."; kill $(jobs -p) 2>/dev/null; exit 0' SIGINT SIGTERM EXIT

echo "=================================================="
echo " Starting cex-rs backend services"
echo "   - rs-engine  (Matching Engine)"
echo "   - rs-gateway (HTTP API & WS on http://127.0.0.1:3000)"
echo " Press Ctrl+C to stop all servers"
echo "=================================================="

# Pre-build workspace once to prevent Cargo file-lock contention across concurrent processes
cargo build --workspace --quiet

cargo run -p rs-engine --quiet &
cargo run -p rs-gateway --quiet &

# Wait for all processes
wait
