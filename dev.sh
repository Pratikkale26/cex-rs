#!/usr/bin/env bash
set -e

# Trap SIGINT, SIGTERM, and EXIT to gracefully terminate child processes on Ctrl+C
trap 'echo -e "\nShutting down cex-rs servers..."; kill $(jobs -p) 2>/dev/null; exit 0' SIGINT SIGTERM EXIT

echo "=================================================="
echo " Starting cex-rs services"
echo "   - rs-engine  (Matching Engine)"
echo "   - rs-gateway (HTTP API on http://127.0.0.1:3000)"
echo " Press Ctrl+C to stop all servers"
echo "=================================================="

cargo run -p rs-engine &
cargo run -p rs-gateway &

# Wait for both processes
wait
