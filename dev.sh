#!/usr/bin/env bash
set -e

# Clean up any lingering background processes or ports
fuser -k 3000/tcp 2>/dev/null || true
fuser -k 5173/tcp 2>/dev/null || true
killall -9 rs-engine 2>/dev/null || true
killall -9 rs-gateway 2>/dev/null || true
killall -9 rs-bot 2>/dev/null || true

# Trap SIGINT, SIGTERM, and EXIT to gracefully terminate child processes on Ctrl+C
trap 'echo -e "\nShutting down cex-rs servers..."; kill $(jobs -p) 2>/dev/null; exit 0' SIGINT SIGTERM EXIT

echo "=================================================="
echo " Starting cex-rs services"
echo "   - rs-engine  (Matching Engine)"
echo "   - rs-gateway (HTTP API on http://127.0.0.1:3000)"
echo "   - rs-bot     (Market Maker & Liquidity Bot)"
echo "   - web        (React Vite Terminal on http://127.0.0.1:5173)"
echo " Press Ctrl+C to stop all servers"
echo "=================================================="

cargo run -p rs-engine &
cargo run -p rs-gateway &
cargo run -p rs-bot &
(cd web && pnpm run dev) &

# Wait for all processes
wait
