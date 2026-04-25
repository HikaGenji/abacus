#!/usr/bin/env bash
# Run the iceoryx2 latency benchmark (ping-pong).
#
# Opens two terminal panes (tmux) or two background processes.
# Results are printed to stdout by the ping side when done.
#
# Usage:
#   ./scripts/run_bench.sh              # 1 000 000 iterations
#   ./scripts/run_bench.sh --count 100000

set -euo pipefail

COUNT=${1:-1000000}
BINARY_DIR="$(dirname "$0")/../target/release"
BENCH="$BINARY_DIR/latency-bench"

if [[ ! -x "$BENCH" ]]; then
    echo "ERROR: $BENCH not found. Run 'cargo build --release' first."
    exit 1
fi

if command -v tmux &>/dev/null; then
    tmux new-session -d -s hft-bench -x 160 -y 40 2>/dev/null || true
    tmux split-window -h -t hft-bench

    tmux send-keys -t hft-bench:0.0 "RUST_LOG=info $BENCH pong" Enter
    sleep 0.5
    tmux send-keys -t hft-bench:0.1 \
        "RUST_LOG=info $BENCH ping --count $COUNT && tmux wait-for -S bench-done" Enter

    tmux wait-for bench-done 2>/dev/null || true
    echo "Benchmark complete. Check the right pane for results."
    echo "Attach with: tmux attach -t hft-bench"
else
    echo "tmux not found — running pong in background"
    RUST_LOG=warn "$BENCH" pong > /tmp/hft-bench-pong.log 2>&1 &
    PONG_PID=$!
    sleep 0.5

    RUST_LOG=warn "$BENCH" ping --count "$COUNT"
    kill "$PONG_PID" 2>/dev/null || true
fi
