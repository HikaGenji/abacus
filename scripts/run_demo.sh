#!/usr/bin/env bash
# Run the full zenoh + iceoryx2 HFT demo pipeline.
#
# Each component runs in a separate terminal tab via tmux.
# If tmux is not available, processes run in the background and
# logs go to /tmp/hft-demo-*.log.
#
# Usage:
#   ./scripts/run_demo.sh            # 10 000 ticks/s (default)
#   ./scripts/run_demo.sh --rate 1000

set -euo pipefail

RATE=${1:-10000}
BINARY_DIR="$(dirname "$0")/../target/release"

check_built() {
    for bin in market-data-pub md-handler strategy order-gateway; do
        if [[ ! -x "$BINARY_DIR/$bin" ]]; then
            echo "ERROR: $BINARY_DIR/$bin not found. Run 'cargo build --release' first."
            exit 1
        fi
    done
}

run_tmux() {
    tmux new-session -d -s hft-demo -x 220 -y 50 2>/dev/null || true
    tmux split-window -h -t hft-demo
    tmux split-window -v -t hft-demo:0.0
    tmux split-window -v -t hft-demo:0.2

    tmux send-keys -t hft-demo:0.0 \
        "RUST_LOG=info $BINARY_DIR/order-gateway" Enter
    tmux send-keys -t hft-demo:0.1 \
        "RUST_LOG=info $BINARY_DIR/strategy" Enter
    tmux send-keys -t hft-demo:0.2 \
        "RUST_LOG=info $BINARY_DIR/md-handler" Enter
    tmux send-keys -t hft-demo:0.3 \
        "sleep 1 && RUST_LOG=info $BINARY_DIR/market-data-pub --rate $RATE" Enter

    echo "Demo running in tmux session 'hft-demo'."
    echo "Attach with:  tmux attach -t hft-demo"
    echo "Kill with:    tmux kill-session -t hft-demo"
}

run_background() {
    echo "tmux not found — running in background, logging to /tmp/hft-demo-*.log"
    RUST_LOG=info "$BINARY_DIR/order-gateway" > /tmp/hft-demo-order-gateway.log 2>&1 &
    RUST_LOG=info "$BINARY_DIR/strategy"      > /tmp/hft-demo-strategy.log      2>&1 &
    sleep 0.2
    RUST_LOG=info "$BINARY_DIR/md-handler"    > /tmp/hft-demo-md-handler.log    2>&1 &
    sleep 1
    RUST_LOG=info "$BINARY_DIR/market-data-pub" --rate "$RATE" \
                                              > /tmp/hft-demo-market-data.log   2>&1 &

    echo "All components started. PIDs:"
    jobs -l
    echo ""
    echo "Tail logs with:  tail -f /tmp/hft-demo-*.log"
    echo "Stop all:        kill \$(cat /tmp/hft-demo.pids)"
    jobs -p > /tmp/hft-demo.pids
}

check_built

if command -v tmux &>/dev/null; then
    run_tmux
else
    run_background
fi
