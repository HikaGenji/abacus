#!/usr/bin/env bash
# Run the full zenoh + iceoryx2 HFT demo pipeline.
#
# Each component runs in a separate tmux pane.
# If tmux is not available, processes run in the background and
# logs go to /tmp/hft-demo-*.log.
#
# Usage:
#   ./scripts/run_demo.sh                   # pipeline only, 10 000 ticks/s
#   ./scripts/run_demo.sh --rate 50000      # faster tick rate
#   ./scripts/run_demo.sh --record          # also start infra + recorder
#   ./scripts/run_demo.sh --rate 5000 --record

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY_DIR="$ROOT/target/release"
RATE=10000
RECORD=0

usage() {
    echo "Usage: $0 [--rate N] [--record]"
    echo "  --rate N   Ticks per second (default: 10000)"
    echo "  --record   Start QuestDB/InfluxDB/Grafana and the recorder crate"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rate)   RATE="$2"; shift 2 ;;
        --record) RECORD=1;  shift   ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ── Pre-flight checks ──────────────────────────────────────────────────────────

check_built() {
    local bins=(market-data-pub md-handler strategy order-gateway)
    [[ $RECORD -eq 1 ]] && bins+=(recorder)
    for bin in "${bins[@]}"; do
        if [[ ! -x "$BINARY_DIR/$bin" ]]; then
            echo "ERROR: $BINARY_DIR/$bin not found. Run 'cargo build --release' first."
            exit 1
        fi
    done
}

start_infra() {
    if ! command -v docker &>/dev/null; then
        echo "ERROR: docker not found — install Docker to use --record."
        exit 1
    fi
    echo "Starting QuestDB, InfluxDB, Grafana..."
    docker compose -f "$ROOT/docker-compose.yml" up -d
    echo ""
}

# ── Launch helpers ─────────────────────────────────────────────────────────────

run_tmux() {
    tmux new-session -d -s hft-demo -x 220 -y 50 2>/dev/null || true

    if [[ $RECORD -eq 1 ]]; then
        # 5-pane layout: left col (gateway, strategy) | right col (md-handler, market-data-pub, recorder)
        tmux split-window -h  -t hft-demo
        tmux split-window -v  -t hft-demo:0.0
        tmux split-window -v  -t hft-demo:0.2
        tmux split-window -v  -t hft-demo:0.3
    else
        # 4-pane 2×2 layout
        tmux split-window -h  -t hft-demo
        tmux split-window -v  -t hft-demo:0.0
        tmux split-window -v  -t hft-demo:0.2
    fi

    tmux send-keys -t hft-demo:0.0 \
        "RUST_LOG=info $BINARY_DIR/order-gateway" Enter
    tmux send-keys -t hft-demo:0.1 \
        "RUST_LOG=info $BINARY_DIR/strategy" Enter
    tmux send-keys -t hft-demo:0.2 \
        "RUST_LOG=info $BINARY_DIR/md-handler" Enter
    tmux send-keys -t hft-demo:0.3 \
        "sleep 1 && RUST_LOG=info $BINARY_DIR/market-data-pub --rate $RATE" Enter

    if [[ $RECORD -eq 1 ]]; then
        tmux send-keys -t hft-demo:0.4 \
            "sleep 2 && RUST_LOG=info $BINARY_DIR/recorder" Enter
    fi

    echo "Demo running in tmux session 'hft-demo'."
    echo "  Attach : tmux attach -t hft-demo"
    echo "  Kill   : tmux kill-session -t hft-demo"
    if [[ $RECORD -eq 1 ]]; then
        echo ""
        echo "Infrastructure:"
        echo "  QuestDB  web console : http://localhost:9000"
        echo "  InfluxDB             : http://localhost:8086  (token: hft_token)"
        echo "  Grafana              : http://localhost:3000  (admin / admin)"
    fi
}

run_background() {
    echo "tmux not found — running in background, logging to /tmp/hft-demo-*.log"
    RUST_LOG=info "$BINARY_DIR/order-gateway"  > /tmp/hft-demo-order-gateway.log  2>&1 &
    RUST_LOG=info "$BINARY_DIR/strategy"       > /tmp/hft-demo-strategy.log       2>&1 &
    sleep 0.2
    RUST_LOG=info "$BINARY_DIR/md-handler"     > /tmp/hft-demo-md-handler.log     2>&1 &
    sleep 1
    RUST_LOG=info "$BINARY_DIR/market-data-pub" --rate "$RATE" \
                                               > /tmp/hft-demo-market-data.log    2>&1 &
    if [[ $RECORD -eq 1 ]]; then
        sleep 2
        RUST_LOG=info "$BINARY_DIR/recorder"   > /tmp/hft-demo-recorder.log       2>&1 &
    fi

    echo ""
    echo "Tail logs : tail -f /tmp/hft-demo-*.log"
    jobs -p > /tmp/hft-demo.pids
    echo "Stop all  : kill \$(cat /tmp/hft-demo.pids)"
}

# ── Main ───────────────────────────────────────────────────────────────────────

check_built
[[ $RECORD -eq 1 ]] && start_infra

if command -v tmux &>/dev/null; then
    run_tmux
else
    run_background
fi
