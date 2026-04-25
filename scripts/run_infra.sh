#!/usr/bin/env bash
# Start QuestDB and Grafana for the HFT demo recorder.
# Requires Docker + Docker Compose.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker &>/dev/null; then
    echo "ERROR: docker is not installed or not in PATH"
    exit 1
fi

ACTION="${1:-up}"

case "$ACTION" in
  up)
    docker compose up -d
    echo ""
    echo "Infrastructure started:"
    echo "  QuestDB  web console : http://localhost:9000"
    echo "  QuestDB  ILP TCP     : localhost:9009"
    echo "  QuestDB  PostgreSQL  : localhost:8812"
    echo "  Grafana              : http://localhost:3000  (admin / admin)"
    echo ""
    echo "Run the recorder:"
    echo "  RUST_LOG=info ./target/release/recorder"
    echo ""
    echo "Quick QuestDB queries:"
    cat <<'EOF'
  SELECT timestamp, symbol, bid, ask, mid, spread
  FROM market_ticks
  LATEST ON timestamp PARTITION BY symbol;

  SELECT timestamp, side, price, e2e_ns, decision_ns
  FROM order_signals
  WHERE timestamp > dateadd('h', -1, now())
  ORDER BY timestamp;
EOF
    ;;
  down)
    docker compose down
    echo "Infrastructure stopped."
    ;;
  logs)
    docker compose logs -f
    ;;
  *)
    echo "Usage: $0 [up|down|logs]"
    exit 1
    ;;
esac
