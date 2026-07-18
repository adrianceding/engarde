#!/usr/bin/env bash
set -euo pipefail

stress_runs="${ENGARDE_STRESS_RUNS:-10}"
if ! [[ "$stress_runs" =~ ^[1-9][0-9]*$ ]]; then
    echo "ENGARDE_STRESS_RUNS must be a positive integer" >&2
    exit 2
fi

echo "==> Low-GC shuffled diagnostic stress (${stress_runs}x)"
GOGC=1 go test -timeout=5m -shuffle=on -count="$stress_runs" \
    ./internal/clientrole \
    ./internal/serverrole \
    ./internal/socks5 \
    ./internal/tcpstream
