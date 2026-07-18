#!/usr/bin/env bash
set -euo pipefail

stress_count="${ENGARDE_STRESS_COUNT:-10}"
if ! [[ "$stress_count" =~ ^[0-9]+$ ]]; then
    echo "ENGARDE_STRESS_COUNT must be a non-negative integer" >&2
    exit 2
fi

echo "==> Go tests"
go test -timeout=3m -count=1 ./...

echo "==> Go vet"
go vet ./...

echo "==> Race detector"
CGO_ENABLED=1 go test -race -timeout=5m -count=1 ./...

if [ "$stress_count" -gt 0 ]; then
    echo "==> Low-GC shuffled stress (${stress_count}x)"
    GOGC=1 go test -timeout=5m -shuffle=on -count="$stress_count" \
        ./internal/clientrole \
        ./internal/serverrole \
        ./internal/socks5 \
        ./internal/tcpstream
fi

./build-scripts/test-fuzz.sh

echo "Production test gate passed."
