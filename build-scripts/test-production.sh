#!/usr/bin/env bash
set -euo pipefail

echo "==> Build version metadata"
./build-scripts/test-build-version.sh

echo "==> Go tests"
go test -timeout=3m -count=1 ./...

echo "==> Go vet"
go vet ./...

echo "==> Race detector"
CGO_ENABLED=1 go test -race -timeout=5m -count=1 ./...

echo "Production test gate passed."
