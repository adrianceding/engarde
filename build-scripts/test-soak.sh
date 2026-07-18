#!/usr/bin/env bash
set -euo pipefail

soak_duration="${ENGARDE_SOAK_DURATION:-10m}"
soak_timeout="${ENGARDE_SOAK_TIMEOUT:-15m}"
soak_race="${ENGARDE_SOAK_RACE:-0}"

if ! [[ "$soak_duration" =~ ^[1-9][0-9]*(s|m|h)$ ]]; then
    echo "ENGARDE_SOAK_DURATION must be a positive duration such as 30s or 10m" >&2
    exit 2
fi
if ! [[ "$soak_timeout" =~ ^[1-9][0-9]*(s|m|h)$ ]]; then
    echo "ENGARDE_SOAK_TIMEOUT must be a positive duration such as 2m or 15m" >&2
    exit 2
fi
if [ "$soak_race" != "0" ] && [ "$soak_race" != "1" ]; then
    echo "ENGARDE_SOAK_RACE must be 0 or 1" >&2
    exit 2
fi

race_args=()
if [ "$soak_race" = "1" ]; then
    race_args=(-race)
fi

echo "==> TCP SOCKS5 production soak for $soak_duration (race=$soak_race)"
CGO_ENABLED=1 ENGARDE_SOAK_DURATION="$soak_duration" \
    go test "${race_args[@]}" -timeout="$soak_timeout" -count=1 \
    -run='^TestTCPSOCKS5ProductionSoak$' -v ./internal/clientrole
