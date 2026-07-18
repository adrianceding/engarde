#!/usr/bin/env bash
set -euo pipefail

fuzz_iterations="${ENGARDE_FUZZ_ITERATIONS:-1000}"
if ! [[ "$fuzz_iterations" =~ ^[1-9][0-9]*$ ]]; then
    echo "ENGARDE_FUZZ_ITERATIONS must be a positive integer" >&2
    exit 2
fi

run_fuzz_target() {
    local package="$1"
    local target="$2"

    go test -timeout=15m -run='^$' -fuzz="^${target}$" \
        -fuzztime="${fuzz_iterations}x" "$package"
}

run_package_fuzzers() {
    local package="$1"
    local targets
    targets="$(go test -list '^Fuzz' "$package" | awk '/^Fuzz/ { print $1 }')"
    if [ -z "$targets" ]; then
        echo "No fuzz targets found in $package" >&2
        exit 1
    fi
    while IFS= read -r target; do
        echo "==> Fuzz $package $target for $fuzz_iterations iterations"
        run_fuzz_target "$package" "$target"
    done <<< "$targets"
}

run_package_fuzzers ./internal/socks5
run_package_fuzzers ./internal/tcpstream
