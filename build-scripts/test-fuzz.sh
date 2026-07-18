#!/usr/bin/env bash
set -euo pipefail

fuzz_time="${ENGARDE_FUZZ_TIME:-5s}"
if [ "$fuzz_time" = "0" ]; then
    echo "Fuzzing disabled by ENGARDE_FUZZ_TIME=0."
    exit 0
fi
if ! [[ "$fuzz_time" =~ ^[1-9][0-9]*(s|m)$ ]]; then
    echo "ENGARDE_FUZZ_TIME must be 0 or a positive duration such as 10s or 2m" >&2
    exit 2
fi

run_fuzz_target() {
    local package="$1"
    local target="$2"
    local output

    output="$(mktemp)"
    if go test -timeout=15m -run='^$' -fuzz="^${target}$" -fuzztime="$fuzz_time" "$package" 2>&1 | tee "$output"; then
        rm -f "$output"
        return 0
    fi

    if grep -Eq '^[[:space:]]+context deadline exceeded[[:space:]]*$' "$output" &&
        ! grep -Fq 'Failing input written to' "$output"; then
        echo "Retrying $target after Go fuzz deadline race (golang/go#75804)." >&2
        rm -f "$output"
        go test -timeout=15m -run='^$' -fuzz="^${target}$" -fuzztime="$fuzz_time" "$package"
        return
    fi

    rm -f "$output"
    return 1
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
        echo "==> Fuzz $package $target for $fuzz_time"
        run_fuzz_target "$package" "$target"
    done <<< "$targets"
}

run_package_fuzzers ./internal/socks5
run_package_fuzzers ./internal/tcpstream
