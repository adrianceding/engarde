#!/usr/bin/env bash
set -euo pipefail

assert_version() {
    local expected="$1"
    shift
    local actual
    actual="$(env "$@" ./build-scripts/build.sh --print-version)"
    if [ "$actual" != "$expected" ]; then
        echo "build version = '$actual', want '$expected'" >&2
        exit 1
    fi
}

assert_version "1.2.3" \
    GITHUB_REF_TYPE=tag \
    GITHUB_REF_NAME=v1.2.3 \
    GITHUB_REF=refs/tags/v1.2.3 \
    GITHUB_SHA=0123456789abcdef

assert_version "0123456" \
    GITHUB_REF_TYPE=branch \
    GITHUB_REF_NAME=master \
    GITHUB_REF=refs/heads/master \
    GITHUB_SHA=0123456789abcdef

assert_version "0123456 (feature/deterministic-tests)" \
    GITHUB_REF_TYPE=branch \
    GITHUB_REF_NAME=feature/deterministic-tests \
    GITHUB_REF=refs/heads/feature/deterministic-tests \
    GITHUB_SHA=0123456789abcdef
