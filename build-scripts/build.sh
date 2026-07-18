#!/bin/bash
set -euo pipefail

if [ "${GITHUB_REF_TYPE:-}" = "tag" ]; then
    version="${GITHUB_REF_NAME:-${GITHUB_REF:-}}"
    version="${version#refs/tags/}"
    version="${version#v}"
elif [ -n "${GITHUB_REF:-}" ]; then
    commit=$(echo "${GITHUB_SHA:-}" | head -c 7)
    branch=${GITHUB_REF#refs/heads/}
    if [ "$branch" = "master" ]; then
        version="$commit"
    else
        version="$commit ($branch)"
    fi
elif command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
    commit=$(git rev-parse HEAD | head -c 7)
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [ "$branch" = "master" ]; then
        version="$commit"
    else
        version="$commit ($branch)"
    fi
    version="$version - UNOFFICIAL BUILD"
else
    version="UNOFFICIAL BUILD"
fi

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != "--print-version" ]; }; then
    echo "usage: $0 [--print-version]" >&2
    exit 2
fi
if [ "${1:-}" = "--print-version" ]; then
    printf '%s\n' "$version"
    exit 0
fi

dst_arch="${GOARCH:-$(go env GOARCH)}"
if [ "$dst_arch" = "386" ]; then
    dst_arch="i386"
fi

goos="${GOOS:-$(go env GOOS)}"
binary_name="engarde"
if [ "$goos" = "windows" ]; then
    binary_name="$binary_name.exe"
fi

rm -rf "dist/$goos/$dst_arch"
mkdir -p "dist/$goos/$dst_arch"
echo "Building engarde for $goos $dst_arch - ver. $version"
go build -ldflags "-s -w -X 'github.com/adrianceding/engarde/internal/version.Version=$version'" -o "dist/$goos/$dst_arch/$binary_name" ./cmd/engarde
