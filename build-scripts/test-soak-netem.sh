#!/usr/bin/env bash
set -euo pipefail

interface="${ENGARDE_NETEM_INTERFACE:-lo}"
delay="${ENGARDE_NETEM_DELAY:-20ms}"
loss="${ENGARDE_NETEM_LOSS:-0.5%}"
mtu="${ENGARDE_NETEM_MTU:-1500}"

if ! [[ "$interface" =~ ^[a-zA-Z0-9_.:-]+$ ]]; then
    echo "ENGARDE_NETEM_INTERFACE contains unsupported characters" >&2
    exit 2
fi
if ! [[ "$delay" =~ ^[1-9][0-9]*(us|ms|s)$ ]]; then
    echo "ENGARDE_NETEM_DELAY must be a positive tc duration such as 20ms" >&2
    exit 2
fi
if ! [[ "$loss" =~ ^([0-9]|[1-9][0-9])([.][0-9]+)?%$|^100%$ ]]; then
    echo "ENGARDE_NETEM_LOSS must be a percentage such as 0.5%" >&2
    exit 2
fi
if ! [[ "$mtu" =~ ^[1-9][0-9]+$ ]] || [ "$mtu" -lt 576 ]; then
    echo "ENGARDE_NETEM_MTU must be an integer of at least 576" >&2
    exit 2
fi
if [ ! -r "/sys/class/net/$interface/mtu" ]; then
    echo "Network interface $interface does not exist" >&2
    exit 1
fi
for command in ip tc; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "$command is required for the netem soak" >&2
        exit 1
    fi
done
if [ "$EUID" -ne 0 ] && ! command -v sudo >/dev/null 2>&1; then
    echo "sudo is required for the netem soak when not running as root" >&2
    exit 1
fi
if [ "$EUID" -ne 0 ] && ! sudo -n true >/dev/null 2>&1; then
    echo "passwordless sudo is required for the netem soak" >&2
    exit 1
fi

run_privileged() {
    if [ "$EUID" -eq 0 ]; then
        "$@"
    else
        sudo -n "$@"
    fi
}

original_mtu="$(cat "/sys/class/net/$interface/mtu")"
original_qdisc="$(run_privileged tc qdisc show dev "$interface" root)"
case "$original_qdisc" in
    ""|qdisc\ noqueue\ *) ;;
    *)
        echo "Refusing to replace the existing root qdisc on $interface: $original_qdisc" >&2
        exit 1
        ;;
esac

cleanup() {
    local exit_code=$?
    local cleanup_failed=0
    trap - EXIT INT TERM
    if ! run_privileged tc qdisc del dev "$interface" root >/dev/null 2>&1; then
        echo "Unable to remove the injected qdisc from $interface" >&2
        cleanup_failed=1
    fi
    if ! run_privileged ip link set dev "$interface" mtu "$original_mtu" >/dev/null 2>&1; then
        echo "Unable to restore the MTU on $interface to $original_mtu" >&2
        cleanup_failed=1
    fi
    if [ "$exit_code" -eq 0 ] && [ "$cleanup_failed" -ne 0 ]; then
        exit_code=1
    fi
    exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "==> Configure $interface: mtu=$mtu delay=$delay loss=$loss"
run_privileged ip link set dev "$interface" mtu "$mtu"
run_privileged tc qdisc replace dev "$interface" root netem delay "$delay" loss "$loss"

./build-scripts/test-soak.sh
