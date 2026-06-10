#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_ID=$(date +%Y%m%d-%H%M%S)
OUT_DIR="${ROOT_DIR}/.tmp/local-wireguard-engarde/${RUN_ID}"
ENGARDE_BIN=${ENGARDE_BIN:-"${ROOT_DIR}/.tmp/engarde"}

CLIENT_NS=eglc
SERVER_NS=egls
FAST_CLIENT_IF=egfastc
FAST_SERVER_IF=egfasts
SLOW_CLIENT_IF=egslowc
SLOW_SERVER_IF=egslows
DIRECT_CLIENT_IF=egdirectc
DIRECT_SERVER_IF=egdirects

CLIENT_WG_IP=10.80.0.2
SERVER_WG_IP=10.80.0.1
CLIENT_WG_PORT=51820
SERVER_WG_PORT=51821
ENGARDE_CLIENT_LISTEN=127.0.0.1:59401
ENGARDE_SERVER_LISTEN_FAST=10.91.0.2:59501
ENGARDE_SERVER_LISTEN_SLOW=10.92.0.2:59501
ENGARDE_SERVER_LISTEN_ANY=0.0.0.0:59501

SLOW_RATE=${SLOW_RATE:-10mbit}
SLOW_DELAY=${SLOW_DELAY:-40ms}
IPERF_TIME=${IPERF_TIME:-8}
UDP_RATE=${UDP_RATE:-50M}

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

usage() {
  cat <<'EOF'
Usage:
  experiments/local-wireguard-engarde/run.sh check
  sudo experiments/local-wireguard-engarde/run.sh

Commands:
  check   Check commands, kernel support hints, and root requirement without changing networking.
EOF
}

require_root() {
  if [[ ${EUID} -ne 0 ]]; then
    echo "This experiment must run as root because it creates netns, veth, WireGuard interfaces, and qdisc rules." >&2
    exit 1
  fi
}

require_commands() {
  local missing=0
  for cmd in ip tc wg iperf3 go; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "Missing required command: ${cmd}" >&2
      missing=1
    fi
  done
  if [[ ${missing} -ne 0 ]]; then
    echo "Install the missing tools, then rerun this script." >&2
    exit 1
  fi
}

check_environment() {
  local missing=0
  for cmd in ip tc wg iperf3 go; do
    if command -v "$cmd" >/dev/null 2>&1; then
      printf '%-8s %s\n' "$cmd" "$(command -v "$cmd")"
    else
      printf '%-8s missing\n' "$cmd"
      missing=1
    fi
  done

  printf 'user     %s\n' "$(id)"
  printf 'kernel   %s\n' "$(uname -r)"
  if [[ -d /sys/module/wireguard ]]; then
    printf 'module   wireguard loaded\n'
  else
    printf 'module   wireguard not loaded or unavailable\n'
  fi
  if [[ ${EUID} -eq 0 ]]; then
    printf 'root     yes\n'
  else
    printf 'root     no; full experiment requires sudo/root\n'
  fi

  if [[ ${missing} -ne 0 ]]; then
    return 1
  fi
}

run_ns() {
  ip netns exec "$1" bash -c "$2"
}

cleanup() {
  set +e
  if [[ -n ${SERVER_PID:-} ]]; then kill "${SERVER_PID}" 2>/dev/null; wait "${SERVER_PID}" 2>/dev/null; fi
  if [[ -n ${CLIENT_PID:-} ]]; then kill "${CLIENT_PID}" 2>/dev/null; wait "${CLIENT_PID}" 2>/dev/null; fi
  ip netns exec "${SERVER_NS}" pkill -f "iperf3 -s" 2>/dev/null
  ip netns del "${CLIENT_NS}" 2>/dev/null
  ip netns del "${SERVER_NS}" 2>/dev/null
}

build_engarde() {
  if [[ -x ${ENGARDE_BIN} ]]; then
    return
  fi
  log "building engarde binary at ${ENGARDE_BIN}"
  mkdir -p "$(dirname "${ENGARDE_BIN}")"
  (cd "${ROOT_DIR}" && go build -o "${ENGARDE_BIN}" ./cmd/engarde)
}

setup_namespaces() {
  cleanup
  ip netns add "${CLIENT_NS}"
  ip netns add "${SERVER_NS}"
  ip -n "${CLIENT_NS}" link set lo up
  ip -n "${SERVER_NS}" link set lo up

  ip link add "${FAST_CLIENT_IF}" type veth peer name "${FAST_SERVER_IF}"
  ip link set "${FAST_CLIENT_IF}" netns "${CLIENT_NS}"
  ip link set "${FAST_SERVER_IF}" netns "${SERVER_NS}"
  ip -n "${CLIENT_NS}" addr add 10.91.0.1/24 dev "${FAST_CLIENT_IF}"
  ip -n "${SERVER_NS}" addr add 10.91.0.2/24 dev "${FAST_SERVER_IF}"
  ip -n "${CLIENT_NS}" link set "${FAST_CLIENT_IF}" up
  ip -n "${SERVER_NS}" link set "${FAST_SERVER_IF}" up

  ip link add "${SLOW_CLIENT_IF}" type veth peer name "${SLOW_SERVER_IF}"
  ip link set "${SLOW_CLIENT_IF}" netns "${CLIENT_NS}"
  ip link set "${SLOW_SERVER_IF}" netns "${SERVER_NS}"
  ip -n "${CLIENT_NS}" addr add 10.92.0.1/24 dev "${SLOW_CLIENT_IF}"
  ip -n "${SERVER_NS}" addr add 10.92.0.2/24 dev "${SLOW_SERVER_IF}"
  ip -n "${CLIENT_NS}" link set "${SLOW_CLIENT_IF}" up
  ip -n "${SERVER_NS}" link set "${SLOW_SERVER_IF}" up

  ip link add "${DIRECT_CLIENT_IF}" type veth peer name "${DIRECT_SERVER_IF}"
  ip link set "${DIRECT_CLIENT_IF}" netns "${CLIENT_NS}"
  ip link set "${DIRECT_SERVER_IF}" netns "${SERVER_NS}"
  ip -n "${CLIENT_NS}" addr add 10.90.0.1/24 dev "${DIRECT_CLIENT_IF}"
  ip -n "${SERVER_NS}" addr add 10.90.0.2/24 dev "${DIRECT_SERVER_IF}"
  ip -n "${CLIENT_NS}" link set "${DIRECT_CLIENT_IF}" up
  ip -n "${SERVER_NS}" link set "${DIRECT_SERVER_IF}" up

  ip netns exec "${CLIENT_NS}" tc qdisc add dev "${SLOW_CLIENT_IF}" root handle 1: tbf rate "${SLOW_RATE}" burst 64kbit latency 400ms
  ip netns exec "${CLIENT_NS}" tc qdisc add dev "${SLOW_CLIENT_IF}" parent 1:1 handle 10: netem delay "${SLOW_DELAY}"
  ip netns exec "${SERVER_NS}" tc qdisc add dev "${SLOW_SERVER_IF}" root handle 1: tbf rate "${SLOW_RATE}" burst 64kbit latency 400ms
  ip netns exec "${SERVER_NS}" tc qdisc add dev "${SLOW_SERVER_IF}" parent 1:1 handle 10: netem delay "${SLOW_DELAY}"
}

generate_keys() {
  CLIENT_PRIV=$(wg genkey)
  CLIENT_PUB=$(printf '%s' "${CLIENT_PRIV}" | wg pubkey)
  SERVER_PRIV=$(wg genkey)
  SERVER_PUB=$(printf '%s' "${SERVER_PRIV}" | wg pubkey)
}

configure_wireguard() {
  local mode=$1
  ip -n "${CLIENT_NS}" link del wg0 2>/dev/null || true
  ip -n "${SERVER_NS}" link del wg0 2>/dev/null || true

  ip -n "${CLIENT_NS}" link add wg0 type wireguard
  ip -n "${SERVER_NS}" link add wg0 type wireguard
  ip -n "${CLIENT_NS}" addr add "${CLIENT_WG_IP}/24" dev wg0
  ip -n "${SERVER_NS}" addr add "${SERVER_WG_IP}/24" dev wg0

  local client_endpoint
  local server_listen
  if [[ ${mode} == direct ]]; then
    client_endpoint="10.90.0.2:${SERVER_WG_PORT}"
    server_listen=${SERVER_WG_PORT}
  else
    client_endpoint="${ENGARDE_CLIENT_LISTEN}"
    server_listen=${SERVER_WG_PORT}
  fi

  ip netns exec "${SERVER_NS}" wg set wg0 private-key <(printf '%s\n' "${SERVER_PRIV}") listen-port "${server_listen}" peer "${CLIENT_PUB}" allowed-ips "${CLIENT_WG_IP}/32"
  ip netns exec "${CLIENT_NS}" wg set wg0 private-key <(printf '%s\n' "${CLIENT_PRIV}") listen-port "${CLIENT_WG_PORT}" peer "${SERVER_PUB}" allowed-ips "${SERVER_WG_IP}/32" endpoint "${client_endpoint}" persistent-keepalive 1
  ip -n "${CLIENT_NS}" link set wg0 mtu 1280 up
  ip -n "${SERVER_NS}" link set wg0 mtu 1280 up
}

write_engarde_configs() {
  local case_name=$1
  local excluded=("wg0")
  case "${case_name}" in
    fast) excluded+=("${SLOW_CLIENT_IF}" "${DIRECT_CLIENT_IF}") ;;
    slow) excluded+=("${FAST_CLIENT_IF}" "${DIRECT_CLIENT_IF}") ;;
    both) excluded+=("${DIRECT_CLIENT_IF}") ;;
    *) echo "unknown engarde case ${case_name}" >&2; exit 1 ;;
  esac

  {
    cat <<EOF
client:
  description: "local experiment client ${case_name}"
  listenAddr: "${ENGARDE_CLIENT_LISTEN}"
  dstAddr: "${ENGARDE_SERVER_LISTEN_FAST}"
  writeTimeout: 10
  excludedInterfaces:
EOF
    for iface in "${excluded[@]}"; do
      printf '    - "%s"\n' "${iface}"
    done
    cat <<EOF
  dstOverrides:
    - ifName: "${SLOW_CLIENT_IF}"
      dstAddr: "${ENGARDE_SERVER_LISTEN_SLOW}"
EOF
  } >"${OUT_DIR}/engarde-client-${case_name}.yml"

  cat >"${OUT_DIR}/engarde-server-${case_name}.yml" <<EOF
server:
  description: "local experiment server ${case_name}"
  listenAddr: "${ENGARDE_SERVER_LISTEN_ANY}"
  dstAddr: "127.0.0.1:${SERVER_WG_PORT}"
  clientTimeout: 30
  writeTimeout: 10
EOF
}

start_iperf_server() {
  ip netns exec "${SERVER_NS}" iperf3 -s -B "${SERVER_WG_IP}" >"${OUT_DIR}/iperf3-server.log" 2>&1 &
  IPERF_SERVER_PID=$!
  sleep 1
}

stop_iperf_server() {
  if [[ -n ${IPERF_SERVER_PID:-} ]]; then
    kill "${IPERF_SERVER_PID}" 2>/dev/null || true
    wait "${IPERF_SERVER_PID}" 2>/dev/null || true
    unset IPERF_SERVER_PID
  fi
}

capture_stats() {
  local label=$1
  {
    echo "=== wg client ==="
    ip netns exec "${CLIENT_NS}" wg show || true
    echo "=== wg server ==="
    ip netns exec "${SERVER_NS}" wg show || true
    echo "=== client links ==="
    ip -n "${CLIENT_NS}" -s link || true
    echo "=== server links ==="
    ip -n "${SERVER_NS}" -s link || true
    echo "=== slow client qdisc ==="
    ip netns exec "${CLIENT_NS}" tc -s qdisc show dev "${SLOW_CLIENT_IF}" || true
    echo "=== slow server qdisc ==="
    ip netns exec "${SERVER_NS}" tc -s qdisc show dev "${SLOW_SERVER_IF}" || true
  } >"${OUT_DIR}/stats-${label}.txt" 2>&1
}

run_iperf_pair() {
  local label=$1
  log "running iperf for ${label}"
  timeout 10s ip netns exec "${CLIENT_NS}" ping -c 3 -W 2 "${SERVER_WG_IP}" >"${OUT_DIR}/ping-${label}.txt" 2>&1 || true
  capture_stats "${label}-before"
  timeout "$((IPERF_TIME + 15))s" ip netns exec "${CLIENT_NS}" iperf3 -c "${SERVER_WG_IP}" -t "${IPERF_TIME}" -J >"${OUT_DIR}/iperf3-tcp-${label}.json" 2>"${OUT_DIR}/iperf3-tcp-${label}.err" || true
  timeout "$((IPERF_TIME + 15))s" ip netns exec "${CLIENT_NS}" iperf3 -c "${SERVER_WG_IP}" -u -b "${UDP_RATE}" -t "${IPERF_TIME}" -J >"${OUT_DIR}/iperf3-udp-${label}.json" 2>"${OUT_DIR}/iperf3-udp-${label}.err" || true
  capture_stats "${label}-after"
}

run_direct_case() {
  log "configuring direct WireGuard baseline"
  configure_wireguard direct
  start_iperf_server
  run_iperf_pair direct
  stop_iperf_server
}

start_engarde_case() {
  local case_name=$1
  write_engarde_configs "${case_name}"
  ip netns exec "${SERVER_NS}" "${ENGARDE_BIN}" "${OUT_DIR}/engarde-server-${case_name}.yml" >"${OUT_DIR}/engarde-server-${case_name}.log" 2>&1 &
  SERVER_PID=$!
  ip netns exec "${CLIENT_NS}" "${ENGARDE_BIN}" "${OUT_DIR}/engarde-client-${case_name}.yml" >"${OUT_DIR}/engarde-client-${case_name}.log" 2>&1 &
  CLIENT_PID=$!
  sleep 2
}

stop_engarde_case() {
  if [[ -n ${CLIENT_PID:-} ]]; then kill "${CLIENT_PID}" 2>/dev/null || true; wait "${CLIENT_PID}" 2>/dev/null || true; unset CLIENT_PID; fi
  if [[ -n ${SERVER_PID:-} ]]; then kill "${SERVER_PID}" 2>/dev/null || true; wait "${SERVER_PID}" 2>/dev/null || true; unset SERVER_PID; fi
}

run_engarde_case() {
  local case_name=$1
  log "configuring engarde ${case_name} case"
  configure_wireguard engarde
  start_engarde_case "${case_name}"
  start_iperf_server
  run_iperf_pair "engarde-${case_name}"
  stop_iperf_server
  stop_engarde_case
}

main() {
  case "${1:-run}" in
    check)
      check_environment
      return
      ;;
    -h|--help|help)
      usage
      return
      ;;
    run)
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac

  require_root
  require_commands
  mkdir -p "${OUT_DIR}"
  trap cleanup EXIT
  build_engarde
  setup_namespaces
  generate_keys

  log "writing outputs to ${OUT_DIR}"
  run_direct_case
  run_engarde_case fast
  run_engarde_case slow
  run_engarde_case both
  log "done; inspect ${OUT_DIR}"
}

main "$@"