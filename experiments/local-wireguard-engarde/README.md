# Local WireGuard + engarde root-cause experiment

This experiment runs both WireGuard endpoints and both engarde roles on one Linux host. It uses two network namespaces, two veth underlay paths, and `tc` shaping to compare:

- direct WireGuard without engarde
- engarde with the fast path only
- engarde with the slow path only
- engarde with fast and slow paths together

The goal is to measure whether useful tunnel throughput is really limited by the slow path, or whether the observation comes from outer-path duplicated bytes, TCP congestion behavior, queue drops, or another bottleneck.

## Requirements

- Linux with network namespace support
- root privileges
- `ip`, `tc`, `wg`, `iperf3`, `go`
- a built engarde binary, or let the script build `.tmp/engarde`

On systems without kernel WireGuard tooling installed, install the distribution package first. For example, on Debian/Ubuntu:

```sh
sudo apt install wireguard-tools iperf3 iproute2
```

## Run

From the repository root:

```sh
experiments/local-wireguard-engarde/run.sh check
sudo experiments/local-wireguard-engarde/run.sh
```

The `check` command is non-invasive and does not change networking. The full run must use root privileges.

The script writes raw outputs to `.tmp/local-wireguard-engarde/<timestamp>/`, including iperf3 results, WireGuard counters, interface counters, qdisc stats, generated configs, and engarde logs.

## Notes

- The direct WireGuard baseline must pass before the engarde cases are meaningful.
- The script tests tunnel-level throughput by running iperf3 through the WireGuard addresses, not against engarde's outer UDP sockets.
- Running everything on one host is good for isolating protocol and queue behavior, but it does not reproduce every real NIC or driver bottleneck.