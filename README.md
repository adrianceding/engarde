# engarde

[简体中文](README.zh-CN.md)

> WARNING: This rewrite was produced with AI assistance and has not yet received a full human code review. Do not deploy it in production or security-sensitive environments without independent review, auditing, and testing.

engarde is a UDP relay for existing WireGuard tunnels. It sends WireGuard UDP traffic across multiple network paths, tracks interface changes while running, and relays return traffic from a server-side endpoint back to active clients.

The project is based on the original [porech/engarde](https://github.com/porech/engarde). Thanks to the original author and contributors for creating engarde and making this work possible.

## What It Does

engarde sits outside WireGuard. It does not create a TUN device, implement WireGuard, or replace `wg`/`wg-quick`. Instead, it expects a working WireGuard endpoint and forwards that endpoint's UDP packets through relay sockets.

Typical use cases:

- Send the same WireGuard UDP packet through multiple uplinks.
- Keep relaying as network interfaces appear, disappear, or change address.
- Exclude specific interfaces from relay traffic.
- Route selected interfaces to different remote relay addresses.
- Observe client/server state through the embedded web management UI or JSON API.

## How It Works

engarde has two roles, selected by `engarde.yml`:

- Client mode listens near the local WireGuard endpoint, opens UDP sockets on active network interfaces, and forwards packets to the configured server relay address.
- Server mode listens for relay packets from clients, forwards them to the server-side WireGuard endpoint, learns active client addresses, and fans return packets back to active clients.

Both roles are shipped in one `engarde` binary. The role is selected from the config file so the same binary can run either side.

## Features

- Multi-path UDP forwarding for existing WireGuard traffic.
- Dynamic interface discovery and route refresh while running.
- Per-interface destination overrides in client mode.
- Active-client learning and return-path fanout in server mode.
- Bounded per-target fanout queues to reduce one slow target blocking others.
- Optional embedded web UI and JSON status API.
- Linux, Windows, and macOS build targets.

## Installation

Download a prebuilt binary for your platform from [GitHub Releases](https://github.com/adrianceding/engarde/releases). On Unix-like systems, make it executable:

```sh
chmod +x engarde-linux-amd64
```

You can also build from source with `make`; see [Build From Source](#build-from-source).

## Quick Start

Create a config from the sample:

```sh
cp engarde.yml.sample engarde.yml
```

Edit `engarde.yml` so it contains exactly one complete role: either `client:` or `server:`.

Run with an explicit config file:

```sh
./engarde engarde.yml
```

Useful commands:

```sh
./engarde -v
./engarde list-interfaces
```

If no config path is provided, `engarde` reads `engarde.yml` from the current directory.

## Configuration

Client mode requires `client.listenAddr` and `client.dstAddr`:

```yaml
client:
  listenAddr: "127.0.0.1:59401"
  dstAddr: "1.2.3.4:59501"
  writeTimeout: 10
  udpBatch:
    enabled: true
    readSize: 32
    writeSize: 32
```

Server mode requires `server.listenAddr` and `server.dstAddr`:

```yaml
server:
  listenAddr: "0.0.0.0:59501"
  dstAddr: "127.0.0.1:59301"
  clientTimeout: 30
  writeTimeout: 10
  udpBatch:
    enabled: true
    readSize: 32
    writeSize: 32
```

Role selection rules:

- A complete `client:` section starts client mode.
- A complete `server:` section starts server mode.
- If both roles are complete, startup fails so the selected role is never ambiguous.

Important fields:

- `writeTimeout`: socket write timeout in milliseconds. Use a plain integer; negative values disable the write deadline.
- `udpBatch`: optional UDP batch I/O settings. It is enabled by default when omitted; set `enabled: false` to force single-packet I/O, or tune `readSize` and `writeSize` for local performance testing.
- `transfer`: optional transfer strategy. `mode: direct` keeps the original redundant UDP fanout. `mode: adaptive` uses lightweight DATA/ACK frames, keepalives, bounded pending/duplicate windows, and a per-path adaptive ACK timeout to send on the best path first, then fall back to all healthy paths. `ackTimeoutMillis` is the minimum/initial timeout. Adaptive DATA frames add a 36-byte header; set WireGuard MTU so inner UDP packets fit within the framed payload limit.
- `excludedInterfaces`: client-side interfaces that must not be used for relay traffic.
- `interfaceLabels`: human-friendly labels shown in the web UI.
- `dstOverrides`: client-side per-interface remote relay address overrides.
- `clientTimeout`: server-side active-client timeout in seconds.
- `webManager`: optional embedded web UI and JSON API listener.

Example `webManager` block:

```yaml
webManager:
  listenAddr: "0.0.0.0:9001"
  username: "engarde"
  password: "engarde"
```

See `engarde.yml.sample` for a fuller example.

## systemd Service

A Linux systemd unit is provided at `contrib/systemd/engarde.service`. It assumes:

- Binary path: `/usr/local/bin/engarde`
- Config path: `/etc/engarde/engarde.yml`
- Service user and group: `engarde`

Install and start it with:

```sh
sudo useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin engarde
sudo install -m 0755 engarde-linux-amd64 /usr/local/bin/engarde
sudo install -d -m 0750 -o root -g engarde /etc/engarde
sudo install -m 0640 -o root -g engarde engarde.yml.sample /etc/engarde/engarde.yml
sudo install -m 0644 contrib/systemd/engarde.service /etc/systemd/system/engarde.service
sudo systemctl daemon-reload
sudo systemctl enable --now engarde
```

Edit `/etc/engarde/engarde.yml` before starting the service.

The default unit runs without Linux capabilities. If Linux client mode needs to bind UDP sockets to specific interfaces and fails with a permission error, install the optional drop-in:

```sh
sudo install -d -m 0755 /etc/systemd/system/engarde.service.d
sudo install -m 0644 contrib/systemd/engarde.service.d/bind-device.conf /etc/systemd/system/engarde.service.d/bind-device.conf
sudo systemctl daemon-reload
sudo systemctl restart engarde
```

## Build From Source

Prerequisites:

- Go 1.25 or later
- Node.js and npm for the web management frontend
- Git and Make for versioned builds

Build all supported platforms:

```sh
make
```

Build only the current platform manually:

```sh
make web-assets
go build -ldflags "-s -w" -o engarde ./cmd/engarde
```

The Makefile writes platform artifacts under `dist/{os}/{arch}/`. Each platform contains one binary: `engarde` on Unix-like systems or `engarde.exe` on Windows.

The embedded web UI is built from `webmanager/` by `make web-assets` and copied into `internal/assets/browser` before Go builds. Generated files in `internal/assets/browser` are intentionally not committed; build through `make` or run `make web-assets` before a manual `go build` when you need the management UI embedded.

## Status

This repository is an AI-assisted rewrite and consolidation of engarde. It has tests and local build checks, but it still needs careful human review before being treated as production-ready.

## License

This project preserves the original engarde GPLv2 license. See `LICENSE.txt` for details.

engarde is a derivative work of the original engarde project, and the original license and attribution are intentionally retained out of respect for the upstream work.
