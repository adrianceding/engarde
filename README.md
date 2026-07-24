# Engarde

**Continuous SOCKS5 TCP relay over multiple uplinks.**

[简体中文](README.zh-CN.md) | [Releases](https://github.com/adrianceding/engarde/releases) | [Container](https://github.com/adrianceding/engarde/pkgs/container/engarde) | [Issues](https://github.com/adrianceding/engarde/issues)

Engarde runs as a client/server pair. Applications connect to a local SOCKS5
listener. The client maintains one multiplexed TCP **session** to the server on
each available IPv4 interface, and every logical TCP stream gets one virtual
**carrier** on every healthy session in the default `redundant` mode. In
`active-standby` mode, a stream uses one adaptively selected active carrier and
keeps the other sessions warm.

```text
                                      +-- eth0 / ISP A --+
SOCKS5 application -> Engarde client -+                  +-> Engarde server -> destination
                                      +-- wlan0 / ISP B -+
```

> [!IMPORTANT]
> Engarde is not MPTCP and does not combine uplink bandwidth. The default
> `redundant` mode duplicates stream data across healthy carriers.
> `active-standby` sends one copy and takes over the same App TCP and target TCP
> through a warm Session when a path fails.

## When Engarde Fits

Engarde is intended for situations where uninterrupted TCP delivery matters
more than bandwidth efficiency, for example:

- a mobile or remote site using several unstable 4G, 5G, Wi-Fi, or wired links;
- a dual-ISP workstation that needs a SOCKS5 exit through a stable server;
- a service where a single uplink failure should not immediately break an
  active TCP stream.

It is not a VPN, an encryption layer, a destination firewall, or a throughput
aggregator. Each selected client interface must have an eligible IPv4 address
and a route to the Engarde server. Per-interface server addresses are supported
when the paths do not share one destination address.

## Highlights

- One persistent multiplexed session on every eligible interface, established
  when the client starts and shared by all SOCKS5 streams.
- Default multi-carrier redundancy or one active carrier with adaptive path
  scoring and bounded recovery.
- Ordered delivery, cumulative acknowledgements, and a bounded replay window
  across carrier changes.
- Runtime interface discovery, whole-name glob filters, and per-interface server
  address overrides.
- Separate authentication for the local SOCKS5 frontend and remote Engarde
  peers, plus server source allowlists and resource limits.
- One cross-platform binary for both roles, with an embedded status UI and JSON
  API.

## Requirements

- One reachable server host and one client host.
- TCP port `59501` allowed from the client paths to the server in the examples
  below.
- At least one non-loopback, non-link-local IPv4 interface on the client.
- Linux client binaries need `CAP_NET_RAW` for interface-bound session sockets.
- Client and server binaries from the same release are recommended.

Session connections use IPv4. The SOCKS5 destination requested by an
application may still be an IPv4 address, IPv6 address, or domain name.

## Install

Download the binary for each host and `SHA256SUMS.txt` from
[GitHub Releases](https://github.com/adrianceding/engarde/releases). For example,
on Linux amd64:

```sh
sha256sum --check SHA256SUMS.txt --ignore-missing
install -m 0755 engarde-linux-amd64 engarde
./engarde -v
```

To build that target from a source checkout instead:

```sh
make linux-amd64
install -m 0755 dist/linux/amd64/engarde ./engarde
```

Building from source requires Go 1.25+, Node.js 22, npm, Git, and Make. See the
[deployment guide](docs/deployment.md) for other platforms, Docker, systemd,
configuration permissions, and upgrades.

## Quick Start

The following setup enables peer authentication between Engarde hosts, local
SOCKS5 authentication, and a loopback-only management UI. Replace every value
marked `CHANGE_ME` before starting either process.

### 1. Check the client interfaces

```sh
./engarde list-interfaces
```

The command lists system interfaces and their first usable IPv4 address. It does
not apply configuration filters. Confirm that at least two intended interfaces
can reach the server if you want path redundancy.

### 2. Create the server configuration

On the server host, create `server.yml`:

```yaml
server:
  description: "Engarde exit"
  listenAddr: "0.0.0.0:59501"
  peerAuth:
    users:
      edge-a: "CHANGE_ME_LONG_RANDOM_PEER_SECRET"
```

Production deployments should also set resource limits appropriate for the
host; count limits default to unlimited. Then start the server:

```sh
./engarde server.yml
```

### 3. Create the client configuration

On the client host, create `client.yml` and replace `SERVER_IPV4` with the
server address reachable from the selected interfaces:

```yaml
client:
  description: "Multi-uplink SOCKS5 client"
  listenAddr: "127.0.0.1:59401"
  dstAddr: "SERVER_IPV4:59501"
  socks5Auth:
    username: "client"
    password: "CHANGE_ME_LOCAL_SOCKS_SECRET"
  peerAuth:
    username: "edge-a"
    password: "CHANGE_ME_LONG_RANDOM_PEER_SECRET"
  excludedInterfaces:
    - "br-*"
    - "docker*"
  webManager:
    listenAddr: "127.0.0.1:9001"
    username: "engarde"
    password: "CHANGE_ME_MANAGEMENT_SECRET"
```

Use `includeInterfaces` when Engarde should use only named interfaces. Patterns
match the whole interface name, for example `eth*`, `enp*`, `wlan*`, or `wlp*`.

The configuration above uses `redundant` by default. To enable traffic
optimization, add the following field to both the client and server files.
Other recovery settings have finite defaults and normally need no override:

```yaml
transfer:
  tcp:
    carrierMode: active-standby
```

On Linux, grant the client binary the capability needed to bind sessions to
specific interfaces, then start it as a regular user:

```sh
sudo setcap cap_net_raw=ep ./engarde
./engarde client.yml
```

The Docker client service already receives only the required `NET_RAW`
capability.

### 4. Verify the relay and its paths

Open `http://127.0.0.1:9001/` and sign in with the management credentials. The
client should report one Session for every reachable, eligible interface.
The same status is available as JSON:

```sh
curl --user engarde:CHANGE_ME_MANAGEMENT_SECRET \
  http://127.0.0.1:9001/api/v1/get-list
```

Send a request through the SOCKS5 listener:

```sh
curl --socks5-hostname 127.0.0.1:59401 \
  --proxy-user client:CHANGE_ME_LOCAL_SOCKS_SECRET \
  https://example.com/
```

The request succeeds only after the server has connected to the requested
destination and at least one carrier is ready. In `redundant` mode, another
existing carrier continues after one is lost. In `active-standby`, Engarde
RESUMEs the same Flow on a warm Session without rebuilding the App TCP or target
TCP during the recovery budget. The Flow closes with a terminal error only
after all paths remain unavailable beyond that budget.

## Configuration

One configuration file starts exactly one role. A client requires
`client.listenAddr` and `client.dstAddr`. A server requires
`server.listenAddr` and at least one admission control:

- `server.allowedClients` for source IP/CIDR filtering;
- `server.peerAuth` for authenticated Engarde clients; or
- `server.allowUnsafeDynamicDestination: true` for isolated testing only.

Configuration parsing is strict: unknown or removed fields stop startup rather
than being ignored. See the [configuration reference](docs/configuration.md),
the [client example](examples/config/tcp-socks5-client.yml), the
[server example](examples/config/tcp-socks5-server.yml), and the fully commented
[`engarde.yml.sample`](engarde.yml.sample).

## Security Boundaries

- RFC 1929 SOCKS5 credentials travel in plaintext on the local TCP connection.
- Peer authentication controls session admission but does not encrypt or
  integrity-protect later OPEN and DATA frames.
- An admitted client can request loopback, private, metadata, and public
  destinations reachable from the server; Engarde has no destination ACL.
- The management listener is plain HTTP. Keep it on loopback and use a TLS
  reverse proxy or VPN for remote access.
- Original redundant-mode count limits default to unlimited; active-standby
  stream and recovery resources have finite defaults.

Read the [security model and deployment checklist](docs/security.md) before
exposing either role outside an isolated network.

## Documentation

- [Configuration reference](docs/configuration.md)
- [Deployment guide](docs/deployment.md)
- [Security model and checklist](docs/security.md)
- [Annotated configuration template](engarde.yml.sample)
- [Repository and contribution guide](AGENTS.md)

## Docker

The repository Compose file runs one role per host with host networking. Pin a
released image tag instead of relying on `latest`:

```sh
export ENGARDE_IMAGE=ghcr.io/adrianceding/engarde:0.6.0
cp examples/config/tcp-socks5-server.yml server.yml
${EDITOR:-vi} server.yml
sudo chown "$(id -u):65532" server.yml
chmod 0640 server.yml
docker compose up -d
```

For a client host, copy and edit the client example, then run:

```sh
export ENGARDE_IMAGE=ghcr.io/adrianceding/engarde:0.6.0
cp examples/config/tcp-socks5-client.yml client.yml
${EDITOR:-vi} client.yml
sudo chown "$(id -u):65532" client.yml
chmod 0640 client.yml
docker compose up -d client
```

Configured listener ports bind directly on the Linux host; they are not Compose
port mappings. The ownership example assumes conventional rootful Docker.
Consult the [deployment guide](docs/deployment.md) for rootless or
user-namespaced Docker and for production use.

## Limitations

- SOCKS5 TCP `CONNECT` only; no raw TCP frontend, UDP relay, `BIND`, or
  `UDP ASSOCIATE`.
- IPv4 client carrier paths only.
- Redundancy or active-standby takeover rather than bandwidth aggregation.
- Active-standby recovery is limited to Sessions on one server process. A
  server restart, or all paths exceeding the recovery budget, cannot preserve
  the existing logical stream.
- No encryption, destination ACL, configuration reload, or built-in TLS for the
  management service.

## Build and Test

```sh
make test
make test-production
make test-fuzz FUZZ_ITERATIONS=1000
make test-stress STRESS_RUNS=10
make test-soak SOAK_DURATION=10m
make
```

`make test-production` is the repeatable correctness gate and runs the Go test
suite, vet, and race detector. Fuzzing uses a fixed iteration budget; shuffled
stress and time-based soak tests are explicit diagnostic tools, not release
gates. Soak tests can expose leaks or timer behavior but do not establish
protocol correctness. The regular Go suite still replays committed fuzz corpus
entries as regression cases. `make` builds the embedded Angular UI and all
supported release targets under `dist/{os}/{arch}/`. Frontend tests are run
separately from `webmanager/` with `npm test`.

## Project Status and License

This is a pre-1.0, AI-assisted rewrite and consolidation of
[porech/engarde](https://github.com/porech/engarde). The automated release gate
reduces protocol, concurrency, resource-cleanup, and platform-build regression
risk, but production deployments still need validation on their target
multi-interface network.

Engarde remains licensed under GPLv2. See [`LICENSE.txt`](LICENSE.txt).
