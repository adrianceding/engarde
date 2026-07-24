# Configuration Reference

[简体中文](configuration.zh-CN.md)

Engarde reads one YAML file and starts one role. Pass the file explicitly:

```sh
./engarde /path/to/engarde.yml
```

With no path, Engarde reads `engarde.yml` from the current directory.

## Role selection and strict parsing

The top-level key must be either `client` or `server`:

```yaml
client:
  listenAddr: "127.0.0.1:59401"
  dstAddr: "203.0.113.20:59501"
```

```yaml
server:
  listenAddr: "0.0.0.0:59501"
  peerAuth:
    users:
      edge-a: "replace-with-a-long-random-secret"
```

A client requires both `client.listenAddr` and `client.dstAddr`. A server
requires `server.listenAddr` and must also satisfy the admission safety check
described under [Server fields](#server-fields). Do not put client and server
settings in the same file; Engarde rejects a complete role when settings for
the other role are also present.

Configuration parsing is strict:

- Key names are case-sensitive and use the camelCase spelling shown here.
- Unknown keys cause startup to fail instead of being ignored.
- Removed keys such as `frontend`, `transfer.mode`, `transfer.protocol`,
  `udpBatch`, and a server-side `dstAddr` are rejected.
- Defaults are applied only after Engarde has resolved the file to one role.

This behavior is intentional: a typo cannot silently disable authentication,
an interface rule, or a resource limit.

## Address rules

Address fields use Go's `host:port` form. Bracket an IPv6 literal, for example
`[::1]:59401`.

There are two important exceptions and boundaries:

1. Unless `client.allowUnsafeFrontend` is `true`, the host text in
   `client.listenAddr` must be `localhost` (case-insensitive) or a literal
   loopback IP such as `127.0.0.1` or `::1`. Engarde does not resolve other
   hostnames during this safety check, so a custom hostname that resolves to
   loopback is still rejected.
2. Client-to-server session connections use IPv4. `client.dstAddr` and every
   `client.dstOverrides[].dstAddr` must therefore contain an IPv4 literal or a
   hostname with a reachable IPv4 address. Each eligible client interface must
   also have a non-loopback, non-link-local IPv4 address. This does not restrict
   SOCKS5 request destinations: the server can connect to IPv4, IPv6, or domain
   name targets.

Listener and destination syntax is ultimately checked when the corresponding
socket is opened. `./engarde list-interfaces` prints the raw system interface
list and the first eligible IPv4 address found on each interface. It does not
load a configuration or apply `includeInterfaces` and `excludedInterfaces`, and
interfaces without an eligible address are still listed with an empty address.

## Client fields

| Field | Required | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `description` | No | Empty | Human-readable instance description, shown in logs and management status. |
| `listenAddr` | Yes | None | Local SOCKS5 TCP listener in `host:port` form. Subject to the literal loopback rule above. |
| `dstAddr` | Yes | None | Default Engarde server address for multiplexed sessions. It must be reachable over IPv4. |
| `socks5Auth` | No | Omitted | Local RFC 1929 username/password authentication. See [Credentials](#credentials). |
| `peerAuth` | No | Omitted | Authentication used from this client to the Engarde server. See [Credentials](#credentials). |
| `allowUnsafeFrontend` | No | `false` | Allows `listenAddr` to use a non-loopback host. Expose the SOCKS5 listener only behind appropriate network access controls. |
| `includeInterfaces` | No | `[]` | Whole-interface-name glob allowlist. An empty list allows every otherwise eligible interface. |
| `excludedInterfaces` | No | `[]` | Whole-interface-name glob denylist. Exclusions take priority over inclusions. |
| `interfaceLabels` | No | `{}` | Map of exact interface names to labels displayed by the management UI. |
| `pathSelection` | No | `adaptive` in active-standby | Path selection strategy. The only currently valid value is `adaptive`. |
| `interfaceHints` | No | `{}` | Optional per-interface cost hints used only by active-standby path scoring. |
| `dstOverrides` | No | `[]` | Per-interface session destination overrides. Each item contains an exact `ifName` and an IPv4-reachable `dstAddr`. |
| `transfer` | No | See [Transfer fields](#transfer-fields) | Transport tuning and resource limits. |
| `webManager` | No | Disabled | Embedded management HTTP listener. See [Web Manager fields](#web-manager-fields). |

### Credentials

`client.socks5Auth` and `client.peerAuth` use the same YAML shape:

```yaml
username: "edge-a"
password: "replace-with-a-long-random-secret"
```

When `socks5Auth` is present, both values must contain 1 to 255 bytes and the
SOCKS5 no-auth method is rejected. RFC 1929 does not encrypt these credentials,
so keep the frontend on loopback unless another layer protects the connection.

When `peerAuth` is present, the username must contain 1 to 255 bytes and the
password 1 to 1024 bytes. The server must have the same username and password
under `server.peerAuth.users`. Byte limits are measured after UTF-8 encoding,
not by character count.

### Interface selection and overrides

Interface patterns use Go `path.Match` syntax and match the entire interface
name. Empty or malformed patterns fail configuration validation. For example:

```yaml
includeInterfaces:
  - "eth*"
  - "wlan?"
excludedInterfaces:
  - "br-*"
  - "docker*"
interfaceLabels:
  eth0: "Primary ISP"
  eth1: "Backup ISP"
pathSelection: adaptive
interfaceHints:
  wwan0:
    cost: metered
dstOverrides:
  - ifName: "eth1"
    dstAddr: "198.51.100.20:59501"
```

An interface is usable only when it passes these filters and Engarde can select
a non-loopback, non-link-local IPv4 address from it. Runtime management actions
can temporarily invert the configured state of an exact interface; resetting
the overrides restores the YAML rules.

`interfaceHints.<interface>.cost` must be `normal`, `metered`, or `avoid`. It
expresses a cost preference that Engarde cannot discover automatically; it is
not a fixed priority. Engarde still scores measured session RTT, jitter,
decaying failures, and current load. Most deployments can omit both
`pathSelection` and `interfaceHints`.

## Server fields

| Field | Required | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `description` | No | Empty | Human-readable instance description, shown in logs and management status. |
| `listenAddr` | Yes | None | TCP listener for Engarde multiplexed sessions, in `host:port` form. |
| `allowedClients` | No | `[]` | Session source allowlist. Every entry must be an IP address or CIDR. |
| `peerAuth` | No | Omitted | Map of authenticated Engarde client identities. See [Server admission](#server-admission). |
| `allowUnsafeDynamicDestination` | No | `false` | Explicitly permits startup without `allowedClients` or `peerAuth`. Intended only for isolated testing. |
| `transfer` | No | See [Transfer fields](#transfer-fields) | Transport tuning and resource limits. |
| `webManager` | No | Disabled | Embedded management HTTP listener. See [Web Manager fields](#web-manager-fields). |

### Server admission

Every server is a dynamic TCP exit: the SOCKS5 request supplies the destination
that the server will dial. Startup therefore requires at least one of:

- a non-empty `allowedClients` list;
- a configured `peerAuth` section; or
- `allowUnsafeDynamicDestination: true` as an explicit unsafe override.

`allowedClients` matches the source IP of each session connection. Entries may
be individual IPv4 or IPv6 addresses or CIDRs; whitespace around an entry is
ignored, while empty or invalid entries are rejected. Current session transport
uses IPv4, so an IPv6-only entry cannot match a current client session. This
setting is not a destination ACL.

`server.peerAuth.users` must contain at least one entry:

```yaml
peerAuth:
  users:
    edge-a: "replace-with-a-long-random-secret"
    edge-b: "replace-with-another-long-random-secret"
```

Each username must contain 1 to 255 bytes and each password 1 to 1024 bytes.
When `allowedClients` and `peerAuth` are both configured, a carrier must pass
both checks. `allowUnsafeDynamicDestination` only satisfies the startup safety
check; it does not create a destination policy.

## Transfer fields

Both roles accept the following structure under `transfer`:

```yaml
transfer:
  keepaliveIntervalMillis: 1000
  keepaliveTimeoutMillis: 5000
  tcp:
    carrierMode: redundant
    chunkSize: 16384
    carrierQueueBytes: 1048576
    reorderWindowBytes: 4194304
    dialTimeoutMillis: 5000
    openTimeoutMillis: 5000
    writeTimeoutMillis: 10000
    clientRecoveryTimeoutMillis: 0
    serverOrphanRetentionMillis: 0
    resumeOpenTimeoutMillis: 0
    maxStreams: 0
    maxCarriersPerStream: 0
    maxPendingConnections: 0
    maxPendingStreams: 0
    maxSessions: 0
    maxConcurrentResumes: 0
    maxPendingResumes: 0
    maxRecoveringStreams: 0
    maxRecoveryBytes: 0
```

For fields with a positive default, omission and an explicit `0` both select
that default. Negative values are invalid. In `redundant` mode, `0` retains the
unlimited meaning for the original five resource `max*` fields. Active-standby
fields are inactive in redundant mode; once active-standby is enabled, `0`
selects their finite defaults below and cannot request an unlimited value.

| Field | Effective default | Valid effective value | Applies to | Meaning |
| --- | ---: | ---: | --- | --- |
| `tcp.carrierMode` | `redundant` | `redundant` or `active-standby` | Both | `redundant` duplicates data on healthy paths; `active-standby` uses one active carrier per Flow while keeping warm Sessions. |
| `keepaliveIntervalMillis` | `1000` | `> 0` | Both | Interval between multiplexed-session keepalive probes. |
| `keepaliveTimeoutMillis` | `5000` | Greater than the same file's interval | Both | Time without a keepalive response before a multiplexed session is closed. |
| `tcp.chunkSize` | `16384` | `1..65536` | Both | Maximum application payload placed in one DATA frame. |
| `tcp.carrierQueueBytes` | `1048576` | `1..2147483647` | Both | Maximum queued outbound application data per carrier and the smux per-stream receive buffer. |
| `tcp.reorderWindowBytes` | `4194304` | `1..2147483647` | Both | Bound for out-of-order receive data, unacknowledged replay history, and the smux session receive buffer. |
| `tcp.dialTimeoutMillis` | `5000` | `> 0` | Both | Client-to-server session dial timeout on the client; target dial timeout on the server. |
| `tcp.openTimeoutMillis` | `5000` | `> 0` | Both | Bound for SOCKS5 negotiation, session handshake, and virtual stream OPEN setup. |
| `tcp.writeTimeoutMillis` | `10000` | `> 0` | Both | Deadline for a carrier or endpoint write that makes no progress. |
| `tcp.clientRecoveryTimeoutMillis` | active-standby: `3000` | `> 0` | Client | Total budget for retaining the App TCP and attempting RESUME after the active carrier is lost. |
| `tcp.serverOrphanRetentionMillis` | active-standby: `9000` | See relationships below | Server | Budget for retaining the original target TCP and Flow state with no carrier. |
| `tcp.resumeOpenTimeoutMillis` | active-standby: `750` | `> 0` and below client recovery | Client | Limit for one RESUME stream open and response. |
| `tcp.maxStreams` | redundant: unlimited; active-standby: `2048` | `>= 0`; active requires `> 0` | Both | Maximum concurrent logical TCP streams. |
| `tcp.maxCarriersPerStream` | Unlimited | `>= 0` | Server | Maximum carriers for a redundant Flow. An active-standby Flow always has one. |
| `tcp.maxPendingConnections` | Unlimited | `>= 0` | Server | Maximum concurrent physical connection handshakes that have not yet become multiplexed sessions. |
| `tcp.maxPendingStreams` | Unlimited | `>= 0` | Server | Maximum concurrent virtual streams still processing OPEN and destination setup. |
| `tcp.maxSessions` | Unlimited | `>= 0` | Server | Maximum established physical multiplexed sessions. |
| `tcp.maxConcurrentResumes` | active-standby: `64` | active requires `> 0` | Client | Concurrent recoverable OPEN, RESUME, or proactive migration operations. |
| `tcp.maxPendingResumes` | active-standby: `128` | active requires `> 0` | Both | Client recovery/migration queue and server concurrent RESUME admission limit. |
| `tcp.maxRecoveringStreams` | active-standby: `1024` | active requires `1..maxStreams` | Both | Maximum recovering Flows whose endpoints remain retained. |
| `tcp.maxRecoveryBytes` | active-standby: `536870912` | active requires at least `reorderWindowBytes` | Both | Aggregate unacknowledged replay history across recovering Flows. |

Settings marked as server-only are still parsed and validated if placed in a
client file, but they do not impose a client-side limit.

### Active-standby mode

Configure both the client and server with:

```yaml
transfer:
  tcp:
    carrierMode: active-standby
```

A peer without the capability, or a mode mismatch, fails the Session
explicitly; Engarde never silently falls back to duplicated data. Every
eligible interface still keeps one authenticated, probed physical Session,
while each healthy logical Flow has exactly one active carrier. Session probes
provide RTT and jitter samples; scoring also includes decaying failure
penalties, active Flow count, and smux stream pressure, with interface name as
a deterministic final tie-breaker.
Sessions carrying active Flows probe every `250ms`; idle warm Sessions probe
every `1s`, with a `400ms` response limit. One failure remains inside the
health grace period. Two consecutive failures make the physical Session
unavailable, enqueue its Flows for priority recovery, and RESUME them through
a healthy warm Session. Any successful probe resets the consecutive-failure
count.

An existing Flow can RESUME only across Sessions with the same
`serverInstanceID`. Different `dstOverrides` may reach different addresses of
one server process, but not independent server instances with separate memory.
A server restart makes old target TCP connections unrecoverable. Successful
switching preserves both the original App TCP and target TCP; the Flow closes
with a terminal error only after all paths remain unavailable beyond the
recovery budget.
The client filters Sessions whose advertised retention is shorter than its
recovery budget, and the server rejects an oversized recoverable OPEN before
dialing the target.

The configuration must satisfy:

```text
resumeOpenTimeoutMillis < clientRecoveryTimeoutMillis
serverOrphanRetentionMillis >= clientRecoveryTimeoutMillis + resumeOpenTimeoutMillis
maxRecoveringStreams <= maxStreams
maxRecoveryBytes >= reorderWindowBytes
```

On recovery overload, Engarde protects established Flows first and releases
excess state deterministically, newest Flow first. Returning to
`carrierMode: redundant` requires a process restart, which interrupts existing
Flows.

### Keepalive settings across both ends

Keepalive settings are validated within each file, not negotiated or compared
between the two hosts. Each side probes its multiplexed session at
`keepaliveIntervalMillis` and closes the session when no response arrives
within `keepaliveTimeoutMillis`.

Each file must also satisfy
`keepaliveTimeoutMillis > keepaliveIntervalMillis`. Using the defaults on both
ends (1 second / 5 seconds) satisfies both rules. Very small values can cause
healthy carrier sessions to churn under transient delay or CPU load.

## Web Manager fields

Both `client.webManager` and `server.webManager` accept:

| Field | Required | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `listenAddr` | Required to enable | Empty / disabled | Management UI and JSON API HTTP listener in `host:port` form. |
| `username` | No | Empty | HTTP Basic Auth username. Must be configured together with `password`. |
| `password` | No | Empty | HTTP Basic Auth password. Must be configured together with `username`. |

Credentials without `listenAddr` are invalid. A configured listener with both
credentials omitted is allowed but unauthenticated. The SOCKS5 frontend's
loopback validation does not apply to this listener, and the management server
does not provide TLS. Prefer a loopback address, configure both credentials,
and use a protected tunnel or reverse proxy when remote access is required.

```yaml
webManager:
  listenAddr: "127.0.0.1:9001"
  username: "engarde"
  password: "replace-with-a-management-secret"
```

## Complete examples

- [Complete annotated template](../engarde.yml.sample)
- [Client example](../examples/config/tcp-socks5-client.yml)
- [Server example](../examples/config/tcp-socks5-server.yml)

The examples use documentation-only addresses and placeholder secrets. Replace
them before running Engarde, and keep real credentials out of version control.
