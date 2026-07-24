# Security Model

[简体中文](security.zh-CN.md)

Engarde is a multi-path TCP relay, not a secure tunnel. It provides admission
controls at several boundaries, but it does not add encryption to the SOCKS5
frontend, session connections, the management listener, or the server's
connection to a destination. Use application-layer encryption and a protected
network or tunnel wherever confidentiality and integrity are required.

```text
application -- SOCKS5 --> Engarde client == TCP/smux sessions ==> Engarde server -- TCP --> destination
operator ---------------- HTTP ----------------> Web Manager on the client or server
```

The Engarde server is a trusted component: it receives the requested
destination, opens the outbound connection, and can observe any application
payload that is not encrypted end to end.

## SOCKS5 frontend

The client accepts SOCKS5 TCP `CONNECT` requests on `client.listenAddr`.

- Without `client.socks5Auth`, the frontend accepts SOCKS5 no-authentication.
- With `client.socks5Auth`, the client requires RFC 1929 username/password
  authentication. RFC 1929 sends both values in cleartext over the SOCKS5 TCP
  connection. It does not encrypt the proxied application traffic.
- By default, configuration validation requires a loopback listener. Setting
  `client.allowUnsafeFrontend: true` only bypasses that guard; it does not add
  authentication, encryption, rate limiting, or a firewall rule.

Keep the frontend on `127.0.0.1`, `[::1]`, or `localhost` when possible. If a
remote application must reach it, place the listener behind an authenticated
VPN or encrypted tunnel and restrict access with host and network firewalls.
Do not expose an unauthenticated SOCKS5 listener to an untrusted network.

`socks5Auth` controls who can use the local frontend. It does not authenticate
the Engarde client to the Engarde server; configure `peerAuth` separately for
that boundary.

## Session authentication and transport

`client.peerAuth` and `server.peerAuth.users` authenticate Engarde session
handshakes. The protocol uses fresh nonces and HMAC proofs, does not send the
configured password directly, and associates every virtual carrier in a
session with the same authenticated username.

This mechanism is an admission control, not a key exchange or encrypted
transport. After the handshake, Engarde does not encrypt or
cryptographically integrity-protect `OPEN`, `DATA`, acknowledgement, or other
carrier frames. An on-path party can observe destinations and unencrypted
payloads, and may alter or drop session traffic.

Use a trusted private network, WireGuard/IPsec VPN, TLS tunnel, or equivalent
protected transport between the client and server. Use a distinct, long,
random peer secret for each client identity so that one client can be revoked
without rotating every deployment.

`server.allowedClients` compares a session connection's source IP with the
configured IP/CIDR list. It is useful as an additional ingress control, but it
does not identify individual clients behind NAT and it does not replace
`peerAuth` on an untrusted network. Prefer using both an ingress firewall and
`peerAuth`.

In active-standby mode, each Flow receives a random 128-bit `resumeToken`; the
server also verifies the original peer principal and a monotonic carrier
generation. The token is carried in Session frames and does not encrypt the
transport. Untrusted paths still require protected transport and `peerAuth`.

## Dynamic exit and SSRF

Every Engarde server is a dynamic TCP exit. An admitted client can request any
IPv4, IPv6, or domain-name destination reachable from the server, including
loopback services, private networks, link-local services, cloud metadata
endpoints, and public Internet hosts. A compromised or overly trusted client
can therefore use the server for network scanning, SSRF, data exfiltration, or
abusive outbound traffic.

`server.allowedClients` is a source-IP allowlist for incoming session
connections. `peerAuth` authenticates a session identity. Neither setting is a
destination ACL. Engarde does not provide a destination or port allowlist.

Server startup requires at least one of `allowedClients`, `peerAuth`, or
`allowUnsafeDynamicDestination: true`. This startup check prevents an
accidentally unrestricted carrier listener; it does not restrict where an
admitted client can connect. Keep `allowUnsafeDynamicDestination` for isolated
tests only.

Enforce destination policy outside Engarde:

- Restrict the carrier listener at the server firewall to expected source
  networks and enable `peerAuth`.
- Apply default-deny egress rules that allow only required destination networks
  and ports. Explicitly account for loopback, private, link-local, metadata,
  and IPv6 paths.
- Remember that a domain name is resolved by the server and may resolve to an
  internal address. Apply policy to the resolved connection, not only to the
  requested hostname.
- For stronger isolation, run the server in a dedicated host, container, or
  network namespace whose routing and firewall policy cannot reach sensitive
  control-plane services.

The server-to-destination hop is ordinary TCP. End-to-end protection depends on
the application protocol, such as TLS in HTTPS or SSH.

## Web Manager

The optional Web Manager uses Go's plain HTTP server. It has no built-in TLS.
Its username and password are also optional: when both are empty, the UI and
API have no authentication. When configured, HTTP Basic authentication sends
the credentials on every request without transport encryption.

The status API exposes operational details such as interface and relay
addresses, active destinations, stream state, and traffic counters. On a
client, the API also has state-changing endpoints that include, exclude,
toggle, or reset interface overrides. These handlers do not enforce HTTP
methods, and the reset endpoint currently changes state on a `GET` request.
There is no separate CSRF protection. Treat access to the Web Manager as
administrative access.

Bind `webManager.listenAddr` to loopback. For remote administration, use SSH
port forwarding, a trusted VPN, or an authenticated TLS reverse proxy, and
firewall the backend so it cannot be reached directly. Configure Web Manager
Basic authentication as defense in depth even when a protected transport is
used. Do not publish the management listener directly on the Internet.

## Resource exhaustion

In `redundant` mode, the following original admission limits default to zero,
which means unlimited:

| Setting | Scope | Effect |
| --- | --- | --- |
| `transfer.tcp.maxStreams` | client and server | Limits concurrent logical streams. |
| `transfer.tcp.maxCarriersPerStream` | server | Limits carriers attached to a redundant stream. The client attempts one carrier on every eligible interface. |
| `transfer.tcp.maxPendingConnections` | server | Limits concurrent physical connection handshakes. |
| `transfer.tcp.maxPendingStreams` | server | Limits virtual streams still processing OPEN and destination setup. |
| `transfer.tcp.maxSessions` | server | Limits established physical multiplexed sessions. |

These are coarse process-wide limits, not per-user or per-source quotas. An
admitted client can consume the shared allowance. In production, set explicit
nonzero values based on expected concurrency and path count. To bound all
server-side connection state, set all five limits. Also size file
descriptor limits, monitor connection and memory usage, and use firewall or
upstream rate limits where appropriate.

`carrierQueueBytes` and `reorderWindowBytes` bound data held by an individual
carrier or stream, and the dial/open/write timeouts bound stalled work. They do
not replace admission limits: memory and socket use still multiply with the
number of streams and carriers.

Active-standby mode does not permit unlimited recovery state and applies these
finite defaults:

| Setting | Default | Effect |
| --- | ---: | --- |
| `transfer.tcp.maxStreams` | `2048` | Total logical Flow limit on both roles. |
| `transfer.tcp.maxConcurrentResumes` | `64` | Concurrent client recovery/migration operations. |
| `transfer.tcp.maxPendingResumes` | `128` | Client recovery queue and server RESUME admission limit. |
| `transfer.tcp.maxRecoveringStreams` | `1024` | Recovering Flows whose endpoints remain retained on each role. |
| `transfer.tcp.maxRecoveryBytes` | `536870912` | Aggregate recovery history on each role. |

At an aggregate recovery limit, Engarde stops accepting new SOCKS5 Flows and
deterministically terminates excess recovering Flows, newest first. This bounds
Engarde replay history but does not replace process memory, file descriptor,
or upstream connection limits.

## Credentials and host controls

Configuration files contain credentials in plaintext. Treat them as secrets:

- Do not commit real credentials, bake them into images, or copy them into
  issue reports and logs.
- Restrict configuration ownership and permissions to the service account.
- Use different secrets for `socks5Auth`, `peerAuth`, and the Web Manager. Do
  not reuse example values.
- Prefer one `peerAuth` username and random secret per client. Rotate and revoke
  credentials when a host or configuration may have been exposed.
- Carry RFC 1929 and HTTP Basic credentials only over loopback or an encrypted
  transport.

Run Engarde with the least OS privileges needed. On Linux, interface-bound
client carriers may require `CAP_NET_RAW`; grant that capability specifically
instead of running the entire service as root. Limit inbound access to the
SOCKS5, carrier, and management ports independently, and apply the server
egress policy described above.

## Production checklist

- Keep the SOCKS5 frontend on loopback, or protect it with authentication,
  encryption, and firewall rules.
- Enable `peerAuth` with a unique long random secret per client, and protect
  carrier traffic with a VPN or TLS tunnel.
- Restrict carrier ingress and server egress at the firewall; do not treat
  `allowedClients` as a destination policy.
- Keep the Web Manager on loopback, configure its Basic authentication, and use
  TLS/VPN/SSH for remote access.
- In redundant mode, set nonzero stream, carrier, and pending-connection
  limits. In active-standby mode, calibrate the finite recovery defaults and
  monitor actual resource use.
- Protect configuration files, separate credentials by purpose, and plan for
  rotation and revocation.
