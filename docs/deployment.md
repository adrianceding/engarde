# Deployment

[简体中文](deployment.zh-CN.md)

This guide covers reproducible deployments from a GitHub Release, the
published container image, or a source checkout. Run one Engarde role per
host. Prepare the server configuration before starting the server, then point
the client configuration at that server.

Configuration files contain credentials. Keep them out of version control,
make them readable only by the service identity, and replace every example
address and secret before startup.

## Install a Release Binary

Release artifacts are published at
[GitHub Releases](https://github.com/adrianceding/engarde/releases). A release
contains the following binaries plus `SHA256SUMS.txt`:

| Platform | Architectures | Artifact name |
| --- | --- | --- |
| Linux | i386, amd64, arm, arm64 | `engarde-linux-<arch>` |
| Windows | i386, amd64, arm64 | `engarde-windows-<arch>.exe` |
| macOS | amd64, arm64 | `engarde-darwin-<arch>` |

For example, a 64-bit Intel or AMD Linux host uses
`engarde-linux-amd64`, while an Apple silicon Mac uses
`engarde-darwin-arm64`. The uploaded release artifacts do not include example
configuration or systemd files; get those from the source tree for the same
tag.

Download an exact version and verify it before installation. Replace
`v1.2.3` with the release being deployed:

```sh
VERSION=v1.2.3
ARTIFACT=engarde-linux-amd64
BASE_URL="https://github.com/adrianceding/engarde/releases/download/${VERSION}"

curl -fLO "${BASE_URL}/${ARTIFACT}"
curl -fLO "${BASE_URL}/SHA256SUMS.txt"
grep "  ${ARTIFACT}$" SHA256SUMS.txt | sha256sum --check -
chmod 0755 "${ARTIFACT}"
./"${ARTIFACT}" -v
```

On macOS, use `shasum -a 256 --check` in place of `sha256sum --check`.
On Windows, compare `Get-FileHash -Algorithm SHA256` with the corresponding
line in `SHA256SUMS.txt`.

To build from a checkout instead, install Go 1.25+, Node.js 22, npm, Git, and
Make. A full cross-build writes binaries to `dist/{os}/{arch}/`:

```sh
make
```

Use a platform target such as `make linux-amd64` when only one binary is
needed. `make docker DOCKER_IMAGE=engarde:local DOCKER_VERSION=local` builds a
local container image for the current platform.

## Docker Compose

The supplied `compose.yml` is intended for Linux Docker Engine and assumes one
role per host. Both roles use host networking, so the listener addresses and
ports in the YAML bind directly on the host. Do not add Compose port mappings;
change the YAML and host firewall instead.

Pin an exact image tag in `.env` so upgrades and rollbacks are reproducible.
Release tag `v1.2.3` publishes image tag `1.2.3`:

```dotenv
ENGARDE_IMAGE=ghcr.io/adrianceding/engarde:1.2.3
```

Avoid `latest` in a deployment. Exact version tags prevent an unrelated image
pull from changing the running version.

### Server host

Create and edit the configuration before starting the container:

```sh
umask 027
cp examples/config/tcp-socks5-server.yml server.yml
${EDITOR:-vi} server.yml
sudo chown "$(id -u):65532" server.yml
chmod 0640 server.yml
```

Add the server config path to `.env` when it is not `./server.yml`:

```dotenv
ENGARDE_SERVER_CONFIG=./server.yml
```

Then start the default server service:

```sh
docker compose pull server
docker compose up -d server
docker compose ps
docker compose logs --tail=100 server
```

### Client host

Prepare the client configuration and set its path when necessary:

```sh
umask 027
cp examples/config/tcp-socks5-client.yml client.yml
${EDITOR:-vi} client.yml
sudo chown "$(id -u):65532" client.yml
chmod 0640 client.yml
```

```dotenv
ENGARDE_CLIENT_CONFIG=./client.yml
```

Targeting the `client` service activates its Compose profile:

```sh
docker compose pull client
docker compose up -d client
docker compose ps
docker compose logs --tail=100 client
```

The bind-mounted source file must already exist; Compose will not create a
missing config path. The image runs as numeric user and group `65532:65532`.
The ownership commands above keep the host user as owner and grant only that
container group read access. User-namespace or rootless Docker configurations
can remap IDs, so confirm the effective mapping before applying this ownership
pattern there.

The container filesystem and config mount are read-only, all capabilities are
dropped, and `no-new-privileges` is enabled. Client mode adds only `NET_RAW`
because binding session sockets to specific host interfaces can require it;
server mode adds no capability. Keep any management listener on loopback,
enable its authentication, and restrict all listeners with the host firewall.

Common lifecycle commands are:

```sh
docker compose ps
docker compose logs -f --tail=100 server
docker compose restart server
docker compose down
```

Replace `server` with `client` on a client host. Use
`docker compose --profile client down` there so the profiled service is
included.

## systemd

The provided unit expects:

- binary: `/usr/local/bin/engarde`
- configuration: `/etc/engarde/engarde.yml`
- service identity: `engarde:engarde`

Run the following from a source checkout matching the release tag, with the
verified release binary in the current directory. This server example edits
the configuration **before** enabling and starting the service:

```sh
sudo useradd --system --user-group --no-create-home \
  --home-dir /nonexistent --shell /usr/sbin/nologin engarde
sudo install -m 0755 engarde-linux-amd64 /usr/local/bin/engarde
sudo install -d -m 0750 -o root -g engarde /etc/engarde
sudo install -m 0640 -o root -g engarde \
  examples/config/tcp-socks5-server.yml /etc/engarde/engarde.yml
sudo install -m 0644 contrib/systemd/engarde.service \
  /etc/systemd/system/engarde.service

sudoedit /etc/engarde/engarde.yml

sudo systemctl daemon-reload
sudo systemctl enable --now engarde
sudo systemctl status --no-pager engarde
sudo journalctl -u engarde -n 100 --no-pager
```

If the `engarde` account already exists, skip `useradd`. For client mode,
install `examples/config/tcp-socks5-client.yml` instead and finish editing it
before startup.

The base unit grants no capabilities. Linux client mode binds TCP session
sockets to selected interfaces with `SO_BINDTODEVICE`; install the supplied
drop-in when that operation requires permission. For a new installation, run
these commands after installing the base unit and before its first
`daemon-reload` and `enable --now`:

```sh
sudo install -d -m 0755 \
  /etc/systemd/system/engarde.service.d
sudo install -m 0644 \
  contrib/systemd/engarde.service.d/bind-device.conf \
  /etc/systemd/system/engarde.service.d/bind-device.conf
sudo systemctl daemon-reload
sudo systemctl enable --now engarde
```

To add the drop-in to an already running client:

```sh
sudo install -d -m 0755 \
  /etc/systemd/system/engarde.service.d
sudo install -m 0644 \
  contrib/systemd/engarde.service.d/bind-device.conf \
  /etc/systemd/system/engarde.service.d/bind-device.conf
sudo systemctl daemon-reload
sudo systemctl restart engarde
sudo systemctl status --no-pager engarde
```

Do not install this drop-in on a server. It adds only `CAP_NET_RAW`; the base
unit also retains `NoNewPrivileges`, a private temporary directory, and strict
filesystem and home-directory protection.

## Upgrade and Rollback

Test a new version with representative traffic before broad rollout. Preserve
the exact previous image or binary and a protected copy of the configuration;
strict config parsing can expose incompatible or misspelled fields at startup.
Restarting either role closes active streams, and wire-protocol compatibility is
not negotiated between different releases. Plan a maintenance window, validate
the client/server version pair before rollout, then upgrade one host at a time
and inspect service health and logs after each change.

For Compose, change `ENGARDE_IMAGE` to a new exact tag, then recreate the role:

```sh
docker compose pull server
docker compose up -d server
docker compose ps
docker compose logs --tail=100 server
```

Rollback is the same operation after restoring the previous exact tag in
`.env`. Replace `server` with `client` for a client deployment. Do not remove
the previous image until the new deployment has been accepted.

For systemd, keep one protected rollback copy before replacing the binary or
changing the configuration:

```sh
sudo install -m 0755 /usr/local/bin/engarde \
  /usr/local/bin/engarde.rollback
sudo cp -p /etc/engarde/engarde.yml \
  /etc/engarde/engarde.yml.rollback
sudo install -m 0755 ./engarde-linux-amd64 \
  /usr/local/bin/engarde.new
sudo mv -f /usr/local/bin/engarde.new /usr/local/bin/engarde
sudo systemctl restart engarde
sudo systemctl is-active engarde
sudo journalctl -u engarde -n 100 --no-pager
```

If health checks fail, restore the previous binary and, only if it changed,
the previous configuration:

```sh
sudo install -m 0755 /usr/local/bin/engarde.rollback \
  /usr/local/bin/engarde.new
sudo mv -f /usr/local/bin/engarde.new /usr/local/bin/engarde
sudo install -m 0640 -o root -g engarde \
  /etc/engarde/engarde.yml.rollback /etc/engarde/engarde.yml
sudo systemctl restart engarde
sudo systemctl status --no-pager engarde
```

The rollback configuration contains credentials; retain its `root:engarde`
ownership and `0640` mode, and remove it securely after the rollout window.
