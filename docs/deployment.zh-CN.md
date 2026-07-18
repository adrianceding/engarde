# 部署指南

[English](deployment.md)

本文介绍如何从 GitHub Release、已发布的容器镜像或源码检出进行可复现部署。
每台主机只运行一个 Engarde 角色。先准备服务端配置并启动服务端，再让客户端配置
指向该服务端。

配置文件包含凭据。不要将其提交到版本控制；只允许服务身份读取，并且必须在启动前
替换所有示例地址和密钥。

## 安装 Release 二进制文件

Release 产物发布在
[GitHub Releases](https://github.com/adrianceding/engarde/releases)。每个 Release
包含下列二进制文件以及 `SHA256SUMS.txt`：

| 平台 | 架构 | 产物名称 |
| --- | --- | --- |
| Linux | i386、amd64、arm、arm64 | `engarde-linux-<arch>` |
| Windows | i386、amd64、arm64 | `engarde-windows-<arch>.exe` |
| macOS | amd64、arm64 | `engarde-darwin-<arch>` |

例如，64 位 Intel 或 AMD Linux 主机使用 `engarde-linux-amd64`，Apple 芯片
Mac 使用 `engarde-darwin-arm64`。上传的 Release 产物不包含示例配置或 systemd
文件；请从相同标签的源码树中获取这些文件。

安装前应下载并校验一个明确版本。将 `v1.2.3` 替换为要部署的 Release：

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

在 macOS 上，用 `shasum -a 256 --check` 替代 `sha256sum --check`。
在 Windows 上，将 `Get-FileHash -Algorithm SHA256` 的结果与
`SHA256SUMS.txt` 中对应行比较。

如需从源码构建，请安装 Go 1.25+、Node.js 22、npm、Git 和 Make。完整交叉构建
会将二进制写入 `dist/{os}/{arch}/`：

```sh
make
```

只需要一个二进制时，可使用 `make linux-amd64` 等平台目标。
`make docker DOCKER_IMAGE=engarde:local DOCKER_VERSION=local` 会为当前平台构建
本地容器镜像。

## Docker Compose

仓库中的 `compose.yml` 面向 Linux Docker Engine，并假设每台主机只部署一个
角色。两个角色都使用 host network，因此 YAML 中配置的监听地址和端口会直接绑定
到主机。不要添加 Compose 端口映射；应修改 YAML 和主机防火墙。

在 `.env` 中固定准确的镜像标签，使升级和回滚可以复现。Release 标签
`v1.2.3` 会发布镜像标签 `1.2.3`：

```dotenv
ENGARDE_IMAGE=ghcr.io/adrianceding/engarde:1.2.3
```

部署时不要使用 `latest`。固定版本可避免一次无关的镜像拉取意外改变运行版本。

### 服务端主机

先创建并编辑配置，再启动容器：

```sh
umask 027
cp examples/config/tcp-socks5-server.yml server.yml
${EDITOR:-vi} server.yml
sudo chown "$(id -u):65532" server.yml
chmod 0640 server.yml
```

如果服务端配置不在 `./server.yml`，请在 `.env` 中设置其路径：

```dotenv
ENGARDE_SERVER_CONFIG=./server.yml
```

然后启动默认的服务端服务：

```sh
docker compose pull server
docker compose up -d server
docker compose ps
docker compose logs --tail=100 server
```

### 客户端主机

准备客户端配置，并在需要时设置其路径：

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

直接指定 `client` 服务会启用它的 Compose profile：

```sh
docker compose pull client
docker compose up -d client
docker compose ps
docker compose logs --tail=100 client
```

作为 bind mount 来源的配置文件必须已经存在；Compose 不会创建缺失的配置路径。
镜像以数字用户和组 `65532:65532` 运行。上述所有权命令会保留当前主机用户为
所有者，并且只给容器组读取权限。用户命名空间或 rootless Docker 可能重新映射 ID，
因此在这些环境中应用此所有权模式前，应先确认实际映射。

容器文件系统和配置挂载均为只读，所有 capabilities 默认移除，并启用
`no-new-privileges`。客户端模式只添加 `NET_RAW`，因为将 Session socket 绑定到
指定主机接口可能需要该权限；服务端模式不添加 capability。管理监听器应保持在
loopback 上并启用认证，同时通过主机防火墙限制所有监听器。

常用生命周期命令如下：

```sh
docker compose ps
docker compose logs -f --tail=100 server
docker compose restart server
docker compose down
```

在客户端主机上将 `server` 替换为 `client`。执行停止操作时使用
`docker compose --profile client down`，以便包含带 profile 的服务。

## systemd

仓库提供的 unit 采用以下约定：

- 二进制：`/usr/local/bin/engarde`
- 配置：`/etc/engarde/engarde.yml`
- 服务身份：`engarde:engarde`

请在与 Release 标签一致的源码检出目录中执行以下命令，并把校验过的 Release
二进制放在当前目录。此服务端示例会在启用和启动服务**之前**编辑配置：

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

如果 `engarde` 账户已经存在，请跳过 `useradd`。客户端模式应改为安装
`examples/config/tcp-socks5-client.yml`，并在启动前完成编辑。

基础 unit 不授予任何 capability。Linux 客户端模式通过 `SO_BINDTODEVICE` 将
TCP Session socket 绑定到选定接口；此操作需要权限时，应安装仓库提供的 drop-in。
对于全新安装，请在安装基础 unit 后、首次执行 `daemon-reload` 和
`enable --now` 前运行：

```sh
sudo install -d -m 0755 \
  /etc/systemd/system/engarde.service.d
sudo install -m 0644 \
  contrib/systemd/engarde.service.d/bind-device.conf \
  /etc/systemd/system/engarde.service.d/bind-device.conf
sudo systemctl daemon-reload
sudo systemctl enable --now engarde
```

如需为正在运行的客户端添加 drop-in：

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

不要在服务端安装此 drop-in。它只添加 `CAP_NET_RAW`；基础 unit 仍会保留
`NoNewPrivileges`、私有临时目录以及严格的文件系统和主目录保护。

## 升级与回滚

大范围发布前，应使用有代表性的流量测试新版本。保留准确的旧镜像或二进制以及一份
受保护的配置副本；严格配置解析可能在启动时暴露不兼容或拼写错误的字段。重启任一
角色都会关闭活动 stream，不同 release 之间也不会协商 wire protocol 兼容性。因此
应安排维护窗口，先验证 client/server 版本组合，再逐台升级，并在每次变更后检查服务
健康状态和日志。

Compose 部署先将 `ENGARDE_IMAGE` 改为新的准确标签，再重新创建对应角色：

```sh
docker compose pull server
docker compose up -d server
docker compose ps
docker compose logs --tail=100 server
```

回滚时，在 `.env` 中恢复上一个准确标签后执行相同操作。客户端部署应将 `server`
替换为 `client`。新部署验收前，不要删除旧镜像。

systemd 部署在替换二进制或修改配置前，应保留一份受保护的回滚副本：

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

如果健康检查失败，应恢复旧二进制；只有配置发生过变化时才恢复旧配置：

```sh
sudo install -m 0755 /usr/local/bin/engarde.rollback \
  /usr/local/bin/engarde.new
sudo mv -f /usr/local/bin/engarde.new /usr/local/bin/engarde
sudo install -m 0640 -o root -g engarde \
  /etc/engarde/engarde.yml.rollback /etc/engarde/engarde.yml
sudo systemctl restart engarde
sudo systemctl status --no-pager engarde
```

回滚配置包含凭据；应保持其 `root:engarde` 所有权和 `0640` 权限，并在发布窗口
结束后安全删除。
