# Engarde

**通过多条上行链路冗余传输 SOCKS5 TCP 流量。**

[English](README.md) | [版本下载](https://github.com/adrianceding/engarde/releases) | [容器镜像](https://github.com/adrianceding/engarde/pkgs/container/engarde) | [问题反馈](https://github.com/adrianceding/engarde/issues)

Engarde 由客户端（client）和服务端（server）配合运行。应用连接本地 SOCKS5
监听端口；客户端在每个具备 IPv4 地址的可用网络接口上，维持一条到远端服务端的长期
复用 **Session**，每条逻辑 TCP 流则在每个健康 Session 上使用一条虚拟 **carrier**。

```text
                                  +-- eth0 / 运营商 A --+
SOCKS5 应用 -> Engarde 客户端 ----+                     +-> Engarde 服务端 -> 请求目标
                                  +-- wlan0 / 运营商 B -+
```

> [!IMPORTANT]
> Engarde 不是 MPTCP，也不会叠加多条链路的带宽。它在健康 carrier 上冗余发送流
> 数据，让最先到达的副本推动传输。因此它能提高链路故障容忍能力，但会消耗额外流量。

## 适用场景

Engarde 适合“TCP 连续性比带宽利用率更重要”的环境，例如：

- 同时使用多条不稳定 4G、5G、Wi-Fi 或有线链路的移动与远程站点；
- 希望通过稳定服务端获得 SOCKS5 出口的双运营商工作站；
- 单条上行链路故障时，不希望活动 TCP 流立即断开的服务。

它不是 VPN、加密层、目标防火墙或吞吐聚合器。每个被选中的客户端接口都必须具有
合格的 IPv4 地址，并且能路由到 Engarde 服务端。如果不同链路不能共用同一个服务端
地址，可以按接口配置不同的目的地址。

## 主要能力

- Client 启动后在每个合格网卡上建立一条长期复用 Session，并由所有 SOCKS5 流共享。
- 在多条 carrier 之间实现有序交付、去重、累计 ACK 和有界重放窗口。
- 运行时发现网卡、按完整网卡名使用 glob 过滤，并支持按网卡覆盖服务端地址。
- 分别鉴权本地 SOCKS5 调用方和远端 Engarde 节点，并提供服务端来源 allowlist 与
  资源限制。
- 同一个跨平台二进制可运行两种角色，并内嵌状态管理界面和 JSON API。

## 运行条件

- 一台可达的服务端主机和一台客户端主机。
- 从客户端各条路径放行到服务端的 TCP `59501` 端口，本文示例使用该端口。
- 客户端至少有一个非 loopback、非 link-local 的 IPv4 网络接口。
- Linux 客户端需要 `CAP_NET_RAW`，用于把 Session socket 绑定到指定接口。
- 建议客户端与服务端使用同一个 release 的二进制。

Session 连接只使用 IPv4；应用通过 SOCKS5 请求的最终目标仍可使用 IPv4、IPv6 或域名。

## 安装

从 [GitHub Releases](https://github.com/adrianceding/engarde/releases) 为两台主机下载
对应二进制和 `SHA256SUMS.txt`。例如 Linux amd64：

```sh
sha256sum --check SHA256SUMS.txt --ignore-missing
install -m 0755 engarde-linux-amd64 engarde
./engarde -v
```

从源码构建相同目标：

```sh
make linux-amd64
install -m 0755 dist/linux/amd64/engarde ./engarde
```

源码构建需要 Go 1.25+、Node.js 22、npm、Git 和 Make。其他平台、Docker、systemd、
配置权限与升级方式见[部署指南](docs/deployment.zh-CN.md)。

## 快速开始

下面的配置会启用 Engarde 两端的 peer 鉴权、本地 SOCKS5 鉴权，以及仅监听 loopback
的管理界面。启动任何进程前，必须替换所有标记为 `CHANGE_ME` 的值。

### 1. 检查客户端网卡

```sh
./engarde list-interfaces
```

该命令列出系统网卡及其第一个可用 IPv4 地址，不会应用配置中的网卡过滤规则。如果
希望获得路径冗余，请确认至少两个预期网卡都能到达服务端。

### 2. 创建服务端配置

在服务端主机创建 `server.yml`：

```yaml
server:
  description: "Engarde 出口"
  listenAddr: "0.0.0.0:59501"
  peerAuth:
    users:
      edge-a: "CHANGE_ME_LONG_RANDOM_PEER_SECRET"
```

生产部署还应根据主机容量设置资源数量限制；这些限制默认均为不限。然后启动服务端：

```sh
./engarde server.yml
```

### 3. 创建客户端配置

在客户端主机创建 `client.yml`，把 `SERVER_IPV4` 替换为各选中网卡都能到达的
服务端地址：

```yaml
client:
  description: "多上行 SOCKS5 客户端"
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

如果 Engarde 只能使用指定网卡，可设置 `includeInterfaces`。规则匹配完整网卡名，
例如 `eth*`、`enp*`、`wlan*` 或 `wlp*`。

Linux 需要先授予客户端二进制绑定指定网卡所需的 capability，之后仍以普通用户启动：

```sh
sudo setcap cap_net_raw=ep ./engarde
./engarde client.yml
```

Docker 客户端服务已经只添加了所需的 `NET_RAW` capability。

### 4. 验证中继和路径

打开 `http://127.0.0.1:9001/`，使用管理凭据登录。每个可达且合格的网卡都应显示一条
Session。相同状态也可以通过 JSON 获取：

```sh
curl --user engarde:CHANGE_ME_MANAGEMENT_SECRET \
  http://127.0.0.1:9001/api/v1/get-list
```

通过本地 SOCKS5 监听发送请求：

```sh
curl --socks5-hostname 127.0.0.1:59401 \
  --proxy-user client:CHANGE_ME_LOCAL_SOCKS_SECRET \
  https://example.com/
```

只有服务端已连接请求目标且至少一条 carrier 就绪后，请求才会成功。在持续时间较长
的传输中，只要仍有另一条 carrier 存活，单条 carrier 丢失不应中断该流；所有活动
carrier 都消失时，逻辑流会关闭。

## 配置

一个配置文件只能启动一个角色。客户端必须配置 `client.listenAddr` 和
`client.dstAddr`。服务端必须配置 `server.listenAddr`，并至少启用一种准入控制：

- 使用 `server.allowedClients` 过滤来源 IP/CIDR；
- 使用 `server.peerAuth` 鉴权 Engarde 客户端；或
- 仅在隔离测试中设置 `server.allowUnsafeDynamicDestination: true`。

配置采用严格解析：未知或已移除字段会阻止启动，不会被静默忽略。完整说明见
[配置参考](docs/configuration.zh-CN.md)、[客户端示例](examples/config/tcp-socks5-client.yml)、
[服务端示例](examples/config/tcp-socks5-server.yml)和带完整注释的
[`engarde.yml.sample`](engarde.yml.sample)。

## 安全边界

- RFC 1929 SOCKS5 凭据会在本地 TCP 连接中明文传输。
- Peer 鉴权只控制 Session 准入，不加密或完整性保护后续 OPEN/DATA frame。
- 已通过准入的客户端可以请求服务端能访问的 loopback、内网、元数据服务和公网目标；
  Engarde 不提供目标 ACL。
- 管理端口使用明文 HTTP。应保持 loopback 监听；远程访问必须通过 TLS 反向代理或 VPN。
- stream、carrier、Session 和 pending connection 的数量限制默认均为不限。

把任一角色暴露到隔离网络以外之前，请阅读[安全模型与部署检查表](docs/security.zh-CN.md)。

## 文档

- [配置参考](docs/configuration.zh-CN.md)
- [部署指南](docs/deployment.zh-CN.md)
- [安全模型与检查表](docs/security.zh-CN.md)
- [带完整注释的配置模板](engarde.yml.sample)
- [仓库与贡献指南](AGENTS.md)

## Docker

仓库中的 Compose 文件按“一台主机运行一个角色”设计，并使用 host network。生产环境
应固定 release 镜像标签，不要依赖 `latest`：

```sh
export ENGARDE_IMAGE=ghcr.io/adrianceding/engarde:0.6.0
cp examples/config/tcp-socks5-server.yml server.yml
${EDITOR:-vi} server.yml
sudo chown "$(id -u):65532" server.yml
chmod 0640 server.yml
docker compose up -d
```

客户端主机复制并编辑客户端示例，然后运行：

```sh
export ENGARDE_IMAGE=ghcr.io/adrianceding/engarde:0.6.0
cp examples/config/tcp-socks5-client.yml client.yml
${EDITOR:-vi} client.yml
sudo chown "$(id -u):65532" client.yml
chmod 0640 client.yml
docker compose up -d client
```

配置中的监听端口会直接绑定到 Linux 主机，并不是 Compose 端口映射。上述所有权命令
假设使用常规 rootful Docker；rootless 或用户命名空间 Docker，以及生产部署方式见
[部署指南](docs/deployment.zh-CN.md)。

## 限制

- 只支持 SOCKS5 TCP `CONNECT`；不支持 raw TCP 前端、UDP 中继、`BIND` 或
  `UDP ASSOCIATE`。
- 客户端 carrier 路径只支持 IPv4。
- 提供冗余而不是带宽聚合。
- 所有活动 carrier 丢失后，不能自动恢复原有逻辑流。
- 不提供加密、目标 ACL、配置热加载或管理服务内建 TLS。

## 构建与测试

```sh
make test
make test-production
make test-fuzz FUZZ_ITERATIONS=1000
make test-stress STRESS_RUNS=10
make test-soak SOAK_DURATION=10m
make
```

`make test-production` 是可重复的正确性门禁，只执行 Go 测试套件、vet 和 race。
fuzz 使用固定迭代预算；随机 shuffle 压力测试与按时间运行的 soak 是显式诊断工具，
不属于发布门禁。soak 可用于发现泄漏或定时器问题，但不能证明协议逻辑正确。常规 Go
测试仍会把已提交的 fuzz corpus 作为回归用例执行。`make` 构建内嵌 Angular UI，并把
全部 release 目标输出到 `dist/{os}/{arch}/`。前端测试需要在 `webmanager/` 中单独运行
`npm test`。

## 项目状态与许可证

本项目是 [porech/engarde](https://github.com/porech/engarde) 的 pre-1.0、AI 辅助重写与
整合版本。自动化 release gate 可以降低协议、并发、资源清理和平台构建回归风险，但
生产部署仍必须在目标多网卡网络中完成实际验证。

Engarde 继续采用 GPLv2，详见 [`LICENSE.txt`](LICENSE.txt)。
