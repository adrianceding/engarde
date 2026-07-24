# 配置参考

[English](configuration.md)

Engarde 从一个 YAML 文件中读取配置，并启动一个角色。可以显式传入配置路径：

```sh
./engarde /path/to/engarde.yml
```

不传路径时，Engarde 会读取当前目录下的 `engarde.yml`。

## 角色选择与严格解析

顶层键必须是 `client` 或 `server`：

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

Client 必须同时配置 `client.listenAddr` 和 `client.dstAddr`。Server 必须配置
`server.listenAddr`，并通过 [Server 字段](#server-字段)中说明的接入安全检查。不要在
同一文件中混合 client 和 server 设置；当一个角色配置完整、另一个角色也存在设置时，
Engarde 会拒绝启动。

配置采用严格解析：

- 键名区分大小写，并使用本文展示的 camelCase 拼写。
- 未知键会导致启动失败，不会被静默忽略。
- 已移除的键也会被拒绝，例如 `frontend`、`transfer.mode`、
  `transfer.protocol`、`udpBatch` 和 server 侧的 `dstAddr`。
- Engarde 确定文件中唯一的角色后，才会应用该角色的默认值。

这样可以防止拼写错误悄悄关闭鉴权、网卡规则或资源限制。

## 地址规则

地址字段使用 Go 的 `host:port` 格式。IPv6 字面地址需要方括号，例如
`[::1]:59401`。

需要特别注意以下边界：

1. 除非将 `client.allowUnsafeFrontend` 设为 `true`，
   `client.listenAddr` 中的 host 文本必须是 `localhost`（不区分大小写），或
   `127.0.0.1`、`::1` 这类 loopback IP 字面值。安全检查不会解析其他主机名，
   因此即使自定义主机名能解析到 loopback，也仍会被拒绝。
2. Client 到 server 的 Session 连接只使用 IPv4。因此，`client.dstAddr` 和每个
   `client.dstOverrides[].dstAddr` 必须使用 IPv4 字面地址，或具有可达 IPv4 地址的
   主机名。每个可用 client 网卡也必须具有非 loopback、非 link-local 的 IPv4
   地址。这不会限制 SOCKS5 请求的目标；server 仍可连接 IPv4、IPv6 或域名目标。

监听和目标地址的最终语法检查发生在打开对应 socket 时。
`./engarde list-interfaces` 会列出原始系统网卡，以及每个网卡上找到的第一个合格
IPv4 地址。该命令不读取配置，也不应用 `includeInterfaces` 和
`excludedInterfaces`；没有合格地址的网卡仍会显示，但地址为空。

## Client 字段

| 字段 | 必填 | 默认值 | 含义与约束 |
| --- | --- | --- | --- |
| `description` | 否 | 空 | 实例说明，显示在日志和管理状态中。 |
| `listenAddr` | 是 | 无 | 本地 SOCKS5 TCP 监听地址，格式为 `host:port`；受上述 loopback 字面规则约束。 |
| `dstAddr` | 是 | 无 | 复用 Session 默认连接的 Engarde server 地址，必须可通过 IPv4 到达。 |
| `socks5Auth` | 否 | 不配置 | 本地 RFC 1929 用户名/密码鉴权。参见[凭据](#凭据)。 |
| `peerAuth` | 否 | 不配置 | Client 连接 Engarde server 时使用的鉴权。参见[凭据](#凭据)。 |
| `allowUnsafeFrontend` | 否 | `false` | 允许 `listenAddr` 使用非 loopback host。只有在具备独立网络访问控制时才应暴露 SOCKS5 监听。 |
| `includeInterfaces` | 否 | `[]` | 按完整网卡名匹配的 glob 允许列表。空列表允许所有其他条件合格的网卡。 |
| `excludedInterfaces` | 否 | `[]` | 按完整网卡名匹配的 glob 拒绝列表；排除规则优先于包含规则。 |
| `interfaceLabels` | 否 | `{}` | 精确网卡名到显示标签的映射，用于管理界面。 |
| `pathSelection` | 否 | active-standby 时为 `adaptive` | 路径选择策略；当前唯一有效值是 `adaptive`。 |
| `interfaceHints` | 否 | `{}` | 精确网卡名到可选资费提示的映射；仅在 active-standby 路径评分中使用。 |
| `dstOverrides` | 否 | `[]` | 按网卡覆盖 Session 目标；每项包含精确的 `ifName` 和可通过 IPv4 到达的 `dstAddr`。 |
| `transfer` | 否 | 参见[传输字段](#传输字段) | 传输调优与资源限制。 |
| `webManager` | 否 | 禁用 | 内嵌管理 HTTP 监听。参见 [Web Manager 字段](#web-manager-字段)。 |

### 凭据

`client.socks5Auth` 和 `client.peerAuth` 使用相同的 YAML 结构：

```yaml
username: "edge-a"
password: "replace-with-a-long-random-secret"
```

配置 `socks5Auth` 后，两个值都必须为 1 至 255 字节，并且 SOCKS5 无鉴权方式会被
拒绝。RFC 1929 不会加密这些凭据，因此除非连接由其他层保护，否则应将 frontend
保留在 loopback 上。

配置 `peerAuth` 后，用户名必须为 1 至 255 字节，密码必须为 1 至 1024 字节。
Server 的 `server.peerAuth.users` 下必须存在相同的用户名和密码。长度按 UTF-8 编码
后的字节数计算，而不是字符数。

### 网卡选择与目标覆盖

网卡模式使用 Go `path.Match` 语法，并匹配完整网卡名。空模式或语法错误的模式会导致
配置校验失败。例如：

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

网卡必须通过这些筛选规则，并且 Engarde 能从中选出非 loopback、非 link-local 的
IPv4 地址，才可用于 Session。管理界面的运行时操作可以临时反转某个精确网卡名的
配置状态；重置 override 后会恢复 YAML 规则。

`interfaceHints.<网卡名>.cost` 必须为 `normal`、`metered` 或 `avoid`。该提示只表达系统
无法自动判断的资费偏好，不是固定优先级：Engarde 仍会结合 Session RTT、抖动、失败
惩罚和当前负载自适应选择。通常可完全省略 `pathSelection` 和 `interfaceHints`。

## Server 字段

| 字段 | 必填 | 默认值 | 含义与约束 |
| --- | --- | --- | --- |
| `description` | 否 | 空 | 实例说明，显示在日志和管理状态中。 |
| `listenAddr` | 是 | 无 | Engarde 复用 Session 的 TCP 监听地址，格式为 `host:port`。 |
| `allowedClients` | 否 | `[]` | Session 来源允许列表；每项必须是 IP 地址或 CIDR。 |
| `peerAuth` | 否 | 不配置 | 已鉴权 Engarde client 身份的映射。参见 [Server 接入控制](#server-接入控制)。 |
| `allowUnsafeDynamicDestination` | 否 | `false` | 显式允许在未配置 `allowedClients` 或 `peerAuth` 时启动，仅用于隔离测试环境。 |
| `transfer` | 否 | 参见[传输字段](#传输字段) | 传输调优与资源限制。 |
| `webManager` | 否 | 禁用 | 内嵌管理 HTTP 监听。参见 [Web Manager 字段](#web-manager-字段)。 |

### Server 接入控制

每个 server 都是动态 TCP 出口：SOCKS5 请求会提供由 server 拨号的目标。因此，启动
时必须至少满足以下一项：

- `allowedClients` 列表非空；
- 配置 `peerAuth`；
- 使用 `allowUnsafeDynamicDestination: true` 显式绕过安全检查。

`allowedClients` 匹配每条 Session 连接的来源 IP。条目可以是单个 IPv4/IPv6 地址或
CIDR；条目前后的空白会被忽略，空条目或无效条目会被拒绝。当前 Session 传输使用
IPv4，因此只包含 IPv6 的条目无法匹配当前 client Session。它不是目标 ACL。

`server.peerAuth.users` 必须至少包含一个条目：

```yaml
peerAuth:
  users:
    edge-a: "replace-with-a-long-random-secret"
    edge-b: "replace-with-another-long-random-secret"
```

每个用户名必须为 1 至 255 字节，每个密码必须为 1 至 1024 字节。同时配置
`allowedClients` 和 `peerAuth` 时，carrier 必须通过两项检查。
`allowUnsafeDynamicDestination` 只用于满足启动安全检查，不会创建目标策略。

## 传输字段

两个角色都接受以下 `transfer` 结构：

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

对于具有正数默认值的字段，不配置和显式填写 `0` 都会采用默认值；负数无效。
`redundant` 模式下，原有五个资源 `max*` 字段的 `0` 仍表示不限。active-standby 专用
字段只在该模式下生效；启用后其中的 `0` 会选择下表中的有限默认值，不能表示不限。

| 字段 | 生效默认值 | 有效生效值 | 生效端 | 含义 |
| --- | ---: | ---: | --- | --- |
| `tcp.carrierMode` | `redundant` | `redundant` 或 `active-standby` | 两端 | `redundant` 在每条健康路径重复发送；`active-standby` 每个 Flow 只使用一个 active carrier，并保留热备 Session。 |
| `keepaliveIntervalMillis` | `1000` | `> 0` | 两端 | 复用 Session 的 keepalive 探测间隔。 |
| `keepaliveTimeoutMillis` | `5000` | 大于同一文件中的 interval | 两端 | 无 keepalive 响应时关闭复用 Session 的等待上限。 |
| `tcp.chunkSize` | `16384` | `1..65536` | 两端 | 一个 DATA frame 中承载的最大应用数据。 |
| `tcp.carrierQueueBytes` | `1048576` | `1..2147483647` | 两端 | 每条 carrier 可排队的最大出站应用数据量，同时作为 smux 单 stream 接收缓冲上限。 |
| `tcp.reorderWindowBytes` | `4194304` | `1..2147483647` | 两端 | 乱序接收数据与未确认重放历史的边界，同时作为 smux Session 接收缓冲上限。 |
| `tcp.dialTimeoutMillis` | `5000` | `> 0` | 两端 | Client 上用于连接 server 的 Session 拨号超时；server 上用于目标拨号超时。 |
| `tcp.openTimeoutMillis` | `5000` | `> 0` | 两端 | SOCKS5 协商、Session 握手和虚拟 stream OPEN 建立的时间边界。 |
| `tcp.writeTimeoutMillis` | `10000` | `> 0` | 两端 | Carrier 或 endpoint 写入无进展时的 deadline。 |
| `tcp.clientRecoveryTimeoutMillis` | active-standby: `3000` | `> 0` | Client | 最后一条 active carrier 失效后保留 App TCP 并尝试 RESUME 的总预算。 |
| `tcp.serverOrphanRetentionMillis` | active-standby: `9000` | 见下方关系 | Server | carrier 为空时保留原 target TCP 和 Flow 状态的预算。 |
| `tcp.resumeOpenTimeoutMillis` | active-standby: `750` | `> 0` 且小于 client recovery | Client | 单次 RESUME stream 建立与结果等待上限。 |
| `tcp.maxStreams` | redundant: 不限；active-standby: `2048` | `>= 0`；active 时 `> 0` | 两端 | 最大并发逻辑 TCP stream 数。 |
| `tcp.maxCarriersPerStream` | 不限 | `>= 0` | Server | redundant Flow 每个 stream 可接受的最大 carrier 数；active-standby Flow 始终只有一个。 |
| `tcp.maxPendingConnections` | 不限 | `>= 0` | Server | 尚未成为复用 Session 的物理连接最大并发握手数。 |
| `tcp.maxPendingStreams` | 不限 | `>= 0` | Server | 仍在处理 OPEN 和目标连接建立的虚拟 stream 最大并发数。 |
| `tcp.maxSessions` | 不限 | `>= 0` | Server | 已建立的物理复用 Session 最大数量。 |
| `tcp.maxConcurrentResumes` | active-standby: `64` | active 时 `> 0` | Client | 同时执行的 recoverable OPEN、RESUME 或主动迁移数量。 |
| `tcp.maxPendingResumes` | active-standby: `128` | active 时 `> 0` | 两端 | Client 恢复/迁移队列和 server 并发 RESUME 准入上限。 |
| `tcp.maxRecoveringStreams` | active-standby: `1024` | active 时 `1..maxStreams` | 两端 | 可同时保留 endpoint 的 recovering Flow 上限。 |
| `tcp.maxRecoveryBytes` | active-standby: `536870912` | active 时至少为 `reorderWindowBytes` | 两端 | 所有 recovering Flow 未确认重放历史的聚合上限。 |

标记为仅 server 生效的设置如果出现在 client 文件中，仍会被解析和校验，但不会施加
client 侧限制。

### Active-standby 模式

Client 与 server 必须同时配置：

```yaml
transfer:
  tcp:
    carrierMode: active-standby
```

不支持该能力或模式不一致的 peer 会使 Session 明确失败，不会静默降级到重复发送。每条
合格网卡仍维持一条已鉴权、已探测的物理 Session，但每个逻辑 Flow 健康时只有一条
active carrier。Session 级探测提供 RTT/抖动样本；路径评分还考虑衰减的失败惩罚、
当前 active Flow 数和 smux stream 压力，完全相同时按网卡名确定性选择。
承载 active Flow 的 Session 每 `250ms` 发起一次探测，空闲热备 Session 每 `1s` 探测一次，
单次响应上限为 `400ms`。一次失败只进入宽限期并保留最近成功状态；连续两次失败会把
整个物理 Session 判为不可用，使其上的 Flow 进入高优先级恢复队列并通过健康热备 Session
执行 RESUME。任意一次成功探测都会清零连续失败计数。

既有 Flow 只能在返回相同 `serverInstanceID` 的 Session 间 RESUME。不同 `dstOverrides`
可以指向同一 server 进程的不同地址，但不能指向互不共享内存状态的多个 server 实例；
server 重启后旧 target TCP 也无法恢复。成功切换会保留原 App TCP 和原 target TCP；
所有路径持续不可用并超过 recovery budget 后，Flow 才会以终端错误关闭。
Client 会过滤握手中声明的 retention 短于自身 recovery budget 的 Session；server 也会在
拨号 target 前拒绝请求预算超过自身 retention 的 recoverable OPEN。

配置必须满足：

```text
resumeOpenTimeoutMillis < clientRecoveryTimeoutMillis
serverOrphanRetentionMillis >= clientRecoveryTimeoutMillis + resumeOpenTimeoutMillis
maxRecoveringStreams <= maxStreams
maxRecoveryBytes >= reorderWindowBytes
```

恢复过载时优先保护已建立 Flow，并按“最晚建立的 Flow 先失败”确定性释放超额状态。
回到 `carrierMode: redundant` 需要重启进程，现有 Flow 会随重启中断。

### 两端之间的 keepalive 设置

Keepalive 只在各自配置文件内校验，不会在两台主机之间协商或互相比较。两端均按自身的
`keepaliveIntervalMillis` 探测复用 Session；如果在 `keepaliveTimeoutMillis` 内没有
收到响应，就关闭该 Session。

每个文件自身还必须满足
`keepaliveTimeoutMillis > keepaliveIntervalMillis`。两端都采用默认值（1 秒 / 5 秒）
时会同时满足两项规则。过小的值可能使健康的 carrier Session 在短暂延迟或 CPU
负载下反复断开重建。

## Web Manager 字段

`client.webManager` 和 `server.webManager` 都接受：

| 字段 | 必填 | 默认值 | 含义与约束 |
| --- | --- | --- | --- |
| `listenAddr` | 启用时必填 | 空 / 禁用 | 管理界面与 JSON API 的 HTTP 监听地址，格式为 `host:port`。 |
| `username` | 否 | 空 | HTTP Basic Auth 用户名；必须与 `password` 同时配置。 |
| `password` | 否 | 空 | HTTP Basic Auth 密码；必须与 `username` 同时配置。 |

在没有 `listenAddr` 时配置凭据是无效的。配置监听但同时省略两项凭据是允许的，此时
管理面没有鉴权。SOCKS5 frontend 的 loopback 校验不适用于这个监听，并且管理服务
不提供 TLS。应优先绑定 loopback、同时配置两项凭据；需要远程访问时，应使用受保护
的隧道或反向代理。

```yaml
webManager:
  listenAddr: "127.0.0.1:9001"
  username: "engarde"
  password: "replace-with-a-management-secret"
```

## 完整示例

- [带完整注释的模板](../engarde.yml.sample)
- [Client 示例](../examples/config/tcp-socks5-client.yml)
- [Server 示例](../examples/config/tcp-socks5-server.yml)

示例使用文档专用地址和占位密钥。运行 Engarde 前必须替换这些值，并避免将真实凭据
提交到版本控制。
