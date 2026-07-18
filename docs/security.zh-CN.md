# 安全边界

[English](security.md)

Engarde 是一个冗余 TCP 中继，而不是安全隧道。它在多个边界提供准入控制，但不会为
SOCKS5 前端、Session 连接、管理监听器或服务器到目标地址的连接增加加密。需要机密性
和完整性时，应使用应用层加密，并将相应连接置于受保护的网络或隧道中。

```text
应用 -- SOCKS5 --> Engarde 客户端 == TCP/smux Sessions ==> Engarde 服务器 -- TCP --> 目标地址
管理员 ------------------ HTTP ----------------> 客户端或服务器上的 Web Manager
```

Engarde 服务器属于受信任组件：它会收到请求的目标地址、建立出口连接，并能看到所有
未经过端到端加密的应用数据。

## SOCKS5 前端

客户端通过 `client.listenAddr` 接受 SOCKS5 TCP `CONNECT` 请求。

- 未配置 `client.socks5Auth` 时，前端接受 SOCKS5 无认证方式。
- 配置 `client.socks5Auth` 后，客户端要求使用 RFC 1929 用户名/密码认证。RFC 1929
  会在 SOCKS5 TCP 连接上明文发送用户名和密码，也不会加密被代理的应用流量。
- 默认情况下，配置校验要求监听 loopback 地址。设置
  `client.allowUnsafeFrontend: true` 只会绕过这项保护，不会增加认证、加密、速率限制
  或防火墙规则。

应尽量将前端限制在 `127.0.0.1`、`[::1]` 或 `localhost`。如果远程应用必须访问它，
请将监听器放在经过认证的 VPN 或加密隧道后，并通过主机及网络防火墙限制来源。不要
把未认证的 SOCKS5 监听器暴露给不受信任的网络。

`socks5Auth` 只控制谁能使用本地前端，不会向 Engarde 服务器认证 Engarde 客户端；
后一个边界需要单独配置 `peerAuth`。

## Session 认证与传输

`client.peerAuth` 与 `server.peerAuth.users` 用于认证 Engarde Session 握手。该协议
使用新生成的 nonce 和 HMAC proof，不会直接发送配置的密码；同一 Session 内的所有
虚拟 carrier 都关联到同一个已认证用户名。

这一机制属于准入控制，不是密钥交换或加密传输。握手完成后，Engarde 不会加密
`OPEN`、`DATA`、ACK 或其他 carrier frame，也不会为它们提供密码学完整性保护。路径上
的第三方可以观察目标地址和未加密的数据，也可能篡改或丢弃 Session 流量。

客户端与服务器之间应使用受信任的专用网络、WireGuard/IPsec VPN、TLS 隧道或同等
的受保护传输。为每个客户端身份使用独立、足够长的随机 peer secret，以便撤销单个
客户端时无需轮换整个部署的凭据。

`server.allowedClients` 会将 Session 连接的源 IP 与配置的 IP/CIDR 列表比较。它可以
作为额外的入口控制，但无法区分同一 NAT 后的不同客户端，在不受信任的网络上也不能
替代 `peerAuth`。建议同时使用入口防火墙和 `peerAuth`。

## 动态出口与 SSRF

每个 Engarde 服务器都是动态 TCP 出口。通过准入的客户端可以请求服务器可达的任意
IPv4、IPv6 或域名目标，包括 loopback 服务、私有网络、链路本地服务、云元数据端点
和公网主机。因此，已被入侵或权限过大的客户端可能借助服务器进行网络扫描、SSRF、
数据外传或滥用出口流量。

`server.allowedClients` 是针对入站 Session 连接源 IP 的 allowlist；`peerAuth` 用于认证
Session 身份。二者都不是目标地址 ACL。Engarde 本身不提供目标地址或端口 allowlist。

服务器启动时至少需要配置 `allowedClients`、`peerAuth`，或设置
`allowUnsafeDynamicDestination: true`。这项启动检查可避免意外开放 carrier 监听器，
但不会限制已通过准入的客户端能够连接到哪里。`allowUnsafeDynamicDestination` 只应
用于隔离的测试环境。

目标地址策略应在 Engarde 之外实施：

- 在服务器防火墙上将 carrier 监听端口限制为预期的源网络，并启用 `peerAuth`。
- 使用默认拒绝的出口规则，只放行必需的目标网络和端口；应明确处理 loopback、私有、
  链路本地、元数据服务和 IPv6 路径。
- 域名由服务器解析，解析结果可能是内网地址。策略必须作用于解析后的实际连接，不能
  只检查请求中的主机名。
- 如需更强隔离，可在专用主机、容器或 network namespace 中运行服务器，并确保其路由
  和防火墙策略无法访问敏感的控制面服务。

服务器到目标地址这一段是普通 TCP。端到端保护取决于应用协议，例如 HTTPS 中的 TLS
或 SSH。

## Web Manager

可选的 Web Manager 使用 Go 的明文 HTTP 服务器，没有内置 TLS。它的用户名和密码也是
可选项：两者均为空时，UI 和 API 不进行认证；配置后使用 HTTP Basic authentication，
如果没有传输加密，凭据会随每次请求明文传输。

状态 API 会暴露接口和中继地址、活动目标、stream 状态及流量计数等运行信息。在客户端
角色中，API 还包含用于 include、exclude、toggle 或 reset 接口 override 的状态变更
端点。这些 handler 不校验 HTTP method，reset 端点当前会通过 `GET` 请求修改状态，也
没有独立的 CSRF 防护。应把 Web Manager 的访问权限视为管理员权限。

将 `webManager.listenAddr` 绑定到 loopback。远程管理应使用 SSH 端口转发、受信任的
VPN 或经过认证的 TLS 反向代理，并通过防火墙确保后端无法被直接访问。即使已经使用
受保护的传输，也应配置 Web Manager Basic authentication 作为纵深防御。不要把管理
监听器直接发布到互联网。

## 资源耗尽

以下准入限制的默认值都是零，即不设上限：

| 配置项 | 作用域 | 效果 |
| --- | --- | --- |
| `transfer.tcp.maxStreams` | 客户端和服务器 | 限制并发逻辑 stream 数。 |
| `transfer.tcp.maxCarriersPerStream` | 服务器 | 限制单个 stream 可附加的 carrier 数；客户端仍会在每个合格接口上尝试一个 carrier。 |
| `transfer.tcp.maxPendingConnections` | 服务器 | 限制物理连接的并发握手数。 |
| `transfer.tcp.maxPendingStreams` | 服务器 | 限制仍在处理 OPEN 和目标连接建立的虚拟 stream 数。 |
| `transfer.tcp.maxSessions` | 服务器 | 限制已建立的物理复用 Session 数。 |

这些是进程级的粗粒度限制，并非按用户或源地址分配的 quota；一个通过准入的客户端可能
耗尽共享额度。生产环境应根据预期并发量和路径数显式设置非零值；要限制全部服务端连接
状态，应同时设置五项限制。还应合理设置文件描述
符上限、监控连接数与内存，并按需要使用防火墙或上游速率限制。

`carrierQueueBytes` 和 `reorderWindowBytes` 会限制单个 carrier 或 stream 持有的数据，
dial/open/write timeout 会限制停滞工作的持续时间，但它们不能代替准入限制：内存和
socket 用量仍会随着 stream 与 carrier 数量成倍增加。

## 凭据与主机控制

配置文件以明文保存凭据，应将其作为 secret 管理：

- 不要提交真实凭据、将其固化进镜像，或复制到 issue、报告和日志中。
- 将配置文件的 owner 和 permission 限制为运行服务的账号。
- 为 `socks5Auth`、`peerAuth` 和 Web Manager 使用不同的 secret，不要复用示例值。
- 建议每个客户端使用独立的 `peerAuth` 用户名和随机 secret；主机或配置可能泄露时应
  轮换并撤销凭据。
- RFC 1929 和 HTTP Basic 凭据只能通过 loopback 或加密传输承载。

以满足运行需求的最低 OS 权限启动 Engarde。在 Linux 上，按接口绑定客户端 carrier
可能需要 `CAP_NET_RAW`；应只授予这一项 capability，而不是让整个服务以 root 运行。
分别限制 SOCKS5、carrier 和管理端口的入站访问，并实施前述服务器出口策略。

## 生产检查清单

- 将 SOCKS5 前端保留在 loopback；否则必须同时使用认证、加密和防火墙规则保护它。
- 为每个客户端启用带独立长随机 secret 的 `peerAuth`，并用 VPN 或 TLS 隧道保护 carrier
  流量。
- 在防火墙上限制 carrier 入口和服务器出口；不要把 `allowedClients` 当成目标策略。
- 将 Web Manager 保留在 loopback，配置 Basic authentication，并通过 TLS/VPN/SSH
  进行远程访问。
- 为 stream、carrier 和 pending connection 设置非零限制，并监控实际资源用量。
- 保护配置文件、按用途拆分凭据，并准备轮换与撤销流程。
