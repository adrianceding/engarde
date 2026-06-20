# engarde

[English](README.md)

> 警告：这个重写版本主要由 AI 协助完成，目前还没有经过完整的人工代码审查。在完成独立审查、审计和充分测试之前，请不要将其用于生产环境或安全敏感场景。

engarde 是用于已有 WireGuard 隧道的 UDP 中继。它可以把 WireGuard UDP 流量发送到多条网络路径上，运行期间跟踪网卡变化，并在服务端把返回流量转发给活跃客户端。

本项目基于原始 [porech/engarde](https://github.com/porech/engarde) 项目。感谢原作者和贡献者创建 engarde，并为这个项目奠定基础。

## 它做什么

engarde 运行在 WireGuard 之外。它不会创建 TUN 设备，不会实现 WireGuard，也不会替代 `wg` 或 `wg-quick`。它需要已有且可用的 WireGuard 端点，并负责转发该端点的 UDP 数据包。

典型用途：

- 把同一份 WireGuard UDP 数据包通过多条上联网卡发送出去。
- 在网卡新增、移除或地址变化后继续转发。
- 排除不希望承载中继流量的网卡。
- 为指定网卡配置不同的远端中继地址。
- 通过嵌入式 Web 管理界面或 JSON API 查看 client/server 状态。

## 工作方式

engarde 有两种角色，由 `engarde.yml` 选择：

- client 模式在本地 WireGuard 端点附近监听，基于活跃网卡打开 UDP socket，并把数据包转发到配置的 server 中继地址。
- server 模式监听来自 client 的中继数据包，将其转发到服务端 WireGuard 端点，学习活跃 client 地址，并把返回数据包分发回活跃 client。

两个角色都包含在同一个 `engarde` 二进制中。角色由配置文件选择，因此同一个二进制可以部署在任意一端。

## 特性

- 为已有 WireGuard 流量提供多路径 UDP 转发。
- 运行期间动态发现网卡并刷新转发路径。
- client 模式支持按网卡配置远端地址覆盖。
- server 模式支持活跃客户端学习和返回路径分发。
- 使用有界的按目标分发队列，降低单个慢目标阻塞其他目标的影响。
- 可选的嵌入式 Web UI 和 JSON 状态 API。
- 支持 Linux、Windows 和 macOS 构建目标。

## 安装

从 [GitHub Releases](https://github.com/adrianceding/engarde/releases) 下载适合你平台的预编译二进制。类 Unix 系统上需要赋予可执行权限：

```sh
chmod +x engarde-linux-amd64
```

也可以使用 `make` 从源码构建，见 [从源码构建](#从源码构建)。

## 快速开始

从示例配置创建配置文件：

```sh
cp engarde.yml.sample engarde.yml
```

编辑 `engarde.yml`，只保留一个完整角色：`client:` 或 `server:`。

使用指定配置文件运行：

```sh
./engarde engarde.yml
```

常用命令：

```sh
./engarde -v
./engarde list-interfaces
```

如果没有提供配置路径，`engarde` 会从当前目录读取 `engarde.yml`。

## 配置

client 模式需要 `client.listenAddr` 和 `client.dstAddr`：

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

server 模式需要 `server.listenAddr` 和 `server.dstAddr`：

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

角色选择规则：

- 完整的 `client:` 配置会启动 client 模式。
- 完整的 `server:` 配置会启动 server 模式。
- 如果两个角色配置都完整，程序会拒绝启动，避免角色不明确。

重要字段：

- `writeTimeout`：socket 写超时，单位是毫秒。请使用普通整数；负数表示禁用写超时。
- `udpBatch`：可选的 UDP 批量 I/O 设置。省略时默认启用；设置 `enabled: false` 可强制使用逐包 I/O，也可以通过 `readSize` 和 `writeSize` 调整批量大小用于本地性能测试。
- `transfer`：可选的传输策略。`mode: direct` 保持原有 UDP 冗余发包；`mode: adaptive` 使用轻量 DATA/ACK 帧、keepalive、有界 pending/duplicate 窗口和每路径自适应 ACK 超时，先走当前最佳路径，超时后回退到所有健康路径。`ackTimeoutMillis` 是最小/初始超时。adaptive DATA 帧会增加 36 字节头部；请设置 WireGuard MTU，让内部 UDP 包不超过 framed payload 上限。
- `excludedInterfaces`：client 侧不参与中继转发的网卡。
- `interfaceLabels`：在 Web UI 中显示的网卡友好名称。
- `dstOverrides`：client 侧按网卡覆盖远端中继地址。
- `clientTimeout`：server 侧活跃客户端超时时间，单位是秒。
- `webManager`：可选的嵌入式 Web UI 和 JSON API 监听配置。

`webManager` 示例：

```yaml
webManager:
  listenAddr: "0.0.0.0:9001"
  username: "engarde"
  password: "engarde"
```

更多配置示例见 `engarde.yml.sample`。

## systemd 服务

Linux systemd unit 示例位于 `contrib/systemd/engarde.service`。它默认使用：

- 二进制路径：`/usr/local/bin/engarde`
- 配置路径：`/etc/engarde/engarde.yml`
- 服务用户和用户组：`engarde`

安装并启动服务：

```sh
sudo useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin engarde
sudo install -m 0755 engarde-linux-amd64 /usr/local/bin/engarde
sudo install -d -m 0750 -o root -g engarde /etc/engarde
sudo install -m 0640 -o root -g engarde engarde.yml.sample /etc/engarde/engarde.yml
sudo install -m 0644 contrib/systemd/engarde.service /etc/systemd/system/engarde.service
sudo systemctl daemon-reload
sudo systemctl enable --now engarde
```

启动服务前请先编辑 `/etc/engarde/engarde.yml`。

默认 unit 不授予 Linux capability。如果 Linux client 模式需要把 UDP socket 绑定到指定网卡，并因为权限不足失败，可以安装可选 drop-in：

```sh
sudo install -d -m 0755 /etc/systemd/system/engarde.service.d
sudo install -m 0644 contrib/systemd/engarde.service.d/bind-device.conf /etc/systemd/system/engarde.service.d/bind-device.conf
sudo systemctl daemon-reload
sudo systemctl restart engarde
```

## 从源码构建

依赖：

- Go 1.25 或更新版本
- 用于 Web 管理前端的 Node.js 和 npm
- 用于版本化构建的 Git 和 Make

构建所有支持的平台：

```sh
make
```

仅手动构建当前平台：

```sh
make web-assets
go build -ldflags "-s -w" -o engarde ./cmd/engarde
```

Makefile 会把平台产物写入 `dist/{os}/{arch}/`。每个平台只有一个二进制：类 Unix 系统为 `engarde`，Windows 为 `engarde.exe`。

嵌入式 Web UI 由 `make web-assets` 从 `webmanager/` 构建，并复制到 `internal/assets/browser`，随后被 Go 二进制嵌入。`internal/assets/browser` 中的生成文件不会提交入库；需要嵌入 Web 管理界面时，请通过 `make` 构建，或在手动 `go build` 前先运行 `make web-assets`。

## 项目状态

本仓库是对 engarde 的 AI 辅助重写与整合。它包含测试和本地构建检查，但仍需要仔细的人工 review，才能被视为生产可用。

## 许可证

本项目保留原始 engarde 的 GPLv2 许可证。详情见 `LICENSE.txt`。

engarde 是原始 engarde 项目的衍生作品；保留原许可证和署名是对上游工作的尊重。
