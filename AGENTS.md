# Repository Guidelines（仓库指南）

## 项目结构与模块组织

`cmd/engarde/` 包含 CLI 入口。后端代码位于 `internal/`，按配置、客户端/服务端角色、SOCKS5、TCP 绑定与流处理、控制 API、统计和嵌入资源等职责划分；Go 测试与实现文件放在同一包内。Angular 管理界面位于 `webmanager/src/`，端到端测试位于 `webmanager/e2e/`。示例配置存放在 `examples/config/`，部署相关文件包括 `contrib/systemd/`、`compose.yml` 和 `Dockerfile`。`internal/assets/browser/` 与 `dist/` 均为生成内容，不要手工修改。

## 构建、测试与开发命令

- `make test`：执行一次完整的 Go 测试套件。
- `make test-production`：执行 PR/发布门禁，包括测试、`go vet`、竞态检测、压力与模糊测试以及覆盖率检查。
- `make`：构建并嵌入前端，同时交叉编译 Linux、Windows 和 macOS 二进制文件，输出到 `dist/{os}/{arch}/`。
- `make web-assets-force`：强制重新生成并嵌入 Angular 资源。
- `make test-fuzz FUZZ_TIME=30s`：运行全部 Go 模糊测试目标。
- `cd webmanager && npm ci && npm start`：安装锁定版本的依赖，并通过本地 API 代理启动前端。
- 在 `webmanager/` 中运行 `npm test`，执行 Karma/Jasmine 单元测试。

开发环境需要 Go 1.25+ 和 Node.js 22；CI 当前使用 Go 1.25.3 与 Node.js 22.21.0。

## 编码风格与命名约定

Go 代码必须经过 `gofmt`；遵循标准 Go 命名方式：导出标识符使用 `PascalCase`，内部标识符使用 `camelCase`，包名简短且全小写。Angular 文件使用 kebab-case 和职责后缀，例如 `dialog.component.ts`、`actionbar.service.ts`。前端 `.editorconfig` 要求 UTF-8、两个空格缩进、文件末尾换行，并清除行尾空格。

## 测试规范

Go 测试文件命名为 `*_test.go`，测试函数使用 `TestXxx`、`BenchmarkXxx` 或 `FuzzXxx`；集成行为可采用 `_e2e_test.go` 等明确后缀。Angular 单元测试使用 `*.spec.ts`。修复缺陷时，应在受影响的包附近增加回归测试。`make test-production` 对关键包执行语句覆盖率门禁，最低要求为 82% 至 100%。

## 提交与 Pull Request 规范

近期历史主要采用简短、祈使语气的 Conventional Commit 标题，例如 `feat: add carrier monitoring`、`fix: prevent ACK deadlock`、`perf: reduce relay allocations`。每个提交只处理一个明确主题。PR 应说明行为或配置变化、列出已执行的验证；涉及 `webmanager/` 可见变化时附上截图。提交前确保 `make test-production` 通过；CI 还会执行跨平台构建、Docker 冒烟测试及 Windows/macOS 原生测试。

## 安全与配置

不要在 YAML 中提交真实凭据；`server.yml` 和 `client.yml` 已被 Git 忽略。示例中只能使用非敏感占位值。新增配置字段时，应保留严格解析行为，并同步更新 `engarde.yml.sample` 和相关示例文件。
