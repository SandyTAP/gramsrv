# gramsrv - 开源 Telegram Server / MTProto Server Go 实现

`gramsrv` 是一个用 Go 编写的开源 Telegram server 实现和 MTProto server。
它是一个 Telegram-like backend，面向真实客户端兼容、自建聊天实验、协议研究，
以及一条长期可演进的社区 server 路线。

协议栈基于已发布的
[`github.com/iamxvbaba/td`](https://github.com/iamxvbaba/td) module
（`v1.2.1`），使用 canonical Layer 228 schema，并通过 sparse `tlprofile`
提供 exact Layer 225-228 compatibility profiles。

如果你正在搜索 **Telegram server 实现**、**MTProto server 实现**、
**Telegram 后端**、**Telegram clone server**、**自建 Telegram-like 聊天服务器**，
这个仓库就是可以运行、研究和共同优化的 server 侧实现。

[English README](README.md) · [官网](https://telesrv.net) · [OwpenGram 客户端](https://owpengram.org/) · [讨论群](https://t.me/telesrv_chat) · [频道](https://t.me/telesrv)

`gramsrv` 是独立的非官方项目，与 Telegram 官方及其团队没有关联，也未获得其背书或赞助。

## 搜索关键词

`telegram server` · `telegram server implementation` · `mtproto server` ·
`mtproto server in go` · `telegram backend` · `telegram-like server` ·
`self-hosted telegram` · `telegram desktop compatible server` ·
`android telegram compatible server` · `open source chat server` ·
`telegram server 实现` · `mtproto server 实现` · `自建 Telegram server`

## Demo Video

https://github.com/user-attachments/assets/25e651dc-a022-4d60-8b9b-ca3e8bfe216c

## 客户端生态

`gramsrv` 是这一套生态里的 server 侧实现，而客户端项目会让自建
Telegram-compatible 网络更容易被真实用户体验和验证。

我们也推荐关注 [OwpenGram](https://owpengram.org/)。它是一个支持多服务器的
Telegram-style 客户端项目，可以在官方网络、私有自建服务器和社区节点之间切换。
OwpenGram 内置了对 `gramsrv` server 的兼容支持，适合希望用同一个客户端连接多个
Telegram-compatible server 的用户。

## 项目特性

| 状态 | 特性 | 说明 |
|---|---|---|
| ✅ | 生产运行时拆分 | Edge 持有 MTProto 长连接，Core 执行业务 RPC，Egress 投递 durable updates，FileData 提供媒体字节，独立 SFU 持有实时媒体房间。 |
| ✅ | 所有 server 功能开源 | 协议接入、业务服务、存储层、兼容 handlers、媒体链路、updates、管理后台和实验模块都在本仓库。 |

## 功能清单

下面这些是开源代码里已经实现的 server 侧功能。

| 状态 | 功能 | 当前已实现 |
|---|---|---|
| ✅ | MTProto server 接入层 | TCP transport、RSA key exchange、auth key、加密 session、salt、ack/resend、bad message、RPC dispatch、canonical Layer 228，以及 sparse exact Layer 225-228 compatibility profiles。 |
| ✅ | 登录与账号 | 开发验证码登录、sign-in、sign-up、log-out、授权设备、账号设置、SRP/password 状态、email/passkey 相关路径。 |
| ✅ | 用户与联系人 | 用户资料、username、头像、联系人导入/搜索、block/privacy 状态、presence、last seen。 |
| ✅ | 会话与同步 | dialog list、置顶、手动未读、folders/filters、草稿、read boundary、durable updates、在线 fan-out、离线 difference 恢复。 |
| ✅ | Chatlists 与公开链接 | 聊天文件夹分享、chatlist invite links、加入/导入流程、撤销邀请处理、公开 username 落地页，以及统一公开链接落地页。 |
| ✅ | 私聊消息 | send、history、read receipts、edit、delete、forward、reply、富文本实体、媒体/相册消息、reactions、scheduled/TTL 相关路径。 |
| ✅ | 富文本消息 | Telegram Desktop rich text message、富文本内容转换、send/edit/scheduled 流程、dialog/history 投影，以及 memory/PostgreSQL 持久化。 |
| ✅ | AI 输入框与 ChatBot | 输入框改写/润色、默认和自定义 tone、addstyle 预览、本地与外部 provider 链、流式 `@ChatBot` 草稿回复、Business AI 回复钩子。 |
| ✅ | 消息翻译 | Telegram `messages.translateText`、provider-backed 批量翻译、peer 语言设置、单账号限流，以及默认不记录正文的日志策略。 |
| ✅ | 超级群与频道 | create、join、leave、邀请链接、成员、管理员、forum topics、关联讨论组 guest 访问、history、send/edit/delete/read、reactions、公开搜索和预览。 |
| ✅ | 媒体与文件 | upload、download、本地 blob 存储、照片、文档、缩略图、规范 GIFv 转换、外链媒体抓取、网页预览、地图缩略图缓存、用户/频道头像。 |
| ✅ | Stickers 与 Reactions | sticker/reaction catalog、seed 支持、saved GIFs、recent reactions、top reactions、default reactions、reaction moderation 相关路径。 |
| ✅ | Gifts 与 Stars | 动态 star gift catalog、后台导入、收藏品/唯一礼物升级流程、预付升级跟踪、频道 Stars 收益领取，以及可选 TON 导出 worker。 |
| ✅ | Stars 购买 Premium | 内置 `@premiumbot`、Layer 228 自购/赠送 invoice、原子 Stars 结算、到期 entitlement、退款，以及 Bot API `giftPremiumSubscription`。详见 [docs/premium-stars.md](docs/premium-stars.md)。 |
| ✅ | Bots 与 Mini Apps | bot 服务基础、callbacks、inline helpers、webview/mini-app 路径、适配 `python-telegram-bot` 等库的最小 Bot API gateway、持久化 `getUpdates` 投递队列和 demo 工具。 |
| ✅ | 通话与直播 | 私聊通话信令基础、group call 状态、RTMP live stream、定时视频通话、频道 `join_as` 身份、独立 SFU owner、liveness 与 expiry worker。 |
| ✅ | 管理与运维 | Admin API/UI backend、PostgreSQL migrations、Redis 易失态、retention workers、pprof/debug hooks、load-test helpers。 |
| ✅ | Desktop、Android、iOS 与 Web 兼容 | Telegram Desktop 是第一目标，Android、iOS 与 Web 兼容路径也由同一套 server 持续覆盖。 |

其中一部分能力仍是兼容优先或实验性质，但它们都是真实开放的 server 代码，不是隐藏的产品版功能。下一步希望大家一起把这些路径打磨得更稳、更快、更好用。

## 快速启动

依赖：

- Go 1.25 或更新版本
- Docker Desktop 或带 Compose 的 Docker Engine
- OpenSSL，如果要编译匹配的 Telegram Desktop 客户端

启动 PostgreSQL 和 Redis：

```powershell
docker compose -f deploy/docker-compose.yml up -d
```

编译拆分后的运行入口：

```powershell
New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin/gramsrv-file.exe ./cmd/telesrv-file
go build -o bin/gramsrv-core.exe ./cmd/telesrv-core
go build -o bin/gramsrv-egress.exe ./cmd/telesrv-egress
go build -o bin/gramsrv-sfu.exe ./cmd/telesrv-sfu
go build -o bin/gramsrv-edge.exe ./cmd/telesrv-edge
```

TON 导出 worker 是可选角色，仅在运维方启用 fail-closed TON 导出配置时单独构建：

```powershell
go build -o bin/gramsrv-ton.exe ./cmd/telesrv-ton
```

仓库内的本地 YAML 示例使用前面 Compose 启动的 PostgreSQL 和 Redis。先设置
这些 YAML 引用的 secret：

```powershell
$env:TELESRV_POSTGRES_DSN="postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable"
$env:TELESRV_CORE_EXEC_TOKEN="dev-coreexec-token"
$env:TELESRV_FILE_TOKEN="dev-file-token"
$env:TELESRV_EGRESS_ACK_TOKEN="dev-egress-ack-token"
$env:TELESRV_GROUPCALL_CONTROL_TOKEN="dev-groupcall-control-token"
$env:TELESRV_SFU_CONTROL_TOKEN="dev-sfu-control-token"
```

在不同终端使用相同环境变量，依次启动 FileData、Core、Egress、SFU 和 Edge：

```powershell
.\bin\gramsrv-file.exe --config configs\examples\file.local.yaml
.\bin\gramsrv-core.exe --config configs\examples\core.local.yaml
.\bin\gramsrv-egress.exe --config configs\examples\egress.local.yaml
.\bin\gramsrv-sfu.exe --config configs\examples\sfu.local.yaml
.\bin\gramsrv-edge.exe --config configs\examples\edge.local.yaml
```

Core 第一次启动时会创建 `data/server_rsa.pem`、执行数据库 migrations，并导入
内置语言包。FileData 持有上传分片和 blob range read，Edge 持有 MTProto 监听
（本地示例为 `0.0.0.0:2398`），Egress 持有 durable PTS 与 non-PTS 投递队列，
SFU 持有群通话媒体房间。v2 不提供单进程业务 fallback。

Windows 下也可以使用本地编排脚本构建并启动全部五个角色：

```powershell
.\scripts\restart-local-microservices.ps1 -AdvertiseIP 127.0.0.1
```

生产入口为每个进程使用一份 YAML，通过 `--config <path>` 或
`TELESRV_CONFIG=<path>` 指定。环境变量只用于展开 YAML 中类似
`${TELESRV_POSTGRES_DSN}` 的字符串，并不是所有字段的通用覆盖机制。完整说明见
[中文配置参数手册](docs/configuration.zh-CN.md)和
[英文配置参数手册](docs/configuration.en.md)。

## 最小公网部署端口清单

在公网服务器部署 `gramsrv` 时，需要根据启用的功能开放以下端口。

### 最小公网部署（仅聊天）

| 端口 | 协议 | 用途 | 是否必须 |
|---|---|---|---|
| 2398 | TCP | MTProto 主端口；`TELESRV_WEBSOCKET_ENABLE=true` 时同时处理 WebSocket | 是 |

### 启用管理后台

| 端口 | 协议 | 用途 | 说明 |
|---|---|---|---|
| 2599 | TCP | Admin REST API | 建议限制可访问 IP 或放在 VPN 后 |
| 2600 | TCP | Admin Web UI | 生产环境建议前面加 Nginx/反向代理 + HTTPS |

### 可选功能端口

| 端口 | 协议 | 用途 | 何时需要 |
|---|---|---|---|
| 2400 | TCP | RTMP 直播推流 ingest | 启用直播 |
| 12399 | UDP | SFU/WebRTC 群通话 | 启用语音/视频群通话 |
| 12400 | UDP | TURN/STUN 服务器 | 启用 P2P/通话 relay |
| 12500-12999 | UDP | TURN relay 端口段 | 启用 TURN relay |
| 可配置 | TCP | Bot API | 设置 `TELESRV_BOT_API_ADDR` 时 |
| 2401 示例 | TCP | username/sticker/chatlist 公开链接落地页 | 设置 `TELESRV_PUBLIC_LINK_WEB_ADDR=127.0.0.1:2401` 时 |

### 内部/调试端口（不要暴露到公网）

| 端口 | 默认监听 | 用途 |
|---|---|---|
| 2420 | `127.0.0.1:2420` | Core group-call control callback |
| 2440 | `127.0.0.1:2440` | CoreExec gRPC |
| 2450 | `127.0.0.1:2450` | SFU control gRPC |
| 2510 | `127.0.0.1:2510` | Egress ACK gRPC |
| 2520 | `127.0.0.1:2520` | FileData gRPC |
| 6060-6064 | loopback | 各角色 pprof/debug 端点 |
| 5432 | `127.0.0.1:5432` | PostgreSQL |
| 6399 | `127.0.0.1:6399` | Redis |

确保设置 `TELESRV_LISTEN=0.0.0.0:2398`，且 `TELESRV_ADVERTISE_IP` 指向公网 IP，客户端才能正确连接。

## 公开链接落地页

`gramsrv` 可以提供 `/<username>`、头像、`/addstickers/<shortName>`、
`/addemoji/<shortName>`、`/addlist/<slug>` 这些公开落地页。

`TELESRV_PUBLIC_LINK_WEB_ADDR` 是本机 HTTP 监听地址：

```env
TELESRV_PUBLIC_LINK_WEB_ADDR=127.0.0.1:2401
```

`TELESRV_PUBLIC_BASE_URL` 是生成公开链接时展示给用户的外部 canonical URL：

```env
TELESRV_PUBLIC_BASE_URL=https://your-domain.example
TELESRV_PUBLIC_APP_SCHEME=yourapp
TELESRV_PUBLIC_WEB_BASE_URL=https://web.your-domain.example
TELESRV_PUBLIC_APP_NAME=YourApp
```

生产环境建议让 `TELESRV_PUBLIC_LINK_WEB_ADDR` 只监听 loopback，再用 HTTPS
反向代理把公开路由转发到这个本地端口。

## 客户端兼容

官方 Telegram 客户端不能直接连接 `gramsrv`，因为它们信任的是 Telegram 官方 DC 列表和 RSA keys。你可以使用 [官网](https://telesrv.net) 提供的体验客户端，也可以自己做最小协议 patch。

当前 Telegram Desktop 基线：

- Telegram Desktop commit：`9caf32dffc90ddd9bb08ad5777b865f729fa167b`
- Canonical TL layer：228
- Exact compatibility profiles：Layer 225-228
- 本地 DC：`127.0.0.1:2398`，DC id `2`

等 `gramsrv` 生成 `data/server_rsa.pem` 后，导出匹配的公钥：

```powershell
openssl rsa -in data/server_rsa.pem -RSAPublicKey_out -out data/server_rsa.pub
```

修改 `Telegram/SourceFiles/mtproto/mtproto_dc_options.cpp`：

1. 把内置 production/test DC 列表替换为你的 `gramsrv` endpoint。
2. 把 `kPublicRSAKeys` 和 `kTestPublicRSAKeys` 都替换为 `data/server_rsa.pub`。
3. 给 built-in DC flags 加上 `Flag::f_tcpo_only`。

客户端 patch 应保持最小：只改 endpoint、RSA key 和 TCP-only flags，不要把 UI 改动混入协议兼容 patch。

## 多端冒烟验证

用不同的 TDesktop working directory，避免 Alice 和 Bob 共用同一个 `tdata`：

```powershell
$tdesktop = "C:\path\to\tdesktop\out\Debug\Telegram.exe"
Start-Process $tdesktop -ArgumentList @("-workdir", "$PWD\.tdata-alice")
Start-Process $tdesktop -ArgumentList @("-workdir", "$PWD\.tdata-bob")
```

用两个不同手机号登录。本地开发默认验证码是 `12345`，除非你修改了 `TELESRV_DEV_AUTH_CODE`。

推荐检查：

- 两个用户之间发送私聊消息、sticker、媒体、reply、forward、edit、delete 和 read receipts。
- 一个设备保持在线，另一个设备重启，验证离线 `updates.getDifference` 恢复。
- 同一账号多 session 登录，确认当前 session 不重复 echo，其它在线 session 能收到 updates。
- 检查 server 日志没有新增 `NOT_IMPLEMENTED`、`Unhandled RPC`、`bad_msg`、panic 或 internal error。

## 贡献者

- [ajarshia](https://github.com/ajarshia) - Android Persian (`fa`) 语言包。

## 仓库结构

```text
cmd/telesrv-edge/         MTProto 长连接 Edge
cmd/telesrv-core/         业务 RPC 与 CoreExec gRPC
cmd/telesrv-egress/       durable update 投递
cmd/telesrv-file/         文件数据面服务
cmd/telesrv-sfu/          独立实时媒体 owner
cmd/telesrv-admin/        管理后台 backend 与 web UI
cmd/telesrv-ton/          可选 TON 礼物导出 worker
configs/examples/         本地按角色拆分的 YAML 示例
deploy/                   docker-compose、migrations、部署辅助
data/                     内置语言包与可选种子数据
internal/mtprotoedge/     MTProto transport、auth key、session、ack/resend
internal/rpc/             TL router 与客户端兼容 handlers
internal/app/             domain services
internal/domain/          不依赖协议生成类型的 domain models
internal/store/           memory/postgres/redis 存储后端
internal/seed/            内置 seed catalog 加载器
internal/sfu/             独立 SFU control 与 ownership
internal/turnsrv/         TURN/STUN building blocks
```

## TODO LIST

- 优化 Bot 适配第三方库调用，如 `python-telegram-bot`。
- 修复一些 bug，持续加固已实现的兼容路径。

## 一起优化

`gramsrv` 非常欢迎大家一起跑、一起测、一起拆问题、一起优化。尤其欢迎这些贡献：

- Telegram Desktop 和 Android 兼容性报告，最好带可复现步骤。
- 启动、同步、聊天、媒体、通话、bots 或边界场景的 RPC trace。
- 围绕已实现路径的小而准的 bug fix。
- 在线/离线 updates、多端 session、read state、媒体、频道行为的测试。
- fan-out、分页、存储查询、媒体上传/下载、连接层等热点路径的性能优化。
- 让拆分后的本地运行拓扑更容易启动和排查的改进。

如果改动会影响客户端可见行为，请说明客户端版本/commit、验证过的 RPC 路径，以及 server 日志是否没有新增 `NOT_IMPLEMENTED`、`Unhandled RPC`、`bad_msg`、panic 或 internal error。

## 授权协议

`gramsrv` 使用 [Apache License 2.0](LICENSE) 发布。你可以在 Apache-2.0 条款下使用、修改、分发，也可以商用。

## 付费定制开发

如需付费定制开发功能，可以通过讨论群或官网联系作者。定制范围不限于某一端，可覆盖 server 功能、Telegram Desktop、Android、Web、部署、兼容适配，或围绕本项目的其它客户端/服务端路径。
