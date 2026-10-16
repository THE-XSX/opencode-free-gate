# opencode-free-gate

使用 Go 实现的 OpenCode 免费模型反代网关。网关从公共代理池选取可用代理，并支持按请求轮换 HTTP、HTTPS、SOCKS5/SOCKS5H 代理。

## 关键特性

- 每次代理尝试使用独立的 `http.Transport`，不共享故障连接。直连与 ZenProxy relay 使用长生命周期连接池，优先 HTTP/2，避免每请求重新握手。
- 候选池拉取统一去重：并发调用共享一次拉取，完成后 2 秒内不重复请求（包括空池与 API 错误），不改变代理重试次数。
- 补池不占用请求预算：公共槽为空且还有后备层（自定义代理、ZenProxy 或直连）时，补池在后台进行，请求最多等 1.5 秒（`poolFillWait`）就交给后备层；探活成功一个立即入槽，槽位补满后放弃剩余探活，不再被一个不响应的代理拖到探活超时。
- 公共池与自定义代理并行预热，配置成自定义层优先时不会被公共池的响应速度拖慢。
- 流式请求默认 3 秒内拿不到响应头就取消当前尝试，整个代理链共享 10 秒总预算。
- 非流式请求不限制响应首字节，允许完整响应在默认 300 秒内结束；下游写入同样带空闲上限，客户端停止读取时不会长期占住连接。
- 流式请求在成功取得响应头后可继续传输；客户端断开、流长时间无数据、或客户端停止读取导致写入阻塞超过空闲上限时都会自动清理连接。完成日志在转发真正结束后输出，包含发送字节数与异常结束原因。
- 普通业务 `400/404/422` 直接返回，不再无意义地轮换代理。
- 支持公共 S 级代理、自定义代理和 ZenProxy relay 多级回退，回退顺序可通过 `PROXY_ORDER` 自定义。
- 上游请求携带完整 OpenCode 客户端头：真实且可自动更新的 `User-Agent`、`x-opencode-client`、`x-opencode-version`、稳定会话哈希 `x-opencode-session`、每请求唯一 `x-opencode-request`（同一请求的代理重试保持不变）与 `x-opencode-project`（透传已有合法 `ses_`/`req_`/`prj_` 标识并消除网关特征）。
- `/v1/messages` 与真实 OpenCode 客户端一致，使用 `x-api-key` 认证并自动补齐 `anthropic-version`。
- 同一会话优先固定同一代理出口（rendezvous 哈希亲和），减少匿名通道按出口 IP 限流带来的抖动；出口故障时自动回退到其他槽位。
- 保留原有 Docker 镜像名、端口、路由和环境变量。

## API 路由

| 客户端类型 | 路由 |
|---|---|
| OpenAI | `/openai/v1/models`、`/openai/v1/chat/completions` |
| Anthropic | `/anthropic/v1/messages` |
| Codex | `/codex/v1/responses` |
| 健康检查 | `/healthz` |

模型缓存有效期为 60 秒：启动时异步预热，过期后由请求触发后台刷新并立即返回旧缓存。冷启动尚无缓存时，模型列表立即返回内置别名与 `big-pickle`，模型重写也只使用内置别名，不等待上游；刷新完成后展示完整免费模型列表并启用完整名称映射。并发请求共享同一次后台刷新。

## 会话与客户端标识

网关为每个上游请求生成符合 OpenCode 官方协议要求的标识头，客户端无需自行构造：

- 优先透传客户端提供的合法 OpenCode 标识：若包含合法的 `ses_` 会话、`req_`/`msg_` 请求或 `prj_`/`global` 项目标识则直接保留，无需重复哈希。
- 没有显式会话标识时，优先使用客户端提供的 `conversation-id`、请求体 `conversation_id`、`metadata.session_id`，或使用第一条用户消息（Responses 请求使用 `input` 或 `previous_response_id`）生成稳定会话哈希，同一段多轮对话始终映射到同一会话与同一代理出口。
- 项目标识默认派生标准项目哈希（彻底消除任何网关特征字串），支持通过 `OPENCODE_PROJECT` 自定义或配置为 `global`。
- `x-opencode-request` 每个客户端请求重新生成，同一请求内的代理重试保持不变。
- 自动随上游请求携带 `x-opencode-version`，并支持后台定时同步最新官方发布版本。

## Docker 部署

```bash
docker compose up -d
```

或直接运行镜像：

```bash
docker run -d \
  --name opencode-free-gate \
  --restart unless-stopped \
  -p 13339:13339 \
  -e PORT=13339 \
  -e PROXY_MODE=auto \
  ghcr.io/guji08233/opencode-free-gate:latest
```

从 Bun 版本升级时，无需修改现有生产环境变量或 Caddy 路由，只需发布并拉取新的同名镜像。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `PORT` | `13339` | HTTP 监听端口 |
| `PROXY_MODE` | `auto` | `auto` 使用完整回退链；`custom` 从自定义代理开始 |
| `PROXY_ORDER` | 空 | 逗号分隔的回退顺序，取值 `public`、`zen`、`custom`（如 `custom,zen,public`）；设置后覆盖 `PROXY_MODE` 的默认顺序，省略的层会被跳过，直连始终作为最后兜底 |
| `OPENCODE_UA` | 空（自动生成） | 显式锁定发送至上游的 `User-Agent`，例如 `opencode/2.0.29 (linux x64)` |
| `OPENCODE_VERSION` | `2.0.29`（定时自动更新） | OpenCode 客户端版本号（同步注入 `x-opencode-version` 与 UA） |
| `OPENCODE_CLIENT` | `cli` | 客户端类型（如 `cli`、`desktop`、`tui`） |
| `OPENCODE_PLATFORM` | `linux x64` | 平台标识（如 `linux x64`、`darwin arm64`、`win32 x64`） |
| `OPENCODE_PROJECT` | `global` | 默认项目标识，设置 `global` 时向未传项目的请求上报 `global` |
| `SLOT_COUNT` | `5` | 公共代理槽位数，限制为 3–5；并发请求从不同槽位轮询开始 |
| `SLOT_RETRIES` | 槽位数 | 单请求最多尝试的公共代理数 |
| `CUSTOM_PROXIES` | 空 | 逗号分隔的代理 URL，支持 HTTP、HTTPS、SOCKS5/SOCKS5H |
| `CUSTOM_RETRIES` | `10` | 自定义代理重试数；`0` 表示按代理数量轮询一轮 |
| `ZENPROXY_RELAY` | `https://zenproxy.top/api/relay` | ZenProxy relay 地址 |
| `ZENPROXY_KEY` | 空 | ZenProxy API key；为空时跳过该层 |
| `ZENPROXY_RETRIES` | `5` | ZenProxy 尝试次数 |
| `FORCE_RELAY` | `0` | `1` 表示强制只走 ZenProxy |
| `PROXY_PROBE_TIMEOUT` | `8000` | 代理探活超时，毫秒 |
| `PROXY_REFRESH_MS` | `300000` | 公共候选池刷新间隔，毫秒 |
| `POOL_EMPTY_BACKOFF_MS` | `20000` | 公共池为空时的退避窗口，毫秒；窗口内不再随请求同步拉取候选池，设为 `0` 关闭退避 |
| `PROXY_FIRST_BYTE_TIMEOUT` | `3000` | 流式请求单次尝试取得响应头的最大时间，毫秒 |
| `HARD_TIMEOUT` | `10000` | 流式请求选择和重试链的总预算，毫秒 |
| `NON_STREAM_TIMEOUT` | `300000` | 非流式请求从进入网关到完整响应结束的最高时间，毫秒 |
| `TZ` | 系统默认 | 容器时区；镜像已包含 `tzdata` |

当前生产使用的 `CUSTOM_PROXIES`、重试次数和 ZenProxy 配置均可原样沿用。

## 重试规则

- 网络错误、连接/握手/首字节超时：关闭当前连接并切换代理。
- `401`、`403`、`408`、`425`、`429`、`5xx`：立即切换下一次尝试，不等待退避。
- 默认顺序为公共代理 5 次、ZenProxy 5 次、自定义代理 10 次，最后直连 1 次；`PROXY_ORDER` 可调整前三层的顺序或省略某些层，直连始终最后兜底。
- 代理层返回的 `429` 不会提前返回客户端；完成全部代理重试后，最终直连仍为 `429` 时才原样返回。
- 公共池为空（上游返回 `null`，即池处于 `emergency`）时，网关记录状态并进入 `POOL_EMPTY_BACKOFF_MS` 退避窗口：窗口内请求不再同步拉取候选池，直接走后续层级；后台刷新与启动期重试协程仍会按原节奏探测，池一恢复即退出退避。
- 公共槽为空但池未进入退避窗口时，补池同样在后台进行；只有公共池是这次请求唯一去处（未开启直连兜底、也没有自定义代理与 ZenProxy）时才同步补池，否则请求最多等 `poolFillWait`（1.5 秒）就交给后备层。
- 其他 `4xx`：视为业务请求错误，立即返回客户端。
- 流式总预算或非流式最高时间耗尽：返回 `504`，并取消仍在进行的底层请求。

## 本地开发

需要 Go 1.24 或更高版本：

```bash
go test ./...
PROXY_MODE=custom go run .
```

构建容器镜像：

```bash
docker build -t opencode-free-gate:local .
```

测试包含一个会接受 TCP 连接但永不返回数据的本地假代理，用于验证超时后连接确实被关闭。
