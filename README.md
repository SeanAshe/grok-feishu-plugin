# grok-feishu-plugin

Grok Bot 的飞书 / Lark **应用机器人**渠道。配置开放平台 App ID + Secret 后，私聊/群聊消息经本机长连接写入收件箱，再经共享 webhook 唤醒专属助手回复。

## 能力

- **应用机器人身份**（`--as bot`），无需用户 OAuth
- **长连接入站**：`lark-cli event consume im.message.receive_v1`
- **发消息 / 回复**：包装 `im +messages-send` / `+messages-reply`
- **全局 webhook**：粘贴一次，monitor 收到消息即 POST 唤醒
- **allowlist**：空则接收全部；非空只处理名单内 open_id
- **专属助手**：按应用登记「飞书1」等侧栏助手，入站转交

## 与 grok-wechat-plugin 的差异

| | 飞书 | 微信 |
|---|---|---|
| 入站方式 | WebSocket 事件长连接（`event consume`） | iLink `getUpdates` 长轮询 |
| 鉴权 | 开放平台 App ID + App Secret | 扫码登录个人号 |
| 本机依赖 | 需安装 `lark-cli`（`npm i -g @larksuite/cli`） | 无额外 CLI |
| 身份 | 仅 bot 即可 | 个人号 |

两边入站模式一致：**本机常驻进程收消息 → 写 inbox → POST webhook → 助手 `*_inbox` → 转交专属助手回复**。

## 前置

1. 飞书开放平台创建应用，开启机器人，开通消息权限。
2. 事件订阅选「长连接」，添加 `im.message.receive_v1`。
3. **发布一个版本**（未发布会漏推送）。
4. 本机：`npm i -g @larksuite/cli`，确认 `lark-cli` 在 PATH。

## 一句话安装

在 Grok Bot 里发（本地开发可给路径）：

```
安装这个飞书插件 /workspace/grok-feishu-plugin
```

安装助手会：

1. 添加 MCP 连接器 `grok-feishu`（`node server/index.js`）
2. 让你提供 App ID / Secret，调用 `feishu_config_init`
3. 创建 Routine **「飞书入站唤醒」**（可选「飞书监听保活」）
4. 请你粘贴一次 Webhook URL + key → `feishu_set_wake`
5. `feishu_start_monitor`，并创建专属助手「飞书1」

细节见 `skills/feishu-channel/SKILL.md`。

## 链路

```
飞书服务器 --长连接--> lark-cli event consume
                         |
                         v
              monitor 写 inbox.jsonl
                         |
                         v
              POST wake.json 的 webhook（Bearer key）
                         |
                         v
              「飞书入站唤醒」→ feishu_inbox → 飞书1
                         |
                         v
              feishu_reply / feishu_send（bot）
```

## 本地状态

目录：`GROK_FEISHU_HOME` 或 `~/.grok-feishu`

| 文件 | 说明 |
|---|---|
| `account.json` | app_id、专属助手、allowlist（**不含** secret，权限 600） |
| `wake.json` | webhook url + key（权限 600） |
| `inbox.jsonl` | 入站缓存 |
| `monitor.pid` / `monitor.log` / `monitor.lock` | monitor 进程与重启互斥 |
| `health.json` | 进程 / 传输 / 端到端 三态（不含 secret） |
| `wake-pending.json` | 已入 inbox、唤醒尚未成功的消息 |
| `processed.txt` | 唤醒成功后的 message_id，防重复唤醒 |

## 入站耐久

`feishu_status.durability` 把三件事分开：

- `process_ready`：`--monitor` 进程活着。心跳比较的是 `boot_uptime_ms`（机器自开机起的单调时间）。`monitor_age_ms` 才是这个 monitor 进程的年龄。
- `transport_state=connected`：当前这一代 bus 自己的 ESTAB 套接字没有持续的 Send-Q 停滞。它不证明飞书消息已经进到助手。
- `e2e_state=wake_accepted`：这次 bus 回收之后，至少有一次 webhook HTTP 成功。`reply_e2e` 仍是 `unproven`，要在 box 上看到助手实际回复才算完整回复。

半开回收只看当前 bus pid 的 ESTAB 套接字，并且要有两份至少相隔 15 秒的单调时钟样本、Send-Q 持续大于 0、bytes_acked 没有前进。刚连通的 15 秒内不回收。一次回收后至少冷却 30 秒。不会因为没有 pid 的 FIN-WAIT、别的进程连到同一个 fake-IP、或单独一次高 RTO 就回收。真正持续半开仍会在这个预算内回收。不会按进程名清理 MCP host。

唤醒在标记已处理之前写入 `wake-pending.json`。超时或崩溃后下次启动会重试。墙钟前跳期间不按消息年龄丢弃事件。

## 开发 / 自检

```bash
cd /workspace/grok-feishu-plugin
npm run check
node server/index.js --list-tools
```

MCP 握手：向 stdin 写入一行 JSON，例如：

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | node server/index.js
```

## 一句话卸载

```
卸掉飞书插件
```

会停 monitor、清状态目录，并提示删除连接器、Routine 与侧栏专属助手。

## 安全提示

- 不要把 App Secret、Webhook key 明文写进仓库或长期聊天记录；用加密输入或环境变量一次性交给 `feishu_config_init` / `feishu_set_wake`。
- 机器人能收发消息，建议 allowlist 限制可对话用户。
- 本插件默认 **bot 身份**；不要把带用户身份的凭证分享给他人。
