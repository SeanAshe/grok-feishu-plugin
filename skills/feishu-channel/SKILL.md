---
name: 飞书渠道
description: 用户要在 Grok Bot 里安装、配置、收发、停用或卸载飞书/Lark 机器人渠道时使用。入站用 webhook 唤醒。依赖本机 lark-cli 长连接。
---
# 飞书渠道

通过 MCP `grok-feishu` 连接飞书开放平台**应用机器人**（bot 身份）。底层用官方 `@larksuite/cli`（`lark-cli`）做长连接事件订阅与发消息，**不**需要用户 OAuth。

`飞书消息 → monitor（lark-cli event consume）→ inbox + POST webhook → 安装助手醒来 → feishu_inbox → 转交专属助手 → feishu_reply / feishu_send`

任务做完或需要用户拍板：立刻回复。

## 与微信渠道的差异

| | 飞书 `grok-feishu` | 微信 `grok-wechat` |
|---|---|---|
| 协议 | 开放平台应用 + 事件长连接 | 腾讯 iLink 个人号 |
| 入站 | `lark-cli event consume im.message.receive_v1 --as bot` | `getUpdates` 长轮询 |
| 登录 | `feishu_config_init`（App ID + Secret） | 扫码 `wechat_login_start/wait` |
| 身份 | 仅 bot（推荐） | 个人号 |
| 依赖 | 本机 PATH 上有 `lark-cli`（`npm i -g @larksuite/cli`） | 无额外 CLI |

## 前置条件（用户侧飞书开放平台）

1. 创建企业自建应用，开启**机器人**能力。
2. 权限：收发单聊/群聊消息等相关 IM 权限。
3. **事件与回调**：订阅方式选「使用长连接接收事件」，添加 `im.message.receive_v1`（接收消息）。
4. **版本管理与发布**：创建版本并发布（未发布时推送会漏/延迟）。
5. 把机器人拉进群或私聊它发一条测试消息。

## 安装

用户发一句「安装这个飞书插件 &lt;仓库或本地路径&gt;」即完成本渠道安装。技能就是本文件。

安装完成后 Grok Bot 上具备：

1. **连接器** `grok-feishu`，入口 `server/index.js`（`AddMcpServer` / 对话里点「加」）。
2. **插件文件**落在 Grok Bot 电脑（如 `/home/box/grok-feishu-plugin` 或 `/workspace/grok-feishu-plugin`）。
3. **依赖**：确认 `lark-cli` 在 PATH（`npm i -g @larksuite/cli`）。
4. **配置应用**：调用 `feishu_config_init`（`app_id`、`app_secret`；或环境变量 `FEISHU_APP_ID` / `FEISHU_APP_SECRET`）。Secret 经 stdin 交给 lark-cli，**不要**写进聊天记录或本插件 `account.json`。
5. **安装助手**创建 Routine（全渠道共用，只建一次）：
   - **「飞书入站唤醒」**（webhook）：入站后 `feishu_inbox` → 转交专属助手
   - （可选）**「飞书监听保活」**：低频调用 `feishu_start_monitor`；用户若反感空转消耗可跳过，靠 MCP initialize / 手动拉起
6. **全局 webhook（粘贴一次）**：平台创建 webhook Routine 时**不会**把 url/key 返回给 agent。用户从该 Routine 面板复制 **Webhook URL** 与 **Webhook key** 发来一次。安装助手调用 `feishu_set_wake`（url、key）写入 `~/.grok-feishu/wake.json`（权限 600）。`feishu_status` 的 `global_wake_configured=true` 表示入站已就绪。
7. **拉起 monitor**：`feishu_start_monitor`（MCP `initialize` 时若已配置也会尝试拉起）。
8. **创建专属助手**（如「飞书1」），调用 `feishu_set_dedicated_assistant`（`assistant_id`、`assistant_name`）。专属助手**不**创建入站 Routine。

### AddMcpServer 示例

```json
{
  "name": "grok-feishu",
  "command": "node",
  "args": ["/workspace/grok-feishu-plugin/server/index.js"]
}
```

或相对插件根目录：`args: ["server/index.js"]`（cwd 为插件根）。

stdio 支持换行分隔 JSON 与 Content-Length 分帧。

## 工具

| 工具 | 职责 |
|---|---|
| `feishu_config_init` | 初始化 lark-cli 应用凭证（app_id + app_secret） |
| `feishu_set_wake` | 保存全局 webhook（url、key） |
| `feishu_set_dedicated_assistant` | 登记专属助手 id / name |
| `feishu_status` | 配置、wake、专属助手、allowlist、inbox、monitor、whoami。`durability.e2e_state=wake_accepted` 只表示 webhook HTTP 成功；`reply_e2e` 才表示还没有看到飞书回复。传输连通不是消息已送达 |
| `feishu_inbox` | 取出 monitor 缓存的入站消息（取出后清空） |
| `feishu_send` | bot 发消息：`chat_id` 或 `user_id` + `text`/`markdown` |
| `feishu_reply` | bot 回复：`message_id` + `text`/`markdown` |
| `feishu_start_monitor` | 后台长连接收 `im.message.receive_v1`，写 inbox 并 POST webhook |
| `feishu_stop_monitor` | 停止 monitor |
| `feishu_approve` | allowlist：`user_id` 加入；`clear=true` 清空 |
| `feishu_uninstall` | 本机完整清理，返回 `platform_cleanup` |

## 入站之后

共享入站助手被 webhook 唤醒后 `feishu_inbox`。每条消息含：

- `message_id` / `chat_id` / `chat_type`
- `sender_id`（open_id）
- `content` / `text`
- `app_id`
- `raw`：原始事件

转交对应专属助手。回复优先 `feishu_reply`（带同一 `message_id`）；也可 `feishu_send` 到同一 `chat_id`。

Webhook body 同时带有完整事件字段与 `events` 数组（兼容「直传 payload」与「先 inbox 再取」两种写法）。推荐仍走 `feishu_inbox`，避免 webhook 丢包时丢消息。

## 入站 Routine 提示词（建议精简）

> 飞书 webhook 入站。调用 feishu_inbox 取消息，转交专属助手（见 feishu_status.dedicated_assistant_*）用 feishu_reply / feishu_send 回复。无新消息则静默。

## 卸载

在 Grok Bot 里发：

```
卸掉飞书插件
```

安装助手执行：

1. 调用 `feishu_uninstall`（或 `scripts/uninstall.sh`）
2. 卸载 `grok-feishu` 连接器
3. 删除 Routine「飞书入站唤醒」「飞书监听保活」
4. 告知用户删除侧栏专属助手（如「飞书1」）

完成后重装需重新 `feishu_config_init` 并粘贴 webhook。
