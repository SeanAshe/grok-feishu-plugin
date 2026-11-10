#!/usr/bin/env node
import { spawn } from "node:child_process";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { deliverEvent } from "./inbound.js";
import {
  configInit,
  configShow,
  larkCliPath,
  normalizeEvent,
  replyMessage,
  sendMessage,
  spawnEventConsume,
  whoami,
} from "./lark.js";
import {
  buildDurabilityView,
  heartbeatAgeMs,
  identifyProcess,
  planEnsure,
  processAgeMs,
  readCmdline,
  readProcChildren,
  readUptimeMs,
  releaseMonitorLock,
  signalOwned,
  tryAcquireMonitorLock,
} from "./procguard.js";
import {
  appendInbox,
  clearPid,
  drainInbox,
  inboxHas,
  isAllowed,
  isConfigured,
  isProcessed,
  loadState,
  loadWake,
  markProcessed,
  paths,
  saveWake,
  setDedicatedAssistant,
  statusPayload,
  updateState,
  writeHealth,
  readHealth,
  writePid,
} from "./store.js";
import { createSupervisor } from "./supervisor.js";
import { createBusLogReader, defaultBusLogPath, samplePidSockets } from "./transport.js";
import {
  connectorAbandoned,
  registerShutdownCleanup,
  runUninstall,
  touchConnectorActive,
  uninstallFeishu,
} from "./uninstall.js";
import { createWakeQueue } from "./wake.js";

const self = fileURLToPath(import.meta.url);

function noteHostSpawn() {
  try {
    fs.mkdirSync("/workspace/.grok-feishu", { recursive: true });
    fs.appendFileSync(
      "/workspace/.grok-feishu/mcp-host.log",
      `${new Date().toISOString()} spawn pid=${process.pid} argv=${JSON.stringify(process.argv)} cwd=${process.cwd()}\n`,
    );
  } catch {
    /* ignore */
  }
}
try {
  if (process.stdout._handle?.setBlocking) process.stdout._handle.setBlocking(true);
  if (process.stdin._handle?.setBlocking) process.stdin._handle.setBlocking(true);
} catch {
  /* ignore */
}

function ok(data) {
  return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
}

function fail(message) {
  return {
    content: [{ type: "text", text: JSON.stringify({ error: message }, null, 2) }],
    isError: true,
  };
}

function killOwned(requests) {
  return signalOwned(requests, {
    readCmdline,
    selfPid: process.pid,
    kill: (pid, sig) => process.kill(pid, sig),
  });
}

function monitorSnapshot(pid) {
  const alive = pid ? isPidAliveSafe(pid) : false;
  const role = alive ? identifyProcess(pid, readCmdline(pid)) : "dead_or_unreadable";
  let uptime = 0;
  try {
    uptime = readUptimeMs();
  } catch {
    uptime = 0;
  }
  const health = readHealth();
  const age = heartbeatAgeMs(health, uptime, pid);
  return {
    pid,
    alive,
    role,
    uptime,
    health,
    age,
    procAge: alive ? processAgeMs(pid, uptime) : null,
    durability: buildDurabilityView({ role, alive, heartbeatAgeMs: age, health }),
  };
}

function isPidAliveSafe(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function waitUptime(timeoutMs, pred) {
  let start = 0;
  try {
    start = readUptimeMs();
  } catch {
    return pred();
  }
  while (readUptimeMs() - start < timeoutMs) {
    if (pred()) return true;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
  }
  return pred();
}

function ensureMonitor() {
  if (!isConfigured()) return { started: false, reason: "not_configured" };
  const rawPid = Number(fs.existsSync(paths().pid) ? fs.readFileSync(paths().pid, "utf8").trim() : 0) || 0;
  const snap = monitorSnapshot(rawPid);
  const plan = planEnsure({
    pid: rawPid,
    role: snap.role,
    alive: snap.alive,
    heartbeatAgeMs: snap.age,
    processAgeMs: snap.procAge,
  });
  if (plan.clearStalePid) clearPid();
  if (plan.kill.length) {
    const signaled = killOwned(plan.kill);
    const gone = waitUptime(5000, () => !isPidAliveSafe(rawPid));
    if (!gone) killOwned(plan.kill.map((item) => ({ ...item, sig: "SIGKILL" })));
    return {
      ...spawnMonitor(),
      replaced: plan.reason,
      signaled,
    };
  }
  if (plan.action === "keep") {
    return {
      already_running: true,
      pid: rawPid,
      reason: plan.reason,
      durability: snap.durability,
    };
  }
  return spawnMonitor();
}

function spawnMonitor() {
  const child = spawn(process.execPath, [self, "--monitor"], {
    detached: true,
    stdio: "ignore",
    env: process.env,
  });
  child.unref();
  writePid(child.pid);
  return {
    started: true,
    pid: child.pid,
    process_ready: false,
    transport_state: "unknown",
    e2e_state: "unproven",
  };
}

const tools = {
  feishu_config_init: {
    description:
      "用飞书开放平台应用凭证初始化 lark-cli（bot 身份）。必填 app_id + app_secret（或环境变量 FEISHU_APP_ID / FEISHU_APP_SECRET）。secret 经 stdin 传入，不写入本插件状态文件。",
    inputSchema: {
      type: "object",
      properties: {
        app_id: { type: "string", description: "飞书应用 App ID，如 cli_xxx" },
        app_secret: { type: "string", description: "飞书应用 App Secret（勿写入日志/聊天）" },
        brand: {
          type: "string",
          enum: ["feishu", "lark"],
          description: "品牌，默认 feishu",
        },
      },
    },
    handle: async ({ app_id, app_secret, brand }) =>
      ok(configInit({ appId: app_id, appSecret: app_secret, brand })),
  },
  feishu_status: {
    description:
      "查看应用是否已配置、wake、专属助手、allowlist、inbox、monitor 是否在跑、lark-cli whoami。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => {
      const payload = statusPayload();
      const rawPid = Number(fs.existsSync(paths().pid) ? fs.readFileSync(paths().pid, "utf8").trim() : 0) || 0;
      const snap = monitorSnapshot(rawPid);
      payload.monitor_pid = snap.role === "monitor" && snap.alive ? rawPid : 0;
      payload.durability = snap.durability;
      try {
        payload.lark_cli_path = larkCliPath();
        payload.whoami = whoami();
      } catch (err) {
        payload.lark_cli_error = err instanceof Error ? err.message : String(err);
      }
      try {
        payload.config_show = configShow();
      } catch {
        // ignore
      }
      return ok(payload);
    },
  },
  feishu_inbox: {
    description:
      "立即取出 monitor 缓存的入站消息（取出后清空）。每条含 message_id、chat_id、sender_id、content/text。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => ok({ messages: drainInbox(), from_monitor: true }),
  },
  feishu_set_wake: {
    description:
      "保存入站 webhook（wake.json，权限 600）。url/key 来自 Routine「飞书入站唤醒」面板，粘贴一次即可。",
    inputSchema: {
      type: "object",
      properties: {
        url: { type: "string", description: "webhook 地址" },
        key: { type: "string", description: "webhook 密钥" },
      },
      required: ["url", "key"],
    },
    handle: async ({ url, key }) => {
      saveWake(url, key);
      return ok({ wake_configured: true, url_host: (() => {
        try {
          return new URL(url).host;
        } catch {
          return "";
        }
      })() });
    },
  },
  feishu_set_dedicated_assistant: {
    description:
      "为当前飞书应用登记专属 Grok Bot 助手 id/名称，供入站转交与卸载侧栏列表。",
    inputSchema: {
      type: "object",
      properties: {
        assistant_id: { type: "string", description: "专属助手 id" },
        assistant_name: { type: "string", description: "专属助手侧栏名称（如「飞书1」）" },
        app_id: { type: "string", description: "可选，校验用" },
      },
      required: ["assistant_name"],
    },
    handle: async ({ assistant_id, assistant_name, app_id }) => {
      const state = setDedicatedAssistant(assistant_id, assistant_name);
      if (app_id && state.appId && app_id !== state.appId) {
        return ok({
          warning: `app_id 与已配置不一致：configured=${state.appId} given=${app_id}`,
          dedicated_assistant: state.dedicatedAssistant,
        });
      }
      return ok({ dedicated_assistant: state.dedicatedAssistant });
    },
  },
  feishu_send: {
    description:
      "用 bot 身份发消息。提供 chat_id 或 user_id（open_id），以及 text 或 markdown。包装 lark-cli im +messages-send --as bot。",
    inputSchema: {
      type: "object",
      properties: {
        chat_id: { type: "string", description: "会话 ID（oc_xxx）" },
        user_id: { type: "string", description: "用户 open_id（ou_xxx），与 chat_id 二选一" },
        text: { type: "string", description: "纯文本" },
        markdown: { type: "string", description: "Markdown（自动转 post）" },
      },
    },
    handle: async ({ chat_id, user_id, text, markdown }) =>
      ok(sendMessage({ chatId: chat_id, userId: user_id, text, markdown })),
  },
  feishu_reply: {
    description:
      "用 bot 身份回复某条消息。必填 message_id，以及 text 或 markdown。包装 lark-cli im +messages-reply --as bot。",
    inputSchema: {
      type: "object",
      properties: {
        message_id: { type: "string", description: "被回复消息 ID（om_xxx）" },
        text: { type: "string" },
        markdown: { type: "string" },
        reply_in_thread: { type: "boolean", description: "是否在话题内回复" },
      },
      required: ["message_id"],
    },
    handle: async ({ message_id, text, markdown, reply_in_thread }) =>
      ok(
        replyMessage({
          messageId: message_id,
          text,
          markdown,
          replyInThread: Boolean(reply_in_thread),
        }),
      ),
  },
  feishu_start_monitor: {
    description:
      "后台启动长连接：lark-cli event consume im.message.receive_v1 --as bot。收到事件先入 inbox 再唤醒 webhook。心跳新鲜则不重启；不杀 MCP host。半开传输由 monitor 回收整条 bus。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => ok(ensureMonitor()),
  },
  feishu_stop_monitor: {
    description: "停止后台长连接 monitor。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => {
      const rawPid = Number(fs.existsSync(paths().pid) ? fs.readFileSync(paths().pid, "utf8").trim() : 0) || 0;
      const snap = monitorSnapshot(rawPid);
      if (snap.role !== "monitor" || !snap.alive) {
        if (rawPid) clearPid();
        return ok({ stopped: true, was_running: false, refused: snap.role, killed: false });
      }
      const signaled = killOwned([{ pid: rawPid, role: "monitor", sig: "SIGTERM" }]);
      return ok({ stopped: true, pid: rawPid, signaled });
    },
  },
  feishu_approve: {
    description:
      "把 open_id 加入 allowlist。空列表表示接收全部私信；非空则只处理名单内用户。",
    inputSchema: {
      type: "object",
      properties: {
        user_id: { type: "string", description: "飞书用户 open_id（ou_xxx）" },
        clear: { type: "boolean", description: "true 时清空 allowlist" },
      },
    },
    handle: async ({ user_id, clear }) => {
      const state = updateState((s) => {
        if (!Array.isArray(s.allowFrom)) s.allowFrom = [];
        if (clear) s.allowFrom = [];
        else if (user_id && !s.allowFrom.includes(user_id)) s.allowFrom.push(user_id);
        return s;
      });
      return ok({ allow_from: state.allowFrom });
    },
  },
  feishu_uninstall: {
    description:
      "完整卸载飞书渠道：停止 monitor、删除状态与插件目录；返回 platform_cleanup 与 dedicated_assistants_sidebar_delete。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => ok(uninstallFeishu()),
  },
};

const TOOL_LIST = Object.entries(tools).map(([name, t]) => ({
  name,
  description: t.description,
  inputSchema: t.inputSchema,
}));

async function dispatch(name, args) {
  const tool = tools[name];
  if (!tool) return fail(`未知工具: ${name}`);
  try {
    return await tool.handle(args || {});
  } catch (err) {
    return fail(err instanceof Error ? err.message : String(err));
  }
}

function writeMessage(msg) {
  const json = JSON.stringify(msg);
  fs.writeSync(1, `${json}\n`);
  try {
    fs.appendFileSync(
      "/workspace/.grok-feishu/mcp-host.log",
      `${new Date().toISOString()} out ${json.slice(0, 200)}\n`,
    );
  } catch {
    /* ignore */
  }
}

async function onRpc(msg) {
  try {
    fs.appendFileSync(
      "/workspace/.grok-feishu/mcp-host.log",
      `${new Date().toISOString()} in ${JSON.stringify({ id: msg.id, method: msg.method }).slice(0, 300)}\n`,
    );
  } catch {
    /* ignore */
  }
  const { id, method, params } = msg;
  if (method === "initialize") {
    touchConnectorActive();
    if (isConfigured()) ensureMonitor();
    return {
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: params?.protocolVersion || "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "grok-feishu", version: "1.0.0" },
      },
    };
  }
  if (method === "notifications/initialized" || method === "initialized") return null;
  if (method === "ping") return { jsonrpc: "2.0", id, result: {} };
  if (method === "tools/list") {
    touchConnectorActive();
    return { jsonrpc: "2.0", id, result: { tools: TOOL_LIST } };
  }
  if (method === "tools/call") {
    touchConnectorActive();
    const result = await dispatch(params?.name, params?.arguments);
    return { jsonrpc: "2.0", id, result };
  }
  if (id === undefined) return null;
  return {
    jsonrpc: "2.0",
    id,
    error: { code: -32601, message: `Method not found: ${method}` },
  };
}

function startMcp() {
  registerShutdownCleanup();
  if (isConfigured()) ensureMonitor();
  let buf = Buffer.alloc(0);
  process.stdin.on("data", async (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      const headerEnd = buf.indexOf("\r\n\r\n");
      if (headerEnd === -1) {
        const nl = buf.indexOf("\n");
        if (nl !== -1) {
          const line = buf.slice(0, nl).toString("utf8").trim();
          buf = buf.slice(nl + 1);
          if (line.startsWith("{")) {
            try {
              const reply = await onRpc(JSON.parse(line));
              if (reply) writeMessage(reply);
            } catch (err) {
              writeMessage({
                jsonrpc: "2.0",
                id: null,
                error: { code: -32700, message: String(err) },
              });
            }
            continue;
          }
        }
        break;
      }
      const header = buf.slice(0, headerEnd).toString("utf8");
      const match = header.match(/Content-Length:\s*(\d+)/i);
      if (!match) {
        buf = buf.slice(headerEnd + 4);
        continue;
      }
      const len = Number(match[1]);
      const start = headerEnd + 4;
      if (buf.length < start + len) break;
      const body = buf.slice(start, start + len).toString("utf8");
      buf = buf.slice(start + len);
      try {
        const reply = await onRpc(JSON.parse(body));
        if (reply) writeMessage(reply);
      } catch (err) {
        writeMessage({
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: String(err) },
        });
      }
    }
  });
}

function postWake(message, { signal }) {
  const cfg = loadWake();
  if (!cfg?.url || !cfg?.key) {
    const error = new Error("no_wake_config");
    error.code = "NO_WAKE";
    throw error;
  }
  const state = loadState();
  const body = {
    source: "grok-feishu",
    count: 1,
    app_id: state.appId || "",
    dedicated_assistant_id: state.dedicatedAssistant?.id || "",
    dedicated_assistant_name: state.dedicatedAssistant?.name || "",
    message_ids: [message.message_id],
    sender_ids: [message.sender_id],
    ...message,
    events: [message],
  };
  return fetch(cfg.url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${cfg.key}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
  }).then(async (res) => {
    const text = await res.text();
    return { ok: res.ok, status: res.status, body: text.slice(0, 180) };
  });
}

async function runMonitor() {
  const logStream = fs.createWriteStream(paths().log, { flags: "a" });
  const log = (line) => {
    logStream.write(`${new Date().toISOString()} ${line}\n`);
  };
  log("monitor start");
  loadState();
  const lock = tryAcquireMonitorLock(paths().lock, {
    pid: process.pid,
    readCmdline,
    isAlive: isPidAliveSafe,
    heartbeatFresh: (holderPid) => {
      let uptime = 0;
      try {
        uptime = readUptimeMs();
      } catch {
        return false;
      }
      const age = heartbeatAgeMs(readHealth(), uptime, holderPid);
      if (age != null && age <= 25_000) return true;
      const procAge = processAgeMs(holderPid, uptime);
      return procAge != null && procAge <= 30_000;
    },
  });
  if (!lock.acquired) {
    log(`monitor lock not acquired: ${lock.reason} holder=${lock.holderPid || 0}`);
    process.exit(0);
  }
  writePid(process.pid);

  const queue = createWakeQueue({
    pendingPath: paths().wakePending,
    readProcessed: isProcessed,
    markProcessed,
    appendInbox,
    inboxHas,
    fetchImpl: postWake,
    log,
    timeoutMs: Number(process.env.FEISHU_WAKE_TIMEOUT_MS || 10_000),
  });
  let busLogPath = "";
  try {
    busLogPath = defaultBusLogPath(loadState().appId || "");
  } catch {
    busLogPath = "";
  }
  const readNewBusEvents = createBusLogReader(busLogPath);
  let supervisor;
  supervisor = createSupervisor({
    log,
    selfPid: process.pid,
    spawnConsume: (handlers) => spawnEventConsume({ ...handlers, log }),
    sampleSockets: (pid) => samplePidSockets(pid),
    readNewBusEvents,
    readCmdline,
    readChildren: readProcChildren,
    isAlive: isPidAliveSafe,
    signalOwned: killOwned,
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    nowMono: () => performance.now(),
    nowWall: () => Date.now(),
    readUptimeMs,
    processAgeMs,
    writeHealth,
    writePid,
    clearPid,
    releaseLock: () => releaseMonitorLock(paths().lock, process.pid),
    pendingCount: () => queue.pendingCount(),
    flushWakes: () => queue.flush(),
    onEventLine: (line) =>
      deliverEvent({
        line,
        normalize: normalizeEvent,
        isAllowed,
        state: loadState(),
        clockSuspect: supervisor.isClockSuspect(),
        queue,
        log,
      }),
    intervalMs: Number(process.env.FEISHU_PROBE_INTERVAL_MS || 10_000),
    probeTimeoutMs: Number(process.env.FEISHU_PROBE_TIMEOUT_MS || 8_000),
    graceMs: Number(process.env.FEISHU_STOP_GRACE_MS || 3_000),
  });

  let stopping = false;
  const onExit = () => {
    if (stopping) return;
    stopping = true;
    supervisor.stop().finally(() => process.exit(0));
  };
  process.on("SIGTERM", onExit);
  process.on("SIGINT", onExit);

  const maintenance = setInterval(() => {
    if (stopping) return;
    if (connectorAbandoned()) {
      log("connector removed, running uninstall");
      stopping = true;
      supervisor.stop().finally(() => {
        runUninstall();
        process.exit(0);
      });
      return;
    }
    if (!isConfigured()) {
      log("not configured, stopping");
      stopping = true;
      supervisor.stop().finally(() => process.exit(0));
    }
  }, 5_000);
  if (typeof maintenance.unref === "function") maintenance.unref();

  const started = await supervisor.start();
  if (!started.started) {
    log(`monitor exit: ${started.reason || "not started"}`);
    process.exit(0);
  }
  const intervalMs = Number(process.env.FEISHU_PROBE_INTERVAL_MS || 10_000);
  while (!stopping) {
    try {
      await supervisor.tick();
    } catch (error) {
      log(`tick error ${error instanceof Error ? error.message : String(error)}`);
    }
    if (stopping) break;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

function isDirectRun() {
  const entry = process.argv[1];
  if (!entry) return false;
  try {
    return fs.realpathSync(entry) === fs.realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
}

if (isDirectRun()) {
  if (process.argv.includes("--monitor")) {
    runMonitor();
  } else if (process.argv.includes("--ensure-monitor")) {
    process.stdout.write(`${JSON.stringify(ensureMonitor())}\n`);
  } else if (process.argv.includes("--list-tools")) {
    process.stdout.write(`${JSON.stringify(TOOL_LIST, null, 2)}\n`);
  } else if (process.argv.includes("--uninstall")) {
    process.stdout.write(`${JSON.stringify(runUninstall({ deferPluginRemoval: false }), null, 2)}\n`);
  } else {
    noteHostSpawn();
    startMcp();
  }
}
