import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { loadState, saveState, updateState } from "./store.js";

const EVENT_KEY = "im.message.receive_v1";

function resolveLarkCli() {
  const fromEnv = process.env.LARK_CLI_PATH?.trim();
  if (fromEnv && fs.existsSync(fromEnv)) return fromEnv;
  const which = spawnSync("bash", ["-lc", "command -v lark-cli || true"], {
    encoding: "utf8",
  });
  const found = (which.stdout || "").trim();
  if (found) return found;
  // Common Grok Bot / npm global location
  const candidates = [
    path.join(process.env.HOME || "/home/box", ".local/bin/lark-cli"),
    "/usr/local/bin/lark-cli",
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  throw new Error(
    "找不到 lark-cli。请先安装：npm i -g @larksuite/cli，并确保在 PATH 中",
  );
}

export function larkCliPath() {
  return resolveLarkCli();
}

/**
 * Run lark-cli and return { ok, code, stdout, stderr, json }.
 * If stdinData is provided, it is written to the child's stdin.
 */
export function runLark(args, { stdinData = null, timeoutMs = 60_000 } = {}) {
  const bin = resolveLarkCli();
  const result = spawnSync(bin, args, {
    encoding: "utf8",
    input: stdinData == null ? undefined : String(stdinData),
    timeout: timeoutMs,
    env: {
      ...process.env,
      PATH: `${path.dirname(bin)}:${process.env.PATH || ""}`,
    },
    maxBuffer: 8 * 1024 * 1024,
  });
  const stdout = (result.stdout || "").trim();
  const stderr = (result.stderr || "").trim();
  let json = null;
  if (stdout) {
    try {
      json = JSON.parse(stdout);
    } catch {
      // not JSON — leave as text
    }
  }
  return {
    ok: result.status === 0,
    code: result.status,
    stdout,
    stderr,
    json,
    error: result.error ? String(result.error.message || result.error) : "",
  };
}

export function configInit({ appId, appSecret, brand = "feishu" } = {}) {
  const id = (appId || process.env.FEISHU_APP_ID || "").trim();
  const secret = (appSecret || process.env.FEISHU_APP_SECRET || "").trim();
  if (!id) throw new Error("缺少 app_id（参数或 FEISHU_APP_ID）");
  if (!secret) throw new Error("缺少 app_secret（参数或 FEISHU_APP_SECRET）");

  const result = runLark(
    [
      "config",
      "init",
      "--app-id",
      id,
      "--app-secret-stdin",
      "--brand",
      brand || "feishu",
      "--force-init",
    ],
    { stdinData: `${secret}\n` },
  );

  if (!result.ok) {
    throw new Error(
      `lark-cli config init 失败: ${result.stderr || result.stdout || result.error || `exit ${result.code}`}`,
    );
  }

  const state = updateState((s) => {
    s.appId = id;
    s.brand = brand || "feishu";
    s.defaultAs = "bot";
    s.configured = true;
    return s;
  });

  return {
    configured: true,
    app_id: state.appId,
    brand: state.brand,
    lark_cli: result.stdout || result.json || { ok: true },
    note: "凭证已写入 lark-cli 本地配置；本插件 account.json 不含 secret",
  };
}

export function whoami() {
  const result = runLark(["whoami", "--as", "bot"]);
  if (!result.ok && !result.json) {
    return {
      available: false,
      error: result.stderr || result.stdout || result.error || "whoami failed",
    };
  }
  return result.json || { raw: result.stdout };
}

export function configShow() {
  const result = runLark(["config", "show"]);
  return {
    ok: result.ok,
    stdout: result.stdout,
    stderr: result.stderr,
    json: result.json,
  };
}

function extractTextContent(content) {
  if (content == null) return "";
  if (typeof content === "string") {
    const trimmed = content.trim();
    if (trimmed.startsWith("{")) {
      try {
        const obj = JSON.parse(trimmed);
        if (typeof obj.text === "string") return obj.text;
        return trimmed;
      } catch {
        return content;
      }
    }
    return content;
  }
  if (typeof content === "object" && typeof content.text === "string") {
    return content.text;
  }
  try {
    return JSON.stringify(content);
  } catch {
    return String(content);
  }
}

/**
 * Normalize a raw lark-cli event line into an inbox message.
 */
export function normalizeEvent(ev) {
  if (!ev || typeof ev !== "object") return null;
  const messageId = ev.message_id || ev.id || "";
  const chatId = ev.chat_id || "";
  const senderId = ev.sender_id || ev.sender?.sender_id?.open_id || "";
  const content = extractTextContent(ev.content);
  return {
    source: "grok-feishu",
    type: ev.type || EVENT_KEY,
    event_id: ev.event_id || "",
    message_id: messageId,
    chat_id: chatId,
    chat_type: ev.chat_type || "",
    message_type: ev.message_type || "text",
    sender_id: senderId,
    sender_type: ev.sender_type || "user",
    content,
    text: content,
    create_time: ev.create_time || ev.timestamp || "",
    app_id: loadState().appId || "",
    raw: ev,
  };
}

export function sendMessage({ chatId, userId, text, markdown } = {}) {
  if (!chatId && !userId) throw new Error("需要 chat_id 或 user_id");
  if (!text && !markdown) throw new Error("需要 text 或 markdown");
  const args = ["im", "+messages-send", "--as", "bot", "--format", "json"];
  if (chatId) args.push("--chat-id", chatId);
  if (userId) args.push("--user-id", userId);
  if (markdown) args.push("--markdown", markdown);
  else args.push("--text", text);
  const result = runLark(args);
  if (!result.ok) {
    throw new Error(
      `发送失败: ${result.stderr || result.stdout || result.error || `exit ${result.code}`}`,
    );
  }
  return result.json || { ok: true, stdout: result.stdout };
}

export function replyMessage({ messageId, text, markdown, replyInThread = false } = {}) {
  if (!messageId) throw new Error("需要 message_id");
  if (!text && !markdown) throw new Error("需要 text 或 markdown");
  const args = [
    "im",
    "+messages-reply",
    "--as",
    "bot",
    "--format",
    "json",
    "--message-id",
    messageId,
  ];
  if (replyInThread) args.push("--reply-in-thread");
  if (markdown) args.push("--markdown", markdown);
  else args.push("--text", text);
  const result = runLark(args);
  if (!result.ok) {
    throw new Error(
      `回复失败: ${result.stderr || result.stdout || result.error || `exit ${result.code}`}`,
    );
  }
  return result.json || { ok: true, stdout: result.stdout };
}

/**
 * Direct `lark-cli event consume` argv.
 * Do not pass `--timeout`: a timed consumer exit only rotates the consumer onto
 * the same bus and does not heal a half-open websocket. The monitor holds stdin
 * open itself and recycles the bus when transport probes say so.
 */
export function eventConsumeArgs() {
  return ["event", "consume", EVENT_KEY, "--as", "bot"];
}

/**
 * Spawn long-lived `lark-cli event consume` and stream NDJSON lines.
 * Stdin stays open until closeStdin() so lark-cli does not exit on EOF.
 * The child is a new process-group leader so stop/recycle can signal that
 * group without touching the MCP host.
 */
export function spawnEventConsume({ onLine, onExit, onStderr, log } = {}) {
  const bin = resolveLarkCli();
  const args = eventConsumeArgs();
  log?.(`spawn ${bin} ${args.join(" ")} (stdin held open, no consume timeout)`);

  const child = spawn(bin, args, {
    stdio: ["pipe", "pipe", "pipe"],
    env: {
      ...process.env,
      PATH: `${path.dirname(bin)}:${process.env.PATH || ""}`,
    },
    detached: true,
  });
  child.stdin?.on("error", () => {});

  let buf = "";
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf("\n")) !== -1) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      onLine?.(line);
    }
  });

  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    for (const line of chunk.split("\n")) {
      const t = line.trim();
      if (!t) continue;
      log?.(`stderr ${t}`);
      onStderr?.(t);
    }
  });

  child.on("exit", (code, signal) => {
    onExit?.(code, signal);
  });

  return {
    child,
    pid: child.pid,
    closeStdin() {
      try {
        child.stdin?.end();
      } catch {
        // stdin already closed
      }
    },
  };
}

export { EVENT_KEY };
