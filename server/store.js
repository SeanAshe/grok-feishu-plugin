import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export function homeDir() {
  const raw = process.env.GROK_FEISHU_HOME?.trim();
  if (raw) return path.resolve(raw);
  return path.join(os.homedir(), ".grok-feishu");
}

export function paths() {
  const home = homeDir();
  return {
    home,
    state: path.join(home, "account.json"),
    wake: path.join(home, "wake.json"),
    inbox: path.join(home, "inbox.jsonl"),
    pid: path.join(home, "monitor.pid"),
    log: path.join(home, "monitor.log"),
    processed: path.join(home, "processed.txt"),
    health: path.join(home, "health.json"),
    wakePending: path.join(home, "wake-pending.json"),
    lock: path.join(home, "monitor.lock"),
  };
}

export function emptyAccount() {
  return {
    appId: "",
    brand: "feishu",
    defaultAs: "bot",
    wake: null,
    dedicatedAssistant: null,
    allowFrom: [],
  };
}

export function emptyState() {
  return {
    appId: "",
    brand: "feishu",
    defaultAs: "bot",
    configured: false,
    wake: null,
    dedicatedAssistant: null,
    allowFrom: [],
  };
}

function ensureHome() {
  const { home } = paths();
  fs.mkdirSync(home, { recursive: true });
}

function chmodSecure(filePath) {
  try {
    fs.chmodSync(filePath, 0o600);
  } catch {
    // ignore
  }
}

export function loadState() {
  ensureHome();
  const { state } = paths();
  if (!fs.existsSync(state)) return emptyState();
  try {
    const raw = JSON.parse(fs.readFileSync(state, "utf8"));
    return { ...emptyState(), ...raw };
  } catch {
    return emptyState();
  }
}

export function saveState(next) {
  ensureHome();
  const { state } = paths();
  const tmp = `${state}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(next, null, 2));
  fs.renameSync(tmp, state);
  chmodSecure(state);
}

export function updateState(mutator) {
  const current = loadState();
  const next = mutator(current) ?? current;
  saveState(next);
  return next;
}

export function isConfigured(state = loadState()) {
  return Boolean(state.configured && state.appId);
}

export function isAllowed(state, senderId) {
  if (!senderId) return false;
  if (!state.allowFrom?.length) return true;
  return state.allowFrom.includes(senderId);
}

export function appendInbox(messages) {
  if (!messages.length) return;
  ensureHome();
  const { inbox } = paths();
  const lines = messages.map((m) => JSON.stringify(m)).join("\n") + "\n";
  fs.appendFileSync(inbox, lines);
}

export function drainInbox() {
  const { inbox } = paths();
  if (!fs.existsSync(inbox)) return [];
  const raw = fs.readFileSync(inbox, "utf8");
  fs.writeFileSync(inbox, "");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

export function inboxHas(messageId) {
  if (!messageId) return false;
  const { inbox } = paths();
  if (!fs.existsSync(inbox)) return false;
  return fs
    .readFileSync(inbox, "utf8")
    .split("\n")
    .some((line) => {
      if (!line.trim()) return false;
      try {
        return JSON.parse(line).message_id === messageId;
      } catch {
        return false;
      }
    });
}

export function readHealth() {
  const { health } = paths();
  if (!fs.existsSync(health)) return null;
  try {
    return JSON.parse(fs.readFileSync(health, "utf8"));
  } catch {
    return null;
  }
}

export function writeHealth(health) {
  ensureHome();
  const file = paths().health;
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(health, null, 2));
  fs.renameSync(tmp, file);
}

export function peekInboxCount() {
  const { inbox } = paths();
  if (!fs.existsSync(inbox)) return 0;
  return fs
    .readFileSync(inbox, "utf8")
    .split("\n")
    .filter((line) => line.trim()).length;
}

export function writePid(pid) {
  ensureHome();
  fs.writeFileSync(paths().pid, String(pid));
}

export function readPid() {
  const { pid } = paths();
  if (!fs.existsSync(pid)) return 0;
  const n = Number(fs.readFileSync(pid, "utf8").trim());
  return Number.isFinite(n) ? n : 0;
}

export function clearPid() {
  const { pid } = paths();
  if (fs.existsSync(pid)) fs.unlinkSync(pid);
}

export function isPidAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function monitorRunning() {
  const pid = readPid();
  if (isPidAlive(pid)) return pid;
  if (pid) clearPid();
  return 0;
}

export function loadWake() {
  const { wake } = paths();
  if (!fs.existsSync(wake)) {
    const state = loadState();
    if (state.wake?.url && state.wake?.key) return state.wake;
    return null;
  }
  try {
    const cfg = JSON.parse(fs.readFileSync(wake, "utf8"));
    if (cfg.url && cfg.key) return { url: cfg.url, key: cfg.key };
  } catch {
    // ignore
  }
  return null;
}

export function saveWake(url, key) {
  ensureHome();
  const { wake } = paths();
  const tmp = `${wake}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ url, key }, null, 2));
  fs.renameSync(tmp, wake);
  chmodSecure(wake);
  updateState((s) => {
    s.wake = { url, key };
    return s;
  });
}

export function hasWake() {
  return Boolean(loadWake());
}

export function setDedicatedAssistant(assistantId, assistantName) {
  return updateState((s) => {
    s.dedicatedAssistant = {
      id: assistantId || "",
      name: assistantName || "",
    };
    return s;
  });
}

export function markProcessed(messageId) {
  if (!messageId) return;
  ensureHome();
  fs.appendFileSync(paths().processed, `${messageId}\n`);
}

export function isProcessed(messageId) {
  if (!messageId) return false;
  const { processed } = paths();
  if (!fs.existsSync(processed)) return false;
  const set = new Set(
    fs
      .readFileSync(processed, "utf8")
      .split(/\s+/)
      .filter(Boolean),
  );
  return set.has(messageId);
}

export function statusPayload() {
  const state = loadState();
  const wake = loadWake();
  return {
    configured: isConfigured(state),
    app_id: state.appId || "",
    brand: state.brand || "feishu",
    default_as: state.defaultAs || "bot",
    global_wake_configured: Boolean(wake),
    dedicated_assistant_id: state.dedicatedAssistant?.id || "",
    dedicated_assistant_name: state.dedicatedAssistant?.name || "",
    allow_from: state.allowFrom || [],
    inbox_count: peekInboxCount(),
    monitor_pid: monitorRunning(),
    home: homeDir(),
  };
}
