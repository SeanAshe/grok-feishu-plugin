import { execSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { identifyProcess, readCmdline, selectStopTargets } from "./procguard.js";
import { clearPid, homeDir, loadState, monitorRunning, readPid } from "./store.js";

const PLUGIN_DIR_NAMES = ["grok-feishu-plugin"];
const GROK_BOX_HOME = "/home/box";
const MONITOR_MATCH = "grok-feishu-plugin/server/index.js --monitor";
const MCP_MATCH = "grok-feishu-plugin/server/index.js";

function isPluginDir(dir) {
  if (!dir || !fs.existsSync(dir)) return false;
  return PLUGIN_DIR_NAMES.includes(path.basename(path.resolve(dir)));
}

export function knownStateHomes() {
  const homes = new Set([
    path.join(GROK_BOX_HOME, ".grok-feishu"),
    path.join(os.homedir(), ".grok-feishu"),
  ]);
  const envHome = process.env.GROK_FEISHU_HOME?.trim();
  if (envHome) homes.add(path.resolve(envHome));
  homes.add(homeDir());
  return [...homes];
}

export function knownPluginDirs() {
  const dirs = new Set([
    path.join(GROK_BOX_HOME, "grok-feishu-plugin"),
    "/workspace/grok-feishu-plugin",
  ]);
  try {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const parent = path.dirname(here);
    if (isPluginDir(parent)) dirs.add(parent);
  } catch {
    // ignore
  }
  return [...dirs];
}

function sh(cmd) {
  return execSync(cmd, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
}

function listMonitorPids() {
  const pids = new Set();
  const fromFile = readPid();
  if (fromFile) pids.add(fromFile);
  const running = monitorRunning();
  if (running) pids.add(running);
  try {
    const out = sh(`pgrep -f "${MONITOR_MATCH}" 2>/dev/null || true`);
    for (const line of out.split("\n")) {
      const n = Number(line.trim());
      if (Number.isFinite(n) && n > 0) pids.add(n);
    }
  } catch {
    // ignore
  }
  // Also stop child lark-cli event consume started by our monitor
  try {
    const out = sh(`pgrep -f "lark-cli event consume im.message.receive_v1" 2>/dev/null || true`);
    for (const line of out.split("\n")) {
      const n = Number(line.trim());
      if (Number.isFinite(n) && n > 0) pids.add(n);
    }
  } catch {
    // ignore
  }
  return [...pids];
}

export function stopAllMonitors() {
  const stopped = [];
  for (const pid of listMonitorPids()) {
    const role = identifyProcess(pid, readCmdline(pid));
    if (!selectStopTargets([{ pid, role }]).length) continue;
    try {
      process.kill(pid, "SIGTERM");
      stopped.push(pid);
    } catch {
      // already gone
    }
  }
  clearPid();
  try {
    sh(`pkill -TERM -f "${MONITOR_MATCH}" 2>/dev/null || true`);
    sh(`pkill -TERM -f "lark-cli event consume im.message.receive_v1" 2>/dev/null || true`);
  } catch {
    // ignore
  }
  return [...new Set(stopped)];
}

function removeDir(target) {
  if (!target || !fs.existsSync(target)) return false;
  fs.rmSync(target, { recursive: true, force: true });
  return true;
}

export function removeAllStateHomes() {
  const removed = [];
  for (const dir of knownStateHomes()) {
    if (removeDir(dir)) removed.push(dir);
  }
  return [...new Set(removed)];
}

export function removeAllPluginDirs() {
  const removed = [];
  for (const dir of knownPluginDirs()) {
    if (!isPluginDir(dir)) continue;
    if (removeDir(dir)) removed.push(dir);
  }
  return removed;
}

function lineMatchesAutostart(line) {
  const text = line.trim();
  if (!text || text.startsWith("#")) return false;
  return /grok-feishu|ensure-monitor\.sh/.test(text);
}

function stripAutostartFromFile(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return false;
  const original = fs.readFileSync(filePath, "utf8");
  const lines = original.split("\n");
  const kept = lines.filter((line) => !lineMatchesAutostart(line));
  if (kept.length === lines.length) return false;
  const next = kept.join("\n");
  if (next.trim()) fs.writeFileSync(filePath, next.endsWith("\n") ? next : `${next}\n`);
  else fs.unlinkSync(filePath);
  return true;
}

export function removeAutostartEntries() {
  const touched = [];
  const profileFiles = [
    path.join(GROK_BOX_HOME, ".profile"),
    path.join(GROK_BOX_HOME, ".bashrc"),
    path.join(GROK_BOX_HOME, ".bash_profile"),
    path.join(os.homedir(), ".profile"),
    path.join(os.homedir(), ".bashrc"),
    path.join(os.homedir(), ".bash_profile"),
  ];
  for (const file of profileFiles) {
    if (stripAutostartFromFile(file)) touched.push(file);
  }
  try {
    const current = sh("crontab -l 2>/dev/null || true");
    if (current) {
      const lines = current.split("\n");
      const kept = lines.filter((line) => !lineMatchesAutostart(line));
      if (kept.length !== lines.length) {
        const tmp = path.join(os.tmpdir(), `grok-feishu-crontab-${process.pid}`);
        fs.writeFileSync(tmp, kept.join("\n") + (kept.length ? "\n" : ""));
        sh(`crontab "${tmp}"`);
        fs.unlinkSync(tmp);
        touched.push("crontab");
      }
    }
  } catch {
    // ignore
  }
  return touched;
}

function mcpStillRunning() {
  try {
    const out = sh(`pgrep -f "${MCP_MATCH}" 2>/dev/null || true`);
    for (const line of out.split("\n")) {
      const pid = Number(line.trim());
      if (!Number.isFinite(pid) || pid <= 0) continue;
      const args = sh(`ps -p ${pid} -o args= 2>/dev/null || true`);
      if (args && !args.includes("--monitor") && !args.includes("--uninstall")) return true;
    }
  } catch {
    // ignore
  }
  return false;
}

let mcpGoneSinceUptime = 0;

export function evaluateConnectorPresence(prevGoneSince, { mcpRunning, nowUptimeMs, thresholdMs = 30_000 } = {}) {
  if (mcpRunning) return { abandoned: false, goneSinceUptime: 0 };
  const since = prevGoneSince || nowUptimeMs;
  return {
    abandoned: nowUptimeMs - since > thresholdMs,
    goneSinceUptime: since,
  };
}

export function connectorAbandoned(readUptime = () => {
  try {
    const raw = fs.readFileSync("/proc/uptime", "utf8");
    return Math.floor(Number(raw.trim().split(/\s+/)[0]) * 1000);
  } catch {
    return 0;
  }
}) {
  const decision = evaluateConnectorPresence(mcpGoneSinceUptime, {
    mcpRunning: mcpStillRunning(),
    nowUptimeMs: readUptime(),
  });
  mcpGoneSinceUptime = decision.goneSinceUptime;
  return decision.abandoned;
}

export function touchConnectorActive() {
  for (const home of knownStateHomes()) {
    try {
      fs.mkdirSync(home, { recursive: true });
      fs.writeFileSync(path.join(home, "connector-active"), `${Date.now()}\n`);
    } catch {
      // ignore
    }
  }
}

function deferredCleanupShell() {
  const stateDirs = knownStateHomes().map((d) => `"${d}"`).join(" ");
  const pluginDirs = knownPluginDirs().map((d) => `"${d}"`).join(" ");
  return `
sleep 3
if pgrep -f "${MCP_MATCH}" >/dev/null 2>&1; then
  for pid in $(pgrep -f "${MCP_MATCH}" 2>/dev/null || true); do
    args=$(ps -p "$pid" -o args= 2>/dev/null || true)
    case "$args" in *--monitor*|*--uninstall*) ;; *) exit 0 ;; esac
  done
fi
pkill -TERM -f "${MONITOR_MATCH}" 2>/dev/null || true
pkill -TERM -f "lark-cli event consume im.message.receive_v1" 2>/dev/null || true
for d in ${stateDirs}; do rm -rf "$d" 2>/dev/null || true; done
for d in ${pluginDirs}; do rm -rf "$d" 2>/dev/null || true; done
for f in /home/box/.profile /home/box/.bashrc /home/box/.bash_profile "$HOME/.profile" "$HOME/.bashrc" "$HOME/.bash_profile"; do
  [ -f "$f" ] || continue
  grep -v -E 'grok-feishu|ensure-monitor\\.sh' "$f" > "$f.tmp" 2>/dev/null && mv "$f.tmp" "$f" || true
done
crontab -l 2>/dev/null | grep -v -E 'grok-feishu|ensure-monitor\\.sh' | crontab - 2>/dev/null || true
`.trim();
}

let shutdownHookRegistered = false;

export function scheduleDeferredUninstall() {
  try {
    const child = spawn("sh", ["-c", deferredCleanupShell()], {
      detached: true,
      stdio: "ignore",
      env: process.env,
    });
    child.unref();
    return true;
  } catch {
    return false;
  }
}

export function registerShutdownCleanup() {
  if (shutdownHookRegistered) return;
  shutdownHookRegistered = true;
  const run = () => scheduleDeferredUninstall();
  process.once("SIGTERM", run);
  process.once("SIGINT", run);
  process.once("SIGHUP", run);
  process.stdin?.once("end", run);
  process.stdin?.once("close", run);
}

function collectDedicatedAssistants() {
  const state = loadState();
  const a = state.dedicatedAssistant;
  if (!a || (!a.id && !a.name)) return [];
  return [
    {
      app_id: state.appId || "",
      assistant_id: a.id || "",
      assistant_name: a.name || "",
    },
  ];
}

function platformCleanup(dedicatedAssistants) {
  const sidebarNames = dedicatedAssistants.map((a) => a.assistant_name).filter(Boolean);
  return {
    connector: "grok-feishu",
    routine_names: ["飞书入站唤醒", "飞书监听保活"],
    dedicated_assistants_sidebar_delete: dedicatedAssistants,
    dedicated_assistant_sidebar_names: sidebarNames,
    execute: [
      "从 Grok Bot Settings 卸载 grok-feishu 连接器（若仍存在）",
      "删除安装助手上 Routine「飞书入站唤醒」与「飞书监听保活」",
      "删除其他助手上残留的同名 Routine",
      sidebarNames.length
        ? `告知用户在侧栏右键 → 删除专属助手：${sidebarNames.join("、")}`
        : "告知用户在侧栏删除绑定期间创建的专属助手",
    ],
  };
}

export function runUninstall({ deferPluginRemoval = false } = {}) {
  const dedicatedAssistants = collectDedicatedAssistants();
  const stoppedPids = stopAllMonitors();
  const removedAutostart = removeAutostartEntries();
  const removedStateDirs = removeAllStateHomes();
  let removedPluginDirs = [];
  let deferredPluginRemoval = false;

  if (deferPluginRemoval || mcpStillRunning()) {
    scheduleDeferredUninstall();
    deferredPluginRemoval = true;
  } else {
    removedPluginDirs = removeAllPluginDirs();
  }

  return {
    uninstalled: true,
    stopped_monitor_pids: stoppedPids,
    removed_state_dirs: removedStateDirs,
    removed_plugin_dirs: removedPluginDirs,
    removed_autostart: removedAutostart,
    deferred_plugin_removal: deferredPluginRemoval,
    dedicated_assistants_sidebar_delete: dedicatedAssistants,
    platform_cleanup: platformCleanup(dedicatedAssistants),
  };
}

export function uninstallFeishu() {
  return runUninstall({ deferPluginRemoval: true });
}

if (process.argv.includes("--uninstall")) {
  const immediate = process.argv.includes("--immediate");
  process.stdout.write(`${JSON.stringify(runUninstall({ deferPluginRemoval: !immediate }), null, 2)}\n`);
}
