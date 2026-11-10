import fs from "node:fs";

export const HEARTBEAT_STALE_MS = 25_000;
export const STARTUP_GRACE_MS = 30_000;

export function identifyProcess(pid, cmdline) {
  const cmd = String(cmdline || "").replace(/\0/g, " ").trim();
  if (!pid || !cmd) return "dead_or_unreadable";
  if (/index\.js/.test(cmd) && /--monitor\b/.test(cmd)) return "monitor";
  if (/index\.js/.test(cmd) && /--uninstall\b/.test(cmd)) return "unrelated";
  if (/index\.js/.test(cmd)) return "mcp_host";
  if (/lark-cli/.test(cmd) && /_bus\b/.test(cmd)) return "bus";
  if (/lark-cli/.test(cmd) && /\bconsume\b/.test(cmd)) return "consume";
  return "unrelated";
}

export function readCmdline(pid) {
  try {
    return fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").replace(/\0/g, " ").trim();
  } catch {
    return "";
  }
}

export function readUptimeMs() {
  const raw = fs.readFileSync("/proc/uptime", "utf8");
  const seconds = Number(raw.trim().split(/\s+/)[0]);
  if (!Number.isFinite(seconds)) throw new Error("unreadable /proc/uptime");
  return Math.floor(seconds * 1000);
}

export function startTimeMsFromStat(statText, clkTck = 100) {
  const end = String(statText || "").lastIndexOf(")");
  if (end < 0) return null;
  const rest = statText.slice(end + 2).trim().split(/\s+/);
  const startTicks = Number(rest[19]);
  if (!Number.isFinite(startTicks)) return null;
  return (startTicks / clkTck) * 1000;
}

export function processAgeMs(pid, nowUptimeMs, reader = fs.readFileSync) {
  try {
    const stat = reader(`/proc/${pid}/stat`, "utf8");
    const started = startTimeMsFromStat(stat);
    if (started == null || !Number.isFinite(nowUptimeMs)) return null;
    return Math.max(0, nowUptimeMs - started);
  } catch {
    return null;
  }
}

export function heartbeatAgeMs(health, nowBootMs, pid) {
  if (!health || health.pid !== pid) return null;
  const mark = typeof health.boot_uptime_ms === "number" ? health.boot_uptime_ms : health.uptime_ms;
  if (typeof mark !== "number" || typeof nowBootMs !== "number") return null;
  return Math.max(0, nowBootMs - mark);
}

export function planEnsure({ pid, role, alive, heartbeatAgeMs: age, processAgeMs: procAge } = {}) {
  if (role === "mcp_host") {
    return { action: "start", kill: [], clearStalePid: true, reason: "pid_file_is_mcp_host" };
  }
  if (role === "monitor" && alive) {
    if (age != null && age <= HEARTBEAT_STALE_MS) {
      return { action: "keep", kill: [], clearStalePid: false, reason: "monitor_heartbeat_fresh" };
    }
    if (procAge == null || procAge <= STARTUP_GRACE_MS) {
      return { action: "keep", kill: [], clearStalePid: false, reason: "monitor_starting" };
    }
    return {
      action: "replace_wedged",
      kill: [{ pid, role: "monitor", sig: "SIGTERM" }],
      clearStalePid: false,
      reason: "monitor_heartbeat_stale",
    };
  }
  return {
    action: "start",
    kill: [],
    clearStalePid: Boolean(pid),
    reason: alive ? "pid_not_monitor" : "not_running",
  };
}

export function selectStopTargets(entries) {
  return (entries || []).filter((entry) => entry.role === "monitor" || entry.role === "consume" || entry.role === "bus");
}

export function signalOwned(requests, { readCmdline: read = readCmdline, kill, selfPid = process.pid } = {}) {
  if (typeof kill !== "function") throw new Error("signalOwned requires kill");
  const done = [];
  const refused = [];
  for (const req of requests || []) {
    const pid = Number(req?.pid);
    if (!pid || pid === selfPid || pid < 0) {
      refused.push({ pid, why: "refused_pid" });
      continue;
    }
    const role = identifyProcess(pid, read(pid));
    if (role !== req.role || role === "mcp_host" || role === "unrelated" || role === "dead_or_unreadable") {
      refused.push({ pid, role, expected: req.role, why: "cmdline_mismatch" });
      continue;
    }
    const sig = req.sig || "SIGTERM";
    try {
      kill(pid, sig);
      done.push({ pid, sig, role });
    } catch (error) {
      refused.push({ pid, why: "kill_failed", error: String(error?.message || error) });
      continue;
    }
    if (req.group && role === "consume") {
      try {
        kill(-pid, sig);
        done.push({ pid: -pid, sig, role: "consume_group" });
      } catch (error) {
        refused.push({ pid: -pid, why: "group_kill_failed", error: String(error?.message || error) });
      }
    }
  }
  return { done, refused };
}

function readHolder(lockPath) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      const text = fs.readFileSync(lockPath, "utf8");
      if (text.trim()) return JSON.parse(text);
    } catch {
      // retry while the winner finishes the write
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
  return null;
}

export function tryAcquireMonitorLock(lockPath, { pid, readCmdline: read = readCmdline, isAlive, heartbeatFresh, depth = 0 } = {}) {
  if (!pid) return { acquired: false, reason: "missing_pid" };
  if (depth > 3) return { acquired: false, reason: "lock_churn" };
  const payload = JSON.stringify({ pid, role: "monitor" });
  try {
    const fd = fs.openSync(lockPath, "wx");
    fs.writeFileSync(fd, payload);
    fs.closeSync(fd);
    return { acquired: true, reason: "acquired", holderPid: pid };
  } catch (error) {
    if (error?.code !== "EEXIST") throw error;
  }
  const holder = readHolder(lockPath);
  const holderPid = Number(holder?.pid) || 0;
  if (holderPid && isAlive?.(holderPid)) {
    const role = identifyProcess(holderPid, read(holderPid));
    if (role === "mcp_host") {
      try {
        fs.unlinkSync(lockPath);
      } catch {
        return { acquired: false, reason: "mcp_lock_stuck", holderPid, refusedMcp: true };
      }
      return tryAcquireMonitorLock(lockPath, { pid, readCmdline: read, isAlive, heartbeatFresh, depth: depth + 1 });
    }
    if (role === "monitor") {
      const fresh = heartbeatFresh?.(holderPid);
      if (fresh) return { acquired: false, reason: "held_live", holderPid };
      return { acquired: false, reason: "wedged", holderPid, wedged: true };
    }
  }
  try {
    fs.unlinkSync(lockPath);
  } catch {
    return { acquired: false, reason: "lock_busy", holderPid };
  }
  return tryAcquireMonitorLock(lockPath, { pid, readCmdline: read, isAlive, heartbeatFresh, depth: depth + 1 });
}

export function releaseMonitorLock(lockPath, pid) {
  try {
    const holder = JSON.parse(fs.readFileSync(lockPath, "utf8"));
    if (Number(holder.pid) === pid) fs.unlinkSync(lockPath);
  } catch {
    // already released
  }
}

export function descendantPids(pid, readChildren) {
  const out = [];
  const stack = [pid];
  const seen = new Set();
  while (stack.length) {
    const current = stack.pop();
    let kids = [];
    try {
      kids = readChildren(current) || [];
    } catch {
      kids = [];
    }
    for (const kid of kids) {
      if (!kid || seen.has(kid)) continue;
      seen.add(kid);
      out.push(kid);
      stack.push(kid);
    }
  }
  return out;
}

export function readProcChildren(pid) {
  try {
    const raw = fs.readFileSync(`/proc/${pid}/task/${pid}/children`, "utf8");
    return raw
      .trim()
      .split(/\s+/)
      .map((item) => Number(item))
      .filter((item) => item > 0);
  } catch {
    return [];
  }
}

export function buildDurabilityView({ role, alive, heartbeatAgeMs: age, health } = {}) {
  const processReady = role === "monitor" && alive && age != null && age <= HEARTBEAT_STALE_MS;
  let transport = health?.transport?.state || "unknown";
  let e2e = health?.e2e?.state || "unproven";
  if (e2e === "proven") e2e = "wake_accepted";
  if (e2e === "failing") e2e = "wake_failing";
  if (e2e === "pending") e2e = "wake_pending";
  if (!processReady) {
    transport = alive ? "unknown" : "down";
    e2e = "unproven";
  }
  return {
    process_ready: processReady,
    process_state: !alive
      ? "down"
      : role === "mcp_host"
        ? "mcp_host_not_monitor"
        : processReady
          ? "ready"
          : "wedged_or_starting",
    transport_state: transport,
    transport_reason: processReady ? health?.transport?.reason || "" : "process_not_ready",
    e2e_state: e2e,
    reply_e2e: "unproven",
    e2e_note:
      "wake_accepted 只表示 webhook HTTP 成功，不是飞书回复已经送达。process_ready、TCP ESTAB、transport_connected 也都不是完整回复。",
    boot_uptime_ms: health?.boot_uptime_ms ?? null,
    monitor_age_ms: health?.monitor_age_ms ?? null,
    rotation_is_health: false,
    pending_wakes: health?.pending_wakes ?? 0,
    clock_suspect: Boolean(health?.clock_suspect),
    heartbeat_age_ms: age,
    recycle_count: health?.recycle_count ?? 0,
  };
}
