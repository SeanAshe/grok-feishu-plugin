import fs from "node:fs";
import { spawn } from "node:child_process";

/**
 * Bounded transport decisions for the Feishu long connection.
 *
 * lark-cli already embeds the official SDK websocket. Rotating `event consume`
 * (for example `--timeout 5m`) attaches a new consumer to the same bus process
 * and does not repair a half-open socket. These helpers only decide when the
 * monitor must recycle the consume process AND the bus process.
 *
 * Timers supplied by callers are monotonic milliseconds. Wall-clock jumps must
 * not shorten or extend the budgets.
 */

export const LIMITS = {
  severeBackoff: 6,
  severeRtoMs: 8000,
  mildPersistMs: 15_000,
  pingRecoveryMs: 45_000,
  clockJumpForwardMs: 30_000,
  clockJumpBackwardMs: 2_000,
  clockSuspectHoldMs: 120_000,
  maxEventAgeMs: 10 * 60 * 1000,
};

const TCP_STATES =
  "ESTAB|FIN-WAIT-1|FIN-WAIT-2|CLOSE-WAIT|LAST-ACK|SYN-SENT|SYN-RECV|TIME-WAIT|CLOSING|CLOSED|LISTEN";
const STATE_RE = new RegExp(`^(${TCP_STATES})\\s+(\\d+)\\s+(\\d+)\\s+(\\S+)\\s+(\\S+)`);

export function parseSsTcp(text) {
  const sockets = [];
  let current = null;
  for (const rawLine of String(text || "").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    const header = line.match(STATE_RE);
    if (header) {
      current = {
        state: header[1],
        recvQ: Number(header[2]),
        sendQ: Number(header[3]),
        local: header[4],
        peer: header[5],
        pid: null,
        detail: line,
      };
      const pid = line.match(/pid=(\d+)/);
      if (pid) current.pid = Number(pid[1]);
      const timer = line.match(/timer:\(on,[^,]+,(\d+)\)/);
      if (timer) current.backoff = Number(timer[1]);
      sockets.push(current);
      continue;
    }
    if (!current) continue;
    current.detail += `\n${line}`;
    const pick = (name) => {
      const match = line.match(new RegExp(`\\b${name}:(\\d+)`));
      return match ? Number(match[1]) : null;
    };
    const rto = pick("rto");
    const backoff = pick("backoff");
    const bytesSent = pick("bytes_sent");
    const bytesRetrans = pick("bytes_retrans");
    const bytesAcked = pick("bytes_acked");
    const notsent = pick("notsent");
    const unacked = pick("unacked");
    const lost = pick("lost");
    if (rto != null) current.rto = rto;
    if (backoff != null) current.backoff = backoff;
    if (bytesSent != null) current.bytesSent = bytesSent;
    if (bytesRetrans != null) current.bytesRetrans = bytesRetrans;
    if (bytesAcked != null) current.bytesAcked = bytesAcked;
    if (notsent != null) current.notsent = notsent;
    if (unacked != null) current.unacked = unacked;
    if (lost != null) current.lost = lost;
    const pid = line.match(/pid=(\d+)/);
    if (pid) current.pid = Number(pid[1]);
  }
  return sockets;
}

function isHttpsPeer(peer) {
  const value = String(peer || "");
  return value.endsWith(":443") || value.endsWith(":https");
}

export function classifySocket(sock) {
  if (!sock) return "other";
  const sendQ = Number(sock.sendQ || 0);
  const backoff = Number(sock.backoff || 0);
  const rto = Number(sock.rto || 0);
  const retrans = Number(sock.bytesRetrans || 0);
  const state = sock.state || "";
  const halfClose = state.startsWith("FIN-WAIT") || state === "CLOSE-WAIT" || state === "LAST-ACK";
  if (halfClose && (sendQ > 0 || backoff >= LIMITS.severeBackoff || rto >= LIMITS.severeRtoMs)) {
    return "half_open_severe";
  }
  if (state !== "ESTAB" || !isHttpsPeer(sock.peer)) return "other";
  if (sendQ > 0 && backoff >= LIMITS.severeBackoff && rto >= LIMITS.severeRtoMs) {
    return "half_open_severe";
  }
  if (sendQ > 0 && (retrans > 0 || backoff >= 1 || rto >= 1000)) return "half_open_mild";
  if (sendQ === 0) return "idle_or_healthy";
  return "other";
}

function socketKey(sock) {
  return `${sock.local || ""}>${sock.peer || ""}`;
}

function persistentMild(samples) {
  const latest = samples[samples.length - 1];
  if (!latest?.sockets?.length) return false;
  for (const sock of latest.sockets) {
    if (classifySocket(sock) !== "half_open_mild" && classifySocket(sock) !== "half_open_severe") continue;
    const earlier = [...samples].reverse().find((sample) => {
      if (sample === latest) return false;
      if (latest.monoMs - sample.monoMs < LIMITS.mildPersistMs) return false;
      return (sample.sockets || []).some((prev) => {
        if (socketKey(prev) !== socketKey(sock)) return false;
        const kind = classifySocket(prev);
        if (kind !== "half_open_mild" && kind !== "half_open_severe") return false;
        if (Number(prev.sendQ || 0) <= 0 || Number(sock.sendQ || 0) <= 0) return false;
        if (prev.bytesAcked == null || sock.bytesAcked == null) return true;
        return prev.bytesAcked === sock.bytesAcked;
      });
    });
    if (earlier) return true;
  }
  return false;
}

function unrecoveredPing(busEvents, samples, nowMono) {
  let lastPing = null;
  for (const event of busEvents) {
    if (event.kind === "ping_timeout") lastPing = event;
  }
  if (!lastPing || typeof lastPing.monoMs !== "number") return false;
  if (nowMono - lastPing.monoMs < LIMITS.pingRecoveryMs) return false;
  for (const event of busEvents) {
    if (event.monoMs > lastPing.monoMs && event.kind === "bus_started") return false;
  }
  for (const sample of samples) {
    if (sample.monoMs <= lastPing.monoMs) continue;
    const kinds = (sample.sockets || []).map(classifySocket);
    if (kinds.includes("idle_or_healthy") && !kinds.includes("half_open_severe") && !kinds.includes("half_open_mild")) {
      return false;
    }
  }
  return true;
}

export function decideRecovery({ samples = [], busEvents = [], nowMono = 0 } = {}) {
  const latest = samples[samples.length - 1];
  if (latest?.sockets?.some((sock) => classifySocket(sock) === "half_open_severe")) {
    return { action: "recycle_full", reason: "half_open_severe", rotation_is_health: false };
  }
  if (persistentMild(samples)) {
    return { action: "recycle_full", reason: "half_open_persistent", rotation_is_health: false };
  }
  if (unrecoveredPing(busEvents, samples, nowMono)) {
    return { action: "recycle_full", reason: "ping_timeout_unrecovered", rotation_is_health: false };
  }
  return { action: "none", reason: "no_bounded_fault", rotation_is_health: false };
}

export function planConsumeExit({ samples = [], busEvents = [], nowMono = 0, busAlive = false } = {}) {
  const recovery = decideRecovery({ samples, busEvents, nowMono });
  if (!busAlive || recovery.action === "recycle_full") {
    return {
      action: "recycle_full",
      reason: recovery.action === "recycle_full" ? recovery.reason : "bus_not_alive",
      rotation_is_health: false,
    };
  }
  return {
    action: "restart_consume_only",
    reason: "consume_exit_reuse_bus_unproven",
    rotation_is_health: false,
  };
}

export function restartDelayMs(exitsInLastMinute) {
  const count = Math.max(1, Number(exitsInLastMinute) || 1);
  if (count <= 1) return 1000;
  return Math.min(1000 * 2 ** (count - 1), 30_000);
}

export function nextClockSample(prev, wallMs, monoMs) {
  if (!prev) {
    return { wallMs, monoMs, jumpedForward: false, jumpedBackward: false, suspect: false, skewMs: 0 };
  }
  const dWall = wallMs - prev.wallMs;
  const dMono = monoMs - prev.monoMs;
  const skewMs = dWall - dMono;
  const jumpedForward = skewMs > LIMITS.clockJumpForwardMs;
  const jumpedBackward = dMono >= 0 && dWall < -LIMITS.clockJumpBackwardMs;
  return {
    wallMs,
    monoMs,
    jumpedForward,
    jumpedBackward,
    suspect: jumpedForward || jumpedBackward,
    skewMs,
  };
}

export function shouldDropForAge({
  createTimeRaw,
  wallNow,
  clockSuspect = false,
  maxAgeMs = LIMITS.maxEventAgeMs,
} = {}) {
  if (clockSuspect) return false;
  const created = Number(createTimeRaw || 0);
  if (!created) return false;
  const ms = created > 1e12 ? created : created * 1000;
  return wallNow - ms > maxAgeMs;
}

export function parseBusLine(line) {
  const text = String(line || "");
  if (/ping timeout/i.test(text)) return { kind: "ping_timeout" };
  if (/trying to reconnect/i.test(text)) return { kind: "reconnect_attempt" };
  const started = text.match(/Bus started\b[\s\S]*?\bpid=(\d+)/);
  if (started) return { kind: "bus_started", pid: Number(started[1]) };
  if (/Bus exited/i.test(text)) return { kind: "bus_exited" };
  if (/idle 30s/i.test(text)) return { kind: "bus_idle_shutdown" };
  const connected = text.match(/Consumer connected:\s*.*?\bpid=(\d+)/);
  if (connected) return { kind: "consumer_connected", pid: Number(connected[1]) };
  const disconnected = text.match(/Consumer disconnected:\s*.*?\bpid=(\d+)/);
  if (disconnected) return { kind: "consumer_disconnected", pid: Number(disconnected[1]) };
  return { kind: "other" };
}

export function transportFrom({ decision, samples = [], probeHangs = 0, hasBus = false, hasConsume = false } = {}) {
  if (!hasConsume) return { state: "down", reason: "consume_down" };
  if (decision?.action === "recycle_full") return { state: "down", reason: decision.reason };
  if (probeHangs > 0) return { state: "degraded", reason: "probe_hang" };
  const latest = samples[samples.length - 1];
  if (!latest) return { state: "unknown", reason: hasBus ? "no_sample" : "bus_unseen" };
  const kinds = (latest.sockets || []).map(classifySocket);
  if (kinds.includes("half_open_severe") || kinds.includes("half_open_mild")) {
    return { state: "degraded", reason: "half_open_unconfirmed" };
  }
  if (kinds.includes("idle_or_healthy")) return { state: "connected", reason: "tcp_idle_or_healthy" };
  return { state: "unknown", reason: latest.sockets?.length ? "unclassified" : "no_socket" };
}

export function e2eFrom({ pending = 0, failCount = 0, provenSinceRecycle = false } = {}) {
  if (pending > 0 && failCount > 0) return "failing";
  if (pending > 0) return "pending";
  if (provenSinceRecycle) return "proven";
  return "unproven";
}

export function runBounded(task, timeoutMs) {
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      resolve({ hung: true });
    }, timeoutMs);
    Promise.resolve()
      .then(task)
      .then((value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve({ hung: false, value });
      })
      .catch((error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve({ hung: false, value: { error: String(error?.message || error), sockets: [] } });
      });
  });
}

export function defaultBusLogPath(appId, env = process.env) {
  if (!appId) return "";
  const root = env.LARK_CLI_HOME || `${env.HOME || ""}/.lark-cli`;
  return `${root.replace(/\/$/, "")}/events/${appId}/bus.log`;
}

export function createBusLogReader(filePath, io = fs) {
  let offset = 0;
  let remainder = "";
  return function readNew(observedMono) {
    if (!filePath || !io.existsSync(filePath)) return [];
    const size = io.statSync(filePath).size;
    if (size < offset) {
      offset = 0;
      remainder = "";
    }
    if (size === offset) return [];
    const fd = io.openSync(filePath, "r");
    try {
      const buf = Buffer.alloc(size - offset);
      io.readSync(fd, buf, 0, buf.length, offset);
      offset = size;
      const lines = (remainder + buf.toString("utf8")).split("\n");
      remainder = lines.pop() || "";
      const events = [];
      for (const line of lines) {
        const parsed = parseBusLine(line);
        if (parsed.kind === "other") continue;
        events.push({ ...parsed, monoMs: observedMono });
      }
      return events;
    } finally {
      io.closeSync(fd);
    }
  };
}

export function samplePidSockets(pid, { command = "ss", timeoutMs = 5000 } = {}) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(command, ["-tinp", "dst", ":443"], { stdio: ["ignore", "pipe", "pipe"] });
    } catch (error) {
      resolve({ error: String(error?.message || error), sockets: [] });
      return;
    }
    let out = "";
    let err = "";
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(value);
    };
    const timer = setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {
        // already gone
      }
      finish({ error: "ss_timeout", sockets: [] });
    }, timeoutMs);
    if (typeof timer.unref === "function") timer.unref();
    child.stdout?.on("data", (chunk) => {
      out += chunk;
    });
    child.stderr?.on("data", (chunk) => {
      err += chunk;
    });
    child.on("error", (error) => {
      finish({ error: String(error?.message || error), sockets: [] });
    });
    child.on("exit", () => {
      const sockets = parseSsTcp(out).filter((sock) => !sock.pid || sock.pid === pid);
      if (!out && err) finish({ error: err.slice(0, 200), sockets });
      else finish({ sockets });
    });
  });
}
