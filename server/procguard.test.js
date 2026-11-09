import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { evaluateConnectorPresence } from "./uninstall.js";
import {
  buildDurabilityView,
  heartbeatAgeMs,
  identifyProcess,
  planEnsure,
  selectStopTargets,
  signalOwned,
  startTimeMsFromStat,
  tryAcquireMonitorLock,
} from "./procguard.js";

const here = path.dirname(fileURLToPath(import.meta.url));

test("pid file pointing at the MCP host is not a kill target", () => {
  const mcp = "node /workspace/grok-feishu-plugin/server/index.js";
  const monitor = "node /workspace/grok-feishu-plugin/server/index.js --monitor";
  assert.equal(identifyProcess(10, mcp), "mcp_host");
  assert.equal(identifyProcess(11, monitor), "monitor");
  const plan = planEnsure({ pid: 10, role: "mcp_host", alive: true, heartbeatAgeMs: 0, processAgeMs: 5_000 });
  assert.deepEqual(plan.kill, []);
  assert.equal(plan.clearStalePid, true);
  assert.equal(plan.action, "start");
  const kills = [];
  const result = signalOwned(
    [
      { pid: 10, role: "monitor", sig: "SIGTERM" },
      { pid: 11, role: "monitor", sig: "SIGTERM" },
      { pid: 12, role: "consume", sig: "SIGTERM", group: true },
      { pid: 13, role: "bus", sig: "SIGTERM" },
    ],
    {
      selfPid: 1,
      readCmdline: (pid) =>
        ({
          10: mcp,
          11: monitor,
          12: "lark-cli event consume im.message.receive_v1 --as bot",
          13: "lark-cli event _bus --profile cli_x",
        })[pid] || "",
      kill: (pid, sig) => kills.push({ pid, sig }),
    },
  );
  assert.equal(result.refused.some((item) => item.pid === 10), true);
  assert.deepEqual(
    kills.map((item) => item.pid),
    [11, 12, -12, 13],
  );
  assert.equal(selectStopTargets([{ pid: 10, role: "mcp_host" }, { pid: 13, role: "bus" }]).length, 1);
});

test("fresh monitor is kept, stale monitor is replaced, startup grace is not a wedge", () => {
  assert.equal(
    planEnsure({ pid: 4, role: "monitor", alive: true, heartbeatAgeMs: 5_000, processAgeMs: 80_000 }).action,
    "keep",
  );
  assert.equal(
    planEnsure({ pid: 4, role: "monitor", alive: true, heartbeatAgeMs: null, processAgeMs: 5_000 }).reason,
    "monitor_starting",
  );
  const stale = planEnsure({ pid: 4, role: "monitor", alive: true, heartbeatAgeMs: 40_000, processAgeMs: 90_000 });
  assert.equal(stale.action, "replace_wedged");
  assert.deepEqual(stale.kill, [{ pid: 4, role: "monitor", sig: "SIGTERM" }]);
  assert.equal(heartbeatAgeMs({ pid: 9, uptime_ms: 10 }, 20, 4), null);
  const stat = "123 (node) S 1 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 500 0 0";
  assert.equal(startTimeMsFromStat(stat, 100), 5_000);
});

test("durability view never promotes a stale or transport-only state to e2e", () => {
  const stale = buildDurabilityView({
    role: "monitor",
    alive: true,
    heartbeatAgeMs: 60_000,
    health: { transport: { state: "connected", reason: "tcp_idle_or_healthy" }, e2e: { state: "proven" } },
  });
  assert.equal(stale.process_ready, false);
  assert.equal(stale.transport_state, "unknown");
  assert.equal(stale.e2e_state, "unproven");
  const live = buildDurabilityView({
    role: "monitor",
    alive: true,
    heartbeatAgeMs: 1_000,
    health: { transport: { state: "connected", reason: "tcp_idle_or_healthy" }, e2e: { state: "unproven" }, pending_wakes: 0 },
  });
  assert.equal(live.process_ready, true);
  assert.equal(live.transport_state, "connected");
  assert.equal(live.e2e_state, "unproven");
  assert.equal(live.rotation_is_health, false);
});

test("connector absence uses uptime, so a wall-clock jump is not an input", () => {
  const first = evaluateConnectorPresence(0, { mcpRunning: false, nowUptimeMs: 1_000 });
  assert.equal(first.abandoned, false);
  const jumpedWallWouldBeIrrelevant = evaluateConnectorPresence(first.goneSinceUptime, {
    mcpRunning: false,
    nowUptimeMs: 10_000,
  });
  assert.equal(jumpedWallWouldBeIrrelevant.abandoned, false);
  const gone = evaluateConnectorPresence(first.goneSinceUptime, { mcpRunning: false, nowUptimeMs: 40_000 });
  assert.equal(gone.abandoned, true);
  const back = evaluateConnectorPresence(gone.goneSinceUptime, { mcpRunning: true, nowUptimeMs: 80_000 });
  assert.equal(back.abandoned, false);
  assert.equal(back.goneSinceUptime, 0);
});

test("a live monitor lock is exclusive and an MCP-owned lock is stolen without signaling", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "feishu-lock-"));
  const lock = path.join(dir, "monitor.lock");
  fs.writeFileSync(lock, JSON.stringify({ pid: 5, role: "monitor" }));
  const raceLock = path.join(dir, "race.lock");
  const stolen = tryAcquireMonitorLock(lock, {
    pid: 9,
    readCmdline: (pid) => (pid === 5 ? "node server/index.js" : "node server/index.js --monitor"),
    isAlive: () => true,
    heartbeatFresh: () => true,
  });
  assert.equal(stolen.acquired, true);
  assert.equal(JSON.parse(fs.readFileSync(lock, "utf8")).pid, 9);

  const script = path.join(dir, "lock.mjs");
  const moduleUrl = path.join(here, "procguard.js");
  fs.writeFileSync(
    script,
    `import { tryAcquireMonitorLock } from ${JSON.stringify(moduleUrl)};
const result = tryAcquireMonitorLock(${JSON.stringify(raceLock)}, {
  pid: Number(process.argv[2]),
  readCmdline: () => "node server/index.js --monitor",
  isAlive: () => true,
  heartbeatFresh: () => true,
});
process.stdout.write(JSON.stringify(result));
`,
  );
  const run = (pid) =>
    new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [script, String(pid)], { stdio: ["ignore", "pipe", "pipe"] });
      let out = "";
      let err = "";
      child.stdout.on("data", (chunk) => {
        out += chunk;
      });
      child.stderr.on("data", (chunk) => {
        err += chunk;
      });
      child.on("exit", (code) => {
        if (code !== 0) reject(new Error(err || `exit ${code}`));
        else resolve(JSON.parse(out));
      });
    });
  const [left, right] = await Promise.all([run(111), run(222)]);
  const wins = [left, right].filter((item) => item.acquired);
  assert.equal(wins.length, 1);
  assert.equal([left, right].find((item) => !item.acquired).reason, "held_live");
});
