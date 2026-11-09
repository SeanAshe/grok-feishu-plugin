import assert from "node:assert/strict";
import test from "node:test";
import { deliverEvent } from "./inbound.js";
import { normalizeEvent } from "./lark.js";
import { signalOwned } from "./procguard.js";
import { createSupervisor } from "./supervisor.js";
import { createWakeQueue } from "./wake.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

process.env.GROK_FEISHU_HOME = fs.mkdtempSync(path.join(os.tmpdir(), "feishu-home-"));

const BUS = 4242;
const MCP = 7777;

function harness(sampleSockets) {
  const kills = [];
  const sleeps = [];
  const spawns = [];
  const cmdline = new Map([
    [BUS, "lark-cli event _bus --profile cli_test"],
    [MCP, "node /workspace/grok-feishu-plugin/server/index.js"],
  ]);
  const dead = new Set();
  let seq = 5000;
  let mono = 0;
  let wall = 1_000_000;
  const busEvents = [];
  let sockets = [];
  const sup = createSupervisor({
    selfPid: 1,
    nowMono: () => mono,
    nowWall: () => wall,
    readUptimeMs: () => 1_000_000 + mono,
    sleep: async (ms) => {
      sleeps.push(ms);
    },
    graceMs: 0,
    probeTimeoutMs: 40,
    spawnConsume: ({ onExit }) => {
      const pid = ++seq;
      cmdline.set(pid, "lark-cli event consume im.message.receive_v1 --as bot");
      const handle = { pid, onExit, closeStdin() {} };
      spawns.push(handle);
      return handle;
    },
    readCmdline: (pid) => cmdline.get(Math.abs(pid)) || "",
    readChildren: (pid) => (String(cmdline.get(pid) || "").includes("consume") ? [BUS, MCP] : []),
    isAlive: (pid) => cmdline.has(Math.abs(pid)) && !dead.has(Math.abs(pid)),
    signalOwned: (requests) =>
      signalOwned(requests, {
        selfPid: 1,
        readCmdline: (pid) => cmdline.get(Math.abs(pid)) || "",
        kill: (pid, sig) => {
          kills.push({ pid, sig });
          dead.add(Math.abs(pid));
        },
      }),
    sampleSockets: sampleSockets || (async () => ({ sockets })),
    readNewBusEvents: async () => busEvents.splice(0, busEvents.length),
    writeHealth: () => {},
    pendingCount: () => 0,
    flushWakes: async () => ({ okCount: 0, failCount: 0 }),
  });
  return {
    sup,
    kills,
    sleeps,
    spawns,
    cmdline,
    dead,
    pushEvent: (event) => busEvents.push(event),
    setSockets: (next) => {
      sockets = next;
    },
    setMono: (next) => {
      mono = next;
    },
    setWall: (next) => {
      wall = next;
    },
    mono: () => mono,
    wall: () => wall,
  };
}

const severe = {
  state: "ESTAB",
  sendQ: 59,
  local: "172.30.0.2:48784",
  peer: "198.18.0.1:443",
  rto: 26112,
  backoff: 7,
  bytesRetrans: 472,
  bytesAcked: 1655,
  bytesSent: 2185,
};

const healthy = {
  state: "ESTAB",
  sendQ: 0,
  local: "172.30.0.2:52800",
  peer: "198.18.0.1:443",
  rto: 204,
  backoff: 0,
  bytesAcked: 1338,
  bytesSent: 1337,
};

function killedPids(kills) {
  return kills.map((item) => item.pid);
}

test("half-open socket recycles consume and bus and does not signal the MCP host", async () => {
  const h = harness();
  h.setSockets([severe]);
  await h.sup.start();
  await h.sup.tick();
  const pids = killedPids(h.kills);
  assert.equal(pids.includes(MCP), false);
  assert.equal(pids.includes(BUS), true);
  assert.equal(pids.includes(h.spawns[0].pid), true);
  assert.equal(pids.includes(-h.spawns[0].pid), true);
  assert.equal(h.sup.snapshot().recycleCount, 1);
  assert.equal(h.spawns.length, 2);
  assert.equal(h.sup.snapshot().e2e, "unproven");
  assert.equal(h.sup.snapshot().rotation_is_health, false);
});

test("failed sdk reconnect recycles and a real bus restart does not", async () => {
  const h = harness(async () => ({ error: "ss unavailable", sockets: [] }));
  await h.sup.start();
  h.pushEvent({ kind: "ping_timeout", monoMs: 0 });
  await h.sup.tick();
  h.setMono(10_000);
  h.pushEvent({ kind: "reconnect_attempt", monoMs: 10_000 });
  await h.sup.tick();
  assert.equal(h.sup.snapshot().recycleCount, 0);
  h.setMono(46_000);
  await h.sup.tick();
  assert.equal(h.sup.snapshot().recycleCount, 1);
  assert.equal(killedPids(h.kills).includes(BUS), true);
  assert.equal(killedPids(h.kills).includes(MCP), false);

  const healed = harness(async () => ({ error: "ss unavailable", sockets: [] }));
  await healed.sup.start();
  healed.pushEvent({ kind: "ping_timeout", monoMs: 0 });
  await healed.sup.tick();
  healed.pushEvent({ kind: "bus_started", pid: BUS, monoMs: 20_000 });
  healed.setMono(50_000);
  await healed.sup.tick();
  assert.equal(healed.sup.snapshot().recycleCount, 0);
});

test("two hung probes recycle; one hang and a clock jump do not", async () => {
  const hung = harness(() => new Promise(() => {}));
  await hung.sup.start();
  await hung.sup.tick();
  assert.equal(hung.sup.snapshot().recycleCount, 0);
  assert.equal(hung.sup.snapshot().probeHangs, 1);
  await hung.sup.tick();
  assert.equal(hung.sup.snapshot().recycleCount, 1);
  assert.equal(killedPids(hung.kills).includes(MCP), false);

  const clock = harness();
  clock.setSockets([healthy]);
  await clock.sup.start();
  await clock.sup.tick();
  clock.setWall(clock.wall() + 180_000);
  clock.setMono(clock.mono() + 1_000);
  await clock.sup.tick();
  assert.equal(clock.sup.isClockSuspect(), true);
  assert.equal(clock.kills.length, 0);
  assert.equal(clock.sup.snapshot().transport.state, "connected");
  assert.equal(clock.sup.snapshot().e2e, "unproven");
});

test("healthy consumer exit reuses the bus; half-open exit recycles it; exit 0 is not immediate", async () => {
  const h = harness();
  h.setSockets([healthy]);
  await h.sup.start();
  await h.sup.tick();
  h.spawns.at(-1).onExit(0, null);
  await h.sup.idle();
  assert.equal(h.kills.length, 0);
  assert.equal(h.spawns.length, 2);
  assert.equal(h.sleeps.at(-1), 1000);
  assert.equal(h.sup.snapshot().rotation_is_health, false);
  assert.equal(h.sup.snapshot().e2e, "unproven");
  assert.equal(h.sup.snapshot().transport.state, "connected");

  h.setSockets([severe]);
  h.spawns.at(-1).onExit(0, null);
  await h.sup.idle();
  assert.equal(killedPids(h.kills).includes(BUS), true);
  assert.equal(killedPids(h.kills).includes(MCP), false);

  const rapid = harness();
  rapid.setSockets([healthy]);
  await rapid.sup.start();
  await rapid.sup.tick();
  const delays = [];
  for (let i = 0; i < 4; i += 1) {
    rapid.setMono(rapid.mono() + 1000);
    rapid.spawns.at(-1).onExit(0, null);
    await rapid.sup.idle();
    delays.push(rapid.sleeps.at(-1));
  }
  assert.deepEqual(delays, [1000, 2000, 4000, 8000]);
});

test("overlapping recycle requests collapse to one restart", async () => {
  const h = harness();
  await h.sup.start();
  await Promise.all([h.sup.requestRecycle("manual-a"), h.sup.requestRecycle("manual-b")]);
  assert.equal(h.spawns.length, 2);
  assert.equal(h.sup.snapshot().recycleCount, 1);
});

test("stop signals only owned consume and bus", async () => {
  const h = harness();
  await h.sup.start();
  await h.sup.tick();
  await h.sup.stop();
  assert.equal(killedPids(h.kills).includes(h.spawns[0].pid), true);
  assert.equal(killedPids(h.kills).includes(BUS), true);
  assert.equal(killedPids(h.kills).includes(MCP), false);
});

test("clock suspect from the supervisor keeps a duplicate-safe wake retryable", async () => {
  const h = harness();
  h.setSockets([healthy]);
  await h.sup.start();
  await h.sup.tick();
  h.setWall(h.wall() + 180_000);
  h.setMono(h.mono() + 500);
  await h.sup.tick();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "feishu-e2e-"));
  const pendingPath = path.join(dir, "pending.json");
  const processed = path.join(dir, "processed.txt");
  let calls = 0;
  const queue = createWakeQueue({
    pendingPath,
    timeoutMs: 50,
    readProcessed: (id) => fs.existsSync(processed) && fs.readFileSync(processed, "utf8").includes(id),
    markProcessed: (id) => fs.appendFileSync(processed, `${id}\n`),
    appendInbox: () => {},
    inboxHas: () => false,
    fetchImpl: async () => {
      calls += 1;
      return { ok: true, status: 200 };
    },
  });
  const line = JSON.stringify({
    message_id: "om_clock",
    chat_id: "oc_test",
    sender_id: "ou_test",
    content: JSON.stringify({ text: "hi" }),
    create_time: 1,
  });
  const delivered = await deliverEvent({
    line,
    normalize: normalizeEvent,
    isAllowed: () => true,
    state: {},
    queue,
    clockSuspect: h.sup.isClockSuspect(),
    wallNow: h.wall(),
  });
  assert.equal(h.sup.isClockSuspect(), true);
  assert.equal(delivered.wakeOk, true);
  assert.equal(calls, 1);
  const again = await deliverEvent({
    line,
    normalize: normalizeEvent,
    isAllowed: () => true,
    state: {},
    queue,
    clockSuspect: true,
    wallNow: h.wall(),
  });
  assert.equal(again.status, "duplicate_processed");
  assert.equal(calls, 1);
  h.sup.noteWakeOk();
  assert.equal(h.sup.snapshot().e2e, "proven");
  assert.equal(h.sup.snapshot().transport.state, "connected");
});
