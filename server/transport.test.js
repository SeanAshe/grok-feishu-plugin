import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  classifySocket,
  createBusLogReader,
  decideRecovery,
  e2eFrom,
  nextClockSample,
  parseBusLine,
  parseSsTcp,
  planConsumeExit,
  restartDelayMs,
  runBounded,
  samplePidSockets,
  shouldDropForAge,
  transportFrom,
} from "./transport.js";

const SENDQ_59 = `
ESTAB      0      59        172.30.0.2:48784   198.18.0.1:443  users:(("lark-cli",pid=1130155,fd=10)) timer:(on,3.220sec,7)
	 ts sack cubic wscale:9,7 rto:26112 backoff:7 rtt:0.275/0.099 ato:40 mss:1448 bytes_sent:2185 bytes_retrans:472 bytes_acked:1655 bytes_received:9324 segs_out:86 segs_in:83
FIN-WAIT-1 0      25        172.30.0.2:32966   198.18.0.1:443                                         timer:(on,31sec,8)
	 ts sack cubic wscale:9,7 rto:52224 backoff:8 rtt:0.218/0.126 bytes_sent:1516 bytes_retrans:192 bytes_acked:1301
`;

const HEALTHY_IDLE = `
ESTAB 0      0         172.30.0.2:52800   198.18.0.1:https
	 cubic rto:204 rtt:0.287/0.073 bytes_sent:1337 bytes_acked:1338 segs_out:49 segs_in:55
`;

const MILD = {
  state: "ESTAB",
  sendQ: 20,
  local: "172.30.0.2:1",
  peer: "198.18.0.1:443",
  rto: 1500,
  backoff: 2,
  bytesRetrans: 100,
  bytesAcked: 1000,
  bytesSent: 1200,
};

test("parses half-open ss evidence and ignores healthy idle", () => {
  const sockets = parseSsTcp(SENDQ_59);
  assert.equal(sockets.length, 2);
  assert.equal(sockets[0].pid, 1130155);
  assert.equal(sockets[0].sendQ, 59);
  assert.equal(sockets[0].rto, 26112);
  assert.equal(sockets[0].backoff, 7);
  assert.equal(sockets[0].bytesRetrans, 472);
  assert.equal(classifySocket(sockets[0]), "half_open_candidate");
  assert.equal(classifySocket(sockets[1]), "other");
  const idle = parseSsTcp(HEALTHY_IDLE)[0];
  assert.equal(idle.sendQ, 0);
  assert.equal(idle.rto, 204);
  assert.equal(classifySocket(idle), "idle_or_healthy");
  assert.equal(
    decideRecovery({ samples: [{ monoMs: 0, busPid: 1130155, sockets: [{ ...idle, pid: 1130155 }] }], nowMono: 20_000, busPid: 1130155 }).action,
    "none",
  );
});

test("one severe sample does not recycle; a second frozen sample of the same bus socket does", () => {
  const severe = { ...parseSsTcp(SENDQ_59)[0], pid: 1130155 };
  assert.equal(
    decideRecovery({
      samples: [{ monoMs: 0, busPid: 1130155, sockets: [severe] }],
      nowMono: 20_000,
      busPid: 1130155,
    }).action,
    "none",
  );
  const mildEarly = { ...MILD, pid: 1130155 };
  const early = decideRecovery({
    samples: [
      { monoMs: 0, busPid: 1130155, sockets: [mildEarly] },
      { monoMs: 5_000, busPid: 1130155, sockets: [mildEarly] },
    ],
    nowMono: 5_000,
    busPid: 1130155,
  });
  assert.equal(early.action, "none");
  const mild = { ...MILD, pid: 1130155 };
  const persisted = decideRecovery({
    samples: [
      { monoMs: 0, busPid: 1130155, sockets: [mild] },
      { monoMs: 16_000, busPid: 1130155, sockets: [{ ...mild, bytesSent: 1200 }] },
    ],
    nowMono: 16_000,
    busPid: 1130155,
  });
  assert.equal(persisted.reason, "half_open_persistent");
  const confirmed = decideRecovery({
    samples: [
      { monoMs: 0, busPid: 1130155, sockets: [severe] },
      { monoMs: 16_000, busPid: 1130155, sockets: [severe] },
    ],
    nowMono: 16_000,
    busPid: 1130155,
  });
  assert.equal(confirmed.reason, "half_open_confirmed");
  assert.match(confirmed.evidence, /send_q=59/);
  assert.match(confirmed.evidence, /rto_ms=26112/);
  const healed = decideRecovery({
    samples: [
      { monoMs: 0, busPid: 1130155, sockets: [mild] },
      { monoMs: 16_000, busPid: 1130155, sockets: [{ ...mild, sendQ: 0, rto: 204, backoff: 0, bytesRetrans: 0 }] },
    ],
    nowMono: 16_000,
    busPid: 1130155,
  });
  assert.equal(healed.action, "none");
});

test("ping timeout budget is monotonic and reconnect attempts do not count as recovery", () => {
  const ping = [{ kind: "ping_timeout", monoMs: 0 }];
  const common = { busPid: 3, busStartedMono: 0 };
  assert.equal(decideRecovery({ busEvents: ping, nowMono: 44_000, ...common }).action, "none");
  const stillTrying = decideRecovery({
    busEvents: [
      ...ping,
      { kind: "reconnect_attempt", monoMs: 10_000 },
      { kind: "consumer_connected", monoMs: 20_000, pid: 9 },
    ],
    nowMono: 46_000,
    ...common,
  });
  assert.equal(stillTrying.reason, "ping_timeout_unrecovered");
  const restarted = decideRecovery({
    busEvents: [...ping, { kind: "reconnect_attempt", monoMs: 10_000 }, { kind: "bus_started", monoMs: 12_000, pid: 3 }],
    nowMono: 90_000,
    ...common,
  });
  assert.equal(restarted.action, "none");
  const healthyAfter = decideRecovery({
    busEvents: ping,
    samples: [{ monoMs: 50_000, busPid: 3, sockets: [{ ...parseSsTcp(HEALTHY_IDLE)[0], pid: 3 }] }],
    nowMono: 50_000,
    ...common,
  });
  assert.equal(healthyAfter.action, "none");
  const stalePing = decideRecovery({
    busEvents: [{ kind: "ping_timeout", monoMs: 0, generation: 1 }],
    nowMono: 90_000,
    busPid: 3,
    generation: 2,
    busStartedMono: 80_000,
  });
  assert.equal(stalePing.action, "none");
});

test("consumer exit reuses a healthy bus without calling rotation health, and recycles a sustained half-open bus", () => {
  const busPid = 1130155;
  const healthySock = { ...parseSsTcp(HEALTHY_IDLE)[0], pid: busPid };
  const healthy = planConsumeExit({
    busAlive: true,
    busPid,
    nowMono: 20_000,
    samples: [{ monoMs: 20_000, busPid, sockets: [healthySock] }],
  });
  assert.equal(healthy.action, "restart_consume_only");
  assert.equal(healthy.rotation_is_health, false);
  const severe = { ...parseSsTcp(SENDQ_59)[0], pid: busPid };
  const oneSample = planConsumeExit({
    busAlive: true,
    busPid,
    nowMono: 20_000,
    samples: [{ monoMs: 0, busPid, sockets: [severe] }],
  });
  assert.equal(oneSample.action, "restart_consume_only");
  const sick = planConsumeExit({
    busAlive: true,
    busPid,
    nowMono: 16_000,
    samples: [
      { monoMs: 0, busPid, sockets: [severe] },
      { monoMs: 16_000, busPid, sockets: [severe] },
    ],
  });
  assert.equal(sick.action, "recycle_full");
  assert.equal(sick.reason, "half_open_confirmed");
  assert.equal(planConsumeExit({ busAlive: false, busPid, nowMono: 0 }).reason, "bus_not_alive");
});

test("stale sockets, fresh connect, clock jump, and cooldown do not confirm a half-open bus", () => {
  const busPid = 1338591;
  const current = {
    state: "ESTAB",
    sendQ: 0,
    rto: 204,
    backoff: 0,
    bytesAcked: 965,
    bytesSent: 964,
    bytesRetrans: 0,
    pid: busPid,
    local: "172.30.0.2:40498",
    peer: "198.18.0.1:443",
  };
  const corpse = {
    state: "FIN-WAIT-1",
    sendQ: 25,
    rto: 52224,
    backoff: 8,
    bytesRetrans: 192,
    bytesAcked: 1301,
    pid: null,
    local: "172.30.0.2:32966",
    peer: "198.18.0.1:443",
  };
  const other = {
    state: "ESTAB",
    sendQ: 59,
    rto: 26112,
    backoff: 7,
    bytesRetrans: 472,
    bytesAcked: 1655,
    bytesSent: 2185,
    pid: 9999,
    local: "172.30.0.2:111",
    peer: "198.18.0.1:443",
  };
  const stalled = { ...other, pid: busPid, local: "172.30.0.2:40498" };
  const mixed = [current, corpse, other];
  const falsePositive = decideRecovery({
    samples: [
      { monoMs: 2_000, busPid, sockets: mixed },
      { monoMs: 4_000, busPid, sockets: mixed },
      { monoMs: 18_000, busPid, sockets: mixed },
    ],
    nowMono: 18_000,
    busPid,
    connectedMono: 0,
    busStartedMono: 0,
  });
  assert.equal(falsePositive.action, "none");
  const duringGrace = decideRecovery({
    samples: [
      { monoMs: 0, busPid, sockets: [stalled] },
      { monoMs: 16_000, busPid, sockets: [stalled] },
    ],
    nowMono: 16_000,
    busPid,
    connectedMono: 10_000,
  });
  assert.equal(duringGrace.reason, "connect_grace");
  const progressed = decideRecovery({
    samples: [
      { monoMs: 0, busPid, sockets: [stalled] },
      { monoMs: 16_000, busPid, sockets: [{ ...stalled, bytesAcked: 1800, bytesSent: 1900 }] },
    ],
    nowMono: 20_000,
    busPid,
  });
  assert.equal(progressed.action, "none");
  const cooling = decideRecovery({
    samples: [
      { monoMs: 0, busPid, sockets: [stalled] },
      { monoMs: 16_000, busPid, sockets: [stalled] },
    ],
    nowMono: 20_000,
    busPid,
    recentRecycleMonos: [10_000],
  });
  assert.equal(cooling.reason, "recycle_cooldown");
  const budget = decideRecovery({
    samples: [
      { monoMs: 0, busPid, sockets: [stalled] },
      { monoMs: 16_000, busPid, sockets: [stalled] },
    ],
    nowMono: 40_000,
    busPid,
    recentRecycleMonos: [0, 1_000, 2_000],
  });
  assert.equal(budget.reason, "recycle_cooldown");
  assert.equal(budget.cooldown_ms, 90_000);
  const afterBudget = decideRecovery({
    samples: [
      { monoMs: 0, busPid, sockets: [stalled] },
      { monoMs: 100_000, busPid, sockets: [stalled] },
    ],
    nowMono: 100_000,
    busPid,
    recentRecycleMonos: [0, 1_000, 2_000],
  });
  assert.equal(afterBudget.reason, "half_open_confirmed");
});

test("clock jump does not age out events or change restart backoff", () => {
  const jumped = nextClockSample({ wallMs: 0, monoMs: 0 }, 120_000, 1_000);
  assert.equal(jumped.suspect, true);
  assert.equal(jumped.jumpedForward, true);
  const backward = nextClockSample({ wallMs: 50_000, monoMs: 1_000 }, 40_000, 2_000);
  assert.equal(backward.jumpedBackward, true);
  assert.equal(shouldDropForAge({ createTimeRaw: 1, wallNow: 11 * 60 * 1000, clockSuspect: true }), false);
  assert.equal(shouldDropForAge({ createTimeRaw: 1, wallNow: 11 * 60 * 1000, clockSuspect: false }), true);
  assert.equal(restartDelayMs(1), 1000);
  assert.equal(restartDelayMs(6), 30_000);
});

test("transport connected is not e2e proven", () => {
  const transport = transportFrom({
    decision: { action: "none" },
    hasConsume: true,
    hasBus: true,
    samples: [{ sockets: [parseSsTcp(HEALTHY_IDLE)[0]] }],
  });
  assert.equal(transport.state, "connected");
  assert.equal(e2eFrom({ pending: 0, wakeAcceptedSinceRecycle: false }), "unproven");
  assert.equal(e2eFrom({ pending: 2, failCount: 1 }), "wake_failing");
  assert.equal(e2eFrom({ pending: 0, wakeAcceptedSinceRecycle: true }), "wake_accepted");
});

test("hung probe resolves on the bound instead of waiting forever", async () => {
  const result = await runBounded(() => new Promise(() => {}), 30);
  assert.equal(result.hung, true);
});

test("bus log reader ignores history and emits only lines appended after open", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bus-log-"));
  const file = path.join(dir, "bus.log");
  fs.writeFileSync(
    file,
    "[SDK ERROR] receive message failed, err: websocket: close 3003: ping timeout[conn_id=secret]\n[SDK] trying to reconnect: 1\n",
  );
  const read = createBusLogReader(file);
  assert.deepEqual(read(10), []);
  fs.appendFileSync(file, "Bus started for app=cli_x pid=42 addr=/tmp/bus.sock\n");
  fs.appendFileSync(file, "[SDK ERROR] receive message failed, err: websocket: close 3003: ping timeout\n");
  const second = read(20);
  assert.equal(second[0].kind, "bus_started");
  assert.equal(second[0].pid, 42);
  assert.equal(second[1].kind, "ping_timeout");
  assert.equal("line" in second[1], false);
  assert.equal(parseBusLine("Bus shutting down (idle 30s, no active connections)").kind, "bus_idle_shutdown");
});

test("samplePidSockets classifies injected ss output for one pid", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fake-ss-"));
  const bin = path.join(dir, "ss");
  fs.writeFileSync(bin, `#!/bin/sh\ncat <<'EOF'\n${SENDQ_59}\nEOF\n`);
  fs.chmodSync(bin, 0o755);
  const result = await samplePidSockets(1130155, { command: bin, timeoutMs: 1000 });
  assert.equal(result.sockets.length, 1);
  assert.equal(result.sockets[0].pid, 1130155);
  assert.equal(result.ignored, 1);
  assert.equal(classifySocket(result.sockets[0]), "half_open_candidate");
});
