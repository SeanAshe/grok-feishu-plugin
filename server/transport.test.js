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
  assert.equal(classifySocket(sockets[0]), "half_open_severe");
  assert.equal(classifySocket(sockets[1]), "half_open_severe");
  const idle = parseSsTcp(HEALTHY_IDLE)[0];
  assert.equal(idle.sendQ, 0);
  assert.equal(idle.rto, 204);
  assert.equal(classifySocket(idle), "idle_or_healthy");
  assert.equal(decideRecovery({ samples: [{ monoMs: 0, sockets: [idle] }], nowMono: 0 }).action, "none");
});

test("severe half-open recycles immediately and mild half-open waits for a bounded second sample", () => {
  const severe = parseSsTcp(SENDQ_59)[0];
  assert.equal(
    decideRecovery({ samples: [{ monoMs: 0, sockets: [severe] }], nowMono: 0 }).reason,
    "half_open_severe",
  );
  const early = decideRecovery({
    samples: [
      { monoMs: 0, sockets: [MILD] },
      { monoMs: 5_000, sockets: [MILD] },
    ],
    nowMono: 5_000,
  });
  assert.equal(early.action, "none");
  const persisted = decideRecovery({
    samples: [
      { monoMs: 0, sockets: [MILD] },
      { monoMs: 16_000, sockets: [{ ...MILD, bytesSent: 1200 }] },
    ],
    nowMono: 16_000,
  });
  assert.equal(persisted.reason, "half_open_persistent");
  const healed = decideRecovery({
    samples: [
      { monoMs: 0, sockets: [MILD] },
      { monoMs: 16_000, sockets: [{ ...MILD, sendQ: 0, rto: 204, backoff: 0, bytesRetrans: 0 }] },
    ],
    nowMono: 16_000,
  });
  assert.equal(healed.action, "none");
});

test("ping timeout budget is monotonic and reconnect attempts do not count as recovery", () => {
  const ping = [{ kind: "ping_timeout", monoMs: 0 }];
  assert.equal(decideRecovery({ busEvents: ping, nowMono: 44_000 }).action, "none");
  const stillTrying = decideRecovery({
    busEvents: [
      ...ping,
      { kind: "reconnect_attempt", monoMs: 10_000 },
      { kind: "consumer_connected", monoMs: 20_000, pid: 9 },
    ],
    nowMono: 46_000,
  });
  assert.equal(stillTrying.reason, "ping_timeout_unrecovered");
  const restarted = decideRecovery({
    busEvents: [...ping, { kind: "reconnect_attempt", monoMs: 10_000 }, { kind: "bus_started", monoMs: 12_000, pid: 3 }],
    nowMono: 90_000,
  });
  assert.equal(restarted.action, "none");
  const healthyAfter = decideRecovery({
    busEvents: ping,
    samples: [{ monoMs: 50_000, sockets: [parseSsTcp(HEALTHY_IDLE)[0]] }],
    nowMono: 50_000,
  });
  assert.equal(healthyAfter.action, "none");
});

test("consumer exit reuses a healthy bus without calling rotation health, and recycles a half-open bus", () => {
  const healthy = planConsumeExit({
    busAlive: true,
    nowMono: 0,
    samples: [{ monoMs: 0, sockets: [parseSsTcp(HEALTHY_IDLE)[0]] }],
  });
  assert.equal(healthy.action, "restart_consume_only");
  assert.equal(healthy.rotation_is_health, false);
  const sick = planConsumeExit({
    busAlive: true,
    nowMono: 0,
    samples: [{ monoMs: 0, sockets: [parseSsTcp(SENDQ_59)[0]] }],
  });
  assert.equal(sick.action, "recycle_full");
  assert.equal(sick.reason, "half_open_severe");
  assert.equal(planConsumeExit({ busAlive: false, nowMono: 0 }).reason, "bus_not_alive");
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
  assert.equal(e2eFrom({ pending: 0, provenSinceRecycle: false }), "unproven");
  assert.equal(e2eFrom({ pending: 2, failCount: 1 }), "failing");
  assert.equal(e2eFrom({ pending: 0, provenSinceRecycle: true }), "proven");
});

test("hung probe resolves on the bound instead of waiting forever", async () => {
  const result = await runBounded(() => new Promise(() => {}), 30);
  assert.equal(result.hung, true);
});

test("bus log reader emits structured events without keeping raw lines", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bus-log-"));
  const file = path.join(dir, "bus.log");
  fs.writeFileSync(
    file,
    "[SDK ERROR] receive message failed, err: websocket: close 3003: ping timeout[conn_id=secret]\n[SDK] trying to reconnect: 1\n",
  );
  const read = createBusLogReader(file);
  const first = read(10);
  assert.deepEqual(
    first.map((event) => event.kind),
    ["ping_timeout", "reconnect_attempt"],
  );
  assert.equal("line" in first[0], false);
  fs.appendFileSync(file, "Bus started for app=cli_x pid=42 addr=/tmp/bus.sock\n");
  const second = read(20);
  assert.equal(second[0].kind, "bus_started");
  assert.equal(second[0].pid, 42);
  assert.equal(parseBusLine("Bus shutting down (idle 30s, no active connections)").kind, "bus_idle_shutdown");
});

test("samplePidSockets classifies injected ss output for one pid", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fake-ss-"));
  const bin = path.join(dir, "ss");
  fs.writeFileSync(bin, `#!/bin/sh\ncat <<'EOF'\n${SENDQ_59}\nEOF\n`);
  fs.chmodSync(bin, 0o755);
  const result = await samplePidSockets(1130155, { command: bin, timeoutMs: 1000 });
  assert.equal(result.sockets.length, 2);
  assert.equal(classifySocket(result.sockets[0]), "half_open_severe");
});
