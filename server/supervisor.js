import {
  decideRecovery,
  e2eFrom,
  nextClockSample,
  planConsumeExit,
  restartDelayMs,
  runBounded,
  transportFrom,
  LIMITS,
} from "./transport.js";
import { descendantPids, identifyProcess } from "./procguard.js";

function countRecent(stamps, nowMono) {
  return stamps.filter((stamp) => nowMono - stamp < 60_000).length;
}

export function createSupervisor(deps) {
  const log = deps.log || (() => {});
  let chain = Promise.resolve();
  let stopped = false;
  let consume = null;
  let busPid = 0;
  let generation = 0;
  let samples = [];
  let busEvents = [];
  let probeHangs = 0;
  let clockState = null;
  let clockSuspectUntil = 0;
  let heartbeatSeq = 0;
  let recycleCount = 0;
  let provenSinceRecycle = false;
  let exitStamps = [];
  let lastRestartDelayMs = 0;
  let lastFailCount = 0;
  let recycleReason = "";
  let recycleQueued = false;
  let startedConsume = false;

  function schedule(fn) {
    const run = chain.then(fn, fn);
    chain = run.then(
      () => {},
      () => {},
    );
    return run;
  }

  function isClockSuspect(nowMono = deps.nowMono()) {
    return nowMono < clockSuspectUntil;
  }

  function noteWakeOk() {
    provenSinceRecycle = true;
  }

  function snapshot() {
    const pending = deps.pendingCount?.() || 0;
    const decision = decideRecovery({ samples, busEvents, nowMono: deps.nowMono() });
    const transport = transportFrom({
      decision: decision.action === "recycle_full" ? { action: "none" } : decision,
      samples,
      probeHangs,
      hasBus: Boolean(busPid),
      hasConsume: Boolean(consume?.pid),
    });
    const e2e = e2eFrom({ pending, failCount: lastFailCount, provenSinceRecycle });
    return {
      stopped,
      consumePid: consume?.pid || 0,
      busPid,
      generation,
      recycleCount,
      probeHangs,
      clockSuspect: isClockSuspect(),
      transport,
      e2e,
      pending,
      lastRestartDelayMs,
      rotation_is_health: false,
      provenSinceRecycle,
      heartbeatSeq,
    };
  }

  function rememberBus(pid) {
    if (!pid || pid === deps.selfPid) return;
    const role = identifyProcess(pid, deps.readCmdline?.(pid) || "");
    if (role === "bus") busPid = pid;
  }

  async function signalCurrent(sig, { onlyIfAlive = false } = {}) {
    const requests = [];
    if (consume?.pid && (!onlyIfAlive || deps.isAlive?.(consume.pid))) {
      requests.push({ pid: consume.pid, role: "consume", sig, group: true });
    }
    if (busPid && (!onlyIfAlive || deps.isAlive?.(busPid))) {
      requests.push({ pid: busPid, role: "bus", sig });
    }
    if (!requests.length || !deps.signalOwned) return { done: [], refused: [] };
    return deps.signalOwned(requests);
  }

  async function spawnLocked() {
    const gen = ++generation;
    const handle = await deps.spawnConsume({
      onLine: (line) => {
        const seenGen = gen;
        void Promise.resolve()
          .then(() => deps.onEventLine?.(line))
          .then((result) => {
            if (stopped || seenGen !== consume?.gen) return;
            if (result?.wakeOk) provenSinceRecycle = true;
            if (result?.failCount) lastFailCount = result.failCount;
          })
          .catch((error) => log(`event error ${error instanceof Error ? error.message : String(error)}`));
      },
      onExit: (code, signal) => {
        void noteExit(gen, code, signal);
      },
    });
    consume = { ...handle, gen };
    startedConsume = true;
    return consume;
  }

  async function doRecycle(reason) {
    if (stopped) return;
    recycleCount += 1;
    provenSinceRecycle = false;
    samples = [];
    busEvents = [];
    probeHangs = 0;
    log(`recycle_full ${reason} consume=${consume?.pid || 0} bus=${busPid || 0}`);
    await signalCurrent("SIGTERM");
    if (consume?.closeStdin) {
      try {
        consume.closeStdin();
      } catch {
        // already closed
      }
    }
    await deps.sleep?.(deps.graceMs ?? 0);
    await signalCurrent("SIGKILL", { onlyIfAlive: true });
    consume = null;
    busPid = 0;
    const now = deps.nowMono();
    exitStamps = exitStamps.filter((stamp) => now - stamp < 60_000);
    exitStamps.push(now);
    lastRestartDelayMs = restartDelayMs(countRecent(exitStamps, now));
    await deps.sleep?.(lastRestartDelayMs);
    if (stopped) return;
    await spawnLocked();
  }

  function requestRecycle(reason) {
    recycleReason = reason;
    if (recycleQueued) return chain;
    recycleQueued = true;
    return schedule(async () => {
      recycleQueued = false;
      const why = recycleReason;
      recycleReason = "";
      if (!why || stopped) return snapshot();
      await doRecycle(why);
      return snapshot();
    });
  }

  async function noteExit(gen, code, signal) {
    return schedule(async () => {
      if (stopped || !consume || gen !== consume.gen) return;
      log(`lark-cli exited code=${code} signal=${signal}`);
      const now = deps.nowMono();
      exitStamps = exitStamps.filter((stamp) => now - stamp < 60_000);
      if (busPid && deps.sampleSockets) {
        const probe = await runBounded(() => deps.sampleSockets(busPid), deps.probeTimeoutMs ?? 8000);
        if (!probe.hung && !probe.value?.error) {
          samples.push({ monoMs: now, sockets: probe.value?.sockets || [] });
          if (samples.length > 8) samples.shift();
        }
      }
      const plan = planConsumeExit({
        samples,
        busEvents,
        nowMono: now,
        busAlive: Boolean(busPid) && deps.isAlive?.(busPid) !== false,
      });
      if (plan.action === "recycle_full") {
        await doRecycle(plan.reason);
        return;
      }
      exitStamps.push(now);
      lastRestartDelayMs = restartDelayMs(countRecent(exitStamps, now));
      log(`restart consume only delay=${lastRestartDelayMs} ${plan.reason}`);
      await deps.sleep?.(lastRestartDelayMs);
      if (stopped) return;
      await spawnLocked();
    });
  }

  async function tick() {
    return schedule(async () => {
      if (stopped) return snapshot();
      const nowMono = deps.nowMono();
      const nowWall = deps.nowWall();
      clockState = nextClockSample(clockState, nowWall, nowMono);
      if (clockState.suspect) clockSuspectUntil = nowMono + LIMITS.clockSuspectHoldMs;

      if (consume?.pid && deps.readChildren && deps.readCmdline) {
        for (const child of descendantPids(consume.pid, deps.readChildren)) {
          rememberBus(child);
        }
      }
      if (deps.readNewBusEvents) {
        const fresh = await deps.readNewBusEvents(nowMono);
        if (fresh?.length) {
          busEvents.push(...fresh.map((event) => ({ ...event, monoMs: event.monoMs ?? nowMono })));
          if (busEvents.length > 80) busEvents = busEvents.slice(-80);
          for (const event of fresh) {
            if (event.kind === "bus_started") rememberBus(event.pid);
          }
        }
      }

      let probeHungThisTick = false;
      if (busPid && deps.sampleSockets) {
        const probe = await runBounded(() => deps.sampleSockets(busPid), deps.probeTimeoutMs ?? 8000);
        if (probe.hung) {
          probeHangs += 1;
          probeHungThisTick = true;
        } else if (probe.value?.error) {
          probeHangs = 0;
          log(`probe error ${probe.value.error}`);
        } else {
          probeHangs = 0;
          const sockets = probe.value?.sockets || [];
          samples.push({ monoMs: nowMono, sockets });
          if (samples.length > 8) samples.shift();
        }
      }

      let decision = decideRecovery({ samples, busEvents, nowMono });
      if (probeHangs >= 2) decision = { action: "recycle_full", reason: "probe_hung", rotation_is_health: false };
      if (!probeHungThisTick && decision.action === "none" && probeHangs === 1) {
        // a single hung probe is not yet a recycle
      }

      if (decision.action === "recycle_full") {
        await doRecycle(decision.reason);
      }

      if (deps.flushWakes) {
        const flushed = await deps.flushWakes();
        if (flushed?.okCount > 0) provenSinceRecycle = true;
        lastFailCount = flushed?.failCount || 0;
      }

      heartbeatSeq += 1;
      const view = snapshot();
      deps.writeHealth?.({
        pid: deps.selfPid || process.pid,
        uptime_ms: deps.readUptimeMs?.() ?? nowMono,
        seq: heartbeatSeq,
        transport: view.transport,
        e2e: {
          state: view.e2e,
          note: "transport_connected is not end-to-end delivery",
        },
        pending_wakes: view.pending,
        clock_suspect: view.clockSuspect,
        recycle_count: recycleCount,
        rotation_is_health: false,
        consume_pid: view.consumePid,
        bus_pid: view.busPid,
        process_ready: true,
      });
      return view;
    });
  }

  async function start() {
    const lock = deps.acquireLock?.();
    if (lock && lock.acquired === false) {
      log(`monitor lock not acquired: ${lock.reason}`);
      return { started: false, reason: lock.reason, holderPid: lock.holderPid || 0 };
    }
    deps.writePid?.(deps.selfPid || process.pid);
    await schedule(async () => {
      if (!startedConsume) await spawnLocked();
    });
    if (deps.flushWakes) {
      await schedule(async () => {
        const flushed = await deps.flushWakes();
        if (flushed?.okCount > 0) provenSinceRecycle = true;
        lastFailCount = flushed?.failCount || 0;
      });
    }
    return { started: true };
  }

  async function stop() {
    stopped = true;
    return schedule(async () => {
      await signalCurrent("SIGTERM");
      if (consume?.closeStdin) {
        try {
          consume.closeStdin();
        } catch {
          // ignore
        }
      }
      await deps.sleep?.(deps.graceMs ?? 0);
      await signalCurrent("SIGKILL", { onlyIfAlive: true });
      consume = null;
      busPid = 0;
      deps.clearPid?.();
      deps.releaseLock?.();
      return snapshot();
    });
  }

  function idle() {
    return schedule(async () => snapshot());
  }

  return {
    start,
    tick,
    stop,
    requestRecycle,
    snapshot,
    idle,
    isClockSuspect,
    noteWakeOk,
  };
}
