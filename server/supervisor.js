import {
  classifySocket,
  decideRecovery,
  e2eFrom,
  formatSocketEvidence,
  nextClockSample,
  planConsumeExit,
  restartDelayMs,
  runBounded,
  selectCurrentSockets,
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
  let wakeAcceptedSinceRecycle = false;
  let connectedMono = null;
  let busStartedMono = null;
  let recentRecycleMonos = [];
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
    wakeAcceptedSinceRecycle = true;
  }

  function noteWsConnected(mono = deps.nowMono()) {
    connectedMono = mono;
  }

  function snapshot() {
    const pending = deps.pendingCount?.() || 0;
    const decision = decideRecovery({
      samples,
      busEvents,
      nowMono: deps.nowMono(),
      busPid,
      generation,
      connectedMono,
      busStartedMono,
      recentRecycleMonos,
    });
    const transport = transportFrom({
      decision: decision.action === "recycle_full" ? { action: "none" } : decision,
      samples,
      probeHangs,
      hasBus: Boolean(busPid),
      hasConsume: Boolean(consume?.pid),
    });
    const e2e = e2eFrom({ pending, failCount: lastFailCount, wakeAcceptedSinceRecycle });
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
      wakeAcceptedSinceRecycle,
      reply_e2e: "unproven",
      heartbeatSeq,
    };
  }

  function rememberBus(pid, mono) {
    if (!pid || pid === deps.selfPid) return;
    if (deps.isAlive && deps.isAlive(pid) === false) return;
    const role = identifyProcess(pid, deps.readCmdline?.(pid) || "");
    if (role !== "bus") return;
    if (pid === busPid) return;
    busPid = pid;
    busStartedMono = mono ?? deps.nowMono();
    connectedMono = null;
    samples = samples.filter((sample) => sample.busPid === pid);
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
            if (result?.wakeOk) wakeAcceptedSinceRecycle = true;
            if (result?.failCount) lastFailCount = result.failCount;
          })
          .catch((error) => log(`event error ${error instanceof Error ? error.message : String(error)}`));
      },
      onExit: (code, signal) => {
        void noteExit(gen, code, signal);
      },
      onStderr: (line) => {
        if (gen !== generation) return;
        if (/websocket:\s*connected/i.test(line)) noteWsConnected();
      },
    });
    consume = { ...handle, gen };
    startedConsume = true;
    return consume;
  }

  async function doRecycle(decision) {
    if (stopped) return;
    const reason = typeof decision === "string" ? decision : decision?.reason || "recycle";
    const evidence = typeof decision === "string" ? "" : decision?.evidence || "";
    recycleCount += 1;
    wakeAcceptedSinceRecycle = false;
    samples = [];
    busEvents = [];
    probeHangs = 0;
    connectedMono = null;
    busStartedMono = null;
    const now = deps.nowMono();
    recentRecycleMonos = recentRecycleMonos.filter((stamp) => now - stamp < LIMITS.recycleBudgetWindowMs);
    recentRecycleMonos.push(now);
    log(
      `recycle_full reason=${reason} consume=${consume?.pid || 0} bus=${busPid || 0} generation=${generation} ${evidence}`.trim(),
    );
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
      if (busPid && deps.sampleSockets && deps.isAlive?.(busPid) !== false) {
        const probe = await runBounded(() => deps.sampleSockets(busPid), deps.probeTimeoutMs ?? 8000);
        if (!probe.hung && !probe.value?.error) {
          const scoped = selectCurrentSockets(probe.value?.sockets || [], busPid);
          samples.push({ monoMs: now, busPid, generation, sockets: scoped.kept });
          if (samples.length > 8) samples.shift();
        }
      }
      const plan = planConsumeExit({
        samples,
        busEvents,
        nowMono: now,
        busAlive: Boolean(busPid) && deps.isAlive?.(busPid) !== false,
        busPid,
        generation,
        connectedMono,
        busStartedMono,
        recentRecycleMonos,
      });
      if (plan.action === "recycle_full") {
        await doRecycle(plan);
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

      if (busPid && deps.isAlive && deps.isAlive(busPid) === false) {
        busPid = 0;
        samples = [];
        busStartedMono = null;
        connectedMono = null;
      }
      if (consume?.pid && deps.readChildren && deps.readCmdline) {
        for (const child of descendantPids(consume.pid, deps.readChildren)) {
          rememberBus(child, nowMono);
        }
      }
      if (deps.readNewBusEvents) {
        const fresh = await deps.readNewBusEvents(nowMono);
        if (fresh?.length) {
          busEvents.push(
            ...fresh.map((event) => ({ ...event, monoMs: event.monoMs ?? nowMono, generation })),
          );
          if (busEvents.length > 80) busEvents = busEvents.slice(-80);
          for (const event of fresh) {
            if (event.kind === "bus_started") rememberBus(event.pid, event.monoMs ?? nowMono);
          }
        }
      }

      let probeHungThisTick = false;
      if (busPid && deps.sampleSockets && deps.isAlive?.(busPid) !== false) {
        const probe = await runBounded(() => deps.sampleSockets(busPid), deps.probeTimeoutMs ?? 8000);
        if (probe.hung) {
          probeHangs += 1;
          probeHungThisTick = true;
        } else if (probe.value?.error) {
          probeHangs = 0;
          log(`probe error ${probe.value.error}`);
        } else {
          probeHangs = 0;
          const raw = probe.value?.sockets || [];
          const scoped = selectCurrentSockets(raw, busPid);
          if (scoped.ignored.length || probe.value?.ignored) {
            const counts = {};
            for (const item of scoped.ignored) counts[item.reason] = (counts[item.reason] || 0) + 1;
            const summary = Object.entries(counts)
              .map(([key, count]) => `${key}:${count}`)
              .join(",");
            log(
              `probe_scope bus=${busPid} generation=${generation} kept=${scoped.kept.length} ignored=${summary || `unscoped:${probe.value?.ignored || 0}`}`,
            );
          }
          for (const sock of scoped.kept) {
            const kind = classifySocket(sock);
            if (kind === "half_open_candidate" || kind === "half_open_mild") {
              log(`probe_candidate bus=${busPid} generation=${generation} ${formatSocketEvidence(sock)}`);
            }
          }
          samples.push({ monoMs: nowMono, busPid, generation, sockets: scoped.kept });
          if (samples.length > 8) samples.shift();
        }
      }

      const recoveryInput = {
        samples,
        busEvents,
        nowMono,
        busPid,
        generation,
        connectedMono,
        busStartedMono,
        recentRecycleMonos,
      };
      let decision = decideRecovery(recoveryInput);
      if (
        probeHangs >= 2 &&
        decision.action === "none" &&
        decision.reason !== "recycle_cooldown" &&
        decision.reason !== "connect_grace"
      ) {
        decision = {
          action: "recycle_full",
          reason: "probe_hung",
          evidence: `probe_hangs=${probeHangs}`,
          rotation_is_health: false,
        };
      }
      if (!probeHungThisTick && decision.action === "none" && probeHangs === 1) {
        log(`probe_hang bus=${busPid} generation=${generation} count=1`);
      }

      if (decision.action === "recycle_full") {
        await doRecycle(decision);
      }

      if (deps.flushWakes) {
        const flushed = await deps.flushWakes();
        if (flushed?.okCount > 0) wakeAcceptedSinceRecycle = true;
        lastFailCount = flushed?.failCount || 0;
      }

      heartbeatSeq += 1;
      const view = snapshot();
      deps.writeHealth?.({
        pid: deps.selfPid || process.pid,
        boot_uptime_ms: deps.readUptimeMs?.() ?? null,
        monitor_age_ms: deps.processAgeMs?.(deps.selfPid || process.pid, deps.readUptimeMs?.() ?? nowMono) ?? null,
        seq: heartbeatSeq,
        transport: view.transport,
        e2e: {
          state: view.e2e,
          note: "wake_accepted is webhook HTTP only, not a Feishu reply",
          reply_e2e: "unproven",
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
        if (flushed?.okCount > 0) wakeAcceptedSinceRecycle = true;
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
