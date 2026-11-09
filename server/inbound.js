import { shouldDropForAge } from "./transport.js";

export async function deliverEvent({
  line,
  normalize,
  isAllowed,
  state,
  clockSuspect = false,
  wallNow = Date.now(),
  maxAgeMs,
  queue,
  log = () => {},
} = {}) {
  let raw = line;
  if (typeof line === "string") {
    const trimmed = line.trim();
    if (!trimmed) return { status: "empty" };
    try {
      raw = JSON.parse(trimmed);
    } catch {
      log(`skip non-json: ${trimmed.slice(0, 80)}`);
      return { status: "non_json" };
    }
  }
  const msg = normalize?.(raw);
  if (!msg?.message_id) {
    log("skip event without message_id");
    return { status: "no_id" };
  }
  if (
    shouldDropForAge({
      createTimeRaw: raw?.create_time || raw?.timestamp,
      wallNow,
      clockSuspect,
      maxAgeMs,
    })
  ) {
    log(`skip-old ${msg.message_id}`);
    return { status: "too_old" };
  }
  if (isAllowed && !isAllowed(state, msg.sender_id)) {
    log(`skip-allowlist ${msg.sender_id}`);
    return { status: "allowlist" };
  }
  const accepted = queue.accept(msg);
  if (accepted.status === "queued") log(`inbox +1 ${msg.message_id} from=${msg.sender_id}`);
  if (accepted.status === "queued" || accepted.status === "duplicate_pending") {
    const flushed = await queue.flush();
    return { ...accepted, ...flushed, wakeOk: flushed.okCount > 0 };
  }
  return { ...accepted, okCount: 0, failCount: 0, wakeOk: false };
}
