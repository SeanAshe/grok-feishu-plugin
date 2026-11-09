import fs from "node:fs";
import path from "node:path";

function writeAtomic(file, data) {
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data));
  fs.renameSync(tmp, file);
}

export function createWakeQueue({
  pendingPath,
  readProcessed,
  markProcessed,
  appendInbox,
  inboxHas,
  fetchImpl,
  log = () => {},
  nowMono = () => performance.now(),
  timeoutMs = 10_000,
} = {}) {
  if (!pendingPath) throw new Error("pendingPath required");
  if (typeof fetchImpl !== "function") throw new Error("fetchImpl required");

  let items = [];
  try {
    if (fs.existsSync(pendingPath)) {
      const parsed = JSON.parse(fs.readFileSync(pendingPath, "utf8"));
      if (Array.isArray(parsed)) items = parsed;
    }
  } catch {
    items = [];
  }

  let chain = Promise.resolve();

  function save() {
    writeAtomic(pendingPath, items);
  }

  function accept(message) {
    const id = message?.message_id;
    if (!id) return { status: "ignored" };
    if (readProcessed?.(id)) return { status: "duplicate_processed" };
    if (items.some((item) => item.message_id === id)) return { status: "duplicate_pending" };
    const inInbox = inboxHas?.(id);
    items.push({ message_id: id, message, attempts: 0, next_mono: 0, last_error: "" });
    save();
    if (!inInbox) appendInbox?.([message]);
    return { status: "queued" };
  }

  async function flushOnce() {
    let okCount = 0;
    let failCount = 0;
    const now = nowMono();
    for (const item of [...items]) {
      if (readProcessed?.(item.message_id)) {
        items = items.filter((candidate) => candidate.message_id !== item.message_id);
        save();
        continue;
      }
      if (item.next_mono && now < item.next_mono) continue;
      item.attempts += 1;
      save();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const res = await fetchImpl(item.message, { timeoutMs, signal: controller.signal });
        if (!res?.ok) {
          const error = new Error(`status ${res?.status ?? "unknown"}`);
          error.status = res?.status;
          throw error;
        }
        markProcessed?.(item.message_id);
        items = items.filter((candidate) => candidate.message_id !== item.message_id);
        save();
        okCount += 1;
        log(`wake ok ${item.message_id}`);
      } catch (error) {
        failCount += 1;
        const name = error?.name || "";
        item.last_error = name === "TimeoutError" || name === "AbortError" ? "timeout" : String(error?.message || error);
        const backoff = Math.min(1000 * 2 ** Math.min(item.attempts, 5), 60_000);
        item.next_mono = nowMono() + backoff;
        save();
        log(`wake retry ${item.message_id} ${item.last_error}`);
      } finally {
        clearTimeout(timer);
      }
    }
    return { okCount, failCount, pending: items.length };
  }

  function flush() {
    const run = chain.then(flushOnce, flushOnce);
    chain = run.then(
      () => {},
      () => {},
    );
    return run;
  }

  return {
    accept,
    flush,
    pendingCount: () => items.length,
    pendingIds: () => items.map((item) => item.message_id),
  };
}
