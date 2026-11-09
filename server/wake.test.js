import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { deliverEvent } from "./inbound.js";
import { normalizeEvent } from "./lark.js";
import { createWakeQueue } from "./wake.js";

process.env.GROK_FEISHU_HOME = fs.mkdtempSync(path.join(os.tmpdir(), "feishu-home-"));

function tempQueue() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "feishu-wake-"));
  const inbox = path.join(dir, "inbox.jsonl");
  const processed = path.join(dir, "processed.txt");
  const pendingPath = path.join(dir, "wake-pending.json");
  let mono = 0;
  let mode = "ok";
  let calls = 0;
  const queue = createWakeQueue({
    pendingPath,
    nowMono: () => mono,
    timeoutMs: 40,
    readProcessed: (id) =>
      fs.existsSync(processed) && fs.readFileSync(processed, "utf8").split(/\s+/).includes(id),
    markProcessed: (id) => fs.appendFileSync(processed, `${id}\n`),
    appendInbox: (messages) => fs.appendFileSync(inbox, `${messages.map((item) => JSON.stringify(item)).join("\n")}\n`),
    inboxHas: (id) => (fs.existsSync(inbox) ? fs.readFileSync(inbox, "utf8").includes(`"message_id":"${id}"`) : false),
    fetchImpl: (message, { signal }) => {
      calls += 1;
      if (mode === "hang") {
        return new Promise((_, reject) => {
          const fail = () => {
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          };
          if (signal.aborted) fail();
          else signal.addEventListener("abort", fail, { once: true });
        });
      }
      if (mode === "fail") return Promise.resolve({ ok: false, status: 503 });
      return Promise.resolve({ ok: true, status: 200, id: message.message_id });
    },
  });
  return {
    dir,
    inbox,
    processed,
    pendingPath,
    queue,
    calls: () => calls,
    setMode: (next) => {
      mode = next;
    },
    setMono: (next) => {
      mono = next;
    },
  };
}

function event(id, ageMs = Date.now()) {
  return JSON.stringify({
    message_id: id,
    chat_id: "oc_test",
    sender_id: "ou_test",
    content: JSON.stringify({ text: "ping" }),
    create_time: ageMs,
  });
}

async function deliver(ctx, line, extra = {}) {
  return deliverEvent({
    line,
    normalize: normalizeEvent,
    isAllowed: () => true,
    state: { allowFrom: [] },
    queue: ctx.queue,
    wallNow: Date.now(),
    ...extra,
  });
}

test("duplicate events wake once and a crash before success stays pending", async () => {
  const ctx = tempQueue();
  const first = await deliver(ctx, event("om_1"));
  const second = await deliver(ctx, event("om_1"));
  assert.equal(first.status, "queued");
  assert.equal(first.wakeOk, true);
  assert.equal(second.status, "duplicate_processed");
  assert.equal(ctx.calls(), 1);
  const lines = fs.readFileSync(ctx.inbox, "utf8").trim().split("\n");
  assert.equal(lines.length, 1);
  assert.equal(fs.readFileSync(ctx.processed, "utf8").trim(), "om_1");
});

test("pending file written before wake survives a restart", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "feishu-crash-"));
  const pendingPath = path.join(dir, "wake-pending.json");
  const processed = path.join(dir, "processed.txt");
  fs.writeFileSync(
    pendingPath,
    JSON.stringify([
      {
        message_id: "om_crash",
        message: { message_id: "om_crash", sender_id: "ou_test", text: "kept" },
        attempts: 1,
        next_mono: 0,
        last_error: "timeout",
      },
    ]),
  );
  let calls = 0;
  const queue = createWakeQueue({
    pendingPath,
    timeoutMs: 50,
    readProcessed: (id) => fs.existsSync(processed) && fs.readFileSync(processed, "utf8").includes(id),
    markProcessed: (id) => fs.appendFileSync(processed, `${id}\n`),
    appendInbox: () => {},
    inboxHas: () => true,
    fetchImpl: async () => {
      calls += 1;
      return { ok: true, status: 200 };
    },
  });
  const flushed = await queue.flush();
  assert.equal(calls, 1);
  assert.equal(flushed.okCount, 1);
  assert.equal(fs.readFileSync(processed, "utf8").includes("om_crash"), true);
  assert.equal(queue.pendingCount(), 0);
});

test("wake timeout does not mark processed and a later retry can succeed", async () => {
  const ctx = tempQueue();
  ctx.setMode("hang");
  const hung = await deliver(ctx, event("om_slow"));
  assert.equal(hung.wakeOk, false);
  assert.equal(fs.existsSync(ctx.processed), false);
  assert.deepEqual(ctx.queue.pendingIds(), ["om_slow"]);
  ctx.setMode("ok");
  ctx.setMono(120_000);
  const retry = await ctx.queue.flush();
  assert.equal(retry.okCount, 1);
  assert.equal(fs.readFileSync(ctx.processed, "utf8").trim(), "om_slow");
  assert.equal(ctx.queue.pendingCount(), 0);
});

test("http failure stays retryable and is not processed first", async () => {
  const ctx = tempQueue();
  ctx.setMode("fail");
  const failed = await deliver(ctx, event("om_fail"));
  assert.equal(failed.wakeOk, false);
  assert.equal(fs.existsSync(ctx.processed), false);
  ctx.setMode("ok");
  ctx.setMono(120_000);
  const ok = await ctx.queue.flush();
  assert.equal(ok.okCount, 1);
  assert.equal(ctx.calls(), 2);
});

test("clock suspect keeps an otherwise old event", async () => {
  const ctx = tempQueue();
  const dropped = await deliver(ctx, event("om_old", 1), { wallNow: Date.now(), clockSuspect: false });
  assert.equal(dropped.status, "too_old");
  assert.equal(ctx.calls(), 0);
  const kept = await deliver(ctx, event("om_old", 1), { clockSuspect: true });
  assert.equal(kept.wakeOk, true);
  assert.equal(ctx.calls(), 1);
});
