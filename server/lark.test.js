import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { eventConsumeArgs, spawnEventConsume } from "./lark.js";

test("event consume does not use a timeout to rotate consumers", () => {
  assert.deepEqual(eventConsumeArgs(), ["event", "consume", "im.message.receive_v1", "--as", "bot"]);
  assert.equal(eventConsumeArgs().includes("--timeout"), false);
});

test("consume keeps stdin open until the monitor closes it", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fake-lark-"));
  const record = path.join(dir, "argv.json");
  const bin = path.join(dir, "lark-cli");
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const fs = require("node:fs");
fs.writeFileSync(process.env.ARGV_OUT, JSON.stringify(process.argv.slice(2)));
process.stdin.resume();
process.stdin.on("end", () => process.exit(0));
process.stdin.on("close", () => process.exit(0));
setInterval(() => {}, 1000);
`,
  );
  fs.chmodSync(bin, 0o755);
  const previous = process.env.LARK_CLI_PATH;
  process.env.LARK_CLI_PATH = bin;
  process.env.ARGV_OUT = record;
  let exitCode = "pending";
  const handle = spawnEventConsume({
    onExit: (code) => {
      exitCode = code;
    },
  });
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("fake lark-cli did not record argv")), 1500);
      const wait = () => {
        if (fs.existsSync(record)) {
          clearTimeout(timer);
          resolve();
          return;
        }
        setTimeout(wait, 20);
      };
      wait();
    });
    assert.deepEqual(JSON.parse(fs.readFileSync(record, "utf8")), eventConsumeArgs());
    assert.equal(exitCode, "pending");
    handle.closeStdin();
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("stdin close did not exit consume")), 1500);
      const wait = () => {
        if (exitCode !== "pending") {
          clearTimeout(timer);
          resolve();
          return;
        }
        setTimeout(wait, 20);
      };
      wait();
    });
    assert.equal(exitCode, 0);
  } finally {
    try {
      process.kill(-handle.pid, "SIGKILL");
    } catch {
      try {
        process.kill(handle.pid, "SIGKILL");
      } catch {
        // already exited
      }
    }
    if (previous == null) delete process.env.LARK_CLI_PATH;
    else process.env.LARK_CLI_PATH = previous;
    delete process.env.ARGV_OUT;
  }
});
