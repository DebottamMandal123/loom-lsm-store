"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const { LoomStore } = require("../dist");

async function main() {
  const count = Number(process.argv[2] ?? 5000);
  const dir = path.resolve(process.argv[3] ?? ".loom-bench");
  await fs.rm(dir, { recursive: true, force: true });

  const store = await LoomStore.open(dir, {
    memtableLimit: 256,
    level0CompactionThreshold: 4,
    blockSize: 32,
    syncWrites: false,
  });

  const writeStart = process.hrtime.bigint();
  for (let i = 0; i < count; i += 1) {
    await store.put(`key:${String(i).padStart(8, "0")}`, { n: i, payload: `value-${i}` });
  }
  await store.flush();
  const writeMs = Number(process.hrtime.bigint() - writeStart) / 1_000_000;

  const readStart = process.hrtime.bigint();
  for (let i = 0; i < count; i += 1) {
    const key = `key:${String((i * 7919) % count).padStart(8, "0")}`;
    await store.get(key);
  }
  const readMs = Number(process.hrtime.bigint() - readStart) / 1_000_000;

  const scanStart = process.hrtime.bigint();
  const sample = await store.scan({ start: "key:00000010", end: "key:00000030" });
  const scanMs = Number(process.hrtime.bigint() - scanStart) / 1_000_000;

  const stats = store.stats();
  await store.close();

  console.log(JSON.stringify({
    directory: dir,
    writes: {
      count,
      ms: Number(writeMs.toFixed(2)),
      opsPerSecond: Number((count / (writeMs / 1000)).toFixed(2)),
    },
    reads: {
      count,
      ms: Number(readMs.toFixed(2)),
      opsPerSecond: Number((count / (readMs / 1000)).toFixed(2)),
    },
    rangeScan: {
      returned: sample.length,
      ms: Number(scanMs.toFixed(2)),
    },
    stats,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
