"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const { BloomFilter, LoomStore } = require("../dist");
const { inspectSstable } = require("../dist/sstable");

async function tempStore(options) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "loom-"));
  return { dir, store: await LoomStore.open(dir, options) };
}

async function manifest(dir) {
  return JSON.parse(await fs.readFile(path.join(dir, "MANIFEST.json"), "utf8"));
}

test("put and get values from memtable and block-based SSTables", async () => {
  const { store } = await tempStore({ memtableLimit: 2, level0CompactionThreshold: 99, blockSize: 1 });
  await store.put("a", "alpha");
  assert.equal(await store.get("a"), "alpha");
  await store.put("b", { value: 2 });
  assert.deepEqual(await store.get("b"), { value: 2 });
  assert.equal(store.stats().levels[0].tables, 1);
  await store.close();
});

test("recovers unflushed WAL records and advances sequence numbers after reopen", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 100 });
  await store.put("session", "alive");

  const recovered = await LoomStore.open(dir, { memtableLimit: 100 });
  assert.equal(await recovered.get("session"), "alive");
  await recovered.put("next", "seq-ok");
  assert.equal(recovered.stats().nextSeq, 3);
  await recovered.close();
  void store;
});

test("ignores a torn trailing WAL line during recovery", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 100 });
  await store.put("safe", "record");
  await fs.appendFile(path.join(dir, "wal.log"), "{\"partial\":", "utf8");

  const recovered = await LoomStore.open(dir);
  assert.equal(await recovered.get("safe"), "record");
  await recovered.close();
});

test("newer SSTables override older values using monotonic sequence numbers", async () => {
  const { store } = await tempStore({ memtableLimit: 1, level0CompactionThreshold: 99 });
  await store.put("k", "v1");
  await store.put("k", "v2");
  assert.equal(await store.get("k"), "v2");
  assert.equal(store.stats().levels[0].tables, 2);
  await store.close();
});

test("delete writes a tombstone and major compaction removes deleted keys", async () => {
  const { store } = await tempStore({ memtableLimit: 1, level0CompactionThreshold: 99 });
  await store.put("gone", "soon");
  await store.delete("gone");
  assert.equal(await store.get("gone"), undefined);
  await store.compact();
  assert.equal(await store.get("gone"), undefined);
  assert.equal(store.stats().diskEntries, 0);
  await store.close();
});

test("range scan returns sorted latest live values", async () => {
  const { store } = await tempStore({ memtableLimit: 2, level0CompactionThreshold: 99, blockSize: 2 });
  await store.put("user:1", "old");
  await store.put("user:2", "two");
  await store.put("user:1", "new");
  await store.put("user:3", "three");
  await store.delete("user:2");

  assert.deepEqual(await store.scan({ start: "user:1", end: "user:9" }), [
    { key: "user:1", value: "new" },
    { key: "user:3", value: "three" },
  ]);
  await store.close();
});

test("automatic leveled compaction promotes level-0 tables to level 1", async () => {
  const { store } = await tempStore({ memtableLimit: 1, level0CompactionThreshold: 3, blockSize: 1 });
  await store.put("a", 1);
  await store.put("b", 2);
  await store.put("c", 3);

  const stats = store.stats();
  assert.equal(stats.levels[0].tables, 0);
  assert.equal(stats.levels[1].tables, 1);
  assert.equal(await store.get("b"), 2);
  await store.close();
});

test("concurrent writes are serialized through the write queue", async () => {
  const { store } = await tempStore({ memtableLimit: 50 });
  await Promise.all(Array.from({ length: 25 }, (_, i) => store.put(`k${String(i).padStart(2, "0")}`, i)));
  const rows = await store.scan({ start: "k00", end: "k99" });
  assert.equal(rows.length, 25);
  assert.deepEqual(rows[0], { key: "k00", value: 0 });
  assert.deepEqual(rows[24], { key: "k24", value: 24 });
  await store.close();
});

test("SSTable block checksums detect corrupted data blocks", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 1, level0CompactionThreshold: 99, blockSize: 1 });
  await store.put("checksum", "protected");
  const data = await manifest(dir);
  const table = data.levels[0][0];
  const footer = await inspectSstable(dir, table);
  const file = path.join(dir, table.file);
  const handle = await fs.open(file, "r+");
  try {
    const byte = Buffer.alloc(1);
    await handle.read(byte, 0, 1, footer.blocks[0].offset);
    byte[0] = byte[0] ^ 0xff;
    await handle.write(byte, 0, 1, footer.blocks[0].offset);
  } finally {
    await handle.close();
  }

  await assert.rejects(() => store.get("checksum"), /checksum mismatch/);
});

test("bloom filter has no false negatives for inserted keys", () => {
  const bloom = new BloomFilter(128, 3);
  bloom.add("alpha");
  bloom.add("beta");
  assert.equal(bloom.mightContain("alpha"), true);
  assert.equal(bloom.mightContain("beta"), true);
});
