"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const { BloomFilter, LoomStore } = require("../dist");
const { checksum32 } = require("../dist/checksum");
const { inspectSstable } = require("../dist/sstable");

async function tempStore(options) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "loom-"));
  return { dir, store: await LoomStore.open(dir, options) };
}

async function manifest(dir) {
  return JSON.parse(await fs.readFile(path.join(dir, "MANIFEST.json"), "utf8"));
}

async function writeLegacyV2Database(dir) {
  const entries = [{ key: "legacy", value: "v2", seq: 1, deleted: false }];
  const block = Buffer.from(JSON.stringify(entries), "utf8");
  const magic = Buffer.from("LOOMST1\n", "utf8");
  const bloom = new BloomFilter(2048, 4);
  bloom.add("legacy");
  const footerBody = {
    version: 2,
    id: 1,
    level: 0,
    minKey: "legacy",
    maxKey: "legacy",
    count: 1,
    minSeq: 1,
    maxSeq: 1,
    bloom: bloom.toJSON(),
    blocks: [{ firstKey: "legacy", lastKey: "legacy", offset: magic.length, length: block.length, checksum: checksum32(block), count: 1 }],
  };
  const footer = Buffer.from(JSON.stringify(footerBody), "utf8");
  const footerLength = Buffer.alloc(4);
  footerLength.writeUInt32LE(footer.length, 0);
  const payload = Buffer.concat([magic, block, footer, footerLength, Buffer.from("LOOMFT1\n", "utf8")]);
  const table = {
    id: 1,
    file: "sst-000001.json",
    level: 0,
    minKey: "legacy",
    maxKey: "legacy",
    count: 1,
    minSeq: 1,
    maxSeq: 1,
    sizeBytes: payload.length,
  };
  await fs.writeFile(path.join(dir, table.file), payload);
  await fs.writeFile(path.join(dir, "MANIFEST.json"), JSON.stringify({ version: 2, nextFileId: 2, nextSeq: 2, levels: [[table]] }));
  await fs.writeFile(path.join(dir, "wal.log"), "");
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

test("opens legacy v2 SSTables and manifests", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "loom-"));
  await writeLegacyV2Database(dir);
  const store = await LoomStore.open(dir);
  assert.equal(await store.get("legacy"), "v2");
  assert.equal((await store.verify()).entries, 1);
  await store.close();
});

test("recovers unflushed WAL records and advances sequence numbers after reopen", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 100 });
  await store.put("session", "alive");
  await store.close({ flush: false });

  const recovered = await LoomStore.open(dir, { memtableLimit: 100 });
  assert.equal(await recovered.get("session"), "alive");
  await recovered.put("next", "seq-ok");
  assert.equal(recovered.stats().nextSeq, 3);
  await recovered.close();
});

test("repairs a torn WAL tail so later writes remain recoverable", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 100 });
  await store.put("safe", "record");
  await fs.appendFile(path.join(dir, "wal.log"), "{\"partial\":", "utf8");
  await store.close({ flush: false });

  const recovered = await LoomStore.open(dir);
  assert.equal(await recovered.get("safe"), "record");
  assert.equal((await fs.readFile(path.join(dir, "wal.log"), "utf8")).includes("partial"), false);
  await recovered.put("after", "repair");
  await recovered.close({ flush: false });

  const reopened = await LoomStore.open(dir);
  assert.equal(await reopened.get("safe"), "record");
  assert.equal(await reopened.get("after"), "repair");
  await reopened.close();
});

test("prevents two writers from opening the same directory", async () => {
  const { dir, store } = await tempStore();
  await assert.rejects(() => LoomStore.open(dir), /already open/);
  await store.close({ flush: false });
  const reopened = await LoomStore.open(dir);
  await reopened.close();
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

test("uses one bytewise key order for SSTables, point reads, and scans", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 2, blockSize: 2, level0CompactionThreshold: 3 });
  const keys = ["a", "A", "á", "Z", "_", "10", "2"];
  for (const key of keys) await store.put(key, key);
  await store.compact();
  await store.close();

  const reopened = await LoomStore.open(dir);
  for (const key of keys) assert.equal(await reopened.get(key), key);
  const expected = [...keys].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  assert.deepEqual((await reopened.scan()).map((row) => row.key), expected);
  await reopened.close();
});

test("copies values before acknowledging writes", async () => {
  const { store } = await tempStore({ memtableLimit: 100 });
  const value = { nested: { count: 1 } };
  await store.put("object", value);
  value.nested.count = 99;
  assert.deepEqual(await store.get("object"), { nested: { count: 1 } });
  await store.close();
});

test("rejects invalid options and non-JSON values", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "loom-"));
  await assert.rejects(() => LoomStore.open(dir, { blockSize: 0 }), /positive integer/);
  const store = await LoomStore.open(dir);
  await assert.rejects(() => store.put("nan", Number.NaN), /finite/);
  const circular = {};
  circular.self = circular;
  await assert.rejects(() => store.put("circular", circular), /circular/);
  await assert.rejects(() => store.scan({ start: "z", end: "a" }), /greater than end/);
  await store.close();
});

test("flushes when the memtable byte budget is reached", async () => {
  const { store } = await tempStore({ memtableLimit: 100, memtableBytesLimit: 32 });
  await store.put("large", "x".repeat(100));
  assert.equal(store.stats().memtableEntries, 0);
  assert.equal(store.stats().levels[0].tables, 1);
  assert.ok(store.stats().diskBytes > 0);
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

test("verifies every live SSTable and its manifest invariants", async () => {
  const { store } = await tempStore({ memtableLimit: 2, blockSize: 1 });
  await store.put("a", 1);
  await store.put("b", 2);
  const result = await store.verify();
  assert.equal(result.ok, true);
  assert.equal(result.tables, 1);
  assert.equal(result.entries, 2);
  assert.ok(result.bytes > 0);
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
  await store.close();
});

test("SSTable footer checksums detect corrupted metadata", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 1 });
  await store.put("footer", "protected");
  const data = await manifest(dir);
  const file = path.join(dir, data.levels[0][0].file);
  const handle = await fs.open(file, "r+");
  try {
    const stat = await handle.stat();
    const byte = Buffer.alloc(1);
    await handle.read(byte, 0, 1, stat.size - 17);
    byte[0] ^= 0x01;
    await handle.write(byte, 0, 1, stat.size - 17);
  } finally {
    await handle.close();
  }
  await assert.rejects(() => store.get("footer"), /footer checksum mismatch/);
  await store.close();
});

test("removes orphan SSTables and stale atomic-write files on open", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "loom-"));
  await fs.writeFile(path.join(dir, "sst-999999.sst"), "orphan");
  await fs.writeFile(path.join(dir, "MANIFEST.json.tmp-crash"), "temp");
  const store = await LoomStore.open(dir);
  assert.equal(await fs.stat(path.join(dir, "sst-999999.sst")).then(() => true, () => false), false);
  assert.equal(await fs.stat(path.join(dir, "MANIFEST.json.tmp-crash")).then(() => true, () => false), false);
  await store.close();
});

test("does not replay WAL records already installed in an SSTable", async () => {
  const { dir, store } = await tempStore({ memtableLimit: 1 });
  await store.put("durable", "value");
  const record = { op: "put", key: "durable", value: "value", seq: 1 };
  const line = JSON.stringify({ record, checksum: checksum32(JSON.stringify(record)) });
  await fs.appendFile(path.join(dir, "wal.log"), `${line}\n`, "utf8");
  await store.close({ flush: false });

  const reopened = await LoomStore.open(dir);
  assert.equal(reopened.stats().memtableEntries, 0);
  assert.equal(await reopened.get("durable"), "value");
  await reopened.close();
});

test("bloom filter has no false negatives for inserted keys", () => {
  const bloom = new BloomFilter(128, 3);
  bloom.add("alpha");
  bloom.add("beta");
  assert.equal(bloom.mightContain("alpha"), true);
  assert.equal(bloom.mightContain("beta"), true);
});
