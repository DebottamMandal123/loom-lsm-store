# Loom

Loom is a dependency-free LSM-tree key-value store in Node.js/TypeScript. It is built to be small enough to read, but it now includes the storage-engine ideas interviewers expect you to understand:

- checksummed write-ahead log records for crash recovery
- torn-WAL repair and durable-record replay suppression
- monotonic sequence numbers instead of wall-clock timestamps
- serialized write pipeline for concurrency safety
- exclusive directory lock for single-writer ownership
- memtable flushes into immutable SSTables
- entry-count and byte-budget memtable limits
- block-indexed SSTables with Bloom filters, block checksums, and footer checksums
- tombstones for deletes
- range scans
- level-0 to level-1 compaction plus manual major compaction
- atomic manifest and SSTable writes through temp-file + fsync + rename
- orphan-file cleanup and an online integrity verifier
- tests and a benchmark script

## How To Run

Loom needs Node.js 18 or newer and has no runtime dependencies. From the project directory:

```powershell
node dist/cli.js demo .loom-demo
node --test tests/*.test.js
node scripts/benchmark.js 2000 .loom-bench
```

## CLI Commands

```powershell
node dist/cli.js put .loom-demo user:1 "{""name"":""Ada""}"
node dist/cli.js get .loom-demo user:1
node dist/cli.js scan .loom-demo user:1 user:9
node dist/cli.js delete .loom-demo user:1
node dist/cli.js compact .loom-demo
node dist/cli.js verify .loom-demo
node dist/cli.js stats .loom-demo
```

The `.loom-demo` or `.loom-bench` folder is the database. After running commands, open it and inspect:

- `MANIFEST.json`: active SSTables, levels, sequence numbers
- `wal.log`: pending unflushed writes
- `LOCK`: single-writer ownership while the database is open
- `sst-000001.sst`, etc.: binary SSTable files with data blocks and a footer

SSTables use a versioned binary envelope around JSON-encoded blocks. Point reads use the footer index and Bloom filter to read only the candidate block. Version 3 adds a checksum over the footer; version 2 SSTables remain readable and are rewritten during compaction.

## API

```js
const { LoomStore } = require("./dist");

const store = await LoomStore.open("./data", {
  memtableLimit: 1000,
  memtableBytesLimit: 4 * 1024 * 1024,
  level0CompactionThreshold: 4,
  blockSize: 32,
});

await store.put("user:1", { name: "Ada" });
console.log(await store.get("user:1"));
console.log(await store.scan({ start: "user:1", end: "user:9" }));
await store.delete("user:1");
await store.compact();
console.log(await store.verify());
await store.close();
```

## Project Map

- `src/`: TypeScript source
- `dist/`: runnable CommonJS build, kept dependency-free
- `tests/`: Node test runner coverage
- `scripts/benchmark.js`: simple throughput benchmark
- `docs/DESIGN.md`: architecture and interview talking points

## What To Show In Interviews

Start with `docs/DESIGN.md`, then run the demo and tests. The strongest talking points are:

- why LSMs trade read amplification for high write throughput
- why WAL records need sequence numbers and checksums
- why SSTables are immutable and block-indexed
- how Bloom filters avoid unnecessary disk reads
- why compaction controls read amplification and disk space
- how atomic rename makes manifest/SSTable installation safer after crashes
- how a single-writer lock avoids lost manifest updates
- how recovery repairs a torn WAL without hiding later writes
