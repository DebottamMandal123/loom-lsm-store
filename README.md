# Loom

Loom is a dependency-free LSM-tree key-value store in Node.js/TypeScript. It is built to be small enough to read, but it now includes the storage-engine ideas interviewers expect you to understand:

- checksummed write-ahead log records for crash recovery
- monotonic sequence numbers instead of wall-clock timestamps
- serialized write pipeline for concurrency safety
- memtable flushes into immutable SSTables
- block-indexed SSTables with Bloom filters and per-block checksums
- tombstones for deletes
- range scans
- level-0 to level-1 compaction plus manual major compaction
- atomic manifest and SSTable writes through temp-file + fsync + rename
- tests and a benchmark script

## How To Run

Open PowerShell in this folder:

```powershell
cd C:\Users\KIIT\Documents\Codex\2026-05-31\goal-choose-any-one-of-these\outputs\loom
```

If `node` works on your machine, use these commands:

```powershell
node dist/cli.js demo .loom-demo
node --test tests/*.test.js
node scripts/benchmark.js 2000 .loom-bench
```

If `node` is blocked on your PATH in Codex, use the bundled Node executable:

```powershell
& 'C:\Users\KIIT\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe' dist/cli.js demo .loom-demo
& 'C:\Users\KIIT\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe' --test tests/*.test.js
& 'C:\Users\KIIT\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe' scripts/benchmark.js 2000 .loom-bench
```

## CLI Commands

```powershell
node dist/cli.js put .loom-demo user:1 "{""name"":""Ada""}"
node dist/cli.js get .loom-demo user:1
node dist/cli.js scan .loom-demo user:1 user:9
node dist/cli.js delete .loom-demo user:1
node dist/cli.js compact .loom-demo
node dist/cli.js stats .loom-demo
```

The `.loom-demo` or `.loom-bench` folder is the database. After running commands, open it and inspect:

- `MANIFEST.json`: active SSTables, levels, sequence numbers
- `wal.log`: pending unflushed writes
- `sst-000001.json`, etc.: binary SSTable files with data blocks and a footer

The SSTable filenames still end in `.json` so they are easy to identify, but the files are binary-ish: they contain block payloads plus a JSON footer and checksums.

## API

```js
const { LoomStore } = require("./dist");

const store = await LoomStore.open("./data", {
  memtableLimit: 1000,
  level0CompactionThreshold: 4,
  blockSize: 32,
});

await store.put("user:1", { name: "Ada" });
console.log(await store.get("user:1"));
console.log(await store.scan({ start: "user:1", end: "user:9" }));
await store.delete("user:1");
await store.compact();
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
