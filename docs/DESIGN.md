# Loom Design Notes

## Goal

Loom is a study-sized LSM-tree storage engine. It is not trying to replace RocksDB, but it implements the core mechanics in a way that supports serious system-design discussion.

## Core Invariants

- exactly one writer owns a database directory at a time, enforced by `LOCK`
- sequence numbers define recency; wall-clock time never participates in conflict resolution
- WAL acknowledgement happens before a value enters the memtable
- a manifest references only fully written, fsynced, atomically renamed SSTables
- every SSTable uses the same bytewise key comparator as point reads and scans
- the highest sequence number wins across the memtable and every level
- tombstones remain until a major compaction has included every older table

## Write Path

```text
put/delete
  -> validate and copy the value
  -> assign monotonic sequence number
  -> append checksummed WAL record
  -> fsync WAL when syncWrites=true
  -> update memtable
  -> flush on entry-count or byte-budget pressure
```

The write queue serializes mutations so concurrent calls cannot interleave WAL appends, sequence-number assignment, memtable updates, flushes, and compactions. Values are deep-copied before acknowledgement so caller mutation cannot make the memtable disagree with WAL bytes already on disk.

## Read Path

```text
get(key)
  -> wait for pending writes
  -> check memtable
  -> check candidate SSTables by key range
  -> use Bloom filter for negative lookups
  -> binary-search SSTable block index
  -> read one data block
  -> verify block checksum
  -> choose highest sequence-numbered entry
```

The highest sequence number wins. A delete is represented as a tombstone, so an older value in another table does not reappear.

## SSTable Format

```text
MAGIC
data block 0
data block 1
...
JSON footer
footer checksum (uint32)
footer length (uint32)
FOOTER_MAGIC
```

The footer contains table identity, level, key and sequence ranges, the Bloom filter bitset, and a block index with first key, last key, offset, length, checksum, and entry count.

Point reads do not load the whole table. Loom reads the footer, chooses a block, verifies its checksum, and binary-searches inside it. The current writer emits format version 3 (`.sst`). The reader also accepts version 2 files so existing data migrates naturally when compaction rewrites tables.

## Recovery

Startup acquires the directory lock, validates `MANIFEST.json`, removes unreferenced SSTables and stale atomic-write files, then replays `wal.log`. WAL records look like:

```json
{ "record": { "op": "put", "key": "x", "value": 1, "seq": 7 }, "checksum": 123 }
```

Recovery scans complete newline-terminated records and tracks the last valid byte offset. A partial, malformed, or checksum-invalid tail is truncated before the store accepts new writes. Records whose sequence is already covered by a manifest-installed SSTable are discarded, preventing duplicate flushes after a crash between manifest installation and WAL truncation.

## Atomic Install

SSTables and manifests are installed with:

```text
write temp file
fsync temp file
rename temp file to final path
best-effort fsync parent directory
```

`MANIFEST.json` is the source of truth. An orphan SSTable is harmless if a crash happens after writing it but before the manifest points to it; startup removes that orphan after taking the writer lock.

## Failure Boundaries

| Crash point | Recovery result |
| --- | --- |
| Before WAL fsync | Write was not acknowledged and may be absent |
| After WAL fsync, before memtable update | WAL replay restores the write |
| During SSTable temp write | Temp file is ignored and removed on startup |
| After SSTable rename, before manifest install | Orphan SSTable is removed on startup |
| After manifest install, before WAL truncate | Durable WAL records are recognized and discarded |
| During WAL truncate | Checksum scan repairs the valid prefix |
| After compaction manifest install, before old-file deletion | New tables are authoritative; old orphan files are removed |

## Compaction

Loom has two compaction modes:

- automatic compaction moves overlapping level-0 tables into the next level
- manual major compaction merges all levels into one level-1 table

Level 0 can contain overlapping ranges. Higher levels are sorted by key range. Automatic compaction retains tombstones because deeper levels may still contain older values. Major compaction includes every live table, so it can safely drop tombstones.

The current implementation produces one output table per compaction. A production extension would split output at a target byte size and select bounded ranges instead of compacting an entire level.

## Integrity Verification

`LoomStore.verify()` and `loom verify <dir>` read every live block and check:

- header, footer, and block checksums
- manifest entry counts
- strict key ordering
- key and sequence ranges declared by manifest metadata

The verifier throws on the first integrity violation and returns aggregate table, entry, byte, and level counts on success.

## Interview Impact

The project provides concrete examples for WAL durability, idempotent recovery, immutable sorted files, sparse block indexes, Bloom-filter tradeoffs, corruption detection, single-writer concurrency control, leveled compaction, tombstone lifecycle, and crash-safe metadata installation.

## Known Limits

- values are JSON-serializable, not arbitrary binary blobs
- compaction outputs one table instead of splitting by target file size
- reads wait for queued writes instead of using snapshot isolation
- scans materialize matching entries instead of using a streaming k-way iterator
- there is no block cache or background compaction worker
- data blocks are not compressed
- stale-lock recovery uses PID liveness and cannot eliminate the rare PID-reuse ambiguity

Good next upgrades would be snapshots, a streaming iterator, a block cache, compression, table splitting, and background compaction with backpressure.
