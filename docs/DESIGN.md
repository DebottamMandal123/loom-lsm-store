# Loom Design Notes

## Goal

Loom is a study-sized LSM-tree storage engine. It is not trying to replace RocksDB, but it does implement the core mechanics in a way that supports serious system-design discussion.

## Write Path

```text
put/delete
  -> assign monotonic sequence number
  -> append checksummed WAL record
  -> fsync WAL when syncWrites=true
  -> update memtable
  -> flush memtable when threshold is reached
```

The write queue serializes mutations so concurrent calls cannot interleave WAL appends, sequence-number assignment, memtable updates, flushes, and compactions.

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
footer length (uint32)
FOOTER_MAGIC
```

The footer contains:

- table id, level, key range, sequence range
- Bloom filter bitset
- block index: first key, last key, offset, length, checksum, count

This avoids loading an entire table for point reads. Loom reads the footer, chooses a block, verifies the checksum, and then binary-searches inside that block.

## Recovery

Startup loads `MANIFEST.json`, then replays `wal.log`. WAL records are stored as:

```json
{ "record": { "op": "put", "key": "x", "value": 1, "seq": 7 }, "checksum": 123 }
```

If recovery sees a partial or checksum-invalid trailing line, it stops replaying. That models a torn final write after a crash.

## Atomic Install

SSTables and manifests are written with:

```text
write temp file
fsync temp file
rename temp file to final path
best-effort fsync parent directory
```

The key invariant is that `MANIFEST.json` is the source of truth. An orphan SSTable is harmless if a crash happens after writing it but before the manifest points to it.

## Compaction

Loom has two compaction modes:

- automatic compaction moves overlapping level-0 tables into level 1
- manual major compaction merges all levels into one level-1 table and drops tombstones

Level 0 can contain overlapping ranges. Level 1 is kept sorted by key range after compaction. This is a simplified version of leveled compaction, but it is enough to explain read amplification, write amplification, and tombstone cleanup.

## Interview Impact

The project gives you concrete examples for:

- WAL durability and recovery
- idempotent replay through sequence numbers
- SSTable immutability
- sparse indexes and block-level reads
- Bloom filter tradeoffs and false positives
- checksums and corruption detection
- compaction strategy
- concurrency control through a single-writer queue

## Known Limits

These are intentional boundaries that make good follow-up discussion:

- values are JSON-serializable, not arbitrary binary blobs
- compaction outputs one table instead of splitting by target file size
- reads wait for queued writes instead of using snapshot isolation
- there is no block cache yet
- no background compaction thread
- no compression

Good next upgrades would be snapshots, iterators backed by a k-way merge, a block cache, compression, and size-tiered or fully leveled compaction with table splitting.
