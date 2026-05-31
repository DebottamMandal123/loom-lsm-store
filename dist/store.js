"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const { checksum32 } = require("./checksum");
const { appendAndSync, atomicWriteFile, truncateAndSync } = require("./io");
const { findInSstable, readEntries, scanSstable, writeSstable } = require("./sstable");

const DEFAULT_OPTIONS = {
  memtableLimit: 100,
  level0CompactionThreshold: 4,
  levelMaxTables: 6,
  blockSize: 16,
  syncWrites: true,
};

class LoomStore {
  constructor(dir, options = {}) {
    this.dir = dir;
    this.walPath = path.join(dir, "wal.log");
    this.manifestPath = path.join(dir, "MANIFEST.json");
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.manifest = { version: 2, nextFileId: 1, nextSeq: 1, levels: [[]] };
    this.memtable = new Map();
    this.writeQueue = Promise.resolve();
    this.closed = false;
  }

  static async open(dir, options = {}) {
    const store = new LoomStore(dir, options);
    await fs.mkdir(dir, { recursive: true });
    await store.loadManifest();
    await store.recoverWal();
    return store;
  }

  async put(key, value) {
    this.assertKey(key);
    await this.enqueueWrite(async () => {
      this.assertOpen();
      const seq = this.manifest.nextSeq++;
      await this.appendWal({ op: "put", key, value, seq });
      this.memtable.set(key, { key, value, seq, deleted: false });
      await this.flushIfNeeded();
    });
  }

  async get(key) {
    this.assertKey(key);
    await this.writeQueue;
    const entry = await this.findLatestEntry(key);
    return !entry || entry.deleted ? undefined : entry.value;
  }

  async delete(key) {
    this.assertKey(key);
    await this.enqueueWrite(async () => {
      this.assertOpen();
      const seq = this.manifest.nextSeq++;
      await this.appendWal({ op: "delete", key, seq });
      this.memtable.set(key, { key, seq, deleted: true });
      await this.flushIfNeeded();
    });
  }

  async scan(options = {}) {
    await this.writeQueue;
    const merged = new Map();
    const consider = (entry) => {
      if (!inRange(entry.key, options.start, options.end)) return;
      const current = merged.get(entry.key);
      if (!current || entry.seq > current.seq) merged.set(entry.key, entry);
    };

    for (const entry of this.memtable.values()) consider(entry);
    for (const table of this.allTables()) {
      for (const entry of await scanSstable(this.dir, table, options.start, options.end)) consider(entry);
    }

    const rows = [...merged.values()]
      .filter((entry) => !entry.deleted)
      .sort((a, b) => a.key.localeCompare(b.key))
      .map((entry) => ({ key: entry.key, value: entry.value }));
    return options.limit ? rows.slice(0, options.limit) : rows;
  }

  async flush() {
    await this.enqueueWrite(async () => {
      this.assertOpen();
      await this.flushMemtable();
    });
  }

  async compact() {
    await this.enqueueWrite(async () => {
      this.assertOpen();
      await this.flushMemtable();
      await this.majorCompact();
    });
  }

  stats() {
    const levels = this.manifest.levels.map((tables, level) => {
      const minKey = minString(tables.map((table) => table.minKey));
      const maxKey = maxString(tables.map((table) => table.maxKey));
      return {
        level,
        tables: tables.length,
        entries: tables.reduce((sum, table) => sum + table.count, 0),
        keyRange: minKey && maxKey ? [minKey, maxKey] : null,
      };
    });
    return {
      memtableEntries: this.memtable.size,
      sstables: this.allTables().length,
      diskEntries: this.allTables().reduce((sum, table) => sum + table.count, 0),
      nextFileId: this.manifest.nextFileId,
      nextSeq: this.manifest.nextSeq,
      levels,
    };
  }

  async close() {
    await this.enqueueWrite(async () => {
      if (!this.closed) {
        await this.flushMemtable();
        this.closed = true;
      }
    });
  }

  async findLatestEntry(key) {
    let best = this.memtable.get(key);
    for (const table of this.allTables()) {
      const entry = await findInSstable(this.dir, table, key);
      if (entry && (!best || entry.seq > best.seq)) best = entry;
    }
    return best;
  }

  async flushMemtable() {
    if (this.memtable.size === 0) return;
    const entries = [...this.memtable.values()];
    const id = this.manifest.nextFileId++;
    const meta = await writeSstable(this.dir, id, 0, entries, this.options.blockSize);
    this.ensureLevel(0);
    this.manifest.levels[0].unshift(meta);
    await this.saveManifest();
    await truncateAndSync(this.walPath);
    this.memtable.clear();
    await this.compactIfNeeded();
  }

  async compactIfNeeded() {
    while ((this.manifest.levels[0]?.length ?? 0) >= this.options.level0CompactionThreshold) {
      await this.compactLevel(0);
    }
    for (let level = 1; level < this.manifest.levels.length; level += 1) {
      while ((this.manifest.levels[level]?.length ?? 0) > this.options.levelMaxTables) {
        await this.compactLevel(level);
      }
    }
  }

  async compactLevel(level) {
    const current = this.manifest.levels[level] ?? [];
    if (current.length === 0) return;
    const minKey = minString(current.map((table) => table.minKey));
    const maxKey = maxString(current.map((table) => table.maxKey));
    if (!minKey || !maxKey) return;

    this.ensureLevel(level + 1);
    const next = this.manifest.levels[level + 1];
    const overlapping = next.filter((table) => rangesOverlap(table.minKey, table.maxKey, minKey, maxKey));
    const keepNext = next.filter((table) => !rangesOverlap(table.minKey, table.maxKey, minKey, maxKey));
    const inputs = [...current, ...overlapping];
    const merged = await this.mergeTables(inputs, false);
    const oldTables = inputs;

    this.manifest.levels[level] = [];
    if (merged.length > 0) {
      const id = this.manifest.nextFileId++;
      const meta = await writeSstable(this.dir, id, level + 1, merged, this.options.blockSize);
      this.manifest.levels[level + 1] = sortTables([...keepNext, meta]);
    } else {
      this.manifest.levels[level + 1] = sortTables(keepNext);
    }

    await this.saveManifest();
    await this.removeTables(oldTables);
  }

  async majorCompact() {
    const oldTables = this.allTables();
    if (oldTables.length === 0) return;
    const merged = await this.mergeTables(oldTables, true);
    const newLevels = [[]];
    if (merged.length > 0) {
      const id = this.manifest.nextFileId++;
      const meta = await writeSstable(this.dir, id, 1, merged, this.options.blockSize);
      newLevels[1] = [meta];
    }
    this.manifest.levels = newLevels;
    await this.saveManifest();
    await this.removeTables(oldTables);
  }

  async mergeTables(tables, dropTombstones) {
    const merged = new Map();
    for (const table of tables) {
      for (const entry of await readEntries(this.dir, table)) {
        const current = merged.get(entry.key);
        if (!current || entry.seq > current.seq) merged.set(entry.key, entry);
      }
    }
    return [...merged.values()]
      .filter((entry) => !(dropTombstones && entry.deleted))
      .sort((a, b) => a.key.localeCompare(b.key));
  }

  async removeTables(tables) {
    await Promise.all(tables.map((table) => fs.rm(path.join(this.dir, table.file), { force: true })));
  }

  async loadManifest() {
    try {
      const parsed = JSON.parse(await fs.readFile(this.manifestPath, "utf8"));
      if (parsed.version === 2 && Array.isArray(parsed.levels)) {
        this.manifest = parsed;
      } else {
        this.manifest = {
          version: 2,
          nextFileId: parsed.nextId ?? 1,
          nextSeq: 1,
          levels: [parsed.tables ?? []],
        };
      }
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      await this.saveManifest();
    }
    if (this.manifest.levels.length === 0) this.manifest.levels = [[]];
  }

  async recoverWal() {
    try {
      const content = await fs.readFile(this.walPath, "utf8");
      for (const line of content.split(/\r?\n/)) {
        if (!line.trim()) continue;
        let parsed;
        try {
          parsed = JSON.parse(line);
        } catch {
          break;
        }
        const body = JSON.stringify(parsed.record);
        if (checksum32(body) !== parsed.checksum) break;
        const record = parsed.record;
        const current = this.memtable.get(record.key);
        if (!current || record.seq >= current.seq) {
          this.memtable.set(record.key, {
            key: record.key,
            value: record.value,
            seq: record.seq,
            deleted: record.op === "delete",
          });
        }
        this.manifest.nextSeq = Math.max(this.manifest.nextSeq, record.seq + 1);
      }
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      await truncateAndSync(this.walPath);
    }
  }

  async appendWal(record) {
    const body = JSON.stringify(record);
    const line = { record, checksum: checksum32(body) };
    await appendAndSync(this.walPath, `${JSON.stringify(line)}\n`, this.options.syncWrites);
  }

  async saveManifest() {
    await atomicWriteFile(this.manifestPath, JSON.stringify(this.manifest, null, 2));
  }

  async flushIfNeeded() {
    if (this.memtable.size >= this.options.memtableLimit) await this.flushMemtable();
  }

  allTables() {
    return this.manifest.levels.flat();
  }

  ensureLevel(level) {
    while (this.manifest.levels.length <= level) this.manifest.levels.push([]);
  }

  enqueueWrite(operation) {
    const run = this.writeQueue.then(operation, operation);
    this.writeQueue = run.then(() => undefined, () => undefined);
    return run;
  }

  assertOpen() {
    if (this.closed) throw new Error("store is closed");
  }

  assertKey(key) {
    if (!key || typeof key !== "string") throw new Error("key must be a non-empty string");
  }
}

function inRange(key, start, end) {
  return (!start || key >= start) && (!end || key <= end);
}

function rangesOverlap(aMin, aMax, bMin, bMax) {
  return aMin <= bMax && bMin <= aMax;
}

function sortTables(tables) {
  return [...tables].sort((a, b) => a.minKey.localeCompare(b.minKey));
}

function minString(values) {
  return values.length ? values.reduce((min, value) => (value < min ? value : min)) : undefined;
}

function maxString(values) {
  return values.length ? values.reduce((max, value) => (value > max ? value : max)) : undefined;
}

module.exports = { LoomStore };
