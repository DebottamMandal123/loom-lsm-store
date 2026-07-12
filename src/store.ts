import { promises as fs } from "node:fs";
import type { FileHandle } from "node:fs/promises";
import path from "node:path";
import { checksum32 } from "./checksum";
import { appendAndSync, atomicWriteFile, truncateAndSync, truncateToAndSync } from "./io";
import { compareKeys } from "./key-order";
import { findInSstable, readEntries, scanSstable, writeSstable } from "./sstable";
import { CloseOptions, Entry, Manifest, ScanOptions, StoreOptions, StoredValue, StoreStats, StoreVerification, SstableMeta, WalLine, WalRecord } from "./types";

const DEFAULT_OPTIONS: Required<StoreOptions> = {
  memtableLimit: 100,
  memtableBytesLimit: 4 * 1024 * 1024,
  level0CompactionThreshold: 4,
  levelMaxTables: 6,
  blockSize: 16,
  syncWrites: true,
};

export class LoomStore {
  private readonly dir: string;
  private readonly walPath: string;
  private readonly manifestPath: string;
  private readonly lockPath: string;
  private readonly options: Required<StoreOptions>;
  private manifest: Manifest = { version: 3, nextFileId: 1, nextSeq: 1, levels: [[]] };
  private memtable = new Map<string, Entry>();
  private memtableBytes = 0;
  private writeQueue: Promise<void> = Promise.resolve();
  private lockHandle?: FileHandle;
  private closed = false;

  private constructor(dir: string, options: StoreOptions = {}) {
    this.dir = dir;
    this.walPath = path.join(dir, "wal.log");
    this.manifestPath = path.join(dir, "MANIFEST.json");
    this.lockPath = path.join(dir, "LOCK");
    this.options = { ...DEFAULT_OPTIONS, ...options };
    validateOptions(this.options);
  }

  static async open(dir: string, options: StoreOptions = {}): Promise<LoomStore> {
    const store = new LoomStore(dir, options);
    await fs.mkdir(dir, { recursive: true });
    await store.acquireLock();
    try {
      await store.loadManifest();
      await store.cleanupOrphans();
      await store.recoverWal();
      return store;
    } catch (error) {
      await store.releaseLock();
      throw error;
    }
  }

  async put(key: string, value: StoredValue): Promise<void> {
    this.assertKey(key);
    const storedValue = cloneStoredValue(value);
    await this.enqueueWrite(async () => {
      this.assertOpen();
      const seq = this.manifest.nextSeq++;
      await this.appendWal({ op: "put", key, value: storedValue, seq });
      this.setMemtableEntry({ key, value: storedValue, seq, deleted: false });
      await this.flushIfNeeded();
    });
  }

  async get(key: string): Promise<StoredValue | undefined> {
    this.assertKey(key);
    await this.writeQueue;
    this.assertOpen();
    const entry = await this.findLatestEntry(key);
    return !entry || entry.deleted ? undefined : entry.value;
  }

  async delete(key: string): Promise<void> {
    this.assertKey(key);
    await this.enqueueWrite(async () => {
      this.assertOpen();
      const seq = this.manifest.nextSeq++;
      await this.appendWal({ op: "delete", key, seq });
      this.setMemtableEntry({ key, seq, deleted: true });
      await this.flushIfNeeded();
    });
  }

  async scan(options: ScanOptions = {}): Promise<Array<{ key: string; value: StoredValue }>> {
    await this.writeQueue;
    this.assertOpen();
    validateScanOptions(options);
    const merged = new Map<string, Entry>();
    const consider = (entry: Entry) => {
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
      .sort((a, b) => compareKeys(a.key, b.key))
      .map((entry) => ({ key: entry.key, value: entry.value as StoredValue }));
    return options.limit ? rows.slice(0, options.limit) : rows;
  }

  async flush(): Promise<void> {
    await this.enqueueWrite(async () => {
      this.assertOpen();
      await this.flushMemtable();
    });
  }

  async compact(): Promise<void> {
    await this.enqueueWrite(async () => {
      this.assertOpen();
      await this.flushMemtable();
      await this.majorCompact();
    });
  }

  stats(): StoreStats {
    const levels = this.manifest.levels.map((tables, level) => {
      const minKey = minString(tables.map((table) => table.minKey));
      const maxKey = maxString(tables.map((table) => table.maxKey));
      return {
        level,
        tables: tables.length,
        entries: tables.reduce((sum, table) => sum + table.count, 0),
        keyRange: minKey && maxKey ? [minKey, maxKey] as [string, string] : null,
      };
    });
    return {
      memtableEntries: this.memtable.size,
      memtableBytes: this.memtableBytes,
      sstables: this.allTables().length,
      diskEntries: this.allTables().reduce((sum, table) => sum + table.count, 0),
      diskBytes: this.allTables().reduce((sum, table) => sum + table.sizeBytes, 0),
      nextFileId: this.manifest.nextFileId,
      nextSeq: this.manifest.nextSeq,
      levels,
    };
  }

  async verify(): Promise<StoreVerification> {
    await this.writeQueue;
    this.assertOpen();
    let entries = 0;
    for (const table of this.allTables()) {
      const rows = await readEntries(this.dir, table);
      if (rows.length !== table.count) throw new Error(`${table.file} entry count does not match its manifest metadata`);
      for (let index = 0; index < rows.length; index += 1) {
        const entry = rows[index];
        if (index > 0 && compareKeys(rows[index - 1].key, entry.key) >= 0) {
          throw new Error(`${table.file} entries are not strictly sorted`);
        }
        if (compareKeys(entry.key, table.minKey) < 0 || compareKeys(entry.key, table.maxKey) > 0) {
          throw new Error(`${table.file} contains a key outside its manifest range`);
        }
        if (entry.seq < table.minSeq || entry.seq > table.maxSeq) {
          throw new Error(`${table.file} contains a sequence outside its manifest range`);
        }
      }
      entries += rows.length;
    }
    return {
      ok: true,
      tables: this.allTables().length,
      entries,
      bytes: this.allTables().reduce((sum, table) => sum + table.sizeBytes, 0),
      levels: this.manifest.levels.length,
    };
  }

  async close(options: CloseOptions = {}): Promise<void> {
    await this.enqueueWrite(async () => {
      if (!this.closed) {
        try {
          if (options.flush !== false) await this.flushMemtable();
        } finally {
          this.closed = true;
          await this.releaseLock();
        }
      }
    });
  }

  private async findLatestEntry(key: string): Promise<Entry | undefined> {
    let best = this.memtable.get(key);
    for (const table of this.allTables()) {
      const entry = await findInSstable(this.dir, table, key);
      if (entry && (!best || entry.seq > best.seq)) best = entry;
    }
    return best;
  }

  private async flushMemtable(): Promise<void> {
    if (this.memtable.size === 0) return;
    const entries = [...this.memtable.values()];
    const id = this.manifest.nextFileId++;
    const meta = await writeSstable(this.dir, id, 0, entries, this.options.blockSize);
    this.ensureLevel(0);
    this.manifest.levels[0].unshift(meta);
    await this.saveManifest();
    await truncateAndSync(this.walPath);
    this.memtable.clear();
    this.memtableBytes = 0;
    await this.compactIfNeeded();
  }

  private async compactIfNeeded(): Promise<void> {
    while ((this.manifest.levels[0]?.length ?? 0) >= this.options.level0CompactionThreshold) {
      await this.compactLevel(0);
    }
    for (let level = 1; level < this.manifest.levels.length; level += 1) {
      while ((this.manifest.levels[level]?.length ?? 0) > this.options.levelMaxTables) {
        await this.compactLevel(level);
      }
    }
  }

  private async compactLevel(level: number): Promise<void> {
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

  private async majorCompact(): Promise<void> {
    const oldTables = this.allTables();
    if (oldTables.length === 0) return;
    const merged = await this.mergeTables(oldTables, true);
    const newLevels: SstableMeta[][] = [[]];
    if (merged.length > 0) {
      const id = this.manifest.nextFileId++;
      const meta = await writeSstable(this.dir, id, 1, merged, this.options.blockSize);
      newLevels[1] = [meta];
    }
    this.manifest.levels = newLevels;
    await this.saveManifest();
    await this.removeTables(oldTables);
  }

  private async mergeTables(tables: SstableMeta[], dropTombstones: boolean): Promise<Entry[]> {
    const merged = new Map<string, Entry>();
    for (const table of tables) {
      for (const entry of await readEntries(this.dir, table)) {
        const current = merged.get(entry.key);
        if (!current || entry.seq > current.seq) merged.set(entry.key, entry);
      }
    }
    return [...merged.values()]
      .filter((entry) => !(dropTombstones && entry.deleted))
      .sort((a, b) => compareKeys(a.key, b.key));
  }

  private async removeTables(tables: SstableMeta[]): Promise<void> {
    await Promise.all(tables.map((table) => fs.rm(path.join(this.dir, table.file), { force: true })));
  }

  private async loadManifest(): Promise<void> {
    try {
      const parsed = JSON.parse(await fs.readFile(this.manifestPath, "utf8"));
      if (parsed.version === 3 && Array.isArray(parsed.levels)) {
        this.manifest = parsed as Manifest;
      } else if (parsed.version === 2 && Array.isArray(parsed.levels)) {
        this.manifest = {
          version: 3,
          nextFileId: parsed.nextFileId,
          nextSeq: parsed.nextSeq,
          levels: parsed.levels,
        };
      } else {
        throw new Error(`unsupported manifest version: ${String(parsed.version ?? "unknown")}`);
      }
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      await this.saveManifest();
    }
    if (this.manifest.levels.length === 0) this.manifest.levels = [[]];
    validateManifest(this.manifest);
  }

  private async recoverWal(): Promise<void> {
    try {
      const content = await fs.readFile(this.walPath);
      const durableSeq = this.allTables().reduce((max, table) => Math.max(max, table.maxSeq), 0);
      let cursor = 0;
      let validLength = 0;
      let replayed = false;
      while (cursor < content.length) {
        const newline = content.indexOf(0x0a, cursor);
        if (newline === -1) break;
        let lineBuffer = content.subarray(cursor, newline);
        if (lineBuffer.at(-1) === 0x0d) lineBuffer = lineBuffer.subarray(0, -1);
        const line = lineBuffer.toString("utf8");
        if (!line.trim()) {
          validLength = newline + 1;
          cursor = newline + 1;
          continue;
        }
        let parsed: WalLine;
        try {
          parsed = JSON.parse(line) as WalLine;
        } catch {
          break;
        }
        const body = JSON.stringify(parsed.record);
        if (checksum32(body) !== parsed.checksum) break;
        const record = parsed.record;
        if (record.seq > durableSeq) {
          replayed = true;
          const current = this.memtable.get(record.key);
          if (!current || record.seq >= current.seq) this.setMemtableEntry({
            key: record.key,
            value: record.value,
            seq: record.seq,
            deleted: record.op === "delete",
          });
        }
        this.manifest.nextSeq = Math.max(this.manifest.nextSeq, record.seq + 1);
        validLength = newline + 1;
        cursor = newline + 1;
      }
      if (content.length > 0 && !replayed) await truncateAndSync(this.walPath);
      else if (validLength < content.length) await truncateToAndSync(this.walPath, validLength);
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      await truncateAndSync(this.walPath);
    }
  }

  private async appendWal(record: WalRecord): Promise<void> {
    const body = JSON.stringify(record);
    const line: WalLine = { record, checksum: checksum32(body) };
    await appendAndSync(this.walPath, `${JSON.stringify(line)}\n`, this.options.syncWrites);
  }

  private async saveManifest(): Promise<void> {
    await atomicWriteFile(this.manifestPath, JSON.stringify(this.manifest, null, 2));
  }

  private async flushIfNeeded(): Promise<void> {
    if (
      this.memtable.size >= this.options.memtableLimit ||
      this.memtableBytes >= this.options.memtableBytesLimit
    ) await this.flushMemtable();
  }

  private setMemtableEntry(entry: Entry): void {
    const previous = this.memtable.get(entry.key);
    if (previous) this.memtableBytes -= entrySize(previous);
    this.memtable.set(entry.key, entry);
    this.memtableBytes += entrySize(entry);
  }

  private async acquireLock(): Promise<void> {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const handle = await fs.open(this.lockPath, "wx");
        await handle.writeFile(JSON.stringify({ pid: process.pid, createdAt: new Date().toISOString() }), "utf8");
        await handle.sync();
        this.lockHandle = handle;
        return;
      } catch (error: any) {
        if (error?.code !== "EEXIST") throw error;
        if (attempt === 1 || !(await this.lockIsStale())) {
          throw new Error(`store is already open: ${this.dir}`);
        }
        await fs.rm(this.lockPath, { force: true });
      }
    }
  }

  private async lockIsStale(): Promise<boolean> {
    try {
      const lock = JSON.parse(await fs.readFile(this.lockPath, "utf8"));
      return !Number.isInteger(lock.pid) || !processIsAlive(lock.pid);
    } catch {
      return true;
    }
  }

  private async releaseLock(): Promise<void> {
    const handle = this.lockHandle;
    this.lockHandle = undefined;
    if (handle) await handle.close();
    await fs.rm(this.lockPath, { force: true });
  }

  private async cleanupOrphans(): Promise<void> {
    const active = new Set(this.allTables().map((table) => table.file));
    for (const entry of await fs.readdir(this.dir, { withFileTypes: true })) {
      if (!entry.isFile()) continue;
      const isSstable = /^sst-\d+\.(?:json|sst)$/.test(entry.name);
      const isTemp = entry.name.includes(".tmp-");
      if ((isSstable && !active.has(entry.name)) || isTemp) {
        await fs.rm(path.join(this.dir, entry.name), { force: true });
      }
    }
  }

  private allTables(): SstableMeta[] {
    return this.manifest.levels.flat();
  }

  private ensureLevel(level: number): void {
    while (this.manifest.levels.length <= level) this.manifest.levels.push([]);
  }

  private enqueueWrite<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.writeQueue.then(operation, operation);
    this.writeQueue = run.then(() => undefined, () => undefined);
    return run;
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("store is closed");
  }

  private assertKey(key: string): void {
    if (!key || typeof key !== "string") throw new Error("key must be a non-empty string");
  }
}

function inRange(key: string, start?: string, end?: string): boolean {
  return (!start || key >= start) && (!end || key <= end);
}

function rangesOverlap(aMin: string, aMax: string, bMin: string, bMax: string): boolean {
  return aMin <= bMax && bMin <= aMax;
}

function sortTables(tables: SstableMeta[]): SstableMeta[] {
  return [...tables].sort((a, b) => compareKeys(a.minKey, b.minKey));
}

function minString(values: string[]): string | undefined {
  return values.length ? values.reduce((min, value) => (value < min ? value : min)) : undefined;
}

function maxString(values: string[]): string | undefined {
  return values.length ? values.reduce((max, value) => (value > max ? value : max)) : undefined;
}

function validateOptions(options: Required<StoreOptions>): void {
  for (const [name, value] of Object.entries(options)) {
    if (name === "syncWrites") continue;
    if (!Number.isSafeInteger(value as number) || (value as number) <= 0) {
      throw new Error(`${name} must be a positive integer`);
    }
  }
  if (typeof options.syncWrites !== "boolean") throw new Error("syncWrites must be a boolean");
}

function validateScanOptions(options: ScanOptions): void {
  if (options.start !== undefined && (typeof options.start !== "string" || options.start.length === 0)) {
    throw new Error("scan start must be a non-empty string");
  }
  if (options.end !== undefined && (typeof options.end !== "string" || options.end.length === 0)) {
    throw new Error("scan end must be a non-empty string");
  }
  if (options.start && options.end && compareKeys(options.start, options.end) > 0) {
    throw new Error("scan start must not be greater than end");
  }
  if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit <= 0)) {
    throw new Error("scan limit must be a positive integer");
  }
}

function validateManifest(manifest: Manifest): void {
  if (!Number.isSafeInteger(manifest.nextFileId) || manifest.nextFileId <= 0) {
    throw new Error("manifest has an invalid nextFileId");
  }
  if (!Number.isSafeInteger(manifest.nextSeq) || manifest.nextSeq <= 0) {
    throw new Error("manifest has an invalid nextSeq");
  }
  if (!Array.isArray(manifest.levels)) throw new Error("manifest levels must be an array");
  for (const [level, tables] of manifest.levels.entries()) {
    if (!Array.isArray(tables)) throw new Error(`manifest level ${level} must be an array`);
    for (const table of tables) {
      if (!/^sst-\d+\.(?:json|sst)$/.test(table.file)) throw new Error(`invalid SSTable filename: ${table.file}`);
      if (table.level !== level) throw new Error(`SSTable ${table.file} is assigned to the wrong level`);
      if (compareKeys(table.minKey, table.maxKey) > 0) throw new Error(`SSTable ${table.file} has an invalid key range`);
    }
  }
}

function cloneStoredValue(value: StoredValue): StoredValue {
  validateStoredValue(value, new Set<object>());
  return JSON.parse(JSON.stringify(value)) as StoredValue;
}

function validateStoredValue(value: unknown, ancestors: Set<object>): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("stored numbers must be finite");
    return;
  }
  if (typeof value !== "object") throw new Error("value must be JSON-serializable");
  if (ancestors.has(value)) throw new Error("value must not contain circular references");
  const prototype = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) {
    throw new Error("stored objects must be plain objects");
  }
  ancestors.add(value);
  for (const nested of Array.isArray(value) ? value : Object.values(value)) validateStoredValue(nested, ancestors);
  ancestors.delete(value);
}

function entrySize(entry: Entry): number {
  return Buffer.byteLength(JSON.stringify(entry), "utf8");
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: any) {
    return error?.code !== "ESRCH";
  }
}
