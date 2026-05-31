import { promises as fs } from "node:fs";
import path from "node:path";
import { checksum32 } from "./checksum";
import { appendAndSync, atomicWriteFile, truncateAndSync } from "./io";
import { findInSstable, readEntries, scanSstable, writeSstable } from "./sstable";
import { Entry, Manifest, ScanOptions, StoreOptions, StoredValue, StoreStats, SstableMeta, WalLine, WalRecord } from "./types";

const DEFAULT_OPTIONS: Required<StoreOptions> = {
  memtableLimit: 100,
  level0CompactionThreshold: 4,
  levelMaxTables: 6,
  blockSize: 16,
  syncWrites: true,
};

export class LoomStore {
  private readonly dir: string;
  private readonly walPath: string;
  private readonly manifestPath: string;
  private readonly options: Required<StoreOptions>;
  private manifest: Manifest = { version: 2, nextFileId: 1, nextSeq: 1, levels: [[]] };
  private memtable = new Map<string, Entry>();
  private writeQueue: Promise<void> = Promise.resolve();
  private closed = false;

  private constructor(dir: string, options: StoreOptions = {}) {
    this.dir = dir;
    this.walPath = path.join(dir, "wal.log");
    this.manifestPath = path.join(dir, "MANIFEST.json");
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  static async open(dir: string, options: StoreOptions = {}): Promise<LoomStore> {
    const store = new LoomStore(dir, options);
    await fs.mkdir(dir, { recursive: true });
    await store.loadManifest();
    await store.recoverWal();
    return store;
  }

  async put(key: string, value: StoredValue): Promise<void> {
    this.assertKey(key);
    await this.enqueueWrite(async () => {
      this.assertOpen();
      const seq = this.manifest.nextSeq++;
      await this.appendWal({ op: "put", key, value, seq });
      this.memtable.set(key, { key, value, seq, deleted: false });
      await this.flushIfNeeded();
    });
  }

  async get(key: string): Promise<StoredValue | undefined> {
    this.assertKey(key);
    await this.writeQueue;
    const entry = await this.findLatestEntry(key);
    return !entry || entry.deleted ? undefined : entry.value;
  }

  async delete(key: string): Promise<void> {
    this.assertKey(key);
    await this.enqueueWrite(async () => {
      this.assertOpen();
      const seq = this.manifest.nextSeq++;
      await this.appendWal({ op: "delete", key, seq });
      this.memtable.set(key, { key, seq, deleted: true });
      await this.flushIfNeeded();
    });
  }

  async scan(options: ScanOptions = {}): Promise<Array<{ key: string; value: StoredValue }>> {
    await this.writeQueue;
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
      .sort((a, b) => a.key.localeCompare(b.key))
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
      sstables: this.allTables().length,
      diskEntries: this.allTables().reduce((sum, table) => sum + table.count, 0),
      nextFileId: this.manifest.nextFileId,
      nextSeq: this.manifest.nextSeq,
      levels,
    };
  }

  async close(): Promise<void> {
    await this.enqueueWrite(async () => {
      if (!this.closed) {
        await this.flushMemtable();
        this.closed = true;
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
      .sort((a, b) => a.key.localeCompare(b.key));
  }

  private async removeTables(tables: SstableMeta[]): Promise<void> {
    await Promise.all(tables.map((table) => fs.rm(path.join(this.dir, table.file), { force: true })));
  }

  private async loadManifest(): Promise<void> {
    try {
      const parsed = JSON.parse(await fs.readFile(this.manifestPath, "utf8"));
      if (parsed.version === 2 && Array.isArray(parsed.levels)) {
        this.manifest = parsed as Manifest;
      } else {
        this.manifest = {
          version: 2,
          nextFileId: parsed.nextId ?? 1,
          nextSeq: 1,
          levels: [parsed.tables ?? []],
        };
      }
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      await this.saveManifest();
    }
    if (this.manifest.levels.length === 0) this.manifest.levels = [[]];
  }

  private async recoverWal(): Promise<void> {
    try {
      const content = await fs.readFile(this.walPath, "utf8");
      for (const line of content.split(/\r?\n/)) {
        if (!line.trim()) continue;
        let parsed: WalLine;
        try {
          parsed = JSON.parse(line) as WalLine;
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
    if (this.memtable.size >= this.options.memtableLimit) await this.flushMemtable();
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
  return [...tables].sort((a, b) => a.minKey.localeCompare(b.minKey));
}

function minString(values: string[]): string | undefined {
  return values.length ? values.reduce((min, value) => (value < min ? value : min)) : undefined;
}

function maxString(values: string[]): string | undefined {
  return values.length ? values.reduce((max, value) => (value > max ? value : max)) : undefined;
}
