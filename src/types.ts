export type StoredValue = string | number | boolean | null | Record<string, unknown> | unknown[];

export interface WalRecord {
  op: "put" | "delete";
  key: string;
  value?: StoredValue;
  seq: number;
}

export interface WalLine {
  record: WalRecord;
  checksum: number;
}

export interface Entry {
  key: string;
  value?: StoredValue;
  seq: number;
  deleted: boolean;
}

export interface BlockIndex {
  firstKey: string;
  lastKey: string;
  offset: number;
  length: number;
  checksum: number;
  count: number;
}

export interface SstableMeta {
  id: number;
  file: string;
  level: number;
  minKey: string;
  maxKey: string;
  count: number;
  minSeq: number;
  maxSeq: number;
  sizeBytes: number;
}

export interface Manifest {
  version: 2;
  nextFileId: number;
  nextSeq: number;
  levels: SstableMeta[][];
}

export interface StoreOptions {
  memtableLimit?: number;
  level0CompactionThreshold?: number;
  levelMaxTables?: number;
  blockSize?: number;
  syncWrites?: boolean;
}

export interface ScanOptions {
  start?: string;
  end?: string;
  limit?: number;
}

export interface StoreStats {
  memtableEntries: number;
  sstables: number;
  diskEntries: number;
  nextFileId: number;
  nextSeq: number;
  levels: Array<{
    level: number;
    tables: number;
    entries: number;
    keyRange: [string, string] | null;
  }>;
}
