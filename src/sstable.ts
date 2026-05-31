import { promises as fs } from "node:fs";
import path from "node:path";
import { BloomFilter } from "./bloom";
import { checksum32 } from "./checksum";
import { atomicWriteFile } from "./io";
import { BlockIndex, Entry, SstableMeta } from "./types";

interface SstableFile {
  version: 2;
  id: number;
  level: number;
  minKey: string;
  maxKey: string;
  count: number;
  minSeq: number;
  maxSeq: number;
  bloom: ReturnType<BloomFilter["toJSON"]>;
  blocks: BlockIndex[];
}

const MAGIC = Buffer.from("LOOMST1\n", "utf8");
const FOOTER_MAGIC = Buffer.from("LOOMFT1\n", "utf8");
const TRAILER_LENGTH = 4 + FOOTER_MAGIC.length;

export async function writeSstable(
  dir: string,
  id: number,
  level: number,
  entries: Entry[],
  blockSize: number,
): Promise<SstableMeta> {
  if (entries.length === 0) throw new Error("cannot write an empty SSTable");
  const sorted = [...entries].sort((a, b) => a.key.localeCompare(b.key));
  const bloom = new BloomFilter(Math.max(2048, sorted.length * 16), 4);
  sorted.forEach((entry) => bloom.add(entry.key));

  const blocks: BlockIndex[] = [];
  const blockBuffers: Buffer[] = [];
  let offset = MAGIC.length;
  for (let i = 0; i < sorted.length; i += blockSize) {
    const chunk = sorted.slice(i, i + blockSize);
    const buffer = Buffer.from(JSON.stringify(chunk), "utf8");
    blocks.push({
      firstKey: chunk[0].key,
      lastKey: chunk[chunk.length - 1].key,
      offset,
      length: buffer.length,
      checksum: checksum32(buffer),
      count: chunk.length,
    });
    blockBuffers.push(buffer);
    offset += buffer.length;
  }

  const file = `sst-${String(id).padStart(6, "0")}.json`;
  const body: SstableFile = {
    version: 2,
    id,
    level,
    minKey: sorted[0]?.key ?? "",
    maxKey: sorted[sorted.length - 1]?.key ?? "",
    count: sorted.length,
    minSeq: Math.min(...sorted.map((entry) => entry.seq)),
    maxSeq: Math.max(...sorted.map((entry) => entry.seq)),
    bloom: bloom.toJSON(),
    blocks,
  };
  const footer = Buffer.from(JSON.stringify(body), "utf8");
  const footerLength = Buffer.alloc(4);
  footerLength.writeUInt32LE(footer.length, 0);
  const payload = Buffer.concat([MAGIC, ...blockBuffers, footer, footerLength, FOOTER_MAGIC]);
  await atomicWriteFile(path.join(dir, file), payload);
  return {
    id,
    file,
    level,
    minKey: body.minKey,
    maxKey: body.maxKey,
    count: body.count,
    minSeq: body.minSeq,
    maxSeq: body.maxSeq,
    sizeBytes: payload.length,
  };
}

export async function findInSstable(dir: string, meta: SstableMeta, key: string): Promise<Entry | undefined> {
  if (key < meta.minKey || key > meta.maxKey) return undefined;
  const table = await readFooter(dir, meta.file);
  const bloom = BloomFilter.fromJSON(table.bloom);
  if (!bloom.mightContain(key)) return undefined;
  const block = findBlock(table.blocks, key);
  if (!block) return undefined;
  const entries = await readBlock(dir, meta.file, block);
  const found = binarySearch(entries, key);
  return found >= 0 ? entries[found] : undefined;
}

export async function readEntries(dir: string, meta: SstableMeta): Promise<Entry[]> {
  return scanSstable(dir, meta);
}

export async function scanSstable(
  dir: string,
  meta: SstableMeta,
  start?: string,
  end?: string,
): Promise<Entry[]> {
  if ((start && meta.maxKey < start) || (end && meta.minKey > end)) return [];
  const table = await readFooter(dir, meta.file);
  const out: Entry[] = [];
  for (const block of table.blocks) {
    if ((start && block.lastKey < start) || (end && block.firstKey > end)) continue;
    const entries = await readBlock(dir, meta.file, block);
    for (const entry of entries) {
      if ((!start || entry.key >= start) && (!end || entry.key <= end)) out.push(entry);
    }
  }
  return out;
}

export async function inspectSstable(dir: string, meta: SstableMeta): Promise<SstableFile> {
  return readFooter(dir, meta.file);
}

async function readFooter(dir: string, file: string): Promise<SstableFile> {
  const fullPath = path.join(dir, file);
  const handle = await fs.open(fullPath, "r");
  try {
    const stat = await handle.stat();
    if (stat.size < MAGIC.length + TRAILER_LENGTH) throw new Error(`${file} is too small to be an SSTable`);
    const magic = Buffer.alloc(MAGIC.length);
    await handle.read(magic, 0, magic.length, 0);
    if (!magic.equals(MAGIC)) throw new Error(`${file} has an invalid SSTable header`);

    const trailer = Buffer.alloc(TRAILER_LENGTH);
    await handle.read(trailer, 0, trailer.length, stat.size - TRAILER_LENGTH);
    if (!trailer.subarray(4).equals(FOOTER_MAGIC)) throw new Error(`${file} has an invalid SSTable footer`);
    const footerLength = trailer.readUInt32LE(0);
    const footerOffset = stat.size - TRAILER_LENGTH - footerLength;
    if (footerOffset < MAGIC.length) throw new Error(`${file} has an invalid footer length`);

    const footer = Buffer.alloc(footerLength);
    await handle.read(footer, 0, footer.length, footerOffset);
    return JSON.parse(footer.toString("utf8")) as SstableFile;
  } finally {
    await handle.close();
  }
}

async function readBlock(dir: string, file: string, block: BlockIndex): Promise<Entry[]> {
  const handle = await fs.open(path.join(dir, file), "r");
  try {
    const buffer = Buffer.alloc(block.length);
    await handle.read(buffer, 0, block.length, block.offset);
    const actual = checksum32(buffer);
    if (actual !== block.checksum) {
      throw new Error(`${file} block checksum mismatch at offset ${block.offset}`);
    }
    return JSON.parse(buffer.toString("utf8")) as Entry[];
  } finally {
    await handle.close();
  }
}

function findBlock(blocks: BlockIndex[], key: string): BlockIndex | undefined {
  let low = 0;
  let high = blocks.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const block = blocks[mid];
    if (key < block.firstKey) high = mid - 1;
    else if (key > block.lastKey) low = mid + 1;
    else return block;
  }
  return undefined;
}

function binarySearch(entries: Entry[], key: string): number {
  let low = 0;
  let high = entries.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const cmp = entries[mid].key.localeCompare(key);
    if (cmp === 0) return mid;
    if (cmp < 0) low = mid + 1;
    else high = mid - 1;
  }
  return -1;
}
