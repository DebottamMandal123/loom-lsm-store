"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const { BloomFilter } = require("./bloom");
const { checksum32 } = require("./checksum");
const { atomicWriteFile } = require("./io");
const { compareKeys } = require("./key-order");

const MAGIC = Buffer.from("LOOMST2\n", "utf8");
const FOOTER_MAGIC = Buffer.from("LOOMFT2\n", "utf8");
const LEGACY_MAGIC = Buffer.from("LOOMST1\n", "utf8");
const LEGACY_FOOTER_MAGIC = Buffer.from("LOOMFT1\n", "utf8");
const TRAILER_LENGTH = 8 + FOOTER_MAGIC.length;
const LEGACY_TRAILER_LENGTH = 4 + LEGACY_FOOTER_MAGIC.length;

async function writeSstable(dir, id, level, entries, blockSize) {
  if (entries.length === 0) throw new Error("cannot write an empty SSTable");
  const sorted = [...entries].sort((a, b) => compareKeys(a.key, b.key));
  const bloom = new BloomFilter(Math.max(2048, sorted.length * 16), 4);
  sorted.forEach((entry) => bloom.add(entry.key));

  const blocks = [];
  const blockBuffers = [];
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

  const file = `sst-${String(id).padStart(6, "0")}.sst`;
  const body = {
    version: 3,
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
  const footerChecksum = Buffer.alloc(4);
  footerChecksum.writeUInt32LE(checksum32(footer), 0);
  const footerLength = Buffer.alloc(4);
  footerLength.writeUInt32LE(footer.length, 0);
  const payload = Buffer.concat([MAGIC, ...blockBuffers, footer, footerChecksum, footerLength, FOOTER_MAGIC]);
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

async function findInSstable(dir, meta, key) {
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

async function readEntries(dir, meta) {
  return scanSstable(dir, meta);
}

async function scanSstable(dir, meta, start, end) {
  if ((start && meta.maxKey < start) || (end && meta.minKey > end)) return [];
  const table = await readFooter(dir, meta.file);
  const out = [];
  for (const block of table.blocks) {
    if ((start && block.lastKey < start) || (end && block.firstKey > end)) continue;
    const entries = await readBlock(dir, meta.file, block);
    for (const entry of entries) {
      if ((!start || entry.key >= start) && (!end || entry.key <= end)) out.push(entry);
    }
  }
  return out;
}

async function inspectSstable(dir, meta) {
  return readFooter(dir, meta.file);
}

async function readFooter(dir, file) {
  const fullPath = path.join(dir, file);
  const handle = await fs.open(fullPath, "r");
  try {
    const stat = await handle.stat();
    if (stat.size < MAGIC.length + LEGACY_TRAILER_LENGTH) throw new Error(`${file} is too small to be an SSTable`);
    const magic = Buffer.alloc(MAGIC.length);
    await readExact(handle, magic, 0);
    const current = magic.equals(MAGIC);
    const legacy = magic.equals(LEGACY_MAGIC);
    if (!current && !legacy) throw new Error(`${file} has an invalid SSTable header`);

    const trailerLength = current ? TRAILER_LENGTH : LEGACY_TRAILER_LENGTH;
    const trailer = Buffer.alloc(trailerLength);
    await readExact(handle, trailer, stat.size - trailerLength);
    const footerMagic = current ? FOOTER_MAGIC : LEGACY_FOOTER_MAGIC;
    const magicOffset = current ? 8 : 4;
    if (!trailer.subarray(magicOffset).equals(footerMagic)) throw new Error(`${file} has an invalid SSTable footer`);
    const footerLength = trailer.readUInt32LE(current ? 4 : 0);
    const footerOffset = stat.size - trailerLength - footerLength;
    if (footerOffset < MAGIC.length) throw new Error(`${file} has an invalid footer length`);

    const footer = Buffer.alloc(footerLength);
    await readExact(handle, footer, footerOffset);
    if (current && checksum32(footer) !== trailer.readUInt32LE(0)) {
      throw new Error(`${file} footer checksum mismatch`);
    }
    return JSON.parse(footer.toString("utf8"));
  } finally {
    await handle.close();
  }
}

async function readBlock(dir, file, block) {
  const handle = await fs.open(path.join(dir, file), "r");
  try {
    const buffer = Buffer.alloc(block.length);
    await readExact(handle, buffer, block.offset);
    const actual = checksum32(buffer);
    if (actual !== block.checksum) throw new Error(`${file} block checksum mismatch at offset ${block.offset}`);
    return JSON.parse(buffer.toString("utf8"));
  } finally {
    await handle.close();
  }
}

function findBlock(blocks, key) {
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

function binarySearch(entries, key) {
  let low = 0;
  let high = entries.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const cmp = compareKeys(entries[mid].key, key);
    if (cmp === 0) return mid;
    if (cmp < 0) low = mid + 1;
    else high = mid - 1;
  }
  return -1;
}

async function readExact(handle, buffer, position) {
  let offset = 0;
  while (offset < buffer.length) {
    const { bytesRead } = await handle.read(buffer, offset, buffer.length - offset, position + offset);
    if (bytesRead === 0) throw new Error("unexpected end of SSTable");
    offset += bytesRead;
  }
}

module.exports = { writeSstable, findInSstable, readEntries, scanSstable, inspectSstable };
