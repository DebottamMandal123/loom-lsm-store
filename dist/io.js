"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");

async function atomicWriteFile(file, data) {
  const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data, "utf8");
  const tmp = `${file}.tmp-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const handle = await fs.open(tmp, "w");
  try {
    await handle.writeFile(buffer);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fs.rename(tmp, file);
  await fsyncParent(file);
}

async function appendAndSync(file, data, sync) {
  const handle = await fs.open(file, "a");
  try {
    await handle.writeFile(data, "utf8");
    if (sync) await handle.sync();
  } finally {
    await handle.close();
  }
}

async function truncateAndSync(file) {
  await truncateToAndSync(file, 0);
}

async function truncateToAndSync(file, length) {
  const handle = await fs.open(file, length === 0 ? "w" : "r+");
  try {
    await handle.truncate(length);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function fsyncParent(file) {
  try {
    const handle = await fs.open(path.dirname(file), "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch {
    // Directory fsync is platform-dependent. Atomic rename still gives the main safety property.
  }
}

module.exports = { atomicWriteFile, appendAndSync, truncateAndSync, truncateToAndSync, fsyncParent };
