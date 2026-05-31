import { promises as fs } from "node:fs";
import path from "node:path";

export async function atomicWriteFile(file: string, data: Buffer | string): Promise<void> {
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

export async function appendAndSync(file: string, data: string, sync: boolean): Promise<void> {
  const handle = await fs.open(file, "a");
  try {
    await handle.writeFile(data, "utf8");
    if (sync) await handle.sync();
  } finally {
    await handle.close();
  }
}

export async function truncateAndSync(file: string): Promise<void> {
  const handle = await fs.open(file, "w");
  try {
    await handle.truncate(0);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export async function fsyncParent(file: string): Promise<void> {
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
