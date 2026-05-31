export function checksum32(data: Buffer | string): number {
  const buffer = typeof data === "string" ? Buffer.from(data, "utf8") : data;
  let hash = 2166136261 >>> 0;
  for (const byte of buffer) {
    hash ^= byte;
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash >>> 0;
}
