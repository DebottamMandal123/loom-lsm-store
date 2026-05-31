"use strict";

class BloomFilter {
  constructor(size = 2048, hashes = 4, bits) {
    this.size = size;
    this.hashes = hashes;
    this.bits = bits ?? new Uint8Array(Math.ceil(size / 8));
  }

  add(value) {
    for (const bit of this.locations(value)) this.set(bit);
  }

  mightContain(value) {
    for (const bit of this.locations(value)) {
      if (!this.get(bit)) return false;
    }
    return true;
  }

  toJSON() {
    return {
      size: this.size,
      hashes: this.hashes,
      bits: Buffer.from(this.bits).toString("base64"),
    };
  }

  static fromJSON(data) {
    return new BloomFilter(data.size, data.hashes, Uint8Array.from(Buffer.from(data.bits, "base64")));
  }

  locations(value) {
    const h1 = fnv1a(value, 2166136261);
    const h2 = fnv1a(value, 16777619);
    const out = [];
    for (let i = 0; i < this.hashes; i += 1) out.push(Math.abs((h1 + i * h2 + i * i) % this.size));
    return out;
  }

  set(index) {
    this.bits[index >> 3] |= 1 << (index & 7);
  }

  get(index) {
    return (this.bits[index >> 3] & (1 << (index & 7))) !== 0;
  }
}

function fnv1a(value, seed) {
  let hash = seed >>> 0;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash;
}

module.exports = { BloomFilter };
