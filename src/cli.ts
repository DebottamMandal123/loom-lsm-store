import { LoomStore } from "./store";

async function main(argv: string[]): Promise<void> {
  const [command, dir = ".loom", key, rawValue, extra] = argv;
  if (!command || command === "help") return usage();

  if (command === "demo") {
    const store = await LoomStore.open(dir, { memtableLimit: 2, level0CompactionThreshold: 3, blockSize: 2 });
    try {
      await store.put("user:1", { name: "Ada", role: "compiler pioneer" });
      await store.put("user:2", { name: "Grace", role: "systems pioneer" });
      await store.put("user:3", { name: "Linus", role: "kernel" });
      await store.delete("user:3");
      await store.put("user:4", { name: "Margaret", role: "flight software" });
      console.log("get user:2 =>", JSON.stringify(await store.get("user:2")));
      console.log("scan user:1..user:9 =>", JSON.stringify(await store.scan({ start: "user:1", end: "user:9" }), null, 2));
      console.log("stats =>", JSON.stringify(store.stats(), null, 2));
      console.log(`database files written to ${dir}`);
    } finally {
      await store.close();
    }
    return;
  }

  const store = await LoomStore.open(dir);
  try {
    if (command === "put") {
      if (!key || rawValue === undefined) throw new Error("put requires <dir> <key> <value>");
      await store.put(key, parseValue(rawValue));
      console.log("ok");
    } else if (command === "get") {
      if (!key) throw new Error("get requires <dir> <key>");
      const value = await store.get(key);
      console.log(value === undefined ? "(nil)" : JSON.stringify(value));
    } else if (command === "delete") {
      if (!key) throw new Error("delete requires <dir> <key>");
      await store.delete(key);
      console.log("ok");
    } else if (command === "flush") {
      await store.flush();
      console.log("ok");
    } else if (command === "compact") {
      await store.compact();
      console.log("ok");
    } else if (command === "stats") {
      console.log(JSON.stringify(store.stats(), null, 2));
    } else if (command === "scan") {
      const rows = await store.scan({ start: key, end: rawValue, limit: extra ? Number(extra) : undefined });
      console.log(JSON.stringify(rows, null, 2));
    } else {
      usage();
      process.exitCode = 1;
    }
  } finally {
    await store.close();
  }
}

function parseValue(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function usage(): void {
  console.log(`Loom LSM store

Usage:
  loom demo <dir>
  loom put <dir> <key> <json-or-string>
  loom get <dir> <key>
  loom delete <dir> <key>
  loom scan <dir> [startKey] [endKey] [limit]
  loom flush <dir>
  loom compact <dir>
  loom stats <dir>`);
}

main(process.argv.slice(2)).catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
