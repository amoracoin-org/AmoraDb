<div align="center">

<img src="https://github.com/amoracoin-org/AmoraDb/blob/main/assets/amoradb-logo.jpeg" alt="AmoraDB" width="180"/>

# AmoraDB

**Ultra-High-Performance Embedded Key-Value Engine**

*Built in C · Compiled to WebAssembly · Bound to Node.js*

[![License: MIT](https://img.shields.io/badge/License-MIT-blueviolet.svg)](#license)
[![WebAssembly](https://img.shields.io/badge/WebAssembly-SIMD128-654ff0?logo=webassembly)](https://webassembly.org/)
[![Node.js](https://img.shields.io/badge/Node.js-Binding-339933?logo=nodedotjs)](https://nodejs.org/)
[![Version](https://img.shields.io/badge/version-2.0-ff69b4)](#)

</div>

---

### 📖 Documentation

- **[CHANGELOG.md](CHANGELOG.md)**: Track all notable changes and version history.
- **[CONTRIBUTING.md](CONTRIBUTING.md)**: Guide for setting up development environment and contributing code.
- **[SPEC.md](SPEC.md)**: Deep dive into the internal architecture, sharding, and binary formats.

---

AmoraDB is a hand-crafted, zero-dependency key-value store written entirely in C and compiled to WebAssembly. It is designed for scenarios where you need **millions of operations per second**, atomic batch writes, WAL-backed durability, and a minimal footprint — all inside a Node.js process with no native addons.

It outperforms LevelDB and competes with LMDB at in-memory workloads, with the portability advantage of running anywhere WebAssembly runs.

---

## ✨ Features

| Capability | Detail |
|---|---|
| **Hash Map** | Swiss Table design with SIMD-accelerated group probing (`wasm_simd128`) |
| **Sharding** | 64 independent shards for lock-free parallelism |
| **Bloom Filters** | 256 KB per shard (2M bits) for zero-cost negative lookups |
| **Skip List** | 16-level sorted index, supports billions of entries |
| **WAL** | Append-only Write-Ahead Log with CRC32C integrity, up to 32 MB |
| **Snapshots** | Full export/import with per-record CRC32C checksums |
| **Atomic Batches** | ACID-like batch writes with rollback on failure |
| **Worker Threads** | Shared `SharedArrayBuffer` memory across up to 8 threads |
| **Slab Allocator** | 32 size classes, `SLAB_MIN_SIZE` 16B → `SLAB_MAX_SIZE` per class |
| **GC / Compaction** | Tombstone-aware compaction with auto-compact threshold |
| **Large Values** | Up to 1 MB per value (16× previous limit) |
| **Large Keys** | Up to 4 KB per key, with 22-byte inline fast path |

---

## 📊 Performance

Benchmarks run with the built-in `db.bench(1_000_000)` harness (C-level, 1M operations, in-memory):

| Operation | Throughput |
|---|---|
| Write | ~1.5M+ ops/s |
| Read | ~1.7M+ ops/s |
| Delete | ~1.7M+ ops/s |

### Comparison (in-memory, single node)

| Engine | Write/s | Read/s | Notes |
|---|---|---|---|
| **AmoraDB v2.0** | ~1.5M+ | ~1.7M+ | WASM · SIMD · 64 shards |
| LMDB | ~1.2M | ~2.0M | mmap · B+Tree · C addon |
| RocksDB | ~700K | ~900K | LSM · disk-tuned · C addon |
| LevelDB | ~400K | ~600K | LSM · disk · C addon |
| Redis (local) | ~500K | ~800K | TCP overhead · RAM |

### Internal benchmark 1,000,000 ops (pure C)

The internal benchmark measures raw performance at the C level, bypassing the Node.js bridge.

### Real-World Benchmark (Node.js)

For a more accurate measure of performance including JavaScript overhead and real-world latency, run:

```bash
node benchmark.js
```

This benchmark covers:
- **JS <-> WASM Bridge**: Real overhead of calling the engine from Node.js.
- **Latency (P50/P99)**: Tracks sub-millisecond response times.
- **Concurrency**: Parallel execution across worker threads.
- **Persistence**: Impact of WAL durability (Async vs Sync).

> Benchmarks are illustrative. Results vary by hardware, key size, value size, and access pattern.
>
> ⚠️ Note: LevelDB and RocksDB are disk-first engines optimized for persistence and compaction. Redis includes TCP overhead. This comparison reflects raw in-process throughput only — not overall capability. Choose the right tool for your use case.
---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────┐
│                   amora.js                      │  Node.js Binding
│  LRU string cache · encodeInto · SharedArrayBuf │
│  Command buffer (512KB) · Worker pool (8 max)   │
└────────────────────┬────────────────────────────┘
                     │  WebAssembly
┌────────────────────▼────────────────────────────┐
│                amora_core.c → .wasm             │  Core Engine
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Shard 0  │  │ Shard 1  │  │  ...63   │      │  64 Shards
│  │ SwissMap │  │ SwissMap │  │ SwissMap │      │
│  │ Bloom    │  │ Bloom    │  │ Bloom    │      │  256KB Bloom/shard
│  └──────────┘  └──────────┘  └──────────┘      │
│                                                 │
│  ┌──────────────────────────────────────────┐   │
│  │         Skip List  (16 levels)           │   │  Sorted index
│  └──────────────────────────────────────────┘   │
│                                                 │
│  ┌──────────────────────────────────────────┐   │
│  │    WAL  (CRC32C · 32MB · append-only)    │   │  Durability
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

**Hash function:** [RapidHash](https://github.com/Nicoshev/rapidhash) — excellent distribution, minimal collisions.

**Integrity:** CRC32C with a 256-entry lookup table, processed 4 bytes per iteration (slice-by-4 unroll).

**Atomics:** When compiled with `__wasm_threads__`, all shard counters use `_Atomic` types with explicit memory ordering (acquire/release/relaxed).

---

## 🚀 Getting Started

### Prerequisites

- Node.js ≥ 18
- A pre-compiled `amora_core_mt_simd.wasm` (or build from source — see below)

### Installation

```bash
npm install  # no native addons required
```

### Basic Usage

```js
const AmoraDB = require('./amora.js');

// Open a database (in-memory + optional WAL persistence)
const db = AmoraDB.open(null, {
  threads:  4,
  cap:      65536,      // Initial capacity (entries)
  walPath:  './my.wal', // Omit for pure in-memory
  walSync:  true,       // fsync on every WAL flush
});

// Basic operations
db.set('user:1', 'alice');
db.get('user:1');         // → 'alice'
db.has('user:1');         // → true
db.delete('user:1');

// Multi-get
const results = db.mget(['user:1', 'user:2', 'missing']);
// → ['alice', 'bob', null]

// Prefix scan
const entries = db.scan('user:');
// → [{ key: 'user:1', value: 'alice' }, ...]

// Range scan
const range = db.range('user:1', 'user:9');
```

### Atomic Batch Writes

```js
db.batch([
  { op: 'set',    key: 'account:1', value: '500' },
  { op: 'set',    key: 'account:2', value: '300' },
  { op: 'delete', key: 'account:old' },
]);
// All succeed or all roll back — no partial writes.
```

### Buffered Writes (High Throughput)

```js
// setBuffered queues writes in a 512KB command buffer
for (let i = 0; i < 100_000; i++) {
  db.setBuffered(`key:${i}`, `value:${i}`);
}
db.flush(); // Commit all at once
```

### Snapshot Export / Import

```js
// Export entire database to a Buffer
const snapshot = db.export(128 * 1024 * 1024); // 128MB max

// Import into a fresh instance
const db2 = AmoraDB.open(null, { cap: 65536 });
const count = db2.import(snapshot);
console.log(`Imported ${count} entries`);
```

---

## ⚙️ Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `threads` | `number` | `1` | Number of worker threads (max 8) |
| `cap` | `number` | `65536` | Initial hash map capacity per shard |
| `walPath` | `string\|null` | `null` | Path for WAL file. `null` = no persistence |
| `walSync` | `boolean` | `true` | `fsync` after every WAL flush |

---

## 🔧 API Reference

### Core Operations

| Method | Returns | Description |
|---|---|---|
| `db.heartbeat()` | `boolean` | Check if WASM core is responsive |
| `db.set(key, value)` | `void` | Insert or update a key |
| `db.get(key)` | `string\|null` | Retrieve a value |
| `db.has(key)` | `boolean` | Check existence (uses bloom filter) |
| `db.delete(key)` | `void` | Remove a key |
| `db.setBuffered(key, value)` | `void` | Buffered write (flush manually) |
| `db.mget(keys[])` | `(string\|null)[]` | Batch get (up to 8192 keys) |
| `db.batch(ops[])` | `void` | Atomic multi-operation batch |
| `db.scan(prefix)` | `{key,value}[]` | All keys with given prefix |
| `db.range(from, to)` | `{key,value}[]` | Lexicographic range scan |

### Maintenance

| Method | Returns | Description |
|---|---|---|
| `db.gc()` | `number` | Compact tombstones across all shards |
| `db.autoCompact()` | `number` | GC if fragmentation > 25% |
| `db.fragmentation()` | `number` | Current fragmentation percentage |
| `db.flush()` | `void` | Flush pending command buffer |
| `db.persist()` | `void` | Flush + WAL write |
| `db.restore()` | `void` | Reload from WAL |
| `db.reset(cap?)` | `void` | Wipe and reinitialize |
| `db.close()` | `Promise<void>` | Async: flush, persist, terminate workers |
| `db.import(buf)` | `number` | Import snapshot buffer |
| `db.export(max?)` | `Buffer` | Export database to snapshot buffer |

### Observability

```js
const s = db.stats();
// {
//   count:          number,   // Live entries
//   capacity:       number,   // Allocated slots
//   deleted:        number,   // Tombstone count
//   shards:         64,
//   threads:        number,
//   load:           '73.2%',
//   fragmentation:  '12%',
//   hit_rate:       '98.7%',
//   total_ops:      number,
//   write_errors:   number,
//   wal_errors:     number,
//   compactions:    number,
//   arena_kb:       string,
//   wal_kb:         string,
//   wasm_mb:        string,
//   mem_shared:     boolean,
// }
```

### Benchmark

```js
const b = db.bench(1_000_000);
// {
//   ops:          1000000,
//   write_ms:     '312ms',   write_ops_s:  '3.20M',
//   read_ms:      '276ms',   read_ops_s:   '3.62M',
//   delete_ms:    '289ms',   delete_ops_s: '1.73M',
//   scan_ms:      '1.42ms',
// }
```

---

## 🔒 Limits & Safety

| Constraint | Limit |
|---|---|
| Maximum key size | 4,096 bytes |
| Maximum value size | 1,048,576 bytes (1 MB) |
| Inline key fast path | ≤ 22 bytes (zero heap allocation) |
| Shards | 64 |
| Worker threads | 8 |
| Scan results per call | 4,096 |
| WAL max size | 32 MB |

All limits are validated on the JavaScript side before reaching WASM. Violations throw a `RangeError` immediately.

---

## 🛠 Building from Source

AmoraDB core is written in standard C99 with optional SIMD and atomics extensions for WebAssembly. Build with [Emscripten](https://emscripten.org/) or [wasi-sdk](https://github.com/WebAssembly/wasi-sdk):

```bash
clang --target=wasm32 -O3 -nostdlib -std=c11 \
  -msimd128 -matomics -mbulk-memory \
  -fvisibility=hidden -ffunction-sections -fdata-sections \
  -Wl,--no-entry -Wl,--export-dynamic \
  -Wl,--import-memory -Wl,--shared-memory \
  -Wl,--max-memory=4294967296 \
  -Wl,--gc-sections -Wl,--allow-undefined -Wl,--lto-O3 \
  -flto -DCACHE_LINE=256 \
  -o amora_core_mt_simd.wasm amora_core.c
```

> The `-msimd128`, `-matomics`, and `-mbulk-memory` flags enable full multi-threaded SIMD mode. Remove them to build a single-threaded fallback.

---

## 📁 Project Structure

```
amora/
├── amora_core.c      # Core engine — hash map, WAL, bloom, skip list, slab
├── amora_core_mt_simd.wasm   # Compiled WebAssembly binary — multi-threaded, SIMD acceleration
├── amora.js          # Node.js binding — workers, caching, serialization
└── test.js          # Full test suite with stress and benchmarks
```

---

## 🧪 Running Tests

```bash
node test.js
```

The test suite covers: heartbeat, set/get/has/delete, large values (512 KB), key/value size validation, update / 1000 rewrites, mget, atomic batch, batch rollback (single and repeated), prefix scan, fragmentation + GC, snapshot export/import with CRC corruption detection, stats, stress (100K keys), and the internal 1M-op C benchmark.

---

## 📄 License

MIT © AmoraDB Authors

---

<div align="center">
  <sub>Built with obsession for performance. No dependencies. No compromises.</sub>
</div>

