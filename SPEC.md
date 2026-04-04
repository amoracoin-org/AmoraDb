# AmoraDB Technical Specification

AmoraDB is an ultra-high-performance, zero-dependency, sharded key-value storage engine built in C and compiled to WebAssembly for Node.js environments.

## 1. Core Architecture

### 1.1 Sharding Strategy
- **Shards**: 64 independent shards (`N_SHARDS = 64`).
- **Parallelism**: Shard-level locking using atomic spinlocks for multi-threaded operations.
- **Distribution**: Keys are hashed using `RapidHash`, and the top 6 bits determine the shard index.

### 1.2 Hash Table (Swiss Table)
- **Design**: Google's Swiss Table inspired design for high cache locality and SIMD probing.
- **Group Size**: 16 slots per group (`GROUP_SIZE = 16`).
- **Control Bytes**: Each slot has a 1-byte control value (Empty `0x80`, Deleted `0xFE`, or 7-bit hash suffix).
- **Probing**: Linear probing with `wasm_simd128` group filtering.

### 1.3 Bloom Filters
- **Memory**: 256 KB per shard (2M bits total per shard).
- **Functions**: 6 hash functions per lookup.
- **Effectiveness**: Near-zero cost for negative lookups (non-existent keys).

### 1.4 Sorted Index (Skip List)
- **Levels**: 16-level probabilistic Skip List.
- **Capacity**: Supports up to 1M nodes.
- **Operations**: Prefix scans and lexicographic range queries.

### 1.5 Memory Management
- **Slab Allocator**: Custom 20-class slab allocator for key/value storage.
- **Classes**: 16B to 1MB size classes.
- **GC/Compaction**: Tombstone-aware garbage collection with a 25% fragmentation threshold.
- **Inline Keys**: Keys ≤ 22 bytes are stored directly within the hash table slot to avoid extra allocations.

## 2. Durability & Integrity

### 2.1 Write-Ahead Log (WAL)
- **Format**: Append-only log with CRC32C checksums for every record.
- **Header**: 8-byte header (`WAL_MAGIC = 0x414D5257`, `WAL_VERSION = 20`).
- **Size Limit**: 32 MB per WAL file.
- **Recovery**: Automatic replay on database open with error reporting for corrupted entries.

### 2.2 Data Integrity
- **Checksums**: Slice-by-4 CRC32C unrolled implementation for high-speed verification.
- **Snapshots**: Full database exports include per-record CRC32C checksums to detect data corruption.

## 3. Concurrency & Performance

### 3.1 Multi-threading
- **Workers**: Up to 8 worker threads using Node.js `worker_threads`.
- **Memory**: Shared `SharedArrayBuffer` memory between main thread and workers.
- **Synchronization**: `stdatomic.h` with `acquire/release/relaxed` memory ordering for lock-free counters and spinlocks.

### 3.2 Performance Targets
- **Writes**: 1.5M+ ops/s (in-memory, single node).
- **Reads**: 1.7M+ ops/s (in-memory, single node).
- **Latency**: Sub-microsecond access times for small key/value pairs.

## 4. API Specification

### 4.1 Core Methods
- `set(key, value)`: Atomic insertion or update.
- `get(key)`: Fast retrieval with Bloom filter bypass for negative hits.
- `has(key)`: Bloom filter check.
- `delete(key)`: Logical deletion with tombstone marking.
- `batch(ops[])`: ACID-like atomic batch operations with rollback capability.

### 4.2 Scanning
- `scan(prefix)`: Prefix-based range scan.
- `range(from, to)`: Inclusive lexicographic range scan.

## 5. Limits & Constraints

- **Maximum Key Size**: 4,096 bytes (4 KB).
- **Maximum Value Size**: 1,048,576 bytes (1 MB).
- **Inline Key Fast Path**: ≤ 22 bytes.
- **Max Threads**: 8.
- **Max WAL Size**: 32 MB.
- **Max Shards**: 64.
- **Max Memory**: Up to 4GB (WASM limit).

## 6. Binary Format (WAL/Snapshots)

- **WAL Record**: `[Type:1][KLen:2][VLen:4][Key:KLen][Value:VLen][CRC:4]`
- **Snapshot Header**: `[Magic:4][Version:4][Count:4][Reserved:20]`
- **Snapshot Record**: `[KLen:2][VLen:4][Key:KLen][Value:VLen][CRC:4]`
