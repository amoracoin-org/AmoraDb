# AmoraDB Technical Specification

AmoraDB is an ultra-high-performance, sharded key-value storage engine written in C and shipped as a native Node.js addon (N-API).

This specification describes the current native implementation located under `npm/`.

## 1. Architecture

### 1.1 Components
- **JavaScript wrapper**: Loads the compiled addon and exposes a small API surface ([npm/index.js](npm/index.js)).
- **Native addon**: Implements the storage engine and N-API bindings ([native.c](npm/src/native.c)).

### 1.2 Sharding Strategy
- **Shard count**: 64 shards (`N_SHARDS = 64`).
- **Shard selection**: `HSHARD(hash)` maps the key hash to a shard index using the upper bits.
- **Locking**: Per-shard spinlock to protect updates and reads (`SPIN_LOCK` / `SPIN_UNLOCK`).

### 1.3 Hashing
- **Hash function**: RapidHash-inspired mixing producing a 32-bit hash (`hash32()`).
- **Fingerprint**: `H2(hash)` stores a 7-bit fingerprint in the control byte to filter candidates during probing.
- **Probe start**: `H1(hash)` picks the initial bucket index (masked to capacity).

### 1.4 Hash Table Layout (Swiss Table-inspired)
- **Control bytes**:
  - `CTRL_EMPTY = 0x80`
  - `CTRL_DELETED = 0xFE`
  - Occupied: stores fingerprint (`H2(hash)`).
- **Group probing**: `GROUP_SIZE = 16`. The implementation scans groups linearly.
- **Slots**: Each bucket has a `Slot` storing key/value pointers and lengths.

### 1.5 Bloom Filters
- **Per-shard bloom**: 2,097,152 bits (256 KB) per shard (`BLOOM_BITS = 2 * 1024 * 1024`).
- **Hash functions**: 2 derived bit positions from the 32-bit hash.
- **Behavior**: Used as a fast negative filter before probing the hash table.

## 2. Memory Management

### 2.1 Heap
- **Allocator**: A bump allocator backed by a non-moving chunked arena (`malloc`), aligned to 16 bytes.
- **Lifecycle**: `db_init()` resets the heap by freeing all allocations and reinitializing shards.

### 2.2 Slab Pooling
- **Size classes**: `SLAB_CLASSES = 20`.
- **Strategy**: Per-class free lists protected by a global slab spinlock.
- **Goal**: Reduce allocation overhead for key/value storage.

## 3. Concurrency

### 3.1 Locking Model
- **Per-shard spinlocks** are used to serialize access to each shard.
- **Counters** (`hits`, `misses`, `ops`) are incremented atomically on Windows and using GCC builtins elsewhere.

## 4. Native API (JavaScript)

### 4.1 Construction
- `AmoraDB.open({ cap })`: Initializes the database with an initial capacity.

### 4.2 Operations
- `db.set(key, value) -> boolean`
- `db.get(key) -> string | null`
- `db.has(key) -> boolean`
- `db.delete(key) -> boolean`

### 4.3 Introspection
- `db.count() -> number`
- `db.capacity() -> number`
- `db.hits() -> number`
- `db.misses() -> number`
- `db.ops() -> number`
- `db.stats() -> object`
- `db.reset(cap?) -> boolean`
- `db.heartbeat() -> boolean`
- `db.bench(n?) -> object`

`db.stats()` fields include:
- `count`, `capacity`, `hits`, `misses`, `shards`
- `total_ops` (all API calls)
- `set_ops`, `get_ops`, `has_ops`, `delete_ops`

## 5. Limits & Constraints

- **Maximum key size**: 4,096 bytes.
- **Maximum value size**: 1,048,576 bytes (1 MB).
- **Inline key threshold**: 22 bytes.
- **Shards**: 64.

## 6. Build Notes

- **Windows**: Requires Visual Studio Build Tools / Visual Studio with “Desktop development with C++”.
- **macOS/Linux**: Requires a working toolchain (clang/gcc) and Python for node-gyp.
