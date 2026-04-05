# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- npm install: use prebuilt `.node` binaries when available, otherwise build via `node-gyp`.

## [2.0.7] - 2026-04-04

### Changed
- Native addon: `stats()` now includes per-operation counters (`set_ops`, `get_ops`, `has_ops`, `delete_ops`) and `total_ops` counts all API calls.

## [2.0.6] - 2026-04-04

### Fixed
- Android/Termux: define `android_ndk_path` gyp variable to avoid `gyp: Undefined variable android_ndk_path` during configure.

## [2.0.5] - 2026-04-04

### Fixed
- Native addon: enforce key/value byte length limits (rejects oversize inputs instead of silently truncating).
- Native addon: fixed hashing to avoid out-of-bounds reads (stabilizes lookups under load).

## [2.0.4] - 2026-04-04

### Fixed
- Native addon: replaced moving `realloc`-based arena with a non-moving chunked arena to prevent access violations during initialization on Windows.

## [2.0.3] - 2026-04-04

### Fixed
- Native addon: fixed bump allocator growth logic to prevent out-of-bounds writes under large shard allocations.
- Native addon: avoided unaligned 64-bit loads/stores in hot paths (improves portability for ARM).

### Changed
- Test and benchmark scripts now perform real end-to-end validation and measurement for the native addon.

## [2.0.2] - 2026-04-04

### Fixed
- Native addon: corrected UTF-8 string length handling (empty keys are rejected; length limits match documented bytes).

## [2.0.1] - 2026-04-04

### Added
- Native Node.js addon implementation (N-API) under `npm/`.

### Changed
- Package entrypoint now targets the native addon wrapper.
- Benchmarks and tests target the native addon.

### Removed
- Legacy JS↔WASM bridge artifacts.

## [2.0.0] - 2026-04-03

### Added
- **Multi-threading support**: Added `WorkerPool` and `SharedArrayBuffer` support for parallel operations (up to 8 threads).
- **SIMD acceleration**: Integrated `wasm_simd128` for faster group probing in Swiss Tables and bulk memory operations.
- **Swiss Table design**: Replaced standard hash map with a high-performance Swiss Table implementation for better cache locality and probing efficiency.
- **Bloom Filters**: Added 256KB Bloom Filters per shard (64 shards total) to minimize expensive lookups for non-existent keys.
- **Skip List index**: Implemented a 16-level sorted index for efficient prefix and range scans.
- **Write-Ahead Log (WAL)**: Added durability via an append-only WAL with CRC32C integrity checks and automatic recovery.
- **Slab Allocator**: Custom 32-class slab allocator to reduce memory fragmentation and allocation overhead.
- **Atomic Batches**: Support for ACID-like batch writes with rollback on failure.
- **Buffered Writes**: Added command buffer (512KB) for high-throughput asynchronous writes.
- **Snapshot Export/Import**: Full database snapshots with per-record CRC32C checksums.
- **RapidHash**: Switched to RapidHash for improved hash distribution and performance.
- **Real-world Benchmark**: Introduced `benchmark.js` for accurate performance profiling including JS bridge overhead and latency percentiles (P50/P99).
- **npm package**: Added `package.json` for publishing as `amoradbx`.

### Changed
- **Increased Limits**: Maximum value size increased to 1MB (16x previous limit).
- **Key Optimization**: Added 22-byte inline fast path for small keys to eliminate heap allocation.
- **Memory Management**: Optimized memory growth and boundary checks for large datasets.

### Fixed
- **Windows WAL fix**: Optimized file-writing strategy in `amora.js` to prevent `EPERM` errors during WAL updates on Windows.
- Improved race condition handling in multi-threaded environments using `_Atomic` types and explicit memory ordering.
- Enhanced CRC32C performance with a slice-by-4 unrolled implementation.

## [1.0.0] - 2026-04-02

### Added
- Initial release of AmoraDB.
- Core key-value engine in C and WebAssembly.
- Basic Node.js bindings.
- Support for `set`, `get`, `has`, and `delete` operations.
- Simple in-memory storage.
- Basic benchmarking harness.
