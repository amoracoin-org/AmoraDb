# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

### Changed
- **Increased Limits**: Maximum value size increased to 1MB (16x previous limit).
- **Key Optimization**: Added 22-byte inline fast path for small keys to eliminate heap allocation.
- **Memory Management**: Optimized memory growth and boundary checks for large datasets.

### Fixed
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
