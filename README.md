<div align="center">

<img src="https://github.com/amoracoin-org/AmoraDb/blob/main/assets/amoradb-logo.jpeg" alt="AmoraDB" width="180"/>

# AmoraDB

**Ultra-High-Performance Embedded Key-Value Engine**

*Built in C · Native Node.js Addon · SIMD Accelerated*

[![License: MIT](https://img.shields.io/badge/License-MIT-blueviolet.svg)](#license)
[![Node.js](https://img.shields.io/badge/Node.js-Native%20Addon-339933?logo=nodedotjs)](https://nodejs.org/)
[![Platform](https://img.shields.io/badge/Platforms-Win/Mac/Linux-4B0082)](#)

</div>

---

### 📖 Documentation

- **[CHANGELOG.md](CHANGELOG.md)**: Track all notable changes and version history.
- **[CONTRIBUTING.md](CONTRIBUTING.md)**: Guide for setting up development environment and contributing code.
- **[SPEC.md](SPEC.md)**: Deep dive into the internal architecture, sharding, and binary formats.

---

AmoraDB is a hand-crafted, zero-runtime-dependency key-value store written entirely in C and shipped as a native Node.js addon (N-API). It is designed for low latency and high throughput inside a Node.js process.

Designed for scenarios where you need maximum performance: caching, session storage, real-time data structures, and high-throughput APIs.

---

## ✨ Features

| Capability | Detail |
|---|---|
| **Native Addon** | Pure C compiled with native optimizations |
| **Hash Map** | Swiss Table-inspired design with SIMD-accelerated probing |
| **Sharding** | 64 independent shards with per-shard spinlocks |
| **Bloom Filters** | 256 KB per shard (2M bits) for zero-cost negative lookups |
| **Spinlocks** | Fast mutex for cross-platform thread safety |
| **Slab Allocator** | 20 size classes, memory pooling |
| **Inline Keys** | Up to 22 bytes stored directly in slot (zero allocation) |
| **Max Values** | Up to 1 MB per value |
| **Max Keys** | Up to 4 KB per key |
| **64-bit Ready** | Full 64-bit hash distribution |

---

## 📊 Performance

Benchmarks depend heavily on your CPU, OS, compiler/toolchain, Node.js version, and workload.

### Run Benchmarks

```bash
npm install
node test.js
node benchmark.js
```

### Performance Tips

1. **Use short keys** — 10-20 byte keys are optimal
2. **Inline keys** — keys ≤ 22 bytes use zero extra allocation
3. **Random keys** — avoid pathological collisions
---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────┐
│                   index.js                      │  JS Wrapper
│        Transparent API · Buffer handling        │
└────────────────────┬────────────────────────────┘
                     │  NAPI (native)
┌────────────────────▼────────────────────────────┐
│              native.c → .node                   │  Native Addon
│                                                 │
│    ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│    │ Shard 0  │  │ Shard 1  │  │  ...63   │     │  64 Shards
│    │ HashMap  │  │ HashMap  │  │ HashMap  │     │
│    │ Bloom    │  │ Bloom    │  │ Bloom    │     │  256KB Bloom/shard
│    │ Spinlock │  │ Spinlock │  │ Spinlock │     │  Thread-safe
│    └──────────┘  └──────────┘  └──────────┘     │
│                                                 │
│  ┌──────────────────────────────────────────┐   │
│  │      Bump Allocator + Slab Pool          │   │  Memory
│  └──────────────────────────────────────────┘   │
│                                                 │
│  ┌──────────────────────────────────────────┐   │
│  │     RapidHash (64-bit, SIMD-ready)       │   │  Hash
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

**Hash function:** RapidHash-inspired 64-bit hash — excellent distribution, minimal collisions.

**Thread safety:** Spinlocks on every shard for safe multi-threaded access.

**Memory:** Bump allocator with slab pooling — fast allocation, no fragmentation.

---

## 🚀 Getting Started

### Prerequisites

- Node.js ≥ 18
- Python 3.x (required by node-gyp on many systems)
- A working C/C++ toolchain (MSVC on Windows, clang/gcc on macOS/Linux)
  - Windows: Visual Studio 2022 (or Build Tools) with “Desktop development with C++”
  - Android (Termux, arm64): clang, make, python, and a working Node.js toolchain

### Installation

```bash
npm install amoradbx
```

On supported platforms, the native binary is provided as a prebuilt `.node` file and no build toolchain is required. If no prebuild is available for your platform, the install falls back to building from source (requires python + compiler).

Termux (Android, arm64):

```bash
pkg update -y
pkg install -y nodejs python make clang
npm install amoradbx
```

If you see `gyp: Undefined variable android_ndk_path`, create this file and retry:

```bash
mkdir -p ~/.gyp
echo "{'variables':{'android_ndk_path':''}}" > ~/.gyp/include.gypi
npm install amoradbx
```

### Basic Usage

```js
const AmoraDB = require('amoradbx');

const db = AmoraDB.open({ cap: 65536 });

db.set('user:1', 'alice');
db.get('user:1');         // → 'alice'
db.has('user:1');         // → true
db.delete('user:1');
```

console.log(db.stats());
// {
//   count: 0,
//   capacity: 4194304,
//   hits: 2,
//   misses: 1,
//   total_ops: 5,
//   set_ops: 1,
//   get_ops: 2,
//   has_ops: 1,
//   delete_ops: 1,
//   shards: 64
// }


---

## 🛠 Building from Source

### Prerequisites

- Node.js ≥ 18
- Python 3.x (for node-gyp)
- C++ compiler (gcc/clang/MSVC)
  - Windows: Visual Studio 2022 (or Build Tools) with “Desktop development with C++”
  - Android (Termux, arm64): `pkg install -y nodejs python make clang`

### Build

```bash
cd npm
npm install
npm run build
```

This will compile the native addon for your current platform.

---

## 📁 Project Structure

```
amora/
├── npm/                    # Native Node.js addon package
│   ├── package.json         # Package config with prebuilds
│   ├── binding.gyp         # node-gyp build config
│   ├── index.js            # JS wrapper (same API as WASM version)
│   └── src/
│       └── native.c       # Pure C NAPI addon
├── test.js                # Test suite
└── benchmark.js          # Performance benchmarks
```

---

## 🧪 Running Tests

```bash
node test.js
```

The test suite covers: heartbeat, set/get/has/delete, updates, large values (512 KB), key/value size validation, and a stress run (50K keys).

---

## 📄 License

MIT © AmoraDB Authors

---

<div align="center">
  <sub>Built with obsession for performance. No dependencies. No compromises.</sub>
</div>

