# Contributing to AmoraDB

Thank you for your interest in contributing to AmoraDB! We're excited to have you join our community.

## Code of Conduct

Please follow our Code of Conduct in all your interactions with the project.

## How to Contribute

1.  **Report Bugs**: If you find a bug, please open an issue with a clear description and steps to reproduce.
2.  **Suggest Features**: Have an idea for a new feature? Open an issue to discuss it.
3.  **Submit Pull Requests**:
    *   Fork the repository.
    *   Create a new branch for your changes.
    *   Implement your changes and add tests.
    *   Ensure all tests pass (`node test.js`).
    *   Submit a pull request.

## Development Environment Setup

### Prerequisites

*   **Node.js**: Version 18 or higher.
*   **Emscripten** or **wasi-sdk**: To compile the C core to WebAssembly.

### Building from Source

To build the `amora_core_mt_simd.wasm` binary with full multi-threading and SIMD support:

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

### Running Tests

Run the full test suite using Node.js:

```bash
node test.js
```

The test suite covers performance benchmarks, stress tests, and core functionality checks.

## Coding Standards

### C Core (`amora_core.c`)

*   Standard C11.
*   No standard library dependencies (`-nostdlib`).
*   Use `u8`, `u16`, `u32`, `u64` for fixed-width integers.
*   Optimize for performance: use `INLINE` for small functions and minimize allocations.
*   Maintain thread safety using `_Atomic` types and memory ordering where necessary.

### Node.js Binding (`amora.js`)

*   Follow standard Node.js practices.
*   Ensure backward compatibility with older Node.js versions if possible (minimum v18).
*   Maintain the high-performance philosophy: avoid unnecessary copying and allocations.
*   Use `SharedArrayBuffer` for multi-threaded communication.

## Documentation

*   Update the `README.md` and `SPEC.md` for any major architectural changes or new features.
*   Keep comments clear and concise.

## Licensing

By contributing to AmoraDB, you agree that your contributions will be licensed under the MIT License.
