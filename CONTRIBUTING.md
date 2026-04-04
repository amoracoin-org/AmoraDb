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
*   **Python 3**: Required by `node-gyp`.
*   **C/C++ toolchain**:
    *   Windows: Visual Studio 2022 (or Build Tools) with “Desktop development with C++”
    *   macOS: Xcode Command Line Tools
    *   Linux: `gcc`/`g++` or `clang`
    *   Android (Termux, arm64): `pkg install -y nodejs python make clang`

### Building from Source

To build the native addon:

```bash
npm install
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
*   Use `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t`, `uintptr_t` for fixed-width integers and pointers.
*   Keep the build compatible with MSVC/clang/gcc (avoid compiler-specific builtins where possible).
*   Prefer safe unaligned loads/stores via `memcpy` where required (especially for ARM).

### Node.js Binding (Native Addon)

*   Follow standard Node.js practices.
*   Maintain the high-performance philosophy: avoid unnecessary copies and allocations.
*   Keep the public API stable across releases.

## Documentation

*   Update the `README.md` and `SPEC.md` for any major architectural changes or new features.
*   Keep comments clear and concise.

## Licensing

By contributing to AmoraDB, you agree that your contributions will be licensed under the MIT License.
