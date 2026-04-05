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

### Android (Termux, arm64) Development Guide

This project can be built directly on Android using Termux (arm64). This is the recommended workflow for mobile developers who want to iterate on the native addon.
If you prefer not to install local native toolchain dependencies, use CI-generated prebuild artifacts from pull requests or releases.

#### 1) Install build prerequisites

```bash
pkg update -y
pkg install -y git nodejs python make clang
```

#### 2) Clone the repository

```bash
git clone https://github.com/amoracoin-org/AmoraDb.git
cd AmoraDb
```

#### 3) Build (node-gyp)

```bash
npm install
```

If your platform has a prebuilt `.node` binary available, the install will not require a toolchain. Otherwise it will compile from source via `node-gyp`.

If you see `gyp: Undefined variable android_ndk_path`, create this file and retry:

```bash
mkdir -p ~/.gyp
echo "{'variables':{'android_ndk_path':''}}" > ~/.gyp/include.gypi
npm install
```

### Contributing Without Local Toolchain (CI Prebuild Artifacts)

Use this flow when you want to contribute from Android without installing Python/clang/make locally:

1. Push your branch and open a pull request.
2. Wait for the workflow **Native CI and Prebuilds** to complete.
3. Download the artifact named `prebuild-<platform>-<arch>` from the workflow run.
4. Place the downloaded `amoradb.node` at:

```bash
npm/prebuilds/<platform>-<arch>/amoradb.node
```

5. Run validation locally using the downloaded binary:

```bash
node test.js
node benchmark.js
```

Release policy: npm publish is blocked unless all required prebuilds are present:
- `win32-x64`
- `linux-x64`
- `linux-arm64`
- `darwin-x64`
- `darwin-arm64`
- `android-arm64`

`android-arm64` is produced by the dedicated CI job **build-android-termux** (Termux-based build path).

### Troubleshooting (Android/Termux)

*   **Build succeeds but crashes at runtime**: Make sure you rebuilt after pulling changes: delete `npm/build/` and rerun `npm install`.
*   **Slow compilation**: Termux compilation is slower than desktop. Prefer smaller test sizes while iterating and use `node test.js` first.

### Running Tests

Run the full test suite using Node.js:

```bash
node test.js
```

The test suite is a sanity and regression harness for the native addon (core API checks, limits, and a stress run).

To run the real-world benchmark:

```bash
node benchmark.js
```

## Coding Standards

### Native Core (`npm/src/native.c`)

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
