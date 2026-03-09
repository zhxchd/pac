# Building the PAC DuckDB Extension

## Prerequisites

- **git** (with submodule support)
- **cmake** (>= 3.11)
- **ninja** (optional but recommended — significantly faster builds)
- **C++ compiler** with C++11 support (clang recommended, especially for benchmarks)

## Clone

```bash
git clone --recurse-submodules https://github.com/cwida/pac.git
cd pac
```

If you already cloned without `--recurse-submodules`, run:

```bash
git submodule update --init --recursive
```

## Build

```bash
GEN=ninja make        # release build (recommended)
make                  # release build without ninja
make debug            # debug build
```

The extension is built into `build/release/extension/pac/pac.duckdb_extension`.

## Run Tests

```bash
# all tests
make test

# PAC SQL tests only
./build/release/test/unittest --test-dir . "test/sql/pac*"

# C++ unit tests
./build/release/extension/pac/pac_test_runner
```

## Build Flags

Compile-time flags control algorithmic behavior for scientific reproducibility.
Set them via `cmake` defines (e.g., `-DPAC_NOAPPROX=1`).

| Flag | Effect |
|------|--------|
| `PAC_NOAPPROX` | Disable approximate sum, use exact cascading |

Additional flags exist for microbenchmark configurations. See
[docs/benchmark/microbenchmarks.md](../benchmark/microbenchmarks.md) for details.

## Updating the DuckDB Version

The DuckDB version is pinned via the `duckdb` and `extension-ci-tools` submodules.
To update to a new release:

```bash
cd duckdb && git fetch --tags && git checkout v1.X.0
cd ../extension-ci-tools && git fetch && git checkout origin/v1.X.0
cd ..
git add duckdb extension-ci-tools
git commit -m "Bump DuckDB to v1.X.0"
```

Replace `v1.X.0` with the target version tag.
