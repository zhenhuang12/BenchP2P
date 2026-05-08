# 3rdparty

This directory holds the inputs and outputs of the wheel + native binary
build pipeline driven by `scripts/build_wheel.sh` (and its container
wrapper `scripts/container_build_wheel.sh`).

## Tracked files

- `manifest.json`: public repository URLs, refs, checkout paths, and wheel
  build directories for MORI, Mooncake, UCCL, and NIXL. Schema per entry:
  - `name`: backend label (matches `--backends` filtering)
  - `repo`: git URL passed to `git clone --recursive`
  - `ref`: branch or tag checked out after fetch
  - `path`: subdirectory under `3rdparty/` for the checkout
  - `build_path`: relative directory inside the checkout where `pip wheel .`
    runs (only used by mori; uccl/mooncake/nixl drive the wheel step from
    each project's own script, see below)
  - `wheel_glob`: glob used to verify the produced wheel under
    `wheelhouse/<name>/`
- `patches/`: local patches for the third-party checkouts. Currently:
  - `uccl-0001-relax-python-requires-to-3.10.patch` relaxes UCCL's
    `python_requires` from `>=3.12` to `>=3.10`.
  - `nixl-0001-doca-telemetry-build-fixes.patch` adds two build fixes for
    NIXL's DOCA telemetry exporter plugin.
  Patches are referenced from `manifest.json` via the optional `patches`
  array (paths relative to `3rdparty/`).

## Generated files (gitignored)

- `mori/`, `Mooncake/`, `uccl/`, `nixl/`: per-backend git checkouts
- `Mooncake/build/`, `nixl/build/`, `nixl/benchmark/nixlbench/build/`,
  `uccl/p2p/*.so`: native build outputs
- `wheelhouse/<backend>/*.whl`: produced Python wheels
- `.benchp2p_runtime/`: per-rank Mooncake target-segment files written at
  benchmark time

## Per-backend build flow

`build_wheel.sh` does **not** just call `pip install`; it dispatches per
backend to that project's official install/build flow:

| Backend | Steps performed |
| --- | --- |
| **mori** | `pip wheel <repo>` (mori is Python-only; no separate native binary) |
| **uccl** | `make -j -f p2p/Makefile.rocm` (mirrors `uccl/build_inner.sh build_p2p`) -> stage `libuccl_p2p.so` and `p2p.*.so` into the `uccl/` package dir -> rename cpython-tagged ABI to `.abi3.so` on Python>=3.12 -> `pip wheel` |
| **mooncake** | `Mooncake/dependencies.sh -y` (apt + Go + yalantinglibs submodule build) -> `cmake -B build -DUSE_HIP=ON -DWITH_TE=ON -DWITH_STORE=OFF ...` -> `ninja` (produces `engine.so` + `tebench`) -> official `Mooncake/scripts/build_wheel.sh` (auditwheel-repaired wheel with binaries embedded) |
| **nixl** | `meson setup nixl/build --prefix=<nixl-prefix>` + `ninja install` -> `meson setup benchmark/nixlbench/build -Dnixl_path=<nixl-prefix>` + `ninja` -> `pip wheel nixl` |

## Building wheels

Default container build (recommended):

```bash
bash scripts/container_build_wheel.sh
```

Equivalent host-side build (requires `jq`, `git`, `python3`, `pip`,
`cmake`, `ninja`, `make`, `hipcc` plus the per-backend system deps below):

```bash
bash scripts/build_wheel.sh
```

Common variants:

```bash
bash scripts/build_wheel.sh --backends mori,uccl
bash scripts/build_wheel.sh --skip-clone           # reuse existing checkouts
bash scripts/build_wheel.sh --skip-binaries        # python wheels only (no native)
bash scripts/build_wheel.sh --skip-apt-deps        # skip Mooncake's dependencies.sh
bash scripts/build_wheel.sh --no-clean             # keep stale wheels
bash scripts/build_wheel.sh --jobs 32              # make/ninja parallelism
bash scripts/build_wheel.sh --nixl-prefix /opt/nixl-1.1
bash scripts/build_wheel.sh --continue-on-error    # don't abort on first failure
bash scripts/build_wheel.sh --dry-run --skip-clone # preview commands only
```

### System dependencies

Inside the runtime container, the per-backend builds rely on these system
packages being present (the rocm/primus image ships some, others come in
via Mooncake's `dependencies.sh`):

- **uccl**: `make`, `hipcc`, `libibverbs-dev`, `libnuma-dev`, plus
  `nanobind` (the script auto `pip install`s `nanobind` if missing).
- **mooncake**: `libgflags-dev`, `libgoogle-glog-dev`, `libjsoncpp-dev`,
  `libgrpc++-dev`, `libgrpc-dev`, `libprotobuf-dev`, `protobuf-compiler-grpc`,
  `pybind11-dev`, `libcurl4-openssl-dev`, `libssl-dev`, `libnuma-dev`,
  `libibverbs-dev`, `librdmacm-dev`, `libxml2-dev`, `uuid-dev`, `libgtest-dev`,
  `libhwloc-dev`, `libaio-dev`, `liburing-dev`, `libcpprest-dev`. Mooncake's
  own `dependencies.sh -y` apt-installs these (and Go + yalantinglibs).
- **nixl**: `meson`, `ninja-build`, `libgflags-dev`, `libgrpc-dev`,
  `libgrpc++-dev`, `libprotobuf-dev`, `protobuf-compiler-grpc`,
  `etcd-server`, `libcurl4-openssl-dev`, `libssl-dev`, `libgoogle-glog-dev`,
  `pybind11-dev`, `libibverbs-dev`, `librdmacm-dev`, `libcpprest-dev`,
  `libxml2-dev`, `uuid-dev`, `libnuma-dev`.

If your container/runtime cannot reach Ubuntu apt mirrors, build a derived
image once (FROM `docker.io/rocm/primus:v26.2`) with the lists above
pre-installed, push it to your registry, and pass `--image <your-image>`
to `container_build_wheel.sh` and `slurm_bench_p2p.sh`.

### Git safe.directory

Inside the runtime container, `build_wheel.sh` writes
`safe.directory = *` to git so bind-mounted host checkouts pass git's
ownership check. The same env-var fallbacks (`GIT_CONFIG_COUNT`,
`GIT_CONFIG_KEY_0`, `GIT_CONFIG_VALUE_0`) are exported in case a backend's
PEP 517 build invokes git outside the inherited shell environment.
