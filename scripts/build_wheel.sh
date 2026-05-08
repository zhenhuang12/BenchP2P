#!/usr/bin/env bash
# Build BenchP2P third-party stacks (Python wheels + native benchmark
# binaries) declared in 3rdparty/manifest.json.
#
# Per-backend dispatch follows each project's own README/install scripts
# (no naive pip-only approach):
#
#   mori     -> pip wheel <repo> -w 3rdparty/wheelhouse/mori/
#   uccl     -> uccl/build_inner.sh-style ROCm build:
#               make -f p2p/Makefile.rocm + stage .so files into uccl/
#               + pip wheel
#   mooncake -> Mooncake/dependencies.sh -y (best-effort apt install)
#               + cmake + ninja tebench
#               + Mooncake/scripts/build_wheel.sh (official wheel script)
#   nixl     -> nixl/contrib/build-wheel.sh-style + nixl/README manual build:
#               meson setup nixl/build (--prefix=<nixl-prefix>) + ninja install
#               + meson setup benchmark/nixlbench/build + ninja
#               + pip wheel nixl
#
# Usage:
#   bash scripts/build_wheel.sh [options]
#
# Options:
#   --backends a,b           Comma-separated subset of manifest names
#   --manifest PATH          Manifest path (default: 3rdparty/manifest.json)
#   --thirdparty-dir PATH    Source root containing checkouts (default: 3rdparty)
#   --wheelhouse PATH        Wheel output root (default: <thirdparty-dir>/wheelhouse)
#   --python BIN             Python interpreter (default: python3)
#   --jobs N                 make/ninja parallelism (default: $(nproc))
#   --nixl-prefix DIR        NIXL install prefix (default: /usr/local/nixl)
#   --timeout SECONDS        Per-command timeout for git/pip (default: 3600)
#   --skip-clone             Skip git clone/fetch/checkout/submodule update
#   --skip-binaries          Build Python wheels only, skip native binaries
#                            (uccl C++ extension, mooncake tebench, nixlbench)
#   --skip-apt-deps          Don't run mooncake/dependencies.sh apt step
#   --no-clean               Keep stale wheels in <wheelhouse>/<name>/
#   --continue-on-error      Don't abort on first backend failure
#   --dry-run                Print commands without executing them
#   -h, --help               Show this help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MANIFEST="${REPO_ROOT}/3rdparty/manifest.json"
THIRDPARTY_DIR="${REPO_ROOT}/3rdparty"
WHEELHOUSE=""
PYTHON_BIN="${PYTHON_BIN:-python3}"
JOBS="$(nproc 2>/dev/null || echo 4)"
NIXL_PREFIX="/usr/local/nixl"
TIMEOUT_S="3600"
BACKEND_FILTER=""
SKIP_CLONE="0"
SKIP_BINARIES="0"
SKIP_APT_DEPS="0"
NO_CLEAN="0"
CONTINUE_ON_ERROR="0"
DRY_RUN="0"

usage() {
  cat <<'EOF'
Build BenchP2P third-party stacks (wheels + native binaries) declared in
3rdparty/manifest.json. Per-backend logic uses each project's own scripts,
not bare pip install.

Usage:
  bash scripts/build_wheel.sh [options]

Options:
  --backends a,b           Comma-separated subset of manifest names
  --manifest PATH          Manifest path (default: 3rdparty/manifest.json)
  --thirdparty-dir PATH    Source root containing checkouts (default: 3rdparty)
  --wheelhouse PATH        Wheel output root (default: <thirdparty-dir>/wheelhouse)
  --python BIN             Python interpreter (default: python3)
  --jobs N                 make/ninja parallelism (default: $(nproc))
  --nixl-prefix DIR        NIXL install prefix (default: /usr/local/nixl)
  --timeout SECONDS        Per-command timeout for git/pip (default: 3600)
  --skip-clone             Skip git clone/fetch/checkout/submodule update
  --skip-binaries          Build Python wheels only, skip native binaries
                           (uccl C++ extension, mooncake tebench, nixlbench)
  --skip-apt-deps          Don't run mooncake/dependencies.sh apt step
  --no-clean               Keep stale wheels in <wheelhouse>/<name>/
  --continue-on-error      Don't abort on first backend failure
  --dry-run                Print commands without executing them
  -h, --help               Show this help
EOF
}

log() { printf '[build_wheel] %s\n' "$*" >&2; }
die() { printf '[build_wheel] ERROR: %s\n' "$*" >&2; exit 1; }

run() {
  printf '+ %s\n' "$(printf '%q ' "$@")"
  if [[ "${DRY_RUN}" == "1" ]]; then
    return 0
  fi
  if command -v timeout >/dev/null 2>&1 && [[ -n "${TIMEOUT_S}" && "${TIMEOUT_S}" != "0" ]]; then
    timeout --signal=TERM "${TIMEOUT_S}" "$@"
  else
    "$@"
  fi
}
run_in() {
  local cwd="$1"; shift
  printf '+ cd %s && %s\n' "${cwd}" "$(printf '%q ' "$@")"
  if [[ "${DRY_RUN}" == "1" ]]; then
    return 0
  fi
  ( cd "${cwd}" && \
    if command -v timeout >/dev/null 2>&1 && [[ -n "${TIMEOUT_S}" && "${TIMEOUT_S}" != "0" ]]; then
      timeout --signal=TERM "${TIMEOUT_S}" "$@"
    else
      "$@"
    fi
  )
}
run_in_sh() {
  # Variant of run_in that runs a literal shell snippet via `bash -c`.
  # Useful for `cp p2p/p2p.*.so uccl/` style globbing that must happen
  # inside the target cwd.
  local cwd="$1"; shift
  local cmd="$*"
  printf '+ cd %s && %s\n' "${cwd}" "${cmd}"
  if [[ "${DRY_RUN}" == "1" ]]; then
    return 0
  fi
  ( cd "${cwd}" && bash -c "${cmd}" )
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backends) BACKEND_FILTER="$2"; shift 2 ;;
    --manifest) MANIFEST="$2"; shift 2 ;;
    --thirdparty-dir) THIRDPARTY_DIR="$2"; shift 2 ;;
    --wheelhouse) WHEELHOUSE="$2"; shift 2 ;;
    --python) PYTHON_BIN="$2"; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --nixl-prefix) NIXL_PREFIX="$2"; shift 2 ;;
    --timeout) TIMEOUT_S="$2"; shift 2 ;;
    --skip-clone) SKIP_CLONE="1"; shift ;;
    --skip-binaries) SKIP_BINARIES="1"; shift ;;
    --skip-apt-deps) SKIP_APT_DEPS="1"; shift ;;
    --no-clean) NO_CLEAN="1"; shift ;;
    --continue-on-error) CONTINUE_ON_ERROR="1"; shift ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1 (use --help)" ;;
  esac
done

command -v jq >/dev/null 2>&1 || die "jq is required to parse ${MANIFEST}"
[[ -f "${MANIFEST}" ]] || die "manifest not found: ${MANIFEST}"

if [[ -z "${WHEELHOUSE}" ]]; then
  WHEELHOUSE="${THIRDPARTY_DIR}/wheelhouse"
fi

# git safe.directory for bind-mounted host checkouts inside ephemeral
# containers. PEP 517 build isolation drops GIT_CONFIG_GLOBAL but reads
# the real ~/.gitconfig, so we write the rule there too.
git config --global --add safe.directory '*' >/dev/null 2>&1 || true
export GIT_CONFIG_COUNT="${GIT_CONFIG_COUNT:-1}"
export GIT_CONFIG_KEY_0="${GIT_CONFIG_KEY_0:-safe.directory}"
export GIT_CONFIG_VALUE_0="${GIT_CONFIG_VALUE_0:-*}"

mkdir -p "${THIRDPARTY_DIR}" "${WHEELHOUSE}"

# Resolve backend list from manifest.
mapfile -t REPO_NAMES < <(jq -r '.repos[].name' "${MANIFEST}")
[[ ${#REPO_NAMES[@]} -gt 0 ]] || die "manifest declares no repos: ${MANIFEST}"

SELECTED=()
if [[ -n "${BACKEND_FILTER}" ]]; then
  IFS=',' read -r -a REQUESTED <<< "${BACKEND_FILTER}"
  declare -A KNOWN=()
  for n in "${REPO_NAMES[@]}"; do KNOWN["$(echo "${n}" | tr '[:upper:]' '[:lower:]')"]="${n}"; done
  for raw in "${REQUESTED[@]}"; do
    key="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]' | xargs)"
    [[ -z "${key}" ]] && continue
    [[ -n "${KNOWN[${key}]+x}" ]] || die "unknown backend: ${raw} (known: ${REPO_NAMES[*]})"
    SELECTED+=("${KNOWN[${key}]}")
  done
else
  SELECTED=("${REPO_NAMES[@]}")
fi

log "Manifest:    ${MANIFEST}"
log "Thirdparty:  ${THIRDPARTY_DIR}"
log "Wheelhouse:  ${WHEELHOUSE}"
log "Python:      ${PYTHON_BIN}"
log "Backends:    ${SELECTED[*]}"
log "Jobs:        ${JOBS}"
[[ "${SKIP_BINARIES}" == "1" ]] && log "Mode:        --skip-binaries (Python wheels only)"
[[ "${DRY_RUN}" == "1" ]] && log "Mode:        --dry-run"

# --------------------------------------------------------------- common ---

ensure_checkout() {
  local name="$1" repo="$2" ref="$3" path="$4"
  local checkout="${THIRDPARTY_DIR}/${path}"
  if [[ "${SKIP_CLONE}" == "1" ]]; then
    [[ -d "${checkout}" ]] || die "${name}: --skip-clone but ${checkout} missing"
    echo "${checkout}"
    return 0
  fi
  if [[ ! -e "${checkout}" ]]; then
    run_in "${THIRDPARTY_DIR}" git clone --recursive "${repo}" "${path}"
  elif [[ -d "${checkout}/.git" ]]; then
    run_in "${checkout}" git fetch --tags origin
  else
    die "${checkout} exists but is not a git checkout"
  fi
  if [[ -n "${ref}" && "${ref}" != "null" ]]; then
    run_in "${checkout}" git checkout "${ref}"
  fi
  run_in "${checkout}" git submodule update --init --recursive
  echo "${checkout}"
}

clean_wheelhouse_for() {
  local name="$1"
  local out="${WHEELHOUSE}/${name}"
  mkdir -p "${out}"
  if [[ "${NO_CLEAN}" == "1" ]]; then return 0; fi
  if [[ "${DRY_RUN}" == "1" ]]; then
    log "    (dry-run) would remove stale ${out}/*.whl"
    return 0
  fi
  while IFS= read -r -d '' f; do
    log "    removing stale wheel: ${f}"
    rm -f "${f}"
  done < <(find "${out}" -maxdepth 1 -type f -name '*.whl' -print0)
}

verify_wheel() {
  local name="$1" wheel_glob="$2"
  if [[ "${DRY_RUN}" == "1" ]]; then return 0; fi
  local out="${WHEELHOUSE}/${name}"
  local produced
  produced="$(find "${out}" -maxdepth 1 -type f -name "${wheel_glob}" -printf '%T@\t%p\n' \
              | sort -nr | head -n 1 | cut -f2-)"
  if [[ -z "${produced}" ]]; then
    log "    available files under ${out}:"
    find "${out}" -maxdepth 1 -type f -printf '      %f\n' || true
    die "${name}: no wheel matched ${wheel_glob} under ${out}"
  fi
  log "    built: ${produced} ($(stat -c%s "${produced}") bytes)"
}

# ---------------------------------------------------------------- mori ---

build_one_mori() {
  local checkout="$1" build_path="$2" wheel_glob="$3"
  local out="${WHEELHOUSE}/mori"
  clean_wheelhouse_for "mori"
  log "==> mori: pip wheel (Python wheel only; mori has no separate native bench binary)"
  run_in "${checkout}/${build_path}" "${PYTHON_BIN}" -m pip wheel --no-deps . -w "${out}"
  verify_wheel "mori" "${wheel_glob}"
}

# ---------------------------------------------------------------- uccl ---
# Mirrors the official uccl/build_inner.sh `build_p2p` flow for ROCm:
# make -f p2p/Makefile.rocm, stage .so into uccl/ package dir, abi3 rename
# on Python>=3.12, then pip wheel. We invoke the steps directly because
# uccl/build.sh wraps everything in docker-in-docker which we cannot run
# from inside an already-running container.

build_one_uccl() {
  local checkout="$1" build_path="$2" wheel_glob="$3"
  local out="${WHEELHOUSE}/uccl"
  local p2p_dir="${checkout}/p2p"
  clean_wheelhouse_for "uccl"

  if [[ "${SKIP_BINARIES}" != "1" ]]; then
    [[ -f "${p2p_dir}/Makefile.rocm" ]] || die "uccl: ${p2p_dir}/Makefile.rocm not found"

    # uccl Makefile.rocm probes nanobind via python; install if missing.
    if ! "${PYTHON_BIN}" -c 'import nanobind' 2>/dev/null; then
      log "==> uccl: installing nanobind (build-time dep for p2p extension)"
      run "${PYTHON_BIN}" -m pip install --no-deps nanobind
    fi

    log "==> uccl: build p2p C++ extension via official Makefile.rocm"
    run_in "${p2p_dir}" make clean -f Makefile.rocm || true
    run_in "${p2p_dir}" make -j"${JOBS}" -f Makefile.rocm

    log "==> uccl: stage .so into uccl/ package dir (mirrors build_inner.sh build_p2p)"
    run mkdir -p "${checkout}/uccl/lib"
    run_in_sh "${checkout}" 'cp p2p/libuccl_p2p.so uccl/lib/'
    run_in_sh "${checkout}" 'cp p2p/p2p.*.so uccl/'
    run_in_sh "${checkout}" 'cp p2p/collective.py uccl/'
    run_in_sh "${checkout}" 'cp p2p/utils.py uccl/'

    if "${PYTHON_BIN}" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 12) else 1)' 2>/dev/null; then
      log "==> uccl: rename cpython-*.so to .abi3.so (nanobind stable ABI, Python>=3.12)"
      if [[ "${DRY_RUN}" != "1" ]]; then
        shopt -s nullglob
        for f in "${checkout}"/uccl/*.cpython-*.so; do
          local newname
          newname="$(echo "${f}" | sed 's/\.cpython-[^.]*-[^.]*-[^.]*\.so/.abi3.so/')"
          log "    $(basename "${f}") -> $(basename "${newname}")"
          mv "${f}" "${newname}"
        done
        shopt -u nullglob
      fi
    fi
  else
    log "==> uccl: --skip-binaries set; producing python-only wheel (will not import at runtime)"
  fi

  log "==> uccl: pip wheel"
  run_in "${checkout}/${build_path}" "${PYTHON_BIN}" -m pip wheel --no-deps . -w "${out}"
  verify_wheel "uccl" "${wheel_glob}"
}

# ------------------------------------------------------------ mooncake ---
# Per Mooncake README:
#   1. dependencies.sh -y     (apt + go + yalantinglibs submodule build)
#   2. cmake -B build && cmake --build build -j   (engine.so + tebench + ...)
#   3. scripts/build_wheel.sh (official wheel script: copies .so + tebench
#                              into mooncake-wheel/mooncake/, runs python -m
#                              build, auditwheel-repairs, etc.)

build_one_mooncake() {
  local checkout="$1" build_path="$2" wheel_glob="$3"
  local out="${WHEELHOUSE}/mooncake"
  clean_wheelhouse_for "mooncake"

  if [[ "${SKIP_BINARIES}" != "1" ]]; then
    if [[ "${SKIP_APT_DEPS}" != "1" ]]; then
      log "==> mooncake: running official dependencies.sh -y (apt + go + yalantinglibs)"
      log "    (use --skip-apt-deps if your container already has these)"
      if [[ -f "${checkout}/dependencies.sh" ]]; then
        # dependencies.sh requires root; fall back to direct call when we
        # already are. The script chmods/installs Go to /usr/local/go, so
        # write access there is required.
        if [[ $EUID -eq 0 ]]; then
          run_in "${checkout}" bash dependencies.sh -y
        else
          run_in "${checkout}" sudo bash dependencies.sh -y
        fi
      else
        log "    WARNING: ${checkout}/dependencies.sh not found, skipping"
      fi
    fi

    log "==> mooncake: cmake configure (HIP, transfer-engine + tebench)"
    run cmake -S "${checkout}" -B "${checkout}/build" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DUSE_HIP=ON \
      -DBUILD_SHARED_LIBS=ON \
      -DWITH_TE=ON \
      -DWITH_STORE=OFF \
      -DWITH_STORE_RUST=OFF \
      -DWITH_RUST_EXAMPLE=OFF \
      -DWITH_P2P_STORE=OFF \
      -DBUILD_UNIT_TESTS=OFF \
      -DUSE_ETCD=OFF
    log "==> mooncake: ninja build (engine.so + tebench)"
    run cmake --build "${checkout}/build" -j "${JOBS}"

    if [[ "${DRY_RUN}" != "1" ]]; then
      local tebench="${checkout}/build/mooncake-transfer-engine/benchmark/tebench"
      [[ -x "${tebench}" ]] || die "mooncake: tebench not produced at ${tebench}"
      log "    tebench: ${tebench} ($(stat -c%s "${tebench}") bytes)"
    fi
  fi

  log "==> mooncake: invoking official scripts/build_wheel.sh (copies .so + builds wheel)"
  if [[ "${SKIP_BINARIES}" == "1" ]]; then
    log "    --skip-binaries: falling back to plain pip wheel mooncake-wheel/"
    run_in "${checkout}/${build_path}" "${PYTHON_BIN}" -m pip wheel --no-deps . -w "${out}"
  else
    # Mooncake's scripts/build_wheel.sh writes to <repo>/mooncake-wheel/dist/
    # by default. After it succeeds, copy the produced wheel into BenchP2P's
    # wheelhouse so the rest of BenchP2P can pick it up.
    run_in "${checkout}" bash scripts/build_wheel.sh \
      "$("${PYTHON_BIN}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
      dist
    if [[ "${DRY_RUN}" != "1" ]]; then
      local src_dist="${checkout}/mooncake-wheel/dist"
      local found
      found="$(find "${src_dist}" -maxdepth 1 -type f -name 'mooncake_transfer_engine-*.whl' -printf '%T@\t%p\n' \
                | sort -nr | head -n 1 | cut -f2-)"
      if [[ -z "${found}" ]]; then
        die "mooncake: scripts/build_wheel.sh produced no wheel under ${src_dist}"
      fi
      log "    copying ${found} -> ${out}/"
      cp "${found}" "${out}/"
    fi
  fi
  verify_wheel "mooncake" "${wheel_glob}"
}

# ---------------------------------------------------------------- nixl ---
# Per nixl/README.md and benchmark/nixlbench/README.md (Manual section):
#   1. cd nixl && meson setup build --prefix=/usr/local/nixl --buildtype=release
#   2. cd build && ninja && ninja install
#   3. cd benchmark/nixlbench && meson setup build -Dnixl_path=/usr/local/nixl
#   4. cd build && ninja
#   5. pip wheel . to produce the python wheel that BenchP2P needs

build_one_nixl() {
  local checkout="$1" build_path="$2" wheel_glob="$3"
  local out="${WHEELHOUSE}/nixl"
  local nixlbench_src="${checkout}/benchmark/nixlbench"
  local nixl_build="${checkout}/build"
  local bench_build="${nixlbench_src}/build"
  clean_wheelhouse_for "nixl"

  if [[ "${SKIP_BINARIES}" != "1" ]]; then
    command -v meson >/dev/null 2>&1 || die "nixl: meson not on PATH (apt install meson, or pip install meson)"
    command -v ninja >/dev/null 2>&1 || die "nixl: ninja not on PATH"

    log "==> nixl: meson setup core (prefix=${NIXL_PREFIX})"
    [[ -d "${nixl_build}" ]] && run rm -rf "${nixl_build}"
    run meson setup "${nixl_build}" "${checkout}" --prefix="${NIXL_PREFIX}" --buildtype=release
    log "==> nixl: ninja install core (provides nixl.pc for nixlbench)"
    run ninja -C "${nixl_build}" -j "${JOBS}"
    run ninja -C "${nixl_build}" install

    log "==> nixlbench: meson setup (-Dnixl_path=${NIXL_PREFIX})"
    [[ -d "${bench_build}" ]] && run rm -rf "${bench_build}"
    export PKG_CONFIG_PATH="${NIXL_PREFIX}/lib/x86_64-linux-gnu/pkgconfig:${NIXL_PREFIX}/lib64/pkgconfig:${NIXL_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    run meson setup "${bench_build}" "${nixlbench_src}" \
      -Dnixl_path="${NIXL_PREFIX}" --buildtype=release
    log "==> nixlbench: ninja"
    run ninja -C "${bench_build}" -j "${JOBS}"
    if [[ "${DRY_RUN}" != "1" ]]; then
      local nixlbench_bin
      nixlbench_bin="$(find "${bench_build}" -type f -name nixlbench -executable | head -1)"
      [[ -n "${nixlbench_bin}" ]] || die "nixlbench: binary not produced under ${bench_build}"
      log "    nixlbench: ${nixlbench_bin} ($(stat -c%s "${nixlbench_bin}") bytes)"
    fi
  fi

  log "==> nixl: pip wheel"
  run_in "${checkout}/${build_path}" "${PYTHON_BIN}" -m pip wheel --no-deps . -w "${out}"
  verify_wheel "nixl" "${wheel_glob}"
}

# --------------------------------------------------------------- driver ---

build_one() {
  local name="$1"
  local entry repo ref path build_path wheel_glob
  entry="$(jq -r --arg n "${name}" '.repos[] | select(.name==$n)' "${MANIFEST}")"
  [[ -n "${entry}" ]] || die "manifest has no entry for ${name}"
  repo="$(jq -r '.repo' <<< "${entry}")"
  ref="$(jq -r '.ref // "main"' <<< "${entry}")"
  path="$(jq -r '.path' <<< "${entry}")"
  build_path="$(jq -r '.build_path // "."' <<< "${entry}")"
  wheel_glob="$(jq -r '.wheel_glob // "*.whl"' <<< "${entry}")"

  log ""
  log "==> ${name}: ${repo} @ ${ref}"
  local checkout
  checkout="$(ensure_checkout "${name}" "${repo}" "${ref}" "${path}")"
  case "${name}" in
    mori) build_one_mori "${checkout}" "${build_path}" "${wheel_glob}" ;;
    uccl) build_one_uccl "${checkout}" "${build_path}" "${wheel_glob}" ;;
    mooncake) build_one_mooncake "${checkout}" "${build_path}" "${wheel_glob}" ;;
    nixl) build_one_nixl "${checkout}" "${build_path}" "${wheel_glob}" ;;
    *) die "no per-backend build dispatch for: ${name}" ;;
  esac
}

FAILED=()
for name in "${SELECTED[@]}"; do
  if build_one "${name}"; then
    :
  else
    rc=$?
    log "${name}: build failed (exit ${rc})"
    FAILED+=("${name}")
    if [[ "${CONTINUE_ON_ERROR}" != "1" ]]; then
      die "aborting after ${name} failure (use --continue-on-error to skip)"
    fi
  fi
done

if [[ ${#FAILED[@]} -gt 0 ]]; then
  log "summary: ${#FAILED[@]} backend(s) failed: ${FAILED[*]}"
  exit 1
fi

log "summary: built ${#SELECTED[@]} backend(s) successfully"
