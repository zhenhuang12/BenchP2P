#!/usr/bin/env bash
# Build the BenchP2P all-backends Docker image declared in ./Dockerfile.
#
# Thin wrapper around `docker build` that:
#   - runs from the repo root regardless of where it's invoked from,
#   - forwards every Dockerfile ARG (APT_PRESET / APT_MIRROR / BACKENDS /
#     JOBS / NIXL_PREFIX / TIMEOUT_S / PIP_INDEX_URL / BASE_IMAGE) as
#     --build-arg,
#   - pre-flights that 3rdparty/<backend> submodules are populated, since
#     the Dockerfile builds with --skip-clone and will fail mid-build
#     otherwise (mooncake/mori pull yalantinglibs / spdlog / msgpack from
#     submodule .git dirs),
#   - supports the usual docker conveniences: --target, --no-cache,
#     --pull, --progress, --platform, --load/--push (buildx), and a
#     --dry-run that just prints the assembled command.
#
# Usage:
#   bash scripts/build_image.sh [options]
#
# Options:
#   -t, --tag TAG              Image tag (default: benchp2p:latest, repeatable)
#   -f, --file PATH            Dockerfile path (default: <repo>/Dockerfile)
#       --context PATH         Build context (default: <repo>)
#       --target STAGE         Stop at this stage (builder|runtime, default: runtime)
#       --base-image IMAGE     Override BASE_IMAGE build-arg
#       --backends LIST        Override BACKENDS build-arg (e.g. mori,uccl)
#       --jobs N               Override JOBS build-arg (default: Dockerfile's $(nproc))
#       --nixl-prefix DIR      Override NIXL_PREFIX build-arg
#       --timeout SECONDS      Override TIMEOUT_S build-arg
#       --pip-index-url URL    Override PIP_INDEX_URL build-arg
#       --apt-preset NAME      Override APT_PRESET build-arg (tuna|aliyun|ustc|...)
#       --apt-mirror URL       Override APT_MIRROR build-arg
#       --build-arg KEY=VALUE  Extra --build-arg to forward, repeatable
#       --no-cache             Pass --no-cache to docker build
#       --pull                 Pass --pull to docker build (refresh BASE_IMAGE)
#       --progress MODE        Pass --progress=MODE (auto|plain|tty)
#       --platform PLATFORM    Pass --platform PLATFORM
#       --buildx               Use `docker buildx build` instead of `docker build`
#       --load                 (buildx) load image into local docker (default on)
#       --push                 (buildx) push to registry instead of --load
#       --docker-bin BIN       Docker CLI (default: docker)
#       --skip-submodule-check Skip the 3rdparty/<backend>/.git pre-flight
#       --dry-run              Print the docker command without executing it
#   -h, --help                 Show this help
#
# Examples:
#   # Default: tag benchp2p:latest, runtime stage, all four backends.
#   bash scripts/build_image.sh
#
#   # Cluster behind the GFW: hit a tuna mirror + use a pip mirror for
#   # the wheel install layer.
#   bash scripts/build_image.sh \
#     --apt-preset tuna \
#     --pip-index-url https://mirrors.aliyun.com/pypi/simple/
#
#   # Just two backends, faster build:
#   bash scripts/build_image.sh --backends mori,uccl --jobs 32
#
#   # Inspect the builder stage without producing the runtime image:
#   bash scripts/build_image.sh --target builder -t benchp2p-builder:latest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DOCKER_BIN="docker"
DOCKERFILE="${REPO_ROOT}/Dockerfile"
CONTEXT="${REPO_ROOT}"
TARGET="runtime"
TAGS=()
EXTRA_BUILD_ARGS=()
BASE_IMAGE=""
BACKENDS=""
JOBS=""
NIXL_PREFIX=""
TIMEOUT_S=""
PIP_INDEX_URL=""
APT_PRESET=""
APT_MIRROR=""
NO_CACHE="0"
PULL="0"
PROGRESS=""
PLATFORM=""
USE_BUILDX="0"
BUILDX_OUTPUT="load"   # load | push | none
SKIP_SUBMODULE_CHECK="0"
DRY_RUN="0"

usage() {
  sed -n '2,/^set -euo pipefail$/p' "${BASH_SOURCE[0]}" \
    | sed -e 's/^# \{0,1\}//' -e '$d'
}

die() { printf '[build_image] ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '[build_image] %s\n' "$*" >&2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--tag) TAGS+=("$2"); shift 2 ;;
    -f|--file) DOCKERFILE="$2"; shift 2 ;;
    --context) CONTEXT="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --base-image) BASE_IMAGE="$2"; shift 2 ;;
    --backends) BACKENDS="$2"; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --nixl-prefix) NIXL_PREFIX="$2"; shift 2 ;;
    --timeout) TIMEOUT_S="$2"; shift 2 ;;
    --pip-index-url) PIP_INDEX_URL="$2"; shift 2 ;;
    --apt-preset) APT_PRESET="$2"; shift 2 ;;
    --apt-mirror) APT_MIRROR="$2"; shift 2 ;;
    --build-arg) EXTRA_BUILD_ARGS+=("$2"); shift 2 ;;
    --no-cache) NO_CACHE="1"; shift ;;
    --pull) PULL="1"; shift ;;
    --progress) PROGRESS="$2"; shift 2 ;;
    --platform) PLATFORM="$2"; shift 2 ;;
    --buildx) USE_BUILDX="1"; shift ;;
    --load) USE_BUILDX="1"; BUILDX_OUTPUT="load"; shift ;;
    --push) USE_BUILDX="1"; BUILDX_OUTPUT="push"; shift ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --skip-submodule-check) SKIP_SUBMODULE_CHECK="1"; shift ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1 (try --help)" ;;
  esac
done

# Default tag if the caller didn't pass any.
if [[ ${#TAGS[@]} -eq 0 ]]; then
  TAGS=("benchp2p:latest")
fi

command -v "${DOCKER_BIN}" >/dev/null 2>&1 || die "${DOCKER_BIN} not in PATH"
[[ -f "${DOCKERFILE}" ]] || die "Dockerfile not found: ${DOCKERFILE}"
[[ -d "${CONTEXT}" ]] || die "context not a directory: ${CONTEXT}"

# Pre-flight: the Dockerfile builds with --skip-clone, so each backend's
# submodules must already be populated in 3rdparty/<backend>/. Check the
# manifest if it's available; otherwise fall back to a directory probe.
if [[ "${SKIP_SUBMODULE_CHECK}" != "1" ]]; then
  manifest="${REPO_ROOT}/3rdparty/manifest.json"
  declare -a backend_dirs=()
  if [[ -f "${manifest}" ]] && command -v jq >/dev/null 2>&1; then
    if [[ -n "${BACKENDS}" ]]; then
      IFS=',' read -r -a _names <<<"${BACKENDS}"
      for n in "${_names[@]}"; do
        d="$(jq -r --arg n "${n}" '.backends[] | select(.name==$n) | .dir' "${manifest}" 2>/dev/null || true)"
        [[ -n "${d}" && "${d}" != "null" ]] && backend_dirs+=("${REPO_ROOT}/${d}")
      done
    else
      while IFS= read -r d; do
        [[ -n "${d}" ]] && backend_dirs+=("${REPO_ROOT}/${d}")
      done < <(jq -r '.backends[].dir' "${manifest}" 2>/dev/null || true)
    fi
  fi
  if [[ ${#backend_dirs[@]} -eq 0 ]]; then
    for d in 3rdparty/mori 3rdparty/Mooncake 3rdparty/uccl 3rdparty/nixl; do
      [[ -d "${REPO_ROOT}/${d}" ]] && backend_dirs+=("${REPO_ROOT}/${d}")
    done
  fi
  missing=()
  for d in "${backend_dirs[@]}"; do
    [[ -d "${d}" ]] || { missing+=("${d} (missing)"); continue; }
    if ! find "${d}" -maxdepth 4 -name .git -print -quit | grep -q .; then
      missing+=("${d} (no .git anywhere; submodules not populated)")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    {
      echo "[build_image] backend submodules look unpopulated:"
      for m in "${missing[@]}"; do echo "  - ${m}"; done
      echo
      echo "Run this once on the host before building:"
      echo "  git -C ${REPO_ROOT} submodule update --init --recursive"
      echo
      echo "Or pass --skip-submodule-check to bypass."
    } >&2
    exit 1
  fi
fi

# Assemble the docker build command.
if [[ "${USE_BUILDX}" == "1" ]]; then
  CMD=("${DOCKER_BIN}" buildx build)
else
  CMD=("${DOCKER_BIN}" build)
fi

CMD+=(-f "${DOCKERFILE}")
[[ -n "${TARGET}" ]] && CMD+=(--target "${TARGET}")
for t in "${TAGS[@]}"; do CMD+=(-t "${t}"); done

add_build_arg() {
  local k="$1" v="$2"
  [[ -z "${v}" ]] && return
  CMD+=(--build-arg "${k}=${v}")
}
add_build_arg BASE_IMAGE     "${BASE_IMAGE}"
add_build_arg APT_PRESET     "${APT_PRESET}"
add_build_arg APT_MIRROR     "${APT_MIRROR}"
add_build_arg BACKENDS       "${BACKENDS}"
add_build_arg JOBS           "${JOBS}"
add_build_arg NIXL_PREFIX    "${NIXL_PREFIX}"
add_build_arg TIMEOUT_S      "${TIMEOUT_S}"
add_build_arg PIP_INDEX_URL  "${PIP_INDEX_URL}"
for ba in "${EXTRA_BUILD_ARGS[@]}"; do
  CMD+=(--build-arg "${ba}")
done

[[ "${NO_CACHE}" == "1" ]] && CMD+=(--no-cache)
[[ "${PULL}" == "1" ]] && CMD+=(--pull)
[[ -n "${PROGRESS}" ]] && CMD+=(--progress "${PROGRESS}")
[[ -n "${PLATFORM}" ]] && CMD+=(--platform "${PLATFORM}")

if [[ "${USE_BUILDX}" == "1" ]]; then
  case "${BUILDX_OUTPUT}" in
    load) CMD+=(--load) ;;
    push) CMD+=(--push) ;;
    none) ;;
    *) die "unknown buildx output mode: ${BUILDX_OUTPUT}" ;;
  esac
fi

CMD+=("${CONTEXT}")

log "docker command:"
printf '  %q' "${CMD[@]}" >&2
printf '\n' >&2

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

exec "${CMD[@]}"
