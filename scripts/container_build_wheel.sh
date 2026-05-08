#!/usr/bin/env bash
# Build BenchP2P third-party wheels inside a runtime container image.
#
# This is a thin docker-run wrapper around scripts/build_wheel.sh. It mounts
# the BenchP2P repo into the container and forwards every argument after `--`
# (or every unrecognised argument) to build_wheel.sh inside the container.
#
# Usage:
#   bash scripts/container_build_wheel.sh [container-options] [-- <build_wheel.sh args>]
#
# Container options:
#   --image IMAGE            Runtime image (default: docker.io/rocm/primus:v26.2)
#   --docker-bin BIN         Docker CLI (default: docker)
#   --container-python BIN   Python interpreter inside container (default: python3)
#   --pull                   Pass --pull=always to docker run
#   --mount-home             Mount $HOME at /root/home inside the container
#   --extra-mount SRC:DST    Extra bind mount, repeatable
#   --extra-docker-args STR  Extra args appended to docker run (parsed with shlex)
#   --dry-run                Print the docker command without executing it
#   -h, --help               Show this help
#
# All remaining arguments (before or after `--`) are forwarded as-is to
# scripts/build_wheel.sh inside the container; --thirdparty-dir / --wheelhouse
# default to the host paths under the bind-mounted repo so the produced wheels
# are visible on the host filesystem after the container exits.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="docker.io/rocm/primus:v26.2"
DOCKER_BIN="docker"
CONTAINER_PYTHON="python3"
PULL="0"
MOUNT_HOME="0"
DRY_RUN="0"
EXTRA_MOUNTS=()
EXTRA_DOCKER_ARGS=""
INNER_ARGS=()

usage() {
  cat <<'EOF'
Build BenchP2P third-party wheels inside a runtime container image.

This is a thin docker-run wrapper around scripts/build_wheel.sh. It mounts
the BenchP2P repo into the container and forwards every argument after `--`
(or every unrecognised argument) to build_wheel.sh inside the container.

Usage:
  bash scripts/container_build_wheel.sh [container-options] [-- <build_wheel.sh args>]

Container options:
  --image IMAGE            Runtime image (default: docker.io/rocm/primus:v26.2)
  --docker-bin BIN         Docker CLI (default: docker)
  --container-python BIN   Python interpreter inside container (default: python3)
  --pull                   Pass --pull=always to docker run
  --mount-home             Mount $HOME at /root/home inside the container
  --extra-mount SRC:DST    Extra bind mount, repeatable
  --extra-docker-args STR  Extra args appended to docker run (parsed with shlex)
  --dry-run                Print the docker command without executing it
  -h, --help               Show this help

All remaining arguments (before or after `--`) are forwarded as-is to
scripts/build_wheel.sh inside the container; --thirdparty-dir / --wheelhouse
default to host paths under the bind-mounted repo so the produced wheels are
visible on the host filesystem after the container exits.
EOF
}

die() { printf '[container_build_wheel] ERROR: %s\n' "$*" >&2; exit 1; }

# Stop consuming our own flags after `--`; everything after that goes inline
# to build_wheel.sh. Anything we don't recognise before `--` is also passed
# through, so callers can mix container options with build-wheel options.
while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --container-python) CONTAINER_PYTHON="$2"; shift 2 ;;
    --pull) PULL="1"; shift ;;
    --mount-home) MOUNT_HOME="1"; shift ;;
    --extra-mount) EXTRA_MOUNTS+=("$2"); shift 2 ;;
    --extra-docker-args) EXTRA_DOCKER_ARGS="$2"; shift 2 ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; INNER_ARGS+=("$@"); break ;;
    *) INNER_ARGS+=("$1"); shift ;;
  esac
done

command -v "${DOCKER_BIN}" >/dev/null 2>&1 || die "${DOCKER_BIN} not in PATH"

DOCKER=(
  "${DOCKER_BIN}" run --rm
  --ipc=host --network=host
  --device=/dev/kfd --device=/dev/dri --device=/dev/infiniband
  --cap-add=SYS_PTRACE --cap-add=CAP_SYS_ADMIN
  --security-opt seccomp=unconfined
  --group-add video
  --privileged
  --workdir "${REPO_ROOT}"
  -v "${REPO_ROOT}:${REPO_ROOT}"
)

# Make sure the wheelhouse parent (which may be outside the repo if the
# caller overrode --thirdparty-dir / --wheelhouse) is mounted too. We scan
# inner args for these flags so the built wheels survive container teardown.
INNER_THIRDPARTY=""
INNER_WHEELHOUSE=""
for ((i=0; i<${#INNER_ARGS[@]}; i++)); do
  case "${INNER_ARGS[i]}" in
    --thirdparty-dir) INNER_THIRDPARTY="${INNER_ARGS[i+1]:-}" ;;
    --wheelhouse) INNER_WHEELHOUSE="${INNER_ARGS[i+1]:-}" ;;
  esac
done
add_mount_if_external() {
  local p="$1"
  [[ -z "${p}" ]] && return
  # Resolve to absolute path even if it doesn't yet exist.
  local abs
  abs="$(cd "$(dirname "${p}")" 2>/dev/null && pwd)/$(basename "${p}")" || true
  [[ -n "${abs}" ]] || return
  if [[ "${abs}" != "${REPO_ROOT}"* ]]; then
    DOCKER+=(-v "${abs}:${abs}")
  fi
}
add_mount_if_external "${INNER_THIRDPARTY}"
add_mount_if_external "${INNER_WHEELHOUSE}"

for m in "${EXTRA_MOUNTS[@]}"; do
  DOCKER+=(-v "${m}")
done
[[ "${MOUNT_HOME}" == "1" ]] && DOCKER+=(-v "${HOME}:/root/home")
[[ "${PULL}" == "1" ]] && DOCKER+=(--pull=always)

if [[ -n "${EXTRA_DOCKER_ARGS}" ]]; then
  mapfile -t _EXTRA < <(python3 -c 'import shlex,sys; print("\n".join(shlex.split(sys.argv[1])))' "${EXTRA_DOCKER_ARGS}")
  DOCKER+=("${_EXTRA[@]}")
fi

INNER_CMD=(
  bash "${REPO_ROOT}/scripts/build_wheel.sh"
  --python "${CONTAINER_PYTHON}"
)
INNER_CMD+=("${INNER_ARGS[@]}")

DOCKER+=("${IMAGE}" bash -lc "$(printf '%q ' "${INNER_CMD[@]}")")

echo "[container_build_wheel] docker command:"
printf '  %q' "${DOCKER[@]}"
printf '\n'

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

exec "${DOCKER[@]}"
