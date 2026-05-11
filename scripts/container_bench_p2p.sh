#!/usr/bin/env bash
# Run scripts/bench_p2p_compare.py inside a runtime container.
#
# Usage:
#   bash scripts/container_bench_p2p.sh [container-options] [-- <bench args>]
#
# Container options:
#   --image IMAGE            Runtime image (default: docker.io/rocm/primus:v26.2)
#   --docker-bin BIN         Docker CLI (default: docker)
#   --container-python BIN   Python interpreter inside container (default: python3)
#   --workdir DIR            Container --workdir (default: repo root)
#   --command run|report     bench_p2p_compare.py subcommand (default: run)
#   --extra-mount SRC:DST    Extra bind mount, repeatable
#   --extra-docker-args STR  Extra args appended to docker run (parsed with shlex)
#   --pull                   Pass --pull=always to docker run
#   --mount-home             Mount $HOME at /root/home inside the container
#   --dry-run                Print docker command without executing
#   -h, --help               Show this help
#
# All remaining arguments (or those after `--`) are forwarded to
# bench_p2p_compare.py inside the container. The wrapper auto-detects
# --output-dir / --source-root / --wheelhouse / --manifest from the inner
# args and adds bind mounts for any path that lives outside the repo so
# logs and reports survive container teardown.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE="docker.io/rocm/primus:v26.2"
DOCKER_BIN="docker"
CONTAINER_PYTHON="python3"
WORKDIR="${REPO_ROOT}"
SUBCOMMAND="run"
PULL="0"
MOUNT_HOME="0"
DRY_RUN="0"
EXTRA_MOUNTS=()
EXTRA_DOCKER_ARGS=""
INNER_ARGS=()

usage() {
  cat <<'EOF'
Run scripts/bench_p2p_compare.py inside a runtime container.

Usage:
  bash scripts/container_bench_p2p.sh [container-options] [-- <bench args>]

Container options:
  --image IMAGE            Runtime image (default: docker.io/rocm/primus:v26.2)
  --docker-bin BIN         Docker CLI (default: docker)
  --container-python BIN   Python interpreter inside container (default: python3)
  --workdir DIR            Container --workdir (default: repo root)
  --command run|report     bench_p2p_compare.py subcommand (default: run)
  --extra-mount SRC:DST    Extra bind mount, repeatable
  --extra-docker-args STR  Extra args appended to docker run (parsed with shlex)
  --pull                   Pass --pull=always to docker run
  --mount-home             Mount $HOME at /root/home inside the container
  --dry-run                Print docker command without executing
  -h, --help               Show this help

All remaining arguments (or those after `--`) are forwarded to
bench_p2p_compare.py inside the container. The wrapper auto-mounts
--output-dir / --source-root / --wheelhouse / --manifest if they live
outside the repo so logs and reports survive container teardown.
EOF
}

die() { printf '[container_bench_p2p] ERROR: %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --container-python) CONTAINER_PYTHON="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    --command) SUBCOMMAND="$2"; shift 2 ;;
    --extra-mount) EXTRA_MOUNTS+=("$2"); shift 2 ;;
    --extra-docker-args) EXTRA_DOCKER_ARGS="$2"; shift 2 ;;
    --pull) PULL="1"; shift ;;
    --mount-home) MOUNT_HOME="1"; shift ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; INNER_ARGS+=("$@"); break ;;
    *) INNER_ARGS+=("$1"); shift ;;
  esac
done

command -v "${DOCKER_BIN}" >/dev/null 2>&1 || die "${DOCKER_BIN} not in PATH"
case "${SUBCOMMAND}" in run|report) ;; *) die "--command must be run or report" ;; esac

DOCKER=(
  "${DOCKER_BIN}" run --rm
  --ipc=host --network=host
  --device=/dev/kfd --device=/dev/dri --device=/dev/infiniband
  --cap-add=SYS_PTRACE --cap-add=CAP_SYS_ADMIN
  --security-opt seccomp=unconfined
  --group-add video
  --privileged
  --workdir "${WORKDIR}"
  -v "${REPO_ROOT}:${REPO_ROOT}"
  --env MASTER_ADDR --env MASTER_PORT
  --env RANK --env WORLD_SIZE --env LOCAL_RANK --env LOCAL_WORLD_SIZE
  --env SLURM_JOB_ID --env SLURM_PROCID --env SLURM_NTASKS
  --env SLURM_LOCALID --env SLURM_NTASKS_PER_NODE
)

# Forward every UCCL_*/NCCL_* env from the host so backend tunables
# (UCCL_P2P_RDMA_CC, UCCL_P2P_LOG_LEVEL, NCCL_DEBUG, ...) propagate
# through `slurm_bench_p2p.sh -> container_bench_p2p.sh -> docker run`
# without per-variable plumbing. Docker has no glob form; enumerate at
# launch time. `compgen -e` lists exported var names; we only forward
# those that are actually set (compgen output may include unset names
# in some bash builds, so we re-check).
while IFS= read -r _envname; do
  [[ -z "${_envname}" ]] && continue
  if [[ -n "${!_envname+x}" ]]; then
    DOCKER+=(--env "${_envname}")
  fi
done < <(compgen -e | grep -E '^(UCCL_|NCCL_)' || true)

# Pull paths out of inner args and bind-mount any that fall outside the repo.
declare -A SEEN_MOUNTS=()
add_mount() {
  local p="$1"
  [[ -z "${p}" ]] && return
  local abs
  abs="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "${p}")"
  [[ -z "${abs}" ]] && return
  if [[ "${abs}" == "${REPO_ROOT}"* ]]; then
    return
  fi
  if [[ -z "${SEEN_MOUNTS[${abs}]+x}" ]]; then
    SEEN_MOUNTS["${abs}"]=1
    DOCKER+=(-v "${abs}:${abs}")
  fi
}
for ((i=0; i<${#INNER_ARGS[@]}; i++)); do
  case "${INNER_ARGS[i]}" in
    --output-dir|--source-root|--wheelhouse|--manifest)
      add_mount "${INNER_ARGS[i+1]:-}" ;;
  esac
done

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
  "${CONTAINER_PYTHON}" "${REPO_ROOT}/scripts/bench_p2p_compare.py" "${SUBCOMMAND}"
)
INNER_CMD+=("${INNER_ARGS[@]}")

DOCKER+=("${IMAGE}" bash -lc "$(printf '%q ' "${INNER_CMD[@]}")")

echo "[container_bench_p2p] docker command:"
printf '  %q' "${DOCKER[@]}"
printf '\n'

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

exec "${DOCKER[@]}"
