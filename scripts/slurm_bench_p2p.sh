#!/usr/bin/env bash
# Submit BenchP2P with srun. Each Slurm task launches container_bench_p2p.sh,
# which docker-runs bench_p2p_compare.py inside the runtime image. After srun
# returns, this script invokes bench_p2p_compare.py report on the submission
# host to aggregate per-rank logs into the final summary report.
#
# Usage:
#   bash scripts/slurm_bench_p2p.sh [slurm-options] [container-options] [-- <bench args>]
#
# Slurm options (forwarded to srun):
#   --srun BIN                       srun executable (default: srun)
#   --nodes N                        --nodes (default: 2)
#   --ntasks N                       --ntasks (default: 2)
#   --ntasks-per-node N              --ntasks-per-node (default: 1)
#   --partition / --account /        Standard srun selectors (passed through)
#     --qos / --time / --constraint /
#     --gres / --gpus-per-task /
#     --cpus-per-task / --job-name
#   --master-port N                  Distributed init port (default: 29500)
#   --extra-srun-args STR            Appended to srun, parsed with shlex
#   --skip-report                    Skip the post-srun report step
#   --report-only                    Only run the report step on the submission host
#   --standalone-allocation          Strip SLURM_* env so srun creates a NEW
#                                    allocation instead of inheriting the
#                                    surrounding salloc/sbatch one
#
# Container options (forwarded to container_bench_p2p.sh):
#   --image IMAGE                    Runtime image (default: docker.io/rocm/primus:v26.2)
#   --docker-bin BIN                 Docker CLI (default: docker)
#   --container-python BIN           Python in container (default: python3)
#   --pull / --mount-home            Forwarded to container_bench_p2p.sh
#   --extra-mount SRC:DST            Extra bind mount (repeatable)
#   --extra-docker-args STR          Extra docker run args
#
# Common options:
#   --output-dir DIR                 Output dir on shared FS (default: results/p2p_compare_<ts>)
#   --dry-run                        Print srun + docker + report commands without running
#   -h, --help                       Show this help
#
# Everything after `--` (or any unrecognised arg) is forwarded to
# bench_p2p_compare.py inside the container. The wrapper auto-injects
# --output-dir on both srun-side and report-side so logs land in one place.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONTAINER_BENCH="${SCRIPT_DIR}/container_bench_p2p.sh"

SRUN_BIN="srun"
NODES="2"
NTASKS="2"
NTASKS_PER_NODE="1"
MASTER_PORT="29500"
PARTITION=""
ACCOUNT=""
QOS=""
TIME=""
CONSTRAINT=""
GRES=""
GPUS_PER_TASK=""
CPUS_PER_TASK=""
JOB_NAME="benchp2p"
EXTRA_SRUN_ARGS=""
SKIP_REPORT="0"
REPORT_ONLY="0"
STANDALONE_ALLOC="1"

IMAGE="docker.io/rocm/primus:v26.2"
DOCKER_BIN="docker"
CONTAINER_PYTHON="python3"
PULL="0"
MOUNT_HOME="0"
EXTRA_MOUNTS=()
EXTRA_DOCKER_ARGS=""

OUTPUT_DIR=""
DRY_RUN="0"
INNER_ARGS=()

export NCCL_IB_HCA=^mlx5_1,mlx5_6
export UCCL_IB_HCA=^mlx5_1,mlx5_6
export MORI_RDMA_DEVICE=^mlx5_1,mlx5_6

usage() {
  cat <<'EOF'
Submit BenchP2P with srun + container_bench_p2p.sh, then aggregate logs.

Usage:
  bash scripts/slurm_bench_p2p.sh [slurm-options] [container-options] [-- <bench args>]

Slurm options (forwarded to srun):
  --srun BIN                       srun executable (default: srun)
  --nodes N                        --nodes (default: 2)
  --ntasks N                       --ntasks (default: 2)
  --ntasks-per-node N              --ntasks-per-node (default: 1)
  --partition / --account /        Standard srun selectors (passed through)
    --qos / --time / --constraint /
    --gres / --gpus-per-task /
    --cpus-per-task / --job-name
  --master-port N                  Distributed init port (default: 29500)
  --extra-srun-args STR            Appended to srun, parsed with shlex
  --skip-report                    Skip the post-srun report step
  --report-only                    Only run the report step on the submission host
  --standalone-allocation          Strip SLURM_* env so srun creates a NEW
                                   allocation instead of inheriting the
                                   surrounding salloc/sbatch one

Container options (forwarded to container_bench_p2p.sh):
  --image IMAGE                    Runtime image (default: docker.io/rocm/primus:v26.2)
  --docker-bin BIN                 Docker CLI (default: docker)
  --container-python BIN           Python in container (default: python3)
  --pull / --mount-home            Forwarded to container_bench_p2p.sh
  --extra-mount SRC:DST            Extra bind mount (repeatable)
  --extra-docker-args STR          Extra docker run args

Common options:
  --output-dir DIR                 Output dir on shared FS (default: results/p2p_compare_<ts>)
  --dry-run                        Print srun + docker + report commands without running
  -h, --help                       Show this help

Everything after `--` (or any unrecognised arg) is forwarded to
bench_p2p_compare.py inside the container.
EOF
}

die() { printf '[slurm_bench_p2p] ERROR: %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    # --- slurm flags ---
    --srun) SRUN_BIN="$2"; shift 2 ;;
    --nodes) NODES="$2"; shift 2 ;;
    --ntasks) NTASKS="$2"; shift 2 ;;
    --ntasks-per-node) NTASKS_PER_NODE="$2"; shift 2 ;;
    --master-port) MASTER_PORT="$2"; shift 2 ;;
    --partition) PARTITION="$2"; shift 2 ;;
    --account) ACCOUNT="$2"; shift 2 ;;
    --qos) QOS="$2"; shift 2 ;;
    --time) TIME="$2"; shift 2 ;;
    --constraint) CONSTRAINT="$2"; shift 2 ;;
    --gres) GRES="$2"; shift 2 ;;
    --gpus-per-task) GPUS_PER_TASK="$2"; shift 2 ;;
    --cpus-per-task) CPUS_PER_TASK="$2"; shift 2 ;;
    --job-name) JOB_NAME="$2"; shift 2 ;;
    --extra-srun-args) EXTRA_SRUN_ARGS="$2"; shift 2 ;;
    --skip-report) SKIP_REPORT="1"; shift ;;
    --report-only) REPORT_ONLY="1"; shift ;;
    --standalone-allocation) STANDALONE_ALLOC="1"; shift ;;
    # --- container flags ---
    --image) IMAGE="$2"; shift 2 ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --container-python) CONTAINER_PYTHON="$2"; shift 2 ;;
    --pull) PULL="1"; shift ;;
    --mount-home) MOUNT_HOME="1"; shift ;;
    --extra-mount) EXTRA_MOUNTS+=("$2"); shift 2 ;;
    --extra-docker-args) EXTRA_DOCKER_ARGS="$2"; shift 2 ;;
    # --- common ---
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; INNER_ARGS+=("$@"); break ;;
    *) INNER_ARGS+=("$1"); shift ;;
  esac
done

# Auto-detect or set --output-dir.
INNER_HAS_OUTPUT="0"
for ((i=0; i<${#INNER_ARGS[@]}; i++)); do
  if [[ "${INNER_ARGS[i]}" == "--output-dir" ]]; then
    INNER_HAS_OUTPUT="1"
    OUTPUT_DIR="${INNER_ARGS[i+1]}"
    break
  fi
done
if [[ "${INNER_HAS_OUTPUT}" == "0" ]]; then
  if [[ -z "${OUTPUT_DIR}" ]]; then
    OUTPUT_DIR="${REPO_ROOT}/results/p2p_compare_$(date +%Y%m%d_%H%M%S)"
  fi
  INNER_ARGS+=("--output-dir" "${OUTPUT_DIR}")
fi
if [[ "${DRY_RUN}" != "1" ]]; then
  mkdir -p "${OUTPUT_DIR}/logs"
fi

run_report() {
  local report_cmd=(
    python3 "${REPO_ROOT}/scripts/bench_p2p_compare.py" report
    --output-dir "${OUTPUT_DIR}"
  )
  echo "[slurm_bench_p2p] report command:"
  printf '  %q' "${report_cmd[@]}"; printf '\n'
  if [[ "${DRY_RUN}" == "1" ]]; then
    return 0
  fi
  "${report_cmd[@]}"
}

if [[ "${REPORT_ONLY}" == "1" ]]; then
  run_report
  exit 0
fi

# Build container_bench_p2p.sh argv. We pass slurm-set env vars by reference
# so each task picks them up from its own shell (the container runs with
# --env RANK --env WORLD_SIZE etc.).
CONTAINER_CMD=(
  bash "${CONTAINER_BENCH}"
  --image "${IMAGE}"
  --docker-bin "${DOCKER_BIN}"
  --container-python "${CONTAINER_PYTHON}"
  --command run
)
[[ "${PULL}" == "1" ]] && CONTAINER_CMD+=(--pull)
[[ "${MOUNT_HOME}" == "1" ]] && CONTAINER_CMD+=(--mount-home)
for m in "${EXTRA_MOUNTS[@]}"; do
  CONTAINER_CMD+=(--extra-mount "${m}")
done
[[ -n "${EXTRA_DOCKER_ARGS}" ]] && CONTAINER_CMD+=(--extra-docker-args "${EXTRA_DOCKER_ARGS}")
CONTAINER_CMD+=(--)
CONTAINER_CMD+=("${INNER_ARGS[@]}")

CONTAINER_CMD_STR="$(printf '%q ' "${CONTAINER_CMD[@]}")"

PREAMBLE='set -euo pipefail
SLURM_PROCID="${SLURM_PROCID:-0}"
SLURM_NTASKS="${SLURM_NTASKS:-1}"
SLURM_LOCALID="${SLURM_LOCALID:-0}"
SLURM_NODELIST_VALUE="${SLURM_JOB_NODELIST:-${SLURM_NODELIST:-}}"
if [[ -z "${MASTER_ADDR:-}" ]]; then
  if [[ -n "${SLURM_NODELIST_VALUE}" ]] && command -v scontrol >/dev/null 2>&1; then
    MASTER_ADDR="$(scontrol show hostnames "${SLURM_NODELIST_VALUE}" | sed -n '\''1p'\'')"
  fi
  MASTER_ADDR="${MASTER_ADDR:-$(hostname -f 2>/dev/null || hostname)}"
fi
MASTER_PORT="${MASTER_PORT:-'"${MASTER_PORT}"'}"
export MASTER_ADDR MASTER_PORT
export RANK="${SLURM_PROCID}"
export WORLD_SIZE="${SLURM_NTASKS}"
export LOCAL_RANK="${SLURM_LOCALID}"
export LOCAL_WORLD_SIZE="${SLURM_NTASKS_PER_NODE:-1}"'

# When --standalone-allocation is on, prepend `env -u SLURM_*` so srun does
# not inherit the surrounding salloc/sbatch allocation and is forced to
# request a fresh one matching --nodes / --ntasks. The list mirrors the
# SLURM_* vars that srun consults to decide "am I inside an allocation?".
SRUN=()
if [[ "${STANDALONE_ALLOC}" == "1" ]]; then
  SRUN+=(
    env
    -u SLURM_JOB_ID -u SLURM_JOBID
    -u SLURM_NODELIST -u SLURM_JOB_NODELIST
    -u SLURM_NNODES -u SLURM_JOB_NUM_NODES
    -u SLURM_NTASKS -u SLURM_NPROCS
    -u SLURM_TASKS_PER_NODE -u SLURM_NTASKS_PER_NODE
    -u SLURM_JOB_PARTITION -u SLURM_JOB_QOS -u SLURM_JOB_ACCOUNT
    -u SLURM_STEP_ID -u SLURM_STEP_NODELIST -u SLURM_STEP_NUM_NODES -u SLURM_STEP_NUM_TASKS
    -u SLURM_SUBMIT_DIR -u SLURM_SUBMIT_HOST
    -u SLURM_CLUSTER_NAME -u SLURM_JOB_CPUS_PER_NODE
  )
fi
SRUN+=(
  "${SRUN_BIN}"
  "--nodes=${NODES}"
  "--ntasks=${NTASKS}"
  "--ntasks-per-node=${NTASKS_PER_NODE}"
  "--kill-on-bad-exit=1"
  "--export=ALL"
)
[[ -n "${PARTITION}" ]] && SRUN+=("--partition=${PARTITION}")
[[ -n "${ACCOUNT}" ]] && SRUN+=("--account=${ACCOUNT}")
[[ -n "${QOS}" ]] && SRUN+=("--qos=${QOS}")
[[ -n "${TIME}" ]] && SRUN+=("--time=${TIME}")
[[ -n "${CONSTRAINT}" ]] && SRUN+=("--constraint=${CONSTRAINT}")
[[ -n "${GRES}" ]] && SRUN+=("--gres=${GRES}")
[[ -n "${GPUS_PER_TASK}" ]] && SRUN+=("--gpus-per-task=${GPUS_PER_TASK}")
[[ -n "${CPUS_PER_TASK}" ]] && SRUN+=("--cpus-per-task=${CPUS_PER_TASK}")
[[ -n "${JOB_NAME}" ]] && SRUN+=("--job-name=${JOB_NAME}")

if [[ -n "${EXTRA_SRUN_ARGS}" ]]; then
  mapfile -t _EXTRA < <(python3 -c 'import shlex,sys; print("\n".join(shlex.split(sys.argv[1])))' "${EXTRA_SRUN_ARGS}")
  SRUN+=("${_EXTRA[@]}")
fi

BODY="${PREAMBLE}
exec ${CONTAINER_CMD_STR}"

SRUN+=(bash -lc "${BODY}")

echo "[slurm_bench_p2p] output dir: ${OUTPUT_DIR}"
echo "[slurm_bench_p2p] srun command:"
printf '  %q' "${SRUN[@]}"; printf '\n'

if [[ "${DRY_RUN}" == "1" ]]; then
  if [[ "${SKIP_REPORT}" != "1" ]]; then
    run_report
  fi
  exit 0
fi

set +e
"${SRUN[@]}"
SRUN_RC=$?
set -e

if [[ ${SRUN_RC} -ne 0 ]]; then
  echo "[slurm_bench_p2p] srun exited with code ${SRUN_RC}; will still attempt report" >&2
fi

if [[ "${SKIP_REPORT}" != "1" ]]; then
  run_report
fi

exit ${SRUN_RC}
