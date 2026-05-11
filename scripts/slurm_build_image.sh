#!/usr/bin/env bash
# Run scripts/build_image.sh inside a Slurm allocation. The actual
# work (docker build of ./Dockerfile) is single-host but typically
# CPU-heavy (~30-45 min on a 64-core box for all four backends), so
# pinning it to a fat compute node via Slurm tends to be a lot
# friendlier than running it on a busy login node.
#
# Layout mirrors scripts/slurm_bench_p2p.sh:
#   - All slurm flags up front, container/build flags after,
#   - Everything past `--` (or any unrecognised arg) is forwarded
#     verbatim to scripts/build_image.sh.
#
# Usage:
#   bash scripts/slurm_build_image.sh [slurm-options] [build-options]
#       [-- <build_image.sh args>]
#
# Slurm options (forwarded to srun):
#   --srun BIN                    srun executable (default: srun)
#   --nodes N                     --nodes (default: 1; build is single-host)
#   --ntasks N                    --ntasks (default: 1)
#   --ntasks-per-node N           --ntasks-per-node (default: 1)
#   --partition / --account /     Standard srun selectors (passed through)
#     --qos / --time / --constraint /
#     --gres / --gpus-per-task /
#     --cpus-per-task / --job-name /
#     --nodelist / --exclude
#   --extra-srun-args STR         Appended to srun, parsed with shlex
#   --standalone-allocation       Strip SLURM_* env so srun creates a NEW
#                                 allocation instead of inheriting the
#                                 surrounding salloc/sbatch one
#   --no-standalone-allocation    Inherit the surrounding allocation
#                                 (default behaviour without this script)
#
# Build wrapper options:
#   --build-script PATH           Wrapper to run inside the allocation
#                                 (default: scripts/build_image.sh)
#   --log-dir DIR                 Tee srun output to DIR/build_<ts>.log on
#                                 the submission host's shared FS
#                                 (default: <repo>/results/build_image)
#   --save-image PATH.tar         After a successful build, run
#                                 `docker save <tag> -o PATH.tar` on the
#                                 build node so the image can be loaded on
#                                 other nodes that don't share the daemon
#   --dry-run                     Print srun command without executing
#   -h, --help                    Show this help
#
# Anything after `--` (or any unrecognised arg) is forwarded as-is to
# scripts/build_image.sh; e.g.:
#
#   bash scripts/slurm_build_image.sh \
#     --partition mi300x --time 02:00:00 --cpus-per-task 64 \
#     -- --apt-preset tuna --pip-index-url https://mirrors.aliyun.com/pypi/simple/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# useocpm2m-097-[132,135]
SRUN_BIN="srun"
NODES="1"
NTASKS_PER_NODE="1"
PARTITION=""
ACCOUNT=""
QOS=""
TIME=""
CONSTRAINT=""
GRES=""
GPUS_PER_TASK=""
CPUS_PER_TASK=""
JOB_NAME="benchp2p-build"
NODELIST="useocpm2m-097-[132,135]"
EXCLUDE=""
EXTRA_SRUN_ARGS=""
STANDALONE_ALLOC="1"

BUILD_SCRIPT="${SCRIPT_DIR}/build_image.sh"
LOG_DIR=""
SAVE_IMAGE=""
DRY_RUN="0"
INNER_ARGS=()

usage() {
  sed -n '2,/^set -euo pipefail$/p' "${BASH_SOURCE[0]}" \
    | sed -e 's/^# \{0,1\}//' -e '$d'
}

die() { printf '[slurm_build_image] ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '[slurm_build_image] %s\n' "$*" >&2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    # --- slurm flags ---
    --srun) SRUN_BIN="$2"; shift 2 ;;
    --nodes) NODES="$2"; shift 2 ;;
    --ntasks-per-node) NTASKS_PER_NODE="$2"; shift 2 ;;
    --partition) PARTITION="$2"; shift 2 ;;
    --account) ACCOUNT="$2"; shift 2 ;;
    --qos) QOS="$2"; shift 2 ;;
    --time) TIME="$2"; shift 2 ;;
    --constraint) CONSTRAINT="$2"; shift 2 ;;
    --gres) GRES="$2"; shift 2 ;;
    --gpus-per-task) GPUS_PER_TASK="$2"; shift 2 ;;
    --cpus-per-task) CPUS_PER_TASK="$2"; shift 2 ;;
    --job-name) JOB_NAME="$2"; shift 2 ;;
    --nodelist|-w) NODELIST="$2"; shift 2 ;;
    --exclude|-x) EXCLUDE="$2"; shift 2 ;;
    --extra-srun-args) EXTRA_SRUN_ARGS="$2"; shift 2 ;;
    --standalone-allocation) STANDALONE_ALLOC="1"; shift ;;
    --no-standalone-allocation) STANDALONE_ALLOC="0"; shift ;;
    # --- build wrapper flags ---
    --build-script) BUILD_SCRIPT="$2"; shift 2 ;;
    --log-dir) LOG_DIR="$2"; shift 2 ;;
    --save-image) SAVE_IMAGE="$2"; shift 2 ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; INNER_ARGS+=("$@"); break ;;
    *) INNER_ARGS+=("$1"); shift ;;
  esac
done

[[ -x "${BUILD_SCRIPT}" || -f "${BUILD_SCRIPT}" ]] \
  || die "build script not found: ${BUILD_SCRIPT}"

# Default log dir on the submission host's shared FS so the srun output
# is captured even if the compute node disappears later.
TS="$(date +%Y%m%d_%H%M%S)"
if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR="${REPO_ROOT}/results/build_image"
fi
LOG_FILE="${LOG_DIR}/build_${TS}.log"
if [[ "${DRY_RUN}" != "1" ]]; then
  mkdir -p "${LOG_DIR}"
fi

# Pull the image tag out of the inner args so we know what to docker-save.
# build_image.sh defaults to benchp2p:latest if -t isn't passed.
SAVE_TAG="benchp2p:latest"
for ((i=0; i<${#INNER_ARGS[@]}; i++)); do
  case "${INNER_ARGS[i]}" in
    -t|--tag) SAVE_TAG="${INNER_ARGS[i+1]:-${SAVE_TAG}}" ;;
  esac
done
if [[ -n "${SAVE_IMAGE}" ]]; then
  SAVE_DIR="$(dirname "${SAVE_IMAGE}")"
  if [[ "${DRY_RUN}" != "1" ]]; then
    mkdir -p "${SAVE_DIR}"
  fi
fi

# Body that runs on the compute node. Wrapped in `set -euo pipefail` so
# a build_image.sh failure aborts the docker save step. We change into
# REPO_ROOT first because build_image.sh resolves its context relative
# to its own location, but log lines are nicer to read with a known cwd.
INNER_BUILD="$(printf '%q ' bash "${BUILD_SCRIPT}" "${INNER_ARGS[@]}")"
BODY=$'set -euo pipefail\n'
BODY+="cd $(printf '%q' "${REPO_ROOT}")"$'\n'
BODY+=$'echo "[slurm_build_image] node=$(hostname) cwd=$PWD start=$(date -Is)"\n'
BODY+="${INNER_BUILD}"$'\n'
if [[ -n "${SAVE_IMAGE}" ]]; then
  BODY+="echo '[slurm_build_image] docker save ${SAVE_TAG} -> ${SAVE_IMAGE}'"$'\n'
  BODY+="docker save $(printf '%q' "${SAVE_TAG}") -o $(printf '%q' "${SAVE_IMAGE}")"$'\n'
  BODY+="ls -lh $(printf '%q' "${SAVE_IMAGE}")"$'\n'
fi
BODY+='echo "[slurm_build_image] done=$(date -Is)"'$'\n'

# Optionally run srun in a fresh allocation. Same env-strip pattern as
# slurm_bench_p2p.sh; useful when invoked from inside an interactive
# salloc / sbatch that has different node-count constraints than the
# build needs (a 2-node bench allocation, say). We also clear SBATCH_*
# defaults: when the wrapping script was launched via sbatch, those
# env vars become srun's defaults and trigger noise like
#   srun: warning: can't run 1 processes on 2 nodes, setting nnodes to 1
# even after we strip the SLURM_* job env.
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
    -u SBATCH_NNODES -u SBATCH_NUM_NODES
    -u SBATCH_NTASKS -u SBATCH_NPROCS
    -u SBATCH_NTASKS_PER_NODE -u SBATCH_TASKS_PER_NODE
    -u SBATCH_PARTITION -u SBATCH_QOS -u SBATCH_ACCOUNT
    -u SBATCH_GRES -u SBATCH_GPUS_PER_TASK -u SBATCH_CPUS_PER_TASK
    -u SBATCH_JOB_NAME -u SBATCH_TIMELIMIT
  )
fi
SRUN+=(
  "${SRUN_BIN}"
  "--nodes=${NODES}"
  "--ntasks-per-node=${NTASKS_PER_NODE}"
  "--kill-on-bad-exit=1"
  "--export=ALL"
)
[[ -n "${PARTITION}" ]]    && SRUN+=("--partition=${PARTITION}")
[[ -n "${ACCOUNT}" ]]      && SRUN+=("--account=${ACCOUNT}")
[[ -n "${QOS}" ]]          && SRUN+=("--qos=${QOS}")
[[ -n "${TIME}" ]]         && SRUN+=("--time=${TIME}")
[[ -n "${CONSTRAINT}" ]]   && SRUN+=("--constraint=${CONSTRAINT}")
[[ -n "${GRES}" ]]         && SRUN+=("--gres=${GRES}")
[[ -n "${GPUS_PER_TASK}" ]] && SRUN+=("--gpus-per-task=${GPUS_PER_TASK}")
[[ -n "${CPUS_PER_TASK}" ]] && SRUN+=("--cpus-per-task=${CPUS_PER_TASK}")
[[ -n "${JOB_NAME}" ]]     && SRUN+=("--job-name=${JOB_NAME}")
[[ -n "${NODELIST}" ]]     && SRUN+=("--nodelist=${NODELIST}")
[[ -n "${EXCLUDE}" ]]      && SRUN+=("--exclude=${EXCLUDE}")

if [[ -n "${EXTRA_SRUN_ARGS}" ]]; then
  mapfile -t _EXTRA < <(python3 -c 'import shlex,sys; print("\n".join(shlex.split(sys.argv[1])))' "${EXTRA_SRUN_ARGS}")
  SRUN+=("${_EXTRA[@]}")
fi

SRUN+=(bash -lc "${BODY}")

log "log file:   ${LOG_FILE}"
log "image tag:  ${SAVE_TAG}"
[[ -n "${SAVE_IMAGE}" ]] && log "save tar:   ${SAVE_IMAGE}"
log "srun command:"
printf '  %q' "${SRUN[@]}" >&2
printf '\n' >&2

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

# tee both stdout/stderr to the log file while preserving the srun exit
# status (default behaviour of `cmd | tee` would clobber it). We use a
# process substitution so the parent shell still sees srun's rc.
set +e
"${SRUN[@]}" > >(tee "${LOG_FILE}") 2>&1
SRUN_RC=$?
set -e

log "srun exited with code ${SRUN_RC}"
log "full log: ${LOG_FILE}"
exit ${SRUN_RC}
