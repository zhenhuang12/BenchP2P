#!/usr/bin/env bash
# Distribute the BenchP2P image tar to compute nodes via `docker load`.
#
# Companion to scripts/slurm_build_image.sh: that script builds the image
# on ONE node and (with --save-image) writes a tar onto the shared FS;
# this script srun's `docker load -i <tar>` on every target node so each
# of them ends up with the same `benchp2p:latest` in its local docker
# daemon. Required before running scripts/slurm_bench_p2p.sh on a
# cluster without a shared registry.
#
# Default tar path is <repo>/_images/benchp2p.tar (matches the "build
# once + save" convention). If that tar is missing, the script aborts
# with a clear hint to build the image first.
#
# Usage:
#   bash scripts/slurm_load_image.sh [slurm-options] [load-options]
#
# Slurm options (forwarded to srun):
#   --srun BIN                    srun executable (default: srun)
#   --nodes N                     --nodes (default: 2; one srun task per node)
#   --partition / --account /     Standard srun selectors (passed through)
#     --qos / --time / --constraint /
#     --gres / --cpus-per-task /
#     --job-name / --nodelist / --exclude
#   --extra-srun-args STR         Appended to srun, parsed with shlex
#   --standalone-allocation       (default) Strip SLURM_* env so srun creates a
#                                 NEW allocation instead of inheriting the
#                                 surrounding salloc/sbatch one
#   --no-standalone-allocation    Inherit the surrounding allocation
#
# Load options:
#   --image-tar PATH              Tar produced by `docker save` (default:
#                                 <repo>/_images/benchp2p.tar)
#   --expected-tag TAG            Verify after load that this tag exists in
#                                 the daemon (default: benchp2p:latest)
#   --docker-bin BIN              Docker CLI on each node (default: docker)
#   --skip-if-present             Skip `docker load` on a node if --expected-tag
#                                 is already in `docker image inspect`
#   --log-dir DIR                 Tee srun output to DIR/load_<ts>.log
#                                 (default: <repo>/results/load_image)
#   --dry-run                     Print srun + load command without executing
#   -h, --help                    Show this help
#
# Typical workflow:
#   # 1) build once (writes _images/benchp2p.tar onto shared FS):
#   bash scripts/slurm_build_image.sh \
#     --nodelist useocpm2m-097-132 \
#     --save-image _images/benchp2p.tar
#
#   # 2) distribute to every bench node:
#   bash scripts/slurm_load_image.sh \
#     --nodelist 'useocpm2m-097-[132,135]' --nodes 2
#
#   # 3) run the bench:
#   bash scripts/slurm_bench_p2p.sh ...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SRUN_BIN="srun"
NODES="2"
PARTITION=""
ACCOUNT=""
QOS=""
TIME=""
CONSTRAINT=""
GRES=""
CPUS_PER_TASK=""
JOB_NAME="benchp2p-load"
NODELIST="useocpm2m-097-[132,135]"
EXCLUDE=""
EXTRA_SRUN_ARGS=""
STANDALONE_ALLOC="1"

IMAGE_TAR="${REPO_ROOT}/_images/benchp2p.tar"
EXPECTED_TAG="benchp2p:latest"
DOCKER_BIN="docker"
SKIP_IF_PRESENT="0"
LOG_DIR=""
DRY_RUN="0"

usage() {
  sed -n '2,/^set -euo pipefail$/p' "${BASH_SOURCE[0]}" \
    | sed -e 's/^# \{0,1\}//' -e '$d'
}

die() { printf '[slurm_load_image] ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '[slurm_load_image] %s\n' "$*" >&2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    # --- slurm flags ---
    --srun) SRUN_BIN="$2"; shift 2 ;;
    --nodes) NODES="$2"; shift 2 ;;
    --partition) PARTITION="$2"; shift 2 ;;
    --account) ACCOUNT="$2"; shift 2 ;;
    --qos) QOS="$2"; shift 2 ;;
    --time) TIME="$2"; shift 2 ;;
    --constraint) CONSTRAINT="$2"; shift 2 ;;
    --gres) GRES="$2"; shift 2 ;;
    --cpus-per-task) CPUS_PER_TASK="$2"; shift 2 ;;
    --job-name) JOB_NAME="$2"; shift 2 ;;
    --nodelist|-w) NODELIST="$2"; shift 2 ;;
    --exclude|-x) EXCLUDE="$2"; shift 2 ;;
    --extra-srun-args) EXTRA_SRUN_ARGS="$2"; shift 2 ;;
    --standalone-allocation) STANDALONE_ALLOC="1"; shift ;;
    --no-standalone-allocation) STANDALONE_ALLOC="0"; shift ;;
    # --- load flags ---
    --image-tar) IMAGE_TAR="$2"; shift 2 ;;
    --expected-tag) EXPECTED_TAG="$2"; shift 2 ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --skip-if-present) SKIP_IF_PRESENT="1"; shift ;;
    --log-dir) LOG_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1 (try --help)" ;;
  esac
done

# --- Pre-flight on submission host -----------------------------------------
# Resolve to absolute so the docker-load step on the compute node sees the
# same path no matter what its cwd is.
case "${IMAGE_TAR}" in
  /*) ;;
  *)  IMAGE_TAR="${REPO_ROOT}/${IMAGE_TAR}" ;;
esac

if [[ ! -f "${IMAGE_TAR}" ]]; then
  cat >&2 <<EOF
[slurm_load_image] ERROR: image tar not found: ${IMAGE_TAR}

  Build the image and save it to that path first, e.g.:

    bash scripts/slurm_build_image.sh \\
      --nodelist useocpm2m-097-132 \\
      --save-image ${IMAGE_TAR#${REPO_ROOT}/}

  (or pass --image-tar PATH to point this script at an existing tar.)

  After the tar exists, re-run:
    bash scripts/slurm_load_image.sh ${NODELIST:+--nodelist '${NODELIST}'} --nodes ${NODES}
EOF
  exit 1
fi

TAR_BYTES="$(stat -c '%s' "${IMAGE_TAR}" 2>/dev/null || echo 0)"
TAR_HUMAN="$(numfmt --to=iec --suffix=B "${TAR_BYTES}" 2>/dev/null || echo "${TAR_BYTES}B")"
log "image tar:      ${IMAGE_TAR} (${TAR_HUMAN})"
log "expected tag:   ${EXPECTED_TAG}"
log "docker bin:     ${DOCKER_BIN}"
log "target nodes:   ${NODELIST:-<srun-default>} (--nodes=${NODES})"

# Default log dir on the submission host's shared FS so the srun output
# is captured even if the compute node disappears later.
TS="$(date +%Y%m%d_%H%M%S)"
if [[ -z "${LOG_DIR}" ]]; then
  LOG_DIR="${REPO_ROOT}/results/load_image"
fi
LOG_FILE="${LOG_DIR}/load_${TS}.log"
if [[ "${DRY_RUN}" != "1" ]]; then
  mkdir -p "${LOG_DIR}"
fi

# --- Body run on each compute node -----------------------------------------
# Each task: optional skip-if-present check, docker load -i <tar>, then list
# the resulting image so the slurm log shows we ended up with the right tag.
# Heredoc-with-EOF interpolates ${VAR}s from THIS shell — exactly what we want
# (DOCKER_BIN / IMAGE_TAR / EXPECTED_TAG / SKIP_IF_PRESENT are all set here).
read -r -d '' BODY <<EOF || true
set -euo pipefail
host="\$(hostname -s)"

if ! command -v ${DOCKER_BIN} >/dev/null 2>&1; then
  echo "[\${host}] ERROR: ${DOCKER_BIN} not on PATH" >&2
  exit 127
fi

if [[ ! -f ${IMAGE_TAR@Q} ]]; then
  echo "[\${host}] ERROR: image tar not visible from this node: ${IMAGE_TAR}" >&2
  echo "[\${host}]   (is the path on a shared FS that all bench nodes can read?)" >&2
  exit 1
fi

if [[ "${SKIP_IF_PRESENT}" == "1" ]]; then
  if ${DOCKER_BIN} image inspect ${EXPECTED_TAG@Q} >/dev/null 2>&1; then
    echo "[\${host}] ${EXPECTED_TAG} already present; skipping docker load"
    ${DOCKER_BIN} image inspect --format '[\${host}]   {{.Id}} ({{.RepoTags}}) size={{.Size}}' ${EXPECTED_TAG@Q} || true
    exit 0
  fi
fi

echo "[\${host}] start=\$(date -Is) loading ${IMAGE_TAR}"
t0=\$(date +%s)
${DOCKER_BIN} load -i ${IMAGE_TAR@Q}
t1=\$(date +%s)
echo "[\${host}] docker load done in \$((t1 - t0))s"

if ! ${DOCKER_BIN} image inspect ${EXPECTED_TAG@Q} >/dev/null 2>&1; then
  echo "[\${host}] ERROR: load succeeded but ${EXPECTED_TAG} not found in daemon" >&2
  echo "[\${host}]   (tar may contain a different tag; pass --expected-tag to override)" >&2
  ${DOCKER_BIN} images >&2 || true
  exit 1
fi
${DOCKER_BIN} image inspect --format '[\${host}]   {{.Id}} ({{.RepoTags}}) size={{.Size}}' ${EXPECTED_TAG@Q} || true
echo "[\${host}] done=\$(date -Is)"
EOF

# --- srun wrapper ---------------------------------------------------------
# Same standalone-allocation pattern as slurm_build_image.sh / slurm_bench_p2p.sh:
# strip SLURM_*/SBATCH_* defaults so a wrapping salloc/sbatch with different
# node-count constraints (e.g. 2-node bench) doesn't bleed into this 1-task-
# per-node load step and trigger "can't run N processes on M nodes" warnings.
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
    -u SBATCH_GRES -u SBATCH_CPUS_PER_TASK
    -u SBATCH_JOB_NAME -u SBATCH_TIMELIMIT
  )
fi
SRUN+=(
  "${SRUN_BIN}"
  "--nodes=${NODES}"
  "--ntasks=${NODES}"
  "--ntasks-per-node=1"
  # Don't kill peers if one node fails — partial loads are still useful info,
  # and we'd rather see all per-node errors than the first one.
  "--kill-on-bad-exit=0"
  "--export=ALL"
)
[[ -n "${PARTITION}" ]]    && SRUN+=("--partition=${PARTITION}")
[[ -n "${ACCOUNT}" ]]      && SRUN+=("--account=${ACCOUNT}")
[[ -n "${QOS}" ]]          && SRUN+=("--qos=${QOS}")
[[ -n "${TIME}" ]]         && SRUN+=("--time=${TIME}")
[[ -n "${CONSTRAINT}" ]]   && SRUN+=("--constraint=${CONSTRAINT}")
[[ -n "${GRES}" ]]         && SRUN+=("--gres=${GRES}")
[[ -n "${CPUS_PER_TASK}" ]] && SRUN+=("--cpus-per-task=${CPUS_PER_TASK}")
[[ -n "${JOB_NAME}" ]]     && SRUN+=("--job-name=${JOB_NAME}")
[[ -n "${NODELIST}" ]]     && SRUN+=("--nodelist=${NODELIST}")
[[ -n "${EXCLUDE}" ]]      && SRUN+=("--exclude=${EXCLUDE}")

if [[ -n "${EXTRA_SRUN_ARGS}" ]]; then
  mapfile -t _EXTRA < <(python3 -c 'import shlex,sys; print("\n".join(shlex.split(sys.argv[1])))' "${EXTRA_SRUN_ARGS}")
  SRUN+=("${_EXTRA[@]}")
fi

SRUN+=(bash -lc "${BODY}")

log "log file:  ${LOG_FILE}"
log "srun command:"
printf '  %q' "${SRUN[@]}" >&2
printf '\n' >&2

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

# tee srun output while preserving its exit status (default `cmd | tee`
# clobbers $?). Process substitution keeps the parent shell seeing srun's rc.
set +e
"${SRUN[@]}" > >(tee "${LOG_FILE}") 2>&1
SRUN_RC=$?
set -e

log "srun exited with code ${SRUN_RC}"
log "full log: ${LOG_FILE}"
exit ${SRUN_RC}
