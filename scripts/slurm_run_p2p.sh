#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SOURCE_ROOT="${REPO_ROOT}/3rdparty"
WHEELHOUSE="${SOURCE_ROOT}/wheelhouse"
OUTPUT_DIR="${REPO_ROOT}/results/slurm_run"
CONTAINER_RUNNER="${SCRIPT_DIR}/container_run_p2p.py"

BACKENDS="mori,mooncake,uccl,nixl"
SIZES="256,1024,4096,16384,65536,262144,1048576,10485760,16777216,104857600"
ITERS="10"
NUM_BLOCKS="1"
DEVICE="gpu"
OP_TYPE="write"
CONTAINER_PYTHON="python3"
PAIR_STARTUP_SECONDS="2.0"

SRUN_BIN="srun"
SLURM_NODES="2"
SLURM_NTASKS="2"
SLURM_NTASKS_PER_NODE="1"
SLURM_MASTER_PORT="29500"
SLURM_JOB_NAME="benchp2p"
SLURM_PARTITION=""
SLURM_ACCOUNT=""
SLURM_QOS=""
SLURM_TIME=""
SLURM_CONSTRAINT=""
SLURM_GRES=""
SLURM_GPUS_PER_TASK=""
SLURM_CPUS_PER_TASK=""
SLURM_EXTRA_ARGS=""

SLURM_CONTAINER_RUNTIME="docker"
SLURM_CONTAINER_IMAGE="docker.io/rocm/primus:v26.2"
SLURM_CONTAINER_WORKDIR="${REPO_ROOT}"
SLURM_CONTAINER_MOUNTS=""

DOCKER_BIN="docker"
DOCKER_GPUS=""
DOCKER_PULL="0"
DOCKER_MOUNT_HOME="0"
DOCKER_EXTRA_ARGS=""

ASYNC_API="0"
SKIP_RUNTIME_WHEEL_INSTALL="0"
MORI_BACKEND="rdma"
MORI_TRANSFER_BATCH_SIZE="1"
MORI_XGMI_MULTIPROCESS="0"
NIXLBENCH_BIN="nixlbench"
NIXL_BACKEND="UCX"
NIXL_ETCD_ENDPOINTS=""
NIXL_START_ETCD="1"
NIXL_DEVICE_LIST=""
MOONCAKE_BENCH_BIN="tebench"
MOONCAKE_BACKEND="tent"
MOONCAKE_XPORT_TYPE="rdma"
MOONCAKE_DURATION="5"
UCCL_SCRIPT=""
NIXL_SCRIPT=""
MOONCAKE_SCRIPT=""
MORI_SCRIPT=""
DRY_RUN="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) REPO_ROOT="$2"; shift 2 ;;
    --source-root) SOURCE_ROOT="$2"; shift 2 ;;
    --wheelhouse) WHEELHOUSE="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --container-runner) CONTAINER_RUNNER="$2"; shift 2 ;;
    --backends) BACKENDS="$2"; shift 2 ;;
    --sizes) SIZES="$2"; shift 2 ;;
    --iters) ITERS="$2"; shift 2 ;;
    --num-blocks) NUM_BLOCKS="$2"; shift 2 ;;
    --device) DEVICE="$2"; shift 2 ;;
    --op-type) OP_TYPE="$2"; shift 2 ;;
    --container-python) CONTAINER_PYTHON="$2"; shift 2 ;;
    --pair-startup-seconds) PAIR_STARTUP_SECONDS="$2"; shift 2 ;;
    --srun) SRUN_BIN="$2"; shift 2 ;;
    --slurm-nodes) SLURM_NODES="$2"; shift 2 ;;
    --slurm-ntasks) SLURM_NTASKS="$2"; shift 2 ;;
    --slurm-ntasks-per-node) SLURM_NTASKS_PER_NODE="$2"; shift 2 ;;
    --slurm-master-port) SLURM_MASTER_PORT="$2"; shift 2 ;;
    --slurm-job-name) SLURM_JOB_NAME="$2"; shift 2 ;;
    --slurm-partition) SLURM_PARTITION="$2"; shift 2 ;;
    --slurm-account) SLURM_ACCOUNT="$2"; shift 2 ;;
    --slurm-qos) SLURM_QOS="$2"; shift 2 ;;
    --slurm-time) SLURM_TIME="$2"; shift 2 ;;
    --slurm-constraint) SLURM_CONSTRAINT="$2"; shift 2 ;;
    --slurm-gres) SLURM_GRES="$2"; shift 2 ;;
    --slurm-gpus-per-task) SLURM_GPUS_PER_TASK="$2"; shift 2 ;;
    --slurm-cpus-per-task) SLURM_CPUS_PER_TASK="$2"; shift 2 ;;
    --slurm-extra-args) SLURM_EXTRA_ARGS="$2"; shift 2 ;;
    --slurm-container-runtime) SLURM_CONTAINER_RUNTIME="$2"; shift 2 ;;
    --slurm-container-image) SLURM_CONTAINER_IMAGE="$2"; shift 2 ;;
    --slurm-container-workdir) SLURM_CONTAINER_WORKDIR="$2"; shift 2 ;;
    --slurm-container-mounts) SLURM_CONTAINER_MOUNTS="$2"; shift 2 ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --docker-gpus) DOCKER_GPUS="$2"; shift 2 ;;
    --docker-extra-args) DOCKER_EXTRA_ARGS="$2"; shift 2 ;;
    --docker-pull) DOCKER_PULL="1"; shift ;;
    --docker-mount-home) DOCKER_MOUNT_HOME="1"; shift ;;
    --async-api) ASYNC_API="1"; shift ;;
    --skip-runtime-wheel-install) SKIP_RUNTIME_WHEEL_INSTALL="1"; shift ;;
    --mori-backend) MORI_BACKEND="$2"; shift 2 ;;
    --mori-transfer-batch-size) MORI_TRANSFER_BATCH_SIZE="$2"; shift 2 ;;
    --mori-xgmi-multiprocess) MORI_XGMI_MULTIPROCESS="1"; shift ;;
    --nixlbench-bin) NIXLBENCH_BIN="$2"; shift 2 ;;
    --nixl-backend) NIXL_BACKEND="$2"; shift 2 ;;
    --nixl-etcd-endpoints) NIXL_ETCD_ENDPOINTS="$2"; shift 2 ;;
    --no-nixl-start-etcd) NIXL_START_ETCD="0"; shift ;;
    --nixl-device-list) NIXL_DEVICE_LIST="$2"; shift 2 ;;
    --mooncake-bench-bin) MOONCAKE_BENCH_BIN="$2"; shift 2 ;;
    --mooncake-backend) MOONCAKE_BACKEND="$2"; shift 2 ;;
    --mooncake-xport-type) MOONCAKE_XPORT_TYPE="$2"; shift 2 ;;
    --mooncake-duration) MOONCAKE_DURATION="$2"; shift 2 ;;
    --uccl-script) UCCL_SCRIPT="$2"; shift 2 ;;
    --nixl-script) NIXL_SCRIPT="$2"; shift 2 ;;
    --mooncake-script) MOONCAKE_SCRIPT="$2"; shift 2 ;;
    --mori-script) MORI_SCRIPT="$2"; shift 2 ;;
    --dry-run) DRY_RUN="1"; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"

RUNNER=(
  "${CONTAINER_PYTHON}" "${CONTAINER_RUNNER}"
  --backends "${BACKENDS}"
  --sizes "${SIZES}"
  --iters "${ITERS}"
  --num-blocks "${NUM_BLOCKS}"
  --device "${DEVICE}"
  --op-type "${OP_TYPE}"
  --pair-startup-seconds "${PAIR_STARTUP_SECONDS}"
  --source-root "${SOURCE_ROOT}"
  --wheelhouse "${WHEELHOUSE}"
  --mori-backend "${MORI_BACKEND}"
  --mori-transfer-batch-size "${MORI_TRANSFER_BATCH_SIZE}"
  --nixlbench-bin "${NIXLBENCH_BIN}"
  --nixl-backend "${NIXL_BACKEND}"
  --mooncake-bench-bin "${MOONCAKE_BENCH_BIN}"
  --mooncake-backend "${MOONCAKE_BACKEND}"
  --mooncake-xport-type "${MOONCAKE_XPORT_TYPE}"
  --mooncake-duration "${MOONCAKE_DURATION}"
)
[[ "${ASYNC_API}" == "1" ]] && RUNNER+=(--async-api)
[[ "${SKIP_RUNTIME_WHEEL_INSTALL}" == "1" ]] && RUNNER+=(--skip-runtime-wheel-install)
[[ "${MORI_XGMI_MULTIPROCESS}" == "1" ]] && RUNNER+=(--mori-xgmi-multiprocess)
[[ -n "${NIXL_ETCD_ENDPOINTS}" ]] && RUNNER+=(--nixl-etcd-endpoints "${NIXL_ETCD_ENDPOINTS}")
[[ "${NIXL_START_ETCD}" == "0" ]] && RUNNER+=(--no-nixl-start-etcd)
[[ -n "${NIXL_DEVICE_LIST}" ]] && RUNNER+=(--nixl-device-list "${NIXL_DEVICE_LIST}")
[[ -n "${UCCL_SCRIPT}" ]] && RUNNER+=(--uccl-script "${UCCL_SCRIPT}")
[[ -n "${NIXL_SCRIPT}" ]] && RUNNER+=(--nixl-script "${NIXL_SCRIPT}")
[[ -n "${MOONCAKE_SCRIPT}" ]] && RUNNER+=(--mooncake-script "${MOONCAKE_SCRIPT}")
[[ -n "${MORI_SCRIPT}" ]] && RUNNER+=(--mori-script "${MORI_SCRIPT}")

printf -v RUNNER_CMD "%q " "${RUNNER[@]}"

SRUN=(
  "${SRUN_BIN}"
  "--nodes=${SLURM_NODES}"
  "--ntasks=${SLURM_NTASKS}"
  "--ntasks-per-node=${SLURM_NTASKS_PER_NODE}"
  "--kill-on-bad-exit=1"
  "--export=ALL"
)
[[ -n "${SLURM_PARTITION}" ]] && SRUN+=("--partition=${SLURM_PARTITION}")
[[ -n "${SLURM_ACCOUNT}" ]] && SRUN+=("--account=${SLURM_ACCOUNT}")
[[ -n "${SLURM_QOS}" ]] && SRUN+=("--qos=${SLURM_QOS}")
[[ -n "${SLURM_TIME}" ]] && SRUN+=("--time=${SLURM_TIME}")
[[ -n "${SLURM_CONSTRAINT}" ]] && SRUN+=("--constraint=${SLURM_CONSTRAINT}")
[[ -n "${SLURM_GRES}" ]] && SRUN+=("--gres=${SLURM_GRES}")
[[ -n "${SLURM_GPUS_PER_TASK}" ]] && SRUN+=("--gpus-per-task=${SLURM_GPUS_PER_TASK}")
[[ -n "${SLURM_CPUS_PER_TASK}" ]] && SRUN+=("--cpus-per-task=${SLURM_CPUS_PER_TASK}")
[[ -n "${SLURM_JOB_NAME}" ]] && SRUN+=("--job-name=${SLURM_JOB_NAME}")

if [[ -n "${SLURM_EXTRA_ARGS}" ]]; then
  mapfile -t EXTRA < <(python3 -c 'import shlex, sys; print("\n".join(shlex.split(sys.argv[1])))' "${SLURM_EXTRA_ARGS}")
  SRUN+=("${EXTRA[@]}")
fi

if [[ "${SLURM_CONTAINER_RUNTIME}" == "pyxis" ]]; then
  SRUN+=("--container-image=${SLURM_CONTAINER_IMAGE}")
  SRUN+=("--container-workdir=${SLURM_CONTAINER_WORKDIR}")
fi

MOUNTS=("${REPO_ROOT}:${REPO_ROOT}" "${SOURCE_ROOT}:${SOURCE_ROOT}" "$(dirname "${WHEELHOUSE}"):$(dirname "${WHEELHOUSE}")")
if [[ -n "${SLURM_CONTAINER_MOUNTS}" ]]; then
  IFS=',' read -r -a EXTRA_MOUNTS <<< "${SLURM_CONTAINER_MOUNTS}"
  MOUNTS+=("${EXTRA_MOUNTS[@]}")
fi
MOUNT_CSV="$(IFS=,; echo "${MOUNTS[*]}")"
if [[ "${SLURM_CONTAINER_RUNTIME}" == "pyxis" ]]; then
  SRUN+=("--container-mounts=${MOUNT_CSV}")
fi

PREAMBLE='set -euo pipefail
SLURM_PROCID="${SLURM_PROCID:-0}"
SLURM_NTASKS="${SLURM_NTASKS:-1}"
SLURM_LOCALID="${SLURM_LOCALID:-0}"
MASTER_ADDR="${MASTER_ADDR:-$(scontrol show hostnames "${SLURM_JOB_NODELIST}" | sed -n '\''1p'\'')}"
MASTER_PORT="${MASTER_PORT:-'"${SLURM_MASTER_PORT}"'}"
export MASTER_ADDR MASTER_PORT
export RANK="${SLURM_PROCID}"
export WORLD_SIZE="${SLURM_NTASKS}"
export LOCAL_RANK="${SLURM_LOCALID}"
export LOCAL_WORLD_SIZE="${SLURM_NTASKS_PER_NODE:-1}"'

if [[ "${SLURM_CONTAINER_RUNTIME}" == "docker" ]]; then
  DOCKER=(
    "${DOCKER_BIN}" run --rm
    --ipc=host --network=host
    --device=/dev/kfd --device=/dev/dri --device=/dev/infiniband
    --cap-add=SYS_PTRACE --cap-add=CAP_SYS_ADMIN
    --security-opt seccomp=unconfined
    --group-add video
    --privileged
    --workdir "${SLURM_CONTAINER_WORKDIR}"
  )
  [[ -n "${DOCKER_GPUS}" ]] && DOCKER+=(--gpus "${DOCKER_GPUS}")
  [[ "${DOCKER_PULL}" == "1" ]] && DOCKER+=(--pull=always)
  for mount in "${MOUNTS[@]}"; do
    DOCKER+=(-v "${mount}")
  done
  [[ "${DOCKER_MOUNT_HOME}" == "1" ]] && DOCKER+=(-v "${HOME}:/root/home")
  if [[ -n "${DOCKER_EXTRA_ARGS}" ]]; then
    mapfile -t EXTRA_DOCKER < <(python3 -c 'import shlex, sys; print("\n".join(shlex.split(sys.argv[1])))' "${DOCKER_EXTRA_ARGS}")
    DOCKER+=("${EXTRA_DOCKER[@]}")
  fi
  DOCKER+=("${SLURM_CONTAINER_IMAGE}" bash -lc "${PREAMBLE}"$'\n'"exec ${RUNNER_CMD}")
  printf -v DOCKER_CMD "%q " "${DOCKER[@]}"
  BODY="${PREAMBLE}"$'\n'"exec ${DOCKER_CMD}"
else
  BODY="${PREAMBLE}"$'\n'"exec ${RUNNER_CMD}"
fi

SRUN+=(bash -lc "${BODY}")

echo "BenchP2P Slurm command:"
printf ' %q' "${SRUN[@]}"
echo

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

exec "${SRUN[@]}"
