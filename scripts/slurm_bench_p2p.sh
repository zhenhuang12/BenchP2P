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
#     --cpus-per-task / --job-name /
#     --nodelist / --exclude
#   --master-port N                  Distributed init port (default: 29500)
#   --extra-srun-args STR            Appended to srun, parsed with shlex
#   --skip-report                    Skip the post-srun report step
#   --report-only                    Only run the report step on the submission host
#   --standalone-allocation          Strip SLURM_* env so srun creates a NEW
#                                    allocation instead of inheriting the
#                                    surrounding salloc/sbatch one
#   --prune-containers               (default) Before the main srun, srun once
#                                    per target node and `docker rm -f` EVERY
#                                    container on the node, so leftover
#                                    containers from a cancelled job don't
#                                    hold MASTER_PORT / RDMA QPs / GPUs / MRs
#                                    and trigger EADDRINUSE on the next
#                                    torch.distributed init.
#   --no-prune-containers            Skip the prune step entirely.
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
NODELIST="useocpm2m-097-[132,135]"
EXCLUDE=""
EXTRA_SRUN_ARGS=""
SKIP_REPORT="0"
REPORT_ONLY="0"
STANDALONE_ALLOC="1"
PRUNE_CONTAINERS="1"

# IMAGE="docker.io/rocm/primus:v26.2"
IMAGE=docker.io/library/benchp2p:latest
DOCKER_BIN="docker"
CONTAINER_PYTHON="python3"
PULL="0"
MOUNT_HOME="0"
EXTRA_MOUNTS=()
EXTRA_DOCKER_ARGS=""

OUTPUT_DIR=""
DRY_RUN="0"
INNER_ARGS=()

# Install-mode controls how the runtime image acquires the BenchP2P backend
# stacks before each per-rank `bench_p2p_compare.py run`:
#   auto       (default) probe the image once on the submission host; if all
#              requested backends are importable / their native binary is on
#              PATH, append --skip-wheel-install so the per-rank harness
#              short-circuits the pip step. If anything is missing, invoke
#              container_build_wheel.sh once to compile + stage wheels into
#              3rdparty/wheelhouse/ before srun, so the regular install path
#              picks them up.
#   skip       Trust the image; just append --skip-wheel-install.
#   wheelhouse Don't probe, don't build; rely on whatever is already in
#              3rdparty/wheelhouse/ (legacy behaviour before this flag).
#   build      Don't probe; force container_build_wheel.sh to (re)compile the
#              wheelhouse, then proceed with the normal install.
INSTALL_MODE="auto"

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
    --cpus-per-task / --job-name /
    --nodelist / --exclude
  --master-port N                  Distributed init port (default: 29500)
  --extra-srun-args STR            Appended to srun, parsed with shlex
  --skip-report                    Skip the post-srun report step
  --report-only                    Only run the report step on the submission host
  --standalone-allocation          Strip SLURM_* env so srun creates a NEW
                                   allocation instead of inheriting the
                                   surrounding salloc/sbatch one
  --prune-containers               (default) Before main srun, srun once per
                                   target node and `docker rm -f` EVERY
                                   container on the node, so a leftover
                                   container from a cancelled job doesn't
                                   hold MASTER_PORT / RDMA QPs / GPUs (the
                                   EADDRINUSE trap on torch.distributed init).
  --no-prune-containers            Skip the prune step entirely.

Container options (forwarded to container_bench_p2p.sh):
  --image IMAGE                    Runtime image (default: docker.io/rocm/primus:v26.2)
  --docker-bin BIN                 Docker CLI (default: docker)
  --container-python BIN           Python in container (default: python3)
  --pull / --mount-home            Forwarded to container_bench_p2p.sh
  --extra-mount SRC:DST            Extra bind mount (repeatable)
  --extra-docker-args STR          Extra docker run args

Common options:
  --install-mode MODE              How to acquire backend stacks in the image:
                                     auto       (default) probe image; if all
                                                requested backends already
                                                installed -> add
                                                --skip-wheel-install. Otherwise
                                                run container_build_wheel.sh
                                                once before srun to compile +
                                                stage wheels.
                                     skip       Trust the image; always add
                                                --skip-wheel-install.
                                     wheelhouse Don't probe / don't build; use
                                                whatever is in 3rdparty/wheelhouse/
                                                already (legacy behaviour).
                                     build      Always (re)compile in container,
                                                then install from wheelhouse.
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
    --nodelist|-w) NODELIST="$2"; shift 2 ;;
    --exclude|-x) EXCLUDE="$2"; shift 2 ;;
    --extra-srun-args) EXTRA_SRUN_ARGS="$2"; shift 2 ;;
    --skip-report) SKIP_REPORT="1"; shift ;;
    --report-only) REPORT_ONLY="1"; shift ;;
    --standalone-allocation) STANDALONE_ALLOC="1"; shift ;;
    --prune-containers) PRUNE_CONTAINERS="1"; shift ;;
    --no-prune-containers) PRUNE_CONTAINERS="0"; shift ;;
    # --- container flags ---
    --image) IMAGE="$2"; shift 2 ;;
    --docker-bin) DOCKER_BIN="$2"; shift 2 ;;
    --container-python) CONTAINER_PYTHON="$2"; shift 2 ;;
    --pull) PULL="1"; shift ;;
    --mount-home) MOUNT_HOME="1"; shift ;;
    --extra-mount) EXTRA_MOUNTS+=("$2"); shift 2 ;;
    --extra-docker-args) EXTRA_DOCKER_ARGS="$2"; shift 2 ;;
    # --- common ---
    --install-mode) INSTALL_MODE="$2"; shift 2 ;;
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

# --- Helpers shared by prune step and main srun ---------------------------- #
# When --standalone-allocation is on, prepend `env -u SLURM_*` so srun does
# not inherit the surrounding salloc/sbatch allocation and is forced to
# request a fresh one matching --nodes / --ntasks. The list mirrors the
# SLURM_* vars that srun consults to decide "am I inside an allocation?".
# Defined once here so both prune_lingering_containers() and the main srun
# wrapper can reuse it verbatim (otherwise prune would happily attach to the
# parent salloc and confuse Slurm's bookkeeping).
STANDALONE_ENV_PREFIX=()
if [[ "${STANDALONE_ALLOC}" == "1" ]]; then
  STANDALONE_ENV_PREFIX=(
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

# srun once per target node (--ntasks=NODES, --ntasks-per-node=1) and
# `docker rm -f` every container on that node. This is the fix-it-with-a-hammer
# for the recurring class of failures where a previously cancelled job (or its
# `srun --kill-on-bad-exit=1` pre-emption) left a container running on the
# node, holding:
#   - MASTER_PORT (29500/...) -> next torch.distributed init -> EADDRINUSE
#   - RDMA QPs / MR registrations -> next benchmark -> CQE errors / hangs
#   - GPU device(s) -> next docker --gpus all -> "Failed to initialize NVML"
#
# Image-agnostic: a container from any image (even an unrelated build tool
# that crashed mid-test) can hold an RDMA QP / GPU handle and break the next
# run, so we wipe everything on the node. The prune is best-effort: a
# non-zero exit (e.g. node went down between allocation and prune step) is
# logged and execution continues.
prune_lingering_containers() {
  if [[ "${PRUNE_CONTAINERS}" != "1" ]]; then
    echo "[slurm_bench_p2p] container prune skipped (--no-prune-containers)"
    return 0
  fi

  # Inline shell snippet kept single-quoted so $-vars expand on the compute
  # node, not in this shell; ${DOCKER_BIN} is substituted via printf -v.
  local prune_script
  printf -v prune_script '%s' '
host=$(hostname -s)
ids=$(__DOCKER__ ps -aq 2>/dev/null || true)
if [ -n "$ids" ]; then
  echo "[$host] prune: removing $(echo $ids | wc -w) container(s)"
  __DOCKER__ rm -f $ids >/dev/null 2>&1 || true
else
  echo "[$host] prune: no containers"
fi
'
  prune_script="${prune_script//__DOCKER__/${DOCKER_BIN}}"

  local prune_cmd=()
  if [[ ${#STANDALONE_ENV_PREFIX[@]} -gt 0 ]]; then
    prune_cmd+=("${STANDALONE_ENV_PREFIX[@]}")
  fi
  prune_cmd+=(
    "${SRUN_BIN}"
    "--nodes=${NODES}"
    "--ntasks=${NODES}"
    "--ntasks-per-node=1"
    "--kill-on-bad-exit=0"
    "--export=ALL"
    "--job-name=${JOB_NAME}-prune"
  )
  [[ -n "${PARTITION}" ]] && prune_cmd+=("--partition=${PARTITION}")
  [[ -n "${ACCOUNT}" ]] && prune_cmd+=("--account=${ACCOUNT}")
  [[ -n "${QOS}" ]] && prune_cmd+=("--qos=${QOS}")
  [[ -n "${TIME}" ]] && prune_cmd+=("--time=${TIME}")
  [[ -n "${CONSTRAINT}" ]] && prune_cmd+=("--constraint=${CONSTRAINT}")
  # Prune doesn't need a GPU but we mirror --gres so partitions that gate on
  # gres availability still schedule us onto the same nodes the main srun
  # will land on (otherwise prune may sit in PD while the actual benchmark
  # is the thing waiting to start).
  [[ -n "${GRES}" ]] && prune_cmd+=("--gres=${GRES}")
  [[ -n "${NODELIST}" ]] && prune_cmd+=("--nodelist=${NODELIST}")
  [[ -n "${EXCLUDE}" ]] && prune_cmd+=("--exclude=${EXCLUDE}")
  # Use `bash -lc` (login shell) to mirror the main bench srun: HPC clusters
  # often ship docker via /etc/profile.d/*.sh or `module load docker`, so
  # plain `bash -c` would not have docker on PATH and prune would silently
  # bail out of the `command -v docker` check above. Aligning shells ensures
  # prune sees the same docker that bench will use.
  prune_cmd+=(bash -lc "${prune_script}")

  echo "[slurm_bench_p2p] prune all containers on target nodes:"
  printf '  %q' "${prune_cmd[@]}"; printf '\n'
  if [[ "${DRY_RUN}" == "1" ]]; then
    return 0
  fi
  if ! "${prune_cmd[@]}"; then
    echo "[slurm_bench_p2p] WARNING: container prune srun returned non-zero; continuing anyway" >&2
  fi
}

# --- Install pre-flight ---------------------------------------------------- #
# Decide whether each per-rank container should pip-install from the wheelhouse
# or short-circuit because the image already ships the backend packages.
# When the image is missing them, build the wheelhouse once on the submission
# host (one docker run, not one per srun task) so all ranks share it.

# Mirror bench_p2p_compare.py's DEFAULT_BACKENDS unless the user passed
# --backends in INNER_ARGS.
REQUESTED_BACKENDS="mori,mooncake,uccl,nixl"
for ((i=0; i<${#INNER_ARGS[@]}; i++)); do
  if [[ "${INNER_ARGS[i]}" == "--backends" ]]; then
    REQUESTED_BACKENDS="${INNER_ARGS[i+1]:-${REQUESTED_BACKENDS}}"
    break
  fi
done

ensure_skip_wheel_install() {
  local arg
  for arg in "${INNER_ARGS[@]}"; do
    [[ "${arg}" == "--skip-wheel-install" ]] && return 0
  done
  INNER_ARGS+=("--skip-wheel-install")
}

# Probe the runtime image for each requested backend. Stdout = comma list of
# missing backends (empty if all present); exit code 0 = all installed,
# 1 = something missing.
probe_image_packages() {
  local backends_csv="$1"
  local _b backends_args=()
  local _saved_ifs="$IFS"
  IFS=','
  for _b in ${backends_csv}; do
    [[ -n "${_b}" ]] && backends_args+=("${_b}")
  done
  IFS="${_saved_ifs}"
  local probe_py='import importlib, shutil, sys
PYMOD = {"mori": "mori", "uccl": "uccl"}
NATIVE_BIN = {"mooncake": "transfer_engine_bench", "nixl": "nixlbench"}
missing = []
for b in sys.argv[1:]:
    mod = PYMOD.get(b)
    bin_ = NATIVE_BIN.get(b)
    is_missing = False
    if mod is not None:
        try:
            importlib.import_module(mod)
        except Exception as exc:
            print(f"[probe] {b}: import {mod} -> {exc.__class__.__name__}: {exc}", file=sys.stderr)
            is_missing = True
    if bin_ is not None and shutil.which(bin_) is None:
        print(f"[probe] {b}: native binary {bin_} not on PATH", file=sys.stderr)
        is_missing = True
    if mod is None and bin_ is None:
        print(f"[probe] {b}: no probe definition; treating as missing", file=sys.stderr)
        is_missing = True
    if is_missing:
        missing.append(b)
print(",".join(missing))
sys.exit(1 if missing else 0)
'
  "${DOCKER_BIN}" run --rm \
    --entrypoint "" \
    "${IMAGE}" \
    "${CONTAINER_PYTHON}" -c "${probe_py}" "${backends_args[@]}"
}

# One-shot in-container build: hands off to scripts/container_build_wheel.sh,
# which mounts the repo and runs build_wheel.sh inside the runtime image.
build_wheelhouse_in_container() {
  local backends_csv="$1"
  local build_cmd=(
    bash "${SCRIPT_DIR}/container_build_wheel.sh"
    --image "${IMAGE}"
    --docker-bin "${DOCKER_BIN}"
    --container-python "${CONTAINER_PYTHON}"
  )
  [[ "${PULL}" == "1" ]] && build_cmd+=(--pull)
  [[ "${MOUNT_HOME}" == "1" ]] && build_cmd+=(--mount-home)
  local m
  for m in "${EXTRA_MOUNTS[@]}"; do build_cmd+=(--extra-mount "${m}"); done
  [[ -n "${EXTRA_DOCKER_ARGS}" ]] && build_cmd+=(--extra-docker-args "${EXTRA_DOCKER_ARGS}")
  [[ "${DRY_RUN}" == "1" ]] && build_cmd+=(--dry-run)
  build_cmd+=(-- --backends "${backends_csv}")
  echo "[slurm_bench_p2p] in-container wheel build:"
  printf '  %q' "${build_cmd[@]}"; printf '\n'
  "${build_cmd[@]}"
}

case "${INSTALL_MODE}" in
  skip)
    echo "[slurm_bench_p2p] install-mode=skip -> --skip-wheel-install"
    ensure_skip_wheel_install
    ;;
  wheelhouse)
    echo "[slurm_bench_p2p] install-mode=wheelhouse -> per-rank pip install from 3rdparty/wheelhouse/"
    ;;
  build)
    echo "[slurm_bench_p2p] install-mode=build -> rebuilding wheelhouse for ${REQUESTED_BACKENDS}"
    build_wheelhouse_in_container "${REQUESTED_BACKENDS}"
    ;;
  auto)
    if [[ "${DRY_RUN}" == "1" ]]; then
      echo "[slurm_bench_p2p] install-mode=auto, --dry-run: skipping image probe; would either"
      echo "                  add --skip-wheel-install (image ready) or run container_build_wheel.sh"
    elif ! command -v "${DOCKER_BIN}" >/dev/null 2>&1; then
      echo "[slurm_bench_p2p] install-mode=auto: ${DOCKER_BIN} not on PATH on submission host;"
      echo "                  cannot probe ${IMAGE}; falling back to install-mode=wheelhouse"
    elif ! "${DOCKER_BIN}" image inspect "${IMAGE}" >/dev/null 2>&1; then
      # Image isn't materialised on the submission host (common when each
      # compute node builds the image locally and there's no shared
      # registry). We can't probe it here without a pull; skip the probe
      # and trust the per-rank install path.
      echo "[slurm_bench_p2p] install-mode=auto: image ${IMAGE} not on submission host;"
      echo "                  cannot probe; falling back to install-mode=wheelhouse"
      echo "                  (use --install-mode skip if you know the image already has the backends,"
      echo "                   or --pull / --install-mode build to fetch / rebuild explicitly)"
    else
      echo "[slurm_bench_p2p] install-mode=auto: probing ${IMAGE} for backends ${REQUESTED_BACKENDS}"
      probe_out=""
      probe_rc=0
      probe_out="$(probe_image_packages "${REQUESTED_BACKENDS}")" || probe_rc=$?
      if [[ ${probe_rc} -eq 0 ]]; then
        echo "[slurm_bench_p2p] all requested backends already installed in image -> --skip-wheel-install"
        ensure_skip_wheel_install
      else
        missing="${probe_out}"
        [[ -z "${missing}" ]] && missing="${REQUESTED_BACKENDS}"
        echo "[slurm_bench_p2p] backends missing in image: ${missing}; building wheelhouse first"
        build_wheelhouse_in_container "${REQUESTED_BACKENDS}"
      fi
    fi
    ;;
  *)
    die "--install-mode must be auto|skip|wheelhouse|build (got: ${INSTALL_MODE})"
    ;;
esac

# Run the prune step AFTER install pre-flight: container_build_wheel.sh
# (install-mode=build / auto-with-missing-backends) does its own `docker run
# --rm`, so by the time we get here that build container has already exited
# and only true leftovers remain. Doing it before main srun keeps the next
# `docker run` from racing port 29500 / RDMA QPs / GPU handles with a zombie.
prune_lingering_containers

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

# Reuses STANDALONE_ENV_PREFIX defined in the helpers section so prune step
# and main srun strip the same SLURM_* vars (see comment near the array def).
SRUN=()
if [[ ${#STANDALONE_ENV_PREFIX[@]} -gt 0 ]]; then
  SRUN+=("${STANDALONE_ENV_PREFIX[@]}")
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
[[ -n "${NODELIST}" ]] && SRUN+=("--nodelist=${NODELIST}")
[[ -n "${EXCLUDE}" ]] && SRUN+=("--exclude=${EXCLUDE}")

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
