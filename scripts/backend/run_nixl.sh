#!/usr/bin/env bash
# Run nixlbench (NIXL) for a single rank.
#
# Mirrors run_nixlbench in scripts/bench_p2p_compare.py: only ranks 0/1
# participate; rank 0 optionally launches a local etcd for handshake;
# rank 1 sleeps --pair-startup-seconds before launching to let etcd come
# up; both iterate over the size sweep and invoke nixlbench once per size.
#
# Sizes must be passed as a comma-separated list of integer bytes.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_nixl.sh [options]

Required:
  --rank N
  --world-size N
  --master-addr ADDR
  --master-port PORT
  --output-dir DIR
  --source-root DIR
  --sizes CSV             Integer-byte sizes
  --iters N
  --batch-size N          Forwarded to nixlbench as both
                          --start_batch_size and --max_batch_size (and used in
                          --total_buffer_size sizing).
                          --num-blocks accepted as deprecated alias.
  --op-type read|write
  --device cpu|gpu

Optional:
  --local-rank N                   (forwarded for parity)
  --ib-hca SPEC                    (forwarded for parity; nixl uses --nixl-device-list)
  --nixlbench-bin PATH             Default: nixlbench (PATH lookup)
  --nixl-backend NAME              Default: LIBFABRIC
  --nixl-etcd-endpoints URL        Default: http://<master>:2379
  --nixl-start-etcd 0|1            Default: 1 (rank 0 starts a local etcd)
  --nixl-device-list LIST          Forwarded as --device_list
  --nixl-seg-type DRAM|VRAM|auto   Default: auto (gpu->VRAM, cpu->DRAM)
  --pair-startup-seconds N         Sleep before rank-1 invokes nixlbench
  -h, --help                       Show this help
EOF
}

RANK=""
WORLD_SIZE=""
LOCAL_RANK="0"
MASTER_ADDR=""
MASTER_PORT="29500"
OUTPUT_DIR=""
SOURCE_ROOT=""
SIZES=""
ITERS=""
BATCH_SIZE="1"
OP_TYPE="write"
DEVICE="gpu"
IB_HCA=""
NIXLBENCH_BIN="nixlbench"
NIXL_BACKEND="LIBFABRIC"
NIXL_ETCD_ENDPOINTS=""
NIXL_START_ETCD="1"
NIXL_DEVICE_LIST=""
NIXL_SEG_TYPE="auto"
PAIR_STARTUP_SECONDS="2"

die() { printf '[run_nixl] ERROR: %s\n' "$*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rank) RANK="$2"; shift 2 ;;
    --world-size) WORLD_SIZE="$2"; shift 2 ;;
    --local-rank) LOCAL_RANK="$2"; shift 2 ;;
    --master-addr) MASTER_ADDR="$2"; shift 2 ;;
    --master-port) MASTER_PORT="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --source-root) SOURCE_ROOT="$2"; shift 2 ;;
    --sizes) SIZES="$2"; shift 2 ;;
    --iters) ITERS="$2"; shift 2 ;;
    --batch-size|--num-blocks) BATCH_SIZE="$2"; shift 2 ;;
    --op-type) OP_TYPE="$2"; shift 2 ;;
    --device) DEVICE="$2"; shift 2 ;;
    --ib-hca) IB_HCA="$2"; shift 2 ;;
    --nixlbench-bin) NIXLBENCH_BIN="$2"; shift 2 ;;
    --nixl-backend) NIXL_BACKEND="$2"; shift 2 ;;
    --nixl-etcd-endpoints) NIXL_ETCD_ENDPOINTS="$2"; shift 2 ;;
    --nixl-start-etcd) NIXL_START_ETCD="$2"; shift 2 ;;
    --nixl-device-list) NIXL_DEVICE_LIST="$2"; shift 2 ;;
    --nixl-seg-type) NIXL_SEG_TYPE="$2"; shift 2 ;;
    --pair-startup-seconds) PAIR_STARTUP_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done

[[ -n "$RANK" ]]        || die "--rank required"
[[ -n "$MASTER_ADDR" ]] || die "--master-addr required"
[[ -n "$OUTPUT_DIR" ]]  || die "--output-dir required"
[[ -n "$SOURCE_ROOT" ]] || die "--source-root required"
[[ -n "$SIZES" ]]       || die "--sizes required"
[[ -n "$ITERS" ]]       || die "--iters required"

resolve_nixlbench_bin() {
  if command -v "$NIXLBENCH_BIN" >/dev/null 2>&1; then
    command -v "$NIXLBENCH_BIN"
    return
  fi
  for cand in \
    "${SOURCE_ROOT}/nixl/benchmark/nixlbench/build/nixlbench" \
    "${SOURCE_ROOT}/nixl/benchmark/nixlbench/build/src/nixlbench"; do
    if [[ -x "$cand" ]]; then echo "$cand"; return; fi
  done
  die "nixlbench binary not found: $NIXLBENCH_BIN"
}

resolve_seg_type() {
  if [[ "$NIXL_SEG_TYPE" != "auto" ]]; then
    echo "$NIXL_SEG_TYPE"
    return
  fi
  if [[ "$DEVICE" == "gpu" ]]; then echo "VRAM"; else echo "DRAM"; fi
}

LOG_PATH="${OUTPUT_DIR}/logs/nixl_rank${RANK}.log"
mkdir -p "$(dirname "$LOG_PATH")"

# nixlbench requires ETCD coordination. Default behavior matches the Python
# version: rank 0 launches a transient local etcd unless --nixl-start-etcd 0
# is explicitly passed (in which case --nixl-etcd-endpoints must be set).
ETCD_PID=""
ETCD_DATA_DIR=""
start_etcd_if_needed() {
  if [[ "$NIXL_START_ETCD" != "1" ]] || [[ "$RANK" -ne 0 ]]; then
    return 0
  fi
  if ! command -v etcd >/dev/null 2>&1; then
    die "nixlbench requires ETCD coordination but 'etcd' is not on PATH; install it in the runtime container or pass --nixl-start-etcd 0 with --nixl-etcd-endpoints"
  fi
  ETCD_DATA_DIR="/tmp/benchp2p-etcd-$$"
  local -a etcd_cmd=(
    etcd
    --data-dir "$ETCD_DATA_DIR"
    --listen-client-urls "http://0.0.0.0:2379"
    --advertise-client-urls "http://${MASTER_ADDR}:2379"
    --listen-peer-urls "http://0.0.0.0:2380"
    --initial-advertise-peer-urls "http://${MASTER_ADDR}:2380"
    --initial-cluster "default=http://${MASTER_ADDR}:2380"
    --log-level error
  )
  printf '+ '
  printf '%q ' "${etcd_cmd[@]}"
  printf '\n'
  "${etcd_cmd[@]}" &
  ETCD_PID=$!
  sleep 2
}

stop_etcd() {
  [[ -z "$ETCD_PID" ]] && return 0
  if kill -0 "$ETCD_PID" 2>/dev/null; then
    kill -TERM "$ETCD_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$ETCD_PID" 2>/dev/null || break
      sleep 1
    done
    kill -KILL "$ETCD_PID" 2>/dev/null || true
  fi
  wait "$ETCD_PID" 2>/dev/null || true
  ETCD_PID=""
  [[ -n "$ETCD_DATA_DIR" ]] && rm -rf "$ETCD_DATA_DIR"
}
trap stop_etcd EXIT

main() {
  if [[ "$RANK" -gt 1 ]]; then
    echo "[nixl] skipping unused rank ${RANK}"
    return 0
  fi
  if [[ "$RANK" -eq 1 ]] && awk "BEGIN { exit !(${PAIR_STARTUP_SECONDS} > 0) }"; then
    sleep "$PAIR_STARTUP_SECONDS"
  fi

  local binary endpoints seg_type
  binary="$(resolve_nixlbench_bin)"
  endpoints="${NIXL_ETCD_ENDPOINTS:-http://${MASTER_ADDR}:2379}"
  seg_type="$(resolve_seg_type)"

  start_etcd_if_needed

  local IFS=','
  read -ra size_list <<< "$SIZES"
  unset IFS

  local size total_buffer
  local op_upper
  op_upper="$(echo "$OP_TYPE" | tr '[:lower:]' '[:upper:]')"

  for size_raw in "${size_list[@]}"; do
    size="${size_raw// /}"
    [[ -z "$size" ]] && continue

    # total_buffer_size = max(size * batch_size * 4, 1 GiB)
    total_buffer=$(( size * BATCH_SIZE * 4 ))
    if [[ "$total_buffer" -lt $((1024 * 1024 * 1024)) ]]; then
      total_buffer=$((1024 * 1024 * 1024))
    fi

    local warmup_iter=$ITERS
    [[ "$warmup_iter" -lt 1 ]]   && warmup_iter=1
    [[ "$warmup_iter" -gt 100 ]] && warmup_iter=100

    local -a CMD=(
      "$binary"
      --etcd_endpoints "$endpoints"
      --backend "$NIXL_BACKEND"
      --initiator_seg_type "$seg_type"
      --target_seg_type "$seg_type"
      --scheme pairwise
      --op_type "$op_upper"
      --total_buffer_size "$total_buffer"
      --start_block_size "$size"
      --max_block_size "$size"
      --start_batch_size "$BATCH_SIZE"
      --max_batch_size "$BATCH_SIZE"
      --num_iter "$ITERS"
      --warmup_iter "$warmup_iter"
      --num_threads 1
      --num_initiator_dev 1
      --num_target_dev 1
    )
    [[ -n "$NIXL_DEVICE_LIST" ]] && CMD+=(--device_list "$NIXL_DEVICE_LIST")

    printf '$ '
    printf '%q ' "${CMD[@]}"
    printf '\n\n'

    ( cd "$SOURCE_ROOT" && "${CMD[@]}" )
  done
}

main 2>&1 | tee "$LOG_PATH"
RC=${PIPESTATUS[0]}
stop_etcd
exit "$RC"
