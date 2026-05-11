#!/usr/bin/env bash
# Run Mooncake transfer_engine_bench for a single rank.
#
# Mirrors run_mooncake_tebench in scripts/bench_p2p_compare.py:
# - rank 0 (target):    starts `transfer_engine_bench --mode=target ...`,
#                       parses the "Transfer Engine RPC ... listening on
#                       HOST:PORT" startup line off its stdout, then
#                       publishes "<MASTER_ADDR>:<port>" into a per-size
#                       segment file on the shared FS for the initiator.
# - rank 1 (initiator): waits for the segment file (defeating NFS attribute
#                       caching by listdir-poll), then runs
#                       `transfer_engine_bench --mode=initiator
#                       --segment_id=...`.
# - rank > 1:           skipped (mooncake is a 1-pair benchmark).
#
# Sizes must be passed as a comma-separated list of integer bytes.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_mooncake.sh [options]

Required:
  --rank N
  --local-rank N
  --master-addr ADDR
  --output-dir DIR
  --source-root DIR
  --sizes CSV             Integer-byte sizes
  --batch-size N          Forwarded to transfer_engine_bench as --batch_size
                          (--num-blocks accepted as deprecated alias).
  --device cpu|gpu        cpu -> --use_vram=false (DRAM)
                          gpu -> --use_vram=true --gpu_id=$LOCAL_RANK
                                 (GPUDirect RDMA, requires the wheel to
                                 be built with -DUSE_HIP=ON / USE_CUDA)
  --op-type read|write

Optional:
  --world-size N                       (forwarded for parity)
  --master-port PORT                   (forwarded for parity)
  --iters N                            (forwarded for parity; mooncake is duration-driven)
  --ib-hca SPEC                        NCCL-style HCA selector. Selects the
                                       matching mooncake flag per the official
                                       recommendation in
                                       Mooncake/docs/source/design/transfer-engine/
                                       transfer-engine-bench-tuning.md:
                                         empty / multi-NIC -> --auto_discovery
                                                              (lets mooncake's
                                                              topology engine
                                                              build the NIC
                                                              priority matrix;
                                                              this is the
                                                              recommended path
                                                              for multi-NIC).
                                         single NIC        -> --device_name=NIC
                                                              (the docs' "test
                                                              a single NIC"
                                                              recipe).
                                       "^foo,bar" NCCL exclusion syntax is NOT
                                       supported by transfer_engine_bench;
                                       it falls through to --auto_discovery.
  --mooncake-bench-bin PATH            Default: transfer_engine_bench (PATH)
  --mooncake-xport-type STR            Default: rdma (transfer_engine_bench --protocol)
  --mooncake-threads N                 Default: 12 (initiator threads)
  --mooncake-duration N                Default: 5 seconds
  --mooncake-target-wait-seconds N     Default: 90 seconds
  --pair-startup-seconds N             Used when sizing the target startup deadline
  -h, --help                           Show this help

Notes:
  --device cpu|gpu maps to transfer_engine_bench's --use_vram flag, which
  is gated behind USE_CUDA / USE_HIP / USE_MUSA / USE_MACA in
  transfer_engine_bench.cpp:110. BenchP2P's build_wheel.sh now configures
  Mooncake with -DUSE_HIP=ON for AMD ROCm, so the binary exposes:
    --use_vram=true|false
    --gpu_id=N           (per-rank GPU; we map it to LOCAL_RANK)
  --device gpu  -> --use_vram=true  --gpu_id=$LOCAL_RANK   (GPUDirect RDMA)
  --device cpu  -> --use_vram=false                        (DRAM)
  If you instead built mooncake with -DUSE_HIP=OFF (no GPU support), the
  binary won't recognize --use_vram and will exit with "Unknown command
  line flag 'use_vram'". Rebuild with build_wheel.sh.
EOF
}

RANK=""
WORLD_SIZE=""
LOCAL_RANK=""
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
BENCH_BIN="transfer_engine_bench"
XPORT_TYPE="rdma"
THREADS="12"
DURATION="5"
TARGET_WAIT_SECONDS="90"
PAIR_STARTUP_SECONDS="2"

die() { printf '[run_mooncake] ERROR: %s\n' "$*" >&2; exit 2; }

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
    --mooncake-bench-bin) BENCH_BIN="$2"; shift 2 ;;
    --mooncake-xport-type) XPORT_TYPE="$2"; shift 2 ;;
    --mooncake-threads) THREADS="$2"; shift 2 ;;
    --mooncake-duration) DURATION="$2"; shift 2 ;;
    --mooncake-target-wait-seconds) TARGET_WAIT_SECONDS="$2"; shift 2 ;;
    --pair-startup-seconds) PAIR_STARTUP_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done

[[ -n "$RANK" ]]        || die "--rank required"
[[ -n "$LOCAL_RANK" ]]  || die "--local-rank required"
[[ -n "$MASTER_ADDR" ]] || die "--master-addr required"
[[ -n "$OUTPUT_DIR" ]]  || die "--output-dir required"
[[ -n "$SOURCE_ROOT" ]] || die "--source-root required"
[[ -n "$SIZES" ]]       || die "--sizes required"

resolve_bin() {
  if command -v "$BENCH_BIN" >/dev/null 2>&1; then
    command -v "$BENCH_BIN"
    return
  fi
  for cand in \
    "${SOURCE_ROOT}/Mooncake/build/mooncake-transfer-engine/example/transfer_engine_bench" \
    "${SOURCE_ROOT}/Mooncake/build/transfer_engine_bench"; do
    if [[ -x "$cand" ]]; then echo "$cand"; return; fi
  done
  die "transfer_engine_bench binary not found: $BENCH_BIN"
}

# count_hca_entries <ib-hca-spec>
# Echoes the number of NICs in an NCCL-style HCA CSV after stripping the
# common '=' (exact-match) and ':<port>' (port suffix) decorations.
# '^' (exclusion) is not counted as a 1-entry whitelist; the caller treats
# any '^' prefix as "let mooncake auto-discover" because
# transfer_engine_bench --device_name doesn't honor exclusion syntax.
count_hca_entries() {
  local spec="${1:-}"
  [[ -z "$spec" ]] && { echo 0; return; }
  spec="${spec#=}"
  local n=0 entry
  IFS=',' read -ra _entries <<< "$spec"
  for entry in "${_entries[@]}"; do
    entry="${entry// /}"
    [[ -z "$entry" ]] && continue
    n=$((n + 1))
  done
  echo "$n"
}

build_hca_args() {
  # Per Mooncake/docs/source/design/transfer-engine/transfer-engine-bench-tuning.md:
  #   "By default, all NICs are used. To test a single NIC, replace
  #    --auto_discovery with --device_name=mlx_XXXX."
  # i.e. the official recommendation is:
  #   - multi-NIC: --auto_discovery (lets the topology engine build the
  #     NIC priority matrix and slice transfers across NICs at MC_SLICE_SIZE
  #     granularity; this is what production vLLM/SGLang deployments use)
  #   - single NIC test: --device_name=<one>
  # transfer_engine_bench --device_name does accept a CSV (per Mooncake
  # zh_archive/transfer-engine.md), but the upstream guide explicitly
  # steers multi-NIC users to auto_discovery, so we follow that.

  # NCCL-style exclusion ("^mlx5_0") is not supported by --device_name;
  # fall through to --auto_discovery and warn loudly so the user sees it.
  if [[ "$IB_HCA" == ^* ]]; then
    echo "[mooncake] WARNING: --ib-hca='${IB_HCA}' uses NCCL exclude syntax;" >&2
    echo "[mooncake]          transfer_engine_bench --device_name does not" >&2
    echo "[mooncake]          support '^'. Falling back to --auto_discovery." >&2
    printf '%s\n' "--auto_discovery"
    return
  fi

  local n
  n="$(count_hca_entries "$IB_HCA")"
  if [[ "$n" -eq 1 ]]; then
    # Strip "=" prefix (exact-match) and ":<port>" suffix so a spec like
    # "=mlx5_2:1" reaches transfer_engine_bench as "mlx5_2".
    local nic="${IB_HCA#=}"
    nic="${nic%:*}"
    # All log messages go to stderr so caller's `read` loop on stdout
    # only captures the actual flag tokens.
    echo "[mooncake] single NIC mode: --device_name=${nic} (--ib-hca='${IB_HCA}')" >&2
    printf '%s\n%s\n' "--device_name" "$nic"
    return
  fi

  if [[ "$n" -ge 2 ]]; then
    echo "[mooncake] multi-NIC (${n}) mode: --auto_discovery (--ib-hca='${IB_HCA}' ignored;" >&2
    echo "[mooncake]                       per Mooncake transfer-engine-bench-tuning.md," >&2
    echo "[mooncake]                       multi-NIC uses topology auto-discovery)" >&2
  else
    echo "[mooncake] no --ib-hca: --auto_discovery" >&2
  fi
  printf '%s\n' "--auto_discovery"
}

# Truncate a possibly-fractional seconds string to an integer (bash arith
# can't divide floats). 2.0 -> 2, 90 -> 90, 1.5 -> 1.
to_int_seconds() {
  local raw="$1"
  raw="${raw%.*}"
  [[ -z "$raw" ]] && raw="0"
  printf '%s' "$raw"
}

LOG_PATH="${OUTPUT_DIR}/logs/mooncake_rank${RANK}.log"
mkdir -p "$(dirname "$LOG_PATH")"

JOBID="${SLURM_JOB_ID:-$PPID}"
RUNTIME_DIR="${SOURCE_ROOT}/.benchp2p_runtime"
mkdir -p "$RUNTIME_DIR"

# Line-buffer the target's stdio so we can parse its startup line over a
# pipe (glibc would otherwise block-buffer when stdout isn't a TTY).
STDBUF_PREFIX=()
if command -v stdbuf >/dev/null 2>&1; then
  STDBUF_PREFIX=(stdbuf -oL -eL)
fi

run_target() {
  local size="$1" segment_file="$2" bin="$3"
  shift 3
  local -a hca_args=("$@")

  rm -f "$segment_file"

  local -a TARGET_CMD=(
    "${STDBUF_PREFIX[@]}"
    "$bin"
    --mode target
    --metadata_server P2PHANDSHAKE
    --protocol "$XPORT_TYPE"
    --operation "$OP_TYPE"
    --block_size "$size"
    --batch_size "$BATCH_SIZE"
    --threads "$THREADS"
    --duration "$DURATION"
    "${hca_args[@]}"
    "${MEM_ARGS[@]}"
  )

  printf '$ '
  printf '%q ' "${TARGET_CMD[@]}"
  printf '\n\n'

  local target_out
  target_out="$(mktemp -t mooncake_target_XXXXXX.log)"
  : > "$target_out"

  "${TARGET_CMD[@]}" >"$target_out" 2>&1 &
  local target_pid=$!

  # Forward target stdout to the harness while we wait for the listening
  # line. Tail keeps running until we explicitly stop it after the target
  # exits.
  tail -F -n +1 "$target_out" 2>/dev/null &
  local tail_pid=$!

  local startup=$(to_int_seconds "$PAIR_STARTUP_SECONDS")
  local deadline=$(( $(date +%s) + 30 + startup ))
  local rpc_port=""
  while [[ -z "$rpc_port" ]]; do
    if [[ $(date +%s) -ge $deadline ]]; then
      break
    fi
    if ! kill -0 "$target_pid" 2>/dev/null; then
      break
    fi
    rpc_port="$(sed -nE 's/.*listening on [^[:space:]]+:([0-9]+).*/\1/p' "$target_out" 2>/dev/null | head -n 1)"
    [[ -z "$rpc_port" ]] && sleep 0.5
  done

  if [[ -z "$rpc_port" ]]; then
    kill -TERM "$target_pid" 2>/dev/null || true
    wait "$target_pid" 2>/dev/null || true
    kill -TERM "$tail_pid" 2>/dev/null || true
    wait "$tail_pid" 2>/dev/null || true
    rm -f "$target_out"
    die "Mooncake transfer_engine_bench target did not print 'Transfer Engine RPC ... listening on HOST:PORT'"
  fi

  local seg_id="${MASTER_ADDR}:${rpc_port}"
  printf '%s\n' "$seg_id" > "$segment_file"
  echo "[bench_p2p_compare] published segment_id=${seg_id}"

  # Drain target stdout while the initiator runs. transfer_engine_bench
  # --mode target is a long-running server that never self-exits (it
  # listens until SIGTERM), so polling `kill -0 $target_pid` alone would
  # always wait the full deadline and burn ~30s/size on a 21-size sweep
  # (~13 min total, vs the ~5s of actual measurement). The initiator
  # writes <segment_file>.done when it finishes, so we exit the drain
  # loop the moment we see that flag. The deadline is kept as a fallback
  # in case the initiator crashes before writing the flag.
  local done_flag="${segment_file}.done"
  local drain_deadline=$(( $(date +%s) + startup + DURATION + 30 ))
  while kill -0 "$target_pid" 2>/dev/null; do
    if [[ -e "$done_flag" ]]; then
      break
    fi
    if [[ $(date +%s) -ge $drain_deadline ]]; then
      break
    fi
    sleep 0.2
  done

  if kill -0 "$target_pid" 2>/dev/null; then
    kill -TERM "$target_pid" 2>/dev/null || true
    # Give the server a second to flush its CQ / close QPs cleanly.
    local term_deadline=$(( $(date +%s) + 2 ))
    while kill -0 "$target_pid" 2>/dev/null; do
      [[ $(date +%s) -ge $term_deadline ]] && break
      sleep 0.1
    done
    kill -KILL "$target_pid" 2>/dev/null || true
  fi
  wait "$target_pid" 2>/dev/null || true
  rm -f "$done_flag"

  # Give tail a moment to flush, then stop it.
  sleep 0.3
  kill -TERM "$tail_pid" 2>/dev/null || true
  wait "$tail_pid" 2>/dev/null || true
  rm -f "$target_out"
}

run_initiator() {
  local size="$1" segment_file="$2" bin="$3"
  shift 3
  local -a hca_args=("$@")

  # On NFS, the runtime dir's directory metadata is attribute-cached
  # client-side (acdirmin defaults to 30s). Polling segment_file.exists()
  # alone can stay false for tens of seconds after rank-0 wrote it; an
  # explicit listdir() per poll forces NFS READDIRPLUS/GETATTR.
  local wait_secs
  wait_secs=$(to_int_seconds "$TARGET_WAIT_SECONDS")
  local deadline=$(( $(date +%s) + wait_secs ))
  while [[ ! -f "$segment_file" ]]; do
    if [[ $(date +%s) -ge $deadline ]]; then
      break
    fi
    ls "$(dirname "$segment_file")" >/dev/null 2>&1 || true
    sleep 0.5
  done
  if [[ ! -f "$segment_file" ]]; then
    die "Mooncake target segment file not found: $segment_file"
  fi

  local seg_id
  seg_id="$(tr -d '[:space:]' < "$segment_file")"

  local -a INIT_CMD=(
    "${STDBUF_PREFIX[@]}"
    "$bin"
    --mode initiator
    --metadata_server P2PHANDSHAKE
    --protocol "$XPORT_TYPE"
    --operation "$OP_TYPE"
    --block_size "$size"
    --batch_size "$BATCH_SIZE"
    --threads "$THREADS"
    --duration "$DURATION"
    "${hca_args[@]}"
    "${MEM_ARGS[@]}"
    --segment_id "$seg_id"
  )

  printf '$ '
  printf '%q ' "${INIT_CMD[@]}"
  printf '\n\n'

  local rc=0
  ( cd "$SOURCE_ROOT" && "${INIT_CMD[@]}" ) || rc=$?

  # Tell run_target() (on rank 0) we're done so it can SIGTERM its
  # transfer_engine_bench --mode target server immediately instead of
  # waiting the full drain_deadline. Best-effort: if the shared FS is
  # unwritable, run_target's deadline still bounds the wait.
  : > "${segment_file}.done" 2>/dev/null || true

  return "$rc"
}

main() {
  if [[ "$RANK" -gt 1 ]]; then
    echo "[mooncake] skipping unused rank ${RANK}"
    return 0
  fi

  # Memory-placement flags. transfer_engine_bench gates --use_vram and
  # --gpu_id behind USE_CUDA / USE_HIP / USE_MUSA / USE_MACA at compile
  # time (transfer_engine_bench.cpp:110); BenchP2P's build_wheel.sh
  # configures mooncake with -DUSE_HIP=ON for ROCm so these flags exist.
  # If somebody rebuilds the wheel with -DUSE_HIP=OFF, gflags will fail
  # loud with "Unknown command line flag 'use_vram'", which is what we
  # want -- silently falling back to DRAM would publish bogus "GPU"
  # numbers in the comparison sheet.
  case "$DEVICE" in
    gpu)
      MEM_ARGS=(--use_vram=true --gpu_id="${LOCAL_RANK}")
      echo "[mooncake] device=gpu  -> --use_vram=true --gpu_id=${LOCAL_RANK}" >&2
      ;;
    cpu)
      MEM_ARGS=(--use_vram=false)
      echo "[mooncake] device=cpu  -> --use_vram=false (DRAM)" >&2
      ;;
    *)
      die "--device must be cpu or gpu (got '${DEVICE}')"
      ;;
  esac

  local bin
  bin="$(resolve_bin)"

  local -a HCA_ARGS=()
  while IFS= read -r tok; do
    [[ -n "$tok" ]] && HCA_ARGS+=("$tok")
  done < <(build_hca_args)

  local IFS=','
  read -ra size_list <<< "$SIZES"
  unset IFS

  local size segment_file
  local any_ok=0
  local any_fail=0
  local last_rc=0
  for size_raw in "${size_list[@]}"; do
    size="${size_raw// /}"
    [[ -z "$size" ]] && continue
    segment_file="${RUNTIME_DIR}/mooncake_target_seg_${JOBID}_${size}.txt"

    # Subshell isolates `set -e` aborts and `die` from a single bad
    # size (e.g. mooncake's HIP-buffer "Corrupted segment descriptor"
    # at very small block_size on AMD GPUs) so the per-size loop keeps
    # going. Both ranks process identical size lists, so they stay in
    # lock-step on size boundaries even when one or two sizes are bad.
    local rc=0
    if [[ "$RANK" -eq 0 ]]; then
      ( run_target "$size" "$segment_file" "$bin" "${HCA_ARGS[@]}" ) || rc=$?
    else
      ( run_initiator "$size" "$segment_file" "$bin" "${HCA_ARGS[@]}" ) || rc=$?
    fi
    if [[ "$rc" -ne 0 ]]; then
      echo "[mooncake] WARNING: rank=${RANK} block_size=${size} batch=${BATCH_SIZE} failed (exit ${rc}); continuing with next size"
      any_fail=1
      last_rc="$rc"
    else
      any_ok=1
    fi
  done

  if [[ "$any_ok" -eq 0 ]] && [[ "$any_fail" -eq 1 ]]; then
    echo "[mooncake] ERROR: rank=${RANK} every size failed (last exit ${last_rc})"
    return "$last_rc"
  fi
  return 0
}

main 2>&1 | tee "$LOG_PATH"
exit "${PIPESTATUS[0]}"
