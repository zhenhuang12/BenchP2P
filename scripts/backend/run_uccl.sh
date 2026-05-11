#!/usr/bin/env bash
# Run UCCL P2P benchmark for a single rank.
#
# Translates BenchP2P's unified args into a uccl/p2p/benchmarks/benchmark_uccl*
# invocation + env vars, then tees stdout/stderr to
# <output-dir>/logs/uccl_rank<RANK>.log. PIPESTATUS preserves the benchmark's
# exit code through the tee.
#
# Three UCCL-specific transforms happen here (kept inline so the echoed
# command is self-documenting in the per-rank log):
#   1) Mode dispatch via --op-type:
#        --op-type write  -> benchmark_uccl_readwrite.py --mode write
#                            (one-sided RDMA WRITE; matches mooncake
#                            --operation=write, nixl --op_type=WRITE,
#                            mori --op-type=write so the four backends
#                            actually compare the same ULP).
#        --op-type read   -> benchmark_uccl_readwrite.py --mode read
#                            (one-sided RDMA READ).
#        --uccl-sendrecv  -> benchmark_uccl.py (two-sided RDMA SEND/RECV;
#                            the legacy default; only useful when you want
#                            to compare UCCL's own ULP cost against its
#                            WRITE path, NOT for cross-backend comparison).
#      The two scripts have *different* CLIs: SEND/RECV uses --num-kvblocks,
#      READ/WRITE uses --num-iovs and accepts --mode + --lazy. We hide that
#      behind BATCH_SIZE here so the harness stays uniform across backends.
#   2) UCCL --sizes is the per-MESSAGE total: both benchmark scripts compute
#      `size_per_block = size // {num_kvblocks|num_iovs}`. Mori / nixl /
#      mooncake take per-block sizes directly. We multiply --sizes by
#      BATCH_SIZE here so the UCCL run transfers the same
#      `block_size * BATCH_SIZE` per iter as the other three.
#   3) UCCL's compiled-in `kNICContextNumber = 4` (see
#      3rdparty/uccl/p2p/rdma/define.h) caps how many HCAs one engine can
#      attach. We trim --ib-hca down to that many for UCCL only and warn
#      loudly. Going past triggers `assert(device_ids.size() <=
#      kNICContextNumber)` in NICEndpoint::initializeContexts -> SIGABRT.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_uccl.sh [options]

Required:
  --rank N
  --world-size N
  --local-rank N
  --output-dir DIR        Logs land in <DIR>/logs/uccl_rank<RANK>.log
  --source-root DIR       3rdparty root containing uccl/
  --sizes CSV             Per-block byte sizes, comma separated
  --iters N
  --device cpu|gpu

Optional:
  --op-type read|write    Selects RDMA op for benchmark_uccl_readwrite.py
                          (default write). Ignored when --uccl-sendrecv
                          is passed (legacy SEND/RECV path has no op type).
  --uccl-sendrecv         Force the legacy two-sided benchmark_uccl.py
                          (RDMA SEND/RECV). Default is one-sided
                          benchmark_uccl_readwrite.py so UCCL is compared
                          on the same ULP as the other three backends.
  --batch-size N          --num-iovs (or --num-kvblocks under sendrecv)
                          for UCCL (default 1).
                          --num-blocks accepted as deprecated alias.
  --no-lazy               Disable benchmark_uccl_readwrite.py --lazy
                          (per-iter ibv_reg_mr; noisier small-size
                          numbers, larger MR registration overhead). Has
                          no effect under --uccl-sendrecv.
  --ib-hca SPEC           NCCL-style HCA selector
                          ("a,b,c" whitelist | "^a,b" exclude).
  --script PATH           Override path to the chosen benchmark_uccl*.py.
  --async-api             Pass --async-api to the benchmark.
  --master-addr ADDR      Forwarded for parity, ignored by UCCL.
  --master-port PORT      Forwarded for parity, ignored by UCCL.
  -h, --help              Show this help.

Env:
  UCCL_MAX_NICS           Override UCCL's compiled-in kNICContextNumber=4
                          NIC cap (only safe after rebuilding UCCL).
EOF
}

die()     { printf '[run_uccl] ERROR: %s\n' "$*" >&2; exit 2; }
require() { [[ -n "$2" ]] || die "$1 required"; }

# Split a CSV string into a named bash array (nameref), trimming whitespace
# from each field and discarding empty fragments. Replaces the verbose
# saved-IFS / restored-IFS dance the original used in three different
# places.
split_csv() {
  local -n __out=$1
  local -a _tmp=()
  IFS=',' read -ra _tmp <<< "$2"
  __out=()
  local _f
  for _f in "${_tmp[@]}"; do
    _f="${_f// /}"
    [[ -n "$_f" ]] && __out+=("$_f")
  done
}

# Join a bash array with a delimiter on stdout (no trailing delimiter).
join_by() {
  local sep="$1"; shift
  if (( $# == 0 )); then return 0; fi
  local first="$1"; shift
  printf '%s' "$first" "${@/#/$sep}"
}

# --- args ---------------------------------------------------------------
RANK=""
WORLD_SIZE=""
LOCAL_RANK=""
OUTPUT_DIR=""
SOURCE_ROOT=""
SIZES=""
ITERS=""
BATCH_SIZE="1"
DEVICE="gpu"
IB_HCA=""
SCRIPT_PATH=""
ASYNC_API="0"
OP_TYPE="write"
SENDRECV="0"
LAZY="1"
UCCL_MAX_NICS="${UCCL_MAX_NICS:-4}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rank)        RANK="$2";        shift 2 ;;
    --world-size)  WORLD_SIZE="$2";  shift 2 ;;
    --local-rank)  LOCAL_RANK="$2";  shift 2 ;;
    --output-dir)  OUTPUT_DIR="$2";  shift 2 ;;
    --source-root) SOURCE_ROOT="$2"; shift 2 ;;
    --sizes)       SIZES="$2";       shift 2 ;;
    --iters)       ITERS="$2";       shift 2 ;;
    --batch-size|--num-blocks) BATCH_SIZE="$2"; shift 2 ;;
    --device)      DEVICE="$2";      shift 2 ;;
    --ib-hca)      IB_HCA="$2";      shift 2 ;;
    --script)      SCRIPT_PATH="$2"; shift 2 ;;
    --async-api)   ASYNC_API="1";    shift ;;
    --op-type)     OP_TYPE="$2";     shift 2 ;;
    --uccl-sendrecv) SENDRECV="1";   shift ;;
    --no-lazy)     LAZY="0";         shift ;;
    --master-addr|--master-port) shift 2 ;;  # accepted for parity, ignored
    -h|--help)     usage; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done

case "$OP_TYPE" in
  read|write) ;;
  *) die "--op-type must be read or write (got: $OP_TYPE)" ;;
esac

# --- validation ---------------------------------------------------------
require --rank        "$RANK"
require --world-size  "$WORLD_SIZE"
require --local-rank  "$LOCAL_RANK"
require --output-dir  "$OUTPUT_DIR"
require --source-root "$SOURCE_ROOT"
require --sizes       "$SIZES"
require --iters       "$ITERS"

# Pick the default benchmark script based on --uccl-sendrecv. --script
# always wins if the user passed it explicitly (escape hatch for testing
# a custom benchmark_uccl* variant).
if [[ -z "$SCRIPT_PATH" ]]; then
  if [[ "$SENDRECV" == "1" ]]; then
    SCRIPT_PATH="${SOURCE_ROOT}/uccl/p2p/benchmarks/benchmark_uccl.py"
  else
    SCRIPT_PATH="${SOURCE_ROOT}/uccl/p2p/benchmarks/benchmark_uccl_readwrite.py"
  fi
fi
[[ -f "$SCRIPT_PATH" ]] || die "benchmark script not found: $SCRIPT_PATH"

LOG_PATH="${OUTPUT_DIR}/logs/uccl_rank${RANK}.log"
mkdir -p "$(dirname "$LOG_PATH")"

# --- helpers ------------------------------------------------------------
# Resolve --ib-hca into the CSV list UCCL should attach to and export it
# as UCCL_P2P_RDMA_DEV. NCCL_IB_HCA is set verbatim so any NCCL co-tenants
# in the same container see the same intent.
#   "a,b,c"  -> use as-is (whitelist)
#   "^a,b"   -> read /sys/class/infiniband, drop the excluded ones
# Then clamp to UCCL_MAX_NICS so we never trigger UCCL's hard NIC cap.
resolve_uccl_hcas() {
  local spec="$1"
  [[ -z "$spec" ]] && return 0
  export NCCL_IB_HCA="$spec"
  echo "[uccl] env: NCCL_IB_HCA=$spec"

  # Explicitly init to empty arrays: under `set -u`, `local -a foo` only
  # sets the array attribute; `${#foo[@]}` on it still triggers
  # "unbound variable". Assigning =() marks the variable as set.
  local -a candidates=()
  if [[ "$spec" != ^* ]]; then
    split_csv candidates "$spec"
  else
    local -a available=() excludes=()
    if [[ -d /sys/class/infiniband ]]; then
      mapfile -t available < <(ls -1 /sys/class/infiniband 2>/dev/null | sort)
    fi
    split_csv excludes "${spec:1}"
    local dev e
    for dev in "${available[@]}"; do
      for e in "${excludes[@]}"; do [[ "$dev" == "$e" ]] && continue 2; done
      candidates+=("$dev")
    done
  fi

  if (( ${#candidates[@]} == 0 )); then
    echo "[uccl] WARNING: --ib-hca=${spec} resolved to no HCAs; leaving UCCL_P2P_RDMA_DEV unset" >&2
    return 0
  fi

  if (( ${#candidates[@]} > UCCL_MAX_NICS )); then
    local -a kept=("${candidates[@]:0:UCCL_MAX_NICS}")
    local -a dropped=("${candidates[@]:UCCL_MAX_NICS}")
    echo "[uccl] WARNING: --ib-hca=${spec} resolved to ${#candidates[@]} HCAs (${candidates[*]});" >&2
    echo "[uccl] WARNING:   UCCL kNICContextNumber=${UCCL_MAX_NICS}; keeping ${kept[*]}, dropping ${dropped[*]}." >&2
    echo "[uccl] WARNING:   bump UCCL_MAX_NICS only after rebuilding UCCL with a larger kNICContextNumber." >&2
    candidates=("${kept[@]}")
  fi

  local resolved
  resolved="$(join_by , "${candidates[@]}")"
  export UCCL_P2P_RDMA_DEV="$resolved"
  echo "[uccl] env: UCCL_P2P_RDMA_DEV=$resolved"
}

# Multiply each comma-separated size by BATCH_SIZE (see header note 1).
scale_sizes_for_uccl() {
  local -a items=() scaled=()
  split_csv items "$1"
  local s
  for s in "${items[@]}"; do scaled+=("$(( s * BATCH_SIZE ))"); done
  join_by , "${scaled[@]}"
}

# --- main ---------------------------------------------------------------
main() {
  resolve_uccl_hcas "$IB_HCA"

  local uccl_sizes
  uccl_sizes="$(scale_sizes_for_uccl "$SIZES")"

  # Effective op type for reporting / marker. SEND/RECV has no notion of
  # WRITE vs READ at the verbs layer (it's a two-sided channel), so the
  # marker says "sendrecv" in that mode regardless of what --op-type was.
  local effective_op
  if [[ "$SENDRECV" == "1" ]]; then
    effective_op="sendrecv"
  else
    effective_op="$OP_TYPE"
  fi

  # Marker keyed off by scripts/bench_p2p_compare.py::parse_metrics so the
  # report converts UCCL's per-message logged size back to the unified
  # per-block size_bytes and stamps Metric.operation correctly. Keep the
  # `batch_size=` token first so the legacy regex
  # _UCCL_BATCH_MARKER_RE still matches older logs without op_type.
  echo "[bench_p2p_compare] uccl batch_size=${BATCH_SIZE} op_type=${effective_op} script=$(basename "$SCRIPT_PATH") block_sizes=${SIZES} uccl_sizes=${uccl_sizes}"

  local -a CMD=(
    python3 "$SCRIPT_PATH"
    --sizes "$uccl_sizes"
    --iters "$ITERS"
    --device "$DEVICE"
    --local-gpu-idx "$LOCAL_RANK"
  )
  if [[ "$SENDRECV" == "1" ]]; then
    # Legacy two-sided benchmark_uccl.py: --num-kvblocks, no --mode/--lazy.
    CMD+=(--num-kvblocks "$BATCH_SIZE")
  else
    # benchmark_uccl_readwrite.py: --num-iovs + --mode + (default) --lazy.
    CMD+=(
      --num-iovs "$BATCH_SIZE"
      --mode "$OP_TYPE"
    )
    [[ "$LAZY" == "1" ]] && CMD+=(--lazy)
  fi
  [[ "$ASYNC_API" == "1" ]] && CMD+=(--async-api)

  printf '$ '; printf '%q ' "${CMD[@]}"; printf '\n\n'
  cd "$SOURCE_ROOT"
  "${CMD[@]}"
}

main 2>&1 | tee "$LOG_PATH"
exit "${PIPESTATUS[0]}"
