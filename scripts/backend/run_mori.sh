#!/usr/bin/env bash
# Run MORI P2P benchmark for a single rank.
#
# Mirrors run_mori in scripts/bench_p2p_compare.py: invokes
# mori/tests/python/io/benchmark.py once per size, sets PYTHONPATH so its
# `from tests.python.utils import ...` resolves, translates --ib-hca to
# MORI_RDMA_DEVICES / NCCL_IB_HCA, and resolves the per-rank --host that
# mori RDMA needs to bind its TCP control socket.
#
# Sizes must be passed as a comma-separated list of integer bytes.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_mori.sh [options]

Required:
  --rank N
  --world-size N
  --local-rank N
  --master-addr ADDR
  --master-port PORT
  --output-dir DIR
  --source-root DIR
  --sizes CSV             Integer-byte sizes
  --iters N
  --op-type read|write

Optional:
  --device cpu|gpu                 (forwarded for parity)
  --batch-size N                   Forwarded to MORI as --transfer-batch-size
                                   (--num-blocks accepted as deprecated alias).
                                   Default: 1
  --ib-hca SPEC                    NCCL-style HCA selector
  --mori-backend rdma|xgmi         Default: rdma
  --mori-xgmi-multiprocess         Pass --xgmi-multiprocess to mori
  --mori-batched-api auto|on|off   Toggle --enable-batch-transfer.
                                   auto = on iff --batch-size > 1. Required for
                                   batch>1 (mori validate() expects strided
                                   offsets only the batched path produces). See
                                   README "Backend notes".
  --mori-use-sess auto|on|off      Toggle --enable-sess (lower setup latency).
                                   auto = on iff --batch-size > 1; matches
                                   mori's docs/MORI-IO-BENCHMARK.md examples.
  --mori-num-qp-per-transfer N|auto
                                   Forwarded as --num-qp-per-transfer.
                                   auto policy (matches MORI-IO-BENCHMARK.md):
                                     1 NIC, bs=1 -> 1 QP   (CX7 bs=1)
                                     1 NIC, bs>1 -> 4 QP   (Thor2 size-sweep)
                                    >1 NIC       -> 2 QP   (8x8 NIC example,
                                                            "2 QPs per NIC")
                                   Pass an explicit integer (e.g. 16) for
                                   batch sweeps where you want max IOPS at
                                   small bs. Default: auto.
  --mori-num-initiator-dev N|auto  Forwarded as --num-initiator-dev (mori
                                   internally torch.multiprocessing.spawn's
                                   N children per node, each binding cuda:i
                                   and 1 NIC). auto = number of NICs parsed
                                   from --ib-hca (1 if --ib-hca is empty).
                                   Mori asserts initiator == target;
                                   defaults keep them equal. Default: auto.
  --mori-num-target-dev N|auto     Forwarded as --num-target-dev. Defaults
                                   the same way as --mori-num-initiator-dev.
  --mori-use-sweep auto|on|off     Use mori's native --all sweep instead of
                                   one process per --sizes entry. mori sweep
                                   only emits powers of two (cur *= 2 in
                                   benchmark.py). Modes:
                                     auto (default) -- enable iff --sizes is
                                       a contiguous power-of-two ladder
                                       (every entry is 2x the previous).
                                       Falls back to per-size loop otherwise
                                       so user-specified non-power-of-two
                                       sizes still run.
                                     on             -- always sweep [min, max]
                                       of --sizes; mori will emit a pow-2
                                       ladder regardless of intermediate
                                       sizes the caller listed (i.e. extra
                                       points may appear, requested non-pow2
                                       points will not).
                                     off            -- per-size invocation
                                       (legacy behavior; one process per
                                       size, slow but exact).
                                   Sweep mode reduces wall-clock time on a
                                   10-size run from ~50-100s init overhead
                                   to ~5-10s (single mori spawn + gloo +
                                   NIC handshake + MR registration).
  --script PATH                    Override path to mori benchmark.py
  -h, --help                       Show this help
EOF
}

die() { printf '[run_mori] ERROR: %s\n' "$*" >&2; exit 2; }

# --- args ---------------------------------------------------------------
RANK=""
WORLD_SIZE=""
LOCAL_RANK=""
MASTER_ADDR=""
MASTER_PORT="29500"
OUTPUT_DIR=""
SOURCE_ROOT=""
SIZES=""
ITERS=""
OP_TYPE="write"
DEVICE="gpu"
BATCH_SIZE="1"
IB_HCA=""
MORI_BACKEND="rdma"
MORI_XGMI_MULTIPROCESS="0"
MORI_BATCHED_API="on"
MORI_USE_SESS="on"
# auto-resolved in main(): see MORI-IO-BENCHMARK.md
#   1 NIC + bs=1 -> 1 QP  (CX7 bs=1)
#   1 NIC + bs>1 -> 4 QP  (Thor2 size-sweep)
#  >1 NIC        -> 2 QP  (8x8 NIC example, "2 QPs per NIC")
MORI_NUM_QP_PER_TRANSFER="auto"
# auto-resolved in main(): default to NIC count parsed from --ib-hca (1
# if --ib-hca empty). User overrides for asymmetric tests are allowed
# but mori asserts initiator == target so the script keeps both equal
# unless explicitly overridden differently on the CLI.
MORI_NUM_INITIATOR_DEV="auto"
MORI_NUM_TARGET_DEV="auto"
# auto: switch to mori's native --all sweep iff --sizes is a contiguous
# power-of-2 ladder; otherwise legacy per-size loop. See usage above.
MORI_USE_SWEEP="auto"
SCRIPT_PATH=""

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
    --op-type) OP_TYPE="$2"; shift 2 ;;
    --device) DEVICE="$2"; shift 2 ;;
    --batch-size|--num-blocks) BATCH_SIZE="$2"; shift 2 ;;
    --ib-hca) IB_HCA="$2"; shift 2 ;;
    --mori-backend) MORI_BACKEND="$2"; shift 2 ;;
    --mori-xgmi-multiprocess) MORI_XGMI_MULTIPROCESS="1"; shift ;;
    --mori-batched-api) MORI_BATCHED_API="$2"; shift 2 ;;
    --mori-use-sess) MORI_USE_SESS="$2"; shift 2 ;;
    --mori-num-qp-per-transfer) MORI_NUM_QP_PER_TRANSFER="$2"; shift 2 ;;
    --mori-num-initiator-dev) MORI_NUM_INITIATOR_DEV="$2"; shift 2 ;;
    --mori-num-target-dev) MORI_NUM_TARGET_DEV="$2"; shift 2 ;;
    --mori-use-sweep) MORI_USE_SWEEP="$2"; shift 2 ;;
    --script) SCRIPT_PATH="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done

require() { [[ -n "$2" ]] || die "$1 required"; }
require --rank        "$RANK"
require --local-rank  "$LOCAL_RANK"
require --master-addr "$MASTER_ADDR"
require --output-dir  "$OUTPUT_DIR"
require --source-root "$SOURCE_ROOT"
require --sizes       "$SIZES"
require --iters       "$ITERS"

# --- paths --------------------------------------------------------------
SCRIPT_PATH="${SCRIPT_PATH:-${SOURCE_ROOT}/mori/tests/python/io/benchmark.py}"
[[ -f "$SCRIPT_PATH" ]] || die "benchmark script not found: $SCRIPT_PATH"

# mori benchmark.py uses `from tests.python.utils import ...`; root the
# process at the mori repo and prepend it to PYTHONPATH so that resolves.
# Default to <source>/mori; fall back to walking up from --script if the
# user pointed --script at a non-default checkout.
MORI_ROOT="${SOURCE_ROOT}/mori"
if [[ ! -f "${MORI_ROOT}/tests/python/utils.py" ]]; then
  MORI_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/../../.." 2>/dev/null && pwd || true)"
fi
[[ -f "${MORI_ROOT}/tests/python/utils.py" ]] \
  || die "couldn't locate mori root (need tests/python/utils.py); tried ${SOURCE_ROOT}/mori and $(dirname "$SCRIPT_PATH")/../../.."
export PYTHONPATH="${MORI_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

LOG_PATH="${OUTPUT_DIR}/logs/mori_rank${RANK}.log"
mkdir -p "$(dirname "$LOG_PATH")"

# --- helpers ------------------------------------------------------------

# mori_flag_for <toggle:auto|on|off> <flag-to-emit>
# Echoes <flag> when the toggle resolves to "on", empty otherwise. auto is
# "on" iff BATCH_SIZE > 1. Caller appends the result to CMD verbatim.
#
# IMPORTANT: must always return 0 so callers can do
# `var="$(mori_flag_for ...)"` under `set -e`. (Bash, unlike POSIX sh, does
# NOT exempt simple variable assignments from set -e when the command
# substitution they capture exits non-zero, so a stray `[[ FALSE ]] &&
# echo` short-circuit inside this function would silently kill the run.)
mori_flag_for() {
  local toggle="$1" flag="$2"
  case "$toggle" in
    on)   echo "$flag" ;;
    off)  : ;;
    auto) if [[ "$BATCH_SIZE" -gt 1 ]]; then echo "$flag"; fi ;;
    *)    die "invalid toggle '$toggle' (expected auto|on|off)" ;;
  esac
  return 0
}

# log_toggle <label> <user-toggle> <resolved-flag>
log_toggle() {
  local state="off"
  if [[ -n "$3" ]]; then state="on"; fi
  printf '[mori] %s = %s (--mori-%s=%s)\n' "$1" "$state" "$1" "$2"
}

# count_ib_hca <ib-hca-spec>
# Echoes the number of NICs in an NCCL-style HCA selector, treating empty
# / missing inputs as 1. Supports the common forms accepted by NCCL:
#   "mlx5_0,mlx5_1,...,mlx5_7"            -> 8
#   "mlx5_0:1,mlx5_1:1,...,mlx5_7:1"      -> 8  (port suffix stripped)
#   "=mlx5_0,mlx5_1"                      -> 2  (exact-match prefix stripped)
#   "^mlx5_2"                             -> 1  (blacklist; we count entries
#                                                because we have no way to
#                                                know the host's full HCA list
#                                                here, so user should pass
#                                                an allow-list for multi-NIC)
# The ^ blacklist is generally the wrong shape for "set num-init-dev to N"
# anyway -- callers running multi-NIC mori configs should always pass an
# explicit whitelist via --ib-hca, which is the canonical form everywhere
# else in BenchP2P (run_mooncake.sh, run_uccl.sh).
count_ib_hca() {
  local spec="${1:-}"
  if [[ -z "$spec" ]]; then echo 1; return 0; fi
  spec="${spec#^}"; spec="${spec#=}"
  local n=0 entry
  IFS=',' read -ra _entries <<< "$spec"
  for entry in "${_entries[@]}"; do
    entry="${entry// /}"
    [[ -z "$entry" ]] && continue
    n=$((n + 1))
  done
  if [[ "$n" -le 0 ]]; then echo 1; else echo "$n"; fi
}

# resolve_dev_count <user-toggle:N|auto> <auto-default> <flag-name>
# Returns the integer dev count to pass to mori. `auto` resolves to the
# auto-default, anything else must be a positive int.
resolve_dev_count() {
  local toggle="$1" auto_default="$2" flag="$3"
  case "$toggle" in
    auto) echo "$auto_default" ;;
    ''|*[!0-9]*) die "invalid --mori-${flag} '$toggle' (expected positive int or 'auto')" ;;
    *) [[ "$toggle" -ge 1 ]] || die "--mori-${flag} must be >= 1"
       echo "$toggle" ;;
  esac
  return 0
}

# is_pow2_ladder <csv>
# Returns 0 (true) iff <csv> is a contiguous power-of-2 ladder with >= 2
# entries: every entry is a positive power of 2 AND each entry is exactly
# 2x the previous. This matches what mori's `--all` sweep produces (see
# benchmark.py's `cur_size *= 2` loop), so when this returns true we can
# replace BenchP2P's per-size invocation with one --all sweep that emits
# the same set of points.
#
# Empty / single-element / non-power-of-2 / non-contiguous lists return
# false so the caller falls back to the per-size loop, which preserves
# exact data points the user asked for.
is_pow2_ladder() {
  local csv="$1"
  local -a elts
  IFS=',' read -ra elts <<< "$csv"
  local n=0 prev="" e
  for e in "${elts[@]}"; do
    e="${e// /}"
    [[ -z "$e" ]] && continue
    [[ "$e" =~ ^[0-9]+$ ]] || return 1
    [[ "$e" -ge 1 ]] || return 1
    # power-of-2 test: e & (e-1) == 0
    (( (e & (e - 1)) == 0 )) || return 1
    if [[ -n "$prev" ]]; then
      (( e == prev * 2 )) || return 1
    fi
    prev="$e"
    n=$((n + 1))
  done
  [[ "$n" -ge 2 ]]
}

# mori RDMA's --host is each engine's *local* TCP listen/bind address (see
# mori/src/application/transport/tcp/tcp.cpp::Listen). Peers exchange the
# resulting handle{host,port} through gloo (torch.distributed bound to
# MASTER_ADDR:MASTER_PORT) inside _initialize_rdma, so we do NOT need to
# pre-share the target's IP. Each rank just needs to bind to one of its
# own NIC IPv4s; rank 0 has MASTER_ADDR by definition (slurm head node),
# other ranks resolve their own hostname.
resolve_mori_host() {
  if [[ "$MORI_BACKEND" != "rdma" ]] || [[ "$RANK" -eq 0 ]]; then
    echo "$MASTER_ADDR"
    return
  fi
  python3 -c 'import socket; print(socket.gethostbyname(socket.gethostname()))' \
    2>/dev/null || echo "$MASTER_ADDR"
}

# --- main ---------------------------------------------------------------
main() {
  if [[ -n "$IB_HCA" ]]; then
    export NCCL_IB_HCA="$IB_HCA" MORI_RDMA_DEVICES="$IB_HCA"
    echo "[mori] env: NCCL_IB_HCA=${IB_HCA}"
    echo "[mori] env: MORI_RDMA_DEVICES=${IB_HCA}"
  fi

  local mori_host
  mori_host="$(resolve_mori_host)"
  [[ "$MORI_BACKEND" == "rdma" ]] \
    && echo "[mori] rank=${RANK} binding --host=${mori_host}"

  local batched_flag sess_flag
  batched_flag="$(mori_flag_for "$MORI_BATCHED_API" --enable-batch-transfer)"
  sess_flag="$(mori_flag_for "$MORI_USE_SESS"      --enable-sess)"
  log_toggle batched-api "$MORI_BATCHED_API" "$batched_flag"
  log_toggle use-sess    "$MORI_USE_SESS"    "$sess_flag"

  # Resolve --num-initiator-dev / --num-target-dev. Defaults to NIC count
  # parsed from --ib-hca; mori asserts initiator == target, and each spawned
  # child binds cuda:role_rank + 1 NIC, so callers with N NICs + N GPUs
  # should typically use the auto-resolved value.
  local hca_count resolved_init resolved_target
  hca_count="$(count_ib_hca "$IB_HCA")"
  resolved_init="$(resolve_dev_count "$MORI_NUM_INITIATOR_DEV" "$hca_count" num-initiator-dev)"
  resolved_target="$(resolve_dev_count "$MORI_NUM_TARGET_DEV"  "$hca_count" num-target-dev)"
  if [[ "$resolved_init" -ne "$resolved_target" ]]; then
    echo "[mori] WARNING: num_initiator_dev (${resolved_init}) != num_target_dev (${resolved_target}); mori benchmark.py will assert. Pass matching --mori-num-initiator-dev / --mori-num-target-dev." >&2
  fi
  printf '[mori] num-initiator-dev = %s, num-target-dev = %s (ib-hca count=%s)\n' \
    "$resolved_init" "$resolved_target" "$hca_count"

  # Resolve --num-qp-per-transfer. Defaults track docs/MORI-IO-BENCHMARK.md:
  #   1 NIC + bs=1 -> 1 QP   ("Results: CX7 RDMA (Batch Size = 1)")
  #   1 NIC + bs>1 -> 4 QPs  (Thor2 size-sweep, fixes batch=128 + 4 QPs and
  #                           reaches ~95% NIC peak on a 400Gb-class HCA)
  #  >1 NIC        -> 2 QPs  (Thor2 8x8 NIC RDMA-read example, "2 QPs per
  #                           NIC" -- the right shape once aggregate inflight
  #                           is dominated by NIC count rather than per-NIC
  #                           pipelining)
  # Users running a batch-size sweep at fixed small message size on a single
  # NIC should pass an explicit value (e.g. --mori-num-qp-per-transfer 16)
  # to match Thor2's batch-sweep example.
  local resolved_qp
  case "$MORI_NUM_QP_PER_TRANSFER" in
    auto)
      if [[ "$resolved_init" -gt 1 ]]; then
        resolved_qp=2
      elif [[ "$BATCH_SIZE" -gt 1 ]]; then
        resolved_qp=4
      else
        resolved_qp=1
      fi
      ;;
    ''|*[!0-9]*) die "invalid --mori-num-qp-per-transfer '$MORI_NUM_QP_PER_TRANSFER' (expected positive int or 'auto')" ;;
    *) [[ "$MORI_NUM_QP_PER_TRANSFER" -ge 1 ]] || die "--mori-num-qp-per-transfer must be >= 1"
       resolved_qp="$MORI_NUM_QP_PER_TRANSFER" ;;
  esac
  printf '[mori] num-qp-per-transfer = %s (--mori-num-qp-per-transfer=%s, batch=%s, init-dev=%s)\n' \
    "$resolved_qp" "$MORI_NUM_QP_PER_TRANSFER" "$BATCH_SIZE" "$resolved_init"
  if [[ "$BATCH_SIZE" -gt 1 ]] && [[ -z "$batched_flag" ]]; then
    # The bare `run_single_once` path uses contiguous offsets while
    # `_validate_rdma` expects strided offsets; with batch>1 every iter
    # asserts. Warn loudly so the user knows why their run is failing.
    echo "[mori] WARNING: --mori-batched-api=off with --batch-size=${BATCH_SIZE}: mori benchmark.py validate() will fail every iter (strided expected vs contiguous actual). Pass --mori-batched-api=on (or leave =auto)." >&2
  fi

  # Build the part of the command that's identical for every size; the
  # per-size loop only appends `--buffer-size N`.
  local -a BASE_CMD=(
    python3 "$SCRIPT_PATH"
    --backend "$MORI_BACKEND"
    --op-type "$OP_TYPE"
    --transfer-batch-size "$BATCH_SIZE"
    --iters "$ITERS"
  )
  [[ -n "$batched_flag" ]] && BASE_CMD+=("$batched_flag")
  [[ -n "$sess_flag" ]]    && BASE_CMD+=("$sess_flag")
  if [[ "$MORI_BACKEND" == "xgmi" ]]; then
    BASE_CMD+=(--src-gpu 0 --dst-gpu 1)
    [[ "$MORI_XGMI_MULTIPROCESS" == "1" ]] && BASE_CMD+=(--xgmi-multiprocess)
  else
    # --num-qp-per-transfer is RDMA-only; xgmi backend's argparser ignores
    # it, but we keep the conditional explicit so future xgmi additions
    # don't silently inherit an RDMA-tuned flag.
    BASE_CMD+=(
      --host "$mori_host"
      --num-initiator-dev "$resolved_init"
      --num-target-dev "$resolved_target"
      --num-qp-per-transfer "$resolved_qp"
    )
  fi

  local -a size_list
  IFS=',' read -ra size_list <<< "$SIZES"

  # Compute min / max / count of the (cleaned) size list once so both
  # branches below share the same numeric view of --sizes.
  local size min_size="" max_size="" size_count=0
  for size in "${size_list[@]}"; do
    size="${size// /}"
    [[ -z "$size" ]] && continue
    [[ "$size" =~ ^[0-9]+$ ]] || die "non-numeric --sizes entry: '$size'"
    size_count=$((size_count + 1))
    if [[ -z "$min_size" || "$size" -lt "$min_size" ]]; then min_size="$size"; fi
    if [[ -z "$max_size" || "$size" -gt "$max_size" ]]; then max_size="$size"; fi
  done

  # Decide between mori's native --all sweep (one process, multiple sizes)
  # and the legacy per-size loop. See is_pow2_ladder() docstring above and
  # the --mori-use-sweep usage block.
  local sweep_mode=0
  case "$MORI_USE_SWEEP" in
    off) sweep_mode=0 ;;
    on)
      if [[ "$size_count" -lt 2 ]]; then
        echo "[mori] --mori-use-sweep=on but only ${size_count} size(s) given; using per-size invocation" >&2
        sweep_mode=0
      else
        sweep_mode=1
        if ! is_pow2_ladder "$SIZES"; then
          echo "[mori] --mori-use-sweep=on with non-pow2-ladder --sizes='${SIZES}'; mori will emit ALL pow2 points in [${min_size}, ${max_size}], non-pow2 sizes will be skipped" >&2
        fi
      fi
      ;;
    auto)
      if [[ "$size_count" -ge 2 ]] && is_pow2_ladder "$SIZES"; then
        sweep_mode=1
      fi
      ;;
    *) die "invalid --mori-use-sweep '$MORI_USE_SWEEP' (expected auto|on|off)" ;;
  esac

  local succeeded=0 failed=0 last_rc=0 rc
  if [[ "$sweep_mode" -eq 1 ]]; then
    # Single mori process drives the whole pow2 ladder via --all. mori
    # benchmark.py reuses TransferEngine / gloo / NIC handshake / MR regs
    # across sizes, replacing N spawn cycles with one. The per-size
    # output table (parsed by parse_mori_table_line in
    # bench_p2p_compare.py) is identical to what the legacy loop would
    # produce, so report parsers don't need changes.
    local -a CMD=(
      "${BASE_CMD[@]}"
      --all
      --sweep-start-size "$min_size"
      --sweep-max-size "$max_size"
    )
    printf '[mori] sweep mode: 1 invocation for sizes [%s .. %s] via --all (--mori-use-sweep=%s, %d sizes)\n' \
      "$min_size" "$max_size" "$MORI_USE_SWEEP" "$size_count"
    printf '$ '; printf '%q ' "${CMD[@]}"; printf '\n\n'
    rc=0
    ( cd "$MORI_ROOT" && "${CMD[@]}" ) || rc=$?
    if [[ "$rc" -eq 0 ]]; then
      succeeded=1
    else
      failed=1
      last_rc="$rc"
      echo "[mori] WARNING: rank=${RANK} sweep [${min_size}..${max_size}] batch=${BATCH_SIZE} failed (exit ${rc})"
    fi
  else
    printf '[mori] per-size mode: %d invocation(s) (--mori-use-sweep=%s)\n' \
      "$size_count" "$MORI_USE_SWEEP"
    for size in "${size_list[@]}"; do
      size="${size// /}"
      [[ -z "$size" ]] && continue

      local -a CMD=("${BASE_CMD[@]}" --buffer-size "$size")
      printf '$ '; printf '%q ' "${CMD[@]}"; printf '\n\n'

      # Per-size invocation in a subshell so an internal exit (e.g. gloo
      # "Connection reset by peer", validate() AssertionError) does NOT
      # abort the whole sweep. Both ranks process identical size lists so
      # they stay in lock-step on size boundaries.
      rc=0
      ( cd "$MORI_ROOT" && "${CMD[@]}" ) || rc=$?
      if [[ "$rc" -eq 0 ]]; then
        succeeded=$((succeeded + 1))
      else
        failed=$((failed + 1))
        last_rc="$rc"
        echo "[mori] WARNING: rank=${RANK} buffer-size=${size} batch=${BATCH_SIZE} failed (exit ${rc}); continuing with next size"
      fi
    done
  fi

  # Surface a non-zero status only if every size failed, so partial
  # results from larger sizes still count as a successful backend run
  # and the report parser can pick them up.
  if [[ "$succeeded" -eq 0 ]] && [[ "$failed" -gt 0 ]]; then
    echo "[mori] ERROR: rank=${RANK} every size failed (last exit ${last_rc})"
    return "$last_rc"
  fi
  return 0
}

main 2>&1 | tee "$LOG_PATH"
exit "${PIPESTATUS[0]}"
