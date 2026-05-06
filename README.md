# BenchP2P

BenchP2P provides a unified harness for comparing point-to-point bandwidth and
latency across common P2P stacks:

- MORI: `https://github.com/ROCm/mori.git`
- Mooncake: `https://github.com/kvcache-ai/Mooncake.git`
- UCCL: `https://github.com/uccl-project/uccl.git`
- NIXL: `https://github.com/ai-dynamo/nixl.git`

The harness clones these public repositories into `3rdparty/`, builds and
installs their wheels before a real benchmark run, keeps each backend's native
benchmark path intact, captures logs, parses bandwidth/latency results, and
emits:

- `p2p_results.csv`: per-size measurements
- `p2p_summary.csv`: best bandwidth and best latency per backend
- `p2p_results.md`: Markdown table
- `p2p_comparison.svg`: bandwidth and latency chart
- `logs/`: raw backend logs

## Quick start

Preview the commands without running GPU/RDMA workloads:

```bash
python3 scripts/bench_p2p_compare.py --dry-run
```

Prepare only: clone/update `3rdparty/`, build wheels, and install them into the
active Python environment:

```bash
python3 scripts/prepare_thirdparty.py
```

Run all four default backends. This automatically runs
`scripts/prepare_thirdparty.py` first:

```bash
python3 scripts/bench_p2p_compare.py \
  --sizes 256,1K,4K,64K,1M,16M,100M \
  --iters 10 \
  --device gpu
```

Parse existing logs instead of launching benchmarks:

```bash
python3 scripts/bench_p2p_compare.py \
  --from-log mori=/path/to/mori.log \
  --from-log mooncake=/path/to/mooncake.log \
  --from-log uccl=/path/to/uccl.log \
  --from-log nixl=/path/to/nixl.log
```

By default the script uses `BenchP2P/3rdparty` as the source root. Use
`--skip-prepare-thirdparty` to reuse an already prepared environment, or use
`--source-root`, `--uccl-script`, `--nixl-script`, `--mooncake-script`, or
`--mori-script` if the benchmark scripts live elsewhere.

## Notes

- UCCL is launched with `torchrun --standalone --nproc-per-node=2`.
- NIXL and Mooncake are launched as local server/client pairs. Set
  `--server-ip` if the client should connect to a non-loopback address.
- Mooncake is measured through the NIXL benchmark with `--backend mooncake`.
- NIXL is measured through the UCCL repo's `benchmark_nixl.py` with
  `--backend ucx`.
- MORI defaults to RDMA mode with a local two-rank `torchrun`. Use
  `--mori-backend xgmi` for single-node GPU-to-GPU XGMI testing.
- MORI's published IO examples often use batched transfers. This harness
  defaults to `--mori-transfer-batch-size 1` for per-transfer comparison; set it
  to `128` if you want to mirror that MORI benchmark style.