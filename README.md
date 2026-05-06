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

Prepare third-party stacks: clone/update `3rdparty/`, build wheels into
`3rdparty/wheelhouse/<backend>/`, and install them into the active Python
environment. **You must run this once (or whenever versions change) before
`bench_p2p_compare.py` is invoked**, because the bench script will only
*install* pre-built wheels, never build them:

```bash
python3 scripts/prepare_thirdparty.py
```

Run all four default backends. The bench script picks up the wheels from
`3rdparty/wheelhouse/` and `pip install`s them into the current environment
before running benchmarks; if any required wheel is missing it stops with an
error pointing back at `prepare_thirdparty.py`:

```bash
python3 scripts/bench_p2p_compare.py \
  --launcher slurm \
  --sizes 256,1K,4K,64K,1M,16M,100M \
  --iters 10 \
  --device gpu
```

The default launcher for real P2P is Slurm with two tasks across two nodes.
Slurm runs the benchmark tasks inside `docker.io/rocm/primus:v26.2`, installs
the wheels from the run's `wheelhouse/` inside that runtime container, and then
starts the backend benchmark:

```bash
python3 scripts/bench_p2p_compare.py \
  --launcher slurm \
  --slurm-container-image docker.io/rocm/primus:v26.2 \
  --slurm-nodes 2 \
  --slurm-ntasks 2 \
  --slurm-ntasks-per-node 1 \
  --slurm-gres gpu:1 \
  --sizes 256,1K,1M,16M \
  --iters 10
```

Add scheduler-specific options with `--slurm-partition`, `--slurm-account`,
`--slurm-qos`, `--slurm-time`, `--slurm-constraint`, or
`--slurm-extra-args`. In Slurm mode the script uses the first allocated
hostname as `MASTER_ADDR`; rank 0 acts as server/initiator and rank 1 acts as
client/target where the backend needs explicit roles.

In Slurm container mode, `scripts/prepare_thirdparty.py` builds wheels into
`3rdparty/wheelhouse/<backend>/` ahead of time. The Slurm task preamble runs:

```bash
python3 -m pip install --force-reinstall --no-deps 3rdparty/wheelhouse/*/*.whl
```

inside `docker.io/rocm/primus:v26.2` before launching the test. The script
auto-adds Pyxis-style `--container-image`, `--container-workdir`, and
`--container-mounts` options. Use `--slurm-container-mounts` for extra mounts,
`--container-python` if the image uses a non-default Python,
`--runtime-wheelhouse` to point at a different wheel cache, or
`--skip-runtime-wheel-install` to disable runtime wheel installation.

Parse existing logs instead of launching benchmarks:

```bash
python3 scripts/bench_p2p_compare.py \
  --from-log mori=/path/to/mori.log \
  --from-log mooncake=/path/to/mooncake.log \
  --from-log uccl=/path/to/uccl.log \
  --from-log nixl=/path/to/nixl.log
```

By default the script uses `BenchP2P/3rdparty` as the source root and reads
wheels from `BenchP2P/3rdparty/wheelhouse/<backend>/`. Use
`--skip-install-wheels` to reuse an already-installed environment, or
`--wheelhouse <path>` / `--manifest <path>` to point at a different wheel
cache. Use `--source-root`, `--uccl-script`, `--nixl-script`,
`--mooncake-script`, or `--mori-script` if the benchmark scripts live
elsewhere.

## Notes

- Use `--launcher local` only for dry-run, log parsing, or single-node smoke
  checks. Cross-node P2P performance should use `--launcher slurm`.
- Slurm container mode assumes the cluster supports Pyxis/Enroot-style
  `srun --container-image` options and that the output directory is on storage
  visible to both allocated nodes.
- UCCL is launched as two Slurm tasks with PyTorch distributed environment
  variables derived from `SLURM_PROCID` and `SLURM_NTASKS`.
- NIXL and Mooncake are launched as one Slurm job where rank 0 runs the server
  and rank 1 runs the client.
- Mooncake is measured through the NIXL benchmark with `--backend mooncake`.
- NIXL is measured through the UCCL repo's `benchmark_nixl.py` with
  `--backend ucx`.
- MORI defaults to RDMA mode with two Slurm tasks. Use
  `--mori-backend xgmi` for single-node GPU-to-GPU XGMI testing.
- MORI's published IO examples often use batched transfers. This harness
  defaults to `--mori-transfer-batch-size 1` for per-transfer comparison; set it
  to `128` if you want to mirror that MORI benchmark style.