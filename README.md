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

## Execution Layers

BenchP2P separates orchestration into three layers:

- `scripts/bench_p2p_compare.py`: external CLI. It accepts user parameters,
  invokes the Slurm script, then parses logs and generates reports.
- `scripts/slurm_run_p2p.sh`: Slurm layer. It performs a single `srun`
  allocation, starts the runtime container on each task, and forwards one
  command into the container.
- `scripts/container_prepare_thirdparty.py`: container preparation layer. Rank 0
  builds third-party wheels inside the runtime container and all other ranks
  wait on the per-job build marker.
- `scripts/container_run_p2p.py`: container benchmark layer. It calls the
  container preparation script when enabled, installs wheels from
  `3rdparty/wheelhouse/*/*.whl`, then runs MORI, Mooncake, UCCL, and NIXL
  sequentially under the same Slurm allocation.

## Official Benchmark Mapping

BenchP2P keeps the workload comparable by using the same message sizes,
operation (`--op-type read|write`), batch size (`--num-blocks` or the matching
backend batch flag), one initiator device, and one target device. The final
report normalizes each backend's native output into `GB/s`, `Gbps`, and
`latency_us`.

| Backend | Official benchmark entry | Native metrics parsed |
| --- | --- | --- |
| UCCL | `3rdparty/uccl/p2p/benchmarks/benchmark_uccl.py` | backend log lines with `GB/s` and latency |
| MORI | `3rdparty/mori/tests/python/io/benchmark.py` | MORI table: `Avg Bw (GB/s)`, `Avg Lat (us)` |
| Mooncake | official `tebench` from `mooncake-transfer-engine/benchmark` | `BW (GB/S)`, `Avg Lat (us)` |
| NIXL | official `nixlbench` from `benchmark/nixlbench` | `B/W (GB/Sec)`, `Avg Lat. (us)` |

NIXLBench uses ETCD for multi-process coordination. By default rank 0 starts a
local ETCD server and both Slurm tasks connect to `http://MASTER_ADDR:2379`.
Use `--no-nixl-start-etcd --nixl-etcd-endpoints <url>` when your container or
cluster provides ETCD separately. Mooncake uses `tebench` in TENT/RDMA mode by
default; pass `--mooncake-bench-bin <path>` if the binary is not on `PATH`.

## Quick start

Preview the commands without running GPU/RDMA workloads:

```bash
python3 scripts/bench_p2p_compare.py --dry-run
```

For Slurm container runs, the default is to clone/update third-party stacks and
build wheels inside `docker.io/rocm/primus:v26.2`, not on the submission host.
Use this host-side command only for local development or to pre-populate
`3rdparty/`:

```bash
python3 scripts/prepare_thirdparty.py
```

Run all four default backends. In Slurm container mode,
`scripts/container_prepare_thirdparty.py` runs first inside the container. It
uses `scripts/prepare_thirdparty.py --container-build` on rank 0, writes wheels
into `3rdparty/wheelhouse/<backend>/`, and the other ranks wait for the build
marker before installing the wheels in their own container process:

```bash
python3 scripts/bench_p2p_compare.py \
  --launcher slurm \
  --sizes 256,1K,4K,64K,1M,16M,100M \
  --iters 10 \
  --device gpu
```

The default launcher for real P2P is Slurm with two tasks across two nodes.
The external CLI calls one Slurm script, and that script wraps each task with
`docker run docker.io/rocm/primus:v26.2`. Inside the container,
`container_prepare_thirdparty.py` builds wheels and `container_run_p2p.py`
installs them before running every selected backend:

```bash
python3 scripts/bench_p2p_compare.py \
  --launcher slurm \
  --slurm-container-runtime docker \
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

You can call the Slurm layer directly for debugging:

```bash
bash scripts/slurm_run_p2p.sh \
  --backends mori,mooncake,uccl,nixl \
  --sizes 256,1K,1M \
  --iters 10 \
  --slurm-gres gpu:1
```

In Slurm container mode, `scripts/container_prepare_thirdparty.py` runs inside
the runtime container before benchmark execution. It serializes rank 0's build
with a wheelhouse lock, writes a per-job completion marker, and all ranks then
run:

```bash
python3 -m pip install --force-reinstall --no-deps 3rdparty/wheelhouse/*/*.whl
```

inside `docker.io/rocm/primus:v26.2` before launching the test. Pass
`--no-prepare-thirdparty-in-container` to reuse an existing wheelhouse,
`--prepare-thirdparty-skip-clone` to rebuild from existing checkouts, or
`--prepare-thirdparty-timeout` for long builds. The default container runtime
is `docker`, so no Pyxis-specific `srun --container-image` option is emitted.
Use `--slurm-container-runtime pyxis` only on clusters that support
Pyxis/Enroot, or `--slurm-container-runtime none` to run directly on the Slurm
allocation. Use `--slurm-container-mounts` for extra mounts, `--container-python`
if the image uses a non-default Python, `--runtime-wheelhouse` to point at a
different wheel cache, or `--skip-runtime-wheel-install` to disable runtime
wheel installation.

The generated Docker command follows the ROCm/RDMA container pattern:
`--ipc=host`, `--network=host`, `--device=/dev/kfd`, `--device=/dev/dri`,
`--device=/dev/infiniband`, `--cap-add=SYS_PTRACE`,
`--cap-add=CAP_SYS_ADMIN`, `--security-opt seccomp=unconfined`,
`--group-add video`, and `--privileged`. Unlike a long-running dev container,
BenchP2P does not use `-d ... sleep infinity`; each Slurm task runs the
benchmark in the foreground so `srun` captures logs and exit status. Pass
`--docker-mount-home` to add `-v $HOME:/root/home`, and
`--docker-extra-args` for site-specific Docker options.

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
`--wheelhouse <path>` / `--manifest <path>` to point at a different wheel cache.
In Slurm container mode, `--manifest`, `--source-root`, and
`--runtime-wheelhouse` are forwarded into the container-side build. Use
`--source-root`, `--uccl-script`, `--mori-script`,
`--nixlbench-bin`, or `--mooncake-bench-bin` if the benchmark entries live
elsewhere.

## Notes

- Use `--launcher local` only for dry-run, log parsing, or single-node smoke
  checks. Cross-node P2P performance should use `--launcher slurm`.
- Slurm container mode defaults to `docker run` inside each Slurm task. If your
  cluster supports Pyxis/Enroot, pass `--slurm-container-runtime pyxis`.
- UCCL and MORI are launched as two Slurm tasks with PyTorch distributed
  environment variables derived from `SLURM_PROCID` and `SLURM_NTASKS`.
- NIXL is measured with the NIXL repository's official `nixlbench`, not the
  UCCL compatibility benchmark.
- Mooncake is measured with the Mooncake repository's official `tebench`, not
  the NIXL backend wrapper.
- MORI defaults to RDMA mode with two Slurm tasks. Use
  `--mori-backend xgmi` for single-node GPU-to-GPU XGMI testing.
- MORI's published IO examples often use batched transfers. This harness
  defaults to `--mori-transfer-batch-size 1` for per-transfer comparison; set it
  to `128` if you want to mirror that MORI benchmark style.