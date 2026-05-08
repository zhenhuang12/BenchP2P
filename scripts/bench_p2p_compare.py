#!/usr/bin/env python3
"""Run per-rank P2P benchmarks (`run`) or aggregate logs into reports (`report`).

This is the only Python entry point left in BenchP2P. Container and Slurm
launchers (``container_bench_p2p.sh`` / ``slurm_bench_p2p.sh``) delegate to
this script for the actual work:

- ``run``   - executed inside the runtime container, once per rank. It
              installs the wheelhouse (unless ``--skip-wheel-install``) and
              invokes each backend's *official* benchmark, capturing its
              stdout/stderr to ``<output-dir>/logs/<backend>_rank<N>.log``.
- ``report``- scans ``<output-dir>/logs/`` after the run, parses metrics,
              and emits CSV/Markdown/PNG/JSON summaries.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import json
import math
import os
import re
import select
import shlex
import shutil
import socket
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_THIRDPARTY_DIR = REPO_ROOT / "3rdparty"
DEFAULT_WHEELHOUSE = DEFAULT_THIRDPARTY_DIR / "wheelhouse"
DEFAULT_MANIFEST = DEFAULT_THIRDPARTY_DIR / "manifest.json"

DEFAULT_SIZES = [256, 1024, 4096, 16384, 65536, 262144, 1048576, 10485760, 16777216, 104857600]
DEFAULT_BACKENDS = ["mori", "mooncake", "uccl", "nixl"]

LOG_LINE_RE = re.compile(
    r"\[(?P<role>[^\]]+)\]\s+"
    r"(?:(?P<mode>DUAL-WRITE)\s+)?"
    r"(?P<size>\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB))\s*:\s*"
    r"(?P<gbps>[0-9.+-eE]+)\s*Gbps\s*\|\s*"
    r"(?P<gb_s>[0-9.+-eE]+)\s*GB/s\s*\|\s*"
    r"(?P<lat_s>[0-9.+-eE]+)\s*s"
)


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #


def parse_size(value: str) -> int:
    text = value.strip().replace(" ", "")
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KkMmGgTt]?)(?:[Bb])?", text)
    if not match:
        raise argparse.ArgumentTypeError(f"bad size: {value}")
    number = float(match.group(1))
    unit = match.group(2).upper()
    multiplier = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}[unit]
    return int(number * multiplier)


def parse_size_list(value: str) -> list[int]:
    sizes = [parse_size(item) for item in value.split(",") if item.strip()]
    if not sizes:
        raise argparse.ArgumentTypeError("size list cannot be empty")
    return sizes


def human_size(size: int) -> str:
    value = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def shell_join(command: Sequence[str]) -> str:
    return shlex.join(str(part) for part in command)


def memory_type(device: str) -> str:
    return "VRAM" if device == "gpu" else "DRAM"


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def rank_world_local() -> tuple[int, int, int]:
    rank = env_int("SLURM_PROCID", env_int("RANK", 0))
    world = env_int("SLURM_NTASKS", env_int("WORLD_SIZE", 1))
    local = env_int("SLURM_LOCALID", env_int("LOCAL_RANK", 0))
    return rank, world, local


# --------------------------------------------------------------------------- #
# `run` subcommand
# --------------------------------------------------------------------------- #


@dataclasses.dataclass
class RunArtifact:
    backend: str
    rank: int
    log_path: Path
    status: str
    exit_code: int | None
    elapsed_s: float
    error: str = ""


def install_wheels(wheelhouse: Path) -> None:
    wheels = sorted(str(p) for p in wheelhouse.glob("*/*.whl"))
    if not wheels:
        raise SystemExit(f"No wheel files found under {wheelhouse}")
    print("Installing BenchP2P wheelhouse:", flush=True)
    for wheel in wheels:
        print(f"  {wheel}", flush=True)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--force-reinstall", "--no-deps", *wheels],
        check=True,
    )


def list_ib_devices() -> list[str]:
    """Names of every IB/RoCE HCA visible via /sys/class/infiniband/."""
    p = Path("/sys/class/infiniband")
    if not p.exists():
        return []
    return sorted(d.name for d in p.iterdir() if d.is_dir())


def hca_env_for_backend(backend: str, spec: str) -> dict[str, str]:
    """Translate NCCL-style ``--ib-hca`` spec into per-backend env vars.

    NCCL syntax: ``^a,b`` excludes a/b, otherwise the list is a whitelist.

    - NCCL/RCCL ``NCCL_IB_HCA``: native `^` syntax, pass-through.
    - MORI ``MORI_RDMA_DEVICES``: native `^` syntax, pass-through.
    - UCCL ``UCCL_P2P_RDMA_DEV``: whitelist-only; for ``^a,b`` we expand to
      every device under /sys/class/infiniband except {a, b}.
    """
    spec = (spec or "").strip()
    if not spec:
        return {}
    out: dict[str, str] = {}
    if backend in ("mori", "uccl"):
        # Always set NCCL_IB_HCA too (RCCL plugins under torch.distributed
        # in mori/uccl can pick it up).
        out["NCCL_IB_HCA"] = spec
    if backend == "mori":
        out["MORI_RDMA_DEVICES"] = spec
    elif backend == "uccl":
        if spec.startswith("^"):
            excluded = {x.strip() for x in spec[1:].split(",") if x.strip()}
            available = list_ib_devices()
            kept = [d for d in available if d not in excluded]
            if kept:
                out["UCCL_P2P_RDMA_DEV"] = ",".join(kept)
            else:
                print(
                    f"[uccl] WARNING: --ib-hca={spec!r} excludes every HCA "
                    f"({available}); leaving UCCL_P2P_RDMA_DEV unset",
                    flush=True,
                )
        else:
            out["UCCL_P2P_RDMA_DEV"] = spec
    return out


def install_runtime_pip_packages(packages_csv: str) -> None:
    """Install pure-python runtime deps that aren't packaged as wheels (e.g. mori needs prettytable)."""
    packages = [item.strip() for item in packages_csv.split(",") if item.strip()]
    if not packages:
        return
    print(f"Installing runtime pip packages: {', '.join(packages)}", flush=True)
    completed = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", *packages],
        check=False,
    )
    if completed.returncode != 0:
        print(
            f"warning: runtime pip install exited {completed.returncode}; "
            "continuing (backends may fail with ImportError later).",
            flush=True,
        )


def open_log(log_path: Path) -> "tuple[Path, object]":
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return log_path, log_path.open("w", encoding="utf-8")


def stream_subprocess(
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    log_file,
) -> int:
    log_file.write(f"$ {shell_join(command)}\n\n")
    log_file.flush()
    proc = subprocess.Popen(
        command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
        log_file.write(line)
    proc.wait()
    return proc.returncode


def terminate_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def require_executable(name: str, candidates: Sequence[Path] = ()) -> str:
    path = Path(name)
    if path.is_absolute() or len(path.parts) > 1:
        if path.exists():
            return str(path)
    found = shutil.which(name)
    if found:
        return found
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    searched = ", ".join(str(item) for item in candidates) or "PATH"
    raise SystemExit(f"benchmark executable not found: {name} (searched {searched})")


def backend_log_path(args: argparse.Namespace, backend: str, rank: int) -> Path:
    return args.output_dir / "logs" / f"{backend}_rank{rank}.log"


def backend_script_paths(args: argparse.Namespace) -> dict[str, Path]:
    src = args.source_root
    return {
        "uccl": Path(args.uccl_script) if args.uccl_script
                else src / "uccl" / "p2p" / "benchmarks" / "benchmark_uccl.py",
        "mori": Path(args.mori_script) if args.mori_script
                else src / "mori" / "tests" / "python" / "io" / "benchmark.py",
        "mooncake": Path(args.mooncake_script) if args.mooncake_script else None,
        "nixl": Path(args.nixl_script) if args.nixl_script else None,
    }


def run_uccl(args, env, src) -> int:
    script = backend_script_paths(args)["uccl"]
    if not script.exists():
        raise SystemExit(f"benchmark script not found: {script}")
    env = dict(env)
    for k, v in hca_env_for_backend("uccl", args.ib_hca).items():
        env[k] = v
        print(f"[uccl] env: {k}={v}", flush=True)
    # uccl benchmark_uccl.py demands "comma-separated integers" for --sizes;
    # the rest of BenchP2P uses human-readable suffixes like "1K"/"1M" and
    # the sweep mode (--size-min/--size-max) emits a list that may include
    # them. Normalise to integer bytes here.
    sizes_int = ",".join(str(parse_size(s)) for s in args.sizes.split(",") if s.strip())
    command = [
        sys.executable, str(script),
        "--sizes", sizes_int,
        "--iters", str(args.iters),
        "--device", args.device,
        "--local-gpu-idx", env["LOCAL_RANK"],
        "--num-kvblocks", str(args.num_blocks),
    ]
    if args.async_api:
        command.append("--async-api")
    log_path, log_file = open_log(backend_log_path(args, "uccl", int(env["RANK"])))
    try:
        rc = stream_subprocess(command, src, env, log_file)
    finally:
        log_file.close()
    if rc != 0:
        raise subprocess.CalledProcessError(rc, command)
    return rc


def run_mori(args, env, src) -> None:
    script = backend_script_paths(args)["mori"]
    if not script.exists():
        raise SystemExit(f"benchmark script not found: {script}")

    # mori benchmark.py uses ``from tests.python.utils import ...``; root the
    # process at the mori repo and put it on PYTHONPATH so that resolves.
    mori_root = src / "mori"
    if not (mori_root / "tests" / "python" / "utils.py").is_file():
        try:
            candidate = script.resolve().parents[3]
            if (candidate / "tests" / "python" / "utils.py").is_file():
                mori_root = candidate
        except IndexError:
            pass
    mori_env = dict(env)
    existing_pp = mori_env.get("PYTHONPATH", "")
    mori_env["PYTHONPATH"] = (
        f"{mori_root}{os.pathsep}{existing_pp}" if existing_pp else str(mori_root)
    )
    for k, v in hca_env_for_backend("mori", args.ib_hca).items():
        mori_env[k] = v
        print(f"[mori] env: {k}={v}", flush=True)

    rank = int(env["RANK"])
    # mori RDMA's --host is each engine's *local* TCP listen/bind address
    # (see mori/src/application/transport/tcp/tcp.cpp::Listen). Peers exchange
    # the resulting handle{host,port} through gloo (torch.distributed bound
    # to MASTER_ADDR:MASTER_PORT) inside _initialize_rdma, so we do NOT need
    # to pre-share the target's IP. Each rank just needs to bind to one of
    # its own NIC IPv4s; rank 0 has MASTER_ADDR by definition (slurm head
    # node), other ranks resolve their own hostname.
    if args.mori_backend == "rdma":
        if rank == 0:
            mori_host = mori_env["MASTER_ADDR"]
        else:
            try:
                mori_host = socket.gethostbyname(socket.gethostname())
            except OSError:
                mori_host = mori_env["MASTER_ADDR"]
        print(f"[mori] rank={rank} binding --host={mori_host}", flush=True)
    else:
        mori_host = mori_env["MASTER_ADDR"]

    log_path, log_file = open_log(backend_log_path(args, "mori", rank))
    try:
        for size in args.sizes.split(","):
            # mori benchmark.py wants integer bytes for --buffer-size; the
            # rest of BenchP2P uses human-readable suffixes like "1K"/"1M".
            size_bytes = parse_size(size.strip())
            command = [
                sys.executable, str(script),
                "--backend", args.mori_backend,
                "--op-type", args.op_type,
                "--buffer-size", str(size_bytes),
                "--transfer-batch-size", str(args.mori_transfer_batch_size),
                "--iters", str(args.iters),
            ]
            if args.mori_backend == "xgmi":
                command.extend(["--src-gpu", "0", "--dst-gpu", "1"])
                if args.mori_xgmi_multiprocess:
                    command.append("--xgmi-multiprocess")
            else:
                command.extend([
                    "--host", mori_host,
                    "--num-initiator-dev", "1",
                    "--num-target-dev", "1",
                ])
            rc = stream_subprocess(command, mori_root, mori_env, log_file)
            if rc != 0:
                raise subprocess.CalledProcessError(rc, command)
    finally:
        log_file.close()


def start_etcd_if_needed(args, env) -> subprocess.Popen | None:
    if not args.nixl_start_etcd or int(env["RANK"]) != 0:
        return None
    etcd = shutil.which("etcd")
    if etcd is None:
        raise SystemExit(
            "nixlbench requires ETCD coordination. Install 'etcd' in the runtime "
            "container or pass --no-nixl-start-etcd with --nixl-etcd-endpoints."
        )
    master = env["MASTER_ADDR"]
    data_dir = f"/tmp/benchp2p-etcd-{os.getpid()}"
    command = [
        etcd, "--data-dir", data_dir,
        "--listen-client-urls", "http://0.0.0.0:2379",
        "--advertise-client-urls", f"http://{master}:2379",
        "--listen-peer-urls", "http://0.0.0.0:2380",
        "--initial-advertise-peer-urls", f"http://{master}:2380",
        "--initial-cluster", f"default=http://{master}:2380",
        "--log-level", "error",
    ]
    print("+ " + shell_join(command), flush=True)
    proc = subprocess.Popen(command)
    time.sleep(2.0)
    return proc


def run_nixlbench(args, env, src) -> None:
    rank = int(env["RANK"])
    if rank > 1:
        print(f"[nixl] skipping unused rank {rank}", flush=True)
        return
    if rank == 1 and args.pair_startup_seconds > 0:
        time.sleep(args.pair_startup_seconds)
    binary = require_executable(
        args.nixlbench_bin,
        [
            src / "nixl" / "benchmark" / "nixlbench" / "build" / "nixlbench",
            src / "nixl" / "benchmark" / "nixlbench" / "build" / "src" / "nixlbench",
        ],
    )
    endpoints = args.nixl_etcd_endpoints or f"http://{env['MASTER_ADDR']}:2379"
    seg_type = memory_type(args.device)
    log_path, log_file = open_log(backend_log_path(args, "nixl", rank))
    etcd_proc = start_etcd_if_needed(args, env)
    try:
        for size in [s.strip() for s in args.sizes.split(",") if s.strip()]:
            command = [
                binary,
                "--etcd_endpoints", endpoints,
                "--backend", args.nixl_backend,
                "--initiator_seg_type", seg_type,
                "--target_seg_type", seg_type,
                "--scheme", "pairwise",
                "--op_type", args.op_type.upper(),
                "--total_buffer_size", str(max(int(size) * args.num_blocks * 4, 1 << 30)),
                "--start_block_size", size,
                "--max_block_size", size,
                "--start_batch_size", str(args.num_blocks),
                "--max_batch_size", str(args.num_blocks),
                "--num_iter", str(args.iters),
                "--warmup_iter", str(max(1, min(100, args.iters))),
                "--num_threads", "1",
                "--num_initiator_dev", "1",
                "--num_target_dev", "1",
            ]
            if args.nixl_device_list:
                command.extend(["--device_list", args.nixl_device_list])
            rc = stream_subprocess(command, src, env, log_file)
            if rc != 0:
                raise subprocess.CalledProcessError(rc, command)
    finally:
        log_file.close()
        if etcd_proc is not None:
            terminate_process(etcd_proc)


def mooncake_segment_file(src: Path, env: dict[str, str], size: str) -> Path:
    job = env.get("SLURM_JOB_ID", str(os.getppid()))
    runtime_dir = src / ".benchp2p_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir / f"mooncake_target_seg_{job}_{size}.txt"


def mooncake_base_command(binary: str, args, env, size) -> list[str]:
    seg_type = memory_type(args.device)
    command = [
        binary,
        "--seg_type", seg_type,
        "--backend", args.mooncake_backend,
        "--op_type", args.op_type,
        "--total_buffer_size", str(max(int(size) * args.num_blocks * 4, 1 << 30)),
        "--start_block_size", size,
        "--max_block_size", size,
        "--start_batch_size", str(args.num_blocks),
        "--max_batch_size", str(args.num_blocks),
        "--start_num_threads", "1",
        "--max_num_threads", "1",
        "--duration", str(args.mooncake_duration),
        "--local_gpu_id", env["LOCAL_RANK"],
        "--target_gpu_id", "0",
    ]
    if args.mooncake_backend == "tent":
        command.extend(["--metadata_type", "p2p"])
        if args.mooncake_xport_type:
            command.extend(["--xport_type", args.mooncake_xport_type])
    return command


def run_mooncake_tebench(args, env, src) -> None:
    rank = int(env["RANK"])
    if rank > 1:
        print(f"[mooncake] skipping unused rank {rank}", flush=True)
        return
    binary = require_executable(
        args.mooncake_bench_bin,
        [
            src / "Mooncake" / "build" / "mooncake-transfer-engine" / "benchmark" / "tebench",
            src / "Mooncake" / "build" / "tebench",
        ],
    )
    log_path, log_file = open_log(backend_log_path(args, "mooncake", rank))
    try:
        for size in [s.strip() for s in args.sizes.split(",") if s.strip()]:
            segment_file = mooncake_segment_file(src, env, size)
            command = mooncake_base_command(binary, args, env, size)
            if rank == 0:
                try:
                    segment_file.unlink()
                except FileNotFoundError:
                    pass
                log_file.write(f"$ {shell_join(command)}\n\n"); log_file.flush()
                print("+ " + shell_join(command), flush=True)
                target = subprocess.Popen(
                    command, cwd=src, env=env,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                )
                deadline = time.monotonic() + args.pair_startup_seconds + 20
                target_seg_name: str | None = None
                assert target.stdout is not None
                while time.monotonic() < deadline and target.poll() is None:
                    ready, _, _ = select.select([target.stdout], [], [], 0.5)
                    if not ready:
                        continue
                    line = target.stdout.readline()
                    if not line:
                        continue
                    sys.stdout.write(line); log_file.write(line)
                    match = re.search(r"--target_seg_name=([^\s]+)", line)
                    if match:
                        target_seg_name = match.group(1)
                        segment_file.write_text(target_seg_name + "\n", encoding="utf-8")
                        break
                if target_seg_name is None:
                    terminate_process(target)
                    raise SystemExit("Mooncake tebench target did not print --target_seg_name")
                # Continue draining target stdout while client runs.
                drain_deadline = time.monotonic() + args.pair_startup_seconds + args.mooncake_duration + 10
                while time.monotonic() < drain_deadline and target.poll() is None:
                    ready, _, _ = select.select([target.stdout], [], [], 0.5)
                    if not ready:
                        continue
                    chunk = target.stdout.readline()
                    if not chunk:
                        break
                    sys.stdout.write(chunk); log_file.write(chunk)
                terminate_process(target)
            else:
                deadline = time.monotonic() + args.pair_startup_seconds + 20
                while not segment_file.exists() and time.monotonic() < deadline:
                    time.sleep(0.5)
                if not segment_file.exists():
                    raise SystemExit(f"Mooncake target segment file not found: {segment_file}")
                target_seg_name = segment_file.read_text(encoding="utf-8").strip()
                command.extend(["--target_seg_name", target_seg_name])
                rc = stream_subprocess(command, src, env, log_file)
                if rc != 0:
                    raise subprocess.CalledProcessError(rc, command)
    finally:
        log_file.close()


def cmd_run(args: argparse.Namespace) -> int:
    args.source_root = Path(args.source_root).resolve()
    args.wheelhouse = Path(args.wheelhouse).resolve()
    args.output_dir = Path(args.output_dir).resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "logs").mkdir(parents=True, exist_ok=True)

    # ib_write_bw -a style power-of-two sweep: when the user gives
    # --size-min/--size-max, build [min, factor*min, factor^2*min, ..., max]
    # and override --sizes. The sweep list is what every backend ends up
    # iterating over (we already loop one size per invocation).
    if args.size_min or args.size_max:
        if not (args.size_min and args.size_max):
            raise SystemExit("--size-min and --size-max must be set together")
        if args.size_step_factor < 2:
            raise SystemExit("--size-step-factor must be >= 2")
        lo = parse_size(args.size_min)
        hi = parse_size(args.size_max)
        if lo <= 0 or hi < lo:
            raise SystemExit(f"invalid sweep range: {lo} .. {hi}")
        sweep: list[int] = []
        s = lo
        while s <= hi:
            sweep.append(s)
            s *= args.size_step_factor
        if not sweep:
            raise SystemExit(f"empty sweep range: {lo} .. {hi}")
        args.sizes = ",".join(str(x) for x in sweep)
        print(
            f"BenchP2P sweep: {len(sweep)} sizes from {human_size(lo)} to "
            f"{human_size(sweep[-1])} (factor x{args.size_step_factor})",
            flush=True,
        )

    rank, world, local = rank_world_local()
    env = os.environ.copy()
    env["RANK"] = str(rank)
    env["WORLD_SIZE"] = str(world)
    env["LOCAL_RANK"] = str(local)
    env.setdefault("LOCAL_WORLD_SIZE", os.environ.get("SLURM_NTASKS_PER_NODE", "1"))
    env.setdefault("MASTER_PORT", "29500")
    env.setdefault("MASTER_ADDR", env.get("MASTER_ADDR") or os.environ.get("HOSTNAME", "127.0.0.1"))

    print(
        f"BenchP2P run: rank={rank} world={world} local_rank={local} "
        f"master={env['MASTER_ADDR']}:{env['MASTER_PORT']}",
        flush=True,
    )

    if not args.skip_wheel_install:
        install_wheels(args.wheelhouse)
    install_runtime_pip_packages(args.runtime_pip_packages)

    backends = [b.strip().lower() for b in args.backends.split(",") if b.strip()]
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    src = args.source_root

    for backend in backends:
        print(f"\n==> [{backend}] starting (rank {rank}/{world})", flush=True)
        try:
            if backend == "uccl":
                run_uccl(args, env, src)
            elif backend == "mori":
                run_mori(args, env, src)
            elif backend == "nixl":
                run_nixlbench(args, env, src)
            elif backend == "mooncake":
                run_mooncake_tebench(args, env, src)
            else:
                raise RuntimeError(f"unknown backend: {backend}")
        except (subprocess.CalledProcessError, RuntimeError, OSError, SystemExit) as exc:
            reason = (
                f"exit {exc.returncode}" if isinstance(exc, subprocess.CalledProcessError)
                else str(exc)
            )
            failed.append((backend, reason))
            print(f"!! {backend} failed ({reason}); continuing", flush=True)
        else:
            succeeded.append(backend)

    print(f"\n==> rank {rank} backend summary", flush=True)
    for b in succeeded:
        print(f"  ok    {b}", flush=True)
    for b, reason in failed:
        print(f"  FAIL  {b}: {reason}", flush=True)
    if failed and not succeeded:
        return 1
    return 0


# --------------------------------------------------------------------------- #
# `report` subcommand
# --------------------------------------------------------------------------- #


@dataclasses.dataclass(frozen=True)
class Metric:
    backend: str
    size_bytes: int
    gbps: float
    gb_s: float
    latency_us: float
    role: str = ""
    batch_size: int = 1
    operation: str = "write"
    source: str = ""
    raw_line: str = ""


def parse_metrics(backend: str, text: str, source: str) -> list[Metric]:
    metrics: list[Metric] = []
    for line in text.splitlines():
        match = LOG_LINE_RE.search(line)
        if match:
            metrics.append(Metric(
                backend=backend,
                size_bytes=parse_size(match.group("size")),
                gbps=float(match.group("gbps")),
                gb_s=float(match.group("gb_s")),
                latency_us=float(match.group("lat_s")) * 1_000_000,
                role=match.group("role"),
                source=source,
                raw_line=line.strip(),
            ))
            continue
        if backend == "mori":
            m = parse_mori_table_line(backend, line, source)
            if m is not None:
                metrics.append(m)
        elif backend in {"mooncake", "nixl"}:
            m = parse_official_table_line(backend, line, source)
            if m is not None:
                metrics.append(m)
    return metrics


def parse_mori_table_line(backend: str, line: str, source: str) -> Metric | None:
    if not line.lstrip().startswith("|"):
        return None
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    if len(cells) != 7 or not cells[0].isdigit():
        return None
    try:
        size = int(cells[0]); batch = int(cells[1])
        gb_s = float(cells[4]); latency_us = float(cells[6])
    except ValueError:
        return None
    return Metric(
        backend=backend, size_bytes=size, gbps=gb_s * 8, gb_s=gb_s, latency_us=latency_us,
        role="initiator", batch_size=batch, source=source, raw_line=line.strip(),
    )


def parse_official_table_line(backend: str, line: str, source: str) -> Metric | None:
    fields = line.strip().split()
    if len(fields) < 4 or not fields[0].isdigit() or not fields[1].isdigit():
        return None
    try:
        size = int(fields[0]); batch = int(fields[1])
        gb_s = float(fields[2]); latency_us = float(fields[3])
    except ValueError:
        return None
    if not math.isfinite(gb_s) or not math.isfinite(latency_us):
        return None
    return Metric(
        backend=backend, size_bytes=size, gbps=gb_s * 8, gb_s=gb_s, latency_us=latency_us,
        role="initiator", batch_size=batch, source=source, raw_line=line.strip(),
    )


def select_metrics(metrics: Sequence[Metric]) -> list[Metric]:
    grouped: dict[tuple[str, int, str], list[Metric]] = {}
    for m in metrics:
        grouped.setdefault((m.backend, m.size_bytes, m.operation), []).append(m)
    selected = []
    for group in grouped.values():
        selected.append(sorted(group, key=metric_preference)[0])
    return sorted(selected, key=lambda m: (m.backend, m.size_bytes))


def metric_preference(metric: Metric) -> tuple[int, str]:
    role = metric.role.lower()
    if "client" in role or "initiator" in role or role.startswith("local"):
        score = 0
    elif "server" in role or "target" in role:
        score = 1
    else:
        score = 2
    return score, metric.source


def collect_log_files(logs_dir: Path) -> list[tuple[str, Path]]:
    """Map ``<backend>_rank<N>.log`` and legacy ``<backend>.log`` filenames to backend names."""
    out: list[tuple[str, Path]] = []
    if not logs_dir.exists():
        return out
    for path in sorted(logs_dir.glob("*.log")):
        stem = path.stem
        for sep in ("_rank", ".server", ".client", "_"):
            if sep in stem:
                stem = stem.split(sep, 1)[0]
                break
        backend = stem.lower()
        out.append((backend, path))
    return out


def metrics_from_logs(sources: Sequence[tuple[str, Path]]) -> list[Metric]:
    metrics: list[Metric] = []
    for backend, path in sources:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        metrics.extend(parse_metrics(backend, text, str(path)))
    return metrics


def write_csv(metrics: Sequence[Metric], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[
            "backend", "size_bytes", "size", "batch_size",
            "gbps", "gb_per_s", "latency_us", "role", "source",
        ])
        writer.writeheader()
        for m in metrics:
            writer.writerow({
                "backend": m.backend, "size_bytes": m.size_bytes, "size": human_size(m.size_bytes),
                "batch_size": m.batch_size, "gbps": f"{m.gbps:.6f}", "gb_per_s": f"{m.gb_s:.6f}",
                "latency_us": f"{m.latency_us:.6f}", "role": m.role, "source": m.source,
            })


def summarize_backends(metrics: Sequence[Metric]) -> list[dict[str, object]]:
    by_backend: dict[str, list[Metric]] = {}
    for m in metrics:
        by_backend.setdefault(m.backend, []).append(m)
    rows = []
    for backend in sorted(by_backend):
        values = by_backend[backend]
        best_bw = max(values, key=lambda x: x.gb_s)
        best_lat = min(values, key=lambda x: x.latency_us)
        rows.append({
            "backend": backend, "bw": best_bw.gb_s, "bw_size": human_size(best_bw.size_bytes),
            "lat": best_lat.latency_us, "lat_size": human_size(best_lat.size_bytes),
            "points": len(values),
        })
    return rows


def write_summary_csv(metrics: Sequence[Metric], path: Path) -> None:
    rows = summarize_backends(metrics)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[
            "backend", "best_bandwidth_gb_s", "best_bandwidth_size",
            "best_latency_us", "best_latency_size", "points",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "backend": row["backend"],
                "best_bandwidth_gb_s": f"{row['bw']:.6f}",
                "best_bandwidth_size": row["bw_size"],
                "best_latency_us": f"{row['lat']:.6f}",
                "best_latency_size": row["lat_size"],
                "points": row["points"],
            })


def write_markdown(metrics: Sequence[Metric], path: Path) -> None:
    lines = [
        "# P2P benchmark comparison",
        "",
        "## Per-size results",
        "",
        "| Backend | Size | Batch | Bandwidth (GB/s) | Bandwidth (Gbps) | Latency (us) | Role |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for m in metrics:
        lines.append(
            f"| {m.backend} | {human_size(m.size_bytes)} | {m.batch_size} | "
            f"{m.gb_s:.3f} | {m.gbps:.3f} | {m.latency_us:.3f} | {m.role or '-'} |"
        )
    lines.extend([
        "", "## Backend summary", "",
        "| Backend | Best bandwidth | Bandwidth size | Best latency | Latency size | Points |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for row in summarize_backends(metrics):
        lines.append(
            f"| {row['backend']} | {row['bw']:.3f} GB/s | {row['bw_size']} | "
            f"{row['lat']:.3f} us | {row['lat_size']} | {row['points']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_metrics_json(metrics: Sequence[Metric], path: Path) -> None:
    path.write_text(
        json.dumps([dataclasses.asdict(m) for m in metrics], indent=2),
        encoding="utf-8",
    )


def nice_max(value: float) -> float:
    if value <= 0:
        return 1.0
    exponent = math.floor(math.log10(value))
    fraction = value / (10**exponent)
    nice = 1 if fraction <= 1 else 2 if fraction <= 2 else 5 if fraction <= 5 else 10
    return nice * (10**exponent)


def color_for_name(name: str) -> str:
    palette = ["#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
    return palette[sum(ord(c) for c in name) % len(palette)]


def write_png(metrics: Sequence[Metric], path: Path) -> None:
    """Render bandwidth + latency comparison chart as a PNG via matplotlib.

    matplotlib is imported lazily so the rest of the harness (which runs
    inside the per-rank container and never calls this) does not gain a hard
    dependency. Plotting only happens on the host during ``report``.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit(
            "matplotlib is required to write the PNG chart "
            f"({path.name}); install it with `pip install matplotlib`"
        ) from exc

    if not metrics:
        fig, ax = plt.subplots(figsize=(4, 1), dpi=150)
        ax.set_axis_off()
        ax.text(0.5, 0.5, "No metrics parsed", ha="center", va="center")
        fig.savefig(path, format="png", bbox_inches="tight")
        plt.close(fig)
        return

    sizes = sorted({m.size_bytes for m in metrics})
    backends = sorted({m.backend for m in metrics})
    colors = {"mori": "#1f77b4", "mooncake": "#ff7f0e", "uccl": "#2ca02c", "nixl": "#d62728"}

    max_bw = nice_max(max((m.gb_s for m in metrics), default=1.0))
    lat_values = [m.latency_us for m in metrics if m.latency_us > 0]
    lat_min = min(lat_values) if lat_values else 1.0
    lat_max = max(lat_values) if lat_values else 1.0
    # log scale: pad by ~10% of the actual decade span so points don't sit on the frame
    log_span = max(math.log10(lat_max / lat_min), 0.2)
    pad = log_span * 0.1
    log_low = lat_min / (10 ** pad)
    log_high = lat_max * (10 ** pad)

    fig, (ax_bw, ax_lat) = plt.subplots(
        2, 1, figsize=(11.2, 7.6), dpi=150, sharex=True,
        gridspec_kw={"hspace": 0.35},
    )
    fig.suptitle("P2P bandwidth and latency comparison", fontsize=16, fontweight="bold", x=0.08, ha="left")

    x_idx = list(range(len(sizes)))
    size_to_x = {size: i for i, size in enumerate(sizes)}

    for backend in backends:
        values = sorted([m for m in metrics if m.backend == backend], key=lambda m: m.size_bytes)
        color = colors.get(backend, color_for_name(backend))
        xs = [size_to_x[m.size_bytes] for m in values]
        ax_bw.plot(xs, [m.gb_s for m in values], marker="o", linewidth=2.0,
                   color=color, label=backend, markeredgecolor="white", markersize=6)
        ax_lat.plot(xs, [m.latency_us for m in values], marker="o", linewidth=2.0,
                    color=color, label=backend, markeredgecolor="white", markersize=6)

    ax_bw.set_title("Bandwidth (GB/s)", loc="left", fontsize=13, fontweight="bold")
    ax_bw.set_ylim(0, max_bw)

    ax_lat.set_title("Latency (us, log scale)", loc="left", fontsize=13, fontweight="bold")
    ax_lat.set_yscale("log")
    ax_lat.set_ylim(log_low, log_high)

    for ax in (ax_bw, ax_lat):
        ax.grid(True, which="major", linestyle="-", linewidth=0.5, color="#dddddd")
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    ax_lat.grid(True, which="minor", linestyle=":", linewidth=0.4, color="#eeeeee")

    ax_lat.set_xticks(x_idx)
    ax_lat.set_xticklabels([human_size(s) for s in sizes], rotation=35, ha="right")
    ax_lat.set_xlabel("Message size")
    if len(x_idx) > 1:
        ax_lat.set_xlim(-0.4, len(x_idx) - 0.6)

    ax_bw.legend(loc="upper left", bbox_to_anchor=(0.0, 1.22), ncol=len(backends),
                 frameon=False, fontsize=11)

    fig.savefig(path, format="png", bbox_inches="tight")
    plt.close(fig)


def cmd_report(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).resolve()
    if not output_dir.exists():
        raise SystemExit(f"output directory not found: {output_dir}")

    logs_dir = output_dir / "logs"
    sources = collect_log_files(logs_dir)
    if not sources and not args.from_log:
        print(f"No logs found under {logs_dir}", file=sys.stderr)
    for entry in args.from_log:
        if "=" not in entry:
            raise SystemExit("--from-log expects BACKEND=PATH")
        b, p = entry.split("=", 1)
        sources.append((b.strip().lower(), Path(p).expanduser()))

    metrics = metrics_from_logs(sources)
    selected = select_metrics(metrics)
    paths = {
        "csv": output_dir / "p2p_results.csv",
        "summary_csv": output_dir / "p2p_summary.csv",
        "markdown": output_dir / "p2p_results.md",
        "metrics_json": output_dir / "p2p_metrics.json",
        "png": output_dir / "p2p_comparison.png",
    }
    write_csv(selected, paths["csv"])
    write_summary_csv(selected, paths["summary_csv"])
    write_markdown(selected, paths["markdown"])
    write_metrics_json(selected, paths["metrics_json"])
    write_png(selected, paths["png"])

    if not selected:
        print("No benchmark metrics were parsed.")
    else:
        print("\nBackend summary:")
        print(f"{'Backend':<12} {'Best GB/s':>12} {'BW size':>12} {'Best us':>12} {'Lat size':>12}")
        for row in summarize_backends(selected):
            print(
                f"{row['backend']:<12} {row['bw']:>12.3f} {row['bw_size']:>12} "
                f"{row['lat']:>12.3f} {row['lat_size']:>12}"
            )
    print("\nReports:")
    for name, path in paths.items():
        print(f"  {name}: {path}")
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backends", default=",".join(DEFAULT_BACKENDS))
    parser.add_argument(
        "--sizes",
        default=",".join(str(s) for s in DEFAULT_SIZES),
        help=(
            "Comma-separated message sizes (bytes or 1K/1M/16M). "
            "Ignored when --size-min/--size-max specify a sweep."
        ),
    )
    parser.add_argument(
        "--size-min",
        default=None,
        help=(
            "Sweep starting size (e.g. 64 or 1K). When set together with "
            "--size-max, the harness emits sizes [min, 2*min, 4*min, ..., max] "
            "(ib_write_bw -a style power-of-two sweep) and overrides --sizes."
        ),
    )
    parser.add_argument(
        "--size-max",
        default=None,
        help="Sweep upper bound (e.g. 16M). Required together with --size-min.",
    )
    parser.add_argument(
        "--size-step-factor",
        type=int,
        default=2,
        help="Multiplier between consecutive sweep sizes (default 2 = doubling).",
    )
    parser.add_argument("--iters", type=int, default=10)
    parser.add_argument("--num-blocks", type=int, default=1)
    parser.add_argument("--device", choices=["cpu", "gpu"], default="gpu")
    parser.add_argument("--op-type", choices=["read", "write"], default="write")
    parser.add_argument("--source-root", default=str(DEFAULT_THIRDPARTY_DIR))
    parser.add_argument("--wheelhouse", default=str(DEFAULT_WHEELHOUSE))
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--output-dir", required=True,
                        help="Directory to receive logs/ and report files")
    parser.add_argument("--skip-wheel-install", action="store_true")
    parser.add_argument("--runtime-pip-packages", default="prettytable",
                        help="Comma-separated runtime pip deps (e.g. prettytable for mori)")
    parser.add_argument("--pair-startup-seconds", type=float, default=2.0)
    parser.add_argument("--async-api", action="store_true")
    parser.add_argument(
        "--ib-hca",
        default="",
        help=(
            "NCCL_IB_HCA-style HCA selector applied to MORI and UCCL "
            "(NCCL_IB_HCA also gets set so RCCL plugins pick it up). "
            "Use a comma-separated whitelist (e.g. 'mlx5_0,mlx5_2') or "
            "prefix with '^' to exclude (e.g. '^mlx5_1,mlx5_6'). "
            "MORI -> MORI_RDMA_DEVICES (native ^ syntax), UCCL -> "
            "UCCL_P2P_RDMA_DEV (whitelist-only; ^ is auto-expanded against "
            "/sys/class/infiniband). Empty disables the override."
        ),
    )
    parser.add_argument("--mori-backend", choices=["rdma", "xgmi"], default="rdma")
    parser.add_argument("--mori-transfer-batch-size", type=int, default=1)
    parser.add_argument("--mori-xgmi-multiprocess", action="store_true")
    parser.add_argument("--nixlbench-bin", default="nixlbench")
    parser.add_argument("--nixl-backend", default="UCX")
    parser.add_argument("--nixl-etcd-endpoints", default=None)
    parser.add_argument("--nixl-start-etcd", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--nixl-device-list", default=None)
    parser.add_argument("--mooncake-bench-bin", default="tebench")
    parser.add_argument("--mooncake-backend", choices=["classic", "tent"], default="tent")
    parser.add_argument("--mooncake-xport-type", default="rdma")
    parser.add_argument("--mooncake-duration", type=int, default=5)
    parser.add_argument("--uccl-script", default=None)
    parser.add_argument("--mori-script", default=None)
    parser.add_argument("--nixl-script", default=None)
    parser.add_argument("--mooncake-script", default=None)


def add_report_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", required=True,
                        help="Directory containing logs/ and where reports will be written")
    parser.add_argument("--from-log", action="append", default=[], metavar="BACKEND=PATH",
                        help="Add an extra log file to parse")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Per-rank P2P benchmark runner and report aggregator."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Per-rank: install wheels and run each backend benchmark")
    add_run_arguments(p_run)

    p_report = sub.add_parser("report", help="Aggregate per-rank logs into reports")
    add_report_arguments(p_report)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "report":
        return cmd_report(args)
    raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
