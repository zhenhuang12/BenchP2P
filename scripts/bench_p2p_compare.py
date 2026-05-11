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
import shlex
import subprocess
import sys
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
#
# The actual benchmark for each backend lives in scripts/backend/run_<backend>.sh.
# This module's job is to: install the wheelhouse, normalise sizes, set up
# distributed env vars, and then dispatch to the per-backend shell script.


@dataclasses.dataclass
class RunArtifact:
    backend: str
    rank: int
    log_path: Path
    status: str
    exit_code: int | None
    elapsed_s: float
    error: str = ""


BACKEND_SCRIPT_DIR = Path(__file__).resolve().parent / "backend"


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


def normalize_sizes_for_backends(sizes_csv: str) -> str:
    """Convert a comma-separated size list (1K/1M/integers) to integer bytes only.

    The per-backend shell scripts expect integer bytes; doing the conversion
    once here matches what each Python-side dispatcher used to do per-backend
    (uccl normalised --sizes; mori parsed each --buffer-size; nixl/mooncake
    assumed integer strings).
    """
    parts = [str(parse_size(s)) for s in sizes_csv.split(",") if s.strip()]
    if not parts:
        raise SystemExit("size list is empty")
    return ",".join(parts)


def backend_script_path(backend: str) -> Path:
    script = BACKEND_SCRIPT_DIR / f"run_{backend}.sh"
    if not script.is_file():
        raise SystemExit(f"backend script not found: {script}")
    return script


def common_backend_args(args: argparse.Namespace, env: dict[str, str],
                        sizes_int_csv: str) -> list[str]:
    """Args shared by every scripts/backend/run_<backend>.sh."""
    return [
        "--rank", env["RANK"],
        "--world-size", env["WORLD_SIZE"],
        "--local-rank", env["LOCAL_RANK"],
        "--master-addr", env["MASTER_ADDR"],
        "--master-port", env["MASTER_PORT"],
        "--output-dir", str(args.output_dir),
        "--source-root", str(args.source_root),
        "--device", args.device,
        "--sizes", sizes_int_csv,
        "--iters", str(args.iters),
        "--batch-size", str(args.batch_size),
        "--op-type", args.op_type,
    ]


def backend_extra_args(args: argparse.Namespace, backend: str) -> list[str]:
    """Backend-specific argv added on top of common_backend_args."""
    if backend == "uccl":
        out: list[str] = []
        if args.ib_hca:
            out += ["--ib-hca", args.ib_hca]
        if args.uccl_script:
            out += ["--script", args.uccl_script]
        if args.async_api:
            out.append("--async-api")
        if args.uccl_sendrecv:
            out.append("--uccl-sendrecv")
        if args.uccl_no_lazy:
            out.append("--no-lazy")
        return out
    if backend == "mori":
        out = ["--mori-backend", args.mori_backend]
        if args.ib_hca:
            out += ["--ib-hca", args.ib_hca]
        if args.mori_script:
            out += ["--script", args.mori_script]
        if args.mori_xgmi_multiprocess:
            out.append("--mori-xgmi-multiprocess")
        return out
    if backend == "nixl":
        out = [
            "--nixlbench-bin", args.nixlbench_bin,
            "--nixl-backend", args.nixl_backend,
            "--nixl-seg-type", args.nixl_seg_type,
            "--pair-startup-seconds", str(args.pair_startup_seconds),
            "--nixl-start-etcd", "1" if args.nixl_start_etcd else "0",
        ]
        if args.nixl_etcd_endpoints:
            out += ["--nixl-etcd-endpoints", args.nixl_etcd_endpoints]
        if args.nixl_device_list:
            out += ["--nixl-device-list", args.nixl_device_list]
        return out
    if backend == "mooncake":
        out = [
            "--mooncake-bench-bin", args.mooncake_bench_bin,
            "--mooncake-xport-type", args.mooncake_xport_type or "rdma",
            "--mooncake-threads", str(args.mooncake_threads),
            "--mooncake-duration", str(args.mooncake_duration),
            "--mooncake-target-wait-seconds", str(args.mooncake_target_wait_seconds),
            "--pair-startup-seconds", str(args.pair_startup_seconds),
        ]
        if args.ib_hca:
            out += ["--ib-hca", args.ib_hca]
        return out
    raise RuntimeError(f"unknown backend: {backend}")


def invoke_backend_script(args: argparse.Namespace, env: dict[str, str],
                          backend: str, sizes_int_csv: str) -> None:
    script = backend_script_path(backend)
    command = [
        "bash", str(script),
        *common_backend_args(args, env, sizes_int_csv),
        *backend_extra_args(args, backend),
    ]
    print("+ " + shell_join(command), flush=True)
    completed = subprocess.run(command, env=env)
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, command)


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
    sizes_int_csv = normalize_sizes_for_backends(args.sizes)

    for backend in backends:
        print(f"\n==> [{backend}] starting (rank {rank}/{world})", flush=True)
        try:
            invoke_backend_script(args, env, backend, sizes_int_csv)
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


def message_bytes(m: Metric) -> int:
    """Wire-level "message" size per iter = per-block size * batch_size.

    BenchP2P normalizes every backend's reported size to a per-BLOCK figure
    (mori `transfer_batch_size`, nixl `--start_batch_size`, mooncake
    `--block_size`, and uccl after run_uccl.sh divides by num-kvblocks).
    The actual amount of bytes the NIC moves per iteration is
    block_size * batch_size, which matters for fairness when comparing
    runs that swept batch_size. Computed lazily because Metric is frozen.
    """
    return m.size_bytes * max(m.batch_size, 1)


_MOONCAKE_TE_BLOCKSIZE_RE = re.compile(r"--block_size[ =](\d+)")
_MOONCAKE_TE_BATCHSIZE_RE = re.compile(r"--batch_size[ =](\d+)")
_MOONCAKE_TE_RESULT_RE = re.compile(
    r"Test completed:\s*duration\s+([\d.]+),\s*batch count\s+(\d+),\s*"
    r"throughput\s+([\d.]+)\s*([KMGT]i?[Bb])/s"
)
# Marker emitted by scripts/backend/run_uccl.sh; carries the unified
# batch_size that run_uccl.sh used to scale UCCL's --sizes (per-MESSAGE
# total) up from BenchP2P's --sizes (per-BLOCK). The parser divides
# UCCL's reported size by this batch_size to recover the per-block
# size_bytes used by mori / nixl / mooncake.
_UCCL_BATCH_MARKER_RE = re.compile(r"\[bench_p2p_compare\]\s+uccl\s+batch_size=(\d+)")
# Same marker line also stamps the op_type (write/read/sendrecv) so
# Metric.operation reflects the real ULP UCCL ran. write/read come from
# benchmark_uccl_readwrite.py; sendrecv from the legacy benchmark_uccl.py
# under --uccl-sendrecv. Older logs without this token fall back to the
# Metric.operation default ("write").
_UCCL_OPTYPE_MARKER_RE = re.compile(r"\[bench_p2p_compare\]\s+uccl\b[^\n]*\bop_type=(\w+)")


def parse_metrics(backend: str, text: str, source: str) -> list[Metric]:
    metrics: list[Metric] = []
    # State for mooncake's transfer_engine_bench, whose output is one
    # `Test completed: duration X, batch count Y, throughput Z UNIT/s`
    # line per run; block/batch size comes from the argv we logged
    # earlier in the same file.
    mooncake_block: int | None = None
    mooncake_batch: int = 1
    # State for UCCL: run_uccl.sh logs `[bench_p2p_compare] uccl
    # batch_size=N op_type=... ...` once at the top so we can divide
    # UCCL's reported per-message size by N to get the per-block size that
    # matches the other three backends, and stamp Metric.operation with
    # the actual RDMA op (write/read/sendrecv).
    uccl_batch: int = 1
    uccl_op: str | None = None
    for line in text.splitlines():
        if backend == "uccl":
            mb = _UCCL_BATCH_MARKER_RE.search(line)
            if mb is not None:
                try:
                    uccl_batch = max(int(mb.group(1)), 1)
                except ValueError:
                    pass
            mo = _UCCL_OPTYPE_MARKER_RE.search(line)
            if mo is not None:
                uccl_op = mo.group(1).lower()
        match = LOG_LINE_RE.search(line)
        if match:
            raw_size = parse_size(match.group("size"))
            if backend == "uccl" and uccl_batch > 1:
                size_bytes = raw_size // uccl_batch
                metric_batch = uccl_batch
            else:
                size_bytes = raw_size
                metric_batch = 1
            kwargs: dict[str, object] = dict(
                backend=backend,
                size_bytes=size_bytes,
                gbps=float(match.group("gbps")),
                gb_s=float(match.group("gb_s")),
                latency_us=float(match.group("lat_s")) * 1_000_000,
                role=match.group("role"),
                batch_size=metric_batch,
                source=source,
                raw_line=line.strip(),
            )
            if backend == "uccl" and uccl_op is not None:
                kwargs["operation"] = uccl_op
            metrics.append(Metric(**kwargs))
            continue
        if backend == "mori":
            m = parse_mori_table_line(backend, line, source)
            if m is not None:
                metrics.append(m)
        elif backend == "mooncake":
            mb = _MOONCAKE_TE_BLOCKSIZE_RE.search(line)
            if mb is not None:
                mooncake_block = int(mb.group(1))
            mc = _MOONCAKE_TE_BATCHSIZE_RE.search(line)
            if mc is not None:
                mooncake_batch = int(mc.group(1))
            m_te = _MOONCAKE_TE_RESULT_RE.search(line)
            if m_te is not None and mooncake_block is not None:
                duration_s = float(m_te.group(1))
                throughput_val = float(m_te.group(3))
                unit = m_te.group(4)
                # Convert reported throughput to GB/s. mooncake's
                # calculateRate() supports GB/GiB/Gb/MB/MiB/Mb/KB/KiB/Kb;
                # we map them to byte-based GB/s for consistency with
                # mori/uccl/nixl.
                unit_to_bytes = {
                    "Gb": 1_000_000_000 / 8,  "GB": 1_000_000_000,  "GiB": 1 << 30,
                    "Mb": 1_000_000 / 8,      "MB": 1_000_000,      "MiB": 1 << 20,
                    "Kb": 1_000 / 8,          "KB": 1_000,          "KiB": 1 << 10,
                }
                bytes_per_sec = throughput_val * unit_to_bytes.get(unit, 1)
                gb_s = bytes_per_sec / 1_000_000_000
                # Per-batch latency = duration_s / batch_count (us).
                try:
                    avg_us = duration_s / int(m_te.group(2)) * 1_000_000
                except ZeroDivisionError:
                    avg_us = 0.0
                metrics.append(Metric(
                    backend=backend,
                    size_bytes=mooncake_block,
                    gbps=gb_s * 8,
                    gb_s=gb_s,
                    latency_us=avg_us,
                    role="initiator",
                    batch_size=mooncake_batch,
                    source=source,
                    raw_line=line.strip(),
                ))
                continue
            # Fall back to the generic 4-column-table parser so the
            # legacy `tebench` output (still possible if user forces
            # --mooncake-bench-bin tebench) keeps working.
            m = parse_official_table_line(backend, line, source)
            if m is not None:
                metrics.append(m)
        elif backend == "nixl":
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
    # Dedup key includes batch_size so a single run that sweeps both
    # block-size AND batch-size (e.g. 1MB at bs=1 vs bs=128) keeps both
    # points instead of folding them onto one row.
    grouped: dict[tuple[str, int, int, str], list[Metric]] = {}
    for m in metrics:
        grouped.setdefault((m.backend, m.size_bytes, m.batch_size, m.operation), []).append(m)
    selected = []
    for group in grouped.values():
        selected.append(sorted(group, key=metric_preference)[0])
    return sorted(selected, key=lambda m: (m.backend, m.size_bytes, m.batch_size))


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
            "message_bytes", "message_size",
            "gbps", "gb_per_s", "latency_us", "role", "source",
        ])
        writer.writeheader()
        for m in metrics:
            mb = message_bytes(m)
            writer.writerow({
                "backend": m.backend, "size_bytes": m.size_bytes, "size": human_size(m.size_bytes),
                "batch_size": m.batch_size,
                "message_bytes": mb, "message_size": human_size(mb),
                "gbps": f"{m.gbps:.6f}", "gb_per_s": f"{m.gb_s:.6f}",
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
            "backend": backend,
            "bw": best_bw.gb_s,
            "bw_size": human_size(best_bw.size_bytes),
            "bw_batch": best_bw.batch_size,
            "bw_message": human_size(message_bytes(best_bw)),
            "lat": best_lat.latency_us,
            "lat_size": human_size(best_lat.size_bytes),
            "lat_batch": best_lat.batch_size,
            "lat_message": human_size(message_bytes(best_lat)),
            "points": len(values),
        })
    return rows


def write_summary_csv(metrics: Sequence[Metric], path: Path) -> None:
    rows = summarize_backends(metrics)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[
            "backend", "best_bandwidth_gb_s", "best_bandwidth_size",
            "best_bandwidth_batch", "best_bandwidth_message_size",
            "best_latency_us", "best_latency_size",
            "best_latency_batch", "best_latency_message_size",
            "points",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "backend": row["backend"],
                "best_bandwidth_gb_s": f"{row['bw']:.6f}",
                "best_bandwidth_size": row["bw_size"],
                "best_bandwidth_batch": row["bw_batch"],
                "best_bandwidth_message_size": row["bw_message"],
                "best_latency_us": f"{row['lat']:.6f}",
                "best_latency_size": row["lat_size"],
                "best_latency_batch": row["lat_batch"],
                "best_latency_message_size": row["lat_message"],
                "points": row["points"],
            })


def write_markdown(metrics: Sequence[Metric], path: Path) -> None:
    lines = [
        "# P2P benchmark comparison",
        "",
        "## Per-size results",
        "",
        "_Message = block size x batch (bytes actually moved per iter)._",
        "",
        "| Backend | Block | Batch | Message | Bandwidth (GB/s) | Bandwidth (Gbps) | Latency (us) | Role |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for m in metrics:
        lines.append(
            f"| {m.backend} | {human_size(m.size_bytes)} | {m.batch_size} | "
            f"{human_size(message_bytes(m))} | "
            f"{m.gb_s:.3f} | {m.gbps:.3f} | {m.latency_us:.3f} | {m.role or '-'} |"
        )
    lines.extend([
        "", "## Backend summary", "",
        "| Backend | Best bandwidth | BW block | BW message | Best latency | Lat block | Lat message | Points |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in summarize_backends(metrics):
        lines.append(
            f"| {row['backend']} | {row['bw']:.3f} GB/s | {row['bw_size']} | "
            f"{row['bw_message']} | "
            f"{row['lat']:.3f} us | {row['lat_size']} | {row['lat_message']} | "
            f"{row['points']} |"
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

    # x-axis = message size (block * batch), i.e. how many bytes the NIC
    # actually moves per iter. With batch sweeps in the same plot, two
    # points at the same per-block size but different batch sizes will
    # land at distinct x positions instead of stacking on top of each
    # other (which they would if we keyed on size_bytes alone).
    sizes = sorted({message_bytes(m) for m in metrics})
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
    # Surface batch_size in the title so a single PNG snapshot is
    # self-describing: a "block=4KB" point at batch=128 moves 512KB on
    # the wire, which matters when comparing it against a batch=1 run.
    batches = sorted({m.batch_size for m in metrics})
    if len(batches) == 1:
        batch_suffix = f"  (batch={batches[0]})"
    elif len(batches) <= 4:
        batch_suffix = f"  (batch={','.join(str(b) for b in batches)})"
    else:
        batch_suffix = f"  (batch={batches[0]}..{batches[-1]})"
    fig.suptitle(
        f"P2P bandwidth and latency comparison{batch_suffix}",
        fontsize=16, fontweight="bold", x=0.08, ha="left",
    )

    x_idx = list(range(len(sizes)))
    size_to_x = {size: i for i, size in enumerate(sizes)}

    for backend in backends:
        values = sorted([m for m in metrics if m.backend == backend], key=message_bytes)
        color = colors.get(backend, color_for_name(backend))
        xs = [size_to_x[message_bytes(m)] for m in values]
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
    # Inline the actual batch_size into the xlabel so the per-iter wire
    # bytes are obvious from the chart alone (e.g. `block x 128`).
    if len(batches) == 1:
        batch_label = str(batches[0])
    elif len(batches) <= 4:
        batch_label = "{" + ",".join(str(b) for b in batches) + "}"
    else:
        batch_label = f"{batches[0]}..{batches[-1]}"
    ax_lat.set_xlabel(f"Message size  (block x {batch_label})")
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
        print("\nBackend summary (Message = block x batch):")
        print(
            f"{'Backend':<12} {'Best GB/s':>12} "
            f"{'BW block':>10} {'BW batch':>9} {'BW msg':>10} "
            f"{'Best us':>12} "
            f"{'Lat block':>10} {'Lat batch':>10} {'Lat msg':>10}"
        )
        for row in summarize_backends(selected):
            print(
                f"{row['backend']:<12} {row['bw']:>12.3f} "
                f"{row['bw_size']:>10} {row['bw_batch']:>9} {row['bw_message']:>10} "
                f"{row['lat']:>12.3f} "
                f"{row['lat_size']:>10} {row['lat_batch']:>10} {row['lat_message']:>10}"
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
    parser.add_argument(
        "--batch-size",
        "--num-blocks",
        dest="batch_size",
        type=int,
        default=1,
        help=(
            "Unified batch / block count, applied to every backend's native "
            "batch flag for an apples-to-apples comparison: "
            "UCCL --num-kvblocks, MORI --transfer-batch-size, "
            "nixlbench --start_batch_size/--max_batch_size, "
            "Mooncake transfer_engine_bench --batch_size. "
            "Default 1 (per-message comparison). `--num-blocks` is a kept "
            "for backward compatibility."
        ),
    )
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
    parser.add_argument("--mori-xgmi-multiprocess", action="store_true")
    parser.add_argument("--nixlbench-bin", default="nixlbench")
    parser.add_argument(
        "--nixl-backend",
        default="LIBFABRIC",
        help=(
            "nixlbench --backend selection. Default LIBFABRIC because the "
            "ROCm/HIP-built nixl in benchp2p:latest skips the UCX plugin "
            "(its meson refuses to use the system UCX 1.12 missing "
            "UCS_BIT_GET). Override with `--nixl-backend UCX` etc. on a "
            "CUDA build with a newer UCX."
        ),
    )
    parser.add_argument("--nixl-etcd-endpoints", default=None)
    parser.add_argument("--nixl-start-etcd", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--nixl-device-list", default=None)
    parser.add_argument(
        "--nixl-seg-type",
        choices=["auto", "DRAM", "VRAM"],
        default="auto",
        help=(
            "Override nixlbench --initiator_seg_type / --target_seg_type. "
            "Default 'auto' follows --device (gpu->VRAM, cpu->DRAM). "
            "On ROCm/HIP base images, nixlbench is built without HAVE_CUDA "
            "and aborts with `VRAM not supported without CUDA or Neuron`; "
            "pass `--nixl-seg-type DRAM` there to keep nixl in the run."
        ),
    )
    parser.add_argument(
        "--mooncake-bench-bin",
        default="transfer_engine_bench",
        help=(
            "Mooncake bench binary. Defaults to `transfer_engine_bench` "
            "(mooncake-transfer-engine/example/, the README-recommended "
            "tool, shipped via the mooncake wheel and on PATH inside the "
            "runtime image at /opt/venv/bin/transfer_engine_bench). The "
            "older `tebench` (mooncake-transfer-engine/benchmark/) is "
            "documented as a `prototype micro-benchmark` and its "
            "metadata_type=p2p+rdma path is broken on ROCm/VRAM."
        ),
    )
    parser.add_argument("--mooncake-backend", choices=["classic", "tent"], default="tent")
    parser.add_argument(
        "--mooncake-xport-type",
        default="rdma",
        help=(
            "transfer_engine_bench --protocol value (rdma|tcp|...). "
            "Default rdma matches the README-recommended cross-node setup."
        ),
    )
    parser.add_argument(
        "--mooncake-threads",
        type=int,
        default=12,
        help=(
            "transfer_engine_bench --threads (initiator side). README "
            "default is 12; multiple threads are needed to fully utilise "
            "RDMA bandwidth because of CPU-side request prep overhead."
        ),
    )
    parser.add_argument("--mooncake-duration", type=int, default=5)
    parser.add_argument(
        "--mooncake-target-wait-seconds",
        type=float,
        default=90.0,
        help=(
            "How long rank>0 waits for the mooncake target segment file to "
            "appear on the shared FS before failing. Defaults to 90 s; bump "
            "this on NFS mounts where directory attribute caching (acdirmin "
            "defaults to 30 s) can hide a freshly-written file from the "
            "initiator side."
        ),
    )
    parser.add_argument("--uccl-script", default=None)
    parser.add_argument(
        "--uccl-sendrecv",
        action="store_true",
        help=(
            "Force UCCL to use the legacy two-sided benchmark_uccl.py "
            "(RDMA SEND/RECV). Default off: UCCL runs "
            "benchmark_uccl_readwrite.py --mode {--op-type} so it shares "
            "the same one-sided ULP as mori/nixl/mooncake. Only enable "
            "when you specifically want UCCL's SEND/RECV cost."
        ),
    )
    parser.add_argument(
        "--uccl-no-lazy",
        action="store_true",
        help=(
            "Disable benchmark_uccl_readwrite.py --lazy (the script will "
            "ibv_reg_mr per iter instead of pre-registering). Off by "
            "default; --lazy is what the README example uses for stable "
            "small-size numbers."
        ),
    )
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
