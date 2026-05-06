#!/usr/bin/env python3
"""Run and summarize P2P bandwidth/latency benchmarks.

The tool is intentionally a thin harness around each backend's own benchmark.
It keeps command generation, log capture, parsing, and report generation in one
place so different P2P stacks can be compared with the same sizes and iters.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import html
import json
import math
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_SIZES = [
    256,
    1024,
    4096,
    16384,
    65536,
    262144,
    1048576,
    10485760,
    16777216,
    104857600,
]

DEFAULT_BACKENDS = ["mori", "mooncake", "uccl", "nixl"]
LOG_LINE_RE = re.compile(
    r"\[(?P<role>[^\]]+)\]\s+"
    r"(?:(?P<mode>DUAL-WRITE)\s+)?"
    r"(?P<size>\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB))\s*:\s*"
    r"(?P<gbps>[0-9.+-eE]+)\s*Gbps\s*\|\s*"
    r"(?P<gb_s>[0-9.+-eE]+)\s*GB/s\s*\|\s*"
    r"(?P<lat_s>[0-9.+-eE]+)\s*s"
)


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


@dataclasses.dataclass(frozen=True)
class RunSpec:
    backend: str
    label: str
    commands: tuple[tuple[str, ...], ...]
    cwd: Path
    env: dict[str, str]
    kind: str = "single"
    startup_seconds: float = 2.0


@dataclasses.dataclass
class RunResult:
    backend: str
    label: str
    status: str
    exit_code: int | None
    logs: list[str]
    command: list[str]
    elapsed_s: float
    error: str = ""


def parse_size_list(value: str) -> list[int]:
    sizes: list[int] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        sizes.append(parse_size(item))
    if not sizes:
        raise argparse.ArgumentTypeError("size list cannot be empty")
    return sizes


def parse_size(value: str) -> int:
    text = value.strip().replace(" ", "")
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KkMmGgTt]?)(?:[Bb])?", text)
    if not match:
        raise argparse.ArgumentTypeError(f"bad size: {value}")
    number = float(match.group(1))
    unit = match.group(2).upper()
    multiplier = {
        "": 1,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }[unit]
    return int(number * multiplier)


def human_size(size: int) -> str:
    value = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def shell_join(command: Sequence[str]) -> str:
    return shlex.join(str(part) for part in command)


def shell_words(command: Sequence[str]) -> str:
    return " ".join(shell_token(part) for part in command)


def shell_token(value: str) -> str:
    if value.startswith("$") or "${" in value:
        return value
    return shlex.quote(str(value))


def default_source_root() -> Path:
    return Path(__file__).resolve().parents[1] / "3rdparty"


def output_dir_for(base_dir: Path) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return base_dir / f"p2p_compare_{stamp}"


def pythonpath_with(*paths: Path) -> str:
    existing = os.environ.get("PYTHONPATH", "")
    parts = [str(path) for path in paths if path.exists()]
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def default_thirdparty_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "3rdparty"


def backend_script_paths(source_root: Path, args: argparse.Namespace) -> dict[str, Path]:
    return {
        "uccl": Path(args.uccl_script)
        if args.uccl_script
        else source_root / "uccl" / "p2p" / "benchmarks" / "benchmark_uccl.py",
        "nixl": Path(args.nixl_script)
        if args.nixl_script
        else source_root / "uccl" / "p2p" / "benchmarks" / "benchmark_nixl.py",
        "mooncake": Path(args.mooncake_script)
        if args.mooncake_script
        else source_root / "uccl" / "p2p" / "benchmarks" / "benchmark_nixl.py",
        "mori": Path(args.mori_script)
        if args.mori_script
        else source_root / "mori" / "tests" / "python" / "io" / "benchmark.py",
    }


def make_run_specs(args: argparse.Namespace, output_dir: Path) -> tuple[list[RunSpec], list[RunResult]]:
    source_root = Path(args.source_root).resolve()
    paths = backend_script_paths(source_root, args)
    backends = [item.strip().lower() for item in args.backends.split(",") if item.strip()]
    sizes_csv = ",".join(str(size) for size in args.sizes)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = pythonpath_with(
        source_root / "uccl",
        source_root / "mori",
        source_root / "mori" / "python",
        source_root / "Mooncake" / "mooncake-wheel",
        source_root / "nixl" / "src" / "bindings" / "python" / "nixl-meta",
        source_root,
    )

    specs: list[RunSpec] = []
    skipped: list[RunResult] = []
    for backend in backends:
        script = paths.get(backend)
        if script is None:
            skipped.append(
                RunResult(backend, backend, "skipped", None, [], [], 0.0, "unknown backend")
            )
            continue
        if not script.exists() and not args.dry_run:
            skipped.append(
                RunResult(
                    backend,
                    backend,
                    "skipped",
                    None,
                    [],
                    [],
                    0.0,
                    f"benchmark script not found: {script}",
                )
            )
            continue

        if backend == "uccl":
            command = [
                args.torchrun,
                "--standalone",
                "--nnodes=1",
                "--nproc-per-node=2",
                str(script),
                "--sizes",
                sizes_csv,
                "--iters",
                str(args.iters),
                "--device",
                args.device,
                "--local-gpu-idx",
                str(args.local_gpu_idx),
                "--num-kvblocks",
                str(args.num_blocks),
            ]
            if args.async_api:
                command.append("--async-api")
            if args.launcher == "slurm":
                command = make_uccl_slurm_command(args, script, sizes_csv)
            specs.append(
                RunSpec(backend, backend, (tuple(command),), source_root, env.copy())
            )
        elif backend in {"nixl", "mooncake"}:
            nixl_backend = "ucx" if backend == "nixl" else "mooncake"
            common = [
                str(script),
                "--sizes",
                sizes_csv,
                "--iters",
                str(args.iters),
                "--device",
                args.device,
                "--local-gpu-idx",
                str(args.local_gpu_idx),
                "--num-kvblocks",
                str(args.num_blocks),
                "--backend",
                nixl_backend,
                "--op-type",
                args.op_type,
            ]
            if args.launcher == "slurm":
                command = make_nixl_family_slurm_command(
                    args, script, sizes_csv, nixl_backend
                )
                specs.append(
                    RunSpec(backend, backend, (tuple(command),), source_root, env.copy())
                )
                continue
            server = [
                args.python,
                *common,
                "--role",
                "server",
                "--remote-ip",
                args.server_bind_ip,
            ]
            client = [
                args.python,
                *common,
                "--role",
                "client",
                "--remote-ip",
                args.server_ip,
            ]
            specs.append(
                RunSpec(
                    backend,
                    backend,
                    (tuple(server), tuple(client)),
                    source_root,
                    env.copy(),
                    kind="pair",
                    startup_seconds=args.pair_startup_seconds,
                )
            )
        elif backend == "mori":
            for size in args.sizes:
                command = make_mori_command(args, script, size)
                if args.launcher == "slurm":
                    command = make_mori_slurm_command(args, script, size)
                specs.append(
                    RunSpec(
                        backend,
                        f"{backend}_{size}",
                        (tuple(command),),
                        source_root,
                        env.copy(),
                    )
                )
    return specs, skipped


def make_mori_command(args: argparse.Namespace, script: Path, size: int) -> list[str]:
    common = [
        str(script),
        "--backend",
        args.mori_backend,
        "--op-type",
        args.op_type,
        "--buffer-size",
        str(size),
        "--transfer-batch-size",
        str(args.mori_transfer_batch_size),
        "--iters",
        str(args.iters),
    ]
    if args.mori_backend == "xgmi":
        command = [
            args.python,
            *common,
            "--src-gpu",
            str(args.local_gpu_idx),
            "--dst-gpu",
            str(args.dst_gpu_idx),
        ]
        if args.mori_xgmi_multiprocess:
            command = [
                args.torchrun,
                "--standalone",
                "--nnodes=1",
                "--nproc-per-node=2",
                *common,
                "--src-gpu",
                str(args.local_gpu_idx),
                "--dst-gpu",
                str(args.dst_gpu_idx),
                "--xgmi-multiprocess",
            ]
        return command
    return [
        args.torchrun,
        "--standalone",
        "--nnodes=1",
        "--nproc-per-node=2",
        *common,
        "--host",
        args.mori_host,
        "--num-initiator-dev",
        "1",
        "--num-target-dev",
        "1",
    ]


def slurm_srun_prefix(args: argparse.Namespace) -> list[str]:
    command = [
        args.srun,
        f"--nodes={args.slurm_nodes}",
        f"--ntasks={args.slurm_ntasks}",
        f"--ntasks-per-node={args.slurm_ntasks_per_node}",
        "--kill-on-bad-exit=1",
        "--export=ALL",
    ]
    optional = [
        ("--partition", args.slurm_partition),
        ("--account", args.slurm_account),
        ("--qos", args.slurm_qos),
        ("--time", args.slurm_time),
        ("--constraint", args.slurm_constraint),
        ("--gres", args.slurm_gres),
        ("--gpus-per-task", args.slurm_gpus_per_task),
        ("--cpus-per-task", args.slurm_cpus_per_task),
        ("--job-name", args.slurm_job_name),
    ]
    for flag, value in optional:
        if value:
            command.append(f"{flag}={value}")
    if args.slurm_container_runtime == "pyxis" and args.slurm_container_image:
        command.append(f"--container-image={args.slurm_container_image}")
        command.append(f"--container-workdir={args.slurm_container_workdir}")
        mounts = slurm_container_mounts(args)
        if mounts:
            command.append(f"--container-mounts={mounts}")
    if args.slurm_extra_args:
        command.extend(shlex.split(args.slurm_extra_args))
    return command


def slurm_preamble(args: argparse.Namespace) -> str:
    return "\n".join(
        [
            "set -euo pipefail",
            'SLURM_PROCID="${SLURM_PROCID:-0}"',
            'SLURM_NTASKS="${SLURM_NTASKS:-1}"',
            'SLURM_LOCALID="${SLURM_LOCALID:-0}"',
            'MASTER_ADDR="${MASTER_ADDR:-$(scontrol show hostnames "${SLURM_JOB_NODELIST}" | sed -n \'1p\')}"',
            f'MASTER_PORT="${{MASTER_PORT:-{args.slurm_master_port}}}"',
            'export MASTER_ADDR MASTER_PORT',
            'export RANK="${SLURM_PROCID}"',
            'export WORLD_SIZE="${SLURM_NTASKS}"',
            'export LOCAL_RANK="${SLURM_LOCALID}"',
            'export LOCAL_WORLD_SIZE="${SLURM_NTASKS_PER_NODE:-1}"',
        ]
    )


def slurm_runtime_install(args: argparse.Namespace) -> str:
    if args.skip_runtime_wheel_install:
        return ""
    python = shell_token(args.container_python)
    return "\n".join(
        [
            f"export BENCHP2P_WHEELHOUSE={shell_token(args.runtime_wheelhouse)}",
            'if [ ! -d "${BENCHP2P_WHEELHOUSE}" ]; then',
            '  echo "BenchP2P wheelhouse not found: ${BENCHP2P_WHEELHOUSE}" >&2',
            "  exit 1",
            "fi",
            f"{python} - <<'PY'",
            "import os",
            "import subprocess",
            "import sys",
            "from pathlib import Path",
            "",
            'wheelhouse = Path(os.environ["BENCHP2P_WHEELHOUSE"])',
            'wheels = sorted(str(path) for path in wheelhouse.glob("*/*.whl"))',
            "if not wheels:",
            '    raise SystemExit(f"No wheel files found under {wheelhouse}")',
            'print("Installing BenchP2P wheelhouse inside runtime container:", flush=True)',
            "for wheel in wheels:",
            '    print(f"  {wheel}", flush=True)',
            "subprocess.run(",
            '    [sys.executable, "-m", "pip", "install", "--force-reinstall", "--no-deps", *wheels],',
            "    check=True,",
            ")",
            "PY",
        ]
    )


def slurm_body_prefix(args: argparse.Namespace) -> str:
    parts = [slurm_preamble(args)]
    install = slurm_runtime_install(args)
    if install:
        parts.append(install)
    return "\n".join(parts)


def slurm_container_mounts(args: argparse.Namespace) -> str:
    mounts = []
    for path_text in [
        str(Path(__file__).resolve().parents[1]),
        str(Path(args.source_root).resolve()),
        str(Path(args.runtime_wheelhouse).resolve().parent),
    ]:
        path = Path(path_text).resolve()
        mount = f"{path}:{path}"
        if mount not in mounts:
            mounts.append(mount)
    if args.slurm_container_mounts:
        mounts.extend(item for item in args.slurm_container_mounts.split(",") if item)
    return ",".join(mounts)


def docker_run_command(args: argparse.Namespace, body: str) -> list[str]:
    command = [
        args.docker_bin,
        "run",
        "--rm",
        "--network=host",
        "--ipc=host",
        "--privileged",
        "--workdir",
        args.slurm_container_workdir,
    ]
    if args.docker_gpus:
        command.extend(["--gpus", args.docker_gpus])
    if args.docker_pull:
        command.append("--pull=always")
    for mount in slurm_container_mounts(args).split(","):
        if mount:
            command.extend(["-v", mount])
    if args.docker_extra_args:
        command.extend(shlex.split(args.docker_extra_args))
    command.extend([args.slurm_container_image, "bash", "-lc", body])
    return command


def make_slurm_command(args: argparse.Namespace, body: str) -> list[str]:
    if args.slurm_container_runtime == "docker":
        body = f"exec {shell_words(docker_run_command(args, body))}"
    return [*slurm_srun_prefix(args), "bash", "-lc", body]


def make_uccl_slurm_command(
    args: argparse.Namespace, script: Path, sizes_csv: str
) -> list[str]:
    command = [
        args.container_python,
        str(script),
        "--sizes",
        sizes_csv,
        "--iters",
        str(args.iters),
        "--device",
        args.device,
        "--local-gpu-idx",
        "${LOCAL_RANK}",
        "--num-kvblocks",
        str(args.num_blocks),
    ]
    if args.async_api:
        command.append("--async-api")
    body = f"{slurm_body_prefix(args)}\nexec {shell_words(command)}"
    return make_slurm_command(args, body)


def make_nixl_family_slurm_command(
    args: argparse.Namespace, script: Path, sizes_csv: str, nixl_backend: str
) -> list[str]:
    common = [
        str(script),
        "--sizes",
        sizes_csv,
        "--iters",
        str(args.iters),
        "--device",
        args.device,
        "--local-gpu-idx",
        "${LOCAL_RANK}",
        "--num-kvblocks",
        str(args.num_blocks),
        "--backend",
        nixl_backend,
        "--op-type",
        args.op_type,
    ]
    server = [
        args.container_python,
        *common,
        "--role",
        "server",
        "--remote-ip",
        args.server_bind_ip,
    ]
    client = [
        args.container_python,
        *common,
        "--role",
        "client",
        "--remote-ip",
        "${MASTER_ADDR}",
    ]
    body = "\n".join(
        [
            slurm_body_prefix(args),
            'if [ "${SLURM_PROCID}" = "0" ]; then',
            f"  exec {shell_words(server)}",
            'elif [ "${SLURM_PROCID}" = "1" ]; then',
            f"  exec {shell_words(client)}",
            "else",
            '  echo "BenchP2P: unused Slurm rank ${SLURM_PROCID}; expected ranks 0 and 1"',
            "fi",
        ]
    )
    return make_slurm_command(args, body)


def make_mori_slurm_command(
    args: argparse.Namespace, script: Path, size: int
) -> list[str]:
    common = [
        args.container_python,
        str(script),
        "--backend",
        args.mori_backend,
        "--op-type",
        args.op_type,
        "--buffer-size",
        str(size),
        "--transfer-batch-size",
        str(args.mori_transfer_batch_size),
        "--iters",
        str(args.iters),
    ]
    if args.mori_backend == "xgmi":
        common.extend(
            [
                "--src-gpu",
                str(args.local_gpu_idx),
                "--dst-gpu",
                str(args.dst_gpu_idx),
            ]
        )
        if args.mori_xgmi_multiprocess:
            common.append("--xgmi-multiprocess")
    else:
        common.extend(
            [
                "--host",
                "${MASTER_ADDR}",
                "--num-initiator-dev",
                "1",
                "--num-target-dev",
                "1",
            ]
        )
    body = f"{slurm_body_prefix(args)}\nexec {shell_words(common)}"
    return make_slurm_command(args, body)


def run_specs(
    specs: Sequence[RunSpec], output_dir: Path, timeout_s: int, dry_run: bool
) -> list[RunResult]:
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    results: list[RunResult] = []
    for spec in specs:
        if dry_run:
            log_path = logs_dir / f"{spec.label}.dry_run.log"
            lines = [f"# {spec.backend} dry-run"]
            for idx, command in enumerate(spec.commands):
                label = "server" if spec.kind == "pair" and idx == 0 else "client" if spec.kind == "pair" else "cmd"
                lines.append(f"{label}: {shell_join(command)}")
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            results.append(
                RunResult(
                    spec.backend,
                    spec.label,
                    "dry-run",
                    None,
                    [str(log_path)],
                    [shell_join(cmd) for cmd in spec.commands],
                    0.0,
                )
            )
            continue
        if spec.kind == "pair":
            results.append(run_pair(spec, logs_dir, timeout_s))
        else:
            results.append(run_single(spec, logs_dir, timeout_s))
    return results

def _load_wheel_globs(manifest_path: Path) -> dict[str, str]:
    """Return ``{backend_name_lower: wheel_glob}`` from the third-party manifest."""
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        item["name"].lower(): item.get("wheel_glob", "*.whl")
        for item in data.get("repos", [])
    }


def _find_backend_wheel(
    wheelhouse: Path, backend: str, wheel_glob: str
) -> Path | None:
    backend_dir = wheelhouse / backend
    candidates = sorted(
        (path for path in backend_dir.glob(wheel_glob) if path.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def install_wheels_from_wheelhouse(
    args: argparse.Namespace, output_dir: Path
) -> RunResult:
    """Install pre-built wheels for the requested backends from ``wheelhouse``.

    The wheels themselves are produced by ``scripts/prepare_thirdparty.py``.
    This function does not clone or build anything: if a wheel is missing it
    fails fast with an actionable message pointing the user back at
    ``prepare_thirdparty.py``.
    """
    started = time.perf_counter()
    log_path = output_dir / "install_wheels.log"
    manifest_path = Path(args.manifest).resolve()
    wheelhouse = Path(args.wheelhouse).resolve()
    backends = [
        item.strip().lower() for item in args.backends.split(",") if item.strip()
    ]

    def _result(
        status: str,
        exit_code: int | None,
        commands: list[list[str]],
        error: str,
    ) -> RunResult:
        return RunResult(
            "thirdparty",
            "install-wheels",
            status,
            exit_code,
            [str(log_path)],
            [shell_join(cmd) for cmd in commands],
            time.perf_counter() - started,
            error,
        )

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"Manifest:   {manifest_path}\n")
        log_file.write(f"Wheelhouse: {wheelhouse}\n")
        log_file.write(f"Backends:   {','.join(backends) or '(none)'}\n\n")

        try:
            wheel_globs = _load_wheel_globs(manifest_path)
        except FileNotFoundError:
            error = (
                f"manifest not found: {manifest_path}. "
                "Run: python3 scripts/prepare_thirdparty.py"
            )
            log_file.write(f"ERROR: {error}\n")
            return _result("failed", None, [], error)
        except (OSError, json.JSONDecodeError) as exc:
            error = f"failed to read manifest {manifest_path}: {exc}"
            log_file.write(f"ERROR: {error}\n")
            return _result("failed", None, [], error)

        selected_wheels: list[Path] = []
        missing: list[tuple[str, Path, str]] = []
        for backend in backends:
            wheel_glob = wheel_globs.get(backend)
            if wheel_glob is None:
                log_file.write(
                    f"[{backend}] not declared in manifest, skipping wheel install\n"
                )
                continue
            wheel = _find_backend_wheel(wheelhouse, backend, wheel_glob)
            if wheel is None:
                backend_dir = wheelhouse / backend
                log_file.write(
                    f"[{backend}] missing: no '{wheel_glob}' under {backend_dir}\n"
                )
                missing.append((backend, backend_dir, wheel_glob))
            else:
                log_file.write(f"[{backend}] selected {wheel}\n")
                selected_wheels.append(wheel)

        if missing:
            backend_list = ",".join(name for name, _, _ in missing)
            error = (
                f"missing wheel(s) for: {backend_list}. "
                "Build them with: "
                f"python3 scripts/prepare_thirdparty.py --backends {backend_list}"
            )
            log_file.write(f"\nERROR: {error}\n")
            for name, backend_dir, glob in missing:
                log_file.write(f"  - {name}: expected {backend_dir}/{glob}\n")
            return _result("failed", None, [], error)

        if not selected_wheels:
            log_file.write("\nNo wheels selected; nothing to install.\n")
            return _result("ok", 0, [], "")

        command = [
            args.python,
            "-m",
            "pip",
            "install",
            "--force-reinstall",
            "--no-deps",
            *(str(path) for path in selected_wheels),
        ]
        log_file.write(f"\n$ {shell_join(command)}\n\n")
        log_file.flush()

        if args.dry_run:
            return _result("dry-run", 0, [command], "")

        try:
            completed = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=os.environ.copy(),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=args.install_wheels_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            error = f"pip install timed out after {args.install_wheels_timeout}s"
            log_file.write(f"\nERROR: {error}\n")
            return _result("timeout", None, [command], error)

        if completed.returncode != 0:
            return _result(
                "failed",
                completed.returncode,
                [command],
                f"pip install exited {completed.returncode}",
            )
        return _result("ok", completed.returncode, [command], "")


def run_single(spec: RunSpec, logs_dir: Path, timeout_s: int) -> RunResult:
    log_path = logs_dir / f"{spec.label}.log"
    started = time.perf_counter()
    exit_code: int | None = None
    status = "ok"
    error = ""
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"$ {shell_join(spec.commands[0])}\n\n")
        log_file.flush()
        try:
            completed = subprocess.run(
                spec.commands[0],
                cwd=spec.cwd,
                env=spec.env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_s,
                check=False,
            )
            exit_code = completed.returncode
            if completed.returncode != 0:
                status = "failed"
                error = f"exit code {completed.returncode}"
        except subprocess.TimeoutExpired:
            status = "timeout"
            error = f"timed out after {timeout_s}s"
    elapsed = time.perf_counter() - started
    return RunResult(
        spec.backend,
        spec.label,
        status,
        exit_code,
        [str(log_path)],
        [shell_join(spec.commands[0])],
        elapsed,
        error,
    )


def run_pair(spec: RunSpec, logs_dir: Path, timeout_s: int) -> RunResult:
    server_log = logs_dir / f"{spec.label}.server.log"
    client_log = logs_dir / f"{spec.label}.client.log"
    started = time.perf_counter()
    status = "ok"
    exit_code: int | None = None
    error = ""

    with server_log.open("w", encoding="utf-8") as server_file:
        server_file.write(f"$ {shell_join(spec.commands[0])}\n\n")
        server_file.flush()
        server_proc = subprocess.Popen(
            spec.commands[0],
            cwd=spec.cwd,
            env=spec.env,
            stdout=server_file,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
        time.sleep(spec.startup_seconds)
        try:
            with client_log.open("w", encoding="utf-8") as client_file:
                client_file.write(f"$ {shell_join(spec.commands[1])}\n\n")
                client_file.flush()
                client = subprocess.run(
                    spec.commands[1],
                    cwd=spec.cwd,
                    env=spec.env,
                    stdout=client_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=timeout_s,
                    check=False,
                )
                exit_code = client.returncode
                if client.returncode != 0:
                    status = "failed"
                    error = f"client exit code {client.returncode}"
            try:
                server_exit = server_proc.wait(timeout=max(5, min(60, timeout_s // 4)))
                if status == "ok" and server_exit != 0:
                    status = "failed"
                    exit_code = server_exit
                    error = f"server exit code {server_exit}"
            except subprocess.TimeoutExpired:
                status = "timeout" if status == "ok" else status
                error = error or "server did not exit"
                terminate_process(server_proc)
        except subprocess.TimeoutExpired:
            status = "timeout"
            error = f"client timed out after {timeout_s}s"
            terminate_process(server_proc)
    elapsed = time.perf_counter() - started
    return RunResult(
        spec.backend,
        spec.label,
        status,
        exit_code,
        [str(server_log), str(client_log)],
        [shell_join(cmd) for cmd in spec.commands],
        elapsed,
        error,
    )


def terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        if hasattr(os, "killpg"):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def metrics_from_logs(logs_by_backend: Sequence[tuple[str, Path]]) -> list[Metric]:
    metrics: list[Metric] = []
    for backend, path in logs_by_backend:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        metrics.extend(parse_metrics(backend, text, str(path)))
    return metrics


def parse_metrics(backend: str, text: str, source: str) -> list[Metric]:
    metrics: list[Metric] = []
    for line in text.splitlines():
        match = LOG_LINE_RE.search(line)
        if match:
            size = parse_size(match.group("size"))
            gb_s = float(match.group("gb_s"))
            latency_us = float(match.group("lat_s")) * 1_000_000
            metrics.append(
                Metric(
                    backend=backend,
                    size_bytes=size,
                    gbps=float(match.group("gbps")),
                    gb_s=gb_s,
                    latency_us=latency_us,
                    role=match.group("role"),
                    source=source,
                    raw_line=line.strip(),
                )
            )
            continue
        if backend == "mori":
            mori_metric = parse_mori_table_line(backend, line, source)
            if mori_metric is not None:
                metrics.append(mori_metric)
    return metrics


def parse_mori_table_line(backend: str, line: str, source: str) -> Metric | None:
    if not line.lstrip().startswith("|"):
        return None
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    if len(cells) != 7 or not cells[0].isdigit():
        return None
    try:
        size = int(cells[0])
        batch = int(cells[1])
        gb_s = float(cells[4])
        latency_us = float(cells[6])
    except ValueError:
        return None
    return Metric(
        backend=backend,
        size_bytes=size,
        gbps=gb_s * 8,
        gb_s=gb_s,
        latency_us=latency_us,
        role="initiator",
        batch_size=batch,
        source=source,
        raw_line=line.strip(),
    )


def select_metrics(metrics: Sequence[Metric]) -> list[Metric]:
    grouped: dict[tuple[str, int, str], list[Metric]] = {}
    for metric in metrics:
        grouped.setdefault((metric.backend, metric.size_bytes, metric.operation), []).append(metric)

    selected: list[Metric] = []
    for group in grouped.values():
        group_sorted = sorted(group, key=metric_preference)
        selected.append(group_sorted[0])
    return sorted(selected, key=lambda item: (item.backend, item.size_bytes))


def metric_preference(metric: Metric) -> tuple[int, str]:
    role = metric.role.lower()
    if "client" in role or "initiator" in role or role.startswith("local"):
        score = 0
    elif "server" in role or "target" in role:
        score = 1
    else:
        score = 2
    return score, metric.source


def write_csv(metrics: Sequence[Metric], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "backend",
                "size_bytes",
                "size",
                "batch_size",
                "gbps",
                "gb_per_s",
                "latency_us",
                "role",
                "source",
            ],
        )
        writer.writeheader()
        for metric in metrics:
            writer.writerow(
                {
                    "backend": metric.backend,
                    "size_bytes": metric.size_bytes,
                    "size": human_size(metric.size_bytes),
                    "batch_size": metric.batch_size,
                    "gbps": f"{metric.gbps:.6f}",
                    "gb_per_s": f"{metric.gb_s:.6f}",
                    "latency_us": f"{metric.latency_us:.6f}",
                    "role": metric.role,
                    "source": metric.source,
                }
            )


def write_summary_csv(metrics: Sequence[Metric], path: Path) -> None:
    by_backend: dict[str, list[Metric]] = {}
    for metric in metrics:
        by_backend.setdefault(metric.backend, []).append(metric)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "backend",
                "best_bandwidth_gb_s",
                "best_bandwidth_size",
                "best_latency_us",
                "best_latency_size",
                "points",
            ],
        )
        writer.writeheader()
        for backend in sorted(by_backend):
            values = by_backend[backend]
            best_bw = max(values, key=lambda item: item.gb_s)
            best_lat = min(values, key=lambda item: item.latency_us)
            writer.writerow(
                {
                    "backend": backend,
                    "best_bandwidth_gb_s": f"{best_bw.gb_s:.6f}",
                    "best_bandwidth_size": human_size(best_bw.size_bytes),
                    "best_latency_us": f"{best_lat.latency_us:.6f}",
                    "best_latency_size": human_size(best_lat.size_bytes),
                    "points": len(values),
                }
            )


def write_markdown(metrics: Sequence[Metric], path: Path, run_results: Sequence[RunResult]) -> None:
    lines = [
        "# P2P benchmark comparison",
        "",
        "## Per-size results",
        "",
        "| Backend | Size | Batch | Bandwidth (GB/s) | Bandwidth (Gbps) | Latency (us) | Role |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for metric in metrics:
        lines.append(
            "| {backend} | {size} | {batch} | {gb_s:.3f} | {gbps:.3f} | {lat:.3f} | {role} |".format(
                backend=metric.backend,
                size=human_size(metric.size_bytes),
                batch=metric.batch_size,
                gb_s=metric.gb_s,
                gbps=metric.gbps,
                lat=metric.latency_us,
                role=metric.role or "-",
            )
        )

    lines.extend(["", "## Backend summary", ""])
    summary_rows = summarize_backends(metrics)
    lines.extend(
        [
            "| Backend | Best bandwidth | Bandwidth size | Best latency | Latency size | Points |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary_rows:
        lines.append(
            "| {backend} | {bw:.3f} GB/s | {bw_size} | {lat:.3f} us | {lat_size} | {points} |".format(
                **row
            )
        )

    if run_results:
        lines.extend(["", "## Run status", ""])
        lines.extend(["| Backend | Label | Status | Elapsed (s) | Error |", "|---|---|---|---:|---|"])
        for result in run_results:
            lines.append(
                "| {backend} | {label} | {status} | {elapsed:.1f} | {error} |".format(
                    backend=result.backend,
                    label=result.label,
                    status=result.status,
                    elapsed=result.elapsed_s,
                    error=result.error or "-",
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize_backends(metrics: Sequence[Metric]) -> list[dict[str, object]]:
    by_backend: dict[str, list[Metric]] = {}
    for metric in metrics:
        by_backend.setdefault(metric.backend, []).append(metric)
    rows: list[dict[str, object]] = []
    for backend in sorted(by_backend):
        values = by_backend[backend]
        best_bw = max(values, key=lambda item: item.gb_s)
        best_lat = min(values, key=lambda item: item.latency_us)
        rows.append(
            {
                "backend": backend,
                "bw": best_bw.gb_s,
                "bw_size": human_size(best_bw.size_bytes),
                "lat": best_lat.latency_us,
                "lat_size": human_size(best_lat.size_bytes),
                "points": len(values),
            }
        )
    return rows


def write_run_json(run_results: Sequence[RunResult], path: Path) -> None:
    payload = [dataclasses.asdict(result) for result in run_results]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_metrics_json(metrics: Sequence[Metric], path: Path) -> None:
    payload = [dataclasses.asdict(metric) for metric in metrics]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_svg(metrics: Sequence[Metric], path: Path) -> None:
    width = 1120
    height = 760
    margin_left = 90
    margin_right = 40
    chart_width = width - margin_left - margin_right
    top_y = 70
    chart_height = 260
    gap = 110
    bottom_y = top_y + chart_height + gap
    sizes = sorted({metric.size_bytes for metric in metrics})
    backends = sorted({metric.backend for metric in metrics})
    colors = {
        "mori": "#1f77b4",
        "mooncake": "#ff7f0e",
        "uccl": "#2ca02c",
        "nixl": "#d62728",
    }

    def x_for(size: int) -> float:
        if len(sizes) == 1:
            return margin_left + chart_width / 2
        index = sizes.index(size)
        return margin_left + index * (chart_width / (len(sizes) - 1))

    max_bw = max((metric.gb_s for metric in metrics), default=1.0)
    max_lat = max((metric.latency_us for metric in metrics), default=1.0)
    max_bw = nice_max(max_bw)
    max_lat = nice_max(max_lat)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text{font-family:Arial,Helvetica,sans-serif;font-size:13px;fill:#222}",
        ".title{font-size:22px;font-weight:700}",
        ".subtitle{font-size:16px;font-weight:700}",
        ".axis{stroke:#333;stroke-width:1}",
        ".grid{stroke:#ddd;stroke-width:1}",
        ".series{fill:none;stroke-width:2.5}",
        ".point{stroke:#fff;stroke-width:1.5}",
        "</style>",
        f'<text class="title" x="{margin_left}" y="32">P2P bandwidth and latency comparison</text>',
    ]
    parts.extend(draw_chart_axes(margin_left, top_y, chart_width, chart_height, max_bw, "Bandwidth (GB/s)"))
    parts.extend(draw_chart_axes(margin_left, bottom_y, chart_width, chart_height, max_lat, "Latency (us)"))

    for backend in backends:
        values = [metric for metric in metrics if metric.backend == backend]
        values.sort(key=lambda item: item.size_bytes)
        color = colors.get(backend, color_for_name(backend))
        bw_points = [(x_for(item.size_bytes), scale_y(item.gb_s, top_y, chart_height, max_bw)) for item in values]
        lat_points = [
            (x_for(item.size_bytes), scale_y(item.latency_us, bottom_y, chart_height, max_lat))
            for item in values
        ]
        parts.append(polyline(bw_points, color))
        parts.append(polyline(lat_points, color))
        for x, y in bw_points + lat_points:
            parts.append(f'<circle class="point" cx="{x:.2f}" cy="{y:.2f}" r="4" fill="{color}"/>')

    label_y = bottom_y + chart_height + 45
    for size in sizes:
        x = x_for(size)
        parts.append(
            f'<text x="{x:.2f}" y="{label_y}" text-anchor="end" transform="rotate(-35 {x:.2f} {label_y})">{html.escape(human_size(size))}</text>'
        )
    parts.append(f'<text x="{margin_left + chart_width / 2:.2f}" y="{height - 22}" text-anchor="middle">Message size</text>')

    legend_x = margin_left
    legend_y = 50
    for idx, backend in enumerate(backends):
        x = legend_x + idx * 145
        color = colors.get(backend, color_for_name(backend))
        parts.append(f'<line x1="{x}" y1="{legend_y}" x2="{x + 28}" y2="{legend_y}" stroke="{color}" stroke-width="3"/>')
        parts.append(f'<text x="{x + 36}" y="{legend_y + 4}">{html.escape(backend)}</text>')

    parts.append("</svg>")
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def draw_chart_axes(
    x: int, y: int, width: int, height: int, max_value: float, title: str
) -> list[str]:
    parts = [
        f'<text class="subtitle" x="{x}" y="{y - 18}">{html.escape(title)}</text>',
        f'<line class="axis" x1="{x}" y1="{y + height}" x2="{x + width}" y2="{y + height}"/>',
        f'<line class="axis" x1="{x}" y1="{y}" x2="{x}" y2="{y + height}"/>',
    ]
    for idx in range(6):
        value = max_value * idx / 5
        tick_y = y + height - (height * idx / 5)
        parts.append(f'<line class="grid" x1="{x}" y1="{tick_y:.2f}" x2="{x + width}" y2="{tick_y:.2f}"/>')
        parts.append(f'<text x="{x - 10}" y="{tick_y + 4:.2f}" text-anchor="end">{value:.2g}</text>')
    return parts


def nice_max(value: float) -> float:
    if value <= 0:
        return 1.0
    exponent = math.floor(math.log10(value))
    fraction = value / (10**exponent)
    if fraction <= 1:
        nice = 1
    elif fraction <= 2:
        nice = 2
    elif fraction <= 5:
        nice = 5
    else:
        nice = 10
    return nice * (10**exponent)


def scale_y(value: float, chart_y: int, chart_height: int, max_value: float) -> float:
    if max_value <= 0:
        return chart_y + chart_height
    return chart_y + chart_height - (value / max_value) * chart_height


def polyline(points: Sequence[tuple[float, float]], color: str) -> str:
    if not points:
        return ""
    coords = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    return f'<polyline class="series" points="{coords}" stroke="{color}"/>'


def color_for_name(name: str) -> str:
    palette = ["#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
    return palette[sum(ord(char) for char in name) % len(palette)]


def parse_from_log_args(items: Sequence[str]) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for item in items:
        if "=" not in item:
            raise argparse.ArgumentTypeError("--from-log expects BACKEND=PATH")
        backend, path = item.split("=", 1)
        parsed.append((backend.strip().lower(), Path(path).expanduser()))
    return parsed


def collect_logs_from_results(run_results: Sequence[RunResult]) -> list[tuple[str, Path]]:
    logs: list[tuple[str, Path]] = []
    for result in run_results:
        for log_path in result.logs:
            logs.append((result.backend, Path(log_path)))
    return logs


def write_reports(
    metrics: Sequence[Metric], run_results: Sequence[RunResult], output_dir: Path
) -> dict[str, str]:
    selected = select_metrics(metrics)
    paths = {
        "csv": output_dir / "p2p_results.csv",
        "summary_csv": output_dir / "p2p_summary.csv",
        "markdown": output_dir / "p2p_results.md",
        "svg": output_dir / "p2p_comparison.svg",
        "metrics_json": output_dir / "p2p_metrics.json",
        "runs_json": output_dir / "run_results.json",
    }
    write_csv(selected, paths["csv"])
    write_summary_csv(selected, paths["summary_csv"])
    write_markdown(selected, paths["markdown"], run_results)
    write_metrics_json(selected, paths["metrics_json"])
    write_run_json(run_results, paths["runs_json"])
    write_svg(selected, paths["svg"])
    return {key: str(value) for key, value in paths.items()}


def print_console_summary(metrics: Sequence[Metric], paths: dict[str, str]) -> None:
    selected = select_metrics(metrics)
    if not selected:
        print("No benchmark metrics were parsed.")
    else:
        print("\nBackend summary:")
        print(f"{'Backend':<12} {'Best GB/s':>12} {'BW size':>12} {'Best us':>12} {'Lat size':>12}")
        for row in summarize_backends(selected):
            print(
                f"{row['backend']:<12} {row['bw']:>12.3f} {row['bw_size']:>12} {row['lat']:>12.3f} {row['lat_size']:>12}"
            )
    print("\nReports:")
    for name in ["csv", "summary_csv", "markdown", "svg", "runs_json"]:
        path = paths.get(name)
        if path:
            print(f"  {name}: {path}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare MORI, Mooncake, UCCL, and NIXL P2P bandwidth/latency."
    )
    parser.add_argument(
        "--backends",
        default=",".join(DEFAULT_BACKENDS),
        help="Comma-separated backends to run: mori,mooncake,uccl,nixl",
    )
    parser.add_argument(
        "--sizes",
        type=parse_size_list,
        default=DEFAULT_SIZES,
        help="Comma-separated message sizes, e.g. 256,1K,1M,16M",
    )
    parser.add_argument("--iters", type=int, default=10, help="Iterations per size")
    parser.add_argument("--num-blocks", type=int, default=1, help="IOV/KV blocks per transfer")
    parser.add_argument("--device", choices=["cpu", "gpu"], default="gpu")
    parser.add_argument("--local-gpu-idx", type=int, default=0)
    parser.add_argument("--dst-gpu-idx", type=int, default=1)
    parser.add_argument("--op-type", choices=["write", "read"], default="write")
    parser.add_argument("--async-api", action="store_true", help="Use UCCL async path")
    parser.add_argument(
        "--launcher",
        choices=["local", "slurm"],
        default="slurm",
        help="Use local processes or Slurm srun to launch P2P benchmarks",
    )
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--torchrun", default="torchrun")
    parser.add_argument("--source-root", default=str(default_source_root()))
    parser.add_argument("--thirdparty-dir", default=str(default_thirdparty_dir()))
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Path to the third-party manifest. Defaults to "
            "<thirdparty-dir>/manifest.json."
        ),
    )
    parser.add_argument(
        "--wheelhouse",
        default=None,
        help=(
            "Directory containing per-backend wheel subfolders "
            "(<wheelhouse>/<backend>/<wheel>). Defaults to "
            "<thirdparty-dir>/wheelhouse, the same path used by "
            "prepare_thirdparty.py."
        ),
    )
    parser.add_argument(
        "--skip-install-wheels",
        action="store_true",
        help=(
            "Do not pip-install backend wheels from --wheelhouse before running "
            "benchmarks. Use this when the wheels are already installed in the "
            "active environment."
        ),
    )
    parser.add_argument(
        "--skip-prepare-thirdparty",
        dest="skip_install_wheels",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--install-wheels-timeout",
        type=int,
        default=900,
        help="Timeout (seconds) for the pip install step.",
    )
    parser.add_argument(
        "--install-thirdparty-on-host",
        action="store_true",
        help="Also install built wheels on the submission host in Slurm container mode",
    )
    parser.add_argument(
        "--runtime-wheelhouse",
        default=None,
        help="Wheelhouse path installed inside Slurm runtime containers",
    )
    parser.add_argument(
        "--skip-runtime-wheel-install",
        action="store_true",
        help="Do not install wheelhouse packages inside Slurm runtime tasks",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only write generated commands")
    parser.add_argument(
        "--from-log",
        action="append",
        default=[],
        metavar="BACKEND=PATH",
        help="Parse an existing backend log instead of, or in addition to, running commands",
    )
    parser.add_argument("--server-ip", default="127.0.0.1", help="Client-visible server IP")
    parser.add_argument("--server-bind-ip", default="0.0.0.0", help="Server bind IP for pair runners")
    parser.add_argument("--pair-startup-seconds", type=float, default=2.0)
    parser.add_argument("--srun", default="srun")
    parser.add_argument("--slurm-nodes", type=int, default=2)
    parser.add_argument("--slurm-ntasks", type=int, default=2)
    parser.add_argument("--slurm-ntasks-per-node", type=int, default=1)
    parser.add_argument("--slurm-master-port", type=int, default=29500)
    parser.add_argument("--slurm-partition", default=None)
    parser.add_argument("--slurm-account", default=None)
    parser.add_argument("--slurm-qos", default=None)
    parser.add_argument("--slurm-time", default=None)
    parser.add_argument("--slurm-constraint", default=None)
    parser.add_argument("--slurm-gres", default=None, help="Example: gpu:1")
    parser.add_argument("--slurm-gpus-per-task", default=None)
    parser.add_argument("--slurm-cpus-per-task", default=None)
    parser.add_argument("--slurm-job-name", default="benchp2p")
    parser.add_argument(
        "--slurm-container-runtime",
        choices=["docker", "pyxis", "none"],
        default="docker",
        help="How Slurm tasks enter the runtime container",
    )
    parser.add_argument("--slurm-container-image", default="docker.io/rocm/primus:v26.2")
    parser.add_argument(
        "--no-slurm-container",
        action="store_const",
        const="none",
        dest="slurm_container_runtime",
        help="Disable Slurm runtime container wrapping",
    )
    parser.add_argument(
        "--slurm-container-mounts",
        default="",
        help="Extra Pyxis container mounts appended to the generated mounts",
    )
    parser.add_argument(
        "--slurm-container-workdir",
        default=str(Path(__file__).resolve().parents[1]),
    )
    parser.add_argument("--container-python", default="python3")
    parser.add_argument("--docker-bin", default="docker")
    parser.add_argument("--docker-gpus", default="all")
    parser.add_argument("--docker-pull", action="store_true")
    parser.add_argument("--docker-extra-args", default="")
    parser.add_argument(
        "--slurm-extra-args",
        default="",
        help="Extra arguments appended to srun, parsed with shlex",
    )
    parser.add_argument("--mori-backend", choices=["rdma", "xgmi"], default="rdma")
    parser.add_argument("--mori-host", default="127.0.0.1")
    parser.add_argument(
        "--mori-transfer-batch-size",
        type=int,
        default=1,
        help="MORI IO transfer batch size; use 128 to match the MORI README example",
    )
    parser.add_argument("--mori-xgmi-multiprocess", action="store_true")
    parser.add_argument("--uccl-script", default=None)
    parser.add_argument("--nixl-script", default=None)
    parser.add_argument("--mooncake-script", default=None)
    parser.add_argument("--mori-script", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if (
        args.source_root == str(default_source_root())
        and args.thirdparty_dir != str(default_thirdparty_dir())
    ):
        args.source_root = args.thirdparty_dir
    thirdparty_dir = Path(args.thirdparty_dir).resolve()
    if args.manifest is None:
        args.manifest = str(thirdparty_dir / "manifest.json")
    if args.wheelhouse is None:
        args.wheelhouse = str(thirdparty_dir / "wheelhouse")
    output_dir = Path(args.output_dir).resolve() if args.output_dir else output_dir_for(Path("results"))
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.runtime_wheelhouse is None:
        args.runtime_wheelhouse = args.wheelhouse

    run_results: list[RunResult] = []
    log_sources: list[tuple[str, Path]] = []
    should_run_benchmarks = not args.from_log or args.dry_run
    use_slurm_container = (
        args.launcher == "slurm"
        and args.slurm_container_image
        and args.slurm_container_runtime != "none"
    )

    if args.from_log:
        log_sources.extend(parse_from_log_args(args.from_log))

    if (
        should_run_benchmarks
        and not args.skip_install_wheels
        and (not use_slurm_container or args.install_thirdparty_on_host)
    ):
        install_result = install_wheels_from_wheelhouse(args, output_dir)
        run_results.append(install_result)
        log_sources.extend(collect_logs_from_results([install_result]))
        if install_result.status not in {"ok", "dry-run"}:
            paths = write_reports(metrics_from_logs(log_sources), run_results, output_dir)
            print_console_summary(metrics_from_logs(log_sources), paths)
            print(f"\n{install_result.error}", file=sys.stderr)
            print(
                "Hint: build the missing wheels with "
                "`python3 scripts/prepare_thirdparty.py` and rerun, or pass "
                "`--skip-install-wheels` if the backends are already installed.",
                file=sys.stderr,
            )
            return 1

    if should_run_benchmarks:
        specs, skipped = make_run_specs(args, output_dir)
        run_results.extend(skipped)
        bench_results = run_specs(specs, output_dir, args.timeout, args.dry_run)
        run_results.extend(bench_results)
        log_sources.extend(collect_logs_from_results(bench_results))

    metrics = metrics_from_logs(log_sources)
    paths = write_reports(metrics, run_results, output_dir)
    print_console_summary(metrics, paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
