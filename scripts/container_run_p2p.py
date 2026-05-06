#!/usr/bin/env python3
"""Run BenchP2P backend benchmarks inside the runtime container."""

from __future__ import annotations

import argparse
import fcntl
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run P2P benchmarks inside container")
    parser.add_argument("--backends", required=True)
    parser.add_argument("--sizes", required=True)
    parser.add_argument("--iters", type=int, required=True)
    parser.add_argument("--num-blocks", type=int, default=1)
    parser.add_argument("--device", choices=["cpu", "gpu"], default="gpu")
    parser.add_argument("--op-type", choices=["write", "read"], default="write")
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--wheelhouse", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--prepare-thirdparty-script", required=True)
    parser.add_argument("--prepare-thirdparty-in-container", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--prepare-thirdparty-timeout", type=int, default=3600)
    parser.add_argument("--prepare-thirdparty-skip-clone", action="store_true")
    parser.add_argument("--skip-runtime-wheel-install", action="store_true")
    parser.add_argument("--pair-startup-seconds", type=float, default=2.0)
    parser.add_argument("--async-api", action="store_true")
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
    parser.add_argument("--nixl-script", default=None)
    parser.add_argument("--mooncake-script", default=None)
    parser.add_argument("--mori-script", default=None)
    return parser.parse_args(argv)


def backend_script_paths(source_root: Path, args: argparse.Namespace) -> dict[str, Path]:
    return {
        "uccl": Path(args.uccl_script)
        if args.uccl_script
        else source_root / "uccl" / "p2p" / "benchmarks" / "benchmark_uccl.py",
        "nixl": Path(args.nixl_script) if args.nixl_script else Path(args.nixlbench_bin),
        "mooncake": Path(args.mooncake_script)
        if args.mooncake_script
        else Path(args.mooncake_bench_bin),
        "mori": Path(args.mori_script)
        if args.mori_script
        else source_root / "mori" / "tests" / "python" / "io" / "benchmark.py",
    }


def install_wheels(wheelhouse: Path) -> None:
    wheels = sorted(str(path) for path in wheelhouse.glob("*/*.whl"))
    if not wheels:
        raise SystemExit(f"No wheel files found under {wheelhouse}")
    print("Installing BenchP2P wheelhouse inside runtime container:", flush=True)
    for wheel in wheels:
        print(f"  {wheel}", flush=True)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--force-reinstall", "--no-deps", *wheels],
        check=True,
    )


def prepare_marker_path(wheelhouse: Path, env: dict[str, str]) -> Path:
    job_id = env.get("SLURM_JOB_ID") or env.get("BENCHP2P_JOB_ID") or "single"
    return wheelhouse / f".benchp2p_prepare_{job_id}.done"


def wait_for_prepare(marker: Path, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if marker.exists():
            return
        time.sleep(2.0)
    raise SystemExit(f"Timed out waiting for container wheel build marker: {marker}")


def prepare_thirdparty_in_container(args: argparse.Namespace, env: dict[str, str]) -> None:
    wheelhouse = Path(args.wheelhouse).resolve()
    wheelhouse.mkdir(parents=True, exist_ok=True)
    marker = prepare_marker_path(wheelhouse, env)
    lock_path = wheelhouse / ".benchp2p_prepare.lock"
    rank = int(env["RANK"])

    if rank != 0:
        print(f"Waiting for rank 0 to build third-party wheels: {marker}", flush=True)
        wait_for_prepare(marker, args.prepare_thirdparty_timeout)
        return

    try:
        marker.unlink()
    except FileNotFoundError:
        pass

    command = [
        sys.executable,
        str(Path(args.prepare_thirdparty_script).resolve()),
        "--manifest",
        str(Path(args.manifest).resolve()),
        "--thirdparty-dir",
        str(Path(args.source_root).resolve()),
        "--wheelhouse",
        str(wheelhouse),
        "--backends",
        args.backends,
        "--python",
        sys.executable,
        "--timeout",
        str(args.prepare_thirdparty_timeout),
        "--skip-install",
    ]
    if args.prepare_thirdparty_skip_clone:
        command.append("--skip-clone")

    with lock_path.open("w", encoding="utf-8") as lock_file:
        print(f"Acquiring container wheel build lock: {lock_path}", flush=True)
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            run(command, env, Path(args.source_root).resolve())
            marker.write_text(f"built_at={time.time()}\n", encoding="utf-8")
            print(f"Container wheel build complete: {marker}", flush=True)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def env_with_dist() -> dict[str, str]:
    env = os.environ.copy()
    rank = env.get("SLURM_PROCID", env.get("RANK", "0"))
    world = env.get("SLURM_NTASKS", env.get("WORLD_SIZE", "1"))
    local_rank = env.get("SLURM_LOCALID", env.get("LOCAL_RANK", "0"))
    env["RANK"] = rank
    env["WORLD_SIZE"] = world
    env["LOCAL_RANK"] = local_rank
    env.setdefault("LOCAL_WORLD_SIZE", env.get("SLURM_NTASKS_PER_NODE", "1"))
    return env


def run(command: list[str], env: dict[str, str], cwd: Path) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def parse_sizes(sizes_csv: str) -> list[str]:
    return [item.strip() for item in sizes_csv.split(",") if item.strip()]


def memory_type(args: argparse.Namespace) -> str:
    return "VRAM" if args.device == "gpu" else "DRAM"


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


def terminate_process(proc: subprocess.Popen[object]) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def start_etcd_if_needed(args: argparse.Namespace, env: dict[str, str]) -> subprocess.Popen[object] | None:
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
        etcd,
        "--data-dir",
        data_dir,
        "--listen-client-urls",
        "http://0.0.0.0:2379",
        "--advertise-client-urls",
        f"http://{master}:2379",
        "--listen-peer-urls",
        "http://0.0.0.0:2380",
        "--initial-advertise-peer-urls",
        f"http://{master}:2380",
        "--initial-cluster",
        f"default=http://{master}:2380",
        "--log-level",
        "error",
    ]
    print("+ " + " ".join(command), flush=True)
    proc: subprocess.Popen[object] = subprocess.Popen(command)
    time.sleep(2.0)
    return proc


def run_uccl(script: Path, args: argparse.Namespace, env: dict[str, str], cwd: Path) -> None:
    command = [
        sys.executable,
        str(script),
        "--sizes",
        args.sizes,
        "--iters",
        str(args.iters),
        "--device",
        args.device,
        "--local-gpu-idx",
        env["LOCAL_RANK"],
        "--num-kvblocks",
        str(args.num_blocks),
    ]
    if args.async_api:
        command.append("--async-api")
    run(command, env, cwd)


def run_nixl_family(
    backend: str, script: Path, args: argparse.Namespace, env: dict[str, str], cwd: Path
) -> None:
    rank = int(env["RANK"])
    if rank > 1:
        print(f"[{backend}] skipping unused rank {rank}", flush=True)
        return
    role = "server" if rank == 0 else "client"
    remote_ip = "0.0.0.0" if rank == 0 else env["MASTER_ADDR"]
    if rank == 1 and args.pair_startup_seconds > 0:
        time.sleep(args.pair_startup_seconds)
    nixl_backend = "ucx" if backend == "nixl" else "mooncake"
    command = [
        sys.executable,
        str(script),
        "--sizes",
        args.sizes,
        "--iters",
        str(args.iters),
        "--device",
        args.device,
        "--local-gpu-idx",
        env["LOCAL_RANK"],
        "--num-kvblocks",
        str(args.num_blocks),
        "--backend",
        nixl_backend,
        "--op-type",
        args.op_type,
        "--role",
        role,
        "--remote-ip",
        remote_ip,
    ]
    run(command, env, cwd)


def run_nixlbench(args: argparse.Namespace, env: dict[str, str], cwd: Path) -> None:
    rank = int(env["RANK"])
    if rank > 1:
        print(f"[nixl] skipping unused rank {rank}", flush=True)
        return
    if rank == 1 and args.pair_startup_seconds > 0:
        time.sleep(args.pair_startup_seconds)
    binary = require_executable(
        args.nixlbench_bin,
        [
            cwd / "nixl" / "benchmark" / "nixlbench" / "build" / "nixlbench",
            cwd / "nixl" / "benchmark" / "nixlbench" / "build" / "src" / "nixlbench",
        ],
    )
    endpoints = args.nixl_etcd_endpoints or f"http://{env['MASTER_ADDR']}:2379"
    seg_type = memory_type(args)
    etcd_proc = start_etcd_if_needed(args, env)
    try:
        for size in parse_sizes(args.sizes):
            command = [
                binary,
                "--etcd_endpoints",
                endpoints,
                "--backend",
                args.nixl_backend,
                "--initiator_seg_type",
                seg_type,
                "--target_seg_type",
                seg_type,
                "--scheme",
                "pairwise",
                "--op_type",
                args.op_type.upper(),
                "--total_buffer_size",
                str(max(int(size) * args.num_blocks * 4, 1 << 30)),
                "--start_block_size",
                size,
                "--max_block_size",
                size,
                "--start_batch_size",
                str(args.num_blocks),
                "--max_batch_size",
                str(args.num_blocks),
                "--num_iter",
                str(args.iters),
                "--warmup_iter",
                str(max(1, min(100, args.iters))),
                "--num_threads",
                "1",
                "--num_initiator_dev",
                "1",
                "--num_target_dev",
                "1",
            ]
            if args.nixl_device_list:
                command.extend(["--device_list", args.nixl_device_list])
            run(command, env, cwd)
    finally:
        if etcd_proc is not None:
            terminate_process(etcd_proc)


def mooncake_segment_file(cwd: Path, env: dict[str, str], size: str) -> Path:
    job = env.get("SLURM_JOB_ID", str(os.getppid()))
    runtime_dir = cwd / ".benchp2p_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir / f"mooncake_target_seg_{job}_{size}.txt"


def mooncake_base_command(
    binary: str, args: argparse.Namespace, env: dict[str, str], size: str
) -> list[str]:
    seg_type = memory_type(args)
    command = [
        binary,
        "--seg_type",
        seg_type,
        "--backend",
        args.mooncake_backend,
        "--op_type",
        args.op_type,
        "--total_buffer_size",
        str(max(int(size) * args.num_blocks * 4, 1 << 30)),
        "--start_block_size",
        size,
        "--max_block_size",
        size,
        "--start_batch_size",
        str(args.num_blocks),
        "--max_batch_size",
        str(args.num_blocks),
        "--start_num_threads",
        "1",
        "--max_num_threads",
        "1",
        "--duration",
        str(args.mooncake_duration),
        "--local_gpu_id",
        env["LOCAL_RANK"],
        "--target_gpu_id",
        "0",
    ]
    if args.mooncake_backend == "tent":
        command.extend(
            [
                "--metadata_type",
                "p2p",
            ]
        )
        if args.mooncake_xport_type:
            command.extend(["--xport_type", args.mooncake_xport_type])
    return command


def run_mooncake_tebench(args: argparse.Namespace, env: dict[str, str], cwd: Path) -> None:
    rank = int(env["RANK"])
    if rank > 1:
        print(f"[mooncake] skipping unused rank {rank}", flush=True)
        return
    binary = require_executable(
        args.mooncake_bench_bin,
        [
            cwd / "Mooncake" / "build" / "mooncake-transfer-engine" / "benchmark" / "tebench",
            cwd / "Mooncake" / "build" / "tebench",
        ],
    )
    for size in parse_sizes(args.sizes):
        segment_file = mooncake_segment_file(cwd, env, size)
        command = mooncake_base_command(binary, args, env, size)
        if rank == 0:
            try:
                segment_file.unlink()
            except FileNotFoundError:
                pass
            print("+ " + " ".join(command), flush=True)
            target = subprocess.Popen(
                command,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
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
                print(line, end="", flush=True)
                match = re.search(r"--target_seg_name=([^\s]+)", line)
                if match:
                    target_seg_name = match.group(1)
                    segment_file.write_text(target_seg_name + "\n", encoding="utf-8")
                    break
            if target_seg_name is None:
                terminate_process(target)
                raise SystemExit("Mooncake tebench target did not print --target_seg_name")
            time.sleep(args.pair_startup_seconds + args.mooncake_duration + 10)
            terminate_process(target)
        else:
            deadline = time.monotonic() + args.pair_startup_seconds + 20
            while not segment_file.exists() and time.monotonic() < deadline:
                time.sleep(0.5)
            if not segment_file.exists():
                raise SystemExit(f"Mooncake target segment file not found: {segment_file}")
            target_seg_name = segment_file.read_text(encoding="utf-8").strip()
            command.extend(["--target_seg_name", target_seg_name])
            run(command, env, cwd)


def run_mori(script: Path, args: argparse.Namespace, env: dict[str, str], cwd: Path) -> None:
    for size in args.sizes.split(","):
        command = [
            sys.executable,
            str(script),
            "--backend",
            args.mori_backend,
            "--op-type",
            args.op_type,
            "--buffer-size",
            size,
            "--transfer-batch-size",
            str(args.mori_transfer_batch_size),
            "--iters",
            str(args.iters),
        ]
        if args.mori_backend == "xgmi":
            command.extend(["--src-gpu", "0", "--dst-gpu", "1"])
            if args.mori_xgmi_multiprocess:
                command.append("--xgmi-multiprocess")
        else:
            command.extend(
                [
                    "--host",
                    env["MASTER_ADDR"],
                    "--num-initiator-dev",
                    "1",
                    "--num-target-dev",
                    "1",
                ]
            )
        run(command, env, cwd)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    source_root = Path(args.source_root).resolve()
    wheelhouse = Path(args.wheelhouse).resolve()
    env = env_with_dist()
    scripts = backend_script_paths(source_root, args)

    print(
        f"BenchP2P container runner rank={env['RANK']} local_rank={env['LOCAL_RANK']} "
        f"master={env.get('MASTER_ADDR', '')}",
        flush=True,
    )
    if args.prepare_thirdparty_in_container:
        prepare_thirdparty_in_container(args, env)
    if not args.skip_runtime_wheel_install:
        install_wheels(wheelhouse)

    for backend in [item.strip().lower() for item in args.backends.split(",") if item.strip()]:
        script = scripts.get(backend)
        if script is None:
            raise SystemExit(f"unknown backend: {backend}")
        if backend in {"uccl", "mori"} and not script.exists():
            raise SystemExit(f"benchmark script not found for {backend}: {script}")
        print(f"\n==> Running {backend}", flush=True)
        if backend == "uccl":
            run_uccl(script, args, env, source_root)
        elif backend == "nixl":
            run_nixlbench(args, env, source_root)
        elif backend == "mooncake":
            run_mooncake_tebench(args, env, source_root)
        elif backend == "mori":
            run_mori(script, args, env, source_root)
        else:
            raise SystemExit(f"unknown backend: {backend}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
