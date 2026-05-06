#!/usr/bin/env python3
"""Prepare BenchP2P third-party wheels inside the runtime container."""

from __future__ import annotations

import argparse
import fcntl
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build BenchP2P third-party wheels inside a Slurm runtime container"
    )
    parser.add_argument("--backends", required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--wheelhouse", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--prepare-thirdparty-script", required=True)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--skip-clone", action="store_true")
    return parser.parse_args(argv)


def env_rank() -> int:
    return int(os.environ.get("SLURM_PROCID", os.environ.get("RANK", "0")))


def marker_path(wheelhouse: Path) -> Path:
    job_id = os.environ.get("SLURM_JOB_ID") or os.environ.get("BENCHP2P_JOB_ID") or "single"
    return wheelhouse / f".benchp2p_prepare_{job_id}.done"


def wait_for_marker(marker: Path, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if marker.exists():
            return
        time.sleep(2.0)
    raise SystemExit(f"Timed out waiting for container wheel build marker: {marker}")


def run(command: list[str], cwd: Path, timeout_s: int) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=os.environ.copy(), check=True, timeout=timeout_s)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    source_root = Path(args.source_root).resolve()
    wheelhouse = Path(args.wheelhouse).resolve()
    wheelhouse.mkdir(parents=True, exist_ok=True)

    marker = marker_path(wheelhouse)
    rank = env_rank()
    if rank != 0:
        print(f"Waiting for rank 0 to build third-party wheels: {marker}", flush=True)
        wait_for_marker(marker, args.timeout)
        return 0

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
        str(source_root),
        "--wheelhouse",
        str(wheelhouse),
        "--backends",
        args.backends,
        "--python",
        sys.executable,
        "--timeout",
        str(args.timeout),
        "--container-build",
    ]
    if args.skip_clone:
        command.append("--skip-clone")

    lock_path = wheelhouse / ".benchp2p_prepare.lock"
    with lock_path.open("w", encoding="utf-8") as lock_file:
        print(f"Acquiring container wheel build lock: {lock_path}", flush=True)
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            run(command, source_root, args.timeout)
            marker.write_text(f"built_at={time.time()}\n", encoding="utf-8")
            print(f"Container wheel build complete: {marker}", flush=True)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
