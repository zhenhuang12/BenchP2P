#!/usr/bin/env python3
"""Clone third-party P2P stacks, build wheels, and install them."""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence


@dataclasses.dataclass(frozen=True)
class RepoSpec:
    name: str
    repo: str
    ref: str
    path: str
    build_path: str
    wheel_glob: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_manifest() -> Path:
    return repo_root() / "3rdparty" / "manifest.json"


def default_thirdparty_dir() -> Path:
    return repo_root() / "3rdparty"


def shell_join(command: Sequence[str]) -> str:
    return shlex.join(str(part) for part in command)


def load_manifest(path: Path) -> list[RepoSpec]:
    data = json.loads(path.read_text(encoding="utf-8"))
    specs = []
    for item in data.get("repos", []):
        specs.append(
            RepoSpec(
                name=item["name"],
                repo=item["repo"],
                ref=item.get("ref", "main"),
                path=item["path"],
                build_path=item.get("build_path", "."),
                wheel_glob=item.get("wheel_glob", "*.whl"),
            )
        )
    return specs


def run_command(
    command: Sequence[str],
    cwd: Path,
    env: dict[str, str],
    dry_run: bool,
    timeout: int | None = None,
) -> None:
    print(f"+ cd {cwd} && {shell_join(command)}", flush=True)
    if dry_run:
        return
    subprocess.run(command, cwd=cwd, env=env, check=True, timeout=timeout)


def ensure_checkout(
    spec: RepoSpec,
    thirdparty_dir: Path,
    env: dict[str, str],
    dry_run: bool,
    skip_clone: bool,
    timeout: int,
) -> Path:
    checkout = thirdparty_dir / spec.path
    if skip_clone:
        return checkout

    thirdparty_dir.mkdir(parents=True, exist_ok=True)
    if not checkout.exists():
        run_command(
            ["git", "clone", "--recursive", spec.repo, str(checkout)],
            cwd=thirdparty_dir,
            env=env,
            dry_run=dry_run,
            timeout=timeout,
        )
    elif (checkout / ".git").exists():
        run_command(
            ["git", "fetch", "--tags", "origin"],
            cwd=checkout,
            env=env,
            dry_run=dry_run,
            timeout=timeout,
        )
    else:
        raise RuntimeError(f"{checkout} exists but is not a git checkout")

    if spec.ref:
        run_command(
            ["git", "checkout", spec.ref],
            cwd=checkout,
            env=env,
            dry_run=dry_run,
            timeout=timeout,
        )
    run_command(
        ["git", "submodule", "update", "--init", "--recursive"],
        cwd=checkout,
        env=env,
        dry_run=dry_run,
        timeout=timeout,
    )
    return checkout


def build_and_install_wheel(
    spec: RepoSpec,
    checkout: Path,
    wheelhouse: Path,
    python: str,
    env: dict[str, str],
    dry_run: bool,
    skip_build: bool,
    skip_install: bool,
    timeout: int,
) -> list[Path]:
    build_dir = checkout / spec.build_path
    if not build_dir.exists() and not dry_run:
        raise RuntimeError(f"build directory not found for {spec.name}: {build_dir}")

    backend_wheelhouse = wheelhouse / spec.name
    backend_wheelhouse.mkdir(parents=True, exist_ok=True)
    before = time.time()

    if not skip_build:
        run_command(
            [
                python,
                "-m",
                "pip",
                "wheel",
                "--no-deps",
                ".",
                "-w",
                str(backend_wheelhouse),
            ],
            cwd=build_dir,
            env=env,
            dry_run=dry_run,
            timeout=timeout,
        )

    if dry_run:
        return []

    wheels = sorted(
        [
            path
            for path in backend_wheelhouse.glob(spec.wheel_glob)
            if path.is_file() and path.stat().st_mtime >= before - 1
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not wheels:
        wheels = sorted(
            [path for path in backend_wheelhouse.glob(spec.wheel_glob) if path.is_file()],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    if not wheels:
        raise RuntimeError(f"no wheel matched {spec.wheel_glob} for {spec.name}")

    if not skip_install:
        run_command(
            [python, "-m", "pip", "install", "--force-reinstall", "--no-deps", str(wheels[0])],
            cwd=build_dir,
            env=env,
            dry_run=dry_run,
            timeout=timeout,
        )
    return wheels[:1]


def parse_backend_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    return {item.strip().lower() for item in value.split(",") if item.strip()}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare MORI, Mooncake, UCCL, and NIXL from public repos."
    )
    parser.add_argument("--manifest", default=str(default_manifest()))
    parser.add_argument("--thirdparty-dir", default=str(default_thirdparty_dir()))
    parser.add_argument("--wheelhouse", default=None)
    parser.add_argument("--backends", default=None, help="Comma-separated backend filter")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-clone", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-install", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    manifest = Path(args.manifest).resolve()
    thirdparty_dir = Path(args.thirdparty_dir).resolve()
    wheelhouse = (
        Path(args.wheelhouse).resolve()
        if args.wheelhouse
        else thirdparty_dir / "wheelhouse"
    )
    requested = parse_backend_filter(args.backends)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    specs = load_manifest(manifest)
    if requested is not None:
        specs = [spec for spec in specs if spec.name.lower() in requested]
    if not specs:
        raise SystemExit("no third-party repos selected")

    print(f"Manifest: {manifest}")
    print(f"Third-party dir: {thirdparty_dir}")
    print(f"Wheelhouse: {wheelhouse}")
    if args.dry_run:
        print("Dry-run mode: commands are printed but not executed.")

    for spec in specs:
        print(f"\n==> {spec.name}: {spec.repo} @ {spec.ref}", flush=True)
        checkout = ensure_checkout(
            spec,
            thirdparty_dir,
            env,
            args.dry_run,
            args.skip_clone,
            args.timeout,
        )
        wheels = build_and_install_wheel(
            spec,
            checkout,
            wheelhouse,
            args.python,
            env,
            args.dry_run,
            args.skip_build,
            args.skip_install,
            args.timeout,
        )
        for wheel in wheels:
            print(f"Installed wheel candidate: {wheel}")

    print("\nThird-party preparation complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
