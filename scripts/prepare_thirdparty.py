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
    patches: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class BuildResult:
    name: str
    checkout: Path
    wheels: tuple[Path, ...]
    installed: bool


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
                patches=tuple(item.get("patches", ())),
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


def apply_patches(
    spec: RepoSpec,
    checkout: Path,
    thirdparty_dir: Path,
    env: dict[str, str],
    dry_run: bool,
    timeout: int,
) -> None:
    if not spec.patches:
        return

    # Reset to a clean tree so re-running the script reapplies cleanly even
    # when a previous run left the patch applied.
    if not dry_run and not (checkout / ".git").exists():
        raise RuntimeError(
            f"cannot apply patches: {checkout} is not a git checkout"
        )
    run_command(
        ["git", "reset", "--hard", "HEAD"],
        cwd=checkout,
        env=env,
        dry_run=dry_run,
        timeout=timeout,
    )

    for rel in spec.patches:
        patch_path = (thirdparty_dir / rel).resolve()
        if not dry_run and not patch_path.is_file():
            raise RuntimeError(
                f"patch file not found for {spec.name}: {patch_path}"
            )
        run_command(
            ["git", "apply", "--whitespace=nowarn", str(patch_path)],
            cwd=checkout,
            env=env,
            dry_run=dry_run,
            timeout=timeout,
        )


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
        apply_patches(spec, checkout, thirdparty_dir, env, dry_run, timeout)
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
    apply_patches(spec, checkout, thirdparty_dir, env, dry_run, timeout)
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
    clean_wheelhouse: bool,
    timeout: int,
) -> list[Path]:
    build_dir = checkout / spec.build_path
    if not build_dir.exists() and not dry_run:
        raise RuntimeError(f"build directory not found for {spec.name}: {build_dir}")

    backend_wheelhouse = wheelhouse / spec.name
    backend_wheelhouse.mkdir(parents=True, exist_ok=True)
    before = time.time()

    if clean_wheelhouse and not skip_build:
        for old_wheel in backend_wheelhouse.glob("*.whl"):
            print(f"Removing stale wheel before build: {old_wheel}", flush=True)
            if not dry_run:
                old_wheel.unlink()

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
        produced = sorted(path.name for path in backend_wheelhouse.glob("*.whl"))
        detail = ", ".join(produced) if produced else "no wheel files produced"
        raise RuntimeError(
            f"{spec.name}: no wheel matched {spec.wheel_glob} under "
            f"{backend_wheelhouse} ({detail})"
        )

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


def selected_specs(specs: Sequence[RepoSpec], requested: set[str] | None) -> list[RepoSpec]:
    known = {spec.name.lower() for spec in specs}
    if requested is None:
        return list(specs)
    unknown = requested - known
    if unknown:
        raise SystemExit(
            "unknown backend(s): "
            + ",".join(sorted(unknown))
            + ". Known backends: "
            + ",".join(sorted(known))
        )
    return [spec for spec in specs if spec.name.lower() in requested]


def print_summary(results: Sequence[BuildResult], skipped_install: bool) -> None:
    print("\nThird-party wheel summary:")
    if not results:
        print("  (no wheels built)")
        return
    for result in results:
        wheel_list = (
            ", ".join(str(path) for path in result.wheels)
            if result.wheels
            else "(dry-run; no wheel file created)"
        )
        install_state = "not installed" if skipped_install else "installed"
        print(f"  {result.name}: {install_state}; {wheel_list}")


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
    parser.add_argument(
        "--container-build",
        action="store_true",
        help="Build wheels for a runtime container; implies --skip-install",
    )
    parser.add_argument(
        "--clean-wheelhouse",
        action="store_true",
        help="Remove existing wheels for each selected backend before building",
    )
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
    if args.container_build:
        args.skip_install = True
        env["BENCHP2P_CONTAINER_BUILD"] = "1"

    specs = selected_specs(load_manifest(manifest), requested)
    if not specs:
        raise SystemExit("no third-party repos selected")

    print(f"Manifest: {manifest}")
    print(f"Third-party dir: {thirdparty_dir}")
    print(f"Wheelhouse: {wheelhouse}")
    print(f"Selected backends: {', '.join(spec.name for spec in specs)}")
    if args.container_build:
        print("Container build mode: wheels are built but not installed here.")
    if args.dry_run:
        print("Dry-run mode: commands are printed but not executed.")

    results: list[BuildResult] = []
    for spec in specs:
        print(f"\n==> {spec.name}: {spec.repo} @ {spec.ref}", flush=True)
        try:
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
                args.clean_wheelhouse or args.container_build,
                args.timeout,
            )
        except (RuntimeError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise SystemExit(f"{spec.name}: failed to build wheel in {thirdparty_dir}: {exc}") from exc
        for wheel in wheels:
            print(f"Built wheel candidate: {wheel}")
        results.append(
            BuildResult(
                name=spec.name,
                checkout=checkout,
                wheels=tuple(wheels),
                installed=not args.skip_install,
            )
        )

    print_summary(results, args.skip_install)
    print("\nThird-party preparation complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
