# 3rdparty

This directory is managed by `scripts/prepare_thirdparty.py`.

Tracked files:

- `manifest.json`: public repository URLs, refs, checkout paths, and wheel build
  directories for MORI, Mooncake, UCCL, and NIXL. Each repo entry may also
  declare a `patches` array of paths (relative to `3rdparty/`) that
  `prepare_thirdparty.py` applies after checkout via `git apply`.
- `patches/`: local patches applied to the third-party checkouts. Currently:
  - `uccl-0001-relax-python-requires-to-3.10.patch`: relaxes UCCL's
    `python_requires` from `>=3.12` to `>=3.10` so the wheel builds on
    Python 3.10/3.11. The build itself already supports those versions
    (it skips the nanobind stable-ABI path and produces a regular
    `cpXY-cpXY` wheel).
  - `nixl-0001-doca-telemetry-build-fixes.patch`: two build fixes for
    NIXL's DOCA telemetry exporter plugin:
    - adds `nixl_common_dep` to the plugin's dependency list so the
      `tomlplusplus` include path is propagated (without it the plugin
      fails with `fatal error: toml++/toml.hpp: No such file or
      directory`);
    - extends the meson probe so the plugin is only built when the host
      DOCA SDK exposes the metrics API (probed via
      `doca_telemetry_exporter_label_set_id_t`). Older DOCA releases
      ship `doca_telemetry_exporter.h` without the metrics symbols used
      by the plugin source, in which case it now skips with a warning
      instead of failing the whole NIXL build.

Generated files are ignored by git:

- `mori/`
- `Mooncake/`
- `uccl/`
- `nixl/`
- `wheelhouse/`

Preview setup commands:

```bash
python3 scripts/prepare_thirdparty.py --dry-run
```

Clone/update, build wheels, and install them into the active Python environment:

```bash
python3 scripts/prepare_thirdparty.py
```

Build all selected wheel packages in the default runtime image
`docker.io/rocm/primus:v26.2` without installing them into the host Python
environment:

```bash
python3 scripts/prepare_thirdparty.py --container-build
```

`--container-build` is what `scripts/container_prepare_thirdparty.py` uses
inside Slurm runtime containers. From the submission host, it wraps itself with
`docker run`; from inside a runtime container, pass `--container-runtime none`
to perform the actual build directly. It builds each selected backend into
`3rdparty/wheelhouse/<backend>/`, skips the install step, and removes stale
wheels for that backend before each build so old artifacts are not mistaken for
fresh output.
