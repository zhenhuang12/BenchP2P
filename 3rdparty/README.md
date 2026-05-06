# 3rdparty

This directory is managed by `scripts/prepare_thirdparty.py`.

Tracked files:

- `manifest.json`: public repository URLs, refs, checkout paths, and wheel build
  directories for MORI, Mooncake, UCCL, and NIXL.

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
