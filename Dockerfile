# syntax=docker/dockerfile:1.7
# BenchP2P all-backends preinstalled image.
#
# Bakes wheels for mori/mooncake/uccl/nixl plus tebench/nixlbench binaries
# and the bench harness into a single runtime image, so a benchmark run is
# just:
#
#   docker run --rm --ipc=host --network=host \
#     --device=/dev/kfd --device=/dev/dri --device=/dev/infiniband \
#     --cap-add=SYS_PTRACE --cap-add=CAP_SYS_ADMIN \
#     --security-opt seccomp=unconfined --group-add video --privileged \
#     -v /tmp/bp2p:/tmp/bp2p benchp2p:latest \
#     bench_p2p_compare run --skip-wheel-install \
#       --output-dir /tmp/bp2p --backends mori,uccl ...
#
# Build (multi-stage, ~30-45 min on a 64-core box the first time):
#
#   git submodule update --init --recursive   # populate 3rdparty/<backend>
#   docker build -t benchp2p:latest .
#
# Build args (all optional):
#
#   APT_PRESET / APT_MIRROR  switch APT during build (clusters where
#                            archive.ubuntu.com is unreachable). Same names
#                            as scripts/switch_apt_mirror.sh, e.g.
#                            --build-arg APT_PRESET=aliyun
#   BACKENDS                 comma-separated subset of backends to build
#                            (default: mori,mooncake,uccl,nixl)
#   JOBS                     parallelism for make/ninja (default: $(nproc))
#   NIXL_PREFIX              install prefix for nixl core (default: /usr/local/nixl)
#   TIMEOUT_S                per-command timeout (default: 7200)
#   PIP_INDEX_URL            optional pip index for cluster-internal mirrors
#                            (e.g. https://mirrors.aliyun.com/pypi/simple/)

ARG BASE_IMAGE=docker.io/rocm/primus:v26.2

##############################################################################
# Stage 1: builder. Compiles all 4 backends and stages artifacts.
##############################################################################
FROM ${BASE_IMAGE} AS builder

ARG APT_PRESET=
ARG APT_MIRROR=
ARG BACKENDS=mori,mooncake,uccl,nixl
ARG JOBS=
ARG NIXL_PREFIX=/usr/local/nixl
ARG TIMEOUT_S=7200
ARG PIP_INDEX_URL=

ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_BREAK_SYSTEM_PACKAGES=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV NIXL_PREFIX=${NIXL_PREFIX}
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

WORKDIR /src/BenchP2P

# Optional APT mirror switch up-front (clusters where archive.ubuntu.com /
# security.ubuntu.com aren't reachable from the build host).
COPY scripts/switch_apt_mirror.sh scripts/switch_apt_mirror.sh
RUN if [ -n "${APT_PRESET}${APT_MIRROR}" ]; then \
      bash scripts/switch_apt_mirror.sh \
        ${APT_PRESET:+--preset "${APT_PRESET}"} \
        ${APT_MIRROR:+--mirror "${APT_MIRROR}"} --no-update ; \
    fi && apt-get update

# System packages spanning the four backends:
#   - cmake/ninja/meson/make/pkg-config/autoconf/automake/libtool/patchelf:
#     build tooling
#   - jq/git/ca-certificates/curl/wget: clone, manifest parsing, downloads
#   - libpci-dev/pciutils: mori topology detection
#   - libibverbs-dev/rdma-core/ibverbs-providers: mori, nixl RDMA transports
#   - libopenmpi-dev/openmpi-bin: mori MPI bootstrap (optional but cheap)
#   - libgflags-dev/libgoogle-glog-dev/libjsoncpp-dev/libcurl4-openssl-dev/
#     libssl-dev/yasm/libnuma-dev/libgtest-dev: mooncake transfer engine
#     (matches Mooncake/dependencies.sh)
#   - libprotobuf-dev/protobuf-compiler/libgrpc++-dev/protobuf-compiler-grpc:
#     mooncake + nixl gRPC layers
#   - etcd-server: nixlbench coordinator (rank-0 starts a local etcd)
#   - sudo: build_wheel.sh's mooncake path uses sudo when not root; harmless
#     here since we already run as root in `docker build`.
RUN apt-get install -y --no-install-recommends \
      jq git ca-certificates curl wget \
      cmake ninja-build make meson pkg-config \
      autoconf automake libtool patchelf \
      libpci-dev pciutils \
      libibverbs-dev rdma-core ibverbs-providers libnuma-dev \
      libopenmpi-dev openmpi-bin \
      libgflags-dev libgoogle-glog-dev libjsoncpp-dev libgtest-dev \
      libprotobuf-dev protobuf-compiler libgrpc++-dev protobuf-compiler-grpc \
      libcurl4-openssl-dev libssl-dev yasm \
      libunwind-dev libboost-all-dev libyaml-cpp-dev libmsgpack-dev libzstd-dev \
      libasio-dev libhiredis-dev libjemalloc-dev liburing-dev libxxhash-dev \
      libcpprest-dev \
      libfabric-dev \
      unzip \
      etcd-server \
      sudo \
    && rm -rf /var/lib/apt/lists/*

# Build-time Python deps that aren't pulled in by individual backends.
RUN python3 -m pip install nanobind 'cmake<4' auditwheel

# Bring in build orchestration + 3rdparty checkouts. The .dockerignore
# strips all .git directories from the context (saves >1.5 GB), so the
# host must have run `git submodule update --init --recursive` inside
# each backend before docker build. The build below uses --skip-clone
# so this is offline-safe (no network access to github needed).
#
# IMPORTANT: only copy the scripts the builder actually invokes, not the
# whole scripts/ tree. Otherwise edits to slurm/container/bench wrappers
# (slurm_*.sh, container_*.sh, bench_p2p_compare.py) would invalidate
# this layer and force the ~10 min `RUN bash scripts/build_wheel.sh`
# below to re-run, even though those scripts are completely unrelated
# to the wheel build.
COPY scripts/build_wheel.sh scripts/build_wheel.sh
COPY 3rdparty/manifest.json 3rdparty/manifest.json
COPY 3rdparty/mori 3rdparty/mori
COPY 3rdparty/Mooncake 3rdparty/Mooncake
COPY 3rdparty/uccl 3rdparty/uccl
COPY 3rdparty/nixl 3rdparty/nixl
COPY 3rdparty/patches 3rdparty/patches

# Build + install yalantinglibs from Mooncake's vendored submodule. Normally
# Mooncake's dependencies.sh handles this, but it runs `git submodule update
# --init --recursive` first and aborts when the .git directory is missing
# (which it always is here -- .dockerignore drops every **/.git/ to keep the
# build context small). Submodules are pre-populated on the host, so the
# header tree is already present under extern/yalantinglibs/; we just need
# to configure/build/install it ourselves so cmake's later
# `find_package(yalantinglibs)` succeeds.
RUN if [ -d 3rdparty/Mooncake/extern/yalantinglibs ]; then \
      rm -rf 3rdparty/Mooncake/extern/yalantinglibs/build && \
      cmake -S 3rdparty/Mooncake/extern/yalantinglibs \
            -B 3rdparty/Mooncake/extern/yalantinglibs/build \
            -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF && \
      cmake --build 3rdparty/Mooncake/extern/yalantinglibs/build -j"$(nproc)" && \
      cmake --install 3rdparty/Mooncake/extern/yalantinglibs/build ; \
    fi

# Build + install a recent Abseil so nixl's meson setup is happy. Ubuntu noble
# ships libabsl-dev 20220623, which lacks `absl_log` and friends; nixl's
# meson.build refuses to fall back to its bundled subproject in that case
# ("would result in a mix of Abseil versions at runtime"). The bundled
# version (subprojects/abseil-cpp-*) is the source-of-truth nixl was tested
# against, so we install it system-wide to /usr/local. /usr/local/lib/pkgconfig
# is searched ahead of /usr/lib/x86_64-linux-gnu/pkgconfig by default so
# meson picks the new one. The existing system libabsl-dev stays in place
# so libgrpc++.so / Mooncake's transfer engine that linked against it
# earlier in this stage keep working.
RUN ABSL_SRC="$(ls -d 3rdparty/nixl/subprojects/abseil-cpp-* 2>/dev/null | head -1)"; \
    if [ -n "${ABSL_SRC}" ] && [ -d "${ABSL_SRC}" ]; then \
      rm -rf /tmp/abseil-build && \
      cmake -S "${ABSL_SRC}" -B /tmp/abseil-build -GNinja \
            -DCMAKE_BUILD_TYPE=Release \
            -DBUILD_SHARED_LIBS=ON \
            -DABSL_PROPAGATE_CXX_STD=ON \
            -DABSL_ENABLE_INSTALL=ON \
            -DCMAKE_CXX_STANDARD=17 \
            -DCMAKE_INSTALL_PREFIX=/usr/local && \
      cmake --build /tmp/abseil-build -j"$(nproc)" && \
      cmake --install /tmp/abseil-build && \
      rm -rf /tmp/abseil-build && \
      ldconfig ; \
    else \
      echo "[Dockerfile] WARN: nixl bundled abseil subproject not found, skipping" >&2 ; \
    fi

# Build + install etcd-cpp-apiv3 (the C++ ETCD client). Needed by nixl's
# meson build to enable the ETCD runtime; without it nixl falls back to
# ASIO-only and `nixlbench --etcd_endpoints ...` aborts at startup with
# `Invalid runtime: ETCD`. Ubuntu noble has no apt package for this lib,
# so we clone & build a pinned release tag. The cmake build pulls in
# libgrpc++/libprotobuf/libcpprest from the apt step above.
ARG ETCD_CPP_TAG=v0.15.4
RUN git clone --depth 1 --branch "${ETCD_CPP_TAG}" \
      https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git /tmp/etcd-cpp && \
    cmake -S /tmp/etcd-cpp -B /tmp/etcd-cpp/build -GNinja \
          -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_ETCD_CORE_ONLY=OFF \
          -DCMAKE_INSTALL_PREFIX=/usr/local && \
    cmake --build /tmp/etcd-cpp/build -j"$(nproc)" && \
    cmake --install /tmp/etcd-cpp/build && \
    rm -rf /tmp/etcd-cpp && \
    ldconfig

# Build all four backends. --skip-apt-deps avoids re-running mooncake's
# dependencies.sh (which would fail at its `git submodule` step inside this
# .git-less context); we already installed every apt package it would have
# pulled, plus yalantinglibs above.
RUN bash scripts/build_wheel.sh \
      --skip-clone \
      --skip-apt-deps \
      --backends "${BACKENDS}" \
      --nixl-prefix "${NIXL_PREFIX}" \
      --timeout "${TIMEOUT_S}" \
      ${JOBS:+--jobs "${JOBS}"}

# Stage native benchmark binaries + the Mooncake runtime libraries they
# need into a stable path so the runtime stage can pick them up with a
# single COPY layer.
#
# Why we re-stage libtransfer_engine.so / libasio.so even though the
# Mooncake wheel already ships them under <site-packages>/mooncake/:
# the wheel goes through `auditwheel repair`, which renames every
# transitive .so (e.g. libglog -> libglog-cc080fc1.so.0.6.0) and bundles
# the renamed copies under mooncake_transfer_engine.libs/. If `tebench`
# were to load libtransfer_engine.so from the wheel path, it would pull
# in BOTH the system libglog.so.1 (its own DT_NEEDED) AND the wheel's
# renamed libglog-*.so via libtransfer_engine's RPATH. Two copies of
# glog both register `--v` and gflags aborts at startup with
#   ERROR: something wrong with flag 'v' ... linked both statically and
#   dynamically into this executable.
# Staging the un-repaired builder copies into /usr/local/lib instead
# means tebench resolves libtransfer_engine.so to the system-glog-linked
# version and only one libglog is loaded.
RUN set -eux; mkdir -p /staging/bin /staging/lib; \
    if [ -x 3rdparty/Mooncake/build/mooncake-transfer-engine/benchmark/tebench ]; then \
      cp 3rdparty/Mooncake/build/mooncake-transfer-engine/benchmark/tebench /staging/bin/tebench; \
    fi; \
    nixlbench_bin="$(find 3rdparty/nixl/benchmark/nixlbench/build -name nixlbench -executable -type f 2>/dev/null | head -1)"; \
    if [ -n "${nixlbench_bin}" ]; then cp "${nixlbench_bin}" /staging/bin/nixlbench; fi; \
    for so in \
      3rdparty/Mooncake/build/mooncake-transfer-engine/src/libtransfer_engine.so \
      3rdparty/Mooncake/build/mooncake-common/libasio.so \
    ; do \
      [ -f "$so" ] && cp -P "$so" /staging/lib/ ; \
    done

##############################################################################
# Stage 2: runtime. Same base + just what's needed to import + run.
##############################################################################
FROM ${BASE_IMAGE} AS runtime

ARG APT_PRESET=
ARG APT_MIRROR=
ARG NIXL_PREFIX=/usr/local/nixl
ARG PIP_INDEX_URL=

ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_BREAK_SYSTEM_PACKAGES=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV NIXL_PREFIX=${NIXL_PREFIX}
ENV NIXL_PLUGIN_DIR=${NIXL_PREFIX}/lib/x86_64-linux-gnu/plugins
ENV LD_LIBRARY_PATH=${NIXL_PREFIX}/lib:${NIXL_PREFIX}/lib/x86_64-linux-gnu:${NIXL_PREFIX}/lib64:${LD_LIBRARY_PATH:-}
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

# Mirror switch helper available in the runtime image too.
COPY scripts/switch_apt_mirror.sh /opt/BenchP2P/scripts/switch_apt_mirror.sh

# Runtime apt deps. We install the same -dev packages as the builder so
# noble's t64 transition and similar package-name churn don't bite us
# (each -dev pulls in the matching runtime libfoo.so* package). The
# overhead vs runtime-only libs is small (~50 MB) and the reliability is
# worth it.
RUN if [ -n "${APT_PRESET}${APT_MIRROR}" ]; then \
      bash /opt/BenchP2P/scripts/switch_apt_mirror.sh \
        ${APT_PRESET:+--preset "${APT_PRESET}"} \
        ${APT_MIRROR:+--mirror "${APT_MIRROR}"} --no-update ; \
    fi && apt-get update && \
    apt-get install -y --no-install-recommends \
      libpci-dev pciutils \
      libibverbs-dev rdma-core ibverbs-providers libnuma-dev \
      libopenmpi-dev openmpi-bin \
      libgflags-dev libgoogle-glog-dev libjsoncpp-dev \
      libprotobuf-dev libgrpc++-dev \
      libcurl4-openssl-dev libssl-dev \
      libcpprest2.10 \
      libhiredis1.1.0 liburing2 \
      libfabric1 \
      etcd-server \
      ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Wheels + native benchmark binaries + nixl install prefix from builder.
COPY --from=builder /src/BenchP2P/3rdparty/wheelhouse /opt/BenchP2P/wheelhouse
COPY --from=builder /staging/bin/ /usr/local/bin/
COPY --from=builder ${NIXL_PREFIX} ${NIXL_PREFIX}

# nixl + nixlbench were linked against the bundled Abseil (20250814.1)
# we built into the builder stage's /usr/local/lib (system Ubuntu noble
# libabsl-dev is too old). Carry just the runtime .so / .so.* files
# over so `import nixl_cu12` and `nixlbench` can resolve their absl
# symbols at process start. We avoid copying the entire /usr/local
# tree to keep the runtime image lean.
#
# We also bring over:
#   - libetcd-cpp-api*.so* + libcpprest dep, so nixlbench's ETCD runtime
#     (`--etcd_endpoints http://...:2379`) can dlopen the etcd-cpp client.
#     Without these in /usr/local/lib, nixlbench bails at startup with
#     `Invalid runtime: ETCD`.
#   - libtransfer_engine.so / libasio.so (the un-auditwheel-repaired
#     copies staged above), so `tebench` resolves them from /usr/local/lib
#     instead of the wheel's mooncake_transfer_engine.libs/, which would
#     drag in a second renamed libglog and trigger gflags' double-link
#     fatal at startup.
COPY --from=builder /usr/local/lib/libabsl*.so* /usr/local/lib/
COPY --from=builder /usr/local/lib/libetcd-cpp-api*.so* /usr/local/lib/
COPY --from=builder /staging/lib/ /usr/local/lib/
RUN ldconfig

# Install backend wheels + runtime python deps system-wide. Wheels were
# built --no-deps on purpose (each backend's package metadata pins
# upstream-only deps that may not exist in our wheelhouse), so we install
# their published runtime deps explicitly via the second pip call.
RUN python3 -m pip install --no-deps /opt/BenchP2P/wheelhouse/*/*.whl && \
    python3 -m pip install \
      prettytable etcd3 matplotlib protobuf

# tebench used to need an /etc/ld.so.conf.d/ entry pointing at the
# mooncake wheel's lib dir, but that loaded the auditwheel-repaired
# libtransfer_engine.so and dragged a second (renamed) libglog into the
# process via that lib's RPATH, causing gflags to abort with the
# "linked both statically and dynamically" double-init error. We now
# stage the un-repaired libtransfer_engine.so / libasio.so straight
# into /usr/local/lib above, so the default linker config finds them
# first and only one libglog is loaded by tebench. Nothing extra to do
# here -- the COPY + ldconfig above is enough.

# BenchP2P harness so `bench_p2p_compare run --skip-wheel-install ...`
# works from any cwd inside the container.
COPY scripts/bench_p2p_compare.py /opt/BenchP2P/scripts/bench_p2p_compare.py
RUN chmod +x /opt/BenchP2P/scripts/bench_p2p_compare.py && \
    ln -sf /opt/BenchP2P/scripts/bench_p2p_compare.py /usr/local/bin/bench_p2p_compare

LABEL org.opencontainers.image.title="BenchP2P (all-backends preinstalled)"
LABEL org.opencontainers.image.description="rocm/primus:v26.2 + mori + mooncake + uccl + nixl wheels and tebench/nixlbench binaries pre-installed"

WORKDIR /workspace
CMD ["bash"]
