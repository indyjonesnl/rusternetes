# syntax=docker/dockerfile:1.6
# Dockerfile for the all-in-one k3rs binary.
#
# Build context is the PARENT directory of k3rs/ (which now vendors
# rhino in-tree as k3rs/rhino) so the rhino crate path (../../rhino
# from crates/storage) resolves.
#
# Build args:
#   CARGO_FEATURES — cargo features to enable (default: "sqlite")
#                    use "redis" for Redis backend, "sqlite,redis" for both
#
# Usage (from compose):
#   build:
#     context: ..
#     dockerfile: k3rs/all-in-one.Dockerfile
#     args:
#       CARGO_FEATURES: redis
#
# Two-stage cargo build (matches services.Dockerfile pattern):
#   Pass 1 — copy crate manifests + dummy src files, compile dep graph
#            into cache-mounted target/. Cached until a Cargo.toml /
#            Cargo.lock / build.rs / proto/ changes.
#   Pass 2 — copy real src, touch Cargo.toml fingerprints, rebuild.
#            Only changed workspace crates recompile.
#
# Workspace member list (Cargo.toml [workspace] members) — the manifest
# COPY block (CRATE-ENUMERATION 1/3) must list ALL of them or cargo can't
# resolve the workspace; the dummy/real-src blocks only need the crates the
# all-in-one binary actually compiles. scripts/tests/test-dockerfile-crate-
# enumeration.sh enforces the manifest block stays complete:
#   admission-webhook, api-server, client, common,
#   controller-manager, cri, discovery, dns, kubectl, kubelet, kube-proxy,
#   middleware, protobuf, k3rs, scheduler, storage,
#   test_support

# Stage 1: Build the console SPA
FROM node:24-slim AS console-builder
WORKDIR /console
COPY k3rs/console/package.json k3rs/console/package-lock.json* ./
RUN npm ci --ignore-scripts
COPY k3rs/console/ ./
RUN npm run build

# Stage 2: Build the Rust binary.
#
# Pin the toolchain — keep in lock-step with `services.Dockerfile` so
# both compose stacks share the same rustc and the BuildKit
# /app/target cache stays valid. See the comment in
# services.Dockerfile for the rationale.
FROM rust:1.99 AS builder

# sccache wraps rustc so identical (crate, source, flags) compilations
# hit the BuildKit cache mount below. Shares cache id `sccache-rusternetes`
# with services.Dockerfile and kubectl.Dockerfile so a build of any of
# them warms the cache for the others.
ARG SCCACHE_VERSION=v0.8.2
# TARGETARCH (amd64 | arm64) is injected by BuildKit; map it to sccache's
# release triple so this Dockerfile builds multi-arch.
ARG TARGETARCH
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) SCCACHE_ARCH=x86_64-unknown-linux-musl ;; \
         arm64) SCCACHE_ARCH=aarch64-unknown-linux-musl ;; \
         *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
        "https://github.com/mozilla/sccache/releases/download/${SCCACHE_VERSION}/sccache-${SCCACHE_VERSION}-${SCCACHE_ARCH}.tar.gz" \
        -o /tmp/sccache.tar.gz \
    && tar -xzf /tmp/sccache.tar.gz -C /tmp \
    && rm /tmp/sccache.tar.gz \
    && install -m 0755 "/tmp/sccache-${SCCACHE_VERSION}-${SCCACHE_ARCH}/sccache" /usr/local/bin/sccache \
    && rm -rf "/tmp/sccache-${SCCACHE_VERSION}-${SCCACHE_ARCH}"

ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=20G \
    SCCACHE_IDLE_TIMEOUT=0 \
    CARGO_INCREMENTAL=0

WORKDIR /build

# ----- PASS 1: dependency-only compile (cache-friendly) -----

# Workspace manifest + lockfile.
COPY k3rs/Cargo.toml k3rs/Cargo.lock* ./k3rs/

# CRATE-ENUMERATION (1/3): one COPY per crate's Cargo.toml.
COPY k3rs/crates/admission-webhook/Cargo.toml  ./k3rs/crates/admission-webhook/Cargo.toml
COPY k3rs/crates/api-server/Cargo.toml         ./k3rs/crates/api-server/Cargo.toml
COPY k3rs/crates/client/Cargo.toml             ./k3rs/crates/client/Cargo.toml
COPY k3rs/crates/common/Cargo.toml             ./k3rs/crates/common/Cargo.toml
COPY k3rs/crates/controller-manager/Cargo.toml ./k3rs/crates/controller-manager/Cargo.toml
COPY k3rs/crates/controller-core/Cargo.toml   ./k3rs/crates/controller-core/Cargo.toml
COPY k3rs/crates/cri/Cargo.toml                ./k3rs/crates/cri/Cargo.toml
COPY k3rs/crates/discovery/Cargo.toml          ./k3rs/crates/discovery/Cargo.toml
COPY k3rs/crates/dns/Cargo.toml                ./k3rs/crates/dns/Cargo.toml
COPY k3rs/crates/kubectl/Cargo.toml            ./k3rs/crates/kubectl/Cargo.toml
COPY k3rs/crates/kubelet/Cargo.toml            ./k3rs/crates/kubelet/Cargo.toml
COPY k3rs/crates/kube-proxy/Cargo.toml         ./k3rs/crates/kube-proxy/Cargo.toml
COPY k3rs/crates/middleware/Cargo.toml         ./k3rs/crates/middleware/Cargo.toml
COPY k3rs/crates/protobuf/Cargo.toml           ./k3rs/crates/protobuf/Cargo.toml
COPY k3rs/crates/k3rs/Cargo.toml        ./k3rs/crates/k3rs/Cargo.toml
COPY k3rs/crates/scheduler/Cargo.toml          ./k3rs/crates/scheduler/Cargo.toml
COPY k3rs/crates/storage/Cargo.toml            ./k3rs/crates/storage/Cargo.toml
COPY k3rs/crates/streamproxy/Cargo.toml        ./k3rs/crates/streamproxy/Cargo.toml
COPY k3rs/crates/cel-checker/Cargo.toml        ./k3rs/crates/cel-checker/Cargo.toml
COPY k3rs/crates/csi/Cargo.toml        ./k3rs/crates/csi/Cargo.toml
COPY k3rs/crates/test_support/Cargo.toml       ./k3rs/crates/test_support/Cargo.toml

# build.rs + proto/. api-server has both; common has a build.rs that stamps
# the git SHA / build time into the version banner (see build_info.rs).
# crates/cri/build.rs runs tonic-build over the vendored CRI proto (needs
# protobuf-compiler, installed in the builder above) — cri is in the
# all-in-one dep graph via kubelet.
COPY k3rs/crates/api-server/build.rs ./k3rs/crates/api-server/build.rs
COPY k3rs/crates/api-server/proto    ./k3rs/crates/api-server/proto
COPY k3rs/crates/common/build.rs     ./k3rs/crates/common/build.rs
COPY k3rs/crates/cri/build.rs        ./k3rs/crates/cri/build.rs
COPY k3rs/crates/cri/proto           ./k3rs/crates/cri/proto
# kubelet/build.rs runs tonic-build over the CSI v1 proto (#2313); without both
# in Pass-1 the generated `proto` module is empty and kubelet fails to compile.
COPY k3rs/crates/csi/build.rs     ./k3rs/crates/csi/build.rs
COPY k3rs/crates/csi/proto        ./k3rs/crates/csi/proto
COPY k3rs/crates/kubelet/build.rs     ./k3rs/crates/kubelet/build.rs
COPY k3rs/crates/kubelet/proto        ./k3rs/crates/kubelet/proto

# CRATE-ENUMERATION (2/3): dummy lib.rs / main.rs per crate.
#   - lib only:   common, controller-core, storage, protobuf,
#                 middleware, admission-webhook, discovery, cri
#   - bin only:   kubectl, k3rs
#   - lib + bin:  api-server, controller-manager, dns, kubelet,
#                 kube-proxy, scheduler
RUN set -eux; \
    cd /build/k3rs; \
    for c in client common controller-core storage protobuf middleware admission-webhook discovery cri streamproxy cel-checker csi test_support; do \
        mkdir -p crates/$c/src && : > crates/$c/src/lib.rs; \
    done; \
    for c in kubectl k3rs; do \
        mkdir -p crates/$c/src && echo "fn main(){}" > crates/$c/src/main.rs; \
    done; \
    for c in api-server controller-manager dns kubelet kube-proxy scheduler; do \
        mkdir -p crates/$c/src && \
        : > crates/$c/src/lib.rs && \
        echo "fn main(){}" > crates/$c/src/main.rs; \
    done

# Dummy bench files — see services.Dockerfile for the detailed rationale.
# Path is rooted at /build/k3rs because this Dockerfile's
# workspace lives there.
RUN mkdir -p /build/k3rs/crates/common/benches \
 && echo "fn main(){}" > /build/k3rs/crates/common/benches/regex_cache.rs \
 && mkdir -p /build/k3rs/crates/storage/benches \
 && echo "fn main(){}" > /build/k3rs/crates/storage/benches/watch_latency.rs

ARG CARGO_FEATURES=sqlite
WORKDIR /build/k3rs

# Pass 1: compile dep graph with dummy sources. Populates the
# cache-mounted target/ with every external dep's .rlib.
#
# Then wipe the dummy workspace artefacts (the empty stubs cargo built
# for our own crates) so Pass 2 compiles them from real source without
# falling for the cached dummy. This replaces the older `find … touch`
# hack which forced ALL workspace crates to recompile on Pass 2 — even
# unchanged ones. With the cleanup, only crates whose real src differs
# from the previous build's cached rlib get recompiled.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/k3rs/target \
    --mount=type=cache,target=/sccache,id=sccache-rusternetes,sharing=locked \
    cargo build --profile release-fast --features ${CARGO_FEATURES} -p k3rs \
 && cargo clean --profile release-fast \
        -p k3rs-admission-webhook \
        -p k3rs-api-server \
        -p k3rs-client \
        -p k3rs-common \
        -p k3rs-cel-checker \
        -p k3rs-controller-manager \
        -p k3rs-cri \
        -p k3rs-discovery \
        -p k3rs-dns \
        -p k3rs-kubectl \
        -p k3rs-kubelet \
        -p k3rs-kube-proxy \
        -p k3rs-middleware \
        -p k3rs-protobuf \
        -p k3rs \
        -p k3rs-scheduler \
        -p k3rs-storage \
        -p k3rs-streamproxy \
        -p k3rs-csi \
 && sccache --show-stats

# ----- PASS 2: real source compile -----

# CRATE-ENUMERATION (3/3): real source COPYs, per crate.
COPY k3rs/crates/admission-webhook/src    ./crates/admission-webhook/src
COPY k3rs/crates/api-server/src           ./crates/api-server/src
COPY k3rs/crates/api-server/openapi       ./crates/api-server/openapi
COPY k3rs/crates/api-server/tests         ./crates/api-server/tests
COPY k3rs/crates/client/src               ./crates/client/src
COPY k3rs/crates/common/src               ./crates/common/src
COPY k3rs/crates/cel-checker/src               ./crates/cel-checker/src
COPY k3rs/crates/common/tests             ./crates/common/tests
COPY k3rs/crates/controller-core/src      ./crates/controller-core/src
COPY k3rs/crates/controller-manager/src   ./crates/controller-manager/src
COPY k3rs/crates/controller-manager/tests ./crates/controller-manager/tests
COPY k3rs/crates/cri/src                  ./crates/cri/src
COPY k3rs/crates/discovery/src            ./crates/discovery/src
COPY k3rs/crates/dns/src                  ./crates/dns/src
COPY k3rs/crates/kubectl/src              ./crates/kubectl/src
COPY k3rs/crates/kubectl/tests            ./crates/kubectl/tests
COPY k3rs/crates/kubelet/src              ./crates/kubelet/src
COPY k3rs/crates/kubelet/tests            ./crates/kubelet/tests
COPY k3rs/crates/kube-proxy/src           ./crates/kube-proxy/src
COPY k3rs/crates/kube-proxy/tests         ./crates/kube-proxy/tests
COPY k3rs/crates/middleware/src           ./crates/middleware/src
COPY k3rs/crates/protobuf/src             ./crates/protobuf/src
COPY k3rs/crates/k3rs/src          ./crates/k3rs/src
COPY k3rs/crates/scheduler/src            ./crates/scheduler/src
COPY k3rs/crates/scheduler/tests          ./crates/scheduler/tests
COPY k3rs/crates/storage/src              ./crates/storage/src
COPY k3rs/crates/streamproxy/src          ./crates/streamproxy/src
COPY k3rs/crates/csi/src          ./crates/csi/src

# Git SHA for the version banner. `.git` is excluded from the build context,
# so the SHA is injected as a build-arg (compose passes ${K3RS_GIT_SHA});
# common/build.rs picks it up and bakes it into the binary. Empty default =>
# build.rs falls back to "unknown".
ARG K3RS_GIT_SHA=""
ENV K3RS_GIT_SHA=${K3RS_GIT_SHA}

# Pass 2: rebuild with real sources. No `touch` hack needed — Pass 1
# already removed the dummy workspace artefacts, so cargo compiles
# every workspace crate from the real src it just copied in.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/k3rs/target \
    --mount=type=cache,target=/sccache,id=sccache-rusternetes,sharing=locked \
    cargo build --profile release-fast --features ${CARGO_FEATURES} -p k3rs && \
    sccache --show-stats && \
    mkdir -p /out && cp target/release-fast/k3rs /out/k3rs

# Stage 3: Runtime image
FROM debian:sid-slim

# iptables + iproute2 are required by the in-process kube-proxy task: it
# programs DNAT rules so pods can reach the kubernetes Service ClusterIP
# (10.96.0.1) the same way upstream kubelet expects. Without these binaries
# the all-in-one stack had to disable kube-proxy and inject a docker-
# network alias as KUBERNETES_SERVICE_HOST — a workaround that broke any
# in-cluster client that did a DNS lookup of the hostname.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    iptables \
    iproute2 \
    && apt-get install -y --no-install-recommends nfs-common \
    && rm -rf /var/lib/apt/lists/*
# ^ nfs-common = /sbin/mount.nfs for the in-process kubelet's nfs volume
# plugin (#3333); see services.Dockerfile. Own apt step: no recommends.

WORKDIR /app

COPY --from=builder /out/k3rs /app/k3rs
COPY --from=console-builder /console/dist /app/console

EXPOSE 6443 10250

ENTRYPOINT ["/app/k3rs"]
CMD ["--storage-backend", "sqlite", "--tls", "--console-dir", "/app/console"]
