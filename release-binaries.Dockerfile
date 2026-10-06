# syntax=docker/dockerfile:1.6
# Static (musl) per-component release binaries: api-server, kubelet, rhino-server (#1930).
#
# Same Alpine-builder trick as all-in-one-musl.Dockerfile: Alpine is musl-native,
# so the host target IS <arch>-unknown-linux-musl and crt-static links a fully
# static binary with no `--target` and no cross C toolchain. `mimalloc` is
# MANDATORY on musl (musl's allocator is ~10x slower under multi-threaded lock
# contention), so it is enabled on both rusternetes components. Keep this file
# arch-neutral (see scripts/tests/test-dockerfile-multiarch.sh for the pattern).
#
# Not an image: the final stage is `scratch` holding just the three binaries,
# meant to be exported with `--output type=local`:
#   git submodule update --init rhino
#   docker buildx build -f release-binaries.Dockerfile --target artifacts \
#     --output type=local,dest=out .
# Output (names are the on-disk build names; the release workflow renames them):
#   out/api-server  out/kubelet  out/rhino-server
#
# rhino is a git submodule (indyjonesnl/rhino) EXCLUDED from the workspace
# (root Cargo.toml `exclude`), with its own Cargo.lock and a `[patch.crates-io]`
# for a vendored h2, so it is built from its own directory. Its binary is
# `rhino-server` (rhino/Cargo.toml [[bin]]).

FROM rust:1.95-alpine AS builder

# Same toolchain set as all-in-one-musl.Dockerfile (ring, bundled SQLite,
# mimalloc, tonic/prost codegen, static libz for the SPDY flate2 backend).
RUN apk add --no-cache build-base musl-dev perl protoc protobuf-dev zlib-dev zlib-static

WORKDIR /build
COPY . .

# `.git` is excluded from the context; the version banner's SHA comes from a
# build-arg (common/build.rs falls back to "unknown" when empty).
ARG RUSTERNETES_GIT_SHA=""
ENV RUSTERNETES_GIT_SHA=${RUSTERNETES_GIT_SHA}

# Shipped artefacts: the regular `release` profile (lto=thin, codegen-units=1),
# not the compile-speed `release-fast`. The consumer runs api-server against
# etcd/rhino over --etcd-servers, so no storage feature is needed.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release \
      -p rusternetes-api-server -p rusternetes-kubelet \
      --bin api-server --bin kubelet \
      --features rusternetes-api-server/mimalloc,rusternetes-kubelet/mimalloc \
 && strip target/release/api-server target/release/kubelet \
 && mkdir -p /out \
 && cp target/release/api-server target/release/kubelet /out/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/rhino/target \
    cd rhino \
 && cargo build --release --bin rhino-server \
 && strip target/release/rhino-server \
 && cp target/release/rhino-server /out/

FROM scratch AS artifacts
COPY --from=builder /out/ /
