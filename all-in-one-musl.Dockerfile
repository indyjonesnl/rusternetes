# syntax=docker/dockerfile:1.6
# Fully static (musl) all-in-one image — the "scratch/distroless" target of #1041.
#
# Produces a zero-OS-dependency, statically-linked `rusternetes` binary on a
# bare `scratch` base, built with the mimalloc global allocator
# (`--features mimalloc`). mimalloc is MANDATORY for a musl build: musl's
# default allocator is ~10x slower under multi-threaded lock contention and
# would regress the all-in-one's throughput (see crates/rusternetes/Cargo.toml).
#
# Arches: linux/amd64 + linux/arm64. Nothing here is arch-specific — the
# builder is Alpine, so the host target IS the build target and no cross-compile
# plumbing (no `--target`, no aarch64-musl cross C toolchain, no cargo-zigbuild)
# is involved. .github/workflows/publish-musl-image.yml builds each arch on a
# NATIVE runner and stitches the two into one multi-arch manifest; keep this
# file arch-neutral (guarded by scripts/tests/test-dockerfile-multiarch.sh).
#
# Build context = repo root, so the rhino in-tree submodule at ./rhino resolves
# the `../../rhino` path-dep from crates/storage. Check it out first:
#   git submodule update --init rhino
#   docker build -f all-in-one-musl.Dockerfile -t rusternetes-all-in-one:musl .
#
# Unlike all-in-one.Dockerfile (glibc / debian-slim, ships iptables + the web
# console), this image is deliberately minimal: no shell, no libc, no iptables.
# The in-process kube-proxy needs host iptables, absent here, so the image runs
# with `--disable-proxy` — the same posture the all-in-one container already
# takes. Run a standalone kube-proxy (host network) for full Service DNAT.
# The web console is served from `--console-dir` at runtime; mount it if wanted.

# ── Builder: Alpine is musl-native, so the host target is already
#    <arch>-unknown-linux-musl (x86_64 or aarch64, whichever the runner is) and
#    links crt-static by default → a fully static binary with no extra
#    target/flag plumbing. ──────────────────────────────────────────────────
FROM rust:1.95-alpine AS builder

# C toolchain + deps that build native code:
#   build-base/musl-dev  — gcc + musl headers for ring, bundled SQLite, mimalloc
#   perl                 — ring's build script
#   protoc/protobuf-dev  — tonic/prost proto codegen in crates/{cri,api-server}
#   zlib-dev/zlib-static — static libz for the flate2 zlib backend (SPDY),
#                          which links `-lz`; a fully static link needs libz.a
#   ca-certificates      — copied into the scratch image for outbound TLS roots
RUN apk add --no-cache build-base musl-dev perl protoc protobuf-dev zlib-dev zlib-static ca-certificates

WORKDIR /build
COPY . .

# `.git` is excluded from the context, so the version banner's SHA comes from a
# build-arg (common/build.rs falls back to "unknown" when empty).
ARG RUSTERNETES_GIT_SHA=""
ENV RUSTERNETES_GIT_SHA=${RUSTERNETES_GIT_SHA}

# Regular `release` profile — this is a shipped artefact, not the compile-speed
# `release-fast` the compose images use. `sqlite` is on via default features;
# `mimalloc` is added explicitly (see header).
# BuildKit cache mounts keep the cargo registry + target tree across builds so
# a rebuild only recompiles what changed. `cp` runs inside the same RUN while
# the target mount is live, then the binary lives in a normal image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --features mimalloc -p rusternetes \
 && strip target/release/rusternetes \
 && cp target/release/rusternetes /rusternetes

# ── Runtime: scratch — the fully static musl binary needs no libc, no shell,
#    nothing. Only the CA-cert bundle is copied in (outbound TLS roots). This
#    is the purest "zero-OS-dependency" target from #1041 and, unlike a
#    distroless base, pulls no external image. ──────────────────────────────
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /rusternetes /rusternetes

# 6443 = API server (TLS), 10250 = kubelet.
EXPOSE 6443 10250

ENTRYPOINT ["/rusternetes"]
# Embedded SQLite, TLS on, in-process kube-proxy off (no iptables here).
CMD ["--storage-backend", "sqlite", "--tls", "--disable-proxy"]
