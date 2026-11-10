# Dockerfile for Rhino - etcd-compatible gRPC server backed by SQLite
# Builds rhino-server from a pinned commit of github.com/indyjonesnl/rhino.
# RHINO_REV is the same commit as the `rhino` git dependency in
# crates/storage/Cargo.toml (scripts/check-rhino-rev.sh guards the pair).
FROM rust:latest AS builder

ARG RHINO_REV=5e07b3ca7d2b04456891d40f6a83e4e10cbcd075

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Fetch exactly the pinned commit. Rhino's own Cargo.toml carries the
# [patch.crates-io] for its vendored h2 (third_party/), which only applies when
# rhino is the build root, so rhino-server is built here from its own tree.
RUN git init -q . \
 && git fetch -q --depth 1 https://github.com/indyjonesnl/rhino.git "${RHINO_REV}" \
 && git checkout -q FETCH_HEAD \
 && cargo build --release --bin rhino-server

FROM debian:sid-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/rhino-server /usr/local/bin/rhino-server

RUN mkdir -p /data/db

EXPOSE 2379

ENTRYPOINT ["rhino-server"]
CMD ["--listen-address", "0.0.0.0:2379", "--endpoint", "/data/db/state.db"]
