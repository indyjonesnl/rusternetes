# Shared CRI runtime for the k3rs cluster: containerd (with the CRI
# plugin) driving crun as the OCI runtime. The kubelets talk to this over CRI
# v1 instead of the Docker/Podman socket the old bollard kubelet used.
FROM debian:sid-slim

ARG CONTAINERD_VERSION=2.2.4
ARG CNI_VERSION=1.6.2
# TARGETARCH (amd64 | arm64) is injected by BuildKit and matches the release
# asset naming of both containerd and the CNI plugins, so the same Dockerfile
# builds multi-arch (see .github/workflows/publish-images.yml). crun comes from
# apt, which is already arch-aware.
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl iptables procps crun \
    && rm -rf /var/lib/apt/lists/*

# containerd (includes containerd, ctr, containerd-shim-runc-v2 under bin/).
# `--retry` survives transient GitHub download failures (e.g. HTTP 504). Download
# to a temp file rather than piping to tar so a retry cannot concatenate a
# partial stream and curl's non-zero exit is not masked by tar.
RUN curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
      "https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}-linux-${TARGETARCH}.tar.gz" \
      -o /tmp/containerd.tar.gz \
    && tar -xzf /tmp/containerd.tar.gz -C /usr/local \
    && rm /tmp/containerd.tar.gz

# CNI plugins (bridge, host-local, portmap, loopback, …) for pod networking.
RUN mkdir -p /opt/cni/bin \
    && curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
      "https://github.com/containernetworking/plugins/releases/download/v${CNI_VERSION}/cni-plugins-linux-${TARGETARCH}-v${CNI_VERSION}.tgz" \
      -o /tmp/cni-plugins.tgz \
    && tar -xzf /tmp/cni-plugins.tgz -C /opt/cni/bin \
    && rm /tmp/cni-plugins.tgz

# containerd's runc-v2 shim can drive crun directly via BinaryName. Keep a
# runc-compatible path around as a safety belt for tools that still expect it.
RUN ln -sf "$(command -v crun)" /usr/local/sbin/runc

COPY deploy/containerd/config.toml /etc/containerd/config.toml
COPY deploy/containerd/cni/10-k3rs.conflist /etc/cni/net.d/10-k3rs.conflist
COPY deploy/containerd/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# CRI gRPC socket the kubelets connect to (shared via a named volume).
VOLUME ["/run/containerd"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
