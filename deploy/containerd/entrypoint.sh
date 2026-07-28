#!/bin/sh
# Entrypoint for the rusternetes containerd (CRI runtime) service.
set -e

# containerd's CRI plugin starts a CNI-conf-dir fsnotify watcher; the default
# fs.inotify.max_user_instances (often 128) is too low and the plugin fails to
# load with "too many open files". Raise it (the container is privileged).
sysctl -w fs.inotify.max_user_instances=1024 >/dev/null 2>&1 || true
sysctl -w fs.inotify.max_user_watches=1048576 >/dev/null 2>&1 || true

# cgroup v2 nesting (the kind/k3d fix): in a private cgroup namespace the
# container's processes sit directly in the cgroup root, so cgroup v2's
# "no internal processes" rule forbids enabling controllers in subtree_control.
# crun then fails with `+io ... Not supported` when it tries to set up a pod
# cgroup. Move our processes into a leaf cgroup, then delegate every available
# controller down so the runtime can create pod cgroups.
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    mkdir -p /sys/fs/cgroup/init
    # Relocate every process out of the cgroup root into the init leaf.
    while read -r pid; do
        echo "$pid" > /sys/fs/cgroup/init/cgroup.procs 2>/dev/null || true
    done < /sys/fs/cgroup/cgroup.procs
    # Now the root has no processes; enable each controller for delegation.
    for c in $(cat /sys/fs/cgroup/cgroup.controllers); do
        echo "+$c" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
    done
fi

# Multi-node stacks derive the CNI config from node-ipam: kube-proxy's
# node-network agent writes /etc/cni/net.d/10-rusternetes.conflist from this
# node's spec.podCIDR (#1691). Drop the image's cluster-wide fallback first, or
# a pod scheduled before the agent's first sync would get an address out of the
# whole 10.244.0.0/16 — i.e. possibly another node's subnet, which nothing
# routes to. Pods stay Pending (CNI conf absent) until the real config lands,
# which is the safe failure mode. Single-node stacks leave this unset and keep
# using the baked conflist.
if [ -n "${CNI_CONF_FROM_NODE_IPAM:-}" ]; then
    rm -f /etc/cni/net.d/10-rusternetes.conflist
    echo "entrypoint: removed fallback CNI conflist; waiting for the node-network agent"
fi

exec /usr/local/bin/containerd --config /etc/containerd/config.toml
