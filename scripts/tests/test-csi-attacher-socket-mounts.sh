#!/usr/bin/env bash
# CSI attacher templating of the kube-controller-manager static pod (#3133).
#
# bootstrap-cluster.sh turns CSI_ATTACHERS="drv=/path/csi.sock,..." into one
# `--csi-attacher drv=/path/csi.sock` arg each (#3128), but the controller-manager
# dials that socket from inside its static pod, so the socket's directory must
# be a hostPath volume of the pod. This is how kubeadm makes a host plugin
# socket dir reachable to the controller-manager: the flexvolume dir is mounted
# read-write as a DirectoryOrCreate hostPath because "it is used for third-party
# plugins to integrate with their storage backends via unix domain socket"
# (cmd/kubeadm/app/phases/controlplane/volumes.go:73-79).
#
# Run with: bash scripts/tests/test-csi-attacher-socket-mounts.sh

set -euo pipefail
IFS=$'\n\t'

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
BOOTSTRAP="$REPO_ROOT/scripts/bootstrap-cluster.sh"
MANIFEST="$REPO_ROOT/manifests/control-plane/kube-controller-manager.yaml"

if ! python3 -c 'import yaml' 2>/dev/null; then
    echo "test-csi-attacher-socket-mounts: needs python3 with PyYAML" >&2
    exit 2
fi

PASS_COUNT=0
FAIL_COUNT=0
FAILED_TESTS=()

fail() {
    echo "    FAIL: $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_TESTS+=("${CURRENT_TEST:-unknown}: $1")
}

assert_eq() {
    local expected="$1" actual="$2" label="${3:-assertion}"
    if [ "$expected" = "$actual" ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        fail "$label: expected '$expected', got '$actual'"
    fi
}

# Source just the helpers under test out of bootstrap-cluster.sh (the script
# itself runs a whole bootstrap when executed), then run $1 with the rest of
# the arguments.
run_helper() {
    bash -c '
        set -euo pipefail
        source <(sed -n "/^csi_socket_dirs()/,/^}/p" "$0")
        source <(sed -n "/^template_csi_attach()/,/^}/p" "$0")
        "$@"
    ' "$BOOTSTRAP" "$@"
}

# Render the committed manifest with the given CSI env and print the result.
render() {
    local tmp; tmp="$(mktemp -d)"
    sed -e 's|@CERTS_PATH@|/certs|g' -e 's|@CONTROL_PLANE_IMAGE@|img|g' \
        -e '/@NODE_IPAM_ARGS@/d' "$MANIFEST" > "$tmp/kcm.yaml"
    run_helper template_csi_attach "$tmp/kcm.yaml" >/dev/null 2>&1 \
        || echo "<template_csi_attach failed>" > "$tmp/kcm.yaml"
    cat "$tmp/kcm.yaml"
    rm -rf "$tmp"
}

# Python reads the rendered pod and prints one fact per line, so the assertions
# compare the parsed structure (not text) — a mis-indented insert fails here.
pod_facts() {
    python3 -c '
import sys, yaml
pod = yaml.safe_load(sys.stdin)
c = pod["spec"]["containers"][0]
print("args=" + " ".join(c["args"]))
for m in c.get("volumeMounts", []):
    print("mount=%s:%s:%s" % (m["name"], m["mountPath"], m.get("readOnly", False)))
for v in pod["spec"].get("volumes", []):
    hp = v.get("hostPath", {})
    print("volume=%s:%s:%s" % (v["name"], hp.get("path"), hp.get("type")))
'
}

# ----- Tests -----

test_socket_dirs_are_unique_parents_without_scheme() {
    local out
    out="$(CSI_ATTACHERS='a.csi=/var/lib/kubelet/plugins/a/csi.sock,b.csi=unix:///var/lib/kubelet/plugins/a/other.sock,c.csi=unix:/run/c/csi.sock' \
        run_helper csi_socket_dirs | paste -sd' ')" || out="<helper-missing>"
    assert_eq "/var/lib/kubelet/plugins/a /run/c" "$out" "unique socket dirs, unix scheme stripped"
}

test_no_attachers_no_dirs() {
    local out
    out="$(CSI_ATTACHERS='' run_helper csi_socket_dirs)" || out="<helper-missing>"
    assert_eq "" "$out" "no CSI_ATTACHERS -> no socket dirs"
}

test_off_by_default_leaves_pod_unchanged() {
    local facts
    facts="$(CSI_ATTACHERS='' ATTACH_DETACH_CONTROLLER='' render | pod_facts)" || facts="<render-failed>"
    assert_eq "mount=certs:/certs:True
volume=certs:/certs:Directory" "$(printf '%s\n' "$facts" | grep -E '^(mount|volume)=')" \
        "default pod mounts only certs"
    if printf '%s\n' "$facts" | grep -q -- '--csi-attacher\|--attach-detach-controller'; then
        fail "default pod must not carry CSI attach flags"
    else
        PASS_COUNT=$((PASS_COUNT + 1))
    fi
}

test_attacher_socket_dir_is_mounted() {
    local facts
    facts="$(CSI_ATTACHERS='hostpath.csi.k8s.io=/var/lib/kubelet/plugins/csi-hostpath/csi.sock,b.csi=unix:///run/b/csi.sock' \
        ATTACH_DETACH_CONTROLLER=1 render | pod_facts)" || facts="<render-failed>"
    assert_eq "args=--api-server-url https://api-server:6443 --kubeconfig /certs/controller-manager.conf --sync-interval 5 --csi-attacher hostpath.csi.k8s.io=/var/lib/kubelet/plugins/csi-hostpath/csi.sock --csi-attacher b.csi=unix:///run/b/csi.sock --attach-detach-controller" \
        "$(printf '%s\n' "$facts" | grep '^args=')" "attacher args"
    assert_eq "mount=certs:/certs:True
mount=csi-socket-0:/var/lib/kubelet/plugins/csi-hostpath:False
mount=csi-socket-1:/run/b:False" "$(printf '%s\n' "$facts" | grep '^mount=')" "socket dir mounts"
    assert_eq "volume=certs:/certs:Directory
volume=csi-socket-0:/var/lib/kubelet/plugins/csi-hostpath:DirectoryOrCreate
volume=csi-socket-1:/run/b:DirectoryOrCreate" "$(printf '%s\n' "$facts" | grep '^volume=')" "socket dir hostPath volumes"
}

test_no_placeholder_survives() {
    local out
    out="$(CSI_ATTACHERS='a=/s/a.sock' render)"
    if printf '%s\n' "$out" | grep -q '@CSI_'; then
        fail "rendered manifest still has a @CSI_ placeholder"
    else
        PASS_COUNT=$((PASS_COUNT + 1))
    fi
    out="$(CSI_ATTACHERS='' render)"
    if printf '%s\n' "$out" | grep -q '@CSI_'; then
        fail "default rendered manifest still has a @CSI_ placeholder"
    else
        PASS_COUNT=$((PASS_COUNT + 1))
    fi
}

# ----- Runner -----

for test_fn in $(declare -F | awk '{print $3}' | grep '^test_'); do
    CURRENT_TEST="$test_fn"
    echo "  $test_fn"
    "$test_fn"
done

echo
echo "passed: $PASS_COUNT  failed: $FAIL_COUNT"
if [ "$FAIL_COUNT" -gt 0 ]; then
    printf '%s\n' "${FAILED_TESTS[@]}"
    exit 1
fi
