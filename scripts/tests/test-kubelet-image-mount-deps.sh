#!/usr/bin/env bash
# Guard #3333: every image that runs the kubelet must carry the NFS mount
# helper, or the nfs volume plugin (#3286/#3337) cannot mount.
#
# The plugin execs `mount -t nfs ...` (mount_ops.rs DEFAULT_MOUNT_COMMAND, a
# port of mount_linux.go doMount: exec.Command(mountCmd, ...)). mount(8) comes
# from debian's base util-linux (already in debian:sid-slim); `mount -t nfs`
# then execs /sbin/mount.nfs, which only the `nfs-common` package provides.
# Without it the kubelet fails with "mount: ... you might need a
# /sbin/mount.<type> helper program".
#
# Static check (no docker needed): each kubelet-bearing Dockerfile stage must
# list nfs-common in its apt-get install. node.Dockerfile layers on the
# containerd image, so it needs its own install.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

fail=0

# Print the stage that starts at the first line matching $2 in file $1, up to
# the next FROM.
stage_body() {
  awk -v pat="$2" '
    on && /^FROM / { exit }
    $0 == pat { on=1 }
    on { print }
  ' "$1"
}

check_stage() {
  local file="$1" pat="$2" label="$3"
  local body
  body="$(stage_body "$file" "$pat")"
  if [ -z "$body" ]; then
    echo "FAIL: $file: stage '$label' not found" >&2
    fail=1
    return
  fi
  if ! grep -qE '(^|[[:space:]])nfs-common([[:space:]\\]|$)' <<<"$body"; then
    echo "FAIL: $file: $label stage does not install nfs-common (nfs volumes cannot mount, #3333)" >&2
    fail=1
  fi
}

check_stage services.Dockerfile "FROM debian:sid-slim AS kubelet" 'kubelet'
check_stage local-binary.services.Dockerfile "FROM debian:sid-slim AS kubelet" 'kubelet'
check_stage all-in-one.Dockerfile "FROM debian:sid-slim" 'runtime (in-process kubelet)'
check_stage node.Dockerfile 'FROM ${CONTAINERD_IMAGE}' 'node (kubelet + containerd)'

if [ "$fail" -ne 0 ]; then
  exit 1
fi
echo "OK: all kubelet-bearing images install nfs-common"
