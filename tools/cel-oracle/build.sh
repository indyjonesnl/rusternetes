#!/usr/bin/env bash
# Build the CEL test oracle from a local Kubernetes release-1.35 checkout.
#
#   K8S_SRC=/path/to/kubernetes tools/cel-oracle/build.sh [output-binary]
#
# Local build only: everything comes from $K8S_SRC (its vendor/ and staging/
# trees); nothing is downloaded (GOPROXY=off, -mod=vendor). The oracle source
# is compiled inside a scratch module that mirrors $K8S_SRC's module and
# workspace files with symlinks to its vendor/ and staging/ trees, so the
# Kubernetes checkout is never modified. The binary is a dev/CI test helper:
# it is never shipped and is not part of the Cargo workspace.
#
# If the system Go is older than the checkout's go.mod requires, set
# GOTOOLCHAIN to an already installed toolchain (e.g. GOTOOLCHAIN=go1.25.12);
# GOPROXY=off keeps Go from fetching one.
set -euo pipefail

: "${K8S_SRC:?set K8S_SRC to a Kubernetes release-1.35 checkout}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="${1:-$here/celoracle}"
case "$out" in /*) ;; *) out="$PWD/$out" ;; esac

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/cmd/celoracle"
cp "$K8S_SRC/go.mod" "$K8S_SRC/go.sum" "$K8S_SRC/go.work" "$work/"
[ -f "$K8S_SRC/go.work.sum" ] && cp "$K8S_SRC/go.work.sum" "$work/"
ln -s "$K8S_SRC/vendor" "$work/vendor"
ln -s "$K8S_SRC/staging" "$work/staging"
cp "$here/main.go" "$work/cmd/celoracle/main.go"

(cd "$work" && GOFLAGS=-mod=vendor GOPROXY=off go build -o "$out" ./cmd/celoracle)
echo "built $out"
