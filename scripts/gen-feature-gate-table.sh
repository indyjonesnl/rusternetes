#!/usr/bin/env bash
# Regenerate crates/common/src/feature_gates/upstream_table.rs from the
# Kubernetes checkout (default ../kubernetes, release-1.35).
#
#   scripts/gen-feature-gate-table.sh            # rewrite the table
#   scripts/gen-feature-gate-table.sh --check    # fail if the table is stale
#
# Needs `go`, `python3` and `rustfmt`. The Go dumper is copied into the
# checkout's hack/zzdump for the run and removed afterwards.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
k8s="${K8S_DIR:-$root/../kubernetes}"
target="$root/crates/common/src/feature_gates/upstream_table.rs"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp" "$k8s/hack/zzdump"' EXIT

sha="$(git -C "$k8s" rev-parse HEAD)"
mkdir -p "$k8s/hack/zzdump"
cp "$root/scripts/feature-gates/dump.go.txt" "$k8s/hack/zzdump/main.go"
(cd "$k8s" && GOFLAGS=-mod=vendor go run ./hack/zzdump) >"$tmp/gates.json"
python3 -I "$root/scripts/gen_feature_gate_table.py" "$sha" <"$tmp/gates.json" >"$tmp/table.rs"
rustfmt --edition 2021 "$tmp/table.rs"

if [ "${1:-}" = "--check" ]; then
    diff -u "$target" "$tmp/table.rs"
else
    cp "$tmp/table.rs" "$target"
fi
