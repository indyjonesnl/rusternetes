#!/usr/bin/env bash
# scripts/k0s-diff/gen-compose.sh — emit compose.k0s-v0.yml … compose.k0s-v6.yml
# from the single template. No hand edits: the per-variant files differ ONLY in
# the swap wiring (build-arg + label) and the variant-scoped names.
#
# Swap mechanism (see SPIKE-FINDINGS.md, Crux 1):
#   v0            baseline, no swap — resolves to the plain all-Go k0s node.
#   v1..v4        baked custom image: the build arg SWAP_COMPONENT=<component>
#                 tells Dockerfile.k0s-node which upstream Go binary to replace
#                 with its Rusternetes build (shim baked in a later task).
#   v5 (kube-proxy), v6 (dns)
#                 workload swap: same base image; run-variant.sh swaps the
#                 DaemonSet/Deployment via kubectl. The com.k0s-diff.swap label
#                 is the machine-readable marker for that path.
#
# v1..v6 shims do NOT exist yet (built in Tasks 3–6); this only wires the
# plumbing and does NOT build or bring those images up.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
source "$here/lib.sh"
tpl="$here/compose.k0s.template.yml"
[ -f "$tpl" ] || { echo "template not found: $tpl" >&2; exit 1; }

for i in "${!VARIANTS[@]}"; do
  v="${VARIANTS[$i]}"
  swap="${SWAP[$i]}"
  out="$here/compose.k0s-${v}.yml"
  awk -v variant="$v" -v swap="$swap" '
    { gsub(/@@VARIANT@@/, variant) }
    /^@@BUILD_ARGS@@$/ {
      if (swap != "") {
        print "      args:"
        print "        SWAP_COMPONENT: \"" swap "\""
      }
      next
    }
    /^@@SWAP_LABEL@@$/ {
      if (swap != "") {
        print "    labels:"
        print "      com.k0s-diff.variant: \"" variant "\""
        print "      com.k0s-diff.swap: \"" swap "\""
      }
      next
    }
    { print }
  ' "$tpl" > "$out"
  log "generated $(basename "$out") (swap='${swap:-<none>}')"
done

log "generated ${#VARIANTS[@]} compose files under $here"
