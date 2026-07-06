#!/usr/bin/env bash
# scripts/k0s-diff/run-matrix.sh — drive the FULL VARIANTS x SIGS conformance
# matrix, sequentially, then render the results-diff.sh grid.
#
# *** WALL-COST WARNING ***
# This is HOURS, not minutes: 7 variants x 9 sigs x (bring-up + smoke gate +
# a full Hydrophone --focus run). A single sig-node run alone (105 specs) ran
# ~10+ minutes; sig-network (47 specs) ~13 minutes; several sigs (sig-apps,
# sig-storage) have far more specs than that. Budget for a multi-hour run
# even with the smoke gate skipping conformance on variants that never
# converge (v1-v5 in the current harness state — see RESULTS.md). Do NOT
# invoke this from CI or an unattended script without that budget; it is
# meant to be run directly, deliberately, by a human at a terminal:
#
#   bash scripts/k0s-diff/run-matrix.sh
#
# All variants publish the SAME host port (26444 -> k0s admin API), so only
# ONE variant's compose stack can be up at a time. Each iteration tears the
# previous variant's stack down (`compose down -v`) before bringing the next
# one up — this is why the matrix is sequential, not parallel.
#
# Guard: this file executes the sweep unconditionally when run, by design
# (Step 3 of the task brief asks for grid-only smoke testing via
# results-diff.sh directly, not via this file). Never `source` this script —
# only invoke it directly, and only when you intend to pay the wall-cost
# above. `bash -n run-matrix.sh` (syntax check only) and sourcing under
# `set -u`/no direct exec both leave BASH_SOURCE != $0, so guard on that:
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "run-matrix.sh must be executed directly (not sourced) — refusing to run the sweep." >&2
  return 1 2>/dev/null || exit 1
fi

set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
source "$here/lib.sh"
require_docker

bash "$here/gen-compose.sh"

for v in "${VARIANTS[@]}"; do
  for s in "${SIGS[@]}"; do
    log "=== $v / $s ==="
    bash "$here/run-variant.sh" "$v" "$s"
    docker compose -f "$here/compose.k0s-${v}.yml" down -v || true
  done
done

bash "$here/results-diff.sh" grid
