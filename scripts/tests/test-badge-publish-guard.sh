#!/usr/bin/env bash
# Guard: only a run on `main` may publish a README badge.
#
# The README badges are the project's source of truth for conformance state.
# Every badge publisher is reachable from a branch — `conformance-target.yml`
# and `vanilla-swap-module.yml` via `workflow_call` from a dispatchable caller,
# `node-conformance.yml` and `cert-manager-smoke.yml` via `workflow_dispatch`
# directly — so without a ref guard a run on any branch silently overwrites
# main's number. That happened: run 33417910655, dispatched on
# `fix/1831-put-should-delete-during-update`, pushed 93/95 over the
# sig-api-machinery badge (#1835).
#
# This asserts every step that RUNS scripts/update-badge.sh is gated on the
# canonical ref token below. It deliberately checks the step's own `if:` rather
# than the workflow's triggers: a reusable workflow cannot restrict how its
# callers are invoked.
#
# Run with: bash scripts/tests/test-badge-publish-guard.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WF_DIR="$REPO_ROOT/.github/workflows"

# The one spelling every badge step must use. Kept as a literal so a reviewer
# can grep for it, and so a near-miss (ref_name, a different branch) fails.
GUARD="github.ref == 'refs/heads/main'"
# Only a step that INVOKES the publisher counts. A `paths:` entry naming the
# script, or a step running scripts/tests/test-update-badge.sh, must not match.
INVOCATION="bash scripts/update-badge.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

[ -d "$WF_DIR" ] || fail "no workflows dir at $WF_DIR"

# awk is mawk in CI: use index() rather than dynamic regex (#1699).
# A step starts at a `- name:` line at any indent; a step "publishes" if its
# block invokes the publisher; such a block must also carry the guard.
report="$(
    for f in "$WF_DIR"/*.yml "$WF_DIR"/*.yaml; do
        [ -e "$f" ] || continue
        grep -qF "$INVOCATION" "$f" || continue
        awk -v file="$f" -v guard="$GUARD" -v invocation="$INVOCATION" '
            function flush_step() {
                if (block != "" && index(block, invocation) > 0) {
                    if (index(block, guard) == 0) {
                        print file "\t" step_line "\t" step_name
                    }
                }
            }
            /^[[:space:]]*-[[:space:]]+name:[[:space:]]/ {
                flush_step()
                block = ""
                step_line = NR
                step_name = $0
                sub(/^[[:space:]]*-[[:space:]]+name:[[:space:]]*/, "", step_name)
            }
            { block = block "\n" $0 }
            END { flush_step() }
        ' "$f"
    done
)"

if [ -n "$report" ]; then
    echo "Badge-publishing steps missing the main-only guard:" >&2
    echo "$report" | while IFS=$'\t' read -r file line name; do
        echo "  $(basename "$file"):$line — \"$name\"" >&2
    done
    echo >&2
    echo "Add to that step's 'if:' condition:  $GUARD" >&2
    fail "a branch run could overwrite a README badge (#1835)"
fi

# Sanity: the assertion above is vacuous if nothing publishes badges at all.
# Counted without a pipeline — under `pipefail` a grep that matches nothing (or
# hits an unexpanded *.yaml glob) would abort the script before it reports.
publishers=0
for f in "$WF_DIR"/*.yml "$WF_DIR"/*.yaml; do
    [ -e "$f" ] || continue
    if grep -qF "$INVOCATION" "$f"; then
        publishers=$((publishers + 1))
    fi
done
[ "$publishers" -ge 1 ] || fail "found no badge publishers — has update-badge.sh moved?"

echo "PASS: all $publishers badge-publishing workflow(s) are gated on main"
