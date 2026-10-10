#!/usr/bin/env bash
# Wire-format coverage report (WARNING-ONLY; exits 0 on gaps).
#
# Runs `wire_format_coverage_report`
# (crates/api-server/tests/it/wire_format_coverage_test.rs), which
#   * compares ProtoRegistry against every bundled upstream generated.proto that is
#     not already a hard gate (crates/api-server/proto/upstream/v1.35/), and
#   * walks every resource served by the real router (live /api + /apis
#     discovery) checking protobuf registry entries, a protobuf create/GET
#     round trip and OpenAPI v2/v3 definitions.
#
# Output (under $WIRE_FORMAT_REPORT_DIR, default target/wire-format-coverage):
#   report.json, summary.md, annotations.txt
#
# Usage:
#   bash scripts/wire-format-coverage.sh            # local: prints a digest
#   CI=1 bash scripts/wire-format-coverage.sh       # CI: also emits ::warning
#                                                   # annotations + $GITHUB_STEP_SUMMARY
#
# Extra cargo args (e.g. feature flags matching an already-built test binary so
# nothing recompiles) go in WFC_CARGO_ARGS.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export WIRE_FORMAT_REPORT_DIR="${WIRE_FORMAT_REPORT_DIR:-$ROOT/target/wire-format-coverage}"
rm -rf "$WIRE_FORMAT_REPORT_DIR"
mkdir -p "$WIRE_FORMAT_REPORT_DIR"

# shellcheck disable=SC2086
if command -v cargo-nextest >/dev/null 2>&1 && [ "${WFC_USE_CARGO_TEST:-0}" != 1 ]; then
    # --workspace + the CI feature set keeps the already-compiled nextest
    # artifacts valid (a `-p` selection would unify features differently and
    # force a rebuild). Passing tests hide output, the report goes to files.
    cargo nextest run --workspace ${WFC_CARGO_ARGS:---features rusternetes-api-server/sqlite} \
        --locked -E 'test(wire_format_coverage_report)' --no-tests=fail --status-level fail
else
    cargo test -p rusternetes-api-server --test it wire_format_coverage_report -- --nocapture
fi

if [ ! -s "$WIRE_FORMAT_REPORT_DIR/summary.md" ]; then
    echo "wire-format-coverage: report was not produced (harness error)" >&2
    exit 1
fi

# Digest for humans.
head -n 3 "$WIRE_FORMAT_REPORT_DIR/summary.md"
echo
cat "$WIRE_FORMAT_REPORT_DIR/annotations.txt" | sed 's/^::warning title=\([^:]*\)::/WARN \1: /'
echo
echo "Full report: $WIRE_FORMAT_REPORT_DIR/summary.md (markdown) and report.json"

if [ "${CI:-}" != "" ]; then
    # GitHub caps annotations at ~10 per step: the test emits one per check
    # (7 max), per-group counts inline; the full table goes to the summary.
    cat "$WIRE_FORMAT_REPORT_DIR/annotations.txt"
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
        cat "$WIRE_FORMAT_REPORT_DIR/summary.md" >>"$GITHUB_STEP_SUMMARY"
    fi
fi
exit 0
