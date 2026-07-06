#!/usr/bin/env bash
# scripts/k0s-diff/run-variant.sh <vN> <sig>
#
# Bring up one variant's compose stack, obtain its admin kubeconfig, run the
# smoke gate, and — only if smoke passes — run ONE conformance sig via
# Hydrophone. Writes:
#   results/<vN>/<sig>/          raw Hydrophone output (e2e.log, junit_*.xml)
#   results/<vN>/<sig>.json      one-line summary {pass,fail,image,...}
# and prints "PASS n / FAIL m" to stdout.
#
# On smoke failure: writes {"smoke":"fail"} to results/<vN>/<sig>.json and
# exits 0 WITHOUT running conformance (the gate).
#
# NOTE: this deliberately does NOT call results-diff.sh (built in Task 7); it
# inlines a minimal pass/fail count parsed from Hydrophone's own output.
#
# Env knobs:
#   K0S_DIFF_FOCUS   override the ginkgo --focus regex (e.g. a single test, to
#                    de-risk the wiring before a full sig run).
#   K0S_DIFF_SKIP    ginkgo --skip regex (default empty = run everything).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
source "$here/lib.sh"
require_docker

v="${1:?usage: run-variant.sh <vN> <sig>}"
sig="${2:?usage: run-variant.sh <vN> <sig>}"

# Validate the variant is known.
known=0
for x in "${VARIANTS[@]}"; do [ "$x" = "$v" ] && known=1; done
[ "$known" = 1 ] || { echo "unknown variant '$v' (known: ${VARIANTS[*]})" >&2; exit 1; }

f="$here/compose.k0s-${v}.yml"
[ -f "$f" ] || { echo "no compose file for $v: $f — run gen-compose.sh first" >&2; exit 1; }

resdir="$here/results/${v}"
mkdir -p "$resdir"

# Bring the variant up. Build the image ONLY when it is missing (or when
# K0S_DIFF_REBUILD=1): an unconditional `up --build` re-exports a fresh image
# digest every run, which forces a container recreate and churns the kube-system
# pods (racing the smoke gate). Building-when-absent keeps an already-healthy
# variant reused in place — no recreate.
img="$(awk '/^[[:space:]]*image:/{print $2; exit}' "$f")"
if [ "${K0S_DIFF_REBUILD:-0}" = 1 ] || ! docker image inspect "$img" >/dev/null 2>&1; then
  log "building image $img"
  docker compose -f "$f" build
fi
log "bringing up $v from $(basename "$f")"
docker compose -f "$f" up -d

# --- kubeconfig from the container, server rewritten to the published port ----
cname="k0s-diff-${v}"
kc="/tmp/k0s-diff-${v}.kubeconfig"
log "waiting for k0s admin kubeconfig from $cname"
for _ in $(seq 1 60); do
  if docker exec "$cname" k0s kubeconfig admin > "$kc" 2>/dev/null && [ -s "$kc" ]; then
    break
  fi
  sleep 2
done
[ -s "$kc" ] || { echo "could not obtain kubeconfig from $cname" >&2; exit 1; }
sed -i 's#server: https://.*:6443#server: https://127.0.0.1:26444#' "$kc"
export KUBECONFIG="$kc"

# --- smoke gate ---------------------------------------------------------------
if ! bash "$here/smoke.sh"; then
  log "SMOKE-FAIL $v — writing marker, skipping conformance"
  printf '{"variant":"%s","sig":"%s","smoke":"fail"}\n' "$v" "$sig" > "$resdir/${sig}.json"
  exit 0
fi

# --- conformance image MUST match the running server version exactly ----------
gitv="$(kubectl get --raw /version | python3 -c 'import json,sys;print(json.load(sys.stdin)["gitVersion"])')"
img_tag="${gitv%%+*}"                       # v1.35.5+k0s -> v1.35.5
conf_image="registry.k8s.io/conformance:${img_tag}"
log "server $gitv -> conformance image $conf_image"

# --- one sig's Conformance tests ----------------------------------------------
sig_short="${sig#sig-}"
focus="${K0S_DIFF_FOCUS:-\\[sig-${sig_short}\\].*\\[Conformance\\]}"
skip="${K0S_DIFF_SKIP:-}"
outdir="$resdir/${sig}"
mkdir -p "$outdir"

# NB: hydrophone treats --conformance and --focus as mutually exclusive, so we
# drive the run with --focus only; the regex carries \[Conformance\] itself.
hydro_args=(--conformance-image "$conf_image"
  --focus "$focus"
  --kubeconfig "$KUBECONFIG"
  --output-dir "$outdir")
[ -n "$skip" ] && hydro_args+=(--skip "$skip")

log "hydrophone --focus '$focus' ${skip:+--skip '$skip'}"
hydrophone "${hydro_args[@]}" || true

# --- minimal pass/fail count (results-diff.sh is Task 7) ----------------------
read -r pass fail < <(python3 - "$outdir" <<'PY'
import glob, os, re, sys, xml.etree.ElementTree as ET
d = sys.argv[1]
passed = failed = 0
found = False

# PRIMARY: the ginkgo summary line in e2e.log — its own authoritative count,
# which (unlike raw junit) excludes the synthetic [ReportBeforeSuite]/
# [ReportAfterSuite]/[SynchronizedBeforeSuite] pseudo-specs.
for lf in glob.glob(os.path.join(d, "**", "e2e.log"), recursive=True):
    try:
        txt = open(lf, encoding="utf-8", errors="replace").read()
    except Exception:
        continue
    m = re.search(r"(\d+)\s+Passed\s*\|\s*(\d+)\s+Failed", txt)
    if m:
        passed, failed, found = int(m.group(1)), int(m.group(2)), True

# FALLBACK: junit, skipping ginkgo's synthetic report/suite nodes.
if not found:
    SYNTH = ("[ReportBeforeSuite", "[ReportAfterSuite", "[SynchronizedBeforeSuite",
             "[SynchronizedAfterSuite", "[BeforeSuite", "[AfterSuite", "[DeferCleanup")
    for jf in glob.glob(os.path.join(d, "**", "junit*.xml"), recursive=True):
        try:
            root = ET.parse(jf).getroot()
        except Exception:
            continue
        for tc in root.iter("testcase"):
            name = tc.get("name", "")
            if name.startswith(SYNTH):
                continue
            kinds = [c.tag for c in tc]
            if "failure" in kinds or "error" in kinds:
                failed += 1
            elif "skipped" in kinds:
                pass
            else:
                passed += 1
print(passed, failed)
PY
)
pass="${pass:-0}"; fail="${fail:-0}"

printf '{"variant":"%s","sig":"%s","image":"%s","serverVersion":"%s","pass":%s,"fail":%s}\n' \
  "$v" "$sig" "$conf_image" "$gitv" "$pass" "$fail" > "$resdir/${sig}.json"

echo "PASS $pass / FAIL $fail"
