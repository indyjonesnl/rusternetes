#!/usr/bin/env bash
# Regenerate the golden corpora of the CEL checker tests from the oracle.
#
#   CEL_ORACLE=/path/to/celoracle tools/cel-oracle/gen-goldens.sh
#
# Two corpora, each `requests.jsonl` -> `goldens.jsonl` (one {"req": ...,
# "resp": ...} object per line):
#   crates/cel-checker/tests/corpus/      checker requests
#   crates/common/tests/corpus/crd_*      CRD rule requests (`schema` set)
# Normal test runs compare the Rust checker against the checked-in goldens and
# need no Go.
set -euo pipefail

: "${CEL_ORACLE:?set CEL_ORACLE to the celoracle binary (see build.sh)}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

gen() {
  local requests="$1" goldens="$2" tmp
  tmp="$(mktemp)"
  "$CEL_ORACLE" <"$requests" >"$tmp"
  python3 -I - "$requests" "$tmp" "$goldens" <<'EOF'
import json, sys
reqs = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
resps = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
assert len(reqs) == len(resps), (len(reqs), len(resps))
with open(sys.argv[3], "w") as out:
    for q, r in zip(reqs, resps):
        out.write(json.dumps({"req": q, "resp": r}, sort_keys=True) + "\n")
EOF
  rm -f "$tmp"
  echo "wrote $goldens"
}

dir="$root/crates/cel-checker/tests/corpus"
gen "$dir/requests.jsonl" "$dir/goldens.jsonl"
dir="$root/crates/common/tests/corpus"
gen "$dir/crd_requests.jsonl" "$dir/crd_goldens.jsonl"
