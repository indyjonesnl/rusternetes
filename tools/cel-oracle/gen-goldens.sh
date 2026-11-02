#!/usr/bin/env bash
# Regenerate the golden corpus of the CEL checker tests from the oracle.
#
#   CEL_ORACLE=/path/to/celoracle tools/cel-oracle/gen-goldens.sh
#
# Reads crates/cel-checker/tests/corpus/requests.jsonl, runs every request
# through the oracle and writes crates/cel-checker/tests/corpus/goldens.jsonl,
# one {"req": ..., "resp": ...} object per line. Normal test runs compare the
# Rust checker against the checked-in goldens and need no Go.
set -euo pipefail

: "${CEL_ORACLE:?set CEL_ORACLE to the celoracle binary (see build.sh)}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dir="$root/crates/cel-checker/tests/corpus"

"$CEL_ORACLE" <"$dir/requests.jsonl" >"$dir/.responses.jsonl"
python3 -I - "$dir/requests.jsonl" "$dir/.responses.jsonl" "$dir/goldens.jsonl" <<'EOF'
import json, sys
reqs = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
resps = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
assert len(reqs) == len(resps), (len(reqs), len(resps))
with open(sys.argv[3], "w") as out:
    for q, r in zip(reqs, resps):
        out.write(json.dumps({"req": q, "resp": r}, sort_keys=True) + "\n")
EOF
rm -f "$dir/.responses.jsonl"
echo "wrote $dir/goldens.jsonl"
