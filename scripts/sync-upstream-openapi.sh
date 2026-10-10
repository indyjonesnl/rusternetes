#!/usr/bin/env bash
# Regenerate crates/api-server/openapi/upstream/v1.35/* from the upstream
# kubernetes checkout (default ../kubernetes, release-1.35).
#
# Upstream publishes the generated OpenAPI documents under
# api/openapi-spec/ (kube-openapi `pkg/builder` output of
# zz_generated.openapi.go). We vendor, per group-version, only the
# `components.schemas` map of the v3 document, plus the transitive closure of
# the few v2 definitions served on top of the hand-written stubs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
K8S="${K8S_CHECKOUT:-$ROOT/../kubernetes}"
OUT="$ROOT/crates/api-server/openapi/upstream/v1.35"
mkdir -p "$OUT"
python3 -I - "$K8S" "$OUT" <<'PY'
import json, sys
k8s, out = sys.argv[1], sys.argv[2]
for gv in ["apps__v1", "batch__v1"]:
    doc = json.load(open(f"{k8s}/api/openapi-spec/v3/apis__{gv}_openapi.json"))
    with open(f"{out}/{gv}.schemas.json", "w") as f:
        json.dump(doc["components"]["schemas"], f, sort_keys=True, separators=(",", ":"))
defs = json.load(open(f"{k8s}/api/openapi-spec/swagger.json"))["definitions"]
roots = ["io.k8s.api.apps.v1.ControllerRevision", "io.k8s.api.autoscaling.v1.Scale"]
seen, todo = {}, list(roots)
def refs(v):
    if isinstance(v, dict):
        for k, x in v.items():
            if k == "$ref":
                yield x.rsplit("/", 1)[-1]
            else:
                yield from refs(x)
    elif isinstance(v, list):
        for x in v:
            yield from refs(x)
while todo:
    n = todo.pop()
    if n in seen:
        continue
    seen[n] = defs[n]
    todo.extend(refs(defs[n]))
with open(f"{out}/v2_extra.definitions.json", "w") as f:
    json.dump(seen, f, sort_keys=True, separators=(",", ":"))
PY
