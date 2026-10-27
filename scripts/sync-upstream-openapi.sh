#!/usr/bin/env bash
# Regenerate crates/api-server/openapi/upstream/v1.35/* from the upstream
# kubernetes checkout (default ../kubernetes, release-1.35).
#
# Upstream publishes the generated OpenAPI documents under
# api/openapi-spec/ (kube-openapi `pkg/builder` output of
# zz_generated.openapi.go). We vendor, per group-version, only the
# `components.schemas` map of the v3 document of every group-version the
# server publishes, plus the v2 definitions of those group-versions with their
# transitive `$ref` closure.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
K8S="${K8S_CHECKOUT:-$ROOT/../kubernetes}"
OUT="$ROOT/crates/api-server/openapi/upstream/v1.35"
mkdir -p "$OUT"
python3 -I - "$K8S" "$OUT" <<'PY'
import json, sys
k8s, out = sys.argv[1], sys.argv[2]
# The group-versions our /openapi/v3 index publishes (handlers/openapi.rs
# `get_openapi_spec`); `api__v1` is the core group.
GVS = [
    "api__v1",
    "apis__admissionregistration.k8s.io__v1",
    "apis__admissionregistration.k8s.io__v1beta1",
    "apis__apiextensions.k8s.io__v1",
    "apis__apiregistration.k8s.io__v1",
    "apis__apps__v1",
    "apis__authentication.k8s.io__v1",
    "apis__authorization.k8s.io__v1",
    "apis__autoscaling__v1",
    "apis__autoscaling__v2",
    "apis__batch__v1",
    "apis__certificates.k8s.io__v1",
    "apis__certificates.k8s.io__v1alpha1",
    "apis__certificates.k8s.io__v1beta1",
    "apis__coordination.k8s.io__v1",
    "apis__discovery.k8s.io__v1",
    "apis__events.k8s.io__v1",
    "apis__flowcontrol.apiserver.k8s.io__v1",
    "apis__networking.k8s.io__v1",
    "apis__node.k8s.io__v1",
    "apis__policy__v1",
    "apis__rbac.authorization.k8s.io__v1",
    "apis__resource.k8s.io__v1",
    "apis__scheduling.k8s.io__v1",
    "apis__storage.k8s.io__v1",
]
served = set()
for gv in GVS:
    doc = json.load(open(f"{k8s}/api/openapi-spec/v3/{gv}_openapi.json"))
    name = gv.replace("apis__", "", 1) if gv.startswith("apis__") else gv
    with open(f"{out}/{name}.schemas.json", "w") as f:
        json.dump(doc["components"]["schemas"], f, sort_keys=True, separators=(",", ":"))
    parts = gv.split("__")
    served.add(("" if parts[0] == "api" else parts[1], parts[-1]))
# v3 group-level documents (`api`, `apis`, `apis/<group>`): each is the
# APIVersions/APIGroupList/APIGroup discovery endpoint definition (tiny).
groups = {}
for gv in GVS:
    parts = gv.split("__")
    if parts[0] == "apis":
        groups.setdefault(parts[1], f"apis__{parts[1]}_openapi.json")
docs = {"api": json.load(open(f"{k8s}/api/openapi-spec/v3/api_openapi.json")),
        "apis": json.load(open(f"{k8s}/api/openapi-spec/v3/apis_openapi.json"))}
for g, f in groups.items():
    docs[f"apis/{g}"] = json.load(open(f"{k8s}/api/openapi-spec/v3/{f}"))
with open(f"{out}/group_docs.json", "w") as f:
    json.dump(docs, f, sort_keys=True, separators=(",", ":"))
# v2: every definition of a served group-version plus its transitive `$ref`
# closure (the swagger.json the kube-apiserver publishes for those APIs).
defs = json.load(open(f"{k8s}/api/openapi-spec/swagger.json"))["definitions"]
roots = [n for n, d in defs.items()
         if any((g["group"], g["version"]) in served
                for g in d.get("x-kubernetes-group-version-kind", []))]
seen, todo = {}, list(roots)
def refs(v):
    if isinstance(v, dict):
        for k, x in v.items():
            if k == "$ref" and isinstance(x, str):
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
with open(f"{out}/v2.definitions.json", "w") as f:
    json.dump(seen, f, sort_keys=True, separators=(",", ":"))
PY
