#!/usr/bin/env python3
"""Generate crates/common/src/patch/patch_meta.json from the vendored
upstream swagger.json (release-1.35).

Strategic merge patch metadata upstream comes from the Go struct tags
`patchStrategy` / `patchMergeKey` (apimachinery strategicpatch
`PatchMetaFromStruct`); the swagger spec is generated from the same tags
(`x-kubernetes-patch-strategy`, `x-kubernetes-patch-merge-key`), so it is
the machine-readable form of that metadata.

Type encoding of a field: `S:<definition>` struct, `L:<elem>` slice,
`M:<elem>` map, `P:<kind>` primitive.

Usage: python3 scripts/gen-patch-meta.py
"""
import json
import pathlib

root = pathlib.Path(__file__).resolve().parent.parent
src = root / "tests/upstream/k8s-1.35/openapi-spec/swagger.json"
dst = root / "crates/common/src/patch/patch_meta.json"
defs = json.load(open(src))["definitions"]

PRIM = {"string": "string", "integer": "int", "number": "float64", "boolean": "bool"}


def enc(p):
    ref = p.get("$ref") or (p.get("allOf") or [{}])[0].get("$ref")
    if ref:
        return "S:" + ref.rsplit("/", 1)[1]
    t = p.get("type")
    if t == "array":
        return "L:" + enc(p["items"])
    if t == "object":
        ap = p.get("additionalProperties")
        return "M:" + (enc(ap) if isinstance(ap, dict) else "P:interface")
    return "P:" + PRIM.get(t, "interface")


out_defs, gvks = {}, {}
for name, d in defs.items():
    fields = {}
    for f, p in (d.get("properties") or {}).items():
        fields[f] = [
            enc(p),
            p.get("x-kubernetes-patch-strategy", ""),
            p.get("x-kubernetes-patch-merge-key", ""),
        ]
    out_defs[name] = fields
    for g in d.get("x-kubernetes-group-version-kind") or []:
        gvks[f'{g["group"]}/{g["version"]}/{g["kind"]}'] = name

with open(dst, "w") as fh:
    json.dump({"gvk": gvks, "defs": out_defs}, fh, separators=(",", ":"), sort_keys=True)
    fh.write("\n")
print(dst, dst.stat().st_size)
