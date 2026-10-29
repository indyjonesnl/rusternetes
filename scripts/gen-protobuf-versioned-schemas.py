#!/usr/bin/env python3
"""Generate crates/protobuf/src/versioned_schemas.rs from upstream generated.proto.

Why: the hand-written registry in `lib.rs` keeps ONE schema per bare kind (the
GA shape). Older API versions (apps/v1beta1 Scale, rbac/v1alpha1 Role, ...) and
kinds the registry never learned (LeaseCandidate, StorageVersion, ...) have a
different wire layout upstream, so their `generated.proto` message is the only
faithful source. This script reads the vendored-by-upstream `generated.proto`
of each listed (group, version), and emits, for every listed root message and
every same-package message reachable from it, a schema registered under the
group-qualified key `<group>/<version>.<Message>` (core: `<version>.<Message>`).
`encode_native` / `decode_k8s_resource` already prefer that key over the bare
kind (see `ProtoRegistry::decode_k8s_resource`).

Source of truth (kubernetes release-1.35), per package (k8s.io/api/<group>/<version>
or k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/<version>):
  <package>/generated.proto   (field numbers/types)
  <package>/types.go          (`json:",inline"` embeds)
  staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto (WatchEvent)

Messages of OTHER packages (core/v1 PodTemplateSpec, meta/v1 ObjectMeta, ...)
are referenced by their bare registry name; `versioned_schemas.rs` carries a
test that every such reference resolves in the registry.

Usage:
  scripts/gen-protobuf-versioned-schemas.py            # rewrite the .rs file
  scripts/gen-protobuf-versioned-schemas.py --stdout   # print, do not write
Env: K8S_SRC=<path to a kubernetes checkout> (default: ../kubernetes next to
the repo, or any ancestor's sibling `kubernetes`).
"""
import glob
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "crates/protobuf/src/versioned_schemas.rs")

# (staging path of the package dir, registry key prefix, root messages).
PACKAGES = [
    ("k8s.io/api/apps/v1beta1", "apps/v1beta1", ["Scale", "Deployment", "DeploymentRollback"]),
    ("k8s.io/api/apps/v1beta2", "apps/v1beta2", ["Scale"]),
    ("k8s.io/api/extensions/v1beta1", "extensions/v1beta1",
     ["Scale", "Deployment", "DaemonSet", "Ingress", "DeploymentRollback"]),
    ("k8s.io/api/authorization/v1beta1", "authorization.k8s.io/v1beta1",
     ["SubjectAccessReview", "LocalSubjectAccessReview"]),
    ("k8s.io/api/autoscaling/v2beta1", "autoscaling/v2beta1", ["HorizontalPodAutoscaler"]),
    ("k8s.io/api/autoscaling/v2beta2", "autoscaling/v2beta2", ["HorizontalPodAutoscaler"]),
    ("k8s.io/api/discovery/v1beta1", "discovery.k8s.io/v1beta1", ["EndpointSlice"]),
    ("k8s.io/api/events/v1beta1", "events.k8s.io/v1beta1", ["Event"]),
    # `Subject`/`PolicyRulesWithSubjects` differ from the bare rbac `Subject`
    # (user/group/serviceAccount sub-messages), so FlowSchema is a root too.
    ("k8s.io/api/flowcontrol/v1", "flowcontrol.apiserver.k8s.io/v1",
     ["FlowSchema", "PriorityLevelConfiguration"]),
    ("k8s.io/api/flowcontrol/v1beta1", "flowcontrol.apiserver.k8s.io/v1beta1",
     ["FlowSchema", "PriorityLevelConfiguration"]),
    ("k8s.io/api/flowcontrol/v1beta2", "flowcontrol.apiserver.k8s.io/v1beta2",
     ["FlowSchema", "PriorityLevelConfiguration"]),
    ("k8s.io/api/flowcontrol/v1beta3", "flowcontrol.apiserver.k8s.io/v1beta3",
     ["FlowSchema", "PriorityLevelConfiguration"]),
    ("k8s.io/api/networking/v1beta1", "networking.k8s.io/v1beta1", ["Ingress"]),
    ("k8s.io/api/node/v1alpha1", "node.k8s.io/v1alpha1", ["RuntimeClass"]),
    ("k8s.io/api/rbac/v1alpha1", "rbac.authorization.k8s.io/v1alpha1",
     ["ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding"]),
    ("k8s.io/api/resource/v1beta1", "resource.k8s.io/v1beta1",
     ["ResourceClaim", "DeviceClass", "ResourceClaimTemplate", "ResourceSlice"]),
    ("k8s.io/api/resource/v1beta2", "resource.k8s.io/v1beta2",
     ["ResourceClaim", "DeviceClass", "ResourceClaimTemplate", "ResourceSlice"]),
    ("k8s.io/api/resource/v1alpha3", "resource.k8s.io/v1alpha3", ["DeviceTaintRule"]),
    ("k8s.io/api/admission/v1", "admission.k8s.io/v1", ["AdmissionReview"]),
    ("k8s.io/api/admission/v1beta1", "admission.k8s.io/v1beta1", ["AdmissionReview"]),
    ("k8s.io/api/apidiscovery/v2", "apidiscovery.k8s.io/v2", ["APIGroupDiscovery", "APIGroupDiscoveryList"]),
    ("k8s.io/api/apidiscovery/v2beta1", "apidiscovery.k8s.io/v2beta1", ["APIGroupDiscovery", "APIGroupDiscoveryList"]),
    ("k8s.io/api/coordination/v1alpha2", "coordination.k8s.io/v1alpha2", ["LeaseCandidate", "LeaseCandidateList"]),
    ("k8s.io/api/coordination/v1beta1", "coordination.k8s.io/v1beta1", ["LeaseCandidate", "LeaseCandidateList"]),
    ("k8s.io/api/imagepolicy/v1alpha1", "imagepolicy.k8s.io/v1alpha1", ["ImageReview"]),
    ("k8s.io/api/apiserverinternal/v1alpha1", "internal.apiserver.k8s.io/v1alpha1",
     ["StorageVersion", "StorageVersionList"]),
    ("k8s.io/api/scheduling/v1alpha1", "scheduling.k8s.io/v1alpha1", ["Workload", "WorkloadList"]),
    ("k8s.io/api/storagemigration/v1beta1", "storagemigration.k8s.io/v1beta1",
     ["StorageVersionMigration", "StorageVersionMigrationList"]),
    ("k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1",
     "apiregistration.k8s.io/v1beta1", ["APIService", "APIServiceList"]),
    ("k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1",
     "apiextensions.k8s.io/v1beta1", ["CustomResourceDefinition"]),
]

PROTO_SCALARS = {
    "string": "String", "bool": "Bool", "bytes": "Bytes", "double": "Double",
    "int32": "Int", "int64": "Int", "uint32": "Int", "uint64": "Int",
    "sint32": "Int", "sint64": "Int", "fixed32": "Int", "fixed64": "Int",
}

# Cross-package types with a special registry FieldType.
SPECIAL = {
    ".k8s.io.apimachinery.pkg.api.resource.Quantity": "Quantity",
    ".k8s.io.apimachinery.pkg.util.intstr.IntOrString": "IntOrString",
    ".k8s.io.apimachinery.pkg.runtime.RawExtension": "JsonRaw",
}


def find_src():
    env = os.environ.get("K8S_SRC")
    cands = [env] if env else []
    d = ROOT
    while True:
        cands.append(os.path.join(d, "kubernetes"))
        nd = os.path.dirname(d)
        if nd == d:
            break
        d = nd
    for c in cands:
        if c and os.path.isdir(os.path.join(c, "staging/src/k8s.io/api")):
            return os.path.join(c, "staging/src")
    sys.exit("kubernetes checkout not found; set K8S_SRC")


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


FIELD = re.compile(
    r"^\s*(?:(optional|repeated)\s+)?"
    r"(map<\s*(\w+)\s*,\s*([\w.]+)\s*>|[\w.]+)\s+(\w+)\s*=\s*(\d+)\s*;", re.M)


def parse_messages(text):
    """Return {name: [(number, name, label, type, map_key, map_val)]}.

    generated.proto of k8s has no nested message definitions, so a flat scan of
    `message X { ... }` blocks is sufficient.
    """
    text = strip_comments(text)
    out = {}
    for m in re.finditer(r"^message (\w+) \{(.*?)^\}", text, re.S | re.M):
        fields = []
        for f in FIELD.finditer(m.group(2)):
            label, typ, mk, mv, name, num = f.groups()
            fields.append((int(num), name, label, typ, mk, mv))
        out[m.group(1)] = fields
    return out


def parse_go_fields(pkg_dir):
    """Scan the package's `types*.go` struct tags.

    Returns ({(struct, protobuf name)} of `json:",inline"` embeds,
             {(struct, protobuf name): json name}).
    The registry names a field by its JSON key, which differs from the proto
    field name for some (`$ref`, `x-kubernetes-embedded-resource`, ...).
    """
    inline, jsonname = set(), {}
    for path in sorted(glob.glob(os.path.join(pkg_dir, "types*.go"))):
        if path.endswith("_test.go"):
            continue
        cur = None
        for line in open(path).read().splitlines():
            m = re.match(r"^type (\w+) struct \{", line)
            if m:
                cur = m.group(1)
                continue
            if line.startswith("}"):
                cur = None
            if not cur:
                continue
            js = re.search(r'json:"([^",]*)', line)
            pb = re.search(r'protobuf:"[^"]*name=(\w+)', line)
            if not (js and pb):
                continue
            if 'json:",inline"' in line:
                inline.add((cur, pb.group(1)))
            elif js.group(1) and js.group(1) != "-":
                jsonname[(cur, pb.group(1))] = js.group(1)
    return inline, jsonname


def rust_type(typ, mk, mv, pkg, local, refs):
    """Rust expression for a field's FieldType, or raise."""
    if typ.startswith("map<"):
        if mk != "string":
            raise SystemExit(f"non-string map key {typ}")
        if mv == "string":
            return "T::StringMap"
        if mv == "bytes":
            return "T::BytesMap"
        if mv == "ExtraValue":
            # `type ExtraValue []string`: map<string, ExtraValue{items}> on the wire.
            return "T::StringListMap"
        if mv == ".k8s.io.apimachinery.pkg.api.resource.Quantity":
            return "T::QuantityMap"
        if mv in local:
            refs.add(mv)
            return f'T::MessageMap("{pkg}.{mv}".into())'
        raise SystemExit(f"unsupported map value {mv}")
    return None


def scalar_or_msg(typ, pkg, local, refs, ext):
    if typ in PROTO_SCALARS:
        return "T::" + PROTO_SCALARS[typ]
    if typ in SPECIAL:
        return "T::" + SPECIAL[typ]
    if typ == "JSON":
        # apiextensions `JSON { optional bytes raw = 1 }`: the raw JSON body.
        return "T::JsonRaw"
    if typ in local:
        refs.add(typ)
        return f'T::Message("{pkg}.{typ}".into())'
    # Cross-package message: referenced by its bare registry name.
    bare = typ.rsplit(".", 1)[-1]
    ext.add(bare)
    return f'T::Message("{bare}".into())'


def main():
    src = find_src()
    out = []
    ext = set()
    for pdir, key, roots in PACKAGES:
        proto = open(os.path.join(src, pdir, "generated.proto")).read()
        msgs = parse_messages(proto)
        inline, jsonname = parse_go_fields(os.path.join(src, pdir))
        want, todo = [], list(roots)
        while todo:
            n = todo.pop()
            if n in want:
                continue
            if n not in msgs:
                raise SystemExit(f"{pdir}: no message {n}")
            want.append(n)
            for _, _, _, typ, mk, mv in msgs[n]:
                for t in (typ, mv):
                    if t in msgs and t not in want and t not in ("JSON", "ExtraValue"):
                        todo.append(t)
        out.append(f"    // ===== {key} ({pdir}/generated.proto) =====")
        for n in sorted(want):
            rows = []
            for num, fname, label, typ, mk, mv in msgs[n]:
                refs = set()
                t = rust_type(typ, mk, mv, key, msgs, refs)
                if t is None:
                    t = scalar_or_msg(typ, key, msgs, refs, ext)
                    if (n, fname) in inline and t.startswith("T::Message("):
                        t = t.replace("T::Message(", "T::InlineMessage(", 1)
                    if label == "repeated":
                        t = f"T::Repeated(Box::new({t}))"
                jname = jsonname.get((n, fname), fname)
                rows.append(f'            ({num}, "{jname}", {t}),')
            out.append(f'    schemas.insert(\n        "{key}.{n}".into(),\n        m(vec![\n'
                       + "\n".join(rows) + "\n        ]),\n    );")
    body = "\n".join(out)
    header = f"""// @generated by scripts/gen-protobuf-versioned-schemas.py -- DO NOT EDIT.
// Source: k8s.io/api/<group>/<version>/{{generated.proto,types.go}} at kubernetes
// release-1.35. Regenerate with: scripts/gen-protobuf-versioned-schemas.py
//
// Group-qualified schemas (`<group>/<version>.<Message>`; core: `<version>.<Message>`)
// for API versions whose wire layout differs from the GA shape the hand-written
// registry keeps under the bare kind, and for kinds with no bare schema.
// Messages of other packages are referenced by bare registry name.

use super::{{FieldType as T, MessageSchema}};
use std::collections::HashMap;

fn m(fields: Vec<(u32, &str, T)>) -> MessageSchema {{
    MessageSchema {{
        fields: fields
            .into_iter()
            .map(|(n, name, t)| (n, (name.to_string(), t)))
            .collect(),
    }}
}}

/// Bare registry names this file references but does not define (checked by
/// the `external_references_resolve` test).
#[cfg(test)]
pub(crate) const EXTERNAL_REFS: &[&str] = {sorted_list(ext)};

#[rustfmt::skip]
pub(crate) fn register(schemas: &mut HashMap<String, MessageSchema>) {{
"""
    footer = """}

#[cfg(test)]
mod tests {
    use super::EXTERNAL_REFS;
    use crate::PROTO_REGISTRY;

    #[test]
    fn external_references_resolve() {
        let have: std::collections::HashSet<&str> =
            PROTO_REGISTRY.iter_schemas().map(|(k, _)| k).collect();
        for r in EXTERNAL_REFS {
            assert!(have.contains(r), "versioned schema references unregistered {r}");
        }
    }
}
"""
    text = header + body + "\n" + footer
    if "--stdout" in sys.argv:
        print(text)
    else:
        open(OUT, "w").write(text)


def sorted_list(s):
    return "&[" + ", ".join(f'"{x}"' for x in sorted(s)) + "]"


main()
