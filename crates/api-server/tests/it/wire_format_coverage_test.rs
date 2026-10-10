//! Wire-format coverage report (WARNING-ONLY).
//!
//! Two reports, neither of which fails the build for a *gap*:
//!
//! 1. **Upstream proto parity** over every `generated.proto` bundled under
//!    `proto/upstream/v1.35/` that is NOT already a hard gate in
//!    `protobuf_schema_parity_upstream.rs` (`PROTO_FILES`): upstream messages
//!    with no `ProtoRegistry` entry, and registered messages whose field
//!    number / name / type differs from upstream.
//! 2. **Served-resource walk**: every resource found through live `/api` +
//!    `/apis` discovery of the real router is checked for (a) registry entry
//!    for its Kind and List kind, (b) a protobuf create + protobuf-Accept GET
//!    round trip, (c) an OpenAPI v2 definition and an OpenAPI v3 component.
//!
//! Gaps are collected as [`Finding`]s and rendered by [`render_outputs`]
//! (stderr, plus `report.json`, `summary.md`, `annotations.txt` under
//! `$WIRE_FORMAT_REPORT_DIR` when set; `scripts/wire-format-coverage.sh` and
//! the CI step consume those). Only harness errors (discovery unreachable,
//! unparseable proto, ...) panic.
//!
//! Upstream references (release-1.35, verified):
//! * `staging/src/k8s.io/apimachinery/pkg/api/apitesting/roundtrip/roundtrip.go`
//!   `RoundTripTypes` (line 62) walks `scheme.AllKnownTypes()` rather than a
//!   hand-written list; `RoundTripProtobufTestOnlyTypes`/`roundTripToAllExternalVersions`
//!   (~lines 76-130) do the protobuf leg per group/version. Here the "scheme"
//!   is live discovery.
//! * `test/integration/apiserver/openapi/openapi_test.go` `TestOpenAPIV3SpecRoundTrip`
//!   (line 56) enumerates `/openapi/v3` paths and fetches each document, which is
//!   the shape of the v3 leg below.
//! * `test/integration/apiserver/apply/apply_crd_test.go`-style GVK matching via
//!   the `x-kubernetes-group-version-kind` extension is what kube-openapi emits
//!   for every definition (`pkg/builder/openapi.go`), so definitions are located
//!   by that extension first, falling back to the `<version>.<Kind>` name suffix.

use super::protobuf_schema_parity_upstream::{
    build_logical, collect_map_entries, compare_types, intentional_field_skip, map_value_type,
    LogicalType, MapEntryNames, UpstreamField, PROTO_FILES,
};
use prost_types::{DescriptorProto, FileDescriptorProto};
use rusternetes_api_server::protobuf::{FieldType, ProtoRegistry};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const PROTO_CT: &str = "application/vnd.kubernetes.protobuf";

// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Status {
    Warn,
    NotChecked,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Warn => "warn",
            Status::NotChecked => "not_checked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Finding {
    /// Check id: `proto-missing-message`, `proto-field-mismatch`,
    /// `registry-kind`, `registry-list`, `protobuf-roundtrip`,
    /// `openapi-v2`, `openapi-v3`.
    check: &'static str,
    /// `group/version` ("v1" for core), the grouping key of the report.
    gv: String,
    /// Kind / message / resource the finding is about.
    subject: String,
    status: Status,
    detail: String,
}

#[derive(Debug, Default)]
struct Totals {
    upstream_files: usize,
    upstream_messages: usize,
    resources_checked: usize,
    kinds_checked: usize,
}

// ---------------------------------------------------------------------------
// Part 1: upstream proto parity (warning-only, files outside the hard gate)
// ---------------------------------------------------------------------------

/// Registry-side view: key -> field number -> (name, type).
type RegistryView = BTreeMap<String, BTreeMap<u32, (String, FieldType)>>;

fn registry_view(registry: &ProtoRegistry) -> RegistryView {
    registry
        .iter_schemas()
        .map(|(k, s)| {
            (
                k.to_string(),
                s.fields.iter().map(|(n, v)| (*n, v.clone())).collect(),
            )
        })
        .collect()
}

/// The `apiVersion` an upstream `generated.proto` path serves, or `None` for
/// the shared apimachinery helper packages (whose messages the registry keeps
/// under bare names and which are covered by the hard gate).
fn api_version_for_proto_path(rel: &str) -> Option<String> {
    let parts: Vec<&str> = rel.split('/').collect();
    // k8s.io/api/<group>/<version>/generated.proto
    if parts.len() == 5 && parts[1] == "api" {
        let (g, v) = (parts[2], parts[3]);
        return Some(match g {
            "core" => v.to_string(),
            "apps" | "batch" | "autoscaling" | "policy" | "extensions" => format!("{g}/{v}"),
            "rbac" => format!("rbac.authorization.k8s.io/{v}"),
            "flowcontrol" => format!("flowcontrol.apiserver.k8s.io/{v}"),
            "apiserverinternal" => format!("internal.apiserver.k8s.io/{v}"),
            other => format!("{other}.k8s.io/{v}"),
        });
    }
    // k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/<v>/generated.proto
    // k8s.io/kube-aggregator/pkg/apis/apiregistration/<v>/generated.proto
    // k8s.io/metrics/pkg/apis/<g>/<v>/generated.proto
    // k8s.io/apiserver/pkg/apis/audit/<v>/generated.proto
    if parts.len() == 7 && parts[2] == "pkg" && parts[3] == "apis" {
        let (g, v) = (parts[4], parts[5]);
        if parts[1] == "apimachinery" {
            return None;
        }
        return Some(match g {
            "custom_metrics" => format!("custom.metrics.k8s.io/{v}"),
            "external_metrics" => format!("external.metrics.k8s.io/{v}"),
            other => format!("{other}.k8s.io/{v}"),
        });
    }
    None
}

type FileIndex = BTreeMap<String, BTreeMap<u32, UpstreamField>>;

/// Per-file message index with `map<>` collapse (the shared helper in the
/// parity test is keyed by bare simple name, which collides across groups).
fn index_file(file: &FileDescriptorProto, map_entries: &MapEntryNames) -> FileIndex {
    fn walk(msg: &DescriptorProto, map_entries: &MapEntryNames, out: &mut FileIndex) {
        if !map_entries.contains(msg.name()) {
            let local: BTreeMap<String, &DescriptorProto> = msg
                .nested_type
                .iter()
                .filter(|n| map_entries.contains(n.name()))
                .map(|n| (n.name().to_string(), n))
                .collect();
            let mut fields = BTreeMap::new();
            for f in &msg.field {
                let mut logical = build_logical(f, map_entries);
                if let LogicalType::Repeated(inner) = &logical {
                    if let LogicalType::Message(name) = inner.as_ref() {
                        if let Some(entry) = local.get(name) {
                            logical =
                                LogicalType::Map(Box::new(map_value_type(entry, map_entries)));
                        }
                    }
                }
                fields.insert(
                    f.number() as u32,
                    UpstreamField {
                        name: f.name().to_string(),
                        logical,
                    },
                );
            }
            out.insert(msg.name().to_string(), fields);
        }
        for n in &msg.nested_type {
            walk(n, map_entries, out);
        }
    }
    let mut out = FileIndex::new();
    for m in &file.message_type {
        walk(m, map_entries, &mut out);
    }
    out
}

/// Pure checker: compare one parsed file against the registry. Returns
/// findings (never panics on a gap). `messages` receives the message count.
fn proto_parity_findings(
    file: &FileDescriptorProto,
    api_version: &str,
    registry: &RegistryView,
    map_entries: &MapEntryNames,
    messages: &mut usize,
) -> Vec<Finding> {
    let mut out = Vec::new();
    for (msg, upstream_fields) in index_file(file, map_entries) {
        *messages += 1;
        let qualified = format!("{api_version}.{msg}");
        // Decode falls back from the qualified to the bare key
        // (`decode_k8s_resource`), so a bare entry counts as registered.
        let (key, ours) = match registry
            .get_key_value(&qualified)
            .or_else(|| registry.get_key_value(&msg))
        {
            Some(kv) => kv,
            None => {
                out.push(Finding {
                    check: "proto-missing-message",
                    gv: api_version.to_string(),
                    subject: msg.clone(),
                    status: Status::Warn,
                    detail: format!("upstream message {msg} has no registry entry"),
                });
                continue;
            }
        };
        for (num, (our_name, our_type)) in ours {
            if intentional_field_skip(&msg, *num) {
                continue;
            }
            let problem = match upstream_fields.get(num) {
                None => Some(format!(
                    "registry '{our_name}' at #{num} but upstream has no such field"
                )),
                Some(up) if up.name != *our_name => Some(format!(
                    "#{num} name ours='{our_name}' upstream='{}'",
                    up.name
                )),
                Some(up) => compare_types(our_type, &up.logical)
                    .map(|r| format!("#{num} '{our_name}': {r}")),
            };
            if let Some(p) = problem {
                out.push(Finding {
                    check: "proto-field-mismatch",
                    gv: api_version.to_string(),
                    subject: msg.clone(),
                    status: Status::Warn,
                    detail: format!("{p} (registry key {key})"),
                });
            }
        }
        // Fields upstream has that the registry silently drops.
        for (num, up) in &upstream_fields {
            if !ours.contains_key(num) && !intentional_field_skip(&msg, *num) {
                out.push(Finding {
                    check: "proto-field-mismatch",
                    gv: api_version.to_string(),
                    subject: msg.clone(),
                    status: Status::Warn,
                    detail: format!(
                        "upstream field #{num} '{}' absent from registry key {key}",
                        up.name
                    ),
                });
            }
        }
    }
    out
}

fn upstream_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto/upstream/v1.35")
}

fn all_bundled_protos() -> Vec<String> {
    fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).expect("read upstream proto dir") {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                walk(&p, base, out);
            } else if p.file_name().is_some_and(|n| n == "generated.proto") {
                out.push(
                    p.strip_prefix(base)
                        .expect("under base")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(&upstream_root(), &upstream_root(), &mut out);
    out.sort();
    out
}

fn upstream_proto_findings(registry: &RegistryView, totals: &mut Totals) -> Vec<Finding> {
    let hard: BTreeSet<&str> = PROTO_FILES.iter().copied().collect();
    let mut parsed = Vec::new();
    for rel in all_bundled_protos() {
        let src = std::fs::read_to_string(upstream_root().join(&rel))
            .unwrap_or_else(|e| panic!("read {rel}: {e}"));
        let file = protox_parse::parse(&rel, &src)
            .unwrap_or_else(|e| panic!("harness error: cannot parse {rel}: {e}"));
        parsed.push(file);
    }
    let map_entries = collect_map_entries(&parsed);
    let mut out = Vec::new();
    for file in &parsed {
        if hard.contains(file.name()) {
            continue; // already a hard gate
        }
        let Some(av) = api_version_for_proto_path(file.name()) else {
            continue;
        };
        totals.upstream_files += 1;
        out.extend(proto_parity_findings(
            file,
            &av,
            registry,
            &map_entries,
            &mut totals.upstream_messages,
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Part 2: per-resource checks (pure helpers)
// ---------------------------------------------------------------------------

/// Registry hit for `kind` served at `gv` (qualified key, else bare key —
/// the same fallback `decode_k8s_resource` applies).
fn registry_has(keys: &BTreeSet<String>, gv: &str, kind: &str) -> bool {
    keys.contains(&format!("{gv}.{kind}")) || keys.contains(kind)
}

/// Does an OpenAPI definitions/schemas map define `kind` at `group`/`version`?
/// Matches the `x-kubernetes-group-version-kind` extension first, then the
/// `io.k8s.*.<version>.<Kind>` name convention.
fn openapi_defines(
    defs: &serde_json::Map<String, Value>,
    group: &str,
    version: &str,
    kind: &str,
) -> bool {
    let suffix = format!(".{version}.{kind}");
    defs.iter().any(|(name, schema)| {
        let by_ext = schema
            .get("x-kubernetes-group-version-kind")
            .and_then(|v| v.as_array())
            .is_some_and(|a| {
                a.iter().any(|e| {
                    e.get("group").and_then(|v| v.as_str()).unwrap_or("") == group
                        && e.get("version").and_then(|v| v.as_str()) == Some(version)
                        && e.get("kind").and_then(|v| v.as_str()) == Some(kind)
                })
            });
        by_ext || name.ends_with(&suffix)
    })
}

/// `expected` is a subset of `actual` (objects recursively, arrays by index,
/// scalars equal). Returns the JSON path of the first loss.
fn json_loss(expected: &Value, actual: &Value, path: &str) -> Option<String> {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => e.iter().find_map(|(k, ev)| match a.get(k) {
            None => Some(format!("{path}.{k}")),
            Some(av) => json_loss(ev, av, &format!("{path}.{k}")),
        }),
        (Value::Array(e), Value::Array(a)) => {
            if a.len() < e.len() {
                return Some(format!("{path}[len]"));
            }
            e.iter()
                .zip(a)
                .enumerate()
                .find_map(|(i, (ev, av))| json_loss(ev, av, &format!("{path}[{i}]")))
        }
        (e, a) if e == a => None,
        // numbers 1 vs 1.0, etc.
        (Value::Number(e), Value::Number(a)) if e.as_f64() == a.as_f64() => None,
        _ => Some(path.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn check_title(check: &str) -> &'static str {
    match check {
        "proto-missing-message" => "Upstream proto message has no registry entry",
        "proto-field-mismatch" => "Registered proto message differs from upstream",
        "registry-kind" => "Served kind has no protobuf registry entry",
        "registry-list" => "Served List kind has no protobuf registry entry",
        "protobuf-roundtrip" => "Protobuf create/GET round trip failed",
        "openapi-v2" => "Kind missing from OpenAPI v2",
        "openapi-v3" => "Kind missing from OpenAPI v3",
        _ => "wire-format coverage",
    }
}

fn render_outputs(findings: &[Finding], totals: &Totals) -> (String, String, Vec<String>) {
    let warns: Vec<&Finding> = findings
        .iter()
        .filter(|f| f.status == Status::Warn)
        .collect();
    let nc: Vec<&Finding> = findings
        .iter()
        .filter(|f| f.status == Status::NotChecked)
        .collect();

    // Markdown: one table per check, rows grouped by group/version.
    let mut md = String::new();
    md.push_str("## Wire-format coverage (warning-only)\n\n");
    md.push_str(&format!(
        "Upstream proto files checked: **{}** ({} messages) | resources walked: **{}** | kinds: **{}** | warnings: **{}** | not checked: **{}**\n\n",
        totals.upstream_files,
        totals.upstream_messages,
        totals.resources_checked,
        totals.kinds_checked,
        warns.len(),
        nc.len()
    ));
    let mut checks: BTreeSet<&str> = BTreeSet::new();
    for f in findings {
        checks.insert(f.check);
    }
    for check in &checks {
        for status in [Status::Warn, Status::NotChecked] {
            let rows: Vec<&Finding> = findings
                .iter()
                .filter(|f| f.check == *check && f.status == status)
                .collect();
            if rows.is_empty() {
                continue;
            }
            md.push_str(&format!(
                "<details><summary><b>{}</b> ({}): {}</summary>\n\n| group/version | subject | detail |\n|---|---|---|\n",
                check_title(check),
                status.as_str(),
                rows.len()
            ));
            for r in rows {
                md.push_str(&format!(
                    "| `{}` | `{}` | {} |\n",
                    r.gv,
                    r.subject,
                    r.detail.replace('|', "\\|").replace('\n', " ")
                ));
            }
            md.push_str("\n</details>\n\n");
        }
    }

    // Annotations: GitHub caps at ~10 per step, so one per check, with
    // per-group counts; the full list is in the step summary.
    let mut ann = Vec::new();
    for check in &checks {
        let rows: Vec<&Finding> = warns
            .iter()
            .copied()
            .filter(|f| f.check == *check)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let mut per_gv: BTreeMap<&str, usize> = BTreeMap::new();
        for r in &rows {
            *per_gv.entry(r.gv.as_str()).or_default() += 1;
        }
        let groups = per_gv
            .iter()
            .map(|(g, n)| format!("{g}={n}"))
            .collect::<Vec<_>>()
            .join(", ");
        ann.push(format!(
            "::warning title={} ({})::{} finding(s) across {} group/version(s): {}. Full list in the job summary (Wire-format coverage).",
            check_title(check),
            check,
            rows.len(),
            per_gv.len(),
            groups
        ));
    }
    (
        serde_json::to_string_pretty(&findings_json(findings, totals)).unwrap(),
        md,
        ann,
    )
}

fn findings_json(findings: &[Finding], totals: &Totals) -> Value {
    json!({
        "totals": {
            "upstream_files": totals.upstream_files,
            "upstream_messages": totals.upstream_messages,
            "resources_checked": totals.resources_checked,
            "kinds_checked": totals.kinds_checked,
            "warnings": findings.iter().filter(|f| f.status == Status::Warn).count(),
            "not_checked": findings.iter().filter(|f| f.status == Status::NotChecked).count(),
        },
        "findings": findings.iter().map(|f| json!({
            "check": f.check, "groupVersion": f.gv, "subject": f.subject,
            "status": f.status.as_str(), "detail": f.detail,
        })).collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------
// Unit tests of the checkers against SYNTHETIC gaps (red/green proof that the
// report really detects what it claims to).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod checker_tests {
    use super::*;

    fn parse(src: &str) -> (FileDescriptorProto, MapEntryNames) {
        let f = protox_parse::parse("k8s.io/api/foo/v1/generated.proto", src).unwrap();
        let me = collect_map_entries(std::slice::from_ref(&f));
        (f, me)
    }

    const SRC: &str = r#"
syntax = "proto2";
package k8s.io.api.foo.v1;
message Widget { optional string name = 1; optional int32 size = 2; map<string,string> labels = 3; }
message WidgetList { repeated Widget items = 2; }
"#;

    #[test]
    fn missing_message_and_field_drift_are_detected() {
        let (f, me) = parse(SRC);
        let mut reg = RegistryView::new();
        // Widget registered with a wrong name at #1, wrong type at #2, and
        // #3 (labels) absent; WidgetList deliberately NOT registered.
        reg.insert(
            "foo.k8s.io/v1.Widget".into(),
            BTreeMap::from([
                (1, ("title".to_string(), FieldType::String)),
                (2, ("size".to_string(), FieldType::String)),
            ]),
        );
        let mut n = 0;
        let out = proto_parity_findings(&f, "foo.k8s.io/v1", &reg, &me, &mut n);
        assert_eq!(n, 2);
        assert!(out
            .iter()
            .any(|f| f.check == "proto-missing-message" && f.subject == "WidgetList"));
        let d: Vec<&str> = out
            .iter()
            .filter(|f| f.check == "proto-field-mismatch")
            .map(|f| f.detail.as_str())
            .collect();
        assert!(d.iter().any(|d| d.contains("name ours='title'")), "{d:?}");
        assert!(d.iter().any(|d| d.contains("'size'")), "{d:?}");
        assert!(d.iter().any(|d| d.contains("#3 'labels' absent")), "{d:?}");
    }

    #[test]
    fn matching_registry_yields_no_findings() {
        let (f, me) = parse(SRC);
        let mut reg = RegistryView::new();
        reg.insert(
            "foo.k8s.io/v1.Widget".into(),
            BTreeMap::from([
                (1, ("name".to_string(), FieldType::String)),
                (2, ("size".to_string(), FieldType::Int)),
                (3, ("labels".to_string(), FieldType::StringMap)),
            ]),
        );
        // Bare-key fallback counts as registered for the list.
        reg.insert(
            "WidgetList".into(),
            BTreeMap::from([(
                2,
                (
                    "items".to_string(),
                    FieldType::Repeated(Box::new(FieldType::Message("Widget".into()))),
                ),
            )]),
        );
        let mut n = 0;
        let out = proto_parity_findings(&f, "foo.k8s.io/v1", &reg, &me, &mut n);
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn registry_lookup_prefers_qualified_then_bare() {
        let keys: BTreeSet<String> = ["Pod".into(), "node.k8s.io/v1.RuntimeClass".into()].into();
        assert!(registry_has(&keys, "v1", "Pod"));
        assert!(registry_has(&keys, "node.k8s.io/v1", "RuntimeClass"));
        assert!(!registry_has(&keys, "node.k8s.io/v1", "RuntimeClassList"));
    }

    #[test]
    fn openapi_matching_by_extension_and_name() {
        let defs = json!({
            "io.k8s.api.foo.v1.Widget": {},
            "x.Other": {"x-kubernetes-group-version-kind":[{"group":"g","version":"v2","kind":"Other"}]},
        });
        let defs = defs.as_object().unwrap();
        assert!(openapi_defines(defs, "foo.k8s.io", "v1", "Widget"));
        assert!(openapi_defines(defs, "g", "v2", "Other"));
        assert!(!openapi_defines(defs, "foo.k8s.io", "v1", "Gadget"));
        assert!(!openapi_defines(defs, "g", "v1", "Other"));
    }

    #[test]
    fn json_loss_finds_dropped_spec_field() {
        let e = json!({"spec":{"a":1,"b":{"c":"x"}}});
        assert_eq!(json_loss(&e, &e, ""), None);
        let a = json!({"spec":{"a":1,"b":{}}});
        assert_eq!(json_loss(&e, &a, "").as_deref(), Some(".spec.b.c"));
    }

    #[test]
    fn proto_path_maps_to_api_version() {
        let m = api_version_for_proto_path;
        assert_eq!(
            m("k8s.io/api/core/v1/generated.proto").as_deref(),
            Some("v1")
        );
        assert_eq!(
            m("k8s.io/api/flowcontrol/v1beta3/generated.proto").as_deref(),
            Some("flowcontrol.apiserver.k8s.io/v1beta3")
        );
        assert_eq!(
            m("k8s.io/api/rbac/v1alpha1/generated.proto").as_deref(),
            Some("rbac.authorization.k8s.io/v1alpha1")
        );
        assert_eq!(
            m("k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto"),
            None
        );
    }
}

// ---------------------------------------------------------------------------
// Live walk of the real router
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Served {
    group: String,
    version: String,
    /// `group/version`, or `v1` for core.
    gv: String,
    name: String,
    kind: String,
    namespaced: bool,
    verbs: Vec<String>,
}

impl Served {
    fn is_sub(&self) -> bool {
        self.name.contains('/')
    }
    fn base(&self) -> String {
        if self.group.is_empty() {
            format!("/api/{}", self.version)
        } else {
            format!("/apis/{}/{}", self.group, self.version)
        }
    }
    fn has(&self, verb: &str) -> bool {
        self.verbs.iter().any(|v| v == verb)
    }
}

async fn discover(api: &TestApiServer) -> Vec<Served> {
    let mut gvs: Vec<(String, String)> = Vec::new();
    let (st, body) = api.get("/api").await;
    assert_eq!(st, 200, "harness error: GET /api unreachable");
    for v in body["versions"].as_array().expect("/api versions") {
        gvs.push((String::new(), v.as_str().unwrap().to_string()));
    }
    let (st, body) = api.get("/apis").await;
    assert_eq!(st, 200, "harness error: GET /apis unreachable");
    for g in body["groups"].as_array().expect("/apis groups") {
        for v in g["versions"].as_array().into_iter().flatten() {
            gvs.push((
                g["name"].as_str().unwrap().to_string(),
                v["version"].as_str().unwrap().to_string(),
            ));
        }
    }
    let mut out = Vec::new();
    for (group, version) in gvs {
        let uri = if group.is_empty() {
            format!("/api/{version}")
        } else {
            format!("/apis/{group}/{version}")
        };
        let (st, body) = api.get(&uri).await;
        assert_eq!(st, 200, "harness error: GET {uri} unreachable");
        let gv = if group.is_empty() {
            version.clone()
        } else {
            format!("{group}/{version}")
        };
        for r in body["resources"].as_array().into_iter().flatten() {
            out.push(Served {
                group: group.clone(),
                version: version.clone(),
                gv: gv.clone(),
                name: r["name"].as_str().unwrap_or_default().to_string(),
                kind: r["kind"].as_str().unwrap_or_default().to_string(),
                namespaced: r["namespaced"].as_bool().unwrap_or(false),
                verbs: r["verbs"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
            });
        }
    }
    out
}

/// Minimal-object seeds for kinds whose validation needs more than metadata.
/// Keyed by Kind; merged under the generated apiVersion/kind/metadata. Kinds
/// not listed here and rejected by validation are reported `not_checked`.
fn spec_seed(kind: &str) -> Option<Value> {
    let tmpl = json!({"metadata":{"labels":{"a":"b"}},"spec":{"containers":[{"name":"c","image":"busybox"}]}});
    Some(match kind {
        "Pod" => json!({"spec":{"containers":[{"name":"c","image":"busybox"}]}}),
        "Service" => json!({"spec":{"ports":[{"port":80,"protocol":"TCP"}],"selector":{"a":"b"}}}),
        "Deployment" | "ReplicaSet" | "StatefulSet" => json!({"spec":{
            "selector":{"matchLabels":{"a":"b"}},
            "template": tmpl}}),
        "DaemonSet" => json!({"spec":{"selector":{"matchLabels":{"a":"b"}},"template":tmpl}}),
        "Job" => json!({"spec":{"template":{"metadata":{"labels":{"a":"b"}},
            "spec":{"restartPolicy":"Never","containers":[{"name":"c","image":"busybox"}]}}}}),
        "PersistentVolumeClaim" => json!({"spec":{"accessModes":["ReadWriteOnce"],
            "resources":{"requests":{"storage":"1Gi"}}}}),
        "StorageClass" => json!({"provisioner":"example.com/p"}),
        "PriorityClass" => json!({"value":100}),
        "ConfigMap" => json!({"data":{"k":"v"}}),
        "Secret" => json!({"stringData":{"k":"v"}}),
        _ => return None,
    })
}

fn minimal_object(s: &Served, name: &str) -> Value {
    let mut obj = json!({
        "apiVersion": s.gv,
        "kind": s.kind,
        "metadata": {"name": name},
    });
    if s.namespaced {
        obj["metadata"]["namespace"] = json!("wfc-ns");
    }
    if let Some(Value::Object(seed)) = spec_seed(&s.kind) {
        for (k, v) in seed {
            obj[k] = v;
        }
    }
    obj
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

fn ld_field(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_varint(buf, ((field as u64) << 3) | 2);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

/// `k8s\0` + runtime.Unknown{typeMeta, raw, contentType}.
fn k8s_envelope(api_version: &str, kind: &str, raw: &[u8]) -> Vec<u8> {
    let mut type_meta = Vec::new();
    ld_field(&mut type_meta, 1, api_version.as_bytes());
    ld_field(&mut type_meta, 2, kind.as_bytes());
    let mut unknown = Vec::new();
    ld_field(&mut unknown, 1, &type_meta);
    ld_field(&mut unknown, 2, raw);
    ld_field(&mut unknown, 4, PROTO_CT.as_bytes());
    let mut out = b"k8s\0".to_vec();
    out.extend_from_slice(&unknown);
    out
}

/// Registry key used to encode `kind` at `gv`: qualified if present, else bare.
fn registry_key(keys: &BTreeSet<String>, gv: &str, kind: &str) -> Option<String> {
    let q = format!("{gv}.{kind}");
    if keys.contains(&q) {
        Some(q)
    } else if keys.contains(kind) {
        Some(kind.to_string())
    } else {
        None
    }
}

/// Outcome of the protobuf round trip for one top-level resource.
enum RoundTrip {
    Ok,
    NotChecked(String),
    Warn(String),
}

async fn proto_roundtrip(
    api: &TestApiServer,
    registry: &ProtoRegistry,
    keys: &BTreeSet<String>,
    s: &Served,
) -> RoundTrip {
    if !s.has("create") {
        return RoundTrip::NotChecked("no create verb".into());
    }
    if s.kind.ends_with("Review") || s.kind == "Binding" || s.kind == "Eviction" {
        return RoundTrip::NotChecked("create-only/virtual kind with side effects".into());
    }
    let Some(key) = registry_key(keys, &s.gv, &s.kind) else {
        return RoundTrip::NotChecked("no registry entry (reported by registry-kind)".into());
    };
    let coll = if s.namespaced {
        format!("{}/namespaces/wfc-ns/{}", s.base(), s.name)
    } else {
        format!("{}/{}", s.base(), s.name)
    };

    // 1. Can a minimal object be built generically at all? Prove with JSON.
    let json_obj = minimal_object(s, "wfc-json");
    let (st, body) = api.post(&coll, &json_obj).await;
    if !st.is_success() {
        let msg = body["message"].as_str().unwrap_or("").to_string();
        return RoundTrip::NotChecked(format!(
            "minimal object rejected by JSON create ({st}): {}",
            msg.chars().take(120).collect::<String>()
        ));
    }

    // 2. protobuf create.
    let obj = minimal_object(s, "wfc-proto");
    let Some(raw) = registry.encode_message(&key, &obj) else {
        return RoundTrip::Warn(format!("registry key {key} cannot encode the object"));
    };
    let (st, _h, bytes, _) = api
        .send_with_headers(
            "POST",
            &coll,
            &[("content-type", PROTO_CT), ("accept", "application/json")],
            Some(k8s_envelope(&s.gv, &s.kind, &raw)),
        )
        .await;
    if !st.is_success() {
        return RoundTrip::Warn(format!(
            "protobuf create -> {st}: {}",
            String::from_utf8_lossy(&bytes)
                .chars()
                .take(160)
                .collect::<String>()
        ));
    }
    if !s.has("get") {
        return RoundTrip::Ok;
    }

    // 3. protobuf GET.
    let (st, headers, bytes, _) = api
        .send_with_headers(
            "GET",
            &format!("{coll}/wfc-proto"),
            &[("accept", PROTO_CT)],
            None,
        )
        .await;
    if !st.is_success() {
        return RoundTrip::Warn(format!("GET after protobuf create -> {st}"));
    }
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct.contains("protobuf") || !bytes.starts_with(b"k8s\0") {
        return RoundTrip::Warn(format!(
            "Accept: {PROTO_CT} answered with content-type '{ct}' (not protobuf)"
        ));
    }
    let Some(decoded) = registry.decode_k8s_resource(&bytes) else {
        return RoundTrip::Warn("protobuf GET response not decodable by the registry".into());
    };
    let decoded: Value = serde_json::from_slice(&decoded).unwrap_or(Value::Null);
    if decoded["metadata"]["name"] != "wfc-proto" {
        return RoundTrip::Warn("protobuf GET lost metadata.name".into());
    }
    let mut expected = obj.clone();
    if let Some(m) = expected.as_object_mut() {
        m.remove("metadata");
    }
    if let Some(p) = json_loss(&expected, &decoded, "") {
        return RoundTrip::Warn(format!("field lost through protobuf round trip: {p}"));
    }
    RoundTrip::Ok
}

async fn openapi_v2_defs(api: &TestApiServer) -> serde_json::Map<String, Value> {
    let (st, body) = api.get("/openapi/v2").await;
    assert_eq!(st, 200, "harness error: GET /openapi/v2 unreachable");
    body["definitions"].as_object().cloned().unwrap_or_default()
}

/// `group/version` -> components.schemas of its v3 document.
async fn openapi_v3_docs(api: &TestApiServer) -> BTreeMap<String, serde_json::Map<String, Value>> {
    let (st, index) = api.get("/openapi/v3").await;
    assert_eq!(st, 200, "harness error: GET /openapi/v3 unreachable");
    let mut out = BTreeMap::new();
    for (path, entry) in index["paths"].as_object().into_iter().flatten() {
        let Some(url) = entry["serverRelativeURL"].as_str() else {
            continue;
        };
        let (st, doc) = api.get(url).await;
        if !st.is_success() {
            continue;
        }
        // "api/v1" -> "v1"; "apis/apps/v1" -> "apps/v1".
        let gv = path
            .strip_prefix("apis/")
            .or_else(|| path.strip_prefix("api/"))
            .unwrap_or(path)
            .to_string();
        out.insert(
            gv,
            doc["components"]["schemas"]
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
    }
    out
}

async fn resource_findings(totals: &mut Totals) -> Vec<Finding> {
    let api = TestApiServer::new();
    let registry = ProtoRegistry::new();
    let keys: BTreeSet<String> = registry
        .iter_schemas()
        .map(|(k, _)| k.to_string())
        .collect();
    let served = discover(&api).await;
    let v2 = openapi_v2_defs(&api).await;
    let v3 = openapi_v3_docs(&api).await;

    let mut out = Vec::new();
    let mut seen_kinds: BTreeSet<(String, String)> = BTreeSet::new();
    for s in &served {
        totals.resources_checked += 1;
        if s.kind.is_empty() {
            continue;
        }
        // Each (group/version, kind) is checked once; a subresource that
        // returns the parent kind adds nothing, the rest (Scale, Eviction,
        // TokenRequest, ...) are new kinds.
        if seen_kinds.insert((s.gv.clone(), s.kind.clone())) {
            totals.kinds_checked += 1;
            let f = |check: &'static str, detail: String| Finding {
                check,
                gv: s.gv.clone(),
                subject: s.kind.clone(),
                status: Status::Warn,
                detail,
            };
            if !registry_has(&keys, &s.gv, &s.kind) {
                out.push(f(
                    "registry-kind",
                    format!("no registry entry for {}", s.kind),
                ));
            }
            let list = format!("{}List", s.kind);
            if s.has("list") && !s.is_sub() && !registry_has(&keys, &s.gv, &list) {
                out.push(f("registry-list", format!("no registry entry for {list}")));
            }
            if !openapi_defines(&v2, &s.group, &s.version, &s.kind) {
                out.push(f(
                    "openapi-v2",
                    "no definition in /openapi/v2 (by x-kubernetes-group-version-kind or name)"
                        .into(),
                ));
            }
            match v3.get(&s.gv) {
                None => out.push(f(
                    "openapi-v3",
                    format!("no /openapi/v3 document for {}", s.gv),
                )),
                Some(schemas) if !openapi_defines(schemas, &s.group, &s.version, &s.kind) => out
                    .push(f(
                        "openapi-v3",
                        "no component schema in the group/version document".into(),
                    )),
                _ => {}
            }
        }
        if s.is_sub() {
            continue;
        }
        let (status, detail) = match proto_roundtrip(&api, &registry, &keys, s).await {
            RoundTrip::Ok => continue,
            RoundTrip::NotChecked(why) => (Status::NotChecked, why),
            RoundTrip::Warn(why) => (Status::Warn, why),
        };
        out.push(Finding {
            check: "protobuf-roundtrip",
            gv: s.gv.clone(),
            subject: s.name.clone(),
            status,
            detail,
        });
    }
    out
}

// ---------------------------------------------------------------------------
// The report test: never fails on a gap.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wire_format_coverage_report() {
    let registry = ProtoRegistry::new();
    let view = registry_view(&registry);
    let mut totals = Totals::default();
    let mut findings = upstream_proto_findings(&view, &mut totals);
    findings.extend(resource_findings(&mut totals).await);
    findings.sort();
    findings.dedup();

    let (json, md, ann) = render_outputs(&findings, &totals);
    let warns = findings.iter().filter(|f| f.status == Status::Warn).count();
    eprintln!(
        "wire-format coverage: {} upstream files / {} messages, {} resources / {} kinds, {} warnings, {} not checked",
        totals.upstream_files,
        totals.upstream_messages,
        totals.resources_checked,
        totals.kinds_checked,
        warns,
        findings.len() - warns
    );
    for f in findings.iter().filter(|f| f.status == Status::Warn) {
        eprintln!("WARN [{}] {} {}: {}", f.check, f.gv, f.subject, f.detail);
    }
    if let Ok(dir) = std::env::var("WIRE_FORMAT_REPORT_DIR") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).expect("create report dir");
        std::fs::write(dir.join("report.json"), json).expect("write report.json");
        std::fs::write(dir.join("summary.md"), md).expect("write summary.md");
        std::fs::write(dir.join("annotations.txt"), ann.join("\n") + "\n")
            .expect("write annotations.txt");
    }
}
