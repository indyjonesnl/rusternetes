//! The shadow type check of a ValidatingAdmissionPolicy against the embedded
//! upstream OpenAPI types (`plugin/policy/validating/typechecking.go`).

use rusternetes_api_server::registry::admissionregistration::vap_typechecking::{
    typed_divergences, types_to_check, MAX_TYPES_TO_CHECK,
};
use rusternetes_common::cel_openapi_resolver::Gvk;
use rusternetes_common::cel_versions::{self, EnvSet};
use rusternetes_common::resources::ValidatingAdmissionPolicy;
use serde_json::{json, Value};

fn policy(spec: Value) -> ValidatingAdmissionPolicy {
    serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "p"},
        "spec": spec,
    }))
    .unwrap()
}

fn rule(groups: &[&str], versions: &[&str], resources: &[&str]) -> Value {
    json!({"apiGroups": groups, "apiVersions": versions, "resources": resources,
           "operations": ["CREATE"]})
}

fn deployments(extra: Value) -> ValidatingAdmissionPolicy {
    let mut spec = json!({
        "matchConstraints": {"resourceRules": [rule(&["apps"], &["v1"], &["deployments"])]},
        "validations": [{"expression": "true"}],
    });
    for (k, v) in extra.as_object().unwrap() {
        spec[k] = v.clone();
    }
    policy(spec)
}

fn set() -> EnvSet {
    EnvSet::must_base_env_set(cel_versions::default_compatibility_version())
}

/// The fields of the typed disagreements, for one expression.
fn fields(p: &ValidatingAdmissionPolicy) -> Vec<(String, Vec<String>)> {
    typed_divergences(p, &set())
        .into_iter()
        .map(|d| (d.field, d.checker))
        .collect()
}

#[test]
fn types_follow_the_resource_rules() {
    let p = deployments(json!({}));
    assert_eq!(
        types_to_check(p.spec.as_ref().unwrap()),
        [Gvk::new("apps", "v1", "Deployment")]
    );
    let p = policy(json!({"matchConstraints": {"resourceRules": [
        rule(&[""], &["v1"], &["pods", "pods/status", "configmaps"]),
        rule(&["apps"], &["*"], &["deployments"]),
        rule(&["apps"], &["v1"], &["*"]),
    ]}}));
    // Wildcards and subresources are skipped (`extractGroups` /
    // `extractVersions` give up, `extractResources` skips them); sorted.
    assert_eq!(
        types_to_check(p.spec.as_ref().unwrap()),
        [Gvk::new("", "v1", "ConfigMap"), Gvk::new("", "v1", "Pod")]
    );
    assert!(types_to_check(policy(json!({})).spec.as_ref().unwrap()).is_empty());
    assert_eq!(MAX_TYPES_TO_CHECK, 10);
}

#[test]
fn a_type_error_is_found_where_the_untyped_probe_accepts() {
    let p = deployments(json!({"validations": [
        {"expression": "object.spec.replicas > 0"},
        {"expression": "object.spec.replicasX > 0"},
        {"expression": "object.spec.replicas == 'a'"},
    ]}));
    let d = fields(&p);
    assert_eq!(d.len(), 2, "{d:?}");
    assert_eq!(d[0].0, "spec.validations[1].expression");
    assert_eq!(d[0].1, ["undefined field 'replicasX'"]);
    assert_eq!(d[1].0, "spec.validations[2].expression");
}

#[test]
fn old_object_request_namespace_and_authorizer_are_declared() {
    let p = deployments(json!({"validations": [
        {"expression": "oldObject.metadata.name == object.metadata.name"},
        {"expression": "request.userInfo.username != '' && request.operation == 'CREATE'"},
        {"expression": "request.kind.kind == 'Deployment' && request.options == null"},
        {"expression": "namespaceObject.metadata.labels['a'] == 'b'"},
        {"expression": "authorizer.group('apps').resource('deployments').check('get').allowed()"},
        {"expression": "object.metadata.labels['a'] == 'b'"},
    ]}));
    assert_eq!(fields(&p), []);
    // `messageExpression` has no authorizer; `request.uid` is omitted upstream.
    let p = deployments(json!({"validations": [{
        "expression": "true",
        "messageExpression": "string(authorizer.group('apps').resource('x').check('get').allowed())",
    }, {"expression": "request.uid == ''"}]}));
    let d = fields(&p);
    assert_eq!(d.len(), 2, "{d:?}");
    assert_eq!(d[0].0, "spec.validations[0].messageExpression");
    assert_eq!(d[1].1, ["undefined field 'uid'"]);
}

#[test]
fn variables_are_typed_by_their_expressions() {
    let p = deployments(json!({
        "variables": [
            {"name": "replicas", "expression": "object.spec.replicas"},
            {"name": "names", "expression": "object.spec.template.spec.containers.map(c, c.name)"},
            {"name": "broken", "expression": "object.nope"},
        ],
        "validations": [
            {"expression": "variables.replicas > 1 && variables.names.size() > 0"},
            {"expression": "variables.replicas == 'x'"},
            {"expression": "variables.broken.anything == 1"},
            {"expression": "variables.missing == 1"},
        ],
    }));
    let d = fields(&p);
    // `variables.broken` does not compile (`dyn`), the rest is as typed.
    let found: Vec<&str> = d.iter().map(|(f, _)| f.as_str()).collect();
    assert_eq!(
        found,
        [
            "spec.validations[1].expression",
            "spec.validations[3].expression",
            "spec.variables[2].expression"
        ],
        "{d:?}"
    );
    assert_eq!(d[1].1, ["undefined field 'missing'"]);
}

#[test]
fn params_are_typed_by_the_param_kind_or_dyn() {
    let p = deployments(json!({
        "paramKind": {"apiVersion": "v1", "kind": "ConfigMap"},
        "validations": [
            {"expression": "params.data['a'] == object.metadata.name"},
            {"expression": "params.nope == 1"},
        ],
    }));
    let d = fields(&p);
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!(d[0].1, ["undefined field 'nope'"]);
    // A kind with no schema (a custom resource) leaves `params` `dyn`.
    let p = deployments(json!({
        "paramKind": {"apiVersion": "example.com/v1", "kind": "Limits"},
        "validations": [{"expression": "params.anything.at.all == 1"}],
    }));
    assert_eq!(fields(&p), []);
    // No paramKind: `params` is undeclared.
    let p = deployments(json!({"validations": [{"expression": "params.a == 1"}]}));
    assert_eq!(fields(&p).len(), 1);
}

#[test]
fn match_conditions_and_audit_annotations_are_checked() {
    let p = deployments(json!({
        "matchConditions": [{"name": "a", "expression": "object.spec.nope == 1"}],
        "auditAnnotations": [{"key": "k", "valueExpression": "object.metadata.nope"}],
    }));
    let d = fields(&p);
    assert_eq!(
        d.iter().map(|(f, _)| f.as_str()).collect::<Vec<_>>(),
        [
            "spec.matchConditions[0].expression",
            "spec.auditAnnotations[0].valueExpression"
        ]
    );
}

#[test]
fn unresolvable_kinds_are_skipped() {
    // A custom resource has no schema in the resolver: no checks at all.
    let p = policy(json!({
        "matchConstraints": {"resourceRules": [rule(&["example.com"], &["v1"], &["widgets"])]},
        "validations": [{"expression": "object.spec.nope == 1"}],
    }));
    assert_eq!(fields(&p), []);
}

/// The resolver's data is `v2.definitions.json` (vendored by
/// `scripts/sync-upstream-openapi.sh`): every definition in it must be,
/// verbatim, the one in the vendored release-1.35 `swagger.json`, so a fixture
/// bump without a resync fails here.
#[test]
fn embedded_definitions_match_the_vendored_swagger() {
    let embedded: Value = serde_json::from_str(include_str!(
        "../../openapi/upstream/v1.35/v2.definitions.json"
    ))
    .unwrap();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/upstream/k8s-1.35/openapi-spec/swagger.json");
    let swagger: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let vendored = swagger["definitions"].as_object().unwrap();
    let embedded = embedded.as_object().unwrap();
    assert!(embedded.len() > 100);
    for (name, def) in embedded {
        assert_eq!(
            vendored.get(name),
            Some(def),
            "{name} differs from the vendored swagger.json; run scripts/sync-upstream-openapi.sh"
        );
    }
}
