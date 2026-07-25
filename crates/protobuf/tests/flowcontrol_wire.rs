//! Wire-level regression tests for flowcontrol.apiserver.k8s.io/v1 protobuf
//! field numbers. A symmetric rusternetes encode↔decode cannot catch a wrong
//! tag (it is consistent with itself), so these feed hand-built payloads with
//! the UPSTREAM field numbers (k8s.io/api/flowcontrol/v1/generated.proto) and
//! assert they decode.
//!
//! Regression: `NonResourcePolicyRule.nonResourceURLs` is upstream field **6**
//! (not sequential 2). Decoding at 2 dropped every nonResourceURL a protobuf
//! client (client-go e2e) sent, so the api-server's validation rejected the
//! conformance FlowSchema template with "spec.rules[0].nonResourceRules[0]
//! .nonResourceURLs: Required value: nonResourceURLs must contain at least one
//! value" — failing `[sig-api-machinery] API priority and fairness should
//! support FlowSchema API operations`.
use rusternetes_protobuf::ProtoRegistry;
use serde_json::json;

/// FlowSchema.spec(2) → rules(4) → nonResourceRules(3) →
/// { verbs(1)="*", nonResourceURLs(6)="*" }, all hand-encoded with upstream
/// tags.
#[test]
fn decodes_non_resource_urls_at_upstream_tag_6() {
    // NonResourcePolicyRule: verbs="*" (tag1), nonResourceURLs="*" (tag6)
    let rule: &[u8] = &[0x0A, 0x01, 0x2A, 0x32, 0x01, 0x2A];
    // PolicyRulesWithSubjects: nonResourceRules (tag3, LEN)
    let mut prws = vec![0x1A, rule.len() as u8];
    prws.extend_from_slice(rule);
    // FlowSchemaSpec: rules (tag4, LEN)
    let mut spec = vec![0x22, prws.len() as u8];
    spec.extend_from_slice(&prws);
    // FlowSchema: spec (tag2, LEN)
    let mut fs = vec![0x12, spec.len() as u8];
    fs.extend_from_slice(&spec);

    let d = ProtoRegistry::new()
        .decode_message("FlowSchema", &fs)
        .expect("decode wire bytes");
    assert_eq!(
        d.pointer("/spec/rules/0/nonResourceRules/0/nonResourceURLs/0"),
        Some(&json!("*")),
        "nonResourceURLs must decode from upstream protobuf tag 6, got: {d}"
    );
    assert_eq!(
        d.pointer("/spec/rules/0/nonResourceRules/0/verbs/0"),
        Some(&json!("*")),
    );
}

/// PriorityLevelConfiguration.spec(2) → limited(2) → lendablePercent(3)=50.
/// The schema previously mapped field 3 to a nonexistent JSON key
/// (`lendingConcurrencyLimit`), hiding the value from `lendablePercent`.
#[test]
fn decodes_lendable_percent_at_upstream_tag_3() {
    // LimitedPriorityLevelConfiguration: lendablePercent (tag3, varint 50)
    let limited: &[u8] = &[0x18, 0x32];
    // PriorityLevelConfigurationSpec: limited (tag2, LEN)
    let mut spec = vec![0x12, limited.len() as u8];
    spec.extend_from_slice(limited);
    // PriorityLevelConfiguration: spec (tag2, LEN)
    let mut plc = vec![0x12, spec.len() as u8];
    plc.extend_from_slice(&spec);

    let d = ProtoRegistry::new()
        .decode_message("PriorityLevelConfiguration", &plc)
        .expect("decode wire bytes");
    assert_eq!(
        d.pointer("/spec/limited/lendablePercent"),
        Some(&json!(50)),
        "lendablePercent must decode from upstream protobuf tag 3, got: {d}"
    );
}

/// Full FlowSchema round-trip through our own encoder must preserve
/// nonResourceURLs (the response the e2e client decodes).
#[test]
fn flowschema_roundtrips_non_resource_rules() {
    let r = ProtoRegistry::new();
    let fs = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "FlowSchema",
        "metadata": {"name": "e2e-example-fs"},
        "spec": {
            "matchingPrecedence": 10000,
            "priorityLevelConfiguration": {"name": "global-default"},
            "distinguisherMethod": {"type": "ByUser"},
            "rules": [{
                "subjects": [{"kind": "User", "user": {"name": "example-e2e-non-existent-user"}}],
                "nonResourceRules": [{"verbs": ["*"], "nonResourceURLs": ["*"]}]
            }]
        }
    });
    let bytes = r
        .encode_message("FlowSchema", &fs)
        .expect("FlowSchema must encode");
    let d = r
        .decode_message("FlowSchema", &bytes)
        .expect("FlowSchema must decode");
    assert_eq!(
        d.pointer("/spec/rules/0/nonResourceRules/0/nonResourceURLs/0"),
        Some(&json!("*")),
        "nonResourceURLs must survive the round-trip, got: {d}"
    );
}
