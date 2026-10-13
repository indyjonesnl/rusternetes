//! `field.TooMany(path, actualQuantity, maxQuantity)` carries the actual
//! quantity as the bad value (`staging/src/k8s.io/apimachinery/pkg/util/
//! validation/field/errors.go:282-302`), so the body reads
//! `Too many: 1001: must have at most 1000 items`. #3330 / #3316.

use rusternetes_common::resources::admission_webhook::MatchCondition;
use rusternetes_common::resources::endpointslice::EndpointSlice;
use rusternetes_common::validation::endpointslice::validate_endpoint_slice;
use rusternetes_common::validation::field::Path;
use rusternetes_common::validation::validating_admission_policy::validate_match_conditions;
use serde_json::json;

fn es(v: serde_json::Value) -> EndpointSlice {
    let mut obj = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "es", "namespace": "default"}
    });
    for (k, val) in v.as_object().unwrap() {
        obj[k] = val.clone();
    }
    serde_json::from_value(obj).unwrap()
}

/// `pkg/apis/discovery/validation/validation.go:98`.
#[test]
fn too_many_endpoints_reports_actual_count() {
    let endpoints: Vec<_> = (0..1001)
        .map(|_| json!({"addresses": ["10.0.0.1"]}))
        .collect();
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": endpoints,
        "ports": []
    })));
    let got: Vec<String> = errs.iter().map(|e| e.to_string()).collect();
    assert!(
        got.contains(&"endpoints: Too many: 1001: must have at most 1000 items".to_string()),
        "got: {got:?}"
    );
}

/// `validation.go:109`.
#[test]
fn too_many_addresses_reports_actual_count() {
    let addrs: Vec<String> = (0..101).map(|i| format!("10.0.0.{i}")).collect();
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": addrs}],
        "ports": []
    })));
    let got: Vec<String> = errs.iter().map(|e| e.to_string()).collect();
    assert!(
        got.contains(
            &"endpoints[0].addresses: Too many: 101: must have at most 100 items".to_string()
        ),
        "got: {got:?}"
    );
}

/// `pkg/apis/admissionregistration/validation/validation.go:968`.
#[test]
fn too_many_match_conditions_reports_actual_count() {
    let conds: Vec<MatchCondition> = (0..65)
        .map(|i| MatchCondition {
            name: format!("c-{i}"),
            expression: "true".into(),
        })
        .collect();
    let errs = validate_match_conditions(&conds, &Path::new("matchConditions"));
    let got: Vec<String> = errs.iter().map(|e| e.to_string()).collect();
    assert!(
        got.contains(&"matchConditions: Too many: 65: must have at most 64 items".to_string()),
        "got: {got:?}"
    );
}
