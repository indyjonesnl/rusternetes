//! The static cost of CRD rules against the cel-go / apiextensions oracle.
//!
//! `tools/cel-oracle` answers a `schema` request with what
//! `apiextensions-apiserver/pkg/apiserver/schema/cel/compilation.go` `Compile`
//! computes for the rule: `CompilationResult.MaxCost` (the library estimator
//! over `model.SchemaDeclType` and the `sizeEstimator`) and `MaxCardinality`
//! (`maxCardinality(declType.MinSerializedSize)`). This test replays the
//! checked-in corpus through the shadow estimator
//! ([`rusternetes_common::cel_checker_shadow::estimate_rule_cost`]),
//! [`schema_decl_type`] and [`max_cardinality`] and compares. It needs no Go:
//! regenerate the goldens with `tools/cel-oracle/gen-goldens.sh`.

use rusternetes_common::cel_checker_shadow::{check_rule_output, estimate_rule_cost, SelfScope};
use rusternetes_common::cel_crd_cost::max_cardinality;
use rusternetes_common::cel_env::ExpectedOutput;
use rusternetes_common::cel_schema_types::schema_decl_type;
use rusternetes_common::cel_versions::{EnvSet, EnvType};
use rusternetes_common::feature_gates::Version;
use rusternetes_common::resources::crd::JSONSchemaProps;
use serde_json::Value;

const GOLDENS: &str = include_str!("../corpus/crd_goldens.jsonl");

/// Requests the Rust side deliberately answers differently, with the reason.
/// Keyed by the request's `expr` + `schema` JSON.
const KNOWN_DIVERGENCES: &[(&str, &str)] = &[];

/// `(ok, maxCost, maxCardinality)`.
type Outcome = (bool, Option<u64>, Option<u64>);

fn ours(req: &Value) -> Outcome {
    let expr = req["expr"].as_str().expect("expr");
    let schema: JSONSchemaProps = serde_json::from_value(req["schema"].clone()).expect("schema");
    let version = req
        .get("kubeVersion")
        .and_then(Value::as_str)
        .unwrap_or("1.35");
    let (major, minor) = version.split_once('.').expect("kubeVersion");
    let set = EnvSet::must_base_env_set(Version::major_minor(
        major.parse().unwrap(),
        minor.parse().unwrap(),
    ));
    let env_type = match req.get("envType").and_then(Value::as_str) {
        Some("stored") => EnvType::StoredExpressions,
        _ => EnvType::NewExpressions,
    };
    let Some(decl) = schema_decl_type(&schema, false) else {
        return (false, None, None);
    };
    let cardinality = max_cardinality(decl.min_serialized_size);
    let scope = SelfScope {
        decl,
        optional_old_self: false,
    };
    // `compileRule` rejects a rule that does not evaluate to a bool before it
    // estimates the cost.
    match check_rule_output(expr, &set, env_type, Some(&scope), ExpectedOutput::Bool) {
        Some(errors) if errors.is_empty() => {}
        _ => return (false, None, None),
    }
    match estimate_rule_cost(expr, &set, env_type, &scope) {
        Some(c) => (true, Some(c.max), Some(cardinality)),
        None => (false, None, None),
    }
}

fn oracle(resp: &Value) -> Outcome {
    let ok = resp["ok"].as_bool().expect("ok");
    (
        ok,
        resp.get("maxCost").and_then(Value::as_u64),
        resp.get("maxCardinality").and_then(Value::as_u64),
    )
}

#[test]
fn crd_rule_costs_match_the_oracle() {
    let mut checked = 0;
    let mut mismatches = Vec::new();
    for line in GOLDENS.lines().filter(|l| !l.trim().is_empty()) {
        let golden: Value = serde_json::from_str(line).unwrap();
        let (req, resp) = (&golden["req"], &golden["resp"]);
        let key = format!("{} @ {}", req["expr"], req["schema"]);
        if KNOWN_DIVERGENCES.iter().any(|(k, _)| *k == key) {
            continue;
        }
        let (want, got) = (oracle(resp), ours(req));
        checked += 1;
        if want != got {
            mismatches.push(format!("{key}\n  oracle: {want:?}\n  ours:   {got:?}"));
        }
    }
    assert!(checked > 100, "corpus unexpectedly small: {checked}");
    assert!(
        mismatches.is_empty(),
        "{} of {checked} CRD rules disagree with the oracle:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
