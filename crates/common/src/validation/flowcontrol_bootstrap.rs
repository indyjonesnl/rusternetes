//! The mandatory APF bootstrap objects, ported from
//! `staging/src/k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap/default.go`
//! (`MandatoryPriorityLevelConfigurations`, `MandatoryFlowSchemas`; objects at
//! :104-122 and :126-175, constructors `newFlowSchema`/`newPriorityLevelConfiguration`
//! at :424-457, helpers `groups`/`resourceRule`/`nonResourceRule` at :459-529) as
//! consumed by `pkg/apis/flowcontrol/internalbootstrap/default-internal.go`.
//!
//! Only the *mandatory* set is modelled: the validators compare against it
//! (`pkg/apis/flowcontrol/validation/validation.go:86-94`, `:364-390`). The
//! suggested set is never compared and is not ported here.

use crate::resources::flowcontrol::{
    ExemptPriorityLevelConfiguration, FlowDistinguisherMethod, FlowDistinguisherMethodType,
    FlowSchema, FlowSchemaSpec, FlowSchemaSubject, GroupSubject, LimitResponse, LimitResponseType,
    LimitedPriorityLevelConfiguration, NonResourcePolicyRule, PolicyRulesWithSubjects,
    PriorityLevelConfiguration, PriorityLevelConfigurationReference,
    PriorityLevelConfigurationSpec, PriorityLevelType, ResourcePolicyRule, SubjectKind,
};
use crate::types::ObjectMeta;

/// `MandatoryFlowSchemas` keys.
pub const MANDATORY_FLOW_SCHEMA_NAMES: [&str; 2] = ["exempt", "catch-all"];
/// `MandatoryPriorityLevelConfigurations` keys.
pub const MANDATORY_PRIORITY_LEVEL_NAMES: [&str; 2] = ["catch-all", "exempt"];

const AUTO_UPDATE_ANNOTATION_KEY: &str = "apf.kubernetes.io/autoupdate-spec";
const API_VERSION: &str = "flowcontrol.apiserver.k8s.io/v1";

fn meta(name: &str) -> ObjectMeta {
    let mut m = ObjectMeta {
        name: name.to_string(),
        ..Default::default()
    };
    m.annotations = Some(
        [(AUTO_UPDATE_ANNOTATION_KEY.to_string(), "true".to_string())]
            .into_iter()
            .collect(),
    );
    m
}

fn groups(names: &[&str]) -> Vec<FlowSchemaSubject> {
    names
        .iter()
        .map(|n| FlowSchemaSubject {
            kind: SubjectKind::Group,
            user: None,
            group: Some(GroupSubject {
                name: n.to_string(),
            }),
            service_account: None,
        })
        .collect()
}

/// `resourceRule([*], [*], [*], [*], true)` plus `nonResourceRule([*], [*])` —
/// the only rules the mandatory schemas use.
fn match_everything_rules(subjects: Vec<FlowSchemaSubject>) -> PolicyRulesWithSubjects {
    let star = || vec!["*".to_string()];
    PolicyRulesWithSubjects {
        subjects,
        resource_rules: Some(vec![ResourcePolicyRule {
            verbs: star(),
            api_groups: star(),
            resources: star(),
            cluster_scope: Some(true),
            namespaces: Some(star()),
        }]),
        non_resource_rules: Some(vec![NonResourcePolicyRule {
            verbs: star(),
            non_resource_urls: star(),
        }]),
    }
}

fn flow_schema(
    name: &str,
    pl_name: &str,
    matching_precedence: i32,
    dm: Option<FlowDistinguisherMethodType>,
    rule: PolicyRulesWithSubjects,
) -> FlowSchema {
    FlowSchema {
        api_version: API_VERSION.to_string(),
        kind: "FlowSchema".to_string(),
        metadata: meta(name),
        spec: FlowSchemaSpec {
            priority_level_configuration: PriorityLevelConfigurationReference {
                name: pl_name.to_string(),
            },
            matching_precedence,
            distinguisher_method: dm.map(|type_| FlowDistinguisherMethod { type_ }),
            rules: Some(vec![rule]),
        },
        status: None,
    }
}

/// `MandatoryFlowSchemas[name]` (default.go:126-175).
pub fn mandatory_flow_schema(name: &str) -> Option<FlowSchema> {
    match name {
        // `user.SystemPrivilegedGroup`
        "exempt" => Some(flow_schema(
            "exempt",
            "exempt",
            1,
            None,
            match_everything_rules(groups(&["system:masters"])),
        )),
        // `user.AllUnauthenticated`, `user.AllAuthenticated`
        "catch-all" => Some(flow_schema(
            "catch-all",
            "catch-all",
            10000,
            Some(FlowDistinguisherMethodType::ByUser),
            match_everything_rules(groups(&["system:unauthenticated", "system:authenticated"])),
        )),
        _ => None,
    }
}

/// `MandatoryPriorityLevelConfigurations[name]` (default.go:104-122).
pub fn mandatory_priority_level_configuration(name: &str) -> Option<PriorityLevelConfiguration> {
    let spec = match name {
        "exempt" => PriorityLevelConfigurationSpec {
            type_: PriorityLevelType::Exempt,
            limited: None,
            exempt: Some(ExemptPriorityLevelConfiguration {
                nominal_concurrency_shares: Some(0),
                lending_concurrency_limit: None,
                lendable_percent: Some(0),
            }),
        },
        "catch-all" => PriorityLevelConfigurationSpec {
            type_: PriorityLevelType::Limited,
            limited: Some(LimitedPriorityLevelConfiguration {
                nominal_concurrency_shares: Some(5),
                lending_concurrency_limit: None,
                lendable_percent: Some(0),
                borrowing_limit_percent: None,
                limit_response: Some(LimitResponse {
                    type_: LimitResponseType::Reject,
                    queuing: None,
                }),
            }),
            exempt: None,
        },
        _ => return None,
    };
    Some(PriorityLevelConfiguration {
        api_version: API_VERSION.to_string(),
        kind: "PriorityLevelConfiguration".to_string(),
        metadata: meta(name),
        spec,
        status: None,
    })
}

/// `apiequality.Semantic.DeepEqual` for two specs: nil and empty slices/maps
/// compare equal, so null and empty collections are dropped before comparing.
pub fn semantic_equal<T: serde::Serialize>(a: &T, b: &T) -> bool {
    use serde_json::Value;
    fn norm(v: Value) -> Option<Value> {
        match v {
            Value::Null => None,
            Value::Array(a) if a.is_empty() => None,
            Value::Array(a) => Some(Value::Array(a.into_iter().filter_map(norm).collect())),
            Value::Object(o) if o.is_empty() => None,
            Value::Object(o) => Some(Value::Object(
                o.into_iter()
                    .filter_map(|(k, v)| norm(v).map(|v| (k, v)))
                    .collect(),
            )),
            other => Some(other),
        }
    }
    let f = |x: &T| serde_json::to_value(x).ok().and_then(norm);
    f(a) == f(b)
}
