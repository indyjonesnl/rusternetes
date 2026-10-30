//! Match a request against a policy's `matchConstraints` and a binding's
//! `matchResources`, shared by MutatingAdmissionPolicy (#2731) and, later,
//! ValidatingAdmissionPolicy.
//!
//! Ported from, in `staging/src/k8s.io/apiserver/pkg/admission/plugin/`:
//! - `webhook/predicates/rules/rules.go` (`rules.Matcher`)
//! - `webhook/predicates/namespace/matcher.go` (`MatchNamespaceSelector`)
//! - `webhook/predicates/object/matcher.go` (`MatchObjectSelector`)
//! - `policy/matching/matching.go` (`Matcher.Matches`, `matchesResourceRules`)
//! - `policy/generic/policy_matcher.go` (`DefinitionMatches`, `BindingMatches`)
//!
//! Tests are ported from `policy/matching/matching_test.go` (`TestMatcher`)
//! and `rules/rules_test.go`.

// Not called from the request path yet: the MutatingAdmissionPolicy plugin
// that consumes it is the rest of #2731. The bin target compiles `admission`
// separately and would flag every item.
#![allow(dead_code)]

use std::collections::HashMap;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use rusternetes_common::resources::validating_admission_policy::{
    MatchPolicyType, MatchResources, NamedRuleWithOperations, OperationType, RuleWithOperations,
};
use rusternetes_common::resources::{LabelSelector, LabelSelectorOperator};
use rusternetes_common::types::{
    label_selector_as_selector, LabelSelector as MetaLabelSelector,
    LabelSelectorRequirement as MetaLabelSelectorRequirement, Selector,
};
use rusternetes_common::{Error, Result};
use serde_json::Value;

/// The part of `admission.Attributes` the matchers read.
#[derive(Debug, Clone)]
pub struct Attributes {
    pub kind: GroupVersionKind,
    pub resource: GroupVersionResource,
    pub subresource: String,
    /// `""` for a cluster-scoped object. For a request on a Namespace itself
    /// this is the namespace's own name.
    pub namespace: String,
    pub name: String,
    pub operation: Operation,
    pub object: Option<Value>,
    pub old_object: Option<Value>,
    /// `GetUserInfo`; `None` is upstream's nil `user.Info`.
    pub user_info: Option<rusternetes_common::auth::UserInfo>,
    /// `IsDryRun`.
    pub dry_run: bool,
    /// `GetOperationOptions` (the `CreateOptions`/`UpdateOptions`/
    /// `DeleteOptions` of the request), as JSON.
    pub operation_options: Option<Value>,
}

/// `runtime.EquivalentResourceMapper`.
pub trait EquivalentResourceMapper: Send + Sync {
    /// `EquivalentResourcesFor`: every resource that shares storage with
    /// `resource` for `subresource`, in registration order (the resource
    /// itself included).
    fn equivalent_resources_for(
        &self,
        resource: &GroupVersionResource,
        subresource: &str,
    ) -> Vec<GroupVersionResource>;

    /// `KindFor`; an all-empty kind is upstream's "unknown".
    fn kind_for(&self, resource: &GroupVersionResource, subresource: &str) -> GroupVersionKind;
}

/// `NamespaceLister.Get`; a missing namespace is `Error::NotFound`.
#[async_trait]
pub trait NamespaceLister: Send + Sync {
    async fn namespace_labels(&self, name: &str) -> Result<HashMap<String, String>>;
}

/// What `Matcher::matches` reports for a request that matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub resource: GroupVersionResource,
    pub kind: GroupVersionKind,
}

fn exact_or_wildcard(items: &Option<Vec<String>>, requested: &str) -> bool {
    items
        .iter()
        .flatten()
        .any(|item| item == "*" || item == requested)
}

fn is_namespace_resource(r: &GroupVersionResource) -> bool {
    r.group.is_empty() && r.version == "v1" && r.resource == "namespaces"
}

/// `rules.Matcher.Matches` (rules.go:36-42).
pub fn rule_matches(rule: &RuleWithOperations, attr: &Attributes) -> bool {
    scope_matches(rule, attr)
        && operation_matches(rule, attr)
        && exact_or_wildcard(&rule.api_groups, &attr.resource.group)
        && exact_or_wildcard(&rule.api_versions, &attr.resource.version)
        && resource_matches(rule, attr)
}

/// rules.go:56-71.
pub fn scope_matches(rule: &RuleWithOperations, attr: &Attributes) -> bool {
    match rule.scope.as_deref() {
        None | Some("*") => true,
        // Namespace objects are cluster-scoped, though attr.namespace is set
        // to the namespace's own name for them.
        Some("Namespaced") => !is_namespace_resource(&attr.resource) && !attr.namespace.is_empty(),
        Some("Cluster") => is_namespace_resource(&attr.resource) || attr.namespace.is_empty(),
        Some(_) => false,
    }
}

/// rules.go:85-98. The constants are the same strings, so upstream casts.
fn operation_matches(rule: &RuleWithOperations, attr: &Attributes) -> bool {
    rule.operations.iter().flatten().any(|op| {
        matches!(
            (op, &attr.operation),
            (OperationType::All, _)
                | (OperationType::Create, Operation::Create)
                | (OperationType::Update, Operation::Update)
                | (OperationType::Delete, Operation::Delete)
                | (OperationType::Connect, Operation::Connect)
        )
    })
}

/// rules.go:100-120 (`splitResource` + `resource`).
fn resource_matches(rule: &RuleWithOperations, attr: &Attributes) -> bool {
    rule.resources.iter().flatten().any(|res_sub| {
        let (res, sub) = res_sub.split_once('/').unwrap_or((res_sub, ""));
        (res == "*" || res == attr.resource.resource) && (sub == "*" || sub == attr.subresource)
    })
}

/// `metav1.LabelSelectorAsSelector` over this API group's selector type.
pub fn selector_of(selector: Option<&LabelSelector>) -> std::result::Result<Selector, String> {
    let converted = selector.map(|s| MetaLabelSelector {
        match_labels: s.match_labels.clone(),
        match_expressions: s.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| MetaLabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: match r.operator {
                        LabelSelectorOperator::Unspecified => "",
                        LabelSelectorOperator::In => "In",
                        LabelSelectorOperator::NotIn => "NotIn",
                        LabelSelectorOperator::Exists => "Exists",
                        LabelSelectorOperator::DoesNotExist => "DoesNotExist",
                    }
                    .to_string(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    });
    label_selector_as_selector(converted.as_ref())
}

/// `meta.Accessor(obj).GetLabels()`.
fn object_labels(obj: &Value) -> Option<HashMap<String, String>> {
    Some(
        obj.get("metadata")?
            .get("labels")?
            .as_object()?
            .iter()
            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
            .collect(),
    )
}

const NIL_SELECTOR: &str =
    "a nil {} selector was passed, please ensure selectors are initialized properly";

fn nil_selector(which: &str) -> Error {
    Error::Internal(NIL_SELECTOR.replace("{}", which))
}

pub struct Matcher<'a> {
    pub namespaces: &'a dyn NamespaceLister,
    pub mapper: &'a dyn EquivalentResourceMapper,
}

impl Matcher<'_> {
    /// `namespace.Matcher.GetNamespaceLabels` (namespace/matcher.go:62-93).
    async fn namespace_labels(&self, attr: &Attributes) -> Result<HashMap<String, String>> {
        // A request creating or updating a Namespace reads the labels from the
        // object: the lister does not have the new ones yet. A delete reads the
        // stored namespace, since attr.Object is a DeleteOptions.
        if attr.resource.resource == "namespaces"
            && attr.subresource.is_empty()
            && matches!(attr.operation, Operation::Create | Operation::Update)
        {
            let obj = attr.object.as_ref().ok_or_else(|| {
                Error::Internal("object does not implement the Object interfaces".into())
            })?;
            return Ok(object_labels(obj).unwrap_or_default());
        }
        self.namespaces.namespace_labels(&attr.namespace).await
    }

    /// `namespace.Matcher.MatchNamespaceSelector` (namespace/matcher.go:96-130).
    async fn match_namespace_selector(
        &self,
        selector: Option<&LabelSelector>,
        attr: &Attributes,
    ) -> Result<bool> {
        if attr.namespace.is_empty() && attr.resource.resource != "namespaces" {
            // A cluster-scoped resource other than a namespace is never exempted.
            return Ok(true);
        }
        let selector = selector_of(selector).map_err(Error::Internal)?;
        if selector.is_everything() {
            return Ok(true);
        }
        match self.namespace_labels(attr).await {
            // A missing namespace is passed through as a 404, for backwards
            // compatibility.
            Err(e @ Error::NotFound(_)) => Err(e),
            Err(Error::Internal(m)) => Err(Error::Internal(m)),
            Err(e) => Err(Error::Internal(e.to_string())),
            Ok(labels) => Ok(selector.matches(Some(&labels))),
        }
    }

    /// `object.Matcher.MatchObjectSelector` (object/matcher.go:46-59).
    fn match_object_selector(
        &self,
        selector: Option<&LabelSelector>,
        attr: &Attributes,
    ) -> Result<bool> {
        let selector = selector_of(selector).map_err(Error::Internal)?;
        if selector.is_everything() {
            return Ok(true);
        }
        let matches = |obj: &Option<Value>| match obj.as_ref() {
            None => false,
            Some(o) => selector.matches(object_labels(o).as_ref()),
        };
        Ok(matches(&attr.object) || matches(&attr.old_object))
    }

    /// `matching.matchesResourceRules` (matching.go:112-176).
    fn matches_resource_rules(
        &self,
        named_rules: &[NamedRuleWithOperations],
        match_policy: Option<&MatchPolicyType>,
        attr: &Attributes,
    ) -> Result<Option<Match>> {
        let name_matches = |rule: &NamedRuleWithOperations| match rule.resource_names.as_deref() {
            // An empty name list always matches.
            None | Some([]) => true,
            Some(names) => names.contains(&attr.name),
        };

        for named in named_rules {
            if rule_matches(&named.rule, attr) && name_matches(named) {
                return Ok(Some(Match {
                    resource: attr.resource.clone(),
                    kind: attr.kind.clone(),
                }));
            }
        }

        // An undefined or Exact policy does no fuzzy matching; the API
        // defaults to Equivalent.
        if !matches!(match_policy, Some(MatchPolicyType::Equivalent)) {
            return Ok(None);
        }

        let equivalents = self
            .mapper
            .equivalent_resources_for(&attr.resource, &attr.subresource);
        let mut with_override = attr.clone();
        for named in named_rules {
            for equivalent in &equivalents {
                if *equivalent == attr.resource {
                    // Already checked the original resource.
                    continue;
                }
                with_override.resource = equivalent.clone();
                if !rule_matches(&named.rule, &with_override) {
                    continue;
                }
                let kind = self.mapper.kind_for(equivalent, &attr.subresource);
                if kind.group.is_empty() && kind.version.is_empty() && kind.kind.is_empty() {
                    return Err(Error::Internal(format!(
                        "unable to convert to {}/{}, Resource={}: unknown kind",
                        equivalent.group, equivalent.version, equivalent.resource
                    )));
                }
                if name_matches(named) {
                    return Ok(Some(Match {
                        resource: equivalent.clone(),
                        kind,
                    }));
                }
            }
        }
        Ok(None)
    }

    /// `matching.Matcher.Matches` (matching.go:76-110).
    pub async fn matches(
        &self,
        attr: &Attributes,
        criteria: &MatchResources,
    ) -> Result<Option<Match>> {
        // A selector error is only reported for a request the policy otherwise
        // applies to; one that does not apply to the request is just a miss.
        let ns = self
            .match_namespace_selector(criteria.namespace_selector.as_ref(), attr)
            .await;
        if let Ok(false) = ns {
            return Ok(None);
        }
        let obj = self.match_object_selector(criteria.object_selector.as_ref(), attr);
        if let Ok(false) = obj {
            return Ok(None);
        }

        let policy = criteria.match_policy.as_ref();
        if let Some(excludes) = &criteria.exclude_resource_rules {
            if self
                .matches_resource_rules(excludes, policy, attr)?
                .is_some()
            {
                return Ok(None);
            }
        }

        let matched = match criteria.resource_rules.as_deref() {
            None | Some([]) => Some(Match {
                resource: attr.resource.clone(),
                kind: attr.kind.clone(),
            }),
            Some(rules) => self.matches_resource_rules(rules, policy, attr)?,
        };
        let Some(matched) = matched else {
            return Ok(None);
        };

        // The request applies; now report any selector error.
        ns?;
        obj?;
        Ok(Some(matched))
    }

    /// `generic.matcher.DefinitionMatches` (policy_matcher.go:57-72).
    pub async fn definition_matches(
        &self,
        attr: &Attributes,
        constraints: Option<&MatchResources>,
    ) -> Result<Option<Match>> {
        let constraints = constraints.ok_or_else(|| {
            Error::Internal("policy contained no match constraints, a required field".into())
        })?;
        if constraints.namespace_selector.is_none() {
            return Err(nil_selector("namespace"));
        }
        if constraints.object_selector.is_none() {
            return Err(nil_selector("object"));
        }
        self.matches(attr, constraints).await
    }

    /// `generic.matcher.BindingMatches` (policy_matcher.go:74-88).
    pub async fn binding_matches(
        &self,
        attr: &Attributes,
        match_resources: Option<&MatchResources>,
    ) -> Result<bool> {
        let Some(match_resources) = match_resources else {
            return Ok(true);
        };
        if match_resources.namespace_selector.is_none() {
            return Err(nil_selector("namespace"));
        }
        if match_resources.object_selector.is_none() {
            return Err(nil_selector("object"));
        }
        Ok(self.matches(attr, match_resources).await?.is_some())
    }
}
