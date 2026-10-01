//! Port of `pkg/registry/rbac/validation/policy_compact.go` and
//! `CompactString` (`pkg/apis/rbac/v1/evaluation_helpers.go:111-132`).

use rusternetes_common::resources::PolicyRule;

/// `simpleResource`: the key rules are compacted by.
#[derive(PartialEq, Eq)]
struct SimpleResource {
    group: String,
    resource: String,
    resource_name: Option<String>,
}

/// `isSimpleResourceRule` (policy_compact.go:60-85): verbs, a single resource,
/// a single API group, at most one resource name, and nothing else.
fn is_simple_resource_rule(rule: &PolicyRule) -> Option<SimpleResource> {
    let names = rule.resource_names.as_deref().unwrap_or_default();
    let urls = rule.non_resource_urls.as_deref().unwrap_or_default();
    if names.len() > 1 || !urls.is_empty() {
        return None;
    }
    let groups = rule.api_groups.as_deref().unwrap_or_default();
    let resources = rule.resources.as_deref().unwrap_or_default();
    if groups.len() != 1 || resources.len() != 1 {
        return None;
    }
    Some(SimpleResource {
        group: groups[0].clone(),
        resource: resources[0].clone(),
        resource_name: names.first().cloned(),
    })
}

/// `CompactRules` (policy_compact.go:34-58): combine rules that contain a
/// single APIGroup/Resource, differ only by verb, and contain no other
/// attributes.
pub fn compact_rules(rules: &[PolicyRule]) -> Vec<PolicyRule> {
    let mut compacted = Vec::with_capacity(rules.len());
    let mut simple_rules: Vec<(SimpleResource, PolicyRule)> = Vec::new();
    for rule in rules {
        match is_simple_resource_rule(rule) {
            Some(resource) => {
                if let Some((_, existing)) = simple_rules.iter_mut().find(|(r, _)| *r == resource) {
                    // Add the new verbs to the existing simple resource rule.
                    existing.verbs.extend(rule.verbs.iter().cloned());
                } else {
                    simple_rules.push((resource, rule.clone()));
                }
            }
            None => compacted.push(rule.clone()),
        }
    }
    // Once the simple resource rules are consolidated, add them to the list.
    compacted.extend(simple_rules.into_iter().map(|(_, rule)| rule));
    compacted
}

/// Go's `%q` of a `[]string`: `["a" "b"]`.
pub fn quote_strings(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|s| format!("{s:?}")).collect();
    format!("[{}]", quoted.join(" "))
}

/// `CompactString` (evaluation_helpers.go:111-132): a compact representation
/// of a rule for escalation error messages.
pub fn compact_string(rule: &PolicyRule) -> String {
    let mut parts = Vec::new();
    let mut push = |label: &str, items: &[String]| {
        if !items.is_empty() {
            parts.push(format!("{label}:{}", quote_strings(items)));
        }
    };
    push("APIGroups", rule.api_groups.as_deref().unwrap_or_default());
    push("Resources", rule.resources.as_deref().unwrap_or_default());
    push(
        "NonResourceURLs",
        rule.non_resource_urls.as_deref().unwrap_or_default(),
    );
    push(
        "ResourceNames",
        rule.resource_names.as_deref().unwrap_or_default(),
    );
    push("Verbs", &rule.verbs);
    format!("{{{}}}", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(verbs: &[&str], groups: &[&str], resources: &[&str], names: &[&str]) -> PolicyRule {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        PolicyRule {
            verbs: v(verbs),
            api_groups: Some(v(groups)),
            resources: Some(v(resources)),
            resource_names: (!names.is_empty()).then(|| v(names)),
            non_resource_urls: None,
        }
    }

    /// TestCompactRules (policy_compact_test.go:29): rules that differ only by
    /// verb merge; anything else is left alone.
    #[test]
    fn rules_differing_only_by_verb_merge() {
        let out = compact_rules(&[
            rule(&["get"], &[""], &["pods"], &[]),
            rule(&["list"], &[""], &["pods"], &[]),
            rule(&["get"], &[""], &["pods"], &["a"]),
            rule(&["get"], &["", "apps"], &["pods"], &[]),
        ]);
        assert_eq!(out.len(), 3);
        assert!(out.contains(&rule(&["get", "list"], &[""], &["pods"], &[])));
        assert!(out.contains(&rule(&["get"], &[""], &["pods"], &["a"])));
        assert!(out.contains(&rule(&["get"], &["", "apps"], &["pods"], &[])));
    }

    #[test]
    fn compact_string_skips_empty_parts() {
        assert_eq!(
            compact_string(&rule(&["get", "list"], &[""], &["pods"], &[])),
            "{APIGroups:[\"\"], Resources:[\"pods\"], Verbs:[\"get\" \"list\"]}"
        );
        let url = PolicyRule {
            verbs: vec!["get".into()],
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: Some(vec!["/x".into()]),
        };
        assert_eq!(
            compact_string(&url),
            "{NonResourceURLs:[\"/x\"], Verbs:[\"get\"]}"
        );
    }
}
