//! Port of
//! `staging/src/k8s.io/component-helpers/auth/rbac/validation/policy_comparator.go`:
//! whether one set of `PolicyRule`s covers another.

use rusternetes_common::resources::PolicyRule;

/// `rbacv1.VerbAll` / `rbacv1.APIGroupAll` / `rbacv1.ResourceAll`.
const ALL: &str = "*";

fn groups(rule: &PolicyRule) -> &[String] {
    rule.api_groups.as_deref().unwrap_or_default()
}

fn resources(rule: &PolicyRule) -> &[String] {
    rule.resources.as_deref().unwrap_or_default()
}

fn resource_names(rule: &PolicyRule) -> &[String] {
    rule.resource_names.as_deref().unwrap_or_default()
}

fn non_resource_urls(rule: &PolicyRule) -> &[String] {
    rule.non_resource_urls.as_deref().unwrap_or_default()
}

/// `Covers` (policy_comparator.go:27-56): whether `owner_rules` cover
/// `servant_rules`, and the atomic rules they do not cover.
pub fn covers(owner_rules: &[PolicyRule], servant_rules: &[PolicyRule]) -> (bool, Vec<PolicyRule>) {
    let uncovered: Vec<PolicyRule> = servant_rules
        .iter()
        .flat_map(breakdown_rule)
        .filter(|subrule| !owner_rules.iter().any(|owner| rule_covers(owner, subrule)))
        .collect();
    (uncovered.is_empty(), uncovered)
}

/// `BreakdownRule` (policy_comparator.go:60-84): an equivalent list of rules
/// that each have at most one verb, one resource, and one resource name.
pub fn breakdown_rule(rule: &PolicyRule) -> Vec<PolicyRule> {
    let mut subrules = Vec::new();
    for group in groups(rule) {
        for resource in resources(rule) {
            for verb in &rule.verbs {
                if !resource_names(rule).is_empty() {
                    for name in resource_names(rule) {
                        subrules.push(PolicyRule {
                            verbs: vec![verb.clone()],
                            api_groups: Some(vec![group.clone()]),
                            resources: Some(vec![resource.clone()]),
                            resource_names: Some(vec![name.clone()]),
                            non_resource_urls: None,
                        });
                    }
                } else {
                    subrules.push(PolicyRule {
                        verbs: vec![verb.clone()],
                        api_groups: Some(vec![group.clone()]),
                        resources: Some(vec![resource.clone()]),
                        resource_names: None,
                        non_resource_urls: None,
                    });
                }
            }
        }
    }

    // Non-resource URLs are unique because they only combine with verbs.
    for url in non_resource_urls(rule) {
        for verb in &rule.verbs {
            subrules.push(PolicyRule {
                verbs: vec![verb.clone()],
                api_groups: None,
                resources: None,
                resource_names: None,
                non_resource_urls: Some(vec![url.clone()]),
            });
        }
    }
    subrules
}

fn has(set: &[String], ele: &str) -> bool {
    set.iter().any(|s| s == ele)
}

fn has_all(set: &[String], contains: &[String]) -> bool {
    contains.iter().all(|ele| has(set, ele))
}

/// `resourceCoversAll` (policy_comparator.go:111-134).
fn resource_covers_all(set_resources: &[String], covers_resources: &[String]) -> bool {
    // If we have a star or an exact match on all resources, then we match.
    if has(set_resources, ALL) || has_all(set_resources, covers_resources) {
        return true;
    }
    for path in covers_resources {
        // An exact match matches.
        if has(set_resources, path) {
            continue;
        }
        // A resource that is not a subresource definitely does not match.
        let Some((_, subresource)) = path.split_once('/') else {
            return false;
        };
        if !has(set_resources, &format!("*/{subresource}")) {
            return false;
        }
    }
    true
}

/// `nonResourceURLsCoversAll` (policy_comparator.go:136-151).
fn non_resource_urls_covers_all(set: &[String], covers: &[String]) -> bool {
    covers
        .iter()
        .all(|path| set.iter().any(|owner| non_resource_url_covers(owner, path)))
}

/// `nonResourceURLCovers` (policy_comparator.go:153-158).
fn non_resource_url_covers(owner_path: &str, sub_path: &str) -> bool {
    if owner_path == sub_path {
        return true;
    }
    owner_path.ends_with('*') && sub_path.starts_with(owner_path.trim_end_matches('*'))
}

/// `ruleCovers` (policy_comparator.go:160-178): whether `owner_rule` (any
/// number of verbs, resources and resourceNames) covers `sub_rule` (at most
/// one of each).
pub fn rule_covers(owner_rule: &PolicyRule, sub_rule: &PolicyRule) -> bool {
    let verb_matches = has(&owner_rule.verbs, ALL) || has_all(&owner_rule.verbs, &sub_rule.verbs);
    let group_matches =
        has(groups(owner_rule), ALL) || has_all(groups(owner_rule), groups(sub_rule));
    let resource_matches = resource_covers_all(resources(owner_rule), resources(sub_rule));
    let non_resource_url_matches =
        non_resource_urls_covers_all(non_resource_urls(owner_rule), non_resource_urls(sub_rule));

    let resource_name_matches = if resource_names(sub_rule).is_empty() {
        resource_names(owner_rule).is_empty()
    } else {
        resource_names(owner_rule).is_empty()
            || has_all(resource_names(owner_rule), resource_names(sub_rule))
    };

    verb_matches
        && group_matches
        && resource_matches
        && resource_name_matches
        && non_resource_url_matches
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

    fn url_rule(verbs: &[&str], urls: &[&str]) -> PolicyRule {
        PolicyRule {
            verbs: verbs.iter().map(|x| x.to_string()).collect(),
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: Some(urls.iter().map(|x| x.to_string()).collect()),
        }
    }

    #[test]
    fn wildcards_cover_everything() {
        let owner = [rule(&["*"], &["*"], &["*"], &[])];
        let servant = [rule(
            &["get", "delete"],
            &["", "apps"],
            &["pods", "deployments/scale"],
            &["a"],
        )];
        assert!(covers(&owner, &servant).0);
    }

    #[test]
    fn an_owner_with_resource_names_does_not_cover_all_names() {
        let owner = [rule(&["get"], &[""], &["pods"], &["a"])];
        assert!(covers(&owner, &[rule(&["get"], &[""], &["pods"], &["a"])]).0);
        assert!(!covers(&owner, &[rule(&["get"], &[""], &["pods"], &["b"])]).0);
        assert!(!covers(&owner, &[rule(&["get"], &[""], &["pods"], &[])]).0);
        // An owner without names covers a named servant.
        let owner = [rule(&["get"], &[""], &["pods"], &[])];
        assert!(covers(&owner, &[rule(&["get"], &[""], &["pods"], &["b"])]).0);
    }

    #[test]
    fn a_star_subresource_covers_the_subresource_of_any_resource() {
        let owner = [rule(&["get"], &[""], &["*/log"], &[])];
        assert!(covers(&owner, &[rule(&["get"], &[""], &["pods/log"], &[])]).0);
        assert!(!covers(&owner, &[rule(&["get"], &[""], &["pods/exec"], &[])]).0);
        assert!(!covers(&owner, &[rule(&["get"], &[""], &["pods"], &[])]).0);
    }

    #[test]
    fn non_resource_urls_cover_by_prefix() {
        let owner = [url_rule(&["get"], &["/healthz/*", "/version"])];
        assert!(
            covers(
                &owner,
                &[url_rule(&["get"], &["/healthz/ping", "/version"])]
            )
            .0
        );
        assert!(!covers(&owner, &[url_rule(&["get"], &["/metrics"])]).0);
        assert!(!covers(&owner, &[url_rule(&["post"], &["/version"])]).0);
        // A resource rule never covers a URL rule and vice versa.
        assert!(
            !covers(
                &[rule(&["*"], &["*"], &["*"], &[])],
                &[url_rule(&["get"], &["/x"])]
            )
            .0
        );
    }

    #[test]
    fn the_uncovered_rules_are_atomic() {
        let owner = [rule(&["get"], &[""], &["pods"], &[])];
        let (ok, missing) = covers(&owner, &[rule(&["get", "list"], &[""], &["pods"], &[])]);
        assert!(!ok);
        assert_eq!(missing, vec![rule(&["list"], &[""], &["pods"], &[])]);
    }
}
