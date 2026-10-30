//! Unit tests moved from `src/registry/rbac/policy_comparator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::PolicyRule;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::rbac::policy_comparator::*;

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
