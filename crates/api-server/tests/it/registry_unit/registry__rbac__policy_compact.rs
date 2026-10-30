//! Unit tests moved from `src/registry/rbac/policy_compact.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::PolicyRule;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::rbac::policy_compact::*;

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
