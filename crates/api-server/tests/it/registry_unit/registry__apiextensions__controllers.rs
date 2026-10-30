//! Unit tests moved from `src/registry/apiextensions/controllers.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{
        CustomResourceDefinition, CustomResourceDefinitionCondition, CustomResourceDefinitionNames,
        CustomResourceDefinitionStatus,
    };
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::controllers::*;

    fn crd(name: &str) -> CustomResourceDefinition {
        let (plural, group) = name.split_once('.').unwrap();
        let mut crd = CustomResourceDefinition::new("x", group, "", plural);
        crd.metadata.name = name.to_string();
        crd.spec.names = CustomResourceDefinitionNames {
            plural: plural.to_string(),
            ..Default::default()
        };
        crd
    }

    fn spec_names(
        mut c: CustomResourceDefinition,
        plural: &str,
        singular: &str,
        kind: &str,
        list_kind: &str,
        short: &[&str],
    ) -> CustomResourceDefinition {
        c.spec.names = names(plural, singular, kind, list_kind, short);
        c
    }

    fn status_names(
        mut c: CustomResourceDefinition,
        plural: &str,
        singular: &str,
        kind: &str,
        list_kind: &str,
        short: &[&str],
    ) -> CustomResourceDefinition {
        c.status.get_or_insert_with(Default::default).accepted_names =
            Some(names(plural, singular, kind, list_kind, short));
        c
    }

    fn names(
        plural: &str,
        singular: &str,
        kind: &str,
        list_kind: &str,
        short: &[&str],
    ) -> CustomResourceDefinitionNames {
        CustomResourceDefinitionNames {
            plural: plural.to_string(),
            singular: Some(singular.to_string()),
            kind: kind.to_string(),
            list_kind: Some(list_kind.to_string()),
            short_names: Some(short.iter().map(|s| s.to_string()).collect()),
            categories: None,
        }
    }

    struct Case {
        name: &'static str,
        input: CustomResourceDefinition,
        existing: Vec<CustomResourceDefinition>,
        want_names: CustomResourceDefinitionNames,
        want_reason: &'static str,
        want_message: &'static str,
        want_established: (&'static str, &'static str),
    }

    /// `TestSync` (status/naming_controller_test.go:136-): each name that
    /// conflicts with another CRD's accepted names is withheld from
    /// `acceptedNames`, and the CRD is not accepted.
    #[test]
    fn calculate_names_and_conditions_matches_upstream_test_sync() {
        let full = |c| {
            spec_names(
                c,
                "alfa",
                "delta-singular",
                "echo-kind",
                "foxtrot-listkind",
                &["golf-shortname-1", "hotel-shortname-2"],
            )
        };
        let cases = vec![
            Case {
                name: "first resource",
                input: crd("alfa.bravo.com"),
                existing: vec![],
                want_names: names("alfa", "", "", "", &[]),
                want_reason: "NoConflicts",
                want_message: "no conflicts found",
                want_established: ("Installing", "the initial names have been accepted"),
            },
            Case {
                name: "different groups",
                input: full(crd("alfa.bravo.com")),
                existing: vec![status_names(
                    crd("alfa.charlie.com"),
                    "alfa",
                    "delta-singular",
                    "echo-kind",
                    "foxtrot-listkind",
                    &["golf-shortname-1", "hotel-shortname-2"],
                )],
                want_names: names(
                    "alfa",
                    "delta-singular",
                    "echo-kind",
                    "foxtrot-listkind",
                    &["golf-shortname-1", "hotel-shortname-2"],
                ),
                want_reason: "NoConflicts",
                want_message: "no conflicts found",
                want_established: ("Installing", "the initial names have been accepted"),
            },
            Case {
                name: "conflict plural to singular",
                input: full(crd("alfa.bravo.com")),
                existing: vec![status_names(
                    crd("india.bravo.com"),
                    "india",
                    "alfa",
                    "",
                    "",
                    &[],
                )],
                want_names: names(
                    "",
                    "delta-singular",
                    "echo-kind",
                    "foxtrot-listkind",
                    &["golf-shortname-1", "hotel-shortname-2"],
                ),
                want_reason: "PluralConflict",
                want_message: "\"alfa\" is already in use",
                want_established: ("NotAccepted", "not all names are accepted"),
            },
            Case {
                name: "conflict singular to shortName",
                input: full(crd("alfa.bravo.com")),
                existing: vec![status_names(
                    crd("india.bravo.com"),
                    "india",
                    "indias",
                    "",
                    "",
                    &["delta-singular"],
                )],
                want_names: names(
                    "alfa",
                    "",
                    "echo-kind",
                    "foxtrot-listkind",
                    &["golf-shortname-1", "hotel-shortname-2"],
                ),
                want_reason: "SingularConflict",
                want_message: "\"delta-singular\" is already in use",
                want_established: ("NotAccepted", "not all names are accepted"),
            },
            Case {
                name: "conflict on shortName to shortName",
                input: full(crd("alfa.bravo.com")),
                existing: vec![status_names(
                    crd("india.bravo.com"),
                    "india",
                    "indias",
                    "",
                    "",
                    &["hotel-shortname-2"],
                )],
                want_names: names(
                    "alfa",
                    "delta-singular",
                    "echo-kind",
                    "foxtrot-listkind",
                    &[],
                ),
                want_reason: "ShortNamesConflict",
                want_message: "\"hotel-shortname-2\" is already in use",
                want_established: ("NotAccepted", "not all names are accepted"),
            },
            Case {
                name: "conflict on kind to listkind",
                input: full(crd("alfa.bravo.com")),
                existing: vec![status_names(
                    crd("india.bravo.com"),
                    "india",
                    "indias",
                    "",
                    "echo-kind",
                    &[],
                )],
                want_names: names(
                    "alfa",
                    "delta-singular",
                    "",
                    "foxtrot-listkind",
                    &["golf-shortname-1", "hotel-shortname-2"],
                ),
                want_reason: "KindConflict",
                want_message: "\"echo-kind\" is already in use",
                want_established: ("NotAccepted", "not all names are accepted"),
            },
        ];
        for case in cases {
            let mut group = case.existing.clone();
            group.push(case.input.clone());
            let (all_resources, all_kinds) =
                accepted_names_for_group(&case.input.spec.group, &group);
            let (got_names, naming, established) =
                calculate_names_and_conditions(&case.input, &all_resources, &all_kinds);
            assert!(
                names_equal(&got_names, &case.want_names),
                "{}: names {got_names:?}",
                case.name
            );
            assert_eq!(
                naming.reason.as_deref(),
                Some(case.want_reason),
                "{}",
                case.name
            );
            assert_eq!(
                naming.message.as_deref(),
                Some(case.want_message),
                "{}",
                case.name
            );
            assert_eq!(
                established.reason.as_deref(),
                Some(case.want_established.0),
                "{}",
                case.name
            );
            assert_eq!(
                established.message.as_deref(),
                Some(case.want_established.1),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn several_short_name_conflicts_are_aggregated() {
        assert_eq!(aggregate(&["a".into()]), "a");
        assert_eq!(aggregate(&["a".into(), "b".into()]), "[a, b]");
    }

    /// The naming controller leaves a CRD whose spec names are already
    /// accepted alone; the establishing controller needs NamesAccepted and
    /// does nothing once Established (establishing_controller.go:132-135).
    #[test]
    fn sync_functions_stop_when_there_is_nothing_to_do() {
        let mut c = spec_names(
            crd("alfa.bravo.com"),
            "alfa",
            "alfa",
            "Alfa",
            "AlfaList",
            &[],
        );
        let group = vec![c.clone()];
        let named = sync_names(&c, &group).expect("first sync accepts the names");
        assert!(sync_names(&named, std::slice::from_ref(&named)).is_none());
        assert_eq!(
            find_crd_condition(&named, ESTABLISHED)
                .unwrap()
                .reason
                .as_deref(),
            Some("Installing")
        );

        let established = sync_establishing(&named).expect("accepted names are established");
        assert!(is_crd_condition_equivalent(
            find_crd_condition(&established, ESTABLISHED),
            Some(&condition(
                ESTABLISHED,
                "True",
                "InitialNamesAccepted",
                "the initial names have been accepted"
            ))
        ));
        assert!(sync_establishing(&established).is_none());
        // Not accepted: not established.
        c.status = None;
        assert!(sync_establishing(&c).is_none());
    }

    /// `SetCRDCondition` moves the transition time only with the status
    /// (helpers.go:38-40).
    #[test]
    fn set_crd_condition_keeps_the_transition_time_while_status_is_equal() {
        let mut c = crd("alfa.bravo.com");
        set_crd_condition(&mut c, condition("X", "True", "A", "a"));
        c.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].last_transition_time =
            Some("2000-01-01T00:00:00Z".to_string());
        set_crd_condition(&mut c, condition("X", "True", "B", "b"));
        let got = find_crd_condition(&c, "X").unwrap();
        assert_eq!(
            got.last_transition_time.as_deref(),
            Some("2000-01-01T00:00:00Z")
        );
        assert_eq!(got.reason.as_deref(), Some("B"));
        set_crd_condition(&mut c, condition("X", "False", "B", "b"));
        assert_ne!(
            find_crd_condition(&c, "X")
                .unwrap()
                .last_transition_time
                .as_deref(),
            Some("2000-01-01T00:00:00Z")
        );
    }
}
