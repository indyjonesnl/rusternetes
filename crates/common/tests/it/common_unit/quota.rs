//! Unit tests moved from `src/quota.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Duration, Utc};
    pub use rusternetes_common::label_selector::{LabelOperator, LabelRequirement};
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::{
        ContainerStatus, PersistentVolumeClaim, Pod, PodAffinityTerm, ResourceQuotaSpec,
        ScopedResourceSelectorRequirement,
    };
    pub use rusternetes_common::types::{Phase, ResourceRequirements};
    pub use std::collections::{BTreeMap, BTreeSet, HashMap};
}

mod tests {
    // Port of TestIsNativeResource (pkg/apis/core/v1/helper/helpers_test.go:29-66)
    #[test]
    fn is_native_resource_matches_upstream_cases() {
        for (name, want) in [
            ("pod.alpha.kubernetes.io/opaque-int-resource-foo", true),
            ("kubernetes.io/resource-foo", true),
            ("foo", true),
            ("a/b", false),
            ("", true),
        ] {
            assert_eq!(is_native_resource_name(name), want, "{name:?}");
        }
    }

    // helpers.go:51-55 uses strings.Contains, not HasPrefix.
    #[test]
    fn is_prefixed_native_resource_uses_contains() {
        assert!(is_prefixed_native_resource("kubernetes.io/foo"));
        assert!(is_prefixed_native_resource("example.kubernetes.io/foo"));
        assert!(!is_prefixed_native_resource("example.com/foo"));
    }

    // helpers.go:39-49: IsQualifiedName("requests."+name) must hold.
    #[test]
    fn is_extended_resource_name_requires_qualified_quota_name() {
        assert!(is_extended_resource_name("example.com/dongle"));
        assert!(!is_extended_resource_name("example.com/-bad"));
        assert!(!is_extended_resource_name("example.com/a/b"));
        assert!(!is_extended_resource_name("example.kubernetes.io/foo"));
        // helpers.go:40: unqualified and `requests.`-prefixed names are never extended.
        assert!(!is_extended_resource_name("cpu"));
        assert!(!is_extended_resource_name("requests.example.com/foo"));
    }

    use super::__parent::*;
    use rusternetes_common::quota::*;
    use rusternetes_common::resources::{Container, PodSpec};

    fn q(s: &str) -> Quantity {
        Quantity::parse(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
    }

    /// `name: quantity` list from string pairs.
    fn rl(pairs: &[(&str, &str)]) -> ResourceList {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), q(value)))
            .collect()
    }

    fn raw(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn container(name: &str, requests: &[(&str, &str)], limits: &[(&str, &str)]) -> Container {
        Container {
            name: name.to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                requests: (!requests.is_empty()).then(|| raw(requests)),
                limits: (!limits.is_empty()).then(|| raw(limits)),
                claims: None,
            }),
            ..Default::default()
        }
    }

    fn sidecar(name: &str, requests: &[(&str, &str)]) -> Container {
        Container {
            restart_policy: Some("Always".to_string()),
            ..container(name, requests, &[])
        }
    }

    fn pod(containers: Vec<Container>, init: Vec<Container>) -> Pod {
        Pod::new(
            "p",
            PodSpec {
                containers,
                init_containers: (!init.is_empty()).then_some(init),
                ..Default::default()
            },
        )
    }

    // -----------------------------------------------------------------
    // ResourceList arithmetic — tables ported from upstream
    // staging/src/k8s.io/apiserver/pkg/quota/v1/resources_test.go
    // -----------------------------------------------------------------

    /// Port of upstream `TestEquals` (`resources_test.go:27-78`).
    #[test]
    fn test_equals_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, bool)> = vec![
            ("isEqual", rl(&[]), rl(&[]), true),
            (
                "isEqualWithKeys",
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                true,
            ),
            (
                "isNotEqualSameKeys",
                rl(&[("cpu", "200m"), ("memory", "1Gi")]),
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                false,
            ),
            (
                "isNotEqualDiffKeys",
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                rl(&[("cpu", "100m"), ("memory", "1Gi"), ("pods", "1")]),
                false,
            ),
        ];
        for (name, a, b, expected) in cases {
            assert_eq!(equals(&a, &b), expected, "{name}");
        }
    }

    /// Port of upstream `TestLessThanOrEqual` (`resources_test.go:80-117`).
    #[test]
    fn test_less_than_or_equal_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, bool, Vec<&str>)> = vec![
            ("isEmpty", rl(&[]), rl(&[]), true, vec![]),
            (
                "isEqual",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
                true,
                vec![],
            ),
            (
                "isLessThan",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "200m")]),
                true,
                vec![],
            ),
            (
                "isGreaterThan",
                rl(&[("cpu", "200m")]),
                rl(&[("cpu", "100m")]),
                false,
                vec!["cpu"],
            ),
        ];
        for (name, a, b, allowed, exceeded) in cases {
            let (got_allowed, got_exceeded) = less_than_or_equal(&a, &b);
            assert_eq!(got_allowed, allowed, "{name}");
            assert_eq!(got_exceeded, exceeded, "{name}");
        }
    }

    /// Port of upstream `TestMax` (`resources_test.go:119-157`).
    #[test]
    fn test_max_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, ResourceList)> = vec![
            ("noKeys", rl(&[]), rl(&[]), rl(&[])),
            (
                "toEmpty",
                rl(&[("cpu", "100m")]),
                rl(&[]),
                rl(&[("cpu", "100m")]),
            ),
            (
                "matching",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "150m")]),
                rl(&[("cpu", "150m")]),
            ),
            (
                "matching(reverse)",
                rl(&[("cpu", "150m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "150m")]),
            ),
            (
                "matching-equal",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
            ),
        ];
        for (name, a, b, expected) in cases {
            assert!(equals(&expected, &max(&a, &b)), "{name}");
        }
    }

    /// Port of upstream `TestAdd` (`resources_test.go:159-187`).
    #[test]
    fn test_add_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, ResourceList)> = vec![
            ("noKeys", rl(&[]), rl(&[]), rl(&[])),
            (
                "toEmpty",
                rl(&[("cpu", "100m")]),
                rl(&[]),
                rl(&[("cpu", "100m")]),
            ),
            (
                "matching",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "200m")]),
            ),
        ];
        for (name, a, b, expected) in cases {
            assert!(equals(&expected, &add(&a, &b)), "{name}");
        }
    }

    /// Port of upstream `TestSubtract` (`resources_test.go:189-222`).
    #[test]
    fn test_subtract_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, ResourceList)> = vec![
            ("noKeys", rl(&[]), rl(&[]), rl(&[])),
            (
                "value-empty",
                rl(&[("cpu", "100m")]),
                rl(&[]),
                rl(&[("cpu", "100m")]),
            ),
            (
                "empty-value",
                rl(&[]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "-100m")]),
            ),
            (
                "value-value",
                rl(&[("cpu", "200m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
            ),
        ];
        for (name, a, b, expected) in cases {
            assert!(equals(&expected, &subtract(&a, &b)), "{name}");
        }
    }

    #[test]
    fn test_subtract_with_non_negative_result_clamps() {
        let got = subtract_with_non_negative_result(
            &rl(&[("cpu", "100m"), ("memory", "2Gi")]),
            &rl(&[("cpu", "500m")]),
        );
        assert!(got["cpu"].is_zero());
        assert!(got["memory"].value_eq(&q("2Gi")));
        // A name only in `b` still appears, at zero.
        let only_b = subtract_with_non_negative_result(&rl(&[]), &rl(&[("pods", "3")]));
        assert!(only_b["pods"].is_zero());
    }

    /// Upstream's `Add` adopts the addend's format when the accumulator is
    /// zero (`quantity.go:604-606`); folding from an empty list must therefore
    /// come out in the addend's suffix, not in raw bytes.
    #[test]
    fn test_add_preserves_binary_si_when_folding_from_empty() {
        let folded = add(
            &add(&rl(&[]), &rl(&[("memory", "512Mi")])),
            &rl(&[("memory", "512Mi")]),
        );
        assert_eq!(folded["memory"].canonical_string(), "1Gi");
    }

    /// The whole point of the module: memory `100m` is a tenth of a byte and
    /// must not compare equal to `1`. The `i64`-bytes route ceilinged both to 1.
    #[test]
    fn test_sub_byte_memory_quantity_survives() {
        let (allowed, exceeded) = less_than_or_equal(
            &rl(&[("requests.memory", "100m")]),
            &rl(&[("requests.memory", "1")]),
        );
        assert!(allowed);
        assert!(exceeded.is_empty());
        assert!(!q("100m").value_eq(&q("1")));
        assert_eq!(
            add(
                &rl(&[("requests.memory", "100m")]),
                &rl(&[("requests.memory", "100m")])
            )["requests.memory"]
                .canonical_string(),
            "200m"
        );
    }

    #[test]
    fn test_mask_and_resource_names_and_pretty_print() {
        let list = rl(&[
            ("requests.cpu", "1"),
            ("requests.memory", "1Gi"),
            ("pods", "2"),
        ]);
        assert_eq!(
            resource_names(&list),
            vec!["pods", "requests.cpu", "requests.memory"]
        );
        let masked = mask(&list, &["requests.cpu".to_string(), "nope".to_string()]);
        assert_eq!(resource_names(&masked), vec!["requests.cpu"]);
        // Upstream prettyPrint: name=value, name-sorted, comma-joined.
        assert_eq!(
            pretty_print(&list),
            "pods=2,requests.cpu=1,requests.memory=1Gi"
        );
    }

    #[test]
    fn test_parse_resource_list_drops_unparseable_and_trims() {
        let list = parse_resource_list(&raw(&[
            ("cpu", " 500m "),
            ("memory", "0.5Gi"),
            ("broken", "1GiGi"),
        ]));
        assert_eq!(list.len(), 2);
        assert!(list["cpu"].value_eq(&q("500m")));
        assert!(list["memory"].value_eq(&q("536870912")));
    }

    #[test]
    fn test_is_zero_and_to_string_map() {
        assert!(is_zero(&rl(&[("cpu", "0"), ("memory", "0")])));
        assert!(!is_zero(&rl(&[("cpu", "0"), ("memory", "1")])));
        let strings = to_string_map(&rl(&[("requests.cpu", "2000m")]));
        // Canonical form, not the input spelling.
        assert_eq!(strings["requests.cpu"], "2");
    }

    // -----------------------------------------------------------------
    // Resource-name classification
    // -----------------------------------------------------------------

    #[test]
    fn test_resource_name_classification() {
        assert!(is_extended_resource_name("example.com/dongle"));
        assert!(is_extended_resource_name("nvidia.com/gpu"));
        // Native: unqualified, or under kubernetes.io/.
        assert!(!is_extended_resource_name("cpu"));
        assert!(!is_extended_resource_name("memory"));
        assert!(!is_extended_resource_name("ephemeral-storage"));
        assert!(!is_extended_resource_name("hugepages-2Mi"));
        assert!(!is_extended_resource_name("kubernetes.io/foo"));
        // A quota key is not itself a resource name.
        assert!(!is_extended_resource_name("requests.example.com/dongle"));
        assert!(is_hugepage_resource_name("hugepages-1Gi"));
        assert!(!is_hugepage_resource_name("hugepages"));
    }

    // -----------------------------------------------------------------
    // Pod usage — rows ported from upstream
    // staging/src/k8s.io/component-helpers/resource/helpers_test.go
    // (TestPodResourceRequests) and pkg/quota/v1/evaluator/core/pods_test.go
    // -----------------------------------------------------------------

    /// "nil options, larger init container" (`helpers_test.go:299-320`): the
    /// init container's `4` wins over the app container's `1`. Summing only
    /// `spec.containers` — what the quota paths used to do — charges `1`.
    #[test]
    fn test_pod_requests_larger_init_container_wins() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![container("i1", &[("cpu", "4")], &[])],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "4");
    }

    /// "nil options, larger containers" (`helpers_test.go:322-350`).
    #[test]
    fn test_pod_requests_larger_containers_win() {
        let p = pod(
            vec![
                container("c1", &[("cpu", "2")], &[]),
                container("c2", &[("cpu", "3")], &[]),
            ],
            vec![container("i1", &[("cpu", "2")], &[])],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "5");
    }

    /// "restartable init container" (`helpers_test.go:691-717`): a sidecar's
    /// request is held for the pod's whole life, so it adds rather than maxes.
    #[test]
    fn test_pod_requests_restartable_init_adds() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![sidecar("ri1", &[("cpu", "1")])],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "2");
    }

    /// "multiple restartable init containers" (`helpers_test.go:718-770`):
    /// max(5, (3+2+1) + 1) = 7.
    #[test]
    fn test_pod_requests_multiple_restartable_init() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![
                container("i1", &[("cpu", "5")], &[]),
                sidecar("ri1", &[("cpu", "1")]),
                sidecar("ri2", &[("cpu", "2")]),
                sidecar("ri3", &[("cpu", "3")]),
            ],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "7");
    }

    /// "multiple restartable and regular init containers"
    /// (`helpers_test.go:771-830`): init-2 needs 5 plus the two sidecars
    /// already running (1+2) = 8; the sidecar starting after it does not count.
    #[test]
    fn test_pod_requests_restartable_then_regular_init() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![
                container("i1", &[("cpu", "5")], &[]),
                sidecar("ri1", &[("cpu", "1")]),
                sidecar("ri2", &[("cpu", "2")]),
                container("i2", &[("cpu", "5")], &[]),
                sidecar("ri3", &[("cpu", "3")]),
            ],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "8");
    }

    /// "restartable-init, init and regular" (`helpers_test.go:831-869`):
    /// init-1's 200 plus the sidecar's 10 = 210.
    #[test]
    fn test_pod_requests_restartable_init_and_regular() {
        let p = pod(
            vec![container("c1", &[("cpu", "100")], &[])],
            vec![
                sidecar("ri1", &[("cpu", "10")]),
                container("i1", &[("cpu", "200")], &[]),
            ],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "210");
    }

    /// "pod overhead included" (`helpers_test.go:386-...`): overhead is added
    /// to requests unconditionally.
    #[test]
    fn test_pod_requests_includes_overhead() {
        let mut p = pod(vec![container("c1", &[("cpu", "5")], &[])], vec![]);
        p.spec.as_mut().unwrap().overhead = Some(raw(&[("cpu", "1"), ("memory", "1Gi")]));
        let reqs = pod_requests(&p);
        assert_eq!(reqs["cpu"].canonical_string(), "6");
        assert_eq!(reqs["memory"].canonical_string(), "1Gi");
    }

    /// `PodLimits` adds overhead only to limits that are already set and
    /// non-zero (`helpers.go:366-373`).
    #[test]
    fn test_pod_limits_overhead_only_touches_present_limits() {
        let mut p = pod(vec![container("c1", &[], &[("cpu", "2")])], vec![]);
        p.spec.as_mut().unwrap().overhead = Some(raw(&[("cpu", "1"), ("memory", "1Gi")]));
        let limits = pod_limits(&p);
        assert_eq!(limits["cpu"].canonical_string(), "3");
        assert!(
            !limits.contains_key("memory"),
            "overhead must not create a memory limit the pod never set"
        );
    }

    /// Pod-level `spec.resources` overrides the container aggregate for the
    /// names it sets (`helpers.go:168-176`), and only for supported names.
    #[test]
    #[serial_test::serial]
    fn test_pod_requests_pod_level_resources_override() {
        let mut p = pod(vec![container("c1", &[("cpu", "1")], &[])], vec![]);
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "4"), ("example.com/dongle", "2")])),
            limits: None,
            claims: None,
        });
        let reqs = pod_requests(&p);
        assert_eq!(reqs["cpu"].canonical_string(), "4");
        assert!(
            !reqs.contains_key("example.com/dongle"),
            "only cpu/memory/hugepages are supported pod-level resources"
        );
    }

    /// Port of `pods_test.go` "init container hugepages" (`:245-260`): a
    /// hugepages request lands under both the bare name and `requests.`.
    #[test]
    fn test_pod_compute_usage_hugepages() {
        let p = pod(
            vec![],
            vec![container("i1", &[("hugepages-2Mi", "100Mi")], &[])],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["hugepages-2Mi"].canonical_string(), "100Mi");
        assert_eq!(usage["requests.hugepages-2Mi"].canonical_string(), "100Mi");
    }

    /// Port of `pods_test.go` "init container extended resources" (`:262-...`):
    /// an extended resource is charged **only** under `requests.<name>`.
    #[test]
    fn test_pod_compute_usage_extended_resource_requests_key_only() {
        let p = pod(
            vec![container(
                "c1",
                &[("example.com/dongle", "3")],
                &[("example.com/dongle", "3")],
            )],
            vec![],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["requests.example.com/dongle"].canonical_string(), "3");
        assert!(!usage.contains_key("example.com/dongle"));
        assert!(!usage.contains_key("limits.example.com/dongle"));
    }

    /// An extended-resource quantity with a suffix. `qty.parse::<i64>()` — the
    /// call this module replaces — returned `None` here, so a `2k`-dongle pod
    /// counted as asking for nothing.
    #[test]
    fn test_pod_compute_usage_extended_resource_with_suffix() {
        let p = pod(
            vec![container("c1", &[("example.com/dongle", "2k")], &[])],
            vec![],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(
            usage["requests.example.com/dongle"].value(),
            2000,
            "a suffixed extended-resource quantity must not read as zero"
        );
    }

    /// `podComputeUsageHelper` charges cpu/memory/ephemeral-storage requests
    /// under both the bare name and `requests.`, and limits under `limits.`
    /// (`pods.go:294-320`).
    #[test]
    fn test_pod_compute_usage_key_layout() {
        let p = pod(
            vec![container(
                "c1",
                &[
                    ("cpu", "500m"),
                    ("memory", "1Gi"),
                    ("ephemeral-storage", "32Mi"),
                ],
                &[
                    ("cpu", "1"),
                    ("memory", "2Gi"),
                    ("ephemeral-storage", "64Mi"),
                ],
            )],
            vec![],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["pods"].canonical_string(), "1");
        for (key, expected) in [
            ("cpu", "500m"),
            ("requests.cpu", "500m"),
            ("limits.cpu", "1"),
            ("memory", "1Gi"),
            ("requests.memory", "1Gi"),
            ("limits.memory", "2Gi"),
            ("ephemeral-storage", "32Mi"),
            ("requests.ephemeral-storage", "32Mi"),
            ("limits.ephemeral-storage", "64Mi"),
        ] {
            assert_eq!(usage[key].canonical_string(), expected, "key {key}");
        }
    }

    /// A fractional request must survive to the quota key. `0.5Gi` read as 0
    /// through every hand-rolled parser this replaces.
    #[test]
    fn test_pod_compute_usage_fractional_memory() {
        let p = pod(vec![container("c1", &[("memory", "0.5Gi")], &[])], vec![]);
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["requests.memory"].value(), 536_870_912);
    }

    // -----------------------------------------------------------------
    // QuotaV1Pod
    // -----------------------------------------------------------------

    /// Port of upstream `QuotaV1Pod` (`pods.go:491-508`).
    #[test]
    fn test_is_quota_charged() {
        use rusternetes_common::resources::PodStatus;
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:10:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let mut p = pod(vec![container("c1", &[("cpu", "1")], &[])], vec![]);
        assert!(is_quota_charged(&p, now), "running pod is charged");

        p.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        assert!(!is_quota_charged(&p, now), "terminal pod is not charged");

        p.status = Some(PodStatus {
            phase: Some(Phase::Failed),
            ..Default::default()
        });
        assert!(!is_quota_charged(&p, now));

        // Terminating but inside the grace period: still charged. Dropping
        // every pod with a deletionTimestamp under-counts usage here.
        p.status = None;
        p.metadata.deletion_timestamp = Some(
            DateTime::parse_from_rfc3339("2026-01-01T00:09:30Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        p.metadata.deletion_grace_period_seconds = Some(60);
        assert!(is_quota_charged(&p, now), "grace period has not elapsed");

        // Past the grace period: stop charging, so a pod wedged Terminating on
        // a lost node cannot block a scale-up (`pods.go:496-498`).
        p.metadata.deletion_grace_period_seconds = Some(5);
        assert!(!is_quota_charged(&p, now));

        // A deletionTimestamp with no grace period never stops the charge.
        p.metadata.deletion_grace_period_seconds = None;
        assert!(is_quota_charged(&p, now));
    }

    /// `pod_usage` always charges `count/pods`, even for a terminal pod —
    /// object-count quota tracks everything in storage (`pods.go:390-397`).
    #[test]
    fn test_pod_usage_count_pods_survives_terminal_phase() {
        use rusternetes_common::resources::PodStatus;
        let now = Utc::now();
        let mut p = pod(vec![container("c1", &[("cpu", "1")], &[])], vec![]);
        let live = pod_usage(&p, now);
        assert_eq!(live["count/pods"].canonical_string(), "1");
        assert_eq!(live["requests.cpu"].canonical_string(), "1");

        p.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let terminal = pod_usage(&p, now);
        assert_eq!(terminal["count/pods"].canonical_string(), "1");
        assert!(!terminal.contains_key("requests.cpu"));
        assert!(!terminal.contains_key("pods"));
    }

    // -----------------------------------------------------------------
    // podEvaluator.Constraints - ported from pods_test.go
    // `TestPodConstraintsFunc` (`:44-170`)
    // -----------------------------------------------------------------

    fn names(n: &[&str]) -> Vec<String> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_pod_constraints_init_container_resource_missing() {
        let p = pod(
            vec![],
            vec![container("dummy", &[("cpu", "1m")], &[("cpu", "2m")])],
        );
        assert_eq!(
            pod_constraints(&p, &names(&["memory"])),
            Err("must specify memory for: dummy".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_multiple_init_containers_sorted() {
        let p = pod(
            vec![],
            vec![
                container("foo", &[("cpu", "1m")], &[("cpu", "2m")]),
                container("bar", &[("cpu", "1m")], &[("cpu", "2m")]),
            ],
        );
        assert_eq!(
            pod_constraints(&p, &names(&["memory"])),
            Err("must specify memory for: bar,foo".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_container_resource_missing() {
        let p = pod(
            vec![container("dummy", &[("cpu", "1m")], &[("cpu", "2m")])],
            vec![],
        );
        assert_eq!(
            pod_constraints(&p, &names(&["memory"])),
            Err("must specify memory for: dummy".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_requests_cpu_omitted() {
        // The issue's case: `hard: requests.cpu` and a container with no request.
        let p = pod(vec![container("c", &[], &[])], vec![]);
        assert_eq!(
            pod_constraints(&p, &names(&["requests.cpu"])),
            Err("must specify requests.cpu for: c".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_satisfied_by_request_or_limit() {
        let p = pod(
            vec![container(
                "c",
                &[("cpu", "1"), ("memory", "1Gi")],
                &[("cpu", "1"), ("memory", "1Gi")],
            )],
            vec![],
        );
        assert_eq!(
            pod_constraints(
                &p,
                &names(&[
                    "cpu",
                    "memory",
                    "requests.cpu",
                    "requests.memory",
                    "limits.cpu",
                    "limits.memory"
                ])
            ),
            Ok(())
        );
    }

    #[test]
    fn test_pod_constraints_multiple_resources_joined_with_semicolon() {
        let p = pod(vec![container("c", &[], &[])], vec![]);
        assert_eq!(
            pod_constraints(&p, &names(&["memory", "limits.cpu"])),
            Err("must specify limits.cpu for: c; memory for: c".to_string())
        );
    }

    /// `validationSet` is only cpu/memory: pods, object counts,
    /// ephemeral-storage and extended resources never impose a requirement
    /// (`pods.go:97-110`).
    #[test]
    fn test_pod_constraints_ignores_non_validation_resources() {
        let p = pod(vec![container("c", &[], &[])], vec![]);
        assert_eq!(
            pod_constraints(
                &p,
                &names(&[
                    "pods",
                    "count/pods",
                    "requests.ephemeral-storage",
                    "requests.example.com/foo",
                    "hugepages-2Mi"
                ])
            ),
            Ok(())
        );
    }

    /// `IsPodLevelResourcesSet` skips the check (`pods.go:131-133`).
    #[test]
    #[serial_test::serial]
    fn test_pod_constraints_skipped_with_pod_level_resources() {
        let mut p = pod(vec![container("c", &[], &[])], vec![]);
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "1")])),
            limits: None,
            claims: None,
        });
        assert_eq!(pod_constraints(&p, &names(&["requests.cpu"])), Ok(()));
    }

    /// With `PodLevelResources` off, `SkipPodLevelResources` is set
    /// (`pods.go:403-404`) so `PodRequests` ignores `spec.resources`
    /// (`helpers.go:155`).
    #[test]
    #[serial_test::serial]
    fn test_pod_requests_limits_skip_pod_level_when_gate_off() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let mut p = pod(
            vec![container("c1", &[("cpu", "1")], &[("cpu", "2")])],
            vec![],
        );
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "4")])),
            limits: Some(raw(&[("cpu", "8")])),
            claims: None,
        });
        let _g = with_feature(Feature::PodLevelResources, false);
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "1");
        assert_eq!(pod_limits(&p)["cpu"].canonical_string(), "2");
    }

    /// `pods.go:143`: the constraints check is only skipped when the gate is on.
    #[test]
    #[serial_test::serial]
    fn test_pod_constraints_not_skipped_when_gate_off() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let mut p = pod(vec![container("c", &[], &[])], vec![]);
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "1")])),
            limits: None,
            claims: None,
        });
        let _g = with_feature(Feature::PodLevelResources, false);
        assert!(pod_constraints(&p, &names(&["requests.cpu"])).is_err());
    }

    fn pvc(
        spec: serde_json::Value,
        status: serde_json::Value,
        ann: serde_json::Value,
    ) -> rusternetes_common::resources::PersistentVolumeClaim {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "c", "namespace": "ns", "annotations": ann},
            "spec": spec, "status": status,
        }))
        .unwrap()
    }

    fn s(list: &ResourceList, k: &str) -> String {
        list[k].canonical_string()
    }

    /// `pvcEvaluator.Usage`: claim count, request, and the per-class keys.
    #[test]
    fn pvc_usage_charges_claim_storage_and_class() {
        let c = pvc(
            serde_json::json!({"storageClassName": "gold", "resources": {"requests": {"storage": "10Gi"}}}),
            serde_json::json!({"phase": "Pending"}),
            serde_json::json!({}),
        );
        let u = pvc_usage(&c);
        assert_eq!(s(&u, "persistentvolumeclaims"), "1");
        assert_eq!(s(&u, "count/persistentvolumeclaims"), "1");
        assert_eq!(s(&u, "requests.storage"), "10Gi");
        assert_eq!(
            s(
                &u,
                "gold.storageclass.storage.k8s.io/persistentvolumeclaims"
            ),
            "1"
        );
        assert_eq!(
            s(&u, "gold.storageclass.storage.k8s.io/requests.storage"),
            "10Gi"
        );
    }

    /// The beta annotation wins over `spec.storageClassName`
    /// (`GetPersistentVolumeClaimClass`), and a claim with no request
    /// charges no `requests.storage`.
    #[test]
    fn pvc_usage_beta_annotation_and_no_request() {
        let c = pvc(
            serde_json::json!({"storageClassName": "gold"}),
            serde_json::json!({}),
            serde_json::json!({"volume.beta.kubernetes.io/storage-class": "bronze"}),
        );
        let u = pvc_usage(&c);
        assert!(u.contains_key("bronze.storageclass.storage.k8s.io/persistentvolumeclaims"));
        assert!(!u.contains_key("requests.storage"));
        assert!(!u.contains_key("gold.storageclass.storage.k8s.io/persistentvolumeclaims"));
    }

    /// `getStorageUsage`: rounded up to whole bytes (issue 94313), and a
    /// larger `status.allocatedResources` wins.
    #[test]
    fn pvc_usage_rounds_up_and_prefers_larger_allocated() {
        let c = pvc(
            serde_json::json!({"resources": {"requests": {"storage": "1500m"}}}),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        assert_eq!(s(&pvc_usage(&c), "requests.storage"), "2");
        let c = pvc(
            serde_json::json!({"resources": {"requests": {"storage": "1Gi"}}}),
            serde_json::json!({"allocatedResources": {"storage": "2Gi"}}),
            serde_json::json!({}),
        );
        assert_eq!(s(&pvc_usage(&c), "requests.storage"), "2Gi");
        let c = pvc(
            serde_json::json!({"resources": {"requests": {"storage": "3Gi"}}}),
            serde_json::json!({"allocatedResources": {"storage": "2Gi"}}),
            serde_json::json!({}),
        );
        assert_eq!(s(&pvc_usage(&c), "requests.storage"), "3Gi");
    }
    fn sel(
        op: &str,
        values: &[&str],
    ) -> rusternetes_common::resources::ScopedResourceSelectorRequirement {
        rusternetes_common::resources::ScopedResourceSelectorRequirement {
            scope_name: "VolumeAttributesClass".to_string(),
            operator: op.to_string(),
            values: if values.is_empty() {
                None
            } else {
                Some(values.iter().map(|v| v.to_string()).collect())
            },
        }
    }

    /// `TestPersistentVolumeClaimEvaluatorMatchingScopes`
    /// (persistent_volume_claims_test.go:46-160), every case.
    #[test]
    fn pvc_matching_scopes_ports_upstream_cases() {
        let selectors_for = |names: &[&[&str]]| -> Vec<_> {
            let mut out = vec![sel("DoesNotExist", &[]), sel("Exists", &[])];
            out.extend(names.iter().map(|n| sel("In", n)));
            out.push(sel("NotIn", &["class4"]));
            out
        };
        let none = |c: &rusternetes_common::resources::PersistentVolumeClaim, s: &[_]| {
            pvc_matching_scopes(c, s).unwrap()
        };

        // EmptyPVC
        let c = pvc(
            serde_json::json!({}),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let s_in = vec![
            sel("DoesNotExist", &[]),
            sel("Exists", &[]),
            sel("In", &["class1"]),
            sel("NotIn", &["class4"]),
        ];
        assert_eq!(
            none(&c, &s_in),
            vec![sel("DoesNotExist", &[]), sel("NotIn", &["class4"])]
        );

        // VolumeAttributesClass
        let c = pvc(
            serde_json::json!({"volumeAttributesClassName": "class1"}),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let all = vec![
            sel("DoesNotExist", &[]),
            sel("Exists", &[]),
            sel("In", &["class1"]),
            sel("In", &["class4"]),
            sel("NotIn", &["class4"]),
        ];
        assert_eq!(
            none(&c, &all),
            vec![
                sel("Exists", &[]),
                sel("In", &["class1"]),
                sel("NotIn", &["class4"])
            ]
        );

        // VolumeAttributesClassWithTarget
        let c = pvc(
            serde_json::json!({"volumeAttributesClassName": "class1"}),
            serde_json::json!({"currentVolumeAttributesClassName": "class2"}),
            serde_json::json!({}),
        );
        let all = selectors_for(&[
            &["class1"],
            &["class2"],
            &["class1", "class2"],
            &["class1", "class2", "class4"],
            &["class4"],
        ]);
        assert_eq!(
            none(&c, &all),
            vec![
                sel("Exists", &[]),
                sel("In", &["class1"]),
                sel("In", &["class2"]),
                sel("In", &["class1", "class2"]),
                sel("In", &["class1", "class2", "class4"]),
                sel("NotIn", &["class4"]),
            ]
        );

        // VolumeAttributesClassWithModityStatus
        let c = pvc(
            serde_json::json!({"volumeAttributesClassName": "class1"}),
            serde_json::json!({
                "currentVolumeAttributesClassName": "class2",
                "modifyVolumeStatus": {"targetVolumeAttributesClassName": "class3", "status": "Pending"},
            }),
            serde_json::json!({}),
        );
        let all = selectors_for(&[
            &["class1"],
            &["class2"],
            &["class3"],
            &["class1", "class2", "class3"],
            &["class1", "class2", "class3", "class4"],
            &["class4"],
        ]);
        assert_eq!(
            none(&c, &all),
            vec![
                sel("Exists", &[]),
                sel("In", &["class1"]),
                sel("In", &["class2"]),
                sel("In", &["class3"]),
                sel("In", &["class1", "class2", "class3"]),
                sel("In", &["class1", "class2", "class3", "class4"]),
                sel("NotIn", &["class4"]),
            ]
        );
    }

    /// `pvcMatchesScopeFunc` (:108-127): a scope other than
    /// VolumeAttributesClass matches nothing.
    #[test]
    fn pvc_matches_scope_other_scope_is_false() {
        let c = pvc(
            serde_json::json!({}),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let mut s = sel("Exists", &[]);
        s.scope_name = "BestEffort".to_string();
        assert!(!pvc_matches_scope(&s, &c).unwrap());
    }

    /// An operator outside the four is an error, as
    /// `ScopedResourceSelectorRequirementsAsSelector` (helpers.go:332-333).
    #[test]
    fn pvc_matches_scope_rejects_unknown_operator() {
        let c = pvc(
            serde_json::json!({"volumeAttributesClassName": "a"}),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let err = pvc_matches_scope(&sel("Bogus", &[]), &c).unwrap_err();
        assert!(
            err.contains("is not a valid scope selector operator"),
            "{err}"
        );
    }

    /// `RequiresQuotaReplenish` (:79-91): the referenced class names, or
    /// `status.allocatedResources[storage]`, changed.
    #[test]
    fn pvc_requires_quota_replenish_cases() {
        let mk = |spec: serde_json::Value, status: serde_json::Value| {
            pvc(spec, status, serde_json::json!({}))
        };
        let a = mk(
            serde_json::json!({"volumeAttributesClassName": "gold"}),
            serde_json::json!({}),
        );
        let same = a.clone();
        assert!(!pvc_requires_quota_replenish(&same, &a));
        let b = mk(
            serde_json::json!({"volumeAttributesClassName": "gold"}),
            serde_json::json!({"currentVolumeAttributesClassName": "silver"}),
        );
        assert!(pvc_requires_quota_replenish(&b, &a));
        let c = mk(
            serde_json::json!({}),
            serde_json::json!({"allocatedResources": {"storage": "2Gi"}}),
        );
        let d = mk(
            serde_json::json!({}),
            serde_json::json!({"allocatedResources": {"storage": "1Gi"}}),
        );
        assert!(pvc_requires_quota_replenish(&c, &d));
        assert!(!pvc_requires_quota_replenish(&d, &d.clone()));
    }

    /// `getScopeSelectorsFromQuota` (generic/evaluator.go:171-181).
    #[test]
    fn scope_selectors_from_quota_cases() {
        let spec: rusternetes_common::resources::ResourceQuotaSpec =
            serde_json::from_value(serde_json::json!({
                "scopes": ["BestEffort"],
                "scopeSelector": {"matchExpressions": [
                    {"scopeName": "VolumeAttributesClass", "operator": "In", "values": ["gold"]}
                ]},
            }))
            .unwrap();
        let got = scope_selectors_from_quota(&spec);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].scope_name, "BestEffort");
        assert_eq!(got[0].operator, "Exists");
        assert_eq!(got[1].operator, "In");
    }

    // ---- podEvaluator scopes: pods_test.go TestPodEvaluatorMatchingScopes ----

    fn scope_sel(scope: &str, op: &str, values: &[&str]) -> ScopedResourceSelectorRequirement {
        ScopedResourceSelectorRequirement {
            scope_name: scope.to_string(),
            operator: op.to_string(),
            values: (!values.is_empty()).then(|| values.iter().map(|v| v.to_string()).collect()),
        }
    }

    fn pod_from(spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": spec,
        }))
        .unwrap()
    }

    /// The scopes (by name) of the test's full selector list a pod matches.
    fn matching(pod: &Pod, selectors: &[ScopedResourceSelectorRequirement]) -> Vec<String> {
        selectors
            .iter()
            .filter(|s| pod_matches_scope(s, pod).unwrap())
            .map(|s| s.scope_name.clone())
            .collect()
    }

    fn all_selectors() -> Vec<ScopedResourceSelectorRequirement> {
        vec![
            scope_sel("Terminating", "", &[]),
            scope_sel("NotTerminating", "", &[]),
            scope_sel("BestEffort", "", &[]),
            scope_sel("NotBestEffort", "", &[]),
            scope_sel("PriorityClass", "In", &["class1"]),
            scope_sel("CrossNamespacePodAffinity", "", &[]),
        ]
    }

    #[test]
    fn scopes_empty_pod_and_priority_class() {
        let pod = pod_from(serde_json::json!({"containers": []}));
        assert_eq!(
            matching(&pod, &all_selectors()),
            ["NotTerminating", "BestEffort"]
        );
        let pod = pod_from(serde_json::json!({"containers": [], "priorityClassName": "class1"}));
        assert_eq!(
            matching(&pod, &all_selectors()),
            ["NotTerminating", "BestEffort", "PriorityClass"]
        );
    }

    #[test]
    fn scopes_not_best_effort_and_terminating() {
        let pod = pod_from(
            serde_json::json!({"containers": [{"name": "c", "image": "i",
            "resources": {"requests": {"cpu": "1", "memory": "50M"},
                          "limits": {"cpu": "2", "memory": "100M"}}}]}),
        );
        assert_eq!(
            matching(&pod, &all_selectors()),
            ["NotTerminating", "NotBestEffort"]
        );
        let pod = pod_from(serde_json::json!({"containers": [], "activeDeadlineSeconds": 30}));
        assert_eq!(
            matching(&pod, &all_selectors()),
            ["Terminating", "BestEffort"]
        );
        // A deletionTimestamp alone is not Terminating (IsTerminating).
        let mut pod = pod_from(serde_json::json!({"containers": []}));
        pod.metadata.deletion_timestamp = Some(Utc::now());
        assert!(!is_terminating(&pod));
    }

    #[test]
    fn scopes_cross_namespace_pod_affinity() {
        let term = serde_json::json!({"labelSelector": {}, "topologyKey": "k"});
        let mut with_ns = term.clone();
        with_ns["namespaces"] = serde_json::json!(["ns1"]);
        let mut with_sel = term.clone();
        with_sel["namespaceSelector"] = serde_json::json!({});
        let weighted =
            |t: &serde_json::Value| serde_json::json!([{"weight": 1, "podAffinityTerm": t}]);
        for affinity in [
            serde_json::json!({"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [with_ns]}}),
            serde_json::json!({"podAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": weighted(&with_sel)}}),
            serde_json::json!({"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": weighted(&with_sel)}}),
            serde_json::json!({"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [with_ns]}}),
        ] {
            let pod = pod_from(serde_json::json!({"containers": [], "affinity": affinity}));
            assert!(uses_cross_namespace_pod_affinity(&pod), "{affinity}");
        }
        let pod = pod_from(serde_json::json!({"containers": [],
            "affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [term]}}}));
        assert!(!uses_cross_namespace_pod_affinity(&pod));
    }

    /// `podMatchesSelector`: PriorityClass In/NotIn/DoesNotExist over the
    /// pod's class, and the selector-parse errors.
    #[test]
    fn scopes_priority_class_operators() {
        let none = pod_from(serde_json::json!({"containers": []}));
        let c1 = pod_from(serde_json::json!({"containers": [], "priorityClassName": "c1"}));
        let m = |p: &Pod, op: &str, v: &[&str]| {
            pod_matches_scope(&scope_sel("PriorityClass", op, v), p)
        };
        assert_eq!(m(&c1, "In", &["c1", "c2"]), Ok(true));
        assert_eq!(m(&none, "In", &["c1"]), Ok(false));
        assert_eq!(m(&c1, "NotIn", &["c1"]), Ok(false));
        assert_eq!(m(&none, "NotIn", &["c1"]), Ok(true));
        assert_eq!(m(&none, "DoesNotExist", &[]), Ok(true));
        assert_eq!(m(&c1, "Exists", &[]), Ok(true));
        assert!(m(&c1, "In", &[]).is_err());
        assert!(m(&c1, "Bogus", &["x"]).is_err());
    }

    /// `getScopeSelectorsFromQuota` + `generic.Matches`: scopes AND
    /// scopeSelector all apply; an unknown scope matches nothing.
    #[test]
    fn quota_scopes_are_anded_and_unknown_matches_nothing() {
        let spec =
            |scopes: &[&str], exprs: Vec<ScopedResourceSelectorRequirement>| ResourceQuotaSpec {
                scopes: Some(scopes.iter().map(|s| s.to_string()).collect()),
                scope_selector: Some(rusternetes_common::resources::ScopeSelector {
                    match_expressions: exprs,
                }),
                ..Default::default()
            };
        let pod = pod_from(serde_json::json!({"containers": [], "priorityClassName": "c1"}));
        assert!(pod_matches_quota_scopes(
            &pod,
            &ResourceQuotaSpec::default()
        ));
        assert!(pod_matches_quota_scopes(
            &pod,
            &spec(
                &["BestEffort"],
                vec![scope_sel("PriorityClass", "In", &["c1"])]
            )
        ));
        assert!(!pod_matches_quota_scopes(
            &pod,
            &spec(&["BestEffort", "Terminating"], vec![])
        ));
        assert!(!pod_matches_quota_scopes(
            &pod,
            &spec(&["Nonsense"], vec![])
        ));
    }

    // -----------------------------------------------------------------
    // PodUsageFunc UseStatusResources — InPlacePodVerticalScaling
    // (pkg/quota/v1/evaluator/core/pods.go:400-404; ported from
    // pods_test.go TestPodEvaluatorUsageResourceResize :1026-1170 and
    // component-helpers/resource/helpers_test.go "resized, infeasible" cases)
    // -----------------------------------------------------------------

    /// A pod with one container (`spec` requests/limits) and one container
    /// status carrying `status_json` fields (merged into `{"name": ...}`).
    fn resized_pod(
        requests: &[(&str, &str)],
        limits: &[(&str, &str)],
        status_json: Option<serde_json::Value>,
        infeasible: bool,
    ) -> Pod {
        let mut p = pod(vec![container("c", requests, limits)], vec![]);
        let mut status = rusternetes_common::resources::PodStatus::default();
        if let Some(mut cs) = status_json {
            cs["name"] = serde_json::json!("c");
            status.container_statuses = Some(vec![serde_json::from_value(cs).unwrap()]);
        }
        if infeasible {
            status.conditions = Some(vec![serde_json::from_value(serde_json::json!({
                "type": "PodResizePending", "status": "True", "reason": "Infeasible"
            }))
            .unwrap()]);
        }
        p.status = Some(status);
        p
    }

    fn usage_of(p: &Pod) -> ResourceList {
        pod_usage(p, Utc::now())
    }

    /// `TestPodEvaluatorUsageResourceResize`: the larger of spec and the
    /// kubelet-actuated request is charged (`determineEffectiveRequests`).
    #[test]
    #[serial_test::serial]
    fn pod_usage_charges_max_of_spec_and_actuated_requests() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodVerticalScaling,
            true,
        );
        let p = resized_pod(
            &[("cpu", "100m"), ("memory", "200Mi")],
            &[("cpu", "200m"), ("memory", "400Mi")],
            Some(
                serde_json::json!({"resources": {"requests": {"cpu": "150m", "memory": "250Mi"}}}),
            ),
            false,
        );
        let u = usage_of(&p);
        assert_eq!(u["requests.cpu"].canonical_string(), "150m");
        assert_eq!(u["cpu"].canonical_string(), "150m");
        assert_eq!(u["requests.memory"].canonical_string(), "250Mi");
        assert_eq!(u["limits.cpu"].canonical_string(), "200m");
        assert_eq!(u["limits.memory"].canonical_string(), "400Mi");
    }

    /// `TestPodEvaluatorUsageResourceResize` "ContainerStatus.Resources==nil":
    /// no actuated resources, spec is charged.
    #[test]
    #[serial_test::serial]
    fn pod_usage_status_without_resources_uses_spec() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodVerticalScaling,
            true,
        );
        let p = resized_pod(
            &[("cpu", "100m")],
            &[("cpu", "200m")],
            Some(serde_json::json!({})),
            false,
        );
        assert_eq!(usage_of(&p)["requests.cpu"].canonical_string(), "100m");
    }

    /// `helpers_test.go` "resized: per-resource 3-way maximum": spec,
    /// actuated and allocated are compared per resource.
    #[test]
    #[serial_test::serial]
    fn pod_usage_requests_are_per_resource_max_of_spec_actuated_allocated() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodVerticalScaling,
            true,
        );
        let p = resized_pod(
            &[
                ("cpu", "30m"),
                ("memory", "20M"),
                ("ephemeral-storage", "10G"),
            ],
            &[],
            Some(serde_json::json!({
                "allocatedResources": {"cpu": "20m", "memory": "10M", "ephemeral-storage": "30G"},
                "resources": {"requests": {"cpu": "10m", "memory": "30M", "ephemeral-storage": "20G"}}
            })),
            false,
        );
        let u = usage_of(&p);
        assert_eq!(u["requests.cpu"].canonical_string(), "30m");
        assert_eq!(u["requests.memory"].canonical_string(), "30M");
        assert_eq!(u["requests.ephemeral-storage"].canonical_string(), "30G");
    }

    /// `helpers_test.go` "resized, infeasible": the spec request is ignored
    /// and the actuated one charged.
    #[test]
    #[serial_test::serial]
    fn pod_usage_infeasible_resize_ignores_spec_request() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodVerticalScaling,
            true,
        );
        let p = resized_pod(
            &[("cpu", "4")],
            &[],
            Some(serde_json::json!({"resources": {"requests": {"cpu": "2"}}})),
            true,
        );
        assert_eq!(usage_of(&p)["requests.cpu"].canonical_string(), "2");

        // "resized, infeasible & in-progress": allocated wins over actuated.
        let p = resized_pod(
            &[("cpu", "6")],
            &[],
            Some(serde_json::json!({
                "allocatedResources": {"cpu": "4"},
                "resources": {"requests": {"cpu": "2"}}
            })),
            true,
        );
        assert_eq!(usage_of(&p)["requests.cpu"].canonical_string(), "4");
    }

    /// `AggregateContainerLimits` + `determineEffectiveLimits`: limits are
    /// max(spec, actuated), or just actuated when the resize is infeasible.
    #[test]
    #[serial_test::serial]
    fn pod_usage_limits_follow_actuated_status() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodVerticalScaling,
            true,
        );
        let status = serde_json::json!({"resources": {"limits": {"cpu": "300m"}}});
        let p = resized_pod(&[], &[("cpu", "200m")], Some(status.clone()), false);
        assert_eq!(usage_of(&p)["limits.cpu"].canonical_string(), "300m");
        let p = resized_pod(&[], &[("cpu", "400m")], Some(status), true);
        assert_eq!(usage_of(&p)["limits.cpu"].canonical_string(), "300m");
    }

    /// With the gate off (`UseStatusResources: false`) only spec counts.
    #[test]
    #[serial_test::serial]
    fn pod_usage_ignores_status_when_gate_off() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodVerticalScaling,
            false,
        );
        let p = resized_pod(
            &[("cpu", "100m")],
            &[],
            Some(serde_json::json!({"resources": {"requests": {"cpu": "150m"}}})),
            false,
        );
        assert_eq!(usage_of(&p)["requests.cpu"].canonical_string(), "100m");
    }
}
