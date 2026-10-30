//! Unit tests moved from `src/defaults.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::{Container, PodSpec};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::defaults::*;
    use rusternetes_common::types::ResourceRequirements;
    use std::collections::HashMap;

    fn container(
        name: &str,
        limits: Option<&[(&str, &str)]>,
        requests: Option<&[(&str, &str)]>,
    ) -> Container {
        let to_map = |kv: &[(&str, &str)]| -> HashMap<String, String> {
            kv.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        Container {
            name: name.to_string(),
            image: "img".to_string(),
            resources: Some(ResourceRequirements {
                limits: limits.map(to_map),
                requests: requests.map(to_map),
                claims: None,
            }),
            ..Default::default()
        }
    }

    fn get<'a>(c: &'a Container, which: &str, key: &str) -> Option<&'a String> {
        let r = c.resources.as_ref()?;
        let map = if which == "requests" {
            r.requests.as_ref()
        } else {
            r.limits.as_ref()
        }?;
        map.get(key)
    }

    #[test]
    fn absent_requests_are_filled_from_limits() {
        let mut c = container("c", Some(&[("cpu", "500m"), ("memory", "128Mi")]), None);
        default_container_requests_from_limits(&mut c);
        assert_eq!(get(&c, "requests", "cpu").map(String::as_str), Some("500m"));
        assert_eq!(
            get(&c, "requests", "memory").map(String::as_str),
            Some("128Mi")
        );
    }

    /// `if _, exists := Requests[key]; !exists` — a present request is kept, and
    /// only the missing keys are filled.
    #[test]
    fn present_requests_win_and_only_gaps_are_filled() {
        let mut c = container(
            "c",
            Some(&[("cpu", "500m"), ("memory", "128Mi")]),
            Some(&[("cpu", "100m")]),
        );
        default_container_requests_from_limits(&mut c);
        assert_eq!(get(&c, "requests", "cpu").map(String::as_str), Some("100m"));
        assert_eq!(
            get(&c, "requests", "memory").map(String::as_str),
            Some("128Mi")
        );
    }

    /// The upstream guard is `Limits != nil`. An explicit empty limits map still
    /// materialises an empty requests map; an absent one leaves requests alone.
    #[test]
    fn nil_versus_empty_limits() {
        let mut empty_limits = container("c", Some(&[]), None);
        default_container_requests_from_limits(&mut empty_limits);
        assert_eq!(
            empty_limits
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .map(HashMap::len),
            Some(0),
            "explicit empty limits materialise an empty requests map"
        );

        let mut no_limits = container("c", None, None);
        default_container_requests_from_limits(&mut no_limits);
        assert!(
            no_limits
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .is_none(),
            "absent limits leave requests untouched"
        );
    }

    /// Upstream runs the identical loop over `Spec.InitContainers`
    /// (defaults.go:181-192).
    #[test]
    fn init_containers_are_defaulted_too() {
        let mut spec = PodSpec {
            containers: vec![container("app", Some(&[("cpu", "500m")]), None)],
            init_containers: Some(vec![container("init", Some(&[("cpu", "250m")]), None)]),
            ..Default::default()
        };
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(
            get(&spec.containers[0], "requests", "cpu").map(String::as_str),
            Some("500m")
        );
        assert_eq!(
            get(
                &spec.init_containers.as_ref().unwrap()[0],
                "requests",
                "cpu"
            )
            .map(String::as_str),
            Some("250m")
        );
    }

    /// Re-running after a mutating webhook must not change an already-defaulted
    /// spec, which is what lets the api-server default before and after the
    /// webhook pass the way upstream does.
    #[test]
    fn defaulting_is_idempotent() {
        let mut spec = PodSpec {
            containers: vec![container(
                "app",
                Some(&[("cpu", "500m")]),
                Some(&[("cpu", "100m")]),
            )],
            ..Default::default()
        };
        default_pod_requests_from_limits(&mut spec);
        let once = spec.clone();
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(spec.containers[0].resources, once.containers[0].resources);
    }

    // ---- Pod-level resources: ports of upstream `TestPodResourcesDefaults`
    // (`pkg/apis/core/v1/defaults_test.go:378`). Upstream's loop only compares
    // quantities present in the actual object; these assert the exact map so an
    // extra or missing key also fails.

    use rusternetes_common::feature_gates::{with_feature, Feature};
    use serial_test::serial;

    fn map(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn rr(
        limits: Option<&[(&str, &str)]>,
        requests: Option<&[(&str, &str)]>,
    ) -> ResourceRequirements {
        ResourceRequirements {
            limits: limits.map(map),
            requests: requests.map(map),
            claims: None,
        }
    }

    fn pod_spec(containers: Vec<Container>, pod: Option<ResourceRequirements>) -> PodSpec {
        PodSpec {
            containers,
            resources: pod,
            ..Default::default()
        }
    }

    fn pod_requests(spec: &PodSpec) -> Option<HashMap<String, String>> {
        spec.resources.as_ref().and_then(|r| r.requests.clone())
    }

    /// "pod requests=unset limits=set, container resources=unset"
    #[test]
    #[serial]
    fn pod_limits_only_default_pod_requests_to_limits() {
        let _g = with_feature(Feature::PodLevelResources, true);
        let mut spec = pod_spec(
            vec![container("a", None, None)],
            Some(rr(Some(&[("cpu", "2m"), ("memory", "1Mi")]), None)),
        );
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(
            pod_requests(&spec),
            Some(map(&[("cpu", "2m"), ("memory", "1Mi")]))
        );
    }

    /// "pod limits=nil" / "pod limits=empty map": `len(Limits) == 0` returns
    /// early, so no pod-level requests appear; containers still default.
    #[test]
    #[serial]
    fn pod_without_limits_gets_no_pod_requests() {
        let _g = with_feature(Feature::PodLevelResources, true);
        for pod_res in [rr(None, None), rr(Some(&[]), None)] {
            let mut spec = pod_spec(
                vec![container(
                    "a",
                    Some(&[("cpu", "2m"), ("memory", "1Mi")]),
                    None,
                )],
                Some(pod_res),
            );
            default_pod_requests_from_limits(&mut spec);
            assert_eq!(pod_requests(&spec).unwrap_or_default(), HashMap::new());
            assert_eq!(
                get(&spec.containers[0], "requests", "cpu").map(String::as_str),
                Some("2m")
            );
        }
    }

    /// "pod requests=empty map limits=set, container requests=unset limits=set":
    /// pod requests default to the aggregated container requests (2m+1m,
    /// 1Mi+5Mi), not to the pod limits (5m, 7Mi).
    #[test]
    #[serial]
    fn pod_requests_default_to_aggregated_container_requests() {
        let _g = with_feature(Feature::PodLevelResources, true);
        let mut spec = pod_spec(
            vec![
                container("a", Some(&[("cpu", "2m"), ("memory", "1Mi")]), None),
                container("b", Some(&[("cpu", "1m"), ("memory", "5Mi")]), None),
            ],
            Some(rr(Some(&[("cpu", "5m"), ("memory", "7Mi")]), Some(&[]))),
        );
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(
            pod_requests(&spec),
            Some(map(&[("cpu", "3m"), ("memory", "6Mi")]))
        );
        assert_eq!(
            spec.resources.as_ref().unwrap().limits,
            Some(map(&[("cpu", "5m"), ("memory", "7Mi")])),
            "pod limits are never rewritten"
        );
    }

    /// "pod hugepages requests=unset limits=set, container hugepages ... different
    /// hugepagesizes between pod and container level".
    #[test]
    #[serial]
    fn hugepages_of_a_different_size_default_pod_limits_and_requests() {
        let _g = with_feature(Feature::PodLevelResources, true);
        let mut spec = pod_spec(
            vec![
                container("a", Some(&[("cpu", "2m"), ("hugepages-1Gi", "1Gi")]), None),
                container("b", Some(&[("cpu", "1m"), ("hugepages-2Mi", "2Mi")]), None),
            ],
            Some(rr(Some(&[("cpu", "5m"), ("hugepages-2Mi", "10Mi")]), None)),
        );
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(
            pod_requests(&spec),
            Some(map(&[
                ("cpu", "3m"),
                ("hugepages-2Mi", "10Mi"),
                ("hugepages-1Gi", "1Gi")
            ]))
        );
        assert_eq!(
            spec.resources.as_ref().unwrap().limits,
            Some(map(&[
                ("cpu", "5m"),
                ("hugepages-2Mi", "10Mi"),
                ("hugepages-1Gi", "1Gi")
            ]))
        );
    }

    /// Gate off (`podLevelResourcesEnabled` false in the upstream table).
    #[test]
    #[serial]
    fn pod_level_defaulting_is_gated() {
        let _g = with_feature(Feature::PodLevelResources, false);
        let mut spec = pod_spec(
            vec![container("a", None, None)],
            Some(rr(Some(&[("cpu", "2m")]), None)),
        );
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(pod_requests(&spec), None);
    }

    fn pod_limits(spec: &PodSpec) -> Option<HashMap<String, String>> {
        spec.resources.as_ref().and_then(|r| r.limits.clone())
    }

    /// Two containers with cpu + hugepages-2Mi limits (and so, after container
    /// defaulting, equal requests).
    fn hugepage_containers() -> Vec<Container> {
        vec![
            container("a", Some(&[("cpu", "2m"), ("hugepages-2Mi", "4Mi")]), None),
            container("b", Some(&[("cpu", "1m"), ("hugepages-2Mi", "2Mi")]), None),
        ]
    }

    /// `defaults_test.go` "pod has cpu limit with hugepages requests=unset
    /// limits=unset, container hugepages requests=unset limits=set": the pod
    /// limit gains the aggregated container hugepages limit (6Mi), and the
    /// request gains aggregated cpu (3m) and, from the new limit, hugepages.
    #[test]
    #[serial]
    fn pod_cpu_limit_gets_aggregated_hugepages_limit() {
        let _g = with_feature(Feature::PodLevelResources, true);
        let mut spec = pod_spec(
            hugepage_containers(),
            Some(rr(Some(&[("cpu", "5m")]), None)),
        );
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(
            pod_limits(&spec),
            Some(map(&[("cpu", "5m"), ("hugepages-2Mi", "6Mi")]))
        );
        assert_eq!(
            pod_requests(&spec),
            Some(map(&[("cpu", "3m"), ("hugepages-2Mi", "6Mi")]))
        );
    }

    /// "pod has cpu request with hugepages requests=unset limits=unset":
    /// requests alone make the block "partly specified", so the hugepages limit
    /// is created from the containers; the explicit cpu request is kept.
    #[test]
    #[serial]
    fn pod_cpu_request_with_container_hugepages_limits() {
        let _g = with_feature(Feature::PodLevelResources, true);
        let mut spec = pod_spec(
            hugepage_containers(),
            Some(rr(None, Some(&[("cpu", "5m")]))),
        );
        default_pod_requests_from_limits(&mut spec);
        let reqs = pod_requests(&spec).unwrap();
        assert_eq!(
            reqs["cpu"], "5m",
            "an explicit pod request is never rewritten"
        );
        assert_eq!(reqs["hugepages-2Mi"], "6Mi");
        assert_eq!(pod_limits(&spec).unwrap()["hugepages-2Mi"], "6Mi");
    }

    /// "pod hugepages requests=set limits=set, container hugepages
    /// requests=unset limits=set": a pod-level hugepages request suppresses
    /// limit defaulting (`defaults.go:507-510`); nothing changes.
    #[test]
    #[serial]
    fn pod_hugepages_request_blocks_limit_defaulting() {
        let _g = with_feature(Feature::PodLevelResources, true);
        let both = &[("cpu", "5m"), ("hugepages-2Mi", "10Mi")];
        let mut spec = pod_spec(hugepage_containers(), Some(rr(Some(both), Some(both))));
        default_pod_requests_from_limits(&mut spec);
        assert_eq!(pod_limits(&spec), Some(map(both)));
        assert_eq!(pod_requests(&spec), Some(map(both)));
    }

    /// Direct: `default_huge_page_pod_limits` does nothing unless the pod-level
    /// block already has a limit or a request (`defaults.go:491-493`).
    #[test]
    fn huge_page_limits_need_a_partly_specified_block() {
        for res in [None, Some(rr(None, None)), Some(rr(Some(&[]), Some(&[])))] {
            let mut spec = pod_spec(hugepage_containers(), res.clone());
            default_huge_page_pod_limits(&mut spec);
            assert_eq!(spec.resources, res);
        }
    }

    /// Direct: only hugepages are copied; cpu is left to `defaultPodRequests`.
    #[test]
    fn huge_page_limits_copy_only_hugepages() {
        let mut spec = pod_spec(
            hugepage_containers(),
            Some(rr(Some(&[("memory", "1Mi")]), None)),
        );
        default_huge_page_pod_limits(&mut spec);
        assert_eq!(
            pod_limits(&spec),
            Some(map(&[("memory", "1Mi"), ("hugepages-2Mi", "6Mi")]))
        );
    }

    /// Direct: an already-set pod hugepages limit is kept, not overwritten.
    #[test]
    fn huge_page_limits_keep_an_explicit_pod_limit() {
        let mut spec = pod_spec(
            hugepage_containers(),
            Some(rr(Some(&[("hugepages-2Mi", "10Mi")]), None)),
        );
        default_huge_page_pod_limits(&mut spec);
        assert_eq!(pod_limits(&spec), Some(map(&[("hugepages-2Mi", "10Mi")])));
    }

    /// Direct: `IsOvercommitAllowed` (`helpers.go:130-133`) - a container
    /// hugepages request is never copied into the pod-level request; only the
    /// pod-level limit can seed it.
    #[test]
    fn pod_requests_do_not_take_hugepages_from_containers() {
        let mut spec = pod_spec(
            vec![container(
                "a",
                None,
                Some(&[("cpu", "2m"), ("hugepages-2Mi", "4Mi")]),
            )],
            Some(rr(Some(&[("cpu", "5m")]), None)),
        );
        default_pod_level_requests(&mut spec);
        assert_eq!(pod_requests(&spec), Some(map(&[("cpu", "2m")])));
    }

    /// "pod limits=set, container unsupported requests=set limits=set": a
    /// pod-level request is created only for supported names; "storage" and
    /// "ephemeral-storage" at container level are not aggregated.
    #[test]
    fn pod_requests_ignore_unsupported_resources() {
        let mut spec = pod_spec(
            vec![container(
                "a",
                None,
                Some(&[("storage", "1Mi"), ("ephemeral-storage", "5Mi")]),
            )],
            Some(rr(Some(&[("storage", "1Mi")]), None)),
        );
        default_pod_level_requests(&mut spec);
        assert_eq!(pod_requests(&spec), None);

        let mut spec = pod_spec(
            vec![container("a", None, Some(&[("storage", "1Mi")]))],
            Some(rr(
                Some(&[("cpu", "2m"), ("memory", "1Mi"), ("storage", "1Mi")]),
                None,
            )),
        );
        default_pod_level_requests(&mut spec);
        assert_eq!(
            pod_requests(&spec),
            Some(map(&[("cpu", "2m"), ("memory", "1Mi")]))
        );
    }
}
