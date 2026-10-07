//! Pod-only API defaults.
//!
//! Upstream splits pod defaulting in two, and the split is load-bearing:
//!
//! - `SetDefaults_PodSpec` (`pkg/apis/core/v1/defaults.go:211`) runs for a Pod
//!   **and** for every embedded `PodTemplateSpec` — see the generated
//!   `zz_generated.defaults.go`, where `SetDefaults_PodSpec` is invoked from
//!   `SetObjectDefaults_Deployment`, `_ReplicaSet`, `_Job`, and friends.
//! - `SetDefaults_Pod` (defaults.go:164) runs **only** on a standalone `v1.Pod`
//!   (`zz_generated.defaults.go:208`, inside `SetObjectDefaults_Pod`).
//!
//! This module is the second one. Upstream states the reason inline
//! (defaults.go:165-167):
//!
//! ```text
//! // If limits are specified, but requests are not, default requests to limits
//! // This is done here rather than a more specific defaulting pass on v1.ResourceRequirements
//! // because we only want this defaulting semantic to take place on a v1.Pod and not a v1.PodTemplate
//! ```
//!
//! It lives in `common` rather than in the api-server because the api-server is
//! not the only decoder of a `v1.Pod`. The kubelet reads **static pod**
//! manifests straight off disk, and upstream defaults those too — the manifest
//! goes through `runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), json)`
//! (`pkg/kubelet/config/common.go:122`), and decoding a versioned object runs
//! its defaulters. A static pod that skipped this pass would be the one pod in
//! the cluster whose requests disagree with every other pod's.

use crate::resources::pod::{Container, PodSpec};

/// Default every missing container request from the matching limit, across
/// `spec.containers` and `spec.initContainers`.
///
/// Port of the resource block of upstream `SetDefaults_Pod`
/// (`pkg/apis/core/v1/defaults.go:168-192`), which is two verbatim-identical
/// loops — one over `Spec.Containers`, one over `Spec.InitContainers`.
///
/// Ephemeral containers are deliberately **not** defaulted: upstream has no
/// third loop for them, and they cannot declare resources in the first place.
///
/// Idempotent, so it is safe to run again after mutating webhooks the way
/// upstream re-runs defaulting on the mutated object.
pub fn default_pod_requests_from_limits(spec: &mut PodSpec) {
    for container in spec
        .containers
        .iter_mut()
        .chain(spec.init_containers.iter_mut().flatten())
    {
        default_container_requests_from_limits(container);
    }

    // defaults.go:194-199 - "Pod Requests default values must be applied after
    // container-level default values have been populated."
    if crate::feature_gates::enabled(crate::feature_gates::Feature::PodLevelResources) {
        default_huge_page_pod_limits(spec);
        default_pod_level_requests(spec);
    }
}

/// Port of upstream `defaultPodRequests` (`pkg/apis/core/v1/defaults.go:436-479`).
///
/// Only when pod-level limits are set (`len(Limits) == 0` returns early), a
/// missing pod-level request is defaulted from (1) the aggregated container
/// request, for overcommittable (native, non-hugepage) resources, then (2) the
/// pod-level limit. This is what makes a pod carrying only
/// `spec.resources.limits` Guaranteed.
pub fn default_pod_level_requests(spec: &mut PodSpec) {
    use crate::quota::{aggregate_container_resources, is_supported_pod_level_resource};

    let Some(resources) = spec.resources.as_ref() else {
        return;
    };
    let Some(limits) = resources.limits.clone().filter(|l| !l.is_empty()) else {
        return;
    };
    let mut pod_reqs = resources.requests.clone().unwrap_or_default();

    let pod = crate::resources::Pod::new("", spec.clone());
    for (key, qty) in aggregate_container_resources(&pod, true) {
        // IsOvercommitAllowed = IsNativeResource && !IsHugePageResourceName
        // (pkg/apis/core/v1/helper/helpers.go:130-133).
        if !pod_reqs.contains_key(&key)
            && is_supported_pod_level_resource(&key)
            && crate::quota::is_native_resource_name(&key)
            && !crate::quota::is_hugepage_resource_name(&key)
        {
            pod_reqs.insert(key, qty.to_string());
        }
    }
    for (key, lim) in &limits {
        if !pod_reqs.contains_key(key) && is_supported_pod_level_resource(key) {
            pod_reqs.insert(key.clone(), lim.clone());
        }
    }
    if !pod_reqs.is_empty() {
        if let Some(r) = spec.resources.as_mut() {
            r.requests = Some(pod_reqs);
        }
    }
}

/// Port of upstream `defaultHugePagePodLimits` (`defaults.go:482-526`): when
/// containers set a hugepages limit and the pod-level block (already partly
/// specified) has neither that limit nor a request for it, the pod-level limit
/// defaults to the aggregated container hugepages limit.
pub fn default_huge_page_pod_limits(spec: &mut PodSpec) {
    use crate::quota::{
        aggregate_container_resources, is_hugepage_resource_name, is_supported_pod_level_resource,
    };

    let Some(resources) = spec.resources.as_ref() else {
        return;
    };
    let has = |m: &Option<std::collections::HashMap<String, String>>| {
        m.as_ref().is_some_and(|m| !m.is_empty())
    };
    if !has(&resources.limits) && !has(&resources.requests) {
        return;
    }
    let mut pod_lims = resources.limits.clone().unwrap_or_default();

    let pod = crate::resources::Pod::new("", spec.clone());
    for (key, qty) in aggregate_container_resources(&pod, false) {
        if !is_supported_pod_level_resource(&key) || !is_hugepage_resource_name(&key) {
            continue;
        }
        if resources
            .requests
            .as_ref()
            .is_some_and(|r| r.contains_key(&key))
        {
            continue;
        }
        pod_lims.entry(key).or_insert_with(|| qty.to_string());
    }
    if !pod_lims.is_empty() {
        if let Some(r) = spec.resources.as_mut() {
            r.limits = Some(pod_lims);
        }
    }
}

/// One container's share of [`default_pod_requests_from_limits`].
///
/// Mirrors the loop body at `pkg/apis/core/v1/defaults.go:169-179`:
///
/// ```text
/// if container.Resources.Limits != nil {
///     if container.Resources.Requests == nil {
///         container.Resources.Requests = make(v1.ResourceList)
///     }
///     for key, value := range container.Resources.Limits {
///         if _, exists := container.Resources.Requests[key]; !exists {
///             container.Resources.Requests[key] = value.DeepCopy()
///         }
///     }
/// }
/// ```
///
/// Note the guard is `Limits != nil`, not "limits is non-empty": a container
/// carrying an explicit empty `limits` map gets an empty `requests` map, and
/// serialises with `"requests":{}` exactly as upstream does. An **absent**
/// `limits` leaves `requests` untouched.
///
/// A request that is already present wins — upstream fills only keys for which
/// `!exists` holds, so an explicit `requests.cpu` is never overwritten by a
/// larger `limits.cpu`.
pub fn default_container_requests_from_limits(container: &mut Container) {
    let Some(resources) = container.resources.as_mut() else {
        return;
    };
    let Some(limits) = resources.limits.clone() else {
        return;
    };
    let requests = resources.requests.get_or_insert_with(Default::default);
    for (key, value) in limits {
        requests.entry(key).or_insert(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ResourceRequirements;
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

    use crate::feature_gates::{with_feature, Feature};
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
