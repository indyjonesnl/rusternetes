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
}
