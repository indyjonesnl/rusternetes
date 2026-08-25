//! Static-pod manifests get the Pod-only API defaults at decode time.
//!
//! Upstream reads a manifest through the scheme —
//! `runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), json)`
//! (`pkg/kubelet/config/common.go:122`) — and decoding a versioned object runs
//! its defaulters, so `SetObjectDefaults_Pod` and therefore `SetDefaults_Pod`
//! (`pkg/apis/core/v1/defaults.go:164-192`) apply.
//!
//! A static pod is the only pod in the cluster that never passes through the
//! api-server. Without this pass it would be the one pod whose limits-only
//! container reaches the runtime declaring no requests at all — a different QoS
//! class and different cgroup shares than the identical manifest posted to the
//! API.

use rusternetes_kubelet::static_pods::{parse_manifest, pod_config_hash};

const MANIFEST: &str = r#"
apiVersion: v1
kind: Pod
metadata:
  name: kube-apiserver
  namespace: kube-system
spec:
  initContainers:
  - name: init
    image: busybox
    resources:
      limits:
        cpu: 250m
        memory: 64Mi
  containers:
  - name: apiserver
    image: ghcr.io/indyjonesnl/rusternetes-api-server:latest
    resources:
      limits:
        cpu: 500m
        memory: 128Mi
      requests:
        cpu: 100m
"#;

#[test]
fn requests_default_from_limits_at_decode() {
    let pod = parse_manifest(MANIFEST.as_bytes(), "kube-apiserver.yaml").unwrap();
    let spec = pod.spec.as_ref().unwrap();

    let app_requests = spec.containers[0]
        .resources
        .as_ref()
        .unwrap()
        .requests
        .as_ref()
        .unwrap();
    assert_eq!(
        app_requests.get("cpu").map(String::as_str),
        Some("100m"),
        "an explicit request is never overwritten by the limit"
    );
    assert_eq!(
        app_requests.get("memory").map(String::as_str),
        Some("128Mi"),
        "the absent memory request is filled from the limit"
    );
}

/// The init-container loop at `defaults.go:181-192` is a verbatim copy of the
/// container loop, and static pods are exactly where init containers carrying
/// only limits are common (control-plane manifests).
#[test]
fn init_container_requests_default_from_limits() {
    let pod = parse_manifest(MANIFEST.as_bytes(), "kube-apiserver.yaml").unwrap();
    let spec = pod.spec.as_ref().unwrap();

    let init_requests = spec.init_containers.as_ref().unwrap()[0]
        .resources
        .as_ref()
        .unwrap()
        .requests
        .as_ref()
        .unwrap();
    assert_eq!(init_requests.get("cpu").map(String::as_str), Some("250m"));
    assert_eq!(
        init_requests.get("memory").map(String::as_str),
        Some("64Mi")
    );
}

/// The config hash is taken from the *defaulted* spec, as upstream hashes the
/// pod its decoder already defaulted. Two manifests differing only in whether
/// they spell out the requests their limits imply describe the same effective
/// pod, so they must hash alike — otherwise the mirror pod churns on a purely
/// cosmetic manifest edit.
#[test]
fn config_hash_is_taken_from_the_defaulted_spec() {
    const IMPLICIT: &str = r#"
apiVersion: v1
kind: Pod
metadata:
  name: p
  namespace: kube-system
spec:
  containers:
  - name: c
    image: busybox
    resources:
      limits:
        cpu: 500m
"#;
    const EXPLICIT: &str = r#"
apiVersion: v1
kind: Pod
metadata:
  name: p
  namespace: kube-system
spec:
  containers:
  - name: c
    image: busybox
    resources:
      limits:
        cpu: 500m
      requests:
        cpu: 500m
"#;
    let implicit = parse_manifest(IMPLICIT.as_bytes(), "a.yaml").unwrap();
    let explicit = parse_manifest(EXPLICIT.as_bytes(), "b.yaml").unwrap();
    assert_eq!(pod_config_hash(&implicit), pod_config_hash(&explicit));
}

/// A manifest that declares no limits keeps `requests` absent — the upstream
/// guard is `Limits != nil`, so nothing is materialised out of nowhere.
#[test]
fn manifests_without_limits_are_untouched() {
    const NO_RESOURCES: &str = r#"
apiVersion: v1
kind: Pod
metadata:
  name: p
  namespace: kube-system
spec:
  containers:
  - name: c
    image: busybox
"#;
    let pod = parse_manifest(NO_RESOURCES.as_bytes(), "n.yaml").unwrap();
    let spec = pod.spec.as_ref().unwrap();
    assert!(
        spec.containers[0]
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .is_none(),
        "absent limits must leave requests unset"
    );
}
