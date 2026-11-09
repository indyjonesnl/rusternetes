//! Subresources whose storage is a `rest.GroupVersionKindProvider`
//! (installer.go:1119-1124, 1127): their kind is registered, and served in
//! discovery, under the group version the provider returns.
//! `ScaleREST.GroupVersionKind` (pkg/registry/{core/replicationcontroller,
//! apps/deployment,apps/replicaset,apps/statefulset}/storage) is
//! autoscaling/v1 `Scale` for every served version; `EvictionREST`
//! (pkg/registry/core/pod/storage/eviction.go:83) is policy/v1 `Eviction`.

use rusternetes_discovery::{registered_kinds, RegisteredKind};

fn kind_of(group: &str, version: &str, resource: &str, sub: &str) -> RegisteredKind {
    registered_kinds()
        .into_iter()
        .find(|k| {
            k.group == group
                && k.version == version
                && k.resource == resource
                && k.subresource == sub
        })
        .unwrap_or_else(|| panic!("{group}/{version} {resource}/{sub} not registered"))
}

fn assert_kind(k: &RegisteredKind, group: &str, version: &str, kind: &str) {
    assert_eq!(
        (
            k.kind_group.as_str(),
            k.kind_version.as_str(),
            k.kind.as_str()
        ),
        (group, version, kind),
        "{k:?}"
    );
}

#[test]
fn scale_subresources_are_autoscaling_v1_scale() {
    for (g, r) in [
        ("", "replicationcontrollers"),
        ("apps", "deployments"),
        ("apps", "replicasets"),
        ("apps", "statefulsets"),
    ] {
        assert_kind(&kind_of(g, "v1", r, "scale"), "autoscaling", "v1", "Scale");
    }
}

#[test]
fn eviction_is_policy_v1_eviction() {
    assert_kind(
        &kind_of("", "v1", "pods", "eviction"),
        "policy",
        "v1",
        "Eviction",
    );
}

#[test]
fn other_subresources_keep_the_group_version_they_are_served_under() {
    assert_kind(&kind_of("", "v1", "pods", "binding"), "", "v1", "Binding");
    assert_kind(
        &kind_of("apps", "v1", "deployments", "status"),
        "apps",
        "v1",
        "Deployment",
    );
}

#[tokio::test]
async fn typed_resource_lists_carry_the_provider_group_version() {
    let (_, list) = rusternetes_discovery::get_apps_v1_resources().await;
    let scale = list
        .0
        .resources
        .iter()
        .find(|r| r.name == "deployments/scale")
        .unwrap();
    assert_eq!(scale.group.as_deref(), Some("autoscaling"));
    assert_eq!(scale.version.as_deref(), Some("v1"));
    let status = list
        .0
        .resources
        .iter()
        .find(|r| r.name == "deployments/status")
        .unwrap();
    assert_eq!(
        (status.group.as_deref(), status.version.as_deref()),
        (None, None)
    );

    let (_, core) = rusternetes_discovery::get_core_resources().await;
    let ev = core
        .0
        .resources
        .iter()
        .find(|r| r.name == "pods/eviction")
        .unwrap();
    assert_eq!(ev.group.as_deref(), Some("policy"));
    assert_eq!(ev.version.as_deref(), Some("v1"));
}
