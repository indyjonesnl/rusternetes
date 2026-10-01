//! Strategies and stores for `snapshot.storage.k8s.io`.
//!
//! These types are not Kubernetes APIs: they are the CRDs of
//! kubernetes-csi/external-snapshotter, so `../kubernetes` has no `pkg/registry`
//! for them. Upstream serves them through the apiextensions-apiserver's
//! generic custom-resource store, whose strategy
//! (`staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresource/
//! {strategy,status_strategy}.go`) is what the strategies here port, for the
//! subresources each CRD declares: `status` on VolumeSnapshot and
//! VolumeSnapshotContent, none on VolumeSnapshotClass.
//!
//! The schema rules (`required:`, `x-kubernetes-validations`) are in
//! `rusternetes_common::validation::volumesnapshot`.

pub mod volumesnapshot;
pub mod volumesnapshotclass;
pub mod volumesnapshotcontent;

/// `snapshot.storage.k8s.io`.
pub const GROUP: &str = "snapshot.storage.k8s.io";

/// Whether two values serialize identically — the stand-in for
/// `apiequality.Semantic.DeepEqual` on the non-metadata part of a custom
/// resource (customresource/strategy.go `PrepareForUpdate`).
pub(crate) fn same_json<T: serde::Serialize>(a: &T, b: &T) -> bool {
    serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
}
