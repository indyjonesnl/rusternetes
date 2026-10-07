//! `HasMetadata`: the one accessor trait every stored resource implements.
//!
//! Upstream gets this from `meta.Accessor`
//! (`staging/src/k8s.io/apimachinery/pkg/api/meta/meta.go`), which every
//! `runtime.Object` satisfies. The registry `Object` bound, the finalizer and
//! delete helpers and the watch paths all read and write `ObjectMeta` through
//! this single trait, re-exported as `registry::rest::HasMetadata`.

/// Trait for resources that have metadata with finalizers.
/// This allows the generic registry and watch helpers to work with any
/// Kubernetes resource type.
pub trait HasMetadata {
    /// Get an immutable reference to the resource's metadata
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta;

    /// Get a mutable reference to the resource's metadata
    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta;
}

// Implement HasMetadata for common resource types
// Note: This can be extended with a macro if needed for many types

impl HasMetadata for rusternetes_common::resources::Namespace {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Pod {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Scale {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Deployment {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Service {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ConfigMap {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Secret {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ServiceAccount {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicaSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::DaemonSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::StatefulSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Job {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CronJob {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PersistentVolume {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PersistentVolumeClaim {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::StorageClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Ingress {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::IngressClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::NetworkPolicy {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ResourceQuota {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::LimitRange {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Eviction {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PodDisruptionBudget {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::HorizontalPodAutoscaler {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Node {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshot {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshotClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshotContent {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSIDriver {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSINode {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSIStorageCapacity {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeAttachment {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeAttributesClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingWebhookConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::MutatingWebhookConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingAdmissionPolicy {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingAdmissionPolicyBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterTrustBundle {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CertificateSigningRequest {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::FlowSchema {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PriorityLevelConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

// DRA resources. These used to be excluded here — they declared their own
// `dra::ObjectMeta`, so neither `HasMetadata` nor the shared finalizer and
// metadata-inheritance helpers typechecked against them, and DRA silently had
// no finalizer support and no server-owned-field reinstatement (#1895). They
// now use `types::ObjectMeta` like every other resource, so the generic rules
// reach them by construction rather than by a per-resource copy.

impl HasMetadata for rusternetes_common::resources::dra::ResourceClaim {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::dra::ResourceClaimTemplate {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::dra::DeviceClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::dra::ResourceSlice {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Role {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::RoleBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterRole {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterRoleBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PodTemplate {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ControllerRevision {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ServiceCIDR {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::IPAddress {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CustomResourceDefinition {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Event {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::APIService {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PriorityClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Lease {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::RuntimeClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::EndpointSlice {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Endpoints {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicationController {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CustomResource {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

// Was only in the watch module's duplicate trait before the two were collapsed.
impl HasMetadata for rusternetes_common::resources::EventV1 {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::HasMetadata;

    fn assert_has_metadata<T: HasMetadata>() {}

    /// Every type the watch paths and the Store serve goes through this one
    /// trait; the watch module used to carry its own copy with its own list.
    #[test]
    fn watch_and_store_types_share_one_trait() {
        use rusternetes_common::resources::*;
        assert_has_metadata::<Pod>();
        assert_has_metadata::<Event>();
        assert_has_metadata::<EventV1>();
        assert_has_metadata::<Endpoints>();
        assert_has_metadata::<EndpointSlice>();
        assert_has_metadata::<Namespace>();
        assert_has_metadata::<CustomResource>();
    }

    /// The accessors read and write the object's own metadata (the accessor
    /// test dropped with `finalizers_test.rs`, #2156).
    #[test]
    fn accessors_read_and_write_the_objects_metadata() {
        let mut pod = rusternetes_common::resources::Pod::new("a", Default::default());
        pod.metadata.name = "a".to_string();
        assert_eq!(HasMetadata::metadata(&pod).name, "a");
        HasMetadata::metadata_mut(&mut pod).finalizers = Some(vec!["f".to_string()]);
        assert_eq!(pod.metadata.finalizers, Some(vec!["f".to_string()]));
    }
}
