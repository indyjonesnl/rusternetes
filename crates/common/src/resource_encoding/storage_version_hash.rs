//! Discovery `storageVersionHash` (#3500).
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/endpoints/discovery/storageversionhash.go:28-36`
//!   (`StorageVersionHash`)
//! * `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:488-498` (the
//!   hash is set only when the `StorageVersionHash` gate is on and the storage
//!   is a `rest.StorageVersionProvider`, from the storage versioner's GVK)

use super::{Example, GroupResource, ResourceEncodingConfig};
use crate::resource_encoding::storage_codec_versions;
use base64::Engine;
use sha2::{Digest, Sha256};

/// `discovery.StorageVersionHash`: the first 8 bytes of the SHA-256 of
/// `group/version/kind`, standard base64.
pub fn storage_version_hash(group: &str, version: &str, kind: &str) -> String {
    let gvk = format!("{group}/{version}/{kind}");
    let digest = Sha256::digest(gvk.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(&digest[..8])
}

/// Only `genericregistry.Store` implements `rest.StorageVersionProvider`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:1694).
/// The top-level resources served by a non-Store `rest.Storage` never get a
/// hash: pkg/registry/authentication/{tokenreview,selfsubjectreview},
/// pkg/registry/authorization/{subjectaccessreview,localsubjectaccessreview,
/// selfsubjectaccessreview,selfsubjectrulesreview}, the core
/// `componentstatuses` (pkg/registry/core/componentstatus/rest.go) and
/// `bindings` (pkg/registry/core/rest/storage_core.go:253, `LegacyBindingREST`).
fn is_store_backed(r: &GroupResource) -> bool {
    !matches!(
        (r.group.as_str(), r.resource.as_str()),
        (
            "authentication.k8s.io",
            "tokenreviews" | "selfsubjectreviews"
        ) | (
            "authorization.k8s.io",
            "subjectaccessreviews"
                | "localsubjectaccessreviews"
                | "selfsubjectaccessreviews"
                | "selfsubjectrulesreviews"
        ) | ("", "componentstatuses" | "bindings")
    )
}

/// The hash `installer.go:488-498` stamps on a top-level resource: the GVK of
/// the storage codec's `EncodeVersioner` (`DefaultStorageFactory.NewConfig`,
/// storage_factory.go:247-263), i.e. the *storage* version, not the served
/// one. `None` when the resource is not storage-backed in the scheme
/// (`isStorageVersionProvider` false) or the version cannot be resolved.
pub fn discovery_storage_version_hash(
    config: &dyn ResourceEncodingConfig,
    resource: &GroupResource,
    kind: &str,
) -> Option<String> {
    if !is_store_backed(resource) {
        return None;
    }
    let example = Example::new(&resource.group, kind);
    let versions = storage_codec_versions(config, resource, resource, Some(&example)).ok()?;
    let gv = versions.storage_version;
    Some(storage_version_hash(&gv.group, &gv.version, kind))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compatibility::EffectiveVersion;
    use crate::resource_encoding::{DefaultResourceEncodingConfig, Scheme, StorageFactoryConfig};

    #[test]
    fn hash_known_vectors() {
        // Values served by a real kube-apiserver in /api/v1 and /apis/apps/v1.
        assert_eq!(storage_version_hash("", "v1", "Pod"), "xPOwRZ+Yhw8=");
        assert_eq!(
            storage_version_hash("apps", "v1", "Deployment"),
            "8aSe+NMegvE="
        );
    }

    fn config(emulation: &str) -> DefaultResourceEncodingConfig {
        let mut e = EffectiveVersion::from_string("1.35.0", "", "").unwrap();
        e.set_emulation_version(
            crate::compatibility::parse_version(emulation)
                .unwrap()
                .major_minor(),
        );
        StorageFactoryConfig::new_effective_version(e).resource_encoding_config()
    }

    #[test]
    fn discovery_hash_uses_storage_version() {
        let c = config("1.35");
        let h = discovery_storage_version_hash(
            &c,
            &GroupResource::new("autoscaling", "horizontalpodautoscalers"),
            "HorizontalPodAutoscaler",
        );
        // HPA is stored in autoscaling/v2, not v1.
        assert_eq!(
            h,
            Some(storage_version_hash(
                "autoscaling",
                "v2",
                "HorizontalPodAutoscaler"
            ))
        );
    }

    #[test]
    fn virtual_resources_have_no_hash() {
        let c = config("1.35");
        for (g, r, k) in [
            ("authentication.k8s.io", "tokenreviews", "TokenReview"),
            (
                "authorization.k8s.io",
                "subjectaccessreviews",
                "SubjectAccessReview",
            ),
            (
                "authorization.k8s.io",
                "selfsubjectrulesreviews",
                "SelfSubjectRulesReview",
            ),
            ("", "componentstatuses", "ComponentStatus"),
            ("", "bindings", "Binding"),
        ] {
            assert_eq!(
                discovery_storage_version_hash(&c, &GroupResource::new(g, r), k),
                None,
                "{g}/{r}"
            );
        }
    }

    #[test]
    fn unknown_group_has_no_hash() {
        let c = DefaultResourceEncodingConfig::new_for_effective_version(Scheme::new(), None);
        assert_eq!(
            discovery_storage_version_hash(&c, &GroupResource::new("nope.io", "x"), "X"),
            None
        );
    }
}
