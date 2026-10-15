//! Discovery `storageVersionHash` (#3500).
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/endpoints/discovery/storageversionhash.go:28-36`
//!   (`StorageVersionHash`)
//! * `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:488-498` (the
//!   hash is set only when the `StorageVersionHash` gate is on and the storage
//!   is a `rest.StorageVersionProvider`, from the storage versioner's GVK)

use super::{GroupResource, ResourceEncodingConfig};

/// `discovery.StorageVersionHash`.
pub fn storage_version_hash(_group: &str, _version: &str, _kind: &str) -> String {
    String::new()
}

/// The hash `installer.go:488-498` stamps on a top-level resource.
pub fn discovery_storage_version_hash(
    _config: &dyn ResourceEncodingConfig,
    _resource: &GroupResource,
    _kind: &str,
) -> Option<String> {
    None
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
            ("", "componentstatuses", "ComponentStatus"),
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
