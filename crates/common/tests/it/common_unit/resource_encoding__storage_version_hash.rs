//! Unit tests moved from `src/resource_encoding/storage_version_hash.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use base64::Engine;
    pub use rusternetes_common::resource_encoding::storage_codec_versions;
    pub use rusternetes_common::resource_encoding::{
        Example, GroupResource, ResourceEncodingConfig,
    };
    pub use sha2::{Digest, Sha256};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::compatibility::EffectiveVersion;
    use rusternetes_common::resource_encoding::storage_version_hash::*;
    use rusternetes_common::resource_encoding::{
        DefaultResourceEncodingConfig, Scheme, StorageFactoryConfig,
    };

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
            rusternetes_common::compatibility::parse_version(emulation)
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
