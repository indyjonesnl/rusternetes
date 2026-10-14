//! Ports of `TestStorageFactoryCompatibilityVersion`
//! (storage_factory_test.go:421-570) plus checks against the release-1.35
//! scheme table.

use super::*;
use crate::compatibility::EffectiveVersion;
use crate::feature_gates::Version;

fn ev(version: &str) -> EffectiveVersion {
    EffectiveVersion::from_string(version, "", "").unwrap()
}

/// `registerFakeLifecycle` calls, in the upstream test's priority order.
fn fake_scheme() -> Scheme {
    let mut s = Scheme::new();
    let fc = "flowcontrol.apiserver.k8s.io";
    s.add_known_type_with_name(fc, "v1", "FlowSchema", Some((1, 29)));
    s.add_known_type_with_name(fc, "v1beta3", "FlowSchema", Some((1, 26)));
    s.add_known_type_with_name(fc, "v1beta2", "FlowSchema", Some((1, 23)));
    s.add_known_type_with_name(fc, "v1beta1", "FlowSchema", Some((1, 20)));
    s.add_known_type_with_name("batch", "v1", "CronJob", Some((1, 21)));
    s.add_known_type_with_name("batch", "v1beta1", "CronJob", Some((1, 8)));
    let ar = "admissionregistration.k8s.io";
    s.add_known_type_with_name(ar, "v1", "ValidatingAdmisisonPolicy", Some((1, 30)));
    s.add_known_type_with_name(ar, "v1beta1", "ValidatingAdmisisonPolicy", Some((1, 28)));
    s.add_known_type_with_name("", "v1", "Pod", Some((1, 31)));
    s
}

#[test]
fn storage_factory_compatibility_version() {
    let fc = "flowcontrol.apiserver.k8s.io";
    let ar = "admissionregistration.k8s.io";
    let vap = "ValidatingAdmisisonPolicy";
    // (effective version, group, kind, expected version)
    let cases = [
        // Basic case. Beta version for long time
        ("1.14.0", "batch", "CronJob", "v1beta1"),
        ("1.20.0", "batch", "CronJob", "v1beta1"),
        // Basic case. GA version for long time
        ("1.28.0", "batch", "CronJob", "v1"),
        // Basic core/v1
        ("1.31.0", "", "Pod", "v1"),
        // Corner case: 1.1.0 has no flowcontrol. Options are to error out or
        // to use the latest version. This test assumes the latter.
        ("1.1.0", fc, "FlowSchema", "v1"),
        ("1.21.0", fc, "FlowSchema", "v1beta1"),
        // v2Beta1 introduced this version, but minCompatibility should force v1beta1
        ("1.23.0", fc, "FlowSchema", "v1beta1"),
        ("1.24.0", fc, "FlowSchema", "v1beta2"),
        ("1.26.0", fc, "FlowSchema", "v1beta2"),
        ("1.27.0", fc, "FlowSchema", "v1beta3"),
        // GA API introduced 1.29 but must keep storing in v1beta3 for downgrades
        ("1.29.0", fc, "FlowSchema", "v1beta3"),
        // Version after GA api is introduced
        ("1.30.0", fc, "FlowSchema", "v1"),
        ("1.30.0", ar, vap, "v1beta1"),
        ("1.31.0", ar, vap, "v1"),
        ("1.29.0", ar, vap, "v1beta1"),
    ];
    for (version, group, kind, expected) in cases {
        let config = DefaultResourceEncodingConfig::new_for_effective_version(
            fake_scheme(),
            Some(ev(version)),
        );
        // resource name "doesnt really matter here" upstream
        let gr = GroupResource::new(group, kind);
        let got = storage_codec_versions(&config, &gr, &gr, Some(&Example::new(group, kind)))
            .unwrap_or_else(|e| panic!("{group}/{kind}@{version}: {e}"));
        assert_eq!(
            got.storage_version,
            GroupVersion::new(group, expected),
            "{group}/{kind}@{version}"
        );
        assert_eq!(
            got.memory_version,
            GroupVersion::new(group, API_VERSION_INTERNAL)
        );
    }
}

#[test]
fn overrides_win_over_emulated_version() {
    // "Always respect overrides" (:95-99)
    let mut config =
        DefaultResourceEncodingConfig::new_for_effective_version(fake_scheme(), Some(ev("1.14.0")));
    let gr = GroupResource::new("batch", "cronjobs");
    config.set_resource_encoding(
        gr.clone(),
        GroupVersion::new("batch", "v1"),
        GroupVersion::new("batch", "internalx"),
    );
    let got = config
        .backward_compatibile_storage_encoding_for(&gr, Some(&Example::new("batch", "CronJob")))
        .unwrap();
    assert_eq!(got, GroupVersion::new("batch", "v1"));
    assert_eq!(
        config.in_memory_encoding_for(&gr).unwrap(),
        GroupVersion::new("batch", "internalx")
    );
}

#[test]
fn unregistered_group_errors() {
    let config = DefaultResourceEncodingConfig::new_for_effective_version(fake_scheme(), None);
    let gr = GroupResource::new("nope.k8s.io", "things");
    let want = "group \"nope.k8s.io\" is not registered in scheme";
    assert_eq!(config.storage_encoding_for(&gr).unwrap_err(), want);
    assert_eq!(config.in_memory_encoding_for(&gr).unwrap_err(), want);
    assert_eq!(
        config
            .backward_compatible_storage_encoding_for(&gr, None)
            .unwrap()
            .unwrap_err(),
        want
    );
}

#[test]
fn nil_example_or_effective_version_uses_binary_version() {
    // :126-128
    let gr = GroupResource::new("batch", "cronjobs");
    let config =
        DefaultResourceEncodingConfig::new_for_effective_version(fake_scheme(), Some(ev("1.14.0")));
    assert_eq!(
        config
            .backward_compatibile_storage_encoding_for(&gr, None)
            .unwrap(),
        GroupVersion::new("batch", "v1")
    );
    let config = DefaultResourceEncodingConfig::new_for_effective_version(fake_scheme(), None);
    assert_eq!(
        config
            .backward_compatibile_storage_encoding_for(&gr, Some(&Example::new("batch", "CronJob")))
            .unwrap(),
        GroupVersion::new("batch", "v1")
    );
}

#[test]
fn example_without_registered_kind_errors() {
    let config =
        DefaultResourceEncodingConfig::new_for_effective_version(fake_scheme(), Some(ev("1.30.0")));
    let gr = GroupResource::new("batch", "widgets");
    let err = config
        .backward_compatibile_storage_encoding_for(&gr, Some(&Example::new("batch", "Widget")))
        .unwrap_err();
    assert!(err.contains("no kind is registered"), "{err}");
}

#[test]
fn zero_compat_version_skips_the_introduced_check() {
    // :177 "skip the introduced check for test when current compatibility
    // version is 0.0 to test all apis": min-compat 0.0 picks the top version.
    let mut e = ev("1.14.0");
    e.set_min_compatibility_version(Version::major_minor(0, 0));
    let config = DefaultResourceEncodingConfig::new_for_effective_version(fake_scheme(), Some(e));
    let gr = GroupResource::new("batch", "cronjobs");
    assert_eq!(
        config
            .backward_compatibile_storage_encoding_for(&gr, Some(&Example::new("batch", "CronJob")))
            .unwrap(),
        GroupVersion::new("batch", "v1")
    );
}

// ---- release-1.35 scheme ----

fn parse_mm(s: &str) -> Version {
    crate::compatibility::parse_version(s)
        .unwrap()
        .major_minor()
}

/// Storage version for a binary-1.35 server emulating `emulation`
/// (min-compat defaults to emulation - 1).
fn legacy_storage(emulation: &str, group: &str, resource: &str, kind: &str) -> GroupVersion {
    let mut e = ev("1.35.0");
    e.set_emulation_version(parse_mm(emulation));
    let config =
        DefaultResourceEncodingConfig::new_for_effective_version(Scheme::legacy(), Some(e));
    config
        .backward_compatibile_storage_encoding_for(
            &GroupResource::new(group, resource),
            Some(&Example::new(group, kind)),
        )
        .unwrap()
}

#[test]
fn legacy_scheme_default_version_is_ga() {
    // default effective version: emulation 1.35, min-compat 1.34
    assert_eq!(
        legacy_storage(
            "1.35",
            "autoscaling",
            "horizontalpodautoscalers",
            "HorizontalPodAutoscaler"
        ),
        GroupVersion::new("autoscaling", "v2")
    );
    assert_eq!(
        legacy_storage("1.35", "", "pods", "Pod"),
        GroupVersion::new("", "v1")
    );
    assert_eq!(
        legacy_storage(
            "1.35",
            "certificates.k8s.io",
            "certificatesigningrequests",
            "CertificateSigningRequest"
        ),
        GroupVersion::new("certificates.k8s.io", "v1")
    );
}

#[test]
fn legacy_scheme_skips_versions_introduced_after_min_compat() {
    // ClusterTrustBundle v1beta1 is introduced 1.33; with min-compat 1.32 the
    // highest version it existed in is v1alpha1.
    assert_eq!(
        legacy_storage(
            "1.33",
            "certificates.k8s.io",
            "clustertrustbundles",
            "ClusterTrustBundle"
        ),
        GroupVersion::new("certificates.k8s.io", "v1alpha1")
    );
    // ...and with min-compat 1.33 it is v1beta1.
    assert_eq!(
        legacy_storage(
            "1.34",
            "certificates.k8s.io",
            "clustertrustbundles",
            "ClusterTrustBundle"
        ),
        GroupVersion::new("certificates.k8s.io", "v1beta1")
    );
    // resource.k8s.io v1 is introduced 1.34: min-compat 1.33 stores v1beta2.
    assert_eq!(
        legacy_storage("1.34", "resource.k8s.io", "resourceclaims", "ResourceClaim"),
        GroupVersion::new("resource.k8s.io", "v1beta2")
    );
    assert_eq!(
        legacy_storage("1.35", "resource.k8s.io", "resourceclaims", "ResourceClaim"),
        GroupVersion::new("resource.k8s.io", "v1")
    );
}

#[test]
fn storage_factory_config_overrides_pin_pre_ga_versions() {
    // default_storage_factory_builder.go:66-98
    let cfg = StorageFactoryConfig::new_effective_version(ev("1.35.0"));
    let config = cfg.resource_encoding_config();
    for (group, resource, kind, version) in [
        (
            "coordination.k8s.io",
            "leasecandidates",
            "LeaseCandidate",
            "v1beta1",
        ),
        (
            "certificates.k8s.io",
            "clustertrustbundles",
            "ClusterTrustBundle",
            "v1beta1",
        ),
        (
            "certificates.k8s.io",
            "podcertificaterequests",
            "PodCertificateRequest",
            "v1beta1",
        ),
        (
            "storage.k8s.io",
            "volumeattributesclasses",
            "VolumeAttributesClass",
            "v1beta1",
        ),
    ] {
        let gr = GroupResource::new(group, resource);
        let got =
            storage_codec_versions(&config, &gr, &gr, Some(&Example::new(group, kind))).unwrap();
        assert_eq!(
            got.storage_version,
            GroupVersion::new(group, version),
            "{resource}"
        );
        assert_eq!(
            got.memory_version,
            GroupVersion::new(group, API_VERSION_INTERNAL)
        );
    }
    // A resource with no override follows the emulated version.
    let gr = GroupResource::new("apps", "deployments");
    let got = storage_codec_versions(&config, &gr, &gr, Some(&Example::new("apps", "Deployment")))
        .unwrap();
    assert_eq!(got.storage_version, GroupVersion::new("apps", "v1"));
}
