//! Ports of `pkg/credentialprovider/plugin/config_test.go`
//! (`Test_readCredentialProviderConfig`, `Test_validateCredentialProviderConfig`)
//! and the checks of `RegisterCredentialProviderPlugins`.
use rusternetes_kubelet::credentialprovider_plugin::*;
use std::os::unix::fs::PermissionsExt;

const V1ALPHA1: &str = r#"---
kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1alpha1
providers:
  - name: test
    matchImages:
    - "registry.io/foobar"
    defaultCacheDuration: 10m
    apiVersion: credentialprovider.kubelet.k8s.io/v1alpha1
    args:
    - --v=5
    env:
    - name: FOO
      value: BAR"#;

fn read_one(data: &str) -> anyhow::Result<(CredentialProviderConfig, String)> {
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("config.yaml");
    std::fs::write(&f, data).unwrap();
    read_credential_provider_config(f.to_str().unwrap())
}

#[test]
fn reads_one_provider_and_hashes_like_upstream() {
    let (cfg, hash) = read_one(V1ALPHA1).unwrap();
    assert_eq!(cfg.providers.len(), 1);
    let p = &cfg.providers[0];
    assert_eq!(p.name, "test");
    assert_eq!(p.match_images, vec!["registry.io/foobar"]);
    assert_eq!(p.default_cache_duration, Some(600_000_000_000));
    assert_eq!(p.args, vec!["--v=5"]);
    assert_eq!(
        (p.env[0].name.as_str(), p.env[0].value.as_str()),
        ("FOO", "BAR")
    );
    // expectedConfigHash of "config with 1 plugin and 1 image matcher".
    assert_eq!(
        hash,
        "sha256:8a62755289ba50c4ca9495baab69eb861068503f8bd49b853e8ba6cf95c72bb8"
    );
}

#[test]
fn decode_errors() {
    let wrong_kind = V1ALPHA1.replace("CredentialProviderConfig", "WrongKind");
    let e = read_one(&wrong_kind).unwrap_err().to_string();
    assert!(
        e.contains(
            r#"no kind "WrongKind" is registered for version "kubelet.config.k8s.io/v1alpha1""#
        ),
        "{e}"
    );

    let wrong_ver = V1ALPHA1.replace("kubelet.config.k8s.io/v1alpha1", "foobar/v1alpha1");
    let e = read_one(&wrong_ver).unwrap_err().to_string();
    assert!(
        e.contains(
            r#"no kind "CredentialProviderConfig" is registered for version "foobar/v1alpha1"#
        ),
        "{e}"
    );

    let typo = V1ALPHA1.replace("    args:", "    unknownField: x\n    args:");
    let e = read_one(&typo).unwrap_err().to_string();
    assert!(
        e.contains(r#"strict decoding error: unknown field "providers[0].unknownField""#),
        "{e}"
    );

    // tokenAttributes exists only in the v1 config type.
    let tok = |ver: &str| {
        V1ALPHA1.replace("v1alpha1\nproviders", &format!("{ver}\nproviders")).replace(
            "    args:",
            "    tokenAttributes:\n      serviceAccountTokenAudience: a\n      cacheType: Token\n      requireServiceAccount: true\n    args:",
        )
    };
    for v in ["v1alpha1", "v1beta1"] {
        let e = read_one(&tok(v)).unwrap_err().to_string();
        assert!(
            e.contains(r#"strict decoding error: unknown field "providers[0].tokenAttributes""#),
            "{v}: {e}"
        );
    }
    assert!(read_one(&tok("v1")).is_ok());
}

#[test]
fn directory_is_merged_in_lexical_order_and_filters_extensions() {
    let d = tempfile::tempdir().unwrap();
    let one = |name: &str| V1ALPHA1.replace("name: test", &format!("name: {name}"));
    std::fs::write(d.path().join("b.yaml"), one("second")).unwrap();
    std::fs::write(
        d.path().join("a.json"),
        serde_json::to_string(&serde_yaml::from_str::<serde_yaml::Value>(&one("first")).unwrap())
            .unwrap(),
    )
    .unwrap();
    std::fs::write(d.path().join("c.txt"), "not config").unwrap();
    let (cfg, hash) = read_credential_provider_config(d.path().to_str().unwrap()).unwrap();
    let names: Vec<_> = cfg.providers.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["first", "second"]);
    assert!(hash.starts_with("sha256:"));

    // Duplicate names across files.
    std::fs::write(d.path().join("c.yml"), one("first")).unwrap();
    let e = read_credential_provider_config(d.path().to_str().unwrap())
        .unwrap_err()
        .to_string();
    assert!(
        e.contains(r#"duplicate provider name "first" found in configuration file(s)"#),
        "{e}"
    );

    let empty = tempfile::tempdir().unwrap();
    std::fs::write(empty.path().join("x.txt"), "x").unwrap();
    let e = read_credential_provider_config(empty.path().to_str().unwrap())
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("no configuration files found in directory"),
        "{e}"
    );
}

fn provider() -> CredentialProvider {
    CredentialProvider {
        name: "foobar".into(),
        match_images: vec!["foobar.registry.io".into()],
        default_cache_duration: Some(60_000_000_000),
        api_version: "credentialprovider.kubelet.k8s.io/v1beta1".into(),
        ..Default::default()
    }
}

fn errs(p: Vec<CredentialProvider>, gate: bool) -> String {
    validate_credential_provider_config(&CredentialProviderConfig { providers: p }, gate)
        .join(" | ")
}

#[test]
fn validation_messages_match_upstream() {
    assert_eq!(errs(vec![provider()], false), "");
    assert_eq!(
        errs(vec![], false),
        "providers: Required value: at least 1 item in plugins is required"
    );
    let with = |f: fn(&mut CredentialProvider)| {
        let mut p = provider();
        f(&mut p);
        errs(vec![p], false)
    };
    assert_eq!(
        with(|p| p.match_images.clear()),
        "providers.matchImages: Required value: at least 1 item in matchImages is required"
    );
    assert_eq!(
        with(|p| p.default_cache_duration = None),
        "providers.defaultCacheDuration: Required value"
    );
    assert_eq!(
        with(|p| p.name = "foo/../bar".into()),
        r#"providers.name: Invalid value: "foo/../bar": provider name cannot contain '/'"#
    );
    assert_eq!(
        with(|p| p.name = ".".into()),
        r#"providers.name: Invalid value: ".": provider name cannot be '.'"#
    );
    assert_eq!(
        with(|p| p.name = "..".into()),
        r#"providers.name: Invalid value: "..": provider name cannot be '..'"#
    );
    assert_eq!(
        with(|p| p.name = "foo bar".into()),
        r#"providers.name: Invalid value: "foo bar": provider name cannot contain spaces"#
    );
    assert_eq!(
        with(|p| p.api_version.clear()),
        "providers.apiVersion: Required value"
    );
    assert_eq!(
        with(|p| p.api_version = "credentialprovider.kubelet.k8s.io/v1alpha0".into()),
        r#"providers.apiVersion: Unsupported value: "credentialprovider.kubelet.k8s.io/v1alpha0": supported values: "credentialprovider.kubelet.k8s.io/v1", "credentialprovider.kubelet.k8s.io/v1alpha1", "credentialprovider.kubelet.k8s.io/v1beta1""#
    );
    assert_eq!(
        with(|p| p.default_cache_duration = Some(-60_000_000_000)),
        r#"providers.defaultCacheDuration: Invalid value: "-1m0s": must be greater than or equal to 0"#
    );
    assert_eq!(
        with(|p| p.match_images = vec!["%invalid%".into()]),
        r#"providers.matchImages: Invalid value: "%invalid%": match image is invalid: parse "https://%invalid%": invalid URL escape "%in""#
    );
    assert_eq!(
        errs(vec![provider(), provider()], false),
        r#"providers.name: Duplicate value: "foobar""#
    );
}

fn token(f: impl FnOnce(&mut ServiceAccountTokenAttributes)) -> CredentialProvider {
    let mut t = ServiceAccountTokenAttributes {
        service_account_token_audience: "audience".into(),
        cache_type: "Token".into(),
        require_service_account: Some(true),
        ..Default::default()
    };
    f(&mut t);
    let mut p = provider();
    p.api_version = "credentialprovider.kubelet.k8s.io/v1".into();
    p.token_attributes = Some(t);
    p
}

#[test]
fn token_attribute_validation_messages_match_upstream() {
    assert_eq!(errs(vec![token(|_| {})], true), "");
    assert_eq!(errs(vec![token(|_| {})], false), "providers.tokenAttributes: Forbidden: tokenAttributes is not supported when KubeletServiceAccountTokenForCredentialProviders feature gate is disabled");
    assert_eq!(
        errs(
            vec![token(|t| t.service_account_token_audience.clear())],
            true
        ),
        "providers.tokenAttributes.serviceAccountTokenAudience: Required value"
    );
    assert_eq!(errs(vec![token(|t| t.cache_type.clear())], true), "providers.tokenAttributes.cacheType: Required value: cacheType is required to be set when tokenAttributes is specified. Supported values are: ServiceAccount, Token");
    assert_eq!(
        errs(
            vec![token(|t| t.cache_type = "invalid-cache-type".into())],
            true
        ),
        r#"providers.tokenAttributes.cacheType: Unsupported value: "invalid-cache-type": supported values: "ServiceAccount", "Token""#
    );
    assert_eq!(
        errs(vec![token(|t| t.require_service_account = None)], true),
        "providers.tokenAttributes.requireServiceAccount: Required value"
    );
    assert_eq!(
        errs(
            vec![token(
                |t| t.required_service_account_annotation_keys = vec![
                    "now-with-dashes/simple".into(),
                    "now-with-dashes/simple".into()
                ]
            )],
            true
        ),
        r#"providers.tokenAttributes.requiredServiceAccountAnnotationKeys: Duplicate value: "now-with-dashes/simple""#
    );
    assert_eq!(
        errs(
            vec![token(|t| {
                t.required_service_account_annotation_keys =
                    vec!["now-with-dashes/simple-2".into()];
                t.optional_service_account_annotation_keys =
                    vec!["now-with-dashes/simple-2".into()];
            })],
            true
        ),
        r#"providers.tokenAttributes: Invalid value: ["now-with-dashes/simple-2"]: annotation keys cannot be both required and optional"#
    );
    assert_eq!(
        errs(vec![token(|t| {
            t.require_service_account = Some(false);
            t.required_service_account_annotation_keys = vec!["now-with-dashes/simple".into()];
        })], true),
        "providers.tokenAttributes.requiredServiceAccountAnnotationKeys: Forbidden: requireServiceAccount cannot be false when requiredServiceAccountAnnotationKeys is set"
    );
    let mut p = token(|_| {});
    p.api_version = "credentialprovider.kubelet.k8s.io/v1alpha1".into();
    assert_eq!(errs(vec![p], true), "providers.tokenAttributes: Forbidden: tokenAttributes is only supported for credentialprovider.kubelet.k8s.io/v1 API version");
    let bad = errs(
        vec![token(|t| {
            t.required_service_account_annotation_keys = vec!["cantendwithadash-".into()]
        })],
        true,
    );
    assert!(bad.starts_with(r#"providers.tokenAttributes.requiredServiceAccountAnnotationKeys: Invalid value: "cantendwithadash-": name part must consist of"#), "{bad}");
}

#[test]
fn registration_checks_bin_dir_validation_and_binaries() {
    let dir = tempfile::tempdir().unwrap();
    let bin = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("c.yaml");
    std::fs::write(&cfg, V1ALPHA1).unwrap();
    let cfg = cfg.to_str().unwrap();
    let bin_dir = bin.path().to_str().unwrap();

    let e = register_credential_provider_plugins(cfg, "/nonexistent-bin-dir", true)
        .unwrap_err()
        .to_string();
    assert_eq!(
        e,
        "plugin binary directory /nonexistent-bin-dir did not exist"
    );

    let e = register_credential_provider_plugins(cfg, bin_dir, true)
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("plugin binary executable") && e.contains("did not exist"),
        "{e}"
    );

    let exe = bin.path().join("test");
    std::fs::write(&exe, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (c, hash) = register_credential_provider_plugins(cfg, bin_dir, true).unwrap();
    assert_eq!(c.providers.len(), 1);
    assert!(hash.starts_with("sha256:"));

    // Validation failures are wrapped like RegisterCredentialProviderPlugins.
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, V1ALPHA1.replace("name: test", "name: a/b")).unwrap();
    let e = register_credential_provider_plugins(bad.to_str().unwrap(), bin_dir, true)
        .unwrap_err()
        .to_string();
    assert!(
        e.starts_with(
            "failed to validate credential provider config: providers.name: Invalid value: \"a/b\""
        ),
        "{e}"
    );
}
