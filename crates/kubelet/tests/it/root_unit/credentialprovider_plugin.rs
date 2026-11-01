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

// ---- plugin.go: provide / cache / exec / keyring -------------------------

mod provide {
    use super::*;
    use rusternetes_kubelet::credentialprovider::AuthConfig;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// `fakeExecPlugin` / `countingFakeExecPlugin`.
    struct Fake {
        resp: Result<CredentialProviderResponse, String>,
        calls: Arc<AtomicUsize>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl Plugin for Fake {
        async fn exec_plugin(&self, _image: &str) -> Result<CredentialProviderResponse, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.resp.clone()
        }
    }

    fn response(key: &str, dur: Option<i64>, pattern: &str) -> CredentialProviderResponse {
        CredentialProviderResponse {
            cache_key_type: key.into(),
            cache_duration: dur,
            auth: BTreeMap::from([(
                pattern.to_string(),
                ("user".to_string(), "password".to_string()),
            )]),
        }
    }

    fn cp(match_images: &[&str], default_dur: i64) -> CredentialProvider {
        CredentialProvider {
            name: "test-plugin".into(),
            match_images: match_images.iter().map(|s| s.to_string()).collect(),
            default_cache_duration: Some(default_dur),
            api_version: "credentialprovider.kubelet.k8s.io/v1".into(),
            ..Default::default()
        }
    }

    fn provider(
        resp: Result<CredentialProviderResponse, String>,
        match_images: &[&str],
        default_dur: i64,
    ) -> (PluginProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let p = PluginProvider::with_plugin(
            &cp(match_images, default_dur),
            Box::new(Fake {
                resp,
                calls: calls.clone(),
                delay: Duration::ZERO,
            }),
        );
        (p, calls)
    }

    const MIN: i64 = 60_000_000_000;

    fn user() -> Vec<(String, AuthConfig)> {
        vec![(
            "*.registry.io".to_string(),
            AuthConfig {
                username: "user".into(),
                password: "password".into(),
                email: String::new(),
            },
        )]
    }

    #[tokio::test]
    async fn cache_key_type_decides_the_cache_key() {
        for (key_type, want) in [
            ("Registry", "test.registry.io"),
            ("Image", "test.registry.io/foo/bar"),
            ("Global", "<global>"),
        ] {
            let (p, calls) = provider(
                Ok(response(key_type, Some(MIN), "*.registry.io")),
                &["*.registry.io"],
                0,
            );
            assert_eq!(p.provide("test.registry.io/foo/bar").await.unwrap(), user());
            assert_eq!(p.cache_keys(), vec![want.to_string()], "{key_type}");
            // Second call is served from the cache.
            assert_eq!(p.provide("test.registry.io/foo/bar").await.unwrap(), user());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn images_not_matching_are_skipped_without_exec() {
        let (p, calls) = provider(
            Ok(response("Global", Some(MIN), "x")),
            &["*.registry.io"],
            0,
        );
        assert!(p.provide("other.io/foo").await.unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn zero_duration_response_is_not_cached() {
        // Test_NoCacheResponse.
        let (p, calls) = provider(
            Ok(response("Global", Some(0), "*.registry.io")),
            &["*.registry.io"],
            MIN,
        );
        assert_eq!(p.provide("test.registry.io/foo/bar").await.unwrap(), user());
        assert!(p.cache_keys().is_empty());
        p.provide("test.registry.io/foo/bar").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unset_duration_uses_the_provider_default_and_zero_default_skips_caching() {
        let (p, _) = provider(
            Ok(response("Registry", None, "*.registry.io")),
            &["*.registry.io"],
            MIN,
        );
        p.provide("test.registry.io/x").await.unwrap();
        assert_eq!(p.cache_keys(), vec!["test.registry.io".to_string()]);
        let (p, _) = provider(
            Ok(response("Registry", None, "*.registry.io")),
            &["*.registry.io"],
            0,
        );
        p.provide("test.registry.io/x").await.unwrap();
        assert!(p.cache_keys().is_empty());
    }

    #[tokio::test]
    async fn invalid_cache_key_type_and_exec_errors() {
        let (p, _) = provider(Ok(response("Bogus", Some(MIN), "x")), &["*.registry.io"], 0);
        assert_eq!(
            p.provide("test.registry.io/x").await.unwrap_err(),
            "credential provider plugin did not return a valid cacheKeyType: Bogus"
        );
        let (p, _) = provider(Err("boom".into()), &["*.registry.io"], 0);
        assert_eq!(
            p.provide("test.registry.io/x").await.unwrap_err(),
            "failed to get credential from external registry credential provider: boom"
        );
    }

    #[tokio::test]
    async fn expired_entries_are_misses_and_purged() {
        // Test_getCachedCredentials: expiry is "now after expiresAt"; the
        // purge every 15 minutes drops expired entries.
        let offset = Arc::new(Mutex::new(Duration::ZERO));
        let base = Instant::now();
        let o = offset.clone();
        let clock: Arc<dyn Fn() -> Instant + Send + Sync> =
            Arc::new(move || base + *o.lock().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let p = PluginProvider::with_plugin_and_clock(
            &cp(&["*.registry.io"], MIN),
            Box::new(Fake {
                resp: Ok(response("Image", Some(MIN), "*.registry.io")),
                calls: calls.clone(),
                delay: Duration::ZERO,
            }),
            clock,
        );
        p.provide("a.registry.io/x").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        *offset.lock().unwrap() = Duration::from_secs(30);
        p.provide("a.registry.io/x").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1, "not expired");
        *offset.lock().unwrap() = Duration::from_secs(61);
        p.provide("a.registry.io/x").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2, "expired entry is a miss");
        // Another image's expired entry is deleted by the purge.
        p.provide("b.registry.io/y").await.unwrap();
        assert_eq!(p.cache_keys().len(), 2);
        *offset.lock().unwrap() = Duration::from_secs(61 + 16 * 60);
        p.provide("a.registry.io/x").await.unwrap();
        assert_eq!(p.cache_keys(), vec!["a.registry.io/x".to_string()]);
    }

    #[tokio::test]
    async fn concurrent_provides_share_one_exec() {
        // TestSingleflightProvide.
        let calls = Arc::new(AtomicUsize::new(0));
        let p = Arc::new(PluginProvider::with_plugin(
            &cp(&["test.registry.io"], 0),
            Box::new(Fake {
                resp: Ok(response("Registry", None, "test.registry.io")),
                calls: calls.clone(),
                delay: Duration::from_millis(100),
            }),
        ));
        let mut tasks = Vec::new();
        for _ in 0..5 {
            let p = p.clone();
            tasks.push(tokio::spawn(
                async move { p.provide("test.registry.io").await },
            ));
        }
        for t in tasks {
            assert!(t.await.unwrap().is_ok());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn keyring_collects_provider_credentials_and_hides_errors() {
        let (ok, _) = provider(
            Ok(response("Global", Some(MIN), "test.registry.io")),
            &["test.registry.io"],
            0,
        );
        let (bad, _) = provider(Err("boom".into()), &["test.registry.io"], 0);
        let kr =
            ExternalCredentialProviderKeyring::from_providers(vec![Arc::new(bad), Arc::new(ok)]);
        let got = kr.lookup("test.registry.io/foo").await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].username, "user");
        assert!(kr.lookup("other.io/foo").await.is_empty());
    }

    #[test]
    fn registering_a_name_twice_fails() {
        let (p, _) = provider(Ok(response("Global", None, "x")), &["x"], 0);
        let p = Arc::new(p);
        register_credential_provider_plugin("dup-test", p.clone()).unwrap();
        let e = register_credential_provider_plugin("dup-test", p)
            .unwrap_err()
            .to_string();
        assert_eq!(e, "Credential provider \"dup-test\" was registered twice");
    }

    // ---- execPlugin --------------------------------------------------

    fn script(dir: &std::path::Path, name: &str, body: &str) {
        let f = dir.join(name);
        std::fs::write(&f, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn exec(dir: &std::path::Path, api: &str) -> ExecPlugin {
        ExecPlugin {
            name: "plug".into(),
            api_version: api.into(),
            plugin_bin_dir: dir.into(),
            args: vec![],
            env_vars: vec![],
            timeout: Duration::from_secs(10),
        }
    }

    const V1: &str = "credentialprovider.kubelet.k8s.io/v1";

    #[tokio::test]
    async fn exec_sends_the_request_on_stdin_and_decodes_the_response() {
        let d = tempfile::tempdir().unwrap();
        // Echo the image from the request back as the auth key.
        script(
            d.path(),
            "plug",
            r#"in=$(cat)
img=$(echo "$in" | sed -n 's/.*"image":"\([^"]*\)".*/\1/p')
case "$in" in *'"kind":"CredentialProviderRequest"'*'"apiVersion":"credentialprovider.kubelet.k8s.io/v1"'*|*'"apiVersion":"credentialprovider.kubelet.k8s.io/v1"'*'"kind":"CredentialProviderRequest"'*) ;; *) echo bad request >&2; exit 3;; esac
echo "{\"kind\":\"CredentialProviderResponse\",\"apiVersion\":\"credentialprovider.kubelet.k8s.io/v1\",\"cacheKeyType\":\"Registry\",\"cacheDuration\":\"1h\",\"auth\":{\"$img\":{\"username\":\"u\",\"password\":\"$FOO\"}}}""#,
        );
        let mut e = exec(d.path(), V1);
        e.env_vars = vec![ExecEnvVar {
            name: "FOO".into(),
            value: "bar".into(),
        }];
        let r = e.exec_plugin("reg.io/a").await.unwrap();
        assert_eq!(r.cache_key_type, "Registry");
        assert_eq!(r.cache_duration, Some(3_600_000_000_000));
        assert_eq!(r.auth["reg.io/a"], ("u".to_string(), "bar".to_string()));
    }

    #[tokio::test]
    async fn exec_configured_env_overrides_the_inherited_one() {
        let d = tempfile::tempdir().unwrap();
        script(
            d.path(),
            "plug",
            r#"echo "{\"kind\":\"CredentialProviderResponse\",\"apiVersion\":\"credentialprovider.kubelet.k8s.io/v1\",\"cacheKeyType\":\"Global\",\"auth\":{\"x\":{\"username\":\"u\",\"password\":\"$PATH\"}}}""#,
        );
        let mut e = exec(d.path(), V1);
        e.env_vars = vec![ExecEnvVar {
            name: "PATH".into(),
            value: "/usr/bin:/bin".into(),
        }];
        let r = e.exec_plugin("x").await.unwrap();
        assert_eq!(r.auth["x"].1, "/usr/bin:/bin");
    }

    #[tokio::test]
    async fn exec_error_cases_match_upstream_messages() {
        let d = tempfile::tempdir().unwrap();
        script(d.path(), "plug", "echo oops >&2; exit 2");
        let err = exec(d.path(), V1).exec_plugin("img").await.unwrap_err();
        assert_eq!(
            err,
            "error execing credential provider plugin plug for image img: exit status 2: oops\n"
        );

        script(
            d.path(),
            "plug",
            r#"echo '{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1beta1","cacheKeyType":"Global"}'"#,
        );
        let err = exec(d.path(), V1).exec_plugin("img").await.unwrap_err();
        assert_eq!(err, "apiVersion from credential plugin response did not match expected apiVersion:credentialprovider.kubelet.k8s.io/v1, actual apiVersion:credentialprovider.kubelet.k8s.io/v1beta1");

        script(
            d.path(),
            "plug",
            r#"echo '{"kind":"WrongKind","apiVersion":"credentialprovider.kubelet.k8s.io/v1"}'"#,
        );
        let err = exec(d.path(), V1).exec_plugin("img").await.unwrap_err();
        assert_eq!(
            err,
            "error decoding credential provider plugin response from stdout"
        );

        script(d.path(), "plug", "echo not json");
        let err = exec(d.path(), V1).exec_plugin("img").await.unwrap_err();
        assert!(err.starts_with("error reading GVK from response"), "{err}");
    }

    #[tokio::test]
    async fn exec_times_out() {
        let d = tempfile::tempdir().unwrap();
        script(d.path(), "plug", "sleep 30");
        let mut e = exec(d.path(), V1);
        e.timeout = Duration::from_millis(200);
        let err = e.exec_plugin("img").await.unwrap_err();
        assert!(err.contains("context deadline exceeded"), "{err}");
    }
}
