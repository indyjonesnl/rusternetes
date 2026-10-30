//! Unit tests moved from `src/admission/image_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::admission::builtin_plugins::{attr_name, new_forbidden};
    pub use rusternetes_api_server::authorization_webhook::{
        is_connection_reset, WebhookClientConfig,
    };
    pub use rusternetes_common::lru_expire_cache::LruExpireCache;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_common::wait::Backoff;
    pub use rusternetes_common::Error;
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::BTreeMap;
    pub use std::path::Path;
    pub use std::sync::{Arc, OnceLock};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::image_policy::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    async fn run(
        wh: &ImagePolicyWebhook,
        pod: &Pod,
        ns: &str,
        sub: Option<&str>,
    ) -> Result<Decision, Error> {
        let mut d = wh.validate(pod, ns, sub).await;
        match d.error.take() {
            Some(e) => Err(e),
            None => Ok(d),
        }
    }

    // config_test.go TestConfigNormalization.
    fn cfg(allow: i64, deny: i64, retry: i64) -> ImagePolicyWebhookConfig {
        ImagePolicyWebhookConfig {
            allow_ttl: allow,
            deny_ttl: deny,
            retry_backoff: retry,
            ..Default::default()
        }
    }

    #[test]
    fn config_within_normal_ranges() {
        let mut c = cfg(
            (MIN_ALLOW_TTL + MAX_ALLOW_TTL) / 2 / SEC,
            (MIN_DENY_TTL + MAX_DENY_TTL) / 2 / SEC,
            (MIN_RETRY_BACKOFF + MAX_RETRY_BACKOFF) / 2 / MS,
        );
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(c.allow_ttl, (MIN_ALLOW_TTL + MAX_ALLOW_TTL) / 2 / SEC * SEC);
        assert_eq!(c.deny_ttl, (MIN_DENY_TTL + MAX_DENY_TTL) / 2 / SEC * SEC);
        assert_eq!(
            c.retry_backoff,
            (MIN_RETRY_BACKOFF + MAX_RETRY_BACKOFF) / 2 / MS * MS
        );
    }

    #[test]
    fn config_out_of_range_is_an_error() {
        // below: minAllowTTL - 1ns scaled by a second is still out of range
        // above max; a negative value other than -1 is below min.
        assert!(normalize_webhook_config(&mut cfg(MIN_ALLOW_TTL - 1, 0, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, MIN_DENY_TTL - 1, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, 0, MIN_RETRY_BACKOFF - 4)).is_err());
        assert!(normalize_webhook_config(&mut cfg(1 + MAX_ALLOW_TTL, 0, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, 1 + MAX_DENY_TTL, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, 0, 1 + MAX_RETRY_BACKOFF)).is_err());
        assert!(normalize_webhook_config(&mut cfg(-2, 0, 0)).is_err());
    }

    #[test]
    fn config_default_and_disabled_sentinels() {
        let mut c = cfg(USE_DEFAULT, USE_DEFAULT, USE_DEFAULT);
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(
            (c.allow_ttl, c.deny_ttl, c.retry_backoff),
            (DEFAULT_ALLOW_TTL, DEFAULT_DENY_TTL, DEFAULT_RETRY_BACKOFF)
        );
        let mut c = cfg(DISABLE_TTL, DISABLE_TTL, DISABLE_TTL);
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!((c.allow_ttl, c.deny_ttl, c.retry_backoff), (0, 0, 0));
    }

    #[test]
    fn config_min_and_max_values() {
        let mut c = cfg(MIN_ALLOW_TTL / SEC, MIN_DENY_TTL / SEC, MIN_RETRY_BACKOFF);
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(
            (c.allow_ttl, c.deny_ttl, c.retry_backoff),
            (MIN_ALLOW_TTL, MIN_DENY_TTL, MIN_RETRY_BACKOFF * MS)
        );
        let mut c = cfg(
            MAX_ALLOW_TTL / SEC,
            MAX_DENY_TTL / SEC,
            MAX_RETRY_BACKOFF / MS,
        );
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(
            (c.allow_ttl, c.deny_ttl, c.retry_backoff),
            (MAX_ALLOW_TTL, MAX_DENY_TTL, MAX_RETRY_BACKOFF)
        );
    }

    // --- a mock backend (admission_test.go `mockService` / `NewTestServer`)

    #[derive(Default)]
    struct MockState {
        allow: bool,
        status_code: u16,
        out_annotations: BTreeMap<String, String>,
        seen_annotations: Option<BTreeMap<String, String>>,
        calls: usize,
        seen_bodies: Vec<serde_json::Value>,
    }

    async fn handle(
        axum::extract::State(st): axum::extract::State<Arc<Mutex<MockState>>>,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
        let mut st = st.lock().unwrap();
        st.calls += 1;
        st.seen_bodies.push(body.clone());
        let images: Vec<&str> = body["spec"]["containers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| c["image"].as_str())
            .collect();
        // `mockService.Review`: allowed unless an image is "bad" (and the
        // service allows).
        let allowed = st.allow && !images.contains(&"bad");
        let seen: BTreeMap<String, String> = body["spec"]["annotations"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();
        st.seen_annotations = Some(seen);
        let code = axum::http::StatusCode::from_u16(st.status_code).unwrap();
        let mut resp = body;
        resp["status"] = serde_json::json!({
            "allowed": allowed,
            "auditAnnotations": st.out_annotations,
        });
        (code, axum::Json(resp))
    }

    struct Server {
        state: Arc<Mutex<MockState>>,
        url: String,
        dir: std::path::PathBuf,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn server(status_code: u16, allow: bool) -> Server {
        let state = Arc::new(Mutex::new(MockState {
            allow,
            status_code,
            ..Default::default()
        }));
        let app = axum::Router::new()
            .route("/", axum::routing::post(handle))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!(
            "imagepolicy-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Server { state, url, dir }
    }

    /// `newImagePolicyWebhook` (admission_test.go:362): a webhook against the
    /// server through a kubeconfig + config file.
    fn plugin(s: &Server, cache_secs: i64, default_allow: bool) -> ImagePolicyWebhook {
        write_plugin(&s.dir, &s.url, cache_secs, default_allow)
    }

    fn write_plugin(
        dir: &Path,
        url: &str,
        cache_secs: i64,
        default_allow: bool,
    ) -> ImagePolicyWebhook {
        let kc = dir.join("kubeconfig");
        std::fs::write(
            &kc,
            format!(
                "clusters:\n- name: c\n  cluster:\n    server: {url}\nusers:\n- name: u\n  user:\n    token: t\ncontexts:\n- name: x\n  context:\n    cluster: c\n    user: u\ncurrent-context: x\n"
            ),
        )
        .unwrap();
        let text = format!(
            "imagePolicy:\n  kubeConfigFile: {}\n  allowTTL: {cache_secs}\n  denyTTL: {cache_secs}\n  retryBackoff: 1\n  defaultAllow: {default_allow}\n",
            kc.display()
        );
        ImagePolicyWebhook::from_config(&text).unwrap()
    }

    fn good_pod(image: &str) -> Pod {
        Pod::new(
            "",
            rusternetes_common::resources::PodSpec {
                containers: vec![container(image)],
                ..Default::default()
            },
        )
    }

    fn container(image: &str) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
            name: "c".into(),
            image: image.into(),
            ..Default::default()
        }
    }

    // TestNewFromConfig: kubeconfig selection and failures.
    #[test]
    fn new_from_config_kubeconfig_cases() {
        let dir = std::env::temp_dir().join(format!("imagepolicy-kc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        std::fs::write(&ca, "x").unwrap();
        let cases = [
            // a single cluster, no context: the current context is unnamed
            // and matches no cluster name -> error
            (
                String::from("clusters:\n- name: foobar\n  cluster:\n    server: https://a.example.com\nusers:\n- name: n\n  user: {}\n"),
                true,
            ),
            // a context selects the cluster; its CA must be loadable
            (
                String::from("clusters:\n- name: foobar\n  cluster:\n    certificate-authority: a bad certificate path\n    server: https://a.example.com\n- name: barfoo\n  cluster:\n    server: https://a.example.com\nusers:\n- name: n\n  user: {}\ncontexts:\n- name: default\n  context:\n    cluster: barfoo\n    user: n\ncurrent-context: default\n"),
                false,
            ),
            (
                String::from("clusters:\n- name: foobar\n  cluster:\n    certificate-authority: a bad certificate path\n    server: https://a.example.com\n- name: barfoo\n  cluster:\n    server: https://a.example.com\nusers:\n- name: n\n  user: {}\ncontexts:\n- name: default\n  context:\n    cluster: foobar\n    user: n\ncurrent-context: default\n"),
                true,
            ),
        ];
        for (i, (kc, want_err)) in cases.iter().enumerate() {
            let p = dir.join(format!("kc{i}"));
            std::fs::write(&p, kc).unwrap();
            let text = format!(
                "imagePolicy:\n  kubeConfigFile: {}\n  allowTTL: 500\n  denyTTL: 500\n  retryBackoff: 500\n  defaultAllow: true\n",
                p.display()
            );
            let r = ImagePolicyWebhook::from_config(&text);
            assert_eq!(r.is_err(), *want_err, "case {i}: {:?}", r.err());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_config_and_missing_kubeconfig_fail() {
        let _g = rusternetes_api_server::admission::plugins::EnabledForTest::new(&[PLUGIN_NAME]);
        assert!(install_from_config(None)
            .unwrap_err()
            .to_string()
            .contains("no config specified"));
        assert!(ImagePolicyWebhook::from_config("imagePolicy:\n  defaultAllow: true\n").is_err());
        assert!(ImagePolicyWebhook::from_config("imagePolicy:\n  allowTTL: -5\n").is_err());
    }

    #[test]
    fn disabled_plugin_needs_no_config() {
        let _g = rusternetes_api_server::admission::plugins::EnabledForTest::new(&[]);
        install_from_config(None).unwrap();
    }

    // TestWebhookCache: error responses are not cached, successes are.
    #[tokio::test]
    async fn webhook_cache() {
        let s = server(500, true).await;
        let wh = plugin(&s, 200, false);
        let pod = good_pod("test");
        let cases = [
            (500u16, false),
            (404, false),
            (403, false),
            (401, false),
            (200, true),
            (500, true), // cached
        ];
        for (code, ok) in cases {
            s.state.lock().unwrap().status_code = code;
            let r = run(&wh, &pod, "namespace", None).await;
            assert_eq!(r.is_ok(), ok, "status {code}: {r:?}");
        }
        // A different request calls the webhook again.
        let pod2 = good_pod("test2");
        for (code, ok) in [(500u16, false), (200, true), (500, true)] {
            s.state.lock().unwrap().status_code = code;
            let r = run(&wh, &pod2, "namespace", None).await;
            assert_eq!(r.is_ok(), ok, "status {code}: {r:?}");
        }
    }

    // TestContainerCombinations.
    #[tokio::test]
    async fn container_combinations() {
        let s = server(200, true).await;
        let wh = plugin(&s, -1, false);
        let mut cases: Vec<(&str, Pod, bool)> = vec![
            ("single allowed", good_pod("good"), true),
            ("single denied", good_pod("bad"), false),
        ];
        for (name, images, init, ok) in [
            ("one good one bad", vec!["bad", "good"], vec![], false),
            ("multiple good", vec!["good", "good"], vec![], true),
            ("multiple bad", vec!["bad", "bad"], vec![], false),
            ("good, bad init", vec!["good"], vec!["bad"], false),
            ("good, good init", vec!["good"], vec!["good"], true),
        ] {
            let mut pod = good_pod("x");
            let spec = pod.spec.as_mut().unwrap();
            spec.containers = images.iter().map(|i| container(i)).collect();
            if !init.is_empty() {
                spec.init_containers = Some(init.iter().map(|i| container(i)).collect());
            }
            cases.push((name, pod, ok));
        }
        for (name, pod, ok) in cases {
            s.state.lock().unwrap().allow = true;
            let r = run(&wh, &pod, "ns", None).await;
            assert_eq!(r.is_ok(), ok, "{name}: {r:?}");
            // With a deny-all backend nothing is admitted.
            s.state.lock().unwrap().allow = false;
            assert!(
                run(&wh, &pod, "ns", None).await.is_err(),
                "{name}: deny-all"
            );
        }
    }

    // Containers and init containers are all reviewed, in order, with the
    // namespace; ephemeralcontainers reviews only the ephemeral containers;
    // other subresources are ignored (admission.go:128-153).
    #[tokio::test]
    async fn review_body_and_subresources() {
        let s = server(200, true).await;
        let wh = plugin(&s, -1, false);
        let mut pod = good_pod("a");
        let spec = pod.spec.as_mut().unwrap();
        spec.init_containers = Some(vec![container("i")]);
        spec.ephemeral_containers = Some(vec![rusternetes_common::resources::EphemeralContainer {
            name: "e".into(),
            image: "eph".into(),
            ..Default::default()
        }]);
        run(&wh, &pod, "ns1", None).await.unwrap();
        run(&wh, &pod, "ns1", Some("ephemeralcontainers"))
            .await
            .unwrap();
        let d = run(&wh, &pod, "ns1", Some("status")).await.unwrap();
        assert!(!d.failed_open && d.audit_annotations.is_empty());
        let st = s.state.lock().unwrap();
        assert_eq!(st.calls, 2, "the status subresource is not reviewed");
        let b = &st.seen_bodies[0];
        assert_eq!(b["apiVersion"], "imagepolicy.k8s.io/v1alpha1");
        assert_eq!(b["kind"], "ImageReview");
        assert_eq!(b["spec"]["namespace"], "ns1");
        assert_eq!(
            b["spec"]["containers"],
            serde_json::json!([{"image": "a"}, {"image": "i"}])
        );
        assert_eq!(
            st.seen_bodies[1]["spec"]["containers"],
            serde_json::json!([{"image": "eph"}])
        );
    }

    // TestDefaultAllow: an erroring backend fails open or closed.
    #[tokio::test]
    async fn default_allow() {
        for (default_allow, image) in [
            (true, "bad"),
            (true, "good"),
            (false, "good"),
            (false, "bad"),
        ] {
            let s = server(500, true).await;
            let wh = plugin(&s, -1, default_allow);
            let pod = good_pod(image);
            let r = run(&wh, &pod, "namespace", None).await;
            if default_allow {
                let d = r.unwrap();
                assert!(d.failed_open);
                assert_eq!(
                    d.audit_annotations,
                    vec![(
                        "imagepolicywebhook.image-policy.k8s.io/failed-open".to_string(),
                        "true".to_string()
                    )]
                );
                let mut pod = pod;
                mark_failed_open(&mut pod);
                assert_eq!(
                    pod.metadata.annotations.unwrap()[IMAGE_POLICY_FAILED_OPEN_KEY],
                    "true"
                );
            } else {
                let e = r.unwrap_err().to_string();
                assert!(e.contains("is forbidden"), "{e}");
            }
        }
    }

    // A refused connection is also a backend failure.
    #[tokio::test]
    async fn unreachable_backend() {
        let dir = std::env::temp_dir().join(format!("imagepolicy-un-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wh = write_plugin(&dir, "http://127.0.0.1:1/", -1, false);
        assert!(run(&wh, &good_pod("good"), "ns", None).await.is_err());
        let wh = write_plugin(&dir, "http://127.0.0.1:1/", -1, true);
        assert!(
            run(&wh, &good_pod("good"), "ns", None)
                .await
                .unwrap()
                .failed_open
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // TestAnnotationFiltering.
    #[tokio::test]
    async fn annotation_filtering() {
        let all: HashMap<String, String> = [
            ("my.image-policy.k8s.io/test", "test"),
            ("other.image-policy.k8s.io/test2", "annotation"),
            ("test", "test"),
            ("another", "another"),
            ("", ""),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for (annotations, want) in [
            (
                HashMap::from([
                    ("test".to_string(), "test".to_string()),
                    ("another".to_string(), "annotation".to_string()),
                    (String::new(), String::new()),
                ]),
                BTreeMap::new(),
            ),
            (
                all,
                BTreeMap::from([
                    (
                        "my.image-policy.k8s.io/test".to_string(),
                        "test".to_string(),
                    ),
                    (
                        "other.image-policy.k8s.io/test2".to_string(),
                        "annotation".to_string(),
                    ),
                ]),
            ),
        ] {
            let s = server(200, true).await;
            let wh = plugin(&s, -1, true);
            let mut pod = good_pod("test");
            pod.metadata.annotations = Some(annotations);
            run(&wh, &pod, "ns", None).await.unwrap();
            assert_eq!(
                s.state.lock().unwrap().seen_annotations.clone().unwrap(),
                want
            );
        }
    }

    // TestReturnedAnnotationAdd.
    #[tokio::test]
    async fn returned_annotations_are_prefixed() {
        let ann = |kv: &[(&str, &str)]| -> BTreeMap<String, String> {
            kv.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let cases = [
            (
                "good",
                ann(&[("foo-test", "true"), ("bar-test", "false")]),
                vec![
                    ("imagepolicywebhook.image-policy.k8s.io/bar-test", "false"),
                    ("imagepolicywebhook.image-policy.k8s.io/foo-test", "true"),
                ],
                false,
            ),
            ("good", ann(&[]), vec![], false),
            (
                "bad",
                ann(&[("foo-test", "false")]),
                vec![("imagepolicywebhook.image-policy.k8s.io/foo-test", "false")],
                true,
            ),
        ];
        for (image, out, want, want_err) in cases {
            let s = server(200, true).await;
            s.state.lock().unwrap().out_annotations = out;
            let wh = plugin(&s, -1, true);
            let d = wh.validate(&good_pod(image), "ns", None).await;
            assert_eq!(d.error.is_some(), want_err, "{image}");
            let want: Vec<(String, String)> = want
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            assert_eq!(d.audit_annotations, want);
        }
    }

    // The denial text (admission.go:210-215), wrapped by NewForbidden.
    #[tokio::test]
    async fn denial_messages() {
        let s = server(200, false).await;
        let wh = plugin(&s, -1, false);
        let e = run(&wh, &good_pod("good"), "ns", None).await.unwrap_err();
        assert_eq!(
            e.to_string().trim_start_matches("Forbidden: "),
            "pods \"Unknown\" is forbidden: one or more images rejected by webhook backend"
        );
    }
}
