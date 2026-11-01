//! Unit tests moved from `src/audit.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        extract::Request,
        http::{HeaderValue, Method},
        middleware::Next,
        response::Response,
    };
    pub use chrono::Utc;
    pub use rusternetes_common::audit::{
        AuditBackend, AuditEvent, AuditLevel, AuditStage, ObjectReference, ResponseStatus, UserInfo,
    };
    pub use serde::Deserialize;
    pub use std::sync::{Arc, Mutex, OnceLock};
    pub use tracing::{error, warn};
}

mod tests {
    use super::__parent::*;
    use axum::{body::Body, routing::get, Router};
    use rusternetes_api_server::audit::*;
    use tower::ServiceExt;

    struct Capture(tokio::sync::Mutex<Vec<AuditEvent>>);
    #[async_trait::async_trait]
    impl AuditBackend for Capture {
        async fn log(&self, e: AuditEvent) -> Result<(), String> {
            self.0.lock().await.push(e);
            Ok(())
        }
        async fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    const POLICY: &str = r#"
apiVersion: audit.k8s.io/v1
kind: Policy
omitStages: ["RequestReceived"]
rules:
- level: None
  users: ["system:kube-proxy"]
- level: None
  nonResourceURLs: ["/healthz*", "/version"]
- level: Metadata
  resources:
  - group: ""
    resources: ["secrets", "pods/log"]
- level: RequestResponse
  verbs: ["create"]
  namespaces: ["prod"]
- level: Request
  resources:
  - group: apps
    resources: ["*/scale"]
- level: Metadata
  omitStages: ["ResponseStarted"]
  resources:
  - group: ""
    resources: ["configmaps"]
    resourceNames: ["audited"]
"#;

    fn attrs(method: Method, path: &str) -> Attributes {
        let mut a = request_info(&method, path, None);
        a.user = Some(UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec!["devs".into()],
            extra: None,
        });
        a
    }

    #[test]
    fn request_info_matches_upstream_shapes() {
        let a = request_info(&Method::GET, "/api/v1/namespaces/ns/pods/p/log", None);
        assert_eq!(
            (a.verb.as_str(), a.namespace.as_str(), a.resource.as_str()),
            ("get", "ns", "pods")
        );
        assert_eq!((a.name.as_str(), a.subresource.as_str()), ("p", "log"));
        let a = request_info(&Method::GET, "/apis/apps/v1/deployments", None);
        assert_eq!((a.verb.as_str(), a.api_group.as_str()), ("list", "apps"));
        let a = request_info(&Method::GET, "/api/v1/pods", Some("watch=true"));
        assert_eq!(a.verb, "watch");
        let a = request_info(&Method::DELETE, "/api/v1/namespaces/ns/pods", None);
        assert_eq!(a.verb, "deletecollection");
        let a = request_info(&Method::GET, "/api/v1/namespaces/ns", None);
        assert_eq!((a.resource.as_str(), a.name.as_str()), ("namespaces", "ns"));
        // requestinfo.go:247-253: a list/watch with an exact `metadata.name`
        // field selector names the object.
        let a = request_info(
            &Method::GET,
            "/api/v1/namespaces/ns/pods",
            Some("watch=true&fieldSelector=metadata.name%3Dmypod"),
        );
        assert_eq!((a.verb.as_str(), a.name.as_str()), ("watch", "mypod"));
        let a = request_info(
            &Method::GET,
            "/api/v1/pods",
            Some("fieldSelector=status.phase=Running,metadata.name==p"),
        );
        assert_eq!((a.verb.as_str(), a.name.as_str()), ("list", "p"));
        let a = request_info(
            &Method::GET,
            "/api/v1/pods",
            Some("fieldSelector=metadata.name!=p"),
        );
        assert_eq!(a.name, "");
        let a = request_info(&Method::GET, "/healthz", None);
        assert!(!a.is_resource_request);
        assert_eq!(a.verb, "get");
    }

    // requestinfo_test.go TestSelectorParsing (name, verb, fieldSelector, labelSelector).
    #[test]
    fn request_info_selector_parsing_matches_upstream() {
        let cases: &[(&str, &str, &str, &str, &str, &str)] = &[
            ("/apis/g/v/resource", "", "", "list", "", ""),
            (
                "/apis/g/v/resource",
                "fieldSelector=metadata.name=name1",
                "name1",
                "list",
                "metadata.name=name1",
                "",
            ),
            (
                "/apis/g/v/resource",
                "watch=true&fieldSelector=metadata.name=name1",
                "name1",
                "watch",
                "metadata.name=name1",
                "",
            ),
            (
                "/apis/g/v/resource",
                "fieldSelector=foo=bar&labelSelector=baz=qux",
                "",
                "list",
                "foo=bar",
                "baz=qux",
            ),
            (
                "/apis/g/v/resource",
                "fieldSelector=metadata.name=name1,foo",
                "",
                "list",
                "metadata.name=name1,foo",
                "",
            ),
            (
                "/apis/g/v/resource",
                "fieldSelector=metadata.name=name1,foo&watch=false",
                "",
                "list",
                "metadata.name=name1,foo",
                "",
            ),
            (
                "/apis/g/v/resource",
                "fieldSelector=metadata.name=foo&fieldSelector=metadata.name=bar&labelSelector=foo=bar&labelSelector=foo=baz",
                "foo",
                "list",
                "metadata.name=foo",
                "foo=bar",
            ),
            (
                "/apis/g/v/resource/name",
                "fieldSelector=foo=bar&labelSelector=foo=bar",
                "name",
                "get",
                "",
                "",
            ),
            (
                "/apis/g/v/watch/resource",
                "fieldSelector=metadata.name=foo&labelSelector=foo=bar",
                "",
                "watch",
                "",
                "",
            ),
            (
                "/apis/g/v/watch/resource/name",
                "fieldSelector=metadata.name=foo&labelSelector=foo=bar",
                "name",
                "watch",
                "",
                "",
            ),
        ];
        for (path, query, name, verb, field, label) in cases {
            let a = request_info(&Method::GET, path, Some(query));
            assert_eq!(
                (
                    &a.name[..],
                    &a.verb[..],
                    &a.field_selector[..],
                    &a.label_selector[..]
                ),
                (*name, *verb, *field, *label),
                "{path}?{query}"
            );
        }
        // deletecollection supports selectors too (verbsWithSelectors).
        let a = request_info(&Method::DELETE, "/api/v1/pods", Some("labelSelector=a=b"));
        assert_eq!(
            (a.verb.as_str(), a.label_selector.as_str()),
            ("deletecollection", "a=b")
        );
    }

    #[test]
    fn policy_first_match_and_default() {
        let p = Policy::from_yaml(POLICY).unwrap();
        let lvl = |a: Attributes| p.evaluate(&a).level;
        // No rule matches: DefaultAuditLevel None.
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/services/s")),
            AuditLevel::None
        );
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/secrets/s")),
            AuditLevel::Metadata
        );
        // "pods/log" matches the subresource; "pods" alone does not.
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/pods/p/log")),
            AuditLevel::Metadata
        );
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/pods/p")),
            AuditLevel::None
        );
        // "*/scale"
        assert_eq!(
            lvl(attrs(
                Method::GET,
                "/apis/apps/v1/namespaces/ns/deployments/d/scale"
            )),
            AuditLevel::Request
        );
        // verbs + namespaces
        assert_eq!(
            lvl(attrs(Method::POST, "/api/v1/namespaces/prod/services")),
            AuditLevel::RequestResponse
        );
        assert_eq!(
            lvl(attrs(Method::POST, "/api/v1/namespaces/dev/services")),
            AuditLevel::None
        );
        // non-resource URLs, trailing * and exact
        assert_eq!(lvl(attrs(Method::GET, "/healthz/ping")), AuditLevel::None);
        // resourceNames
        assert_eq!(
            lvl(attrs(
                Method::GET,
                "/api/v1/namespaces/ns/configmaps/audited"
            )),
            AuditLevel::Metadata
        );
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/configmaps/other")),
            AuditLevel::None
        );
    }

    #[test]
    fn policy_users_rule_and_omit_stage_union() {
        let p = Policy::from_yaml(POLICY).unwrap();
        let mut a = attrs(Method::GET, "/api/v1/namespaces/ns/secrets/s");
        a.user.as_mut().unwrap().username = "system:kube-proxy".into();
        assert_eq!(p.evaluate(&a).level, AuditLevel::None);
        // Policy.omitStages is unioned into the rule's (checker.go:33-38).
        let c = p.evaluate(&attrs(
            Method::GET,
            "/api/v1/namespaces/ns/configmaps/audited",
        ));
        let mut s = c.omit_stages.clone();
        s.sort();
        assert_eq!(s, vec!["RequestReceived", "ResponseStarted"]);
        // The default (no rule) config carries the policy's omitStages.
        let c = p.evaluate(&attrs(Method::GET, "/api/v1/namespaces/ns/services/s"));
        assert_eq!(c.omit_stages, vec!["RequestReceived"]);
    }

    #[test]
    fn policy_with_unknown_fields_falls_back_to_lenient() {
        // reader.go LoadPolicyFromBytes: a strict-decode error only warns.
        let p = Policy::from_yaml(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nbogus: 1\nrules:\n- level: Metadata\n  nope: [a]\n",
        )
        .expect("lenient fallback");
        assert_eq!(p.rules.len(), 1);
        // A type error is not a strict-decoding error: hard failure.
        let e =
            Policy::from_yaml("apiVersion: audit.k8s.io/v1\nkind: Policy\nrules: 5\n").unwrap_err();
        assert!(e.starts_with("failed decoding"), "{e}");
    }

    #[test]
    fn policy_validation_matches_upstream() {
        let bad = |yaml: &str| Policy::from_yaml(yaml).unwrap_err();
        let hdr = "apiVersion: audit.k8s.io/v1\nkind: Policy\n";
        assert!(bad(&format!("{hdr}rules: []")).contains("0 rules"));
        assert!(bad(&format!("{hdr}rules:\n- verbs: [get]")).contains("rules[0].level: Required"));
        assert!(bad(&format!("{hdr}rules:\n- level: Bogus")).contains("Unsupported value"));
        assert!(bad(&format!(
            "{hdr}rules:\n- level: None\n  nonResourceURLs: [\"healthz\", \"/a*b\"]"
        ))
        .contains("must begin with a '/'"));
        assert!(bad(&format!(
            "{hdr}rules:\n- level: None\n  nonResourceURLs: [\"/a*b\"]"
        ))
        .contains("must be the final character"));
        assert!(bad(&format!(
            "{hdr}rules:\n- level: None\n  namespaces: [a]\n  nonResourceURLs: [\"/a\"]"
        ))
        .contains("cannot apply to both"));
        assert!(
            bad(&format!("{hdr}omitStages: [Nope]\nrules:\n- level: None"))
                .contains("allowed stages are")
        );
        assert!(bad("apiVersion: v1\nkind: Policy\nrules:\n- level: None")
            .contains("unknown group version"));
    }

    fn app(cfg: Arc<AuditConfig>) -> Router {
        Router::new()
            .route(
                "/api/v1/namespaces/:ns/secrets/:name",
                get(|| async {
                    add_audit_annotation("pod-security.kubernetes.io/enforce-policy", "x:latest");
                    "ok"
                }),
            )
            .route(
                "/api/v1/namespaces/:ns/services/:name",
                get(|| async {
                    add_audit_annotation("k.example/a", "b");
                    "ok"
                }),
            )
            .layer(axum::middleware::from_fn(move |req, next| {
                let cfg = cfg.clone();
                async move { with_audit(cfg, req, next).await }
            }))
    }

    #[tokio::test]
    async fn filter_emits_events_per_stage_with_annotations() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(&POLICY.replace("omitStages: [\"RequestReceived\"]\n", ""))
                .unwrap(),
            sink: cap.clone(),
        });
        let resp = app(cfg)
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                    .header("User-Agent", "kubectl/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let id = resp.headers()["Audit-ID"].to_str().unwrap().to_string();
        let ev = cap.0.lock().await;
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].stage, AuditStage::RequestReceived);
        assert_eq!(ev[0].stage_timestamp, ev[0].request_received_timestamp);
        assert!(ev[0].annotations.is_none());
        assert_eq!(ev[1].stage, AuditStage::ResponseComplete);
        assert_eq!(ev[1].audit_id, id);
        assert_eq!(ev[1].level, AuditLevel::Metadata);
        assert_eq!(ev[1].verb, "get");
        assert_eq!(ev[1].user_agent.as_deref(), Some("kubectl/1"));
        assert_eq!(ev[1].response_status.as_ref().unwrap().code, 200);
        let oref = ev[1].object_ref.as_ref().unwrap();
        assert_eq!(oref.resource.as_deref(), Some("secrets"));
        assert_eq!(oref.namespace.as_deref(), Some("ns"));
        assert_eq!(oref.name.as_deref(), Some("s"));
        assert_eq!(
            ev[1].annotations.as_ref().unwrap()["pod-security.kubernetes.io/enforce-policy"],
            "x:latest"
        );
    }

    /// Annotations authentication recorded (`AuthAuditAnnotations`, e.g.
    /// legacy.go:143-170) land on the audit event.
    #[tokio::test]
    async fn filter_applies_annotations_recorded_by_authentication() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(POLICY).unwrap(),
            sink: cap.clone(),
        });
        let mut req = axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(rusternetes_middleware::AuthAuditAnnotations(vec![(
                "authentication.k8s.io/legacy-token".to_string(),
                "system:serviceaccount:ns:sa".to_string(),
            )]));
        app(cfg).oneshot(req).await.unwrap();
        let ev = cap.0.lock().await;
        assert_eq!(
            ev[0].annotations.as_ref().unwrap()["authentication.k8s.io/legacy-token"],
            "system:serviceaccount:ns:sa"
        );
    }

    /// `writeLatencyToAnnotation` (filters/audit.go:150-174): above 500ms the
    /// per-layer latencies and `apiserver.latency.k8s.io/total` land on the
    /// ResponseComplete event; below it nothing is added.
    #[tokio::test]
    async fn slow_request_gets_latency_annotations_fast_does_not() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(POLICY).unwrap(),
            sink: cap.clone(),
        });
        let app = Router::new()
            .route(
                "/api/v1/namespaces/:ns/secrets/:name",
                get(|axum::extract::Path((_, name)): axum::extract::Path<(String, String)>| async move {
                    rusternetes_common::latency_trackers::track_storage_latency(
                        std::time::Duration::from_millis(7),
                    );
                    if name == "slow" {
                        tokio::time::sleep(std::time::Duration::from_millis(560)).await;
                    }
                    "ok"
                }),
            )
            .layer(axum::middleware::from_fn(move |req, next| {
                let cfg = cfg.clone();
                async move { with_audit(cfg, req, next).await }
            }))
            .layer(axum::middleware::from_fn(
                rusternetes_api_server::apiserver_request_metrics::with_latency_trackers_filter,
            ));
        for n in ["fast", "slow"] {
            app.clone()
                .oneshot(
                    axum::http::Request::get(format!("/api/v1/namespaces/ns/secrets/{n}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        let ev = cap.0.lock().await;
        assert_eq!(ev.len(), 2);
        assert!(ev[0].annotations.is_none(), "{:?}", ev[0].annotations);
        let a = ev[1].annotations.as_ref().unwrap();
        assert_eq!(a["apiserver.latency.k8s.io/etcd"], "7ms");
        assert!(a["apiserver.latency.k8s.io/total"].ends_with("ms"));
    }

    /// A policy that audits every request at `level`.
    fn level_policy(level: &str, omit_managed: bool) -> Policy {
        Policy::from_yaml(&format!(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nomitManagedFields: {omit_managed}\nrules:\n- level: {level}\n"
        ))
        .unwrap()
    }

    async fn run_policy_app(
        policy: Policy,
        route: axum::routing::MethodRouter,
    ) -> (Response, Vec<AuditEvent>) {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy,
            sink: cap.clone(),
        });
        let app = Router::new()
            .route("/api/v1/namespaces/:ns/secrets/:name", route)
            .layer(axum::middleware::from_fn(move |req, next| {
                let cfg = cfg.clone();
                async move { with_audit(cfg, req, next).await }
            }));
        let resp = app
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let ev = cap.0.lock().await.clone();
        (resp, ev)
    }

    fn pod_json() -> serde_json::Value {
        serde_json::json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": {"name": "test-pod", "namespace": "test-namespace",
                "managedFields": [{"manager": "kubectl"}]},
            "spec": {"containers": [{"name": "test-container", "image": "test-image"}]}
        })
    }

    async fn run_status_app(route: axum::routing::MethodRouter) -> (Response, Vec<AuditEvent>) {
        run_policy_app(level_policy("Metadata", false), route).await
    }

    /// A handler failing with an `Error` is recorded through the response
    /// extension `Error::into_response` sets (the `LogResponseObject` call in
    /// `WriteObjectNegotiated`): the NotFound carries its status/reason/
    /// message/details, and the client's body is untouched.
    #[tokio::test]
    async fn failed_response_status_is_recorded() {
        let (resp, ev) = run_status_app(get(|| async {
            Err::<&str, _>(rusternetes_common::Error::NotFound(
                "secrets \"s\" not found".into(),
            ))
        }))
        .await;
        assert_eq!(resp.status(), 404);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("NotFound"));
        let rs = ev.last().unwrap().response_status.as_ref().unwrap();
        assert_eq!(rs.code, 404);
        assert_eq!(rs.status.as_deref(), Some("Failure"));
        assert_eq!(rs.reason.as_deref(), Some("NotFound"));
        assert_eq!(rs.message.as_deref(), Some("secrets \"s\" not found"));
        assert_eq!(rs.details.as_ref().unwrap()["name"], "s");
        let json = serde_json::to_value(rs).unwrap();
        assert_eq!(json["metadata"], serde_json::json!({}));
        assert!(ev.last().unwrap().response_object.is_none());
    }

    /// The filter no longer reads a body back: a Status-shaped JSON body the
    /// handler never passed to `log_response_object` leaves only the code.
    #[tokio::test]
    async fn unhooked_status_body_is_not_read_back() {
        let (_resp, ev) = run_status_app(get(|| async {
            (
                axum::http::StatusCode::NOT_FOUND,
                [("content-type", "application/json")],
                r#"{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"NotFound","code":404}"#,
            )
        }))
        .await;
        let rs = ev.last().unwrap().response_status.as_ref().unwrap();
        assert_eq!(rs.code, 404);
        assert!(rs.status.is_none() && rs.reason.is_none());
    }

    /// TestLogResponseObjectWithStatus, Metadata case: the Status fields are
    /// logged without the object; the code set by the writer is kept.
    #[tokio::test]
    async fn status_at_metadata_logs_fields_without_object() {
        let (_resp, ev) = run_status_app(get(|| async {
            log_response_object(&serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "metadata": {},
                "status": "Success", "message": "Test message", "code": 200
            }));
            "ok"
        }))
        .await;
        let last = ev.last().unwrap();
        let rs = last.response_status.as_ref().unwrap();
        assert_eq!(rs.status.as_deref(), Some("Success"));
        assert_eq!(rs.message.as_deref(), Some("Test message"));
        assert_eq!(rs.code, 200);
        assert!(last.response_object.is_none());
    }

    /// TestLogResponseObjectWithStatus, RequestResponse case: fields AND the
    /// encoded object.
    #[tokio::test]
    async fn status_at_request_response_logs_fields_and_object() {
        let status = serde_json::json!({
            "kind": "Status", "apiVersion": "v1", "metadata": {},
            "status": "Success", "message": "Test message", "code": 200
        });
        let s2 = status.clone();
        let (_resp, ev) = run_policy_app(
            level_policy("RequestResponse", false),
            get(move || async move {
                log_response_object(&s2);
                "ok"
            }),
        )
        .await;
        let last = ev.last().unwrap();
        let rs = last.response_status.as_ref().unwrap();
        assert_eq!(rs.status.as_deref(), Some("Success"));
        assert_eq!(rs.message.as_deref(), Some("Test message"));
        assert_eq!(last.response_object.as_ref(), Some(&status));
    }

    /// TestLogResponseObjectWithPod: a non-Status object at RequestResponse is
    /// recorded as `responseObject`; `responseStatus` keeps only the code.
    #[tokio::test]
    async fn pod_at_request_response_is_recorded() {
        let (_resp, ev) = run_policy_app(
            level_policy("RequestResponse", false),
            get(|| async {
                log_response_object(&pod_json());
                "ok"
            }),
        )
        .await;
        let last = ev.last().unwrap();
        let obj = last.response_object.as_ref().expect("responseObject");
        assert_eq!(obj["metadata"]["name"], "test-pod");
        assert_eq!(obj["spec"]["containers"][0]["image"], "test-image");
        let rs = last.response_status.as_ref().unwrap();
        assert_eq!(rs.code, 200);
        assert!(rs.status.is_none() && rs.message.is_none());
    }

    /// TestLogResponseObjectLevelCheck: below RequestResponse a non-Status
    /// object records nothing; outside an audit context the call is a no-op.
    #[tokio::test]
    async fn pod_below_request_response_is_not_recorded() {
        for level in ["Metadata", "Request"] {
            let (_resp, ev) = run_policy_app(
                level_policy(level, false),
                get(|| async {
                    log_response_object(&pod_json());
                    "ok"
                }),
            )
            .await;
            assert!(ev.last().unwrap().response_object.is_none(), "{level}");
        }
        log_response_object(&pod_json());
    }

    /// `shouldOmitManagedFields` / `copyWithoutManagedFields`: the object and
    /// the items of a list lose `metadata.managedFields`.
    #[tokio::test]
    async fn omit_managed_fields_strips_object_and_list_items() {
        let list = serde_json::json!({
            "kind": "PodList", "apiVersion": "v1", "metadata": {},
            "items": [pod_json()]
        });
        for (omit, obj) in [(true, pod_json()), (true, list), (false, pod_json())] {
            let o = obj.clone();
            let (_resp, ev) = run_policy_app(
                level_policy("RequestResponse", omit),
                get(move || async move {
                    log_response_object(&o);
                    "ok"
                }),
            )
            .await;
            let rec = ev.last().unwrap().response_object.as_ref().unwrap();
            let meta = if rec["kind"] == "PodList" {
                &rec["items"][0]["metadata"]
            } else {
                &rec["metadata"]
            };
            assert_eq!(meta.get("managedFields").is_none(), omit);
        }
    }

    /// A 2xx Status written by a handler through the shared `respond` helper
    /// (a delete's `Success`) is recorded handler-side.
    #[tokio::test]
    async fn respond_helper_records_the_object() {
        use rusternetes_api_server::endpoints::handlers::rest::respond;
        use rusternetes_api_server::registry::rest::RequestContext;
        let (resp, ev) = run_policy_app(
            level_policy("RequestResponse", false),
            get(|| async {
                respond(
                    axum::http::StatusCode::OK,
                    &serde_json::json!({"kind": "Status", "apiVersion": "v1", "metadata": {},
                        "status": "Success", "details": {"name": "s", "uid": "u1"}, "code": 200}),
                    &RequestContext::new(Some("ns")),
                )
            }),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("Success"));
        let last = ev.last().unwrap();
        let rs = last.response_status.as_ref().unwrap();
        assert_eq!(rs.status.as_deref(), Some("Success"));
        assert_eq!(rs.details.as_ref().unwrap()["uid"], "u1");
        assert!(last.response_object.is_some());
    }

    /// A streamed (watch-style) body must not be buffered: the first chunk
    /// reaches the client while the stream is still open.
    #[tokio::test]
    async fn streamed_success_body_is_not_buffered() {
        let (resp, _ev) = run_status_app(get(|| async {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(1);
            tx.send(Ok(b"{\"type\":\"ADDED\"}\n".to_vec()))
                .await
                .unwrap();
            // Keep the stream open for the response's lifetime.
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                drop(tx);
            });
            (
                [("content-type", "application/json")],
                Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)),
            )
        }))
        .await;
        use futures::StreamExt;
        let mut s = resp.into_body().into_data_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), s.next())
            .await
            .expect("first chunk must arrive without waiting for stream end");
        assert!(first.is_some());
    }

    #[tokio::test]
    async fn filter_honours_level_none_and_omit_stages() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(POLICY).unwrap(),
            sink: cap.clone(),
        });
        // Level None (no rule): nothing logged, no Audit-ID header.
        let r = app(cfg.clone())
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/services/s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(r.headers().get("Audit-ID").is_none());
        assert!(cap.0.lock().await.is_empty());
        // omitStages RequestReceived: only ResponseComplete.
        app(cfg)
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let ev = cap.0.lock().await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].stage, AuditStage::ResponseComplete);
    }

    fn test_ac() -> Arc<AuditContext> {
        let cap = Arc::new(Capture(Default::default()));
        Arc::new(AuditContext {
            inner: Mutex::new(AuditInner {
                event: AuditEvent {
                    api_version: "audit.k8s.io/v1".into(),
                    kind: "Event".into(),
                    level: AuditLevel::Metadata,
                    audit_id: "id".into(),
                    stage: AuditStage::RequestReceived,
                    request_uri: "/".into(),
                    verb: "get".into(),
                    user: UserInfo {
                        username: "u".into(),
                        uid: String::new(),
                        groups: vec![],
                        extra: None,
                    },
                    user_agent: None,
                    source_ips: vec![],
                    object_ref: None,
                    response_status: None,
                    response_object: None,
                    request_object: None,
                    request_received_timestamp: Utc::now(),
                    stage_timestamp: Utc::now(),
                    annotations: None,
                },
                config: RequestAuditConfig {
                    level: AuditLevel::Metadata,
                    omit_stages: vec![],
                    omit_managed_fields: false,
                },
            }),
            sink: cap,
        })
    }

    #[tokio::test]
    async fn annotation_conflict_keeps_first_and_unaudited_is_noop() {
        // Outside any request: AddAuditAnnotation with no AuditContext is a no-op.
        add_audit_annotation("a.example/k", "v");
        let ac = test_ac();
        AUDIT_CONTEXT
            .scope(ac.clone(), async {
                add_audit_annotation("a.example/k", "first");
                add_audit_annotation("a.example/k", "second");
            })
            .await;
        let g = ac.inner.lock().unwrap();
        assert_eq!(
            g.event.annotations.as_ref().unwrap()["a.example/k"],
            "first"
        );
    }

    #[tokio::test]
    async fn pod_security_outcome_is_recorded_with_the_psa_prefix() {
        let ac = test_ac();
        let outcome = rusternetes_api_server::admission::PodSecurityOutcome {
            warnings: vec![],
            audit_annotations: std::collections::BTreeMap::from([
                ("enforce-policy".to_string(), "baseline:latest".to_string()),
                ("audit-violations".to_string(), "would violate".to_string()),
            ]),
        };
        AUDIT_CONTEXT
            .scope(ac.clone(), async {
                rusternetes_api_server::admission::record_pod_security_audit(&outcome)
            })
            .await;
        let g = ac.inner.lock().unwrap();
        let a = g.event.annotations.as_ref().unwrap();
        assert_eq!(
            a["pod-security.kubernetes.io/enforce-policy"],
            "baseline:latest"
        );
        assert_eq!(
            a["pod-security.kubernetes.io/audit-violations"],
            "would violate"
        );
        assert_eq!(a.len(), 2);
    }

    // TestWithFailedRequestAudit (request_deadline_test.go:350-): the event is
    // written once, at ResponseStarted, with the code and statusErr message;
    // a request the policy does not audit passes through (`!ac.Enabled()`).
    #[tokio::test]
    async fn failed_request_audit_writes_one_response_started_event() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = AuditConfig {
            policy: Policy::from_yaml(POLICY).unwrap(),
            sink: cap.clone(),
        };
        let status =
            rusternetes_common::types::Status::failure("error serving request", "BadRequest", 400);
        let failed = || {
            let mut r = Response::new(Body::empty());
            *r.status_mut() = axum::http::StatusCode::BAD_REQUEST;
            r
        };
        let h = axum::http::HeaderMap::new();
        // Audited (secrets: Metadata).
        let uri: axum::http::Uri = "/api/v1/namespaces/ns/secrets/s".parse().unwrap();
        let resp = with_failed_request_audit(&cfg, &Method::GET, &uri, &h, &status, failed()).await;
        assert!(resp.headers().contains_key("Audit-ID"));
        // Not audited (no rule matches pods/p): untouched, no event.
        let uri: axum::http::Uri = "/api/v1/namespaces/ns/pods/p".parse().unwrap();
        let resp = with_failed_request_audit(&cfg, &Method::GET, &uri, &h, &status, failed()).await;
        assert!(!resp.headers().contains_key("Audit-ID"));
        let ev = cap.0.lock().await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].stage, AuditStage::ResponseStarted);
        let rs = ev[0].response_status.as_ref().unwrap();
        assert_eq!(rs.code, 400);
        assert_eq!(rs.message.as_deref(), Some("error serving request"));
        assert_eq!(rs.reason.as_deref(), Some("BadRequest"));
        assert_eq!(ev[0].user.username, "");
    }
    // #2933: every response object is recorded where it is serialized
    // (`WriteObjectNegotiated` -> `audit.LogResponseObject`,
    // responsewriters/writers.go:346), lists included. The typed list handlers
    // returned `Json(list)` directly and bypassed it.
    async fn list_handler_response_object(
        route: axum::routing::MethodRouter,
    ) -> Option<serde_json::Value> {
        let (_resp, ev) = run_policy_app(level_policy("RequestResponse", false), route).await;
        ev.last().and_then(|e| e.response_object.clone())
    }

    fn list_state() -> Arc<rusternetes_api_server::state::ApiServerState> {
        use rusternetes_common::auth::TokenManager;
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::StorageBackend;
        Arc::new(rusternetes_api_server::state::ApiServerState::new(
            Arc::new(StorageBackend::new_memory()),
            Arc::new(TokenManager::new(b"test-secret")),
            Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer)
                as Arc<dyn rusternetes_common::authz::Authorizer>,
            Arc::new(MetricsRegistry::new()),
            true,
        ))
    }

    fn anon() -> axum::Extension<rusternetes_api_server::middleware::AuthContext> {
        axum::Extension(rusternetes_api_server::middleware::AuthContext {
            user: rusternetes_common::auth::UserInfo::anonymous(),
        })
    }

    #[tokio::test]
    async fn deployment_list_is_audited_at_request_response() {
        use axum::extract::{Path, Query, State};
        let state = list_state();
        let obj = list_handler_response_object(get(move || async move {
            rusternetes_api_server::handlers::deployment::list(
                State(state),
                anon(),
                Path("ns".to_string()),
                axum::http::HeaderMap::new(),
                Query(Default::default()),
            )
            .await
            .unwrap()
        }))
        .await
        .expect("a DeploymentList must be recorded as responseObject");
        assert_eq!(obj["kind"], "DeploymentList");
    }

    #[tokio::test]
    async fn secret_list_is_audited_at_request_response() {
        use axum::extract::{Path, Query, State};
        let state = list_state();
        let obj = list_handler_response_object(get(move || async move {
            rusternetes_api_server::handlers::secret::list(
                State(state),
                anon(),
                Path("ns".to_string()),
                Query(Default::default()),
            )
            .await
            .unwrap()
        }))
        .await
        .expect("a SecretList must be recorded as responseObject");
        assert_eq!(obj["kind"], "SecretList");
    }

    #[tokio::test]
    async fn pod_list_is_audited_at_request_response() {
        use axum::extract::{Path, State};
        let state = list_state();
        let obj = list_handler_response_object(get(move || async move {
            rusternetes_api_server::handlers::pod::list(
                State(state),
                anon(),
                Path("ns".to_string()),
                axum::http::HeaderMap::new(),
                axum::extract::Query(Default::default()),
            )
            .await
            .unwrap()
        }))
        .await
        .expect("a PodList must be recorded as responseObject");
        assert_eq!(obj["kind"], "PodList");
    }
}
