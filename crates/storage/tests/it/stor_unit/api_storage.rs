//! Unit tests moved from `src/api_storage.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use futures::StreamExt;
    pub use rusternetes_client::api_status::{
        is_already_exists, is_bad_request, is_conflict, is_forbidden, is_gone, is_invalid,
        is_not_found, is_resource_expired, is_unsupported_media_type,
    };
    pub use rusternetes_client::http::{ApiClient, GetError, KubernetesList};
    pub use rusternetes_client::watch::{watch_stream, WatchEvent as ClientWatchEvent};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{Storage, WatchEvent, WatchStream};
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use serde_json::Value;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tokio::sync::{broadcast, Mutex, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::api_storage::*;

    /// Resolve a built-in object key through the static table + pure builder,
    /// mirroring the old free-fn `object_path` for test purposes.
    fn static_object_path(key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = static_resource_info(&rt).ok_or_else(|| unmapped(&rt))?;
        build_object_path(root, namespaced, &rt, &rest)
    }

    fn static_collection_prefix(prefix: &str) -> Result<String> {
        let (rt, rest) = parse_key(prefix)?;
        let (root, namespaced) = static_resource_info(&rt).ok_or_else(|| unmapped(&rt))?;
        build_collection_for_prefix(root, namespaced, &rt, &rest)
    }

    /// Regression: the Api backend's `delete` must force an immediate (hard)
    /// delete. Without `gracePeriodSeconds=0` the api-server only soft-deletes
    /// a pod (stamps deletionTimestamp), so the kubelet's finalize never
    /// removes an already-terminated pod and loops forever (#1150 pod-deletion
    /// regression — conformance "wait for pod to disappear" 300s timeouts).
    /// The graceful path must send NO delete options: the api-server then
    /// applies the pod's own terminationGracePeriodSeconds, which is what
    /// upstream's controllers rely on. Sending gracePeriodSeconds=0 here would
    /// force-kill every scaled-down StatefulSet pod.
    #[test]
    fn graceful_delete_sends_no_options() {
        assert!(
            graceful_delete_query().is_empty(),
            "graceful delete must not override the server's deletion defaults"
        );
    }

    #[test]
    fn delete_forces_zero_grace_period() {
        let q = force_delete_query();
        assert!(
            q.iter().any(|(k, v)| k == "gracePeriodSeconds" && v == "0"),
            "ApiStorage::delete must send gracePeriodSeconds=0 for a hard delete, got {q:?}"
        );
    }

    /// A hard delete leaves the dependents to the garbage collector, as the
    /// other backends do, whatever the owner's default policy.
    #[test]
    fn delete_propagates_in_the_background() {
        let q = force_delete_query();
        assert!(
            q.iter()
                .any(|(k, v)| k == "propagationPolicy" && v == "Background"),
            "ApiStorage::delete must not leave propagation to the owner's default, got {q:?}"
        );
    }

    #[test]
    fn object_path_namespaced_and_cluster() {
        assert_eq!(
            static_object_path("/registry/pods/default/nginx").unwrap(),
            "/api/v1/namespaces/default/pods/nginx"
        );
        assert_eq!(
            static_object_path("/registry/deployments/kube-system/coredns").unwrap(),
            "/apis/apps/v1/namespaces/kube-system/deployments/coredns"
        );
        assert_eq!(
            static_object_path("/registry/nodes/node-1").unwrap(),
            "/api/v1/nodes/node-1"
        );
        assert_eq!(
            static_object_path("/registry/priorityclasses/high").unwrap(),
            "/apis/scheduling.k8s.io/v1/priorityclasses/high"
        );
    }

    #[test]
    fn collection_paths() {
        assert_eq!(
            static_collection_prefix("/registry/replicasets/").unwrap(),
            "/apis/apps/v1/replicasets"
        );
        assert_eq!(
            static_collection_prefix("/registry/pods/default/").unwrap(),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            static_collection_prefix("/registry/nodes/").unwrap(),
            "/api/v1/nodes"
        );
        let (rt, rest) = parse_key("/registry/pods/default/nginx").unwrap();
        let (root, ns) = static_resource_info(&rt).unwrap();
        assert_eq!(
            build_collection_for_object(root, ns, &rt, &rest).unwrap(),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn non_builtin_type_is_not_static() {
        // CRD types fall through the static table (resolved via discovery at
        // runtime); they are not hard-coded.
        assert!(static_resource_info("verticalpodautoscalers").is_none());
        assert!(static_object_path("/registry/verticalpodautoscalers/ns/v").is_err());
    }

    #[test]
    fn discovery_ingest_maps_plurals_and_skips_subresources() {
        let mut map = HashMap::new();
        let list = serde_json::json!({
            "resources": [
                {"name": "volumesnapshots", "namespaced": true},
                {"name": "volumesnapshots/status", "namespaced": true},
                {"name": "volumesnapshotcontents", "namespaced": false},
            ]
        });
        ingest_resource_list(&mut map, "/apis/snapshot.storage.k8s.io/v1", &list);
        assert_eq!(
            map.get("volumesnapshots"),
            Some(&("/apis/snapshot.storage.k8s.io/v1".to_string(), true))
        );
        assert_eq!(
            map.get("volumesnapshotcontents"),
            Some(&("/apis/snapshot.storage.k8s.io/v1".to_string(), false))
        );
        // Subresource entry must not become its own mapping.
        assert!(!map.contains_key("volumesnapshots/status"));
    }

    #[test]
    fn storage_key_roundtrip() {
        let obj = serde_json::json!({"metadata": {"name": "x", "namespace": "ns"}});
        assert_eq!(
            storage_key_for("pods", true, &obj).unwrap(),
            "/registry/pods/ns/x"
        );
        let cobj = serde_json::json!({"metadata": {"name": "node-1"}});
        assert_eq!(
            storage_key_for("nodes", false, &cobj).unwrap(),
            "/registry/nodes/node-1"
        );
    }

    /// `isExpiredError` (reflector.go:1051-1058) = `IsResourceExpired ||
    /// IsGone`; `IsGone` has the code fallback (errors.go:574-583), so a bare
    /// 410 with no reason is expired, and the rendered text is irrelevant.
    #[test]
    fn is_expired_error_uses_typed_status() {
        use rusternetes_client::api_status::ApiStatusError;
        let typed = |code: u16, reason: &str, message: &str| {
            anyhow::Error::new(ApiStatusError {
                code,
                reason: reason.into(),
                message: message.into(),
                retry_after_seconds: None,
            })
        };
        assert!(is_expired_error(&typed(
            410,
            "",
            "request failed with status 410"
        )));
        assert!(is_expired_error(&typed(410, "Expired", "too old")));
        assert!(is_expired_error(&typed(
            0,
            "Expired",
            "wrapped text without the token"
        )));
        assert!(!is_expired_error(&typed(
            500,
            "InternalError",
            "Error from server (Gone): spoofed"
        )));
    }

    #[test]
    fn map_write_err_maps_unsupported_media_type() {
        let e = anyhow::Error::new(rusternetes_client::api_status::ApiStatusError {
            code: 415,
            reason: "UnsupportedMediaType".into(),
            message: "Error from server (UnsupportedMediaType): Apply not supported".into(),
            retry_after_seconds: None,
        });
        assert!(matches!(map_write_err(e), Error::UnsupportedMediaType(_)));
    }

    /// One-shot HTTP server answering every request with `status` + `body`.
    async fn spawn_status_server(status: &'static str, body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    if let Ok(n) = sock.read(&mut buf).await {
                        if n > 0 {
                            let resp = format!(
                                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = sock.write_all(resp.as_bytes()).await;
                        }
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    async fn post_err(status: &'static str, body: &'static str) -> Error {
        let url = spawn_status_server(status, body).await;
        let client = rusternetes_client::http::ApiClient::new(&url, true, None).unwrap();
        let e = client
            .post::<_, Value>("/api/v1/x", &serde_json::json!({}))
            .await
            .unwrap_err();
        map_write_err(e)
    }

    // IsUnsupportedMediaType (apimachinery errors.go:607-615): an unknown
    // reason with code 415 is still unsupported-media-type (#3360).
    #[tokio::test]
    async fn write_err_415_with_unknown_reason_is_unsupported_media_type() {
        let e = post_err(
            "415 Unsupported Media Type",
            r#"{"kind":"Status","status":"Failure","message":"nope","code":415}"#,
        )
        .await;
        assert!(matches!(e, Error::UnsupportedMediaType(_)), "{e:?}");
    }

    // Non-Status body: client-go NewGenericServerResponse (errors.go:437-493)
    // derives the reason from the code (415 -> UnsupportedMediaType).
    #[tokio::test]
    async fn write_err_415_plain_body_is_unsupported_media_type() {
        let e = post_err("415 Unsupported Media Type", "text").await;
        assert!(matches!(e, Error::UnsupportedMediaType(_)), "{e:?}");
    }

    // Known reason wins over the code (knownReasons gate, errors.go:47-68).
    #[tokio::test]
    async fn write_err_known_reason_not_overridden_by_code() {
        let e = post_err(
            "415 Unsupported Media Type",
            r#"{"kind":"Status","status":"Failure","reason":"Forbidden","message":"x","code":415}"#,
        )
        .await;
        assert!(matches!(e, Error::Forbidden(_)), "{e:?}");
    }

    // 409 on POST -> AlreadyExists, on PUT -> Conflict (errors.go:441-446);
    // 422 with no reason -> Invalid (IsInvalid code fallback).
    #[tokio::test]
    async fn write_err_code_fallbacks() {
        let e = post_err("409 Conflict", "").await;
        assert!(matches!(e, Error::AlreadyExists(_)), "{e:?}");
        let e = post_err(
            "422 Unprocessable Entity",
            r#"{"kind":"Status","status":"Failure","message":"bad","code":422}"#,
        )
        .await;
        assert!(matches!(e, Error::BadRequest(_)), "{e:?}");
        let e = post_err(
            "404 Not Found",
            r#"{"kind":"Status","status":"Failure","message":"m","code":404}"#,
        )
        .await;
        assert!(matches!(e, Error::NotFound(_)), "{e:?}");
    }

    // A transport failure (e.g. the kubelet POSTing a Node to an unresolvable
    // api-server host) arrives as an anyhow error whose *source* carries the
    // actionable detail (DNS / connect / TLS). `map_write_err` must preserve
    // that full chain, not just the top-level context — otherwise the kubelet
    // log reads only "Storage error: Failed to send POST request" and the real
    // cause (e.g. `dns error: failed to lookup address`) is lost. Regression
    // for the Mikronetes M2b worker-registration opaque-error hunt.
    #[test]
    fn map_write_err_preserves_source_chain() {
        let chained = anyhow::anyhow!("dns error: failed to lookup address information")
            .context("POST https://api-server:6443/api/v1/nodes failed");
        match map_write_err(chained) {
            Error::Storage(s) => {
                assert!(
                    s.contains("POST https://api-server:6443/api/v1/nodes failed"),
                    "top-level context must remain: {s}"
                );
                assert!(
                    s.contains("dns error: failed to lookup address information"),
                    "underlying source cause must survive into the Storage error: {s}"
                );
            }
            other => panic!("expected Error::Storage, got {other:?}"),
        }
    }

    // Same guarantee for the read path (`map_get_err`): a `GetError::Other`
    // carrying a chained anyhow must surface the full chain.
    #[test]
    fn map_get_err_preserves_source_chain() {
        let chained = anyhow::anyhow!("connection refused")
            .context("GET https://api-server:6443/api/v1/nodes/node-2 failed");
        match map_get_err(GetError::Other(chained)) {
            Error::Storage(s) => {
                assert!(
                    s.contains("GET https://api-server:6443"),
                    "context lost: {s}"
                );
                assert!(s.contains("connection refused"), "source lost: {s}");
            }
            other => panic!("expected Error::Storage, got {other:?}"),
        }
    }

    // ---- update_status optimistic-concurrency retry (#1638) ----------------
    //
    // Regression for #1638: running the rusternetes kubelet (API mode) against
    // a *vanilla* api-server, the node status write 409s when a concurrent
    // writer (node-lifecycle controller) bumps the node's resourceVersion
    // between our GET and the /status PUT. `ApiStorage::update_status` must
    // re-GET the fresh object and retry — mirroring the default
    // `Storage::update_status` and upstream kubelet's `nodeStatusUpdateRetry`
    // loop — instead of surfacing the first 409 as fatal (which crash-loops the
    // kubelet right after "Node registered successfully").

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// A minimal HTTP/1.1 server that fails the first `conflicts_before_ok`
    /// PUTs to `/status` with a 409 Conflict, then succeeds. Each response sets
    /// `Connection: close` so reqwest opens a fresh connection per request
    /// (one request per accepted socket — no keep-alive parsing needed).
    /// Returns the base URL and a shared counter of PUT-to-status attempts.
    async fn spawn_conflict_server(conflicts_before_ok: usize) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let put_attempts = Arc::new(AtomicUsize::new(0));
        let counter = put_attempts.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let req = read_request(&mut stream).await;
                let req_line = req.lines().next().unwrap_or("").to_string();
                let is_put_status = req_line.starts_with("PUT") && req_line.contains("/status");

                let node = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"rusternetes-node","resourceVersion":"1"},"status":{}}"#;

                if is_put_status {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < conflicts_before_ok {
                        // Kubernetes Status object with reason=Conflict → maps
                        // to Error::Conflict via format_status_error/map_write_err.
                        let body = r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","code":409,"message":"the object has been modified; please apply your changes to the latest version"}"#;
                        respond(&mut stream, "409 Conflict", body).await;
                    } else {
                        respond(&mut stream, "200 OK", node).await;
                    }
                } else {
                    // GET (object read) — hand back a fresh node each time.
                    respond(&mut stream, "200 OK", node).await;
                }
            }
        });

        (format!("http://{addr}"), put_attempts)
    }

    /// Read one HTTP request (headers + any Content-Length body) off the socket.
    async fn read_request(stream: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            let n = stream.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(hdr_end) = find_subslice(&buf, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_ascii_lowercase();
                let content_len = headers
                    .split("\r\n")
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if buf.len() >= hdr_end + 4 + content_len {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    async fn respond(stream: &mut TcpStream, status_line: &str, body: &str) {
        let resp = format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(resp.as_bytes()).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    #[tokio::test]
    async fn update_status_retries_on_conflict() {
        // Three 409s then success: within the retry budget → must succeed.
        let (base, attempts) = spawn_conflict_server(3).await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let node = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "rusternetes-node"},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        });

        let result: Result<Value> = storage
            .update_status("/registry/nodes/rusternetes-node", &node)
            .await;

        assert!(
            result.is_ok(),
            "update_status must retry through 409 Conflicts, got {result:?}"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            4,
            "expected 3 conflicting PUTs + 1 success"
        );
    }

    /// EventRecorder writes every Event with `*_with_ttl` (#2303). Over the
    /// API the server applies the event TTL itself (registry/core/event), so
    /// ApiStorage must accept a non-zero ttl rather than fall back to the
    /// Storage default, which refuses it and would drop every Event written
    /// by a component running in API mode.
    #[tokio::test]
    async fn update_with_ttl_is_accepted_over_the_api() {
        let (base, _attempts) = spawn_conflict_server(0).await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);
        let node = serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "rusternetes-node"}, "status": {}
        });
        let result: Result<Value> = storage
            .update_with_ttl("/registry/nodes/rusternetes-node", &node, 3600)
            .await;
        assert!(
            result.is_ok(),
            "ApiStorage must not refuse a ttl, got {result:?}"
        );
    }

    #[tokio::test]
    async fn update_status_gives_up_after_max_retries() {
        // A permanently-conflicting api-server must eventually surface Conflict
        // (bounded retry), not spin forever.
        let (base, _attempts) = spawn_conflict_server(usize::MAX).await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let node = serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "rusternetes-node"}, "status": {}
        });

        let result: Result<Value> = storage
            .update_status("/registry/nodes/rusternetes-node", &node)
            .await;

        assert!(
            matches!(result, Err(Error::Conflict(_))),
            "persistent conflict must surface as Error::Conflict, got {result:?}"
        );
    }

    /// A minimal server that captures the request and replies to any POST with a
    /// TokenRequest carrying `status.token`. Returns (base_url, shared capture).
    async fn spawn_tokenrequest_server(token: &'static str) -> (String, Arc<Mutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let cap = captured.clone();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let req = read_request(&mut stream).await;
                *cap.lock().await = req;
                let body = format!(
                    r#"{{"apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest","status":{{"token":"{token}","expirationTimestamp":"2026-01-01T00:00:00Z"}}}}"#
                );
                respond(&mut stream, "201 Created", &body).await;
            }
        });
        (format!("http://{addr}"), captured)
    }

    // Regression for #1648 chain: the kubelet must obtain projected SA tokens
    // from the api-server's TokenRequest subresource (a vanilla api-server only
    // trusts tokens it signed; self-minted ones 401 — breaking kindnet/CNI).
    // Assert create_sa_token POSTs serviceaccounts/<name>/token and returns the
    // api-server-issued token.
    #[tokio::test]
    async fn create_sa_token_posts_tokenrequest_and_extracts_token() {
        let (base, captured) = spawn_tokenrequest_server("ISSUED-BY-APISERVER").await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let tok = storage
            .create_sa_token("kube-system", "kindnet", &[], 3600, None)
            .await
            .expect("token issued");
        assert_eq!(tok, "ISSUED-BY-APISERVER");

        let req = captured.lock().await.clone();
        let req_line = req.lines().next().unwrap_or("");
        assert!(
            req_line.starts_with("POST")
                && req_line
                    .contains("/api/v1/namespaces/kube-system/serviceaccounts/kindnet/token"),
            "must POST the TokenRequest subresource, got: {req_line}"
        );
        assert!(
            req.contains("\"expirationSeconds\":3600"),
            "request body must carry expirationSeconds, got: {req}"
        );
    }

    // `create_sa_token_status` also returns `status.expirationTimestamp`, which
    // the CSI mounter serialises into `csi.storage.k8s.io/serviceAccount.tokens`.
    #[tokio::test]
    async fn create_sa_token_status_returns_the_expiration_timestamp() {
        let (base, _captured) = spawn_tokenrequest_server("ISSUED").await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);
        let (tok, exp) = storage
            .create_sa_token_status("ns", "sa", &[], 3600, None)
            .await
            .expect("token issued");
        assert_eq!(tok, "ISSUED");
        assert_eq!(exp, "2026-01-01T00:00:00Z");
    }

    // Regression for #1684: a projected SA token must be bound to the pod that
    // mounts it. Upstream's projected volume plugin sets
    // `spec.boundObjectRef = {apiVersion: v1, kind: Pod, name, uid}`
    // (pkg/volume/projected/projected.go), which is what makes the api-server
    // put `authentication.kubernetes.io/pod-name` / `pod-uid` / `node-name`
    // into the TokenReview's `status.user.extra`. Without the ref the issued
    // token carries no pod claims and `[sig-auth] ServiceAccounts should mount
    // an API token into pods [Conformance]` fails with
    // "expected single authentication.kubernetes.io/pod-name extra info item".
    #[tokio::test]
    async fn create_sa_token_binds_the_token_to_the_pod() {
        let (base, captured) = spawn_tokenrequest_server("BOUND-TOKEN").await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let tok = storage
            .create_sa_token(
                "svcaccounts-5118",
                "default",
                &[],
                3600,
                Some(("pod-service-account-601cae9b", "9a1f0e2c-uid")),
            )
            .await
            .expect("token issued");
        assert_eq!(tok, "BOUND-TOKEN");

        let req = captured.lock().await.clone();
        let body = req.split("\r\n\r\n").nth(1).unwrap_or("");
        let spec: Value = serde_json::from_str(body).unwrap_or_else(|e| {
            panic!("TokenRequest body must be JSON ({e}), got: {body}");
        });
        let bound = &spec["spec"]["boundObjectRef"];
        assert_eq!(bound["kind"], "Pod", "boundObjectRef.kind, got: {spec}");
        assert_eq!(
            bound["apiVersion"], "v1",
            "boundObjectRef.apiVersion, got: {spec}"
        );
        assert_eq!(
            bound["name"], "pod-service-account-601cae9b",
            "boundObjectRef.name, got: {spec}"
        );
        assert_eq!(
            bound["uid"], "9a1f0e2c-uid",
            "boundObjectRef.uid, got: {spec}"
        );
    }

    // A token requested with no owning pod (non-volume callers) must omit the
    // ref entirely rather than send an empty one — the api-server rejects a
    // boundObjectRef with no name.
    #[tokio::test]
    async fn create_sa_token_without_pod_omits_bound_object_ref() {
        let (base, captured) = spawn_tokenrequest_server("UNBOUND-TOKEN").await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        storage
            .create_sa_token("kube-system", "kindnet", &[], 3600, None)
            .await
            .expect("token issued");

        let req = captured.lock().await.clone();
        assert!(
            !req.contains("boundObjectRef"),
            "unbound TokenRequest must not carry a boundObjectRef, got: {req}"
        );
    }

    #[tokio::test]
    async fn update_subresource_puts_namespace_finalize_path() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let capture = captured.clone();

        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let request = read_request(&mut stream).await;
                *capture.lock().await = request;
                let body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"conformance"},"spec":{"finalizers":[]}}"#;
                respond(&mut stream, "200 OK", body).await;
            }
        });

        let client = Arc::new(ApiClient::new(&format!("http://{addr}"), true, None).unwrap());
        let storage = ApiStorage::new(client);
        let namespace = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "conformance"},
            "spec": {"finalizers": []}
        });

        let updated: Value = storage
            .update_subresource("/registry/namespaces/conformance", "finalize", &namespace)
            .await
            .expect("namespace finalize update should succeed");
        assert_eq!(updated["metadata"]["name"], "conformance");

        let request = captured.lock().await;
        let request_line = request.lines().next().unwrap_or("");
        assert!(
            request_line.starts_with("PUT ")
                && request_line.contains("/api/v1/namespaces/conformance/finalize"),
            "namespace finalization must target /finalize, got: {request_line}"
        );
    }

    /// `Storage::patch_strategic_merge` must reach the api-server as a real
    /// PATCH with the strategic-merge content type, not a GET + PUT
    /// (upstream `nodeutil.PatchNodeCIDRs`,
    /// `component-helpers/node/util/cidr.go:54`: `Nodes().Patch(ctx, name,
    /// types.StrategicMergePatchType, patchBytes, ...)`).
    #[tokio::test]
    async fn patch_strategic_merge_sends_a_strategic_merge_patch() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(h) = text.find("\r\n\r\n") {
                    let len = text[..h]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if buf.len() >= h + 4 + len {
                        break;
                    }
                }
            }
            let body = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"},"spec":{"podCIDR":"10.0.0.0/24","podCIDRs":["10.0.0.0/24"]}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf).to_string()
        });
        let client = Arc::new(ApiClient::new(&format!("http://{addr}"), false, None).unwrap());
        let storage = ApiStorage::new(client);
        let patch =
            serde_json::json!({"spec": {"podCIDR": "10.0.0.0/24", "podCIDRs": ["10.0.0.0/24"]}});
        let out: Value = storage
            .patch_strategic_merge("/registry/nodes/n1", &patch)
            .await
            .unwrap();
        assert_eq!(out["spec"]["podCIDR"], "10.0.0.0/24");
        let req = server.await.unwrap();
        let first = req.lines().next().unwrap();
        assert_eq!(first, "PATCH /api/v1/nodes/n1 HTTP/1.1", "{req}");
        assert!(
            req.to_ascii_lowercase()
                .contains("content-type: application/strategic-merge-patch+json"),
            "{req}"
        );
        assert!(req.contains(r#""podCIDRs":["10.0.0.0/24"]"#), "{req}");
    }

    /// `Apply(..., ApplyOptions{FieldManager, Force})` (here `ApplyStatus`) is a PATCH with the apply
    /// content type and `fieldManager`/`force` query parameters.
    #[tokio::test]
    async fn apply_sends_an_apply_patch_with_manager_and_force() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(h) = text.find("\r\n\r\n") {
                    let len = text[..h]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if buf.len() >= h + 4 + len {
                        break;
                    }
                }
            }
            let body =
                r#"{"apiVersion":"v1","kind":"ClusterRole","metadata":{"name":"agg"},"rules":[]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf).to_string()
        });
        let client = Arc::new(ApiClient::new(&format!("http://{addr}"), false, None).unwrap());
        let storage = ApiStorage::new(client);
        let cfg = serde_json::json!({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "agg"}, "rules": []});
        let out: Value = storage
            .apply(
                "/registry/clusterroles/agg",
                &cfg,
                "clusterrole-aggregation-controller",
                true,
                Some("status"),
            )
            .await
            .unwrap();
        assert_eq!(out["metadata"]["name"], "agg");
        let req = server.await.unwrap();
        let first = req.lines().next().unwrap();
        assert_eq!(
            first,
            "PATCH /apis/rbac.authorization.k8s.io/v1/clusterroles/agg/status?fieldManager=clusterrole-aggregation-controller&force=true HTTP/1.1",
            "{req}"
        );
        assert!(
            req.to_ascii_lowercase()
                .contains("content-type: application/apply-patch+json"),
            "{req}"
        );
    }

    /// `patch_status_strategic_merge` PATCHes `/status` with the
    /// strategic-merge content type (nodeutil.PatchNodeStatus,
    /// component-helpers/node/util/status.go:38).
    #[tokio::test]
    async fn patch_status_strategic_merge_patches_the_status_subresource() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(h) = text.find("\r\n\r\n") {
                    let len = text[..h]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if buf.len() >= h + 4 + len {
                        break;
                    }
                }
            }
            let body = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf).to_string()
        });
        let client = Arc::new(ApiClient::new(&format!("http://{addr}"), false, None).unwrap());
        let storage = ApiStorage::new(client);
        let patch = serde_json::json!({"status": {"phase": "x"}});
        let full = serde_json::json!({});
        let _: Value = storage
            .patch_status_strategic_merge("/registry/nodes/n1", &patch, &full)
            .await
            .unwrap();
        let req = server.await.unwrap();
        assert_eq!(
            req.lines().next().unwrap(),
            "PATCH /api/v1/nodes/n1/status HTTP/1.1",
            "{req}"
        );
        assert!(
            req.to_ascii_lowercase()
                .contains("content-type: application/strategic-merge-patch+json"),
            "{req}"
        );
    }

    /// A server answering every request with `body` at `status`, recording each
    /// request line.
    async fn spawn_fixed_server(
        status: &'static str,
        body: &'static str,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let req = read_request(&mut stream).await;
                log.lock()
                    .await
                    .push(req.lines().next().unwrap_or("").to_string());
                respond(&mut stream, status, body).await;
            }
        });
        (format!("http://{addr}"), seen)
    }

    /// `current_revision` was a constant 0: a caller resuming from it would
    /// start at the beginning of time. It must be the api-server's list
    /// `resourceVersion` (`reflector.go:677`).
    #[tokio::test]
    async fn current_revision_is_the_servers_list_resource_version() {
        let (base, seen) = spawn_fixed_server(
            "200 OK",
            r#"{"apiVersion":"v1","kind":"NamespaceList","metadata":{"resourceVersion":"4242"},"items":[]}"#,
        )
        .await;
        let storage = ApiStorage::new(Arc::new(ApiClient::new(&base, true, None).unwrap()));
        assert_eq!(storage.current_revision().await.unwrap(), 4242);
        let line = seen.lock().await[0].clone();
        assert!(
            line.starts_with("GET /api/v1/namespaces?limit=1 "),
            "{line}"
        );
    }

    #[tokio::test]
    async fn current_revision_errors_when_the_list_has_no_resource_version() {
        let (base, _) = spawn_fixed_server(
            "200 OK",
            r#"{"apiVersion":"v1","kind":"NamespaceList","metadata":{},"items":[]}"#,
        )
        .await;
        let storage = ApiStorage::new(Arc::new(ApiClient::new(&base, true, None).unwrap()));
        assert!(storage.current_revision().await.is_err());
    }

    /// `is_revision_compacted` was a constant false. An `Expired` 410 for the
    /// exact revision means compacted (`isExpiredError`, `reflector.go:1051`).
    #[tokio::test]
    async fn is_revision_compacted_true_on_expired_410() {
        let (base, seen) = spawn_fixed_server(
            "410 Gone",
            r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Expired","code":410,"message":"too old resource version: 5 (100)"}"#,
        )
        .await;
        let storage = ApiStorage::new(Arc::new(ApiClient::new(&base, true, None).unwrap()));
        assert!(storage.is_revision_compacted(5).await.unwrap());
        let line = seen.lock().await[0].clone();
        assert!(
            line.contains("resourceVersion=5") && line.contains("resourceVersionMatch=Exact"),
            "{line}"
        );
    }

    #[tokio::test]
    async fn is_revision_compacted_false_when_the_revision_is_served() {
        let (base, _) = spawn_fixed_server(
            "200 OK",
            r#"{"apiVersion":"v1","kind":"NamespaceList","metadata":{"resourceVersion":"5"},"items":[]}"#,
        )
        .await;
        let storage = ApiStorage::new(Arc::new(ApiClient::new(&base, true, None).unwrap()));
        assert!(!storage.is_revision_compacted(5).await.unwrap());
    }

    /// A 500 is not evidence of compaction: surface it rather than answering
    /// `false`, which would let a caller trust a revision it could not verify.
    #[tokio::test]
    async fn is_revision_compacted_surfaces_other_failures() {
        let (base, _) = spawn_fixed_server(
            "500 Internal Server Error",
            r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500,"message":"boom"}"#,
        )
        .await;
        let storage = ApiStorage::new(Arc::new(ApiClient::new(&base, true, None).unwrap()));
        assert!(storage.is_revision_compacted(5).await.is_err());
    }

    // ---- list_at_revision / list_paginated go to the server (#2709) --------
    //
    // Both used the trait defaults: list everything, sort, slice in memory (and
    // ignore the revision). A reflector's pager sends limit/continue and
    // resourceVersion(Match) to the server instead
    // (`client-go/tools/pager/pager.go:80-100`).

    const POD_LIST_PAGE: &str = r#"{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"77","continue":"abc+/="},"items":[{"apiVersion":"v1","kind":"Pod","metadata":{"name":"a","namespace":"default"}}]}"#;

    fn pod_storage(base: &str) -> ApiStorage {
        ApiStorage::new(Arc::new(ApiClient::new(base, true, None).unwrap()))
    }

    #[tokio::test]
    async fn list_at_revision_asks_the_server_for_that_exact_revision() {
        let (base, seen) = spawn_fixed_server("200 OK", POD_LIST_PAGE).await;
        let storage = pod_storage(&base);
        let items = Storage::list_at_revision::<Value>(&storage, "/registry/pods/default/", 42)
            .await
            .unwrap();
        assert_eq!(items.len(), 1);
        let line = seen.lock().await.last().unwrap().clone();
        assert!(
            line.contains("resourceVersion=42") && line.contains("resourceVersionMatch=Exact"),
            "{line}"
        );
    }

    #[tokio::test]
    async fn list_paginated_sends_limit_and_continue_and_returns_the_servers_token() {
        let (base, seen) = spawn_fixed_server("200 OK", POD_LIST_PAGE).await;
        let storage = pod_storage(&base);
        let (items, next) = Storage::list_paginated::<Value>(
            &storage,
            "/registry/pods/default/",
            1,
            Some("tok+/="),
        )
        .await
        .unwrap();
        assert_eq!(items.len(), 1);
        // The server's own token, passed through untouched.
        assert_eq!(next.as_deref(), Some("abc+/="));
        let line = seen.lock().await.last().unwrap().clone();
        assert!(line.contains("limit=1"), "{line}");
        assert!(line.contains("continue=tok%2B%2F%3D"), "{line}");
    }

    #[tokio::test]
    async fn list_paginated_maps_an_expired_continue_to_gone() {
        let (base, _) = spawn_fixed_server(
            "410 Gone",
            r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Expired","code":410,"message":"too old resource version"}"#,
        )
        .await;
        let storage = pod_storage(&base);
        let err =
            Storage::list_paginated::<Value>(&storage, "/registry/pods/default/", 1, Some("t"))
                .await
                .unwrap_err();
        assert!(matches!(err, Error::Gone(_)), "{err:?}");
    }

    // ---- watch resume (#2708) ------------------------------------------
    //
    // Upstream: client-go/tools/cache/reflector.go. `watch` (:520-535) sends
    // `ResourceVersion: r.LastSyncResourceVersion()` and
    // `AllowWatchBookmarks: true`; `watchHandler` (:962-970) takes a
    // `watch.Bookmark`'s rv as `setLastSyncResourceVersion`; an expired watch
    // (:561 `isExpiredError`) ends the watch so the caller relists.
    // reflector_test.go `TestReflectorListAndWatchWithErrors` /
    // `TestReflectorWatchHandler` pin: rv carried across a re-watch, and
    // bookmarks advance rv without surfacing as store events.

    fn ev(kind: &str, name: &str, rv: &str) -> String {
        format!(
            r#"{{"type":"{kind}","object":{{"apiVersion":"v1","kind":"ConfigMap","metadata":{{"name":"{name}","namespace":"default","resourceVersion":"{rv}"}}}}}}"#
        ) + "\n"
    }

    const EXPIRED: &str = r#"{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","status":"Failure","message":"too old resource version: 1 (9)","reason":"Expired","code":410}}"#;

    /// Serves `bodies[i]` to the i-th connection (then empty bodies), logging
    /// each request line.
    async fn spawn_watch_server(bodies: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            let mut i = 0;
            while let Ok((mut stream, _)) = listener.accept().await {
                let req = read_request(&mut stream).await;
                log.lock()
                    .await
                    .push(req.lines().next().unwrap_or("").to_string());
                let body = bodies.get(i).cloned().unwrap_or_default();
                i += 1;
                respond(&mut stream, "200 OK", &body).await;
            }
        });
        (format!("http://{addr}"), seen)
    }

    fn storage_for(base: &str) -> ApiStorage {
        ApiStorage::new(Arc::new(ApiClient::new(base, true, None).unwrap()))
    }

    const CM_PREFIX: &str = "/registry/configmaps/default/";

    /// `watch_from_revision` ignored its revision: the watch must carry
    /// `resourceVersion=<rv>` (`reflector.go:526-527`) and ask for bookmarks
    /// (`:534`).
    #[tokio::test]
    async fn watch_from_revision_sends_the_revision_and_asks_for_bookmarks() {
        let (base, seen) = spawn_watch_server(vec![ev("ADDED", "a", "11")]).await;
        let mut w = storage_for(&base)
            .watch_from_revision(CM_PREFIX, 10)
            .await
            .unwrap();
        let first = w.next().await.expect("an event").expect("not an error");
        assert!(matches!(first, WatchEvent::Added(..)), "{first:?}");
        let line = seen.lock().await[0].clone();
        assert!(line.contains("resourceVersion=10"), "{line}");
        assert!(line.contains("allowWatchBookmarks=true"), "{line}");
    }

    /// A compacted revision is the in-stream 410 `Expired` envelope; the
    /// subscriber must be told (`isExpiredError`, `reflector.go:1051-1058`)
    /// so it relists, not silently see an empty stream.
    #[tokio::test]
    async fn watch_from_revision_surfaces_expiry_as_gone() {
        let (base, _seen) = spawn_watch_server(vec![format!("{EXPIRED}\n")]).await;
        let mut w = storage_for(&base)
            .watch_from_revision(CM_PREFIX, 1)
            .await
            .unwrap();
        match w.next().await {
            Some(Err(Error::Gone(_))) => {}
            other => panic!("expected Err(Gone), got {other:?}"),
        }
    }

    /// The shared upstream drops a BOOKMARK for subscribers but remembers its
    /// rv: when the watch ends it is re-established `resourceVersion=<bookmark
    /// rv>` and the subscriber's stream carries on (no close, no relist).
    #[tokio::test]
    async fn shared_watch_resumes_from_the_bookmark_revision() {
        let first = ev("ADDED", "a", "5") + &ev("BOOKMARK", "", "9");
        let second = ev("ADDED", "b", "10");
        let (base, seen) = spawn_watch_server(vec![first, second]).await;
        let mut w = storage_for(&base).watch(CM_PREFIX).await.unwrap();
        let mut names = Vec::new();
        for _ in 0..2 {
            match w.next().await.expect("stream stayed open").unwrap() {
                WatchEvent::Added(k, _) => names.push(k),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(
            names,
            vec![
                "/registry/configmaps/default/a",
                "/registry/configmaps/default/b"
            ]
        );
        let reqs = seen.lock().await.clone();
        assert!(!reqs[0].contains("resourceVersion"), "{reqs:?}");
        assert!(reqs[0].contains("allowWatchBookmarks=true"), "{reqs:?}");
        assert!(reqs[1].contains("resourceVersion=9"), "{reqs:?}");
    }

    /// An expired resume ends the shared stream so subscribers relist
    /// (`reflector.go:561`), rather than looping on a dead revision.
    #[tokio::test]
    async fn shared_watch_ends_on_expiry() {
        let first = ev("ADDED", "a", "5");
        let second = format!("{EXPIRED}\n");
        let (base, _seen) = spawn_watch_server(vec![first, second]).await;
        let mut w = storage_for(&base).watch(CM_PREFIX).await.unwrap();
        assert!(matches!(w.next().await, Some(Ok(WatchEvent::Added(..)))));
        assert!(w.next().await.is_none(), "stream must close for a relist");
    }
}
