//! Tests for `kubectl cordon|uncordon|drain`
//! (`crates/kubectl/src/commands/drain.rs`).
//!
//! The cases mirror upstream's
//! `staging/src/k8s.io/kubectl/pkg/cmd/drain/drain_test.go` (`TestDrain`,
//! `TestDeletePods`), `pkg/drain/filter_test.go`, `pkg/drain/cordon_test.go`
//! and `pkg/drain/default_test.go`, driven against an in-process API server.

use rusternetes_client::http::ApiClient;
use rusternetes_kubectl::commands::drain::{
    execute_cordon, format_go_duration, parse_go_duration, DrainCmdOptions, DrainFlags,
    DryRunStrategy, IoStreams, Mode, PodDeleteStatus,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// (method, path-and-query, body)
type Seen = (String, String, String);
type Handler = Arc<dyn Fn(&str, &str, &str) -> (u16, Value) + Send + Sync>;

struct Mock {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Mock {
    fn requests(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    fn client(&self) -> ApiClient {
        ApiClient::new(&self.url, false, None).unwrap()
    }
}

async fn start(handler: Handler) -> Mock {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let seen2 = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let handler = handler.clone();
            let seen = seen2.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let handler = handler.clone();
                    let seen = seen.clone();
                    async move {
                        let method = req.method().to_string();
                        let pq = req
                            .uri()
                            .path_and_query()
                            .map(|p| p.to_string())
                            .unwrap_or_default();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let body = String::from_utf8_lossy(&body).to_string();
                        seen.lock()
                            .unwrap()
                            .push((method.clone(), pq.clone(), body.clone()));
                        let (code, v) = handler(&method, &pq, &body);
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(code)
                                .header("content-type", "application/json")
                                .body(Full::new(Bytes::from(v.to_string())))
                                .unwrap(),
                        )
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    Mock { url, seen }
}

fn capture() -> (IoStreams, Arc<Mutex<String>>, Arc<Mutex<String>>) {
    let out: Arc<Mutex<String>> = Arc::default();
    let err: Arc<Mutex<String>> = Arc::default();
    let (o, e) = (out.clone(), err.clone());
    (
        IoStreams {
            out: Arc::new(move |s| o.lock().unwrap().push_str(s)),
            err_out: Arc::new(move |s| e.lock().unwrap().push_str(s)),
        },
        out,
        err,
    )
}

fn status(code: u16, reason: &str, message: &str) -> Value {
    json!({"kind":"Status","apiVersion":"v1","status":"Failure","message":message,
           "reason":reason,"code":code})
}

fn node(name: &str, unschedulable: bool) -> Value {
    let mut n = json!({"apiVersion":"v1","kind":"Node","metadata":{"name":name},"spec":{}});
    if unschedulable {
        n["spec"]["unschedulable"] = json!(true);
    }
    n
}

fn pod(name: &str, owner_kind: Option<(&str, &str)>) -> Value {
    let mut p = json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"name":name,"namespace":"ns","uid":format!("uid-{name}")},
        "spec":{"nodeName":"n1"},"status":{"phase":"Running"}
    });
    if let Some((kind, api_version)) = owner_kind {
        p["metadata"]["ownerReferences"] = json!([{
            "apiVersion": api_version, "kind": kind, "name": "owner",
            "uid": "owner-uid", "controller": true
        }]);
    }
    p
}

fn discovery_with_eviction() -> Value {
    json!({"kind":"APIResourceList","groupVersion":"v1","resources":[
        {"name":"pods","kind":"Pod","namespaced":true},
        {"name":"pods/eviction","kind":"Eviction","group":"policy","version":"v1","namespaced":true}
    ]})
}

fn discovery_without_eviction() -> Value {
    json!({"kind":"APIResourceList","groupVersion":"v1","resources":[
        {"name":"pods","kind":"Pod","namespaced":true}
    ]})
}

fn flags() -> DrainFlags {
    DrainFlags::default()
}

async fn run_cordon(
    mock: &Mock,
    mode: Mode,
    args: &[&str],
    f: &DrainFlags,
) -> (anyhow::Result<()>, String, String) {
    let client = mock.client();
    let (streams, out, err) = capture();
    let mut o = DrainCmdOptions::new(mode, &client, streams);
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let r = match o.complete(&args, f).await {
        Ok(()) => o.run_cordon_or_uncordon(mode == Mode::Cordon).await,
        Err(e) => Err(e),
    };
    let (o, e) = (out.lock().unwrap().clone(), err.lock().unwrap().clone());
    (r, o, e)
}

// ---- cordon_test.go / drain_test.go TestCordon ------------------------------

#[tokio::test]
async fn cordon_patches_unschedulable_true() {
    let mock = start(Arc::new(|m, p, _| match (m, p) {
        ("GET", "/api/v1/nodes/n1") => (200, node("n1", false)),
        ("PATCH", "/api/v1/nodes/n1") => (200, node("n1", true)),
        _ => (500, status(500, "InternalError", p)),
    }))
    .await;
    let (r, out, _) = run_cordon(&mock, Mode::Cordon, &["n1"], &flags()).await;
    r.unwrap();
    assert_eq!(out, "node/n1 cordoned\n");
    let reqs = mock.requests();
    let patch = reqs.iter().find(|r| r.0 == "PATCH").unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&patch.2).unwrap(),
        json!({"spec":{"unschedulable":true}})
    );
}

#[tokio::test]
async fn uncordon_patches_unschedulable_null() {
    let mock = start(Arc::new(|m, p, _| match (m, p) {
        ("GET", "/api/v1/nodes/n1") => (200, node("n1", true)),
        ("PATCH", "/api/v1/nodes/n1") => (200, node("n1", false)),
        _ => (500, status(500, "InternalError", p)),
    }))
    .await;
    let (r, out, _) = run_cordon(&mock, Mode::Uncordon, &["n1"], &flags()).await;
    r.unwrap();
    assert_eq!(out, "node/n1 uncordoned\n");
    let reqs = mock.requests();
    let patch = reqs.iter().find(|r| r.0 == "PATCH").unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&patch.2).unwrap(),
        json!({"spec":{"unschedulable":null}})
    );
}

#[tokio::test]
async fn cordon_already_cordoned_does_not_patch() {
    let mock = start(Arc::new(|_, _, _| (200, node("n1", true)))).await;
    let (r, out, _) = run_cordon(&mock, Mode::Cordon, &["n1"], &flags()).await;
    r.unwrap();
    assert_eq!(out, "node/n1 already cordoned\n");
    assert!(mock.requests().iter().all(|r| r.0 != "PATCH"));

    let (r, out, _) = run_cordon(&mock, Mode::Uncordon, &["n1"], &flags()).await;
    r.unwrap();
    // The mock reports the node as cordoned, so uncordon must patch.
    assert_eq!(out, "node/n1 uncordoned\n");
}

#[tokio::test]
async fn cordon_dry_run_client_and_server() {
    let mock = start(Arc::new(|m, p, _| {
        match (m, p.split('?').next().unwrap()) {
            ("GET", "/api/v1/nodes/n1") => (200, node("n1", false)),
            ("PATCH", "/api/v1/nodes/n1") => (200, node("n1", true)),
            _ => (500, status(500, "InternalError", p)),
        }
    }))
    .await;
    let mut f = flags();
    f.dry_run = "client".into();
    let (r, out, _) = run_cordon(&mock, Mode::Cordon, &["n1"], &f).await;
    r.unwrap();
    assert_eq!(out, "node/n1 cordoned (dry run)\n");
    assert!(mock.requests().iter().all(|r| r.0 != "PATCH"));

    f.dry_run = "server".into();
    let (r, out, _) = run_cordon(&mock, Mode::Cordon, &["n1"], &f).await;
    r.unwrap();
    assert_eq!(out, "node/n1 cordoned (server dry run)\n");
    assert!(mock
        .requests()
        .iter()
        .any(|r| r.0 == "PATCH" && r.1 == "/api/v1/nodes/n1?dryRun=All"));
}

#[tokio::test]
async fn cordon_patch_failure_is_reported_and_not_printed_as_changed() {
    let mock = start(Arc::new(|m, p, _| match (m, p) {
        ("GET", "/api/v1/nodes/n1") => (200, node("n1", false)),
        ("PATCH", _) => (409, status(409, "Conflict", "the object has been modified")),
        _ => (500, status(500, "InternalError", p)),
    }))
    .await;
    let (r, out, err) = run_cordon(&mock, Mode::Cordon, &["n1"], &flags()).await;
    r.unwrap();
    assert_eq!(out, "");
    assert_eq!(
        err,
        "error: unable to cordon node \"n1\": the object has been modified\n"
    );
}

#[tokio::test]
async fn cordon_by_selector_follows_continue() {
    let mock = start(Arc::new(|m, p, _| {
        if m == "GET" && p.starts_with("/api/v1/nodes?") {
            if p.contains("continue=tok") {
                return (200, json!({"items":[node("b", true)],"metadata":{}}));
            }
            return (
                200,
                json!({"items":[node("a", true)],"metadata":{"continue":"tok"}}),
            );
        }
        (500, status(500, "InternalError", p))
    }))
    .await;
    let mut f = flags();
    f.selector = Some("role=worker".into());
    f.chunk_size = 1;
    let (r, out, _) = run_cordon(&mock, Mode::Cordon, &[], &f).await;
    r.unwrap();
    assert_eq!(out, "node/a already cordoned\nnode/b already cordoned\n");
    let reqs = mock.requests();
    assert!(reqs[0].1.contains("labelSelector=role%3Dworker"));
    assert!(reqs[0].1.contains("limit=1"));
}

// ---- Complete() validation (drain.go) --------------------------------

#[tokio::test]
async fn complete_usage_errors() {
    let mock = start(Arc::new(|_, _, _| (200, node("n1", false)))).await;
    let (r, _, _) = run_cordon(&mock, Mode::Drain, &[], &flags()).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "USAGE: drain NODE [flags]\nSee 'kubectl drain -h' for help and examples"
    );

    let mut f = flags();
    f.selector = Some("a=b".into());
    let (r, _, _) = run_cordon(&mock, Mode::Drain, &["n1"], &f).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "error: cannot specify both a node name and a --selector option\nSee 'kubectl drain -h' for help and examples"
    );

    let mut f = flags();
    f.pod_selector = "a=(".into();
    let (r, _, _) = run_cordon(&mock, Mode::Drain, &["n1"], &f).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "--pod-selector=<pod_selector> must be a valid label selector"
    );

    let mut f = flags();
    f.dry_run = "bogus".into();
    let (r, _, _) = run_cordon(&mock, Mode::Drain, &["n1"], &f).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "Invalid dry-run value (bogus). Must be \"none\", \"server\", or \"client\"."
    );
}

#[tokio::test]
async fn complete_unknown_node_and_wrong_resource() {
    let mock = start(Arc::new(|_, p, _| {
        (404, status(404, "NotFound", &format!("nope {p}")))
    }))
    .await;
    let (r, _, _) = run_cordon(&mock, Mode::Cordon, &["missing"], &flags()).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "Error from server (NotFound): nodes \"missing\" not found"
    );
    let (r, _, _) = run_cordon(&mock, Mode::Cordon, &["pod/foo"], &flags()).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "error: expected resource of type node, got \"pod\""
    );
}

#[test]
fn dry_run_strategy_parse() {
    // GetDryRunStrategy (helpers.go)
    assert_eq!(DryRunStrategy::parse("none").unwrap(), DryRunStrategy::None);
    assert_eq!(
        DryRunStrategy::parse("client").unwrap(),
        DryRunStrategy::Client
    );
    assert_eq!(
        DryRunStrategy::parse("server").unwrap(),
        DryRunStrategy::Server
    );
    assert_eq!(
        DryRunStrategy::parse("unchanged").unwrap(),
        DryRunStrategy::Client
    );
    assert_eq!(
        DryRunStrategy::parse("true").unwrap(),
        DryRunStrategy::Client
    );
    assert_eq!(
        DryRunStrategy::parse("false").unwrap(),
        DryRunStrategy::None
    );
}

#[test]
fn go_duration_parse_and_format() {
    assert_eq!(parse_go_duration("0").unwrap(), Duration::ZERO);
    assert_eq!(parse_go_duration("30s").unwrap(), Duration::from_secs(30));
    assert_eq!(
        parse_go_duration("1h30m").unwrap(),
        Duration::from_secs(5400)
    );
    assert_eq!(
        parse_go_duration("1.5s").unwrap(),
        Duration::from_millis(1500)
    );
    assert!(parse_go_duration("10").is_err());
    assert!(parse_go_duration("abc").is_err());
    assert_eq!(format_go_duration(Duration::from_secs(5)), "5s");
    assert_eq!(format_go_duration(Duration::from_secs(60)), "1m0s");
    assert_eq!(format_go_duration(Duration::from_secs(3661)), "1h1m1s");
    assert_eq!(format_go_duration(Duration::from_millis(10)), "10ms");
    assert_eq!(format_go_duration(Duration::from_millis(1500)), "1.5s");
}

// ---- filter_test.go / drain_test.go TestDrain pod filters --------------------

/// A server that has one node and the given pods bound to it, plus the
/// optional DaemonSet lookups.
fn drain_server(
    pods: Vec<Value>,
    eviction: bool,
    pod_gone: bool,
    daemonset_exists: bool,
) -> Handler {
    Arc::new(move |m, p, _| {
        let path = p.split('?').next().unwrap();
        match (m, path) {
            ("GET", "/api/v1/nodes/n1") => (200, node("n1", false)),
            ("PATCH", "/api/v1/nodes/n1") => (200, node("n1", true)),
            ("GET", "/api/v1/pods") => (200, json!({"items": pods, "metadata": {}})),
            ("GET", "/api/v1") => (
                200,
                if eviction {
                    discovery_with_eviction()
                } else {
                    discovery_without_eviction()
                },
            ),
            ("GET", x) if x.starts_with("/apis/apps/v1/namespaces/ns/daemonsets/") => {
                if daemonset_exists {
                    (200, json!({"kind":"DaemonSet","metadata":{"name":"owner"}}))
                } else {
                    (
                        404,
                        status(404, "NotFound", "daemonsets.apps \"owner\" not found"),
                    )
                }
            }
            ("POST", x) if x.ends_with("/eviction") => {
                (201, json!({"kind":"Status","status":"Success"}))
            }
            ("DELETE", _) => (200, json!({"kind":"Status","status":"Success"})),
            ("GET", x) if x.starts_with("/api/v1/namespaces/ns/pods/") => {
                if pod_gone {
                    (404, status(404, "NotFound", "pod gone"))
                } else {
                    let name = x.rsplit('/').next().unwrap();
                    (200, pod(name, Some(("ReplicaSet", "apps/v1"))))
                }
            }
            _ => (500, status(500, "InternalError", &format!("{m} {p}"))),
        }
    })
}

async fn run_drain(
    mock: &Mock,
    f: &DrainFlags,
    tweak: impl FnOnce(&mut DrainCmdOptions<'_>),
) -> (anyhow::Result<()>, String, String) {
    let client = mock.client();
    let (streams, out, err) = capture();
    let mut o = DrainCmdOptions::new(Mode::Drain, &client, streams);
    let args = vec!["n1".to_string()];
    let r = match o.complete(&args, f).await {
        Ok(()) => {
            tweak(&mut o);
            o.run_drain().await
        }
        Err(e) => Err(e),
    };
    let (o, e) = (out.lock().unwrap().clone(), err.lock().unwrap().clone());
    (r, o, e)
}

#[tokio::test]
async fn drain_evicts_replicaset_pod() {
    let mock = start(drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        true,
        true,
    ))
    .await;
    let mut f = flags();
    f.grace_period = 7;
    let (r, out, err) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    assert_eq!(err, "");
    assert_eq!(
        out,
        "node/n1 cordoned\nevicting pod ns/p1\npod/p1 evicted\nnode/n1 drained\n"
    );
    let reqs = mock.requests();
    let list = reqs
        .iter()
        .find(|r| r.1.starts_with("/api/v1/pods?"))
        .unwrap();
    assert!(list.1.contains("fieldSelector=spec.nodeName%3Dn1"));
    let ev = reqs.iter().find(|r| r.0 == "POST").unwrap();
    assert_eq!(ev.1, "/api/v1/namespaces/ns/pods/p1/eviction");
    assert_eq!(
        serde_json::from_str::<Value>(&ev.2).unwrap(),
        json!({"apiVersion":"policy/v1","kind":"Eviction",
               "metadata":{"name":"p1","namespace":"ns"},
               "deleteOptions":{"gracePeriodSeconds":7}})
    );
}

#[tokio::test]
async fn drain_deletes_when_eviction_unsupported_or_disabled() {
    // (eviction supported by the server, --disable-eviction)
    for (supported, disable) in [(false, false), (true, true)] {
        let mock = start(drain_server(
            vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
            supported,
            true,
            true,
        ))
        .await;
        let mut f = flags();
        f.disable_eviction = disable;
        let (r, out, _) = run_drain(&mock, &f, |_| {}).await;
        r.unwrap();
        assert_eq!(out, "node/n1 cordoned\npod/p1 deleted\nnode/n1 drained\n");
        let reqs = mock.requests();
        let del = reqs.iter().find(|r| r.0 == "DELETE").unwrap();
        assert_eq!(del.1, "/api/v1/namespaces/ns/pods/p1");
        assert!(reqs.iter().all(|r| r.0 != "POST"));
    }
}

#[tokio::test]
async fn drain_falls_back_to_v1beta1_eviction() {
    let mock = start(Arc::new(|m, p, _| {
        match (m, p.split('?').next().unwrap()) {
            ("GET", "/api/v1") => (
                200,
                json!({"resources":[{"name":"pods/eviction","kind":"Eviction",
                                     "group":"policy","version":"v1beta1"}]}),
            ),
            ("GET", "/api/v1/pods") => (
                200,
                json!({"items":[pod("p1", Some(("ReplicaSet","apps/v1")))],"metadata":{}}),
            ),
            ("GET", "/api/v1/nodes/n1") => (200, node("n1", true)),
            ("POST", _) => (201, json!({})),
            _ => (404, status(404, "NotFound", "gone")),
        }
    }))
    .await;
    let (r, _, _) = run_drain(&mock, &flags(), |_| {}).await;
    r.unwrap();
    let reqs = mock.requests();
    let ev = reqs.iter().find(|r| r.0 == "POST").unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ev.2).unwrap()["apiVersion"],
        "policy/v1beta1"
    );
}

#[tokio::test]
async fn drain_server_dry_run_evicts_with_dry_run_all() {
    let mock = start(drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        false,
        true,
    ))
    .await;
    let mut f = flags();
    f.dry_run = "server".into();
    let (r, out, _) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    assert_eq!(
        out,
        "node/n1 cordoned (server dry run)\nevicting pod ns/p1 (server dry run)\nnode/n1 drained (server dry run)\n"
    );
    let reqs = mock.requests();
    let ev = reqs.iter().find(|r| r.0 == "POST").unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ev.2).unwrap()["deleteOptions"],
        json!({"dryRun":["All"]})
    );
}

#[tokio::test]
async fn drain_client_dry_run_only_lists() {
    let mock = start(drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        false,
        true,
    ))
    .await;
    let mut f = flags();
    f.dry_run = "client".into();
    let (r, out, _) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    assert_eq!(
        out,
        "node/n1 cordoned (dry run)\nevicting pod ns/p1 (dry run)\nnode/n1 drained (dry run)\n"
    );
    assert!(mock.requests().iter().all(|r| r.0 == "GET"));
}

#[tokio::test]
async fn drain_unmanaged_pod_needs_force() {
    let mock = start(drain_server(vec![pod("bare", None)], true, true, true)).await;
    let (r, out, err) = run_drain(&mock, &flags(), |_| {}).await;
    let e = r.unwrap_err().to_string();
    assert_eq!(
        e,
        "cannot delete cannot delete Pods that declare no controller (use --force to override): ns/bare"
    );
    assert!(err.contains("error: unable to drain node \"n1\" due to error: "));
    assert!(err.contains("There are pending nodes to be drained:\n n1\n"));
    assert_eq!(out, "node/n1 cordoned\n");

    let mut f = flags();
    f.force = true;
    let (r, out, _) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    assert!(out.contains("pod/bare evicted"));
}

#[tokio::test]
async fn drain_daemonset_pod_needs_ignore_flag() {
    let mock = start(drain_server(
        vec![pod("ds-pod", Some(("DaemonSet", "apps/v1")))],
        true,
        true,
        true,
    ))
    .await;
    let (r, _, _) = run_drain(&mock, &flags(), |_| {}).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "cannot delete DaemonSet-managed Pods (use --ignore-daemonsets to ignore): ns/ds-pod"
    );

    let mut f = flags();
    f.ignore_daemonsets = true;
    let (r, out, err) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    // The DaemonSet pod is ignored (warning), never evicted.
    assert!(err.contains("Warning: ignoring DaemonSet-managed Pods: ns/ds-pod\n"));
    assert!(!out.contains("evicting"));
}

#[tokio::test]
async fn drain_orphaned_daemonset_pod_is_removed_only_with_force() {
    let mock = start(drain_server(
        vec![pod("ds-pod", Some(("DaemonSet", "apps/v1")))],
        true,
        true,
        false,
    ))
    .await;
    let mut f = flags();
    f.ignore_daemonsets = true;
    let (r, _, _) = run_drain(&mock, &f, |_| {}).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "cannot delete daemonsets.apps \"owner\" not found: ns/ds-pod"
    );

    f.force = true;
    let (r, out, err) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    // The warning is dropped by the later filters (only the last filter's
    // status is retained, filters.go makeFilters comment).
    assert_eq!(err, "");
    assert!(out.contains("pod/ds-pod evicted"));
}

#[tokio::test]
async fn drain_emptydir_pod_needs_delete_emptydir_data() {
    let mut p = pod("local", Some(("ReplicaSet", "apps/v1")));
    p["spec"]["volumes"] = json!([{"name":"scratch","emptyDir":{}}]);
    let mock = start(drain_server(vec![p], true, true, true)).await;
    let (r, _, _) = run_drain(&mock, &flags(), |_| {}).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "cannot delete Pods with local storage (use --delete-emptydir-data to override): ns/local"
    );

    let mut f = flags();
    f.delete_emptydir_data = true;
    let (r, out, err) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    // The warning of the local-storage filter is dropped by the later
    // unreplicatedFilter (upstream TODO in filters.go).
    assert_eq!(err, "");
    assert!(out.contains("pod/local evicted"));
}

#[tokio::test]
async fn drain_skips_mirror_pods_and_finished_pods_are_removable() {
    let mut mirror = pod("mirror", None);
    mirror["metadata"]["annotations"] = json!({"kubernetes.io/config.mirror": "x"});
    let mut done = pod("done", None);
    done["status"]["phase"] = json!("Succeeded");
    let mock = start(drain_server(vec![mirror, done], true, true, true)).await;
    let (r, out, _) = run_drain(&mock, &flags(), |_| {}).await;
    r.unwrap();
    assert!(!out.contains("mirror"));
    assert!(out.contains("pod/done evicted"));
}

#[tokio::test]
async fn drain_retries_eviction_on_429() {
    let attempts = Arc::new(Mutex::new(0));
    let a = attempts.clone();
    let base = drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        true,
        true,
    );
    let mock = start(Arc::new(move |m, p, b| {
        if m == "POST" && p.ends_with("/eviction") {
            let mut n = a.lock().unwrap();
            *n += 1;
            if *n == 1 {
                return (
                    429,
                    status(
                        429,
                        "TooManyRequests",
                        "Cannot evict pod as it would violate the pod's disruption budget.",
                    ),
                );
            }
        }
        base(m, p, b)
    }))
    .await;
    let (r, out, err) = run_drain(&mock, &flags(), |o| {
        o.drainer.evict_error_retry_delay = Duration::from_millis(10);
    })
    .await;
    r.unwrap();
    assert_eq!(*attempts.lock().unwrap(), 2);
    assert_eq!(
        err,
        "error when evicting pods/\"p1\" -n \"ns\" (will retry after 10ms): Cannot evict pod as it would violate the pod's disruption budget.\n"
    );
    // The "evicting pod" progress line is printed on every attempt.
    assert_eq!(out.matches("evicting pod ns/p1\n").count(), 2);
}

#[tokio::test]
async fn drain_namespace_terminating_forbidden_retries_until_pod_deleted() {
    // eviction.go / drain.go: Forbidden + NamespaceTerminating cause.
    let attempts = Arc::new(Mutex::new(0));
    let a = attempts.clone();
    let base = drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        true,
        true,
    );
    let mock = start(Arc::new(move |m, p, b| {
        if m == "POST" && p.ends_with("/eviction") {
            let mut n = a.lock().unwrap();
            *n += 1;
            if *n == 1 {
                let mut s = status(
                    403,
                    "Forbidden",
                    "unable to create new content in namespace ns because it is being terminated",
                );
                s["details"] = json!({"causes":[{"reason":"NamespaceTerminating","message":"m"}]});
                return (403, s);
            }
        }
        base(m, p, b)
    }))
    .await;
    let (r, _, err) = run_drain(&mock, &flags(), |o| {
        o.drainer.evict_error_retry_delay = Duration::from_millis(10);
    })
    .await;
    r.unwrap();
    assert!(err.starts_with(
        "error when evicting pod \"p1\" from terminating namespace \"ns\" (will retry after 10ms): "
    ));
}

#[tokio::test]
async fn drain_other_eviction_errors_are_fatal() {
    let base = drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        false,
        true,
    );
    let mock = start(Arc::new(move |m, p, b| {
        if m == "POST" && p.ends_with("/eviction") {
            return (500, status(500, "InternalError", "boom"));
        }
        base(m, p, b)
    }))
    .await;
    let (r, _, err) = run_drain(&mock, &flags(), |_| {}).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "error when evicting pods/\"p1\" -n \"ns\": boom"
    );
    assert!(err.contains("There are pending pods in node \"n1\" when an error occurred: "));
    assert!(err.contains("pod/p1\n"));
}

#[tokio::test]
async fn drain_global_timeout_while_waiting_for_delete() {
    // The pod never goes away: the global timeout ends the wait.
    let mock = start(drain_server(
        vec![pod("p1", Some(("ReplicaSet", "apps/v1")))],
        true,
        false,
        true,
    ))
    .await;
    let mut f = flags();
    f.timeout = Duration::from_millis(300);
    let (r, _, _) = run_drain(&mock, &f, |_| {}).await;
    assert_eq!(
        r.unwrap_err().to_string(),
        "error when waiting for pod \"p1\" in namespace \"ns\" to terminate: context deadline exceeded"
    );
}

#[tokio::test]
async fn drain_skip_wait_for_delete_timeout_skips_old_terminating_pods() {
    // skipDeletedFilter / waitForDelete (filters.go, drain.go)
    let mut old = pod("old", Some(("ReplicaSet", "apps/v1")));
    old["metadata"]["deletionTimestamp"] = json!("2020-01-01T00:00:00Z");
    let mock = start(drain_server(vec![old], true, false, true)).await;
    let mut f = flags();
    f.skip_wait_for_delete_timeout = 60;
    let (r, out, _) = run_drain(&mock, &f, |_| {}).await;
    r.unwrap();
    // Filtered out by skipDeletedFilter: nothing is evicted.
    assert_eq!(out, "node/n1 cordoned\nnode/n1 drained\n");
}

#[tokio::test]
async fn pod_delete_status_helpers() {
    assert!(PodDeleteStatus::okay().delete);
    assert!(!PodDeleteStatus::skip().delete);
    assert!(PodDeleteStatus::with_warning(true, "w").delete);
    assert!(!PodDeleteStatus::with_error("e").delete);
}

#[tokio::test]
async fn execute_cordon_entry_point_uses_defaults() {
    let mock = start(Arc::new(|m, p, _| match (m, p) {
        ("GET", "/api/v1/nodes/n1") => (200, node("n1", true)),
        _ => (500, status(500, "InternalError", p)),
    }))
    .await;
    execute_cordon(&mock.client(), &["n1".to_string()], &flags())
        .await
        .unwrap();
}
