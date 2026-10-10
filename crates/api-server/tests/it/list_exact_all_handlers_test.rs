//! #2825: `resourceVersionMatch=Exact` pins the read in EVERY list handler,
//! not just pods/configmaps/podtemplates (#2684). Upstream the pin lives in
//! the one shared store: `ValidateListOptions` sets `withRev = parsedRV` for
//! Exact (`staging/src/k8s.io/apiserver/pkg/storage/interfaces.go:374-375`),
//! `GetList` ranges at that revision (`etcd3/store.go:781-786`) and stamps the
//! list with it (`UpdateList(..., withRev, ...)`, `etcd3/store.go:898`).
//! Mirrors `RunTestList` "resource version of second write, match=Exact"
//! (`storage/testing/store_tests.go:1580`).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

struct Case {
    /// Collection URL the objects are created at and listed from.
    coll: &'static str,
    /// Extra list URLs (e.g. all-namespaces) that must pin the same way.
    also: &'static [&'static str],
    kind: &'static str,
    api_version: &'static str,
    extra: fn() -> Value,
}

fn none() -> Value {
    json!({})
}

fn svc() -> Value {
    json!({"spec": {"ports": [{"port": 80}]}})
}

fn sc() -> Value {
    json!({"provisioner": "example.com/p"})
}

fn pc() -> Value {
    json!({"value": 5})
}

fn rc() -> Value {
    json!({"handler": "h"})
}

fn obj(c: &Case, name: &str) -> Value {
    let mut o = json!({
        "apiVersion": c.api_version,
        "kind": c.kind,
        "metadata": {"name": name},
    });
    for (k, v) in (c.extra)().as_object().unwrap() {
        o[k] = v.clone();
    }
    o
}

fn names(list: &Value) -> Vec<String> {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items: {list}"))
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn exact_pins_every_list_handler() {
    let cases = [
        Case {
            coll: "/api/v1/namespaces/default/secrets",
            also: &["/api/v1/secrets"],
            kind: "Secret",
            api_version: "v1",
            extra: none,
        },
        Case {
            coll: "/api/v1/namespaces/default/serviceaccounts",
            also: &["/api/v1/serviceaccounts"],
            kind: "ServiceAccount",
            api_version: "v1",
            extra: none,
        },
        Case {
            coll: "/api/v1/namespaces/default/services",
            also: &["/api/v1/services"],
            kind: "Service",
            api_version: "v1",
            extra: svc,
        },
        Case {
            coll: "/api/v1/namespaces",
            also: &[],
            kind: "Namespace",
            api_version: "v1",
            extra: none,
        },
        Case {
            coll: "/apis/storage.k8s.io/v1/storageclasses",
            also: &[],
            kind: "StorageClass",
            api_version: "storage.k8s.io/v1",
            extra: sc,
        },
        Case {
            coll: "/apis/scheduling.k8s.io/v1/priorityclasses",
            also: &[],
            kind: "PriorityClass",
            api_version: "scheduling.k8s.io/v1",
            extra: pc,
        },
        Case {
            coll: "/apis/node.k8s.io/v1/runtimeclasses",
            also: &[],
            kind: "RuntimeClass",
            api_version: "node.k8s.io/v1",
            extra: rc,
        },
    ];

    let mut failures = Vec::new();
    for c in &cases {
        // A fresh server per case: each lists a distinct collection.
        let api = TestApiServer::new();
        let (st, one) = api.post(c.coll, &obj(c, "one")).await;
        assert!(
            st == StatusCode::CREATED,
            "{} create one: {st} {one}",
            c.coll
        );
        let rv1 = one["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();
        let (st, two) = api.post(c.coll, &obj(c, "two")).await;
        assert!(
            st == StatusCode::CREATED,
            "{} create two: {st} {two}",
            c.coll
        );

        for url in std::iter::once(&c.coll).chain(c.also.iter()) {
            let (st, l) = api
                .get(&format!(
                    "{url}?resourceVersion={rv1}&resourceVersionMatch=Exact"
                ))
                .await;
            if st != StatusCode::OK {
                failures.push(format!("{url}: status {st}: {l}"));
                continue;
            }
            let got = names(&l);
            if !(got.contains(&"one".to_string()) && !got.contains(&"two".to_string())) {
                failures.push(format!(
                    "{url}: Exact@{rv1} must list the collection as of that revision, got {got:?}"
                ));
            }
            if l["metadata"]["resourceVersion"] != rv1.as_str() {
                failures.push(format!(
                    "{url}: list rv must be the pinned {rv1}, got {}",
                    l["metadata"]["resourceVersion"]
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
