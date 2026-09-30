//! The ClusterAuthenticationTrust controller's sync and the bootstrap RBAC
//! it relies on, against storage — port of `syncConfigMap` and
//! `createNamespaceIfNeeded`
//! (pkg/controlplane/controller/clusterauthenticationtrust/cluster_authentication_trust_controller.go:140-197)
//! and the `extension-apiserver-authentication-reader` namespace policy
//! (plugin/pkg/auth/authorizer/rbac/bootstrappolicy/namespace_policy.go:75-82, 124-126).

use std::sync::Arc;

use rusternetes_api_server::bootstrap::{
    bootstrap_extension_apiserver_authentication_rbac, sync_cluster_authentication_trust,
};
use rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo;
use rusternetes_common::resources::{ConfigMap, Namespace};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// `someRandomCA` and `anotherRandomCA`
/// (cluster_authentication_trust_controller_test.go:44-78).
const SOME_RANDOM_CA: &str = r#"-----BEGIN CERTIFICATE-----
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUMRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1dveps=
-----END CERTIFICATE-----
"#;
const ANOTHER_RANDOM_CA: &str = r#"-----BEGIN CERTIFICATE-----
MIIDQDCCAiigAwIBAgIJANWw74P5KJk2MA0GCSqGSIb3DQEBCwUAMDQxMjAwBgNV
BAMMKWdlbmVyaWNfd2ViaG9va19hZG1pc3Npb25fcGx1Z2luX3Rlc3RzX2NhMCAX
DTE3MTExNjAwMDUzOVoYDzIyOTEwOTAxMDAwNTM5WjAjMSEwHwYDVQQDExh3ZWJo
b29rLXRlc3QuZGVmYXVsdC5zdmMwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDXd/nQ89a5H8ifEsigmMd01Ib6NVR3bkJjtkvYnTbdfYEBj7UzqOQtHoLa
dIVmefny5uIHvj93WD8WDVPB3jX2JHrXkDTXd/6o6jIXHcsUfFTVLp6/bZ+Anqe0
r/7hAPkzA2A7APyTWM3ZbEeo1afXogXhOJ1u/wz0DflgcB21gNho4kKTONXO3NHD
XLpspFqSkxfEfKVDJaYAoMnYZJtFNsa2OvsmLnhYF8bjeT3i07lfwrhUZvP+7Gsp
7UgUwc06WuNHjfx1s5e6ySzH0QioMD1rjYneqOvk0pKrMIhuAEWXqq7jlXcDtx1E
j+wnYbVqqVYheHZ8BCJoVAAQGs9/AgMBAAGjZDBiMAkGA1UdEwQCMAAwCwYDVR0P
BAQDAgXgMB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEFBQcDATApBgNVHREEIjAg
hwR/AAABghh3ZWJob29rLXRlc3QuZGVmYXVsdC5zdmMwDQYJKoZIhvcNAQELBQAD
ggEBAD/GKSPNyQuAOw/jsYZesb+RMedbkzs18sSwlxAJQMUrrXwlVdHrA8q5WhE6
ABLqU1b8lQ8AWun07R8k5tqTmNvCARrAPRUqls/ryER+3Y9YEcxEaTc3jKNZFLbc
T6YtcnkdhxsiO136wtiuatpYL91RgCmuSpR8+7jEHhuFU01iaASu7ypFrUzrKHTF
bKwiLRQi1cMzVcLErq5CDEKiKhUkoDucyARFszrGt9vNIl/YCcBOkcNvM3c05Hn3
M++C29JwS3Hwbubg6WO3wjFjoEhpCwU6qRYUz3MRp4tHO4kxKXx+oQnUiFnR7vW0
YkNtGc1RUDHwecCTFpJtPb7Yu/E=
-----END CERTIFICATE-----
"#;

const CM_KEY: &str = "/registry/configmaps/kube-system/extension-apiserver-authentication";

fn backend() -> Arc<StorageBackend> {
    Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())))
}

fn required(client_ca: &str) -> ClusterAuthenticationInfo {
    ClusterAuthenticationInfo {
        client_ca: Some(client_ca.to_string()),
        request_header_username_headers: Some(vec!["X-Remote-User".into()]),
        request_header_group_headers: Some(vec!["X-Remote-Group".into()]),
        request_header_extra_header_prefixes: Some(vec!["X-Remote-Extra-".into()]),
        request_header_allowed_names: Some(vec![]),
        request_header_ca: Some(client_ca.to_string()),
        ..Default::default()
    }
}

/// With no ConfigMap and no `kube-system`, a sync creates both: the
/// namespace as the Namespace strategy would store it, and the ConfigMap
/// with `getConfigMapDataFor`'s keys.
#[tokio::test]
async fn a_sync_creates_the_namespace_and_the_config_map() {
    let storage = backend();
    sync_cluster_authentication_trust(&storage, &required(SOME_RANDOM_CA))
        .await
        .unwrap();

    let ns: Namespace = storage
        .get(&build_key("namespaces", None, "kube-system"))
        .await
        .expect("kube-system created");
    let ns = serde_json::to_value(ns).unwrap();
    assert_eq!(ns["status"]["phase"], "Active", "{ns}");
    assert_eq!(ns["spec"]["finalizers"], json!(["kubernetes"]), "{ns}");
    assert_eq!(
        ns["metadata"]["labels"]["kubernetes.io/metadata.name"],
        "kube-system"
    );

    let cm: ConfigMap = storage.get(CM_KEY).await.expect("configmap created");
    let data = cm.data.unwrap();
    assert_eq!(data["client-ca-file"], SOME_RANDOM_CA);
    assert_eq!(data["requestheader-client-ca-file"], SOME_RANDOM_CA);
    assert_eq!(
        data["requestheader-username-headers"],
        r#"["X-Remote-User"]"#
    );
    assert_eq!(data["requestheader-group-headers"], r#"["X-Remote-Group"]"#);
    assert_eq!(
        data["requestheader-extra-headers-prefix"],
        r#"["X-Remote-Extra-"]"#
    );
    assert_eq!(data["requestheader-allowed-names"], "[]");
    assert!(!data.contains_key("requestheader-uid-headers"));
}

/// "skip on no change": a second sync writes nothing.
#[tokio::test]
async fn an_unchanged_sync_does_not_write() {
    let storage = backend();
    sync_cluster_authentication_trust(&storage, &required(SOME_RANDOM_CA))
        .await
        .unwrap();
    let before: ConfigMap = storage.get(CM_KEY).await.unwrap();
    sync_cluster_authentication_trust(&storage, &required(SOME_RANDOM_CA))
        .await
        .unwrap();
    let after: ConfigMap = storage.get(CM_KEY).await.unwrap();
    assert_eq!(
        before.metadata.resource_version,
        after.metadata.resource_version
    );
}

/// "overwrite extension-apiserver-authentication": a CA another api-server
/// published is kept, and this one's is appended.
#[tokio::test]
async fn a_stored_ca_is_kept_and_the_required_one_appended() {
    let storage = backend();
    sync_cluster_authentication_trust(&storage, &required(ANOTHER_RANDOM_CA))
        .await
        .unwrap();
    sync_cluster_authentication_trust(&storage, &required(SOME_RANDOM_CA))
        .await
        .unwrap();
    let cm: ConfigMap = storage.get(CM_KEY).await.unwrap();
    assert_eq!(
        cm.data.unwrap()["client-ca-file"],
        format!("{ANOTHER_RANDOM_CA}{SOME_RANDOM_CA}")
    );
}

/// The reader Role and its binding are seeded, idempotently, with the
/// bootstrap policy's labels and annotations.
#[tokio::test]
async fn the_reader_role_and_binding_are_bootstrapped() {
    let storage = backend();
    bootstrap_extension_apiserver_authentication_rbac(storage.clone())
        .await
        .unwrap();
    bootstrap_extension_apiserver_authentication_rbac(storage.clone())
        .await
        .unwrap();

    let role: Value = storage
        .get(&build_key(
            "roles",
            Some("kube-system"),
            "extension-apiserver-authentication-reader",
        ))
        .await
        .expect("role");
    assert_eq!(
        role["rules"],
        json!([{
            "apiGroups": [""],
            "resources": ["configmaps"],
            "resourceNames": ["extension-apiserver-authentication"],
            "verbs": ["get", "list", "watch"]
        }])
    );
    assert_eq!(
        role["metadata"]["labels"]["kubernetes.io/bootstrapping"],
        "rbac-defaults"
    );

    let binding: Value = storage
        .get(&build_key(
            "rolebindings",
            Some("kube-system"),
            "system::extension-apiserver-authentication-reader",
        ))
        .await
        .expect("binding");
    assert_eq!(
        binding["roleRef"]["name"],
        "extension-apiserver-authentication-reader"
    );
    let subjects: Vec<&str> = binding["subjects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        subjects,
        ["system:kube-controller-manager", "system:kube-scheduler"]
    );
}

/// Creating `kube-system` through the API has no side effect on the
/// ConfigMap: upstream's namespace strategy creates nothing else.
#[tokio::test]
async fn creating_kube_system_does_not_write_the_config_map() {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "kube-system"}}),
        )
        .await;
    assert!(status.is_success(), "{out}");
    assert!(api.storage.get::<Value>(CM_KEY).await.is_err());
}
