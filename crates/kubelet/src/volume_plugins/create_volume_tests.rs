//! Tests driving `VolumeManager::create_volume` through the service-account
//! token, CA-cert and configMap branches of the secret, projected and
//! configMap plugins (#1979).
//!
//! Every expected value is a literal, never recomputed through the
//! implementation's helpers. These were written AFTER the plugin move, so
//! unlike the secret/downwardAPI/projected characterization tests in
//! `volumes.rs` they pin current behaviour rather than proving the move
//! equivalent; where upstream defines the behaviour the assertion follows
//! upstream (`pkg/volume/configmap/configmap_test.go` `TestCollectData`,
//! `pkg/volume/projected/projected_test.go`
//! `TestCollectDataWithServiceAccountToken`, and `requiresRefresh` in
//! `pkg/kubelet/token/token_manager.go`).

use crate::volumes::VolumeManager;
use rusternetes_common::auth::{ServiceAccountClaims, TokenManager};
use rusternetes_common::resources::{ConfigMap, Pod, Secret, Volume};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const CA: &[u8] = b"-----BEGIN CERTIFICATE-----\nFAKE-CA\n-----END CERTIFICATE-----\n";

struct Env {
    storage: Arc<StorageBackend>,
    tmp: tempfile::TempDir,
    vm: VolumeManager,
}

fn env() -> Env {
    let storage = Arc::new(StorageBackend::new_memory());
    let tmp = tempfile::tempdir().unwrap();
    let vm = VolumeManager::new(
        tmp.path().to_string_lossy().to_string(),
        Some(storage.clone()),
        TokenManager::new_auto(b"test-secret"),
    );
    Env { storage, tmp, vm }
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
        "spec": {"containers": [], "serviceAccountName": "sa1", "nodeName": "node-1"}
    }))
    .unwrap()
}

fn volume(v: serde_json::Value) -> Volume {
    serde_json::from_value(v).unwrap()
}

fn mode(path: &str) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

async fn seed_sa_and_node(e: &Env) {
    Storage::create(
        e.storage.as_ref(),
        &build_key("serviceaccounts", Some("default"), "sa1"),
        &json!({"metadata": {"name": "sa1", "namespace": "default", "uid": "sa-uid-1"}}),
    )
    .await
    .unwrap();
    Storage::create(
        e.storage.as_ref(),
        &build_key("nodes", None::<&str>, "node-1"),
        &json!({"metadata": {"name": "node-1", "uid": "node-uid-1"}}),
    )
    .await
    .unwrap();
}

async fn seed_secret(e: &Env, name: &str, pairs: &[(&str, &[u8])]) {
    let data: HashMap<String, Vec<u8>> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_vec()))
        .collect();
    Storage::create(
        e.storage.as_ref(),
        &build_key("secrets", Some("default"), name),
        &Secret::new(name, "default").with_data(data),
    )
    .await
    .unwrap();
}

fn decode(token: &str) -> ServiceAccountClaims {
    TokenManager::new_auto(b"test-secret")
        .validate_token(token)
        .unwrap_or_else(|err| panic!("token must validate: {err}; token={token}"))
}

fn read_claims(file: &str) -> ServiceAccountClaims {
    decode(&std::fs::read_to_string(file).unwrap())
}

/// Restores `CA_CERT_PATH` on drop; tests that touch it are `serial`.
struct CaEnvGuard(Option<std::ffi::OsString>);
impl CaEnvGuard {
    fn set(value: Option<&str>) -> Self {
        let old = std::env::var_os("CA_CERT_PATH");
        match value {
            Some(v) => std::env::set_var("CA_CERT_PATH", v),
            None => std::env::remove_var("CA_CERT_PATH"),
        }
        Self(old)
    }
}
impl Drop for CaEnvGuard {
    fn drop(&mut self) {
        match &self.0 {
            Some(v) => std::env::set_var("CA_CERT_PATH", v),
            None => std::env::remove_var("CA_CERT_PATH"),
        }
    }
}

// ---------------------------------------------------------------- configMap

async fn seed_cm(e: &Env) {
    let mut cm = ConfigMap::new("cfg", "default").with_data(HashMap::from([
        ("app.conf".to_string(), "hello".to_string()),
        ("other".to_string(), "x".to_string()),
    ]));
    cm.binary_data = Some(HashMap::from([("blob".to_string(), vec![0u8, 1, 2, 255])]));
    Storage::create(
        e.storage.as_ref(),
        &build_key("configmaps", Some("default"), "cfg"),
        &cm,
    )
    .await
    .unwrap();
}

/// `TestCollectData` "no items": every `data` and `binaryData` key becomes a
/// file at `defaultMode`.
#[tokio::test]
async fn config_map_without_items_projects_data_and_binary_data() {
    let e = env();
    seed_cm(&e).await;
    let v = volume(json!({"name": "c", "configMap": {"name": "cfg", "defaultMode": 0o400}}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();

    assert_eq!(
        path,
        format!(
            "{}/pods/uid-1/volumes/kubernetes.io~configmap/c",
            e.tmp.path().to_string_lossy()
        )
    );
    assert_eq!(std::fs::read(format!("{path}/app.conf")).unwrap(), b"hello");
    assert_eq!(std::fs::read(format!("{path}/other")).unwrap(), b"x");
    assert_eq!(
        std::fs::read(format!("{path}/blob")).unwrap(),
        vec![0u8, 1, 2, 255]
    );
    assert_eq!(mode(&format!("{path}/app.conf")), 0o400);
    assert_eq!(mode(&format!("{path}/blob")), 0o400);
}

/// `TestCollectData` "items": only listed keys, at their `path`, per-item
/// `mode` overriding `defaultMode`.
#[tokio::test]
async fn config_map_items_select_keys_paths_and_modes() {
    let e = env();
    seed_cm(&e).await;
    let v = volume(json!({"name": "c", "configMap": {
        "name": "cfg", "defaultMode": 0o644,
        "items": [
            {"key": "app.conf", "path": "sub/dir/a.conf", "mode": 0o600},
            {"key": "blob", "path": "b.bin"}
        ]
    }}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();

    assert_eq!(
        std::fs::read(format!("{path}/sub/dir/a.conf")).unwrap(),
        b"hello"
    );
    assert_eq!(mode(&format!("{path}/sub/dir/a.conf")), 0o600);
    assert_eq!(
        std::fs::read(format!("{path}/b.bin")).unwrap(),
        vec![0u8, 1, 2, 255]
    );
    assert_eq!(mode(&format!("{path}/b.bin")), 0o644);
    assert!(
        !std::path::Path::new(&format!("{path}/other")).exists(),
        "a key not named in items must not be projected"
    );
}

/// `TestCollectData` "key not found": a mapped key the ConfigMap lacks is an
/// error unless the volume is optional.
#[tokio::test]
async fn config_map_missing_mapped_key_errors_unless_optional() {
    let e = env();
    seed_cm(&e).await;
    let items = json!([{"key": "nope", "path": "n"}, {"key": "other", "path": "o"}]);

    let required = volume(json!({"name": "c", "configMap": {"name": "cfg", "items": items}}));
    let err = e.vm.create_volume(&pod(), &required).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("references non-existent config key: nope"),
        "got: {err:#}"
    );

    let optional = volume(json!({"name": "c2", "configMap": {
        "name": "cfg", "optional": true, "items": items}}));
    let path = e.vm.create_volume(&pod(), &optional).await.unwrap();
    assert_eq!(std::fs::read(format!("{path}/o")).unwrap(), b"x");
    assert!(!std::path::Path::new(&format!("{path}/n")).exists());
}

/// A required ConfigMap that does not exist fails set-up (the pod stays
/// ContainerCreating and the kubelet retries).
#[tokio::test]
async fn config_map_missing_required_is_an_error() {
    let e = env();
    let v = volume(json!({"name": "c", "configMap": {"name": "ghost"}}));
    let err = e.vm.create_volume(&pod(), &v).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("ConfigMap ghost not found in namespace default"),
        "got: {err:#}"
    );
}

/// `TestPluginOptional`: a missing optional ConfigMap yields an empty volume.
#[tokio::test]
async fn config_map_missing_optional_yields_an_empty_volume() {
    let e = env();
    let v = volume(json!({"name": "c", "configMap": {"name": "ghost", "optional": true}}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();
    assert!(std::path::Path::new(&path).is_dir());
    // Upstream runs the AtomicWriter over an empty payload, so the only
    // entries are `..data` and the timestamped dir it points at, and that dir
    // is empty (`configmap_test.go:506-535`).
    let target = std::fs::read_link(std::path::Path::new(&path).join("..data")).unwrap();
    assert_eq!(
        std::fs::read_dir(std::path::Path::new(&path).join(target))
            .unwrap()
            .count(),
        0
    );
    for entry in std::fs::read_dir(&path).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        assert!(name.starts_with(".."), "unexpected entry {name}");
    }
}

// ------------------------------------------------- secret: SA token + CA cert

fn sa_token_secret_volume() -> Volume {
    volume(json!({"name": "kube-api-access-abc", "secret": {"secretName": "sa1-token"}}))
}

/// An SA-token secret volume (volume name `kube-api-access*` or secret name
/// `*-token`) has the stored static `token` replaced by a freshly minted
/// token bound to the pod, ServiceAccount and Node (uids read from storage).
/// Rusternetes-only (secret.go has no such step; declared CLAUDE.md rule 8).
#[tokio::test]
#[serial_test::serial]
async fn secret_sa_token_volume_replaces_token_with_bound_jwt() {
    let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
    let e = env();
    seed_sa_and_node(&e).await;
    seed_secret(&e, "sa1-token", &[("token", b"static-token"), ("x", b"y")]).await;

    let path =
        e.vm.create_volume(&pod(), &sa_token_secret_volume())
            .await
            .unwrap();

    let token = std::fs::read_to_string(format!("{path}/token")).unwrap();
    assert_ne!(token, "static-token");
    let c = decode(&token);
    assert_eq!(c.sub, "system:serviceaccount:default:sa1");
    assert_eq!(c.namespace, "default");
    assert_eq!(c.uid, "sa-uid-1");
    assert_eq!(c.iss, "https://kubernetes.default.svc.cluster.local");
    assert_eq!(c.aud, vec!["rusternetes".to_string()]);
    assert_eq!(c.exp - c.iat, 3600);
    assert_eq!(c.pod_name.as_deref(), Some("p"));
    assert_eq!(c.pod_uid.as_deref(), Some("uid-1"));
    assert_eq!(c.node_name.as_deref(), Some("node-1"));
    assert_eq!(c.node_uid.as_deref(), Some("node-uid-1"));
    let k = c.kubernetes.expect("kubernetes.io claims");
    assert_eq!(k.svcacct.name, "sa1");
    assert_eq!(k.svcacct.uid, "sa-uid-1");
    assert_eq!(k.pod.unwrap().uid, "uid-1");
    assert_eq!(k.node.unwrap().uid, "node-uid-1");
    // Other keys are untouched.
    assert_eq!(std::fs::read(format!("{path}/x")).unwrap(), b"y");
}

/// With `items`, the token substitution follows the `key: token` item's path.
#[tokio::test]
#[serial_test::serial]
async fn secret_sa_token_substitution_follows_items_path() {
    let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
    let e = env();
    seed_sa_and_node(&e).await;
    seed_secret(&e, "sa1-token", &[("token", b"static-token"), ("x", b"y")]).await;
    let v = volume(json!({"name": "kube-api-access-abc", "secret": {
        "secretName": "sa1-token",
        "items": [{"key": "token", "path": "t/jwt"}, {"key": "x", "path": "t/token"}]
    }}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();

    assert_eq!(read_claims(&format!("{path}/t/jwt")).uid, "sa-uid-1");
    // An item that merely LANDS on a path named "token" but maps another key
    // is not the token key and keeps its own data.
    assert_eq!(std::fs::read(format!("{path}/t/token")).unwrap(), b"y");
}

/// A secret that merely has a `token` key, under a name/volume that is not an
/// SA-token one, keeps its stored token (no minting).
#[tokio::test]
#[serial_test::serial]
async fn secret_with_token_key_but_ordinary_name_is_not_reminted() {
    let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
    let e = env();
    seed_secret(&e, "mysecret", &[("token", b"static-token")]).await;
    let v = volume(json!({"name": "sec", "secret": {"secretName": "mysecret"}}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();
    assert_eq!(
        std::fs::read(format!("{path}/token")).unwrap(),
        b"static-token"
    );
}

/// The cluster CA is injected as `ca.crt` into a service-account secret
/// volume, from `CA_CERT_PATH` when set.
#[tokio::test]
#[serial_test::serial]
async fn secret_sa_volume_gets_ca_crt_from_env_path() {
    let e = env();
    let ca_file = e.tmp.path().join("env-ca.crt");
    std::fs::write(&ca_file, CA).unwrap();
    let _g = CaEnvGuard::set(Some(ca_file.to_str().unwrap()));
    seed_sa_and_node(&e).await;
    seed_secret(&e, "sa1-token", &[("token", b"t")]).await;
    let v =
        volume(json!({"name": "s", "secret": {"secretName": "sa1-token", "defaultMode": 0o440}}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();

    assert_eq!(std::fs::read(format!("{path}/ca.crt")).unwrap(), CA);
    assert_eq!(mode(&format!("{path}/ca.crt")), 0o440);
}

/// Without `CA_CERT_PATH`, the CA is read from `<volumes base>/_certs/ca.crt`
/// (`VolumeHost::get_volumes_base_path`).
#[tokio::test]
#[serial_test::serial]
async fn secret_sa_volume_gets_ca_crt_from_volumes_certs_dir() {
    let _g = CaEnvGuard::set(None);
    let e = env();
    std::fs::create_dir_all(e.tmp.path().join("_certs")).unwrap();
    std::fs::write(e.tmp.path().join("_certs/ca.crt"), CA).unwrap();
    seed_secret(&e, "sa1-token", &[("token", b"t")]).await;
    let path =
        e.vm.create_volume(&pod(), &sa_token_secret_volume())
            .await
            .unwrap();
    assert_eq!(std::fs::read(format!("{path}/ca.crt")).unwrap(), CA);
}

/// A `ca.crt` already present in the Secret is never overwritten.
#[tokio::test]
#[serial_test::serial]
async fn secret_existing_ca_crt_is_not_overwritten() {
    let e = env();
    let ca_file = e.tmp.path().join("env-ca.crt");
    std::fs::write(&ca_file, CA).unwrap();
    let _g = CaEnvGuard::set(Some(ca_file.to_str().unwrap()));
    seed_secret(&e, "sa1-token", &[("token", b"t"), ("ca.crt", b"OWN")]).await;
    let path =
        e.vm.create_volume(&pod(), &sa_token_secret_volume())
            .await
            .unwrap();
    assert_eq!(std::fs::read(format!("{path}/ca.crt")).unwrap(), b"OWN");
}

/// A non-service-account secret (no `token` key, name not `*-token`) gets no
/// CA injected even when one is available.
#[tokio::test]
#[serial_test::serial]
async fn secret_non_sa_volume_gets_no_ca_crt() {
    let e = env();
    let ca_file = e.tmp.path().join("env-ca.crt");
    std::fs::write(&ca_file, CA).unwrap();
    let _g = CaEnvGuard::set(Some(ca_file.to_str().unwrap()));
    seed_secret(&e, "plain", &[("password", b"p")]).await;
    let v = volume(json!({"name": "s", "secret": {"secretName": "plain"}}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();
    assert!(!std::path::Path::new(&format!("{path}/ca.crt")).exists());
}

/// A missing CA is a warning, not a failure: the volume is still created.
#[tokio::test]
#[serial_test::serial]
async fn secret_sa_volume_without_any_ca_still_succeeds() {
    let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
    let e = env();
    seed_secret(&e, "sa1-token", &[("token", b"t")]).await;
    let path =
        e.vm.create_volume(&pod(), &sa_token_secret_volume())
            .await
            .unwrap();
    assert!(!std::path::Path::new(&format!("{path}/ca.crt")).exists());
    assert!(std::path::Path::new(&format!("{path}/token")).exists());
}

// ---------------------------------------------- projected serviceAccountToken

fn projected_token_volume(extra: serde_json::Value) -> Volume {
    let mut t = json!({"path": "sub/token"});
    t.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    volume(json!({"name": "p", "projected": {
        "defaultMode": 0o440,
        "sources": [{"serviceAccountToken": t}]
    }}))
}

/// `TestCollectDataWithServiceAccountToken`: the token lands at `path` with
/// `defaultMode`; claims carry the pod/SA/node identity. Default lifetime is
/// 3600s, default audience (self-mint fallback) is "rusternetes".
#[tokio::test]
async fn projected_sa_token_is_minted_with_bound_claims() {
    let e = env();
    seed_sa_and_node(&e).await;
    let path =
        e.vm.create_volume(&pod(), &projected_token_volume(json!({})))
            .await
            .unwrap();

    let file = format!("{path}/sub/token");
    assert_eq!(mode(&file), 0o440);
    let c = read_claims(&file);
    assert_eq!(c.sub, "system:serviceaccount:default:sa1");
    assert_eq!(c.uid, "sa-uid-1");
    assert_eq!(c.aud, vec!["rusternetes".to_string()]);
    assert_eq!(c.exp - c.iat, 3600);
    assert_eq!(c.pod_name.as_deref(), Some("p"));
    assert_eq!(c.pod_uid.as_deref(), Some("uid-1"));
    assert_eq!(c.node_name.as_deref(), Some("node-1"));
    assert_eq!(c.node_uid.as_deref(), Some("node-uid-1"));
    assert_eq!(c.kubernetes.unwrap().svcacct.uid, "sa-uid-1");
}

/// `audience` and `expirationSeconds` are honoured; the lifetime is floored
/// at 600s (the TokenRequest minimum).
#[tokio::test]
async fn projected_sa_token_honours_audience_and_floors_expiration() {
    let e = env();
    seed_sa_and_node(&e).await;
    let path =
        e.vm.create_volume(
            &pod(),
            &projected_token_volume(json!({"audience": "vault", "expirationSeconds": 7200})),
        )
        .await
        .unwrap();
    let c = read_claims(&format!("{path}/sub/token"));
    assert_eq!(c.aud, vec!["vault".to_string()]);
    assert_eq!(c.exp - c.iat, 7200);

    let e2 = env();
    seed_sa_and_node(&e2).await;
    let path = e2
        .vm
        .create_volume(
            &pod(),
            &projected_token_volume(json!({"expirationSeconds": 10})),
        )
        .await
        .unwrap();
    let c = read_claims(&format!("{path}/sub/token"));
    assert_eq!(c.exp - c.iat, 600);
}

/// A ServiceAccount or Node absent from storage does not fail set-up: a
/// token is still written (not the placeholder). Its payload is inspected
/// raw because `KubeRef.uid` is skipped on serialize when empty yet required
/// on deserialize, so the token does not round-trip (filed separately).
#[tokio::test]
async fn projected_sa_token_without_sa_or_node_in_storage_still_writes_a_token() {
    let e = env();
    let path =
        e.vm.create_volume(&pod(), &projected_token_volume(json!({})))
            .await
            .unwrap();
    let token = std::fs::read_to_string(format!("{path}/sub/token")).unwrap();
    assert_eq!(token.split('.').count(), 3, "a JWT, got: {token}");
    assert!(!token.ends_with(".placeholder"));
}

/// The token file is `defaultMode` 0o440; the kubelet runs as root and can
/// overwrite it, a non-root test run needs the owner write bit back.
fn make_writable(file: &str) {
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o640)).unwrap();
}

fn set_age(file: &str, age_secs: u64) {
    // Read-only open: the token file is 0o440, and futimens needs only ownership.
    let f = std::fs::File::open(file).unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(age_secs))
        .unwrap();
}

/// Freshness check (upstream `requiresRefresh`, token_manager.go:174): a token
/// younger than 80% of its lifetime is reused, an older one is re-minted. A
/// sentinel written into the file distinguishes reuse from re-mint.
/// expirationSeconds=600 => refresh after 480s.
#[tokio::test]
async fn projected_sa_token_is_reminted_only_when_stale() {
    let e = env();
    seed_sa_and_node(&e).await;
    let v = projected_token_volume(json!({"expirationSeconds": 600}));
    let path = e.vm.create_volume(&pod(), &v).await.unwrap();
    let file = format!("{path}/sub/token");
    let c = read_claims(&file);
    assert_eq!(c.exp - c.iat, 600);

    // Fresh (400s old < 480s): the sentinel survives the per-sync re-SetUp.
    make_writable(&file);
    std::fs::write(&file, "SENTINEL").unwrap();
    set_age(&file, 400);
    e.vm.create_volume(&pod(), &v).await.unwrap();
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "SENTINEL");

    // Stale (500s old > 480s): re-minted.
    make_writable(&file);
    set_age(&file, 500);
    e.vm.create_volume(&pod(), &v).await.unwrap();
    assert_eq!(read_claims(&file).sub, "system:serviceaccount:default:sa1");
}
