//! #2952 end to end: a native-protobuf PVC with `storageClassName: ""`
//! (explicit "no class") must NOT get the default StorageClass.
//! Upstream: `PersistentVolumeClaimHasClass` is `Spec.StorageClassName != nil`
//! (pkg/apis/core/helper/helpers.go:476-487), and generated.pb.go writes a
//! non-nil empty `*string` as `0x2a 0x00`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

const PROTO_CT: &str = "application/vnd.kubernetes.protobuf";

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

fn ld(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_varint(buf, ((field as u64) << 3) | 2);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

fn envelope(raw: &[u8]) -> Vec<u8> {
    let mut tm = Vec::new();
    ld(&mut tm, 1, b"v1");
    ld(&mut tm, 2, b"PersistentVolumeClaim");
    let mut unknown = Vec::new();
    ld(&mut unknown, 1, &tm);
    ld(&mut unknown, 2, raw);
    ld(&mut unknown, 4, PROTO_CT.as_bytes());
    let mut out = b"k8s\0".to_vec();
    out.extend_from_slice(&unknown);
    out
}

/// PVC bytes; `storage_class` = Some(s) writes field 5 (even when empty).
fn pvc_bytes(name: &str, storage_class: Option<&str>) -> Vec<u8> {
    let mut meta = Vec::new();
    ld(&mut meta, 1, name.as_bytes());
    // resources(2) = VolumeResourceRequirements{requests(2): {"storage": Quantity{"1Gi"}}}
    let mut quantity = Vec::new();
    ld(&mut quantity, 1, b"1Gi");
    let mut entry = Vec::new();
    ld(&mut entry, 1, b"storage");
    ld(&mut entry, 2, &quantity);
    let mut reqs = Vec::new();
    ld(&mut reqs, 2, &entry);
    let mut spec = Vec::new();
    ld(&mut spec, 1, b"ReadWriteOnce"); // accessModes
    ld(&mut spec, 2, &reqs); // resources
    if let Some(sc) = storage_class {
        ld(&mut spec, 5, sc.as_bytes()); // storageClassName *string
    }
    let mut pvc = Vec::new();
    ld(&mut pvc, 1, &meta);
    ld(&mut pvc, 2, &spec);
    pvc
}

async fn server_with_default_class() -> TestApiServer {
    let s = TestApiServer::new();
    let (st, body) = s
        .post(
            "/apis/storage.k8s.io/v1/storageclasses",
            &json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": "default-sc", "annotations":
                    {"storageclass.kubernetes.io/is-default-class": "true"}},
                "provisioner": "kubernetes.io/no-provisioner"
            }),
        )
        .await;
    assert!(st.is_success(), "create default class: {st} {body}");
    s
}

async fn post_pvc(s: &TestApiServer, body: Vec<u8>) {
    let (status, _h, bytes, _) = s
        .send_with_headers(
            "POST",
            "/api/v1/namespaces/default/persistentvolumeclaims",
            &[("content-type", PROTO_CT), ("accept", "application/json")],
            Some(body),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create PVC: {}",
        String::from_utf8_lossy(&bytes)
    );
}

#[tokio::test]
async fn proto_pvc_with_explicit_empty_storage_class_is_not_defaulted() {
    let s = server_with_default_class().await;
    post_pvc(&s, envelope(&pvc_bytes("empty-class", Some("")))).await;
    let (st, got) = s
        .get("/api/v1/namespaces/default/persistentvolumeclaims/empty-class")
        .await;
    assert!(st.is_success(), "{st} {got}");
    assert_eq!(
        got["spec"]["storageClassName"],
        json!(""),
        "explicit \"\" must be kept, not defaulted; spec={}",
        got["spec"]
    );
}

#[tokio::test]
async fn proto_pvc_without_storage_class_gets_the_default() {
    let s = server_with_default_class().await;
    post_pvc(&s, envelope(&pvc_bytes("no-class", None))).await;
    let (st, got) = s
        .get("/api/v1/namespaces/default/persistentvolumeclaims/no-class")
        .await;
    assert!(st.is_success(), "{st} {got}");
    assert_eq!(got["spec"]["storageClassName"], json!("default-sc"));
}
