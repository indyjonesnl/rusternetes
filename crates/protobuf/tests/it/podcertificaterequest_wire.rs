//! Wire-level tests for certificates.k8s.io/v1beta1 PodCertificateRequest
//! (+ List). Payloads are hand-encoded with the UPSTREAM field numbers from
//! staging/src/k8s.io/api/certificates/v1beta1/generated.proto
//! (`message PodCertificateRequestSpec` / `...Status`; note Status skips
//! field 3, so `notBefore` is tag 4). The API server serves
//! `/apis/certificates.k8s.io/v1beta1/.../podcertificaterequests`, so a
//! protobuf client (Accept: application/vnd.kubernetes.protobuf) must work.
use rusternetes_protobuf::ProtoRegistry;
use serde_json::json;

fn len_field(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![tag, body.len() as u8];
    v.extend_from_slice(body);
    v
}

fn pcr_bytes() -> Vec<u8> {
    let mut spec = Vec::new();
    spec.extend(len_field(0x0A, b"a")); // signerName = 1
    spec.extend([0x40, 0x90, 0x1C]); // maxExpirationSeconds = 8 -> 3600
    spec.extend(len_field(0x4A, &[1, 2])); // pkixPublicKey = 9
    spec.extend(len_field(0x5A, &[0x0A, 1, b'k', 0x12, 1, b'v'])); // unverifiedUserAnnotations = 11
    let mut status = Vec::new();
    status.extend(len_field(0x12, b"X")); // certificateChain = 2
    status.extend(len_field(0x22, &[0x08, 0x01])); // notBefore = 4
    let mut pcr = Vec::new();
    pcr.extend(len_field(0x12, &spec)); // spec = 2
    pcr.extend(len_field(0x1A, &status)); // status = 3
    pcr
}

#[test]
fn decodes_podcertificaterequest_at_upstream_tags() {
    let d = ProtoRegistry::new()
        .decode_message("PodCertificateRequest", &pcr_bytes())
        .expect("PodCertificateRequest must be registered");
    assert_eq!(d.pointer("/spec/signerName"), Some(&json!("a")), "{d}");
    assert_eq!(d.pointer("/spec/maxExpirationSeconds"), Some(&json!(3600)));
    assert_eq!(d.pointer("/spec/pkixPublicKey"), Some(&json!("AQI=")));
    assert_eq!(
        d.pointer("/spec/unverifiedUserAnnotations/k"),
        Some(&json!("v"))
    );
    assert_eq!(d.pointer("/status/certificateChain"), Some(&json!("X")));
    assert!(d.pointer("/status/notBefore").is_some(), "{d}");
}

#[test]
fn decodes_podcertificaterequest_list() {
    let item = pcr_bytes();
    let list = len_field(0x12, &item); // items = 2
    let d = ProtoRegistry::new()
        .decode_message("PodCertificateRequestList", &list)
        .expect("PodCertificateRequestList must be registered");
    assert_eq!(
        d.pointer("/items/0/spec/signerName"),
        Some(&json!("a")),
        "{d}"
    );
}

#[test]
fn podcertificaterequest_round_trips() {
    let reg = ProtoRegistry::new();
    let v = json!({
        "metadata": {"name": "r", "namespace": "ns"},
        "spec": {"signerName": "a", "podName": "p", "podUID": "u",
                 "serviceAccountName": "sa", "serviceAccountUID": "su",
                 "nodeName": "n", "nodeUID": "nu", "maxExpirationSeconds": 3600,
                 "pkixPublicKey": "AQI=", "proofOfPossession": "AwQ=",
                 "unverifiedUserAnnotations": {"k": "v"}},
        "status": {"certificateChain": "X"}
    });
    let b = reg.encode_message("PodCertificateRequest", &v).unwrap();
    let d = reg.decode_message("PodCertificateRequest", &b).unwrap();
    for p in [
        "/spec/signerName",
        "/spec/podName",
        "/spec/podUID",
        "/spec/serviceAccountName",
        "/spec/serviceAccountUID",
        "/spec/nodeName",
        "/spec/nodeUID",
        "/spec/maxExpirationSeconds",
        "/spec/pkixPublicKey",
        "/spec/proofOfPossession",
        "/spec/unverifiedUserAnnotations/k",
        "/status/certificateChain",
    ] {
        assert_eq!(d.pointer(p), v.pointer(p), "{p}: {d}");
    }
}
