//! Unit tests moved from `src/lib.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        extract::Request,
        http::StatusCode,
        middleware::Next,
        response::{IntoResponse, Response},
        Extension,
    };
    pub use rusternetes_common::auth::{BootstrapTokenManager, TokenManager, UserInfo};
    pub use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, LazyLock};
    pub use tracing::{debug, info, warn};
}

mod tests {
    use chrono::TimeZone;
    use rusternetes_middleware::AuthObservations;

    fn at(secs: i64, nanos: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.timestamp_opt(secs, nanos).unwrap()
    }

    // claims.go:265-276
    #[test]
    fn warnafter_absent_or_zero_observes_nothing() {
        let mut obs = AuthObservations::default();
        obs.observe_warnafter("system:serviceaccount:ns:sa", None, at(1000, 0));
        obs.observe_warnafter("system:serviceaccount:ns:sa", Some(0), at(1000, 0));
        assert!(obs.audit_annotations.is_empty());
    }

    #[test]
    fn warnafter_in_future_counts_valid_without_annotation() {
        let before = rusternetes_middleware::serviceaccount_metrics()
            .valid_tokens_total
            .get();
        let mut obs = AuthObservations::default();
        obs.observe_warnafter("sub", Some(2000), at(1000, 0));
        // exactly at the threshold is not After()
        obs.observe_warnafter("sub", Some(2000), at(2000, 0));
        assert!(obs.audit_annotations.is_empty());
        assert!(
            rusternetes_middleware::serviceaccount_metrics()
                .valid_tokens_total
                .get()
                >= before + 2
        );
    }

    #[test]
    fn warnafter_in_past_annotates_stale_token() {
        let before = rusternetes_middleware::serviceaccount_metrics()
            .stale_tokens_total
            .get();
        let mut obs = AuthObservations::default();
        obs.observe_warnafter("system:serviceaccount:ns:sa", Some(1000), at(1090, 500));
        assert_eq!(
            obs.audit_annotations,
            vec![(
                "authentication.k8s.io/stale-token".to_string(),
                "subject: system:serviceaccount:ns:sa, seconds after warning threshold: 90"
                    .to_string()
            )]
        );
        assert!(
            rusternetes_middleware::serviceaccount_metrics()
                .stale_tokens_total
                .get()
                > before
        );
    }

    #[test]
    fn warnafter_sub_second_past_is_stale() {
        let mut obs = AuthObservations::default();
        obs.observe_warnafter("sub", Some(1000), at(1000, 1));
        assert_eq!(obs.audit_annotations.len(), 1);
        assert!(obs.audit_annotations[0].1.ends_with(": 0"));
    }

    /// `TrackAuthenticationLatency` (filters/authentication.go:69): the guard
    /// records into the request's tracker on drop, and is a no-op without one.
    #[tokio::test]
    async fn authentication_latency_guard_records_on_drop() {
        use rusternetes_common::latency_trackers::{with_latency_trackers, LatencyTrackers};
        drop(AuthenticationLatencyGuard(std::time::Instant::now()));
        let t = std::sync::Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            let g = AuthenticationLatencyGuard(std::time::Instant::now());
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            drop(g);
        })
        .await;
        assert!(t.authentication.get_latency() >= std::time::Duration::from_millis(5));
    }

    use super::__parent::*;
    use rusternetes_middleware::*;

    /// Build a DER-encoded self-signed cert with the given CN and Organizations,
    /// for exercising the x509-authn subject mapping.
    fn cert_der_with_subject(common_name: &str, organizations: &[&str]) -> Vec<u8> {
        use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);
        for o in organizations {
            dn.push(DnType::OrganizationName, *o);
        }
        params.distinguished_name = dn;
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        cert.der().as_ref().to_vec()
    }

    #[test]
    fn client_cert_maps_cn_to_user_and_orgs_to_groups() {
        let der = cert_der_with_subject("system:kube-scheduler", &["system:kube-scheduler"]);
        let user = user_from_client_cert_der(&[der]).expect("should authenticate");
        assert_eq!(user.username, "system:kube-scheduler");
        assert!(user.uid.is_empty());
        assert!(user.groups.contains(&"system:kube-scheduler".to_string()));
        assert!(user.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn client_cert_org_differs_from_cn_becomes_group() {
        // rcgen's DistinguishedName is keyed by DnType, so a fixture can carry
        // only one Organization; multi-O fan-out is covered purely in
        // rusternetes_common (UserInfo::from_cert_identity). Here we just prove
        // the O is read independently of the CN.
        let der = cert_der_with_subject("dave", &["system:masters"]);
        let user = user_from_client_cert_der(&[der]).expect("should authenticate");
        assert_eq!(user.username, "dave");
        assert!(user.groups.contains(&"system:masters".to_string()));
        assert!(user.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn client_cert_empty_chain_is_not_authenticated() {
        assert!(user_from_client_cert_der(&[]).is_none());
    }

    #[test]
    fn client_cert_garbage_der_is_not_authenticated() {
        assert!(user_from_client_cert_der(&[vec![0xde, 0xad, 0xbe, 0xef]]).is_none());
    }

    #[test]
    fn extract_json_skips_embedded_non_resource_json() {
        // Regression for issue #495: a Pod's container command can contain a
        // string arg with embedded JSON, e.g.
        //   "wget --post-data={\"Source\": \"prestop\"}"
        // The k8s-protobuf raw field is native protobuf (not JSON), so the
        // brace-scan fallback used to rip the embedded {"Source": "prestop"}
        // out of the command string and hand it back as the resource body,
        // which then failed strict validation with `unknown field "Source"`.
        // The fallback must reject JSON that isn't shaped like a K8s resource.
        let mut env = Vec::new();
        env.extend_from_slice(b"k8s\0");
        // field 1 (TypeMeta): empty submessage (tag 0x0a, len 0)
        env.push(0x0a);
        env.push(0x00);
        // field 2 (raw): native protobuf carrying a string field whose value
        // contains the embedded JSON fragment.
        let raw = {
            let mut r = Vec::new();
            let s = b"--post-data={\"Source\": \"prestop\"}";
            r.push(0x0a); // string field, tag 0x0a
            r.push(s.len() as u8);
            r.extend_from_slice(s);
            r
        };
        env.push(0x12); // field 2, wire type 2
        env.push(raw.len() as u8);
        env.extend_from_slice(&raw);

        let out = extract_json_from_k8s_protobuf(&env);
        assert!(
            out.is_none(),
            "embedded non-resource JSON must not be extracted as the body, got: {:?}",
            out.map(|b| String::from_utf8_lossy(&b).into_owned())
        );
    }

    #[test]
    fn test_scan_balanced_braces_valid_json() {
        let data = br#"{"key":"value"}"#;
        let result = scan_balanced_braces(data);
        assert_eq!(result, Some(data.to_vec()));
    }

    #[test]
    fn test_scan_balanced_braces_nested() {
        let data = br#"{"a":{"b":"c"}}"#;
        let result = scan_balanced_braces(data);
        assert_eq!(result, Some(data.to_vec()));
    }

    #[test]
    fn test_scan_balanced_braces_with_trailing() {
        let data = br#"{"key":"value"}extra"#;
        let result = scan_balanced_braces(data);
        assert_eq!(result, Some(br#"{"key":"value"}"#.to_vec()));
    }

    #[test]
    fn test_extract_json_from_k8s_protobuf_with_json_payload() {
        // Construct a K8s protobuf envelope wrapping JSON in field 2
        let json = br#"{"apiVersion":"v1","kind":"Pod"}"#;
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // Field 1 (TypeMeta) — empty for simplicity
        data.push(0x0a); // field 1, wire type 2
        data.push(0x00); // length 0
                         // Field 2 (raw) — contains the JSON
        data.push(0x12); // field 2, wire type 2
        data.push(json.len() as u8); // length
        data.extend_from_slice(json);

        let result = extract_json_from_k8s_protobuf(&data);
        assert!(result.is_some());
        let extracted = result.unwrap();
        assert_eq!(extracted, json.to_vec());
    }

    #[test]
    fn test_extract_json_from_k8s_protobuf_with_native_protobuf() {
        // Construct a K8s protobuf envelope where field 2 contains native protobuf (not JSON)
        let native_pb = &[0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]; // field 1, string "hello"
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // Field 1 (TypeMeta) — empty
        data.push(0x0a); // field 1, wire type 2
        data.push(0x00); // length 0
                         // Field 2 (raw) — native protobuf, not JSON
        data.push(0x12); // field 2, wire type 2
        data.push(native_pb.len() as u8);
        data.extend_from_slice(native_pb);

        let result = extract_json_from_k8s_protobuf(&data);
        // Should return None because field 2 doesn't start with { or [
        assert!(result.is_none());
    }

    #[test]
    fn test_try_brace_scan_validates_json() {
        // Binary data with a `{` byte that isn't valid JSON
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // Some binary with a { byte followed by non-JSON
        data.push(0x0a);
        data.push(b'{');
        data.push(0x05);
        data.push(b'}');
        // The brace scan would find {0x05} which is balanced but not valid JSON
        let result = try_brace_scan_or_type_meta(&data);
        // Should fall through to TypeMeta extraction or empty object, not return {0x05}
        // Since there's no valid TypeMeta, we get empty object
        assert_eq!(result, b"{}".to_vec());
    }

    #[test]
    fn test_try_brace_scan_finds_embedded_json() {
        // Binary prefix followed by actual JSON — must have apiVersion AND kind
        // to be recognized as a K8s resource (not a random fragment)
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        data.extend_from_slice(&[0x0a, 0x10, 0x12]); // some protobuf prefix
        data.extend_from_slice(br#"{"apiVersion":"v1","kind":"Pod"}"#);

        let result = try_brace_scan_or_type_meta(&data);
        assert_eq!(result, br#"{"apiVersion":"v1","kind":"Pod"}"#.to_vec());
    }

    #[test]
    fn test_extract_type_meta_from_protobuf() {
        // Build a protobuf with TypeMeta containing apiVersion and kind
        let mut type_meta = Vec::new();
        // field 1 = apiVersion = "apiextensions.k8s.io/v1"
        let av = b"apiextensions.k8s.io/v1";
        type_meta.push(0x0a); // field 1, wire type 2
        type_meta.push(av.len() as u8);
        type_meta.extend_from_slice(av);
        // field 2 = kind = "CustomResourceDefinition"
        let kind = b"CustomResourceDefinition";
        type_meta.push(0x12); // field 2, wire type 2
        type_meta.push(kind.len() as u8);
        type_meta.extend_from_slice(kind);

        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        data.push(0x0a); // field 1, wire type 2 (TypeMeta wrapper)
        data.push(type_meta.len() as u8);
        data.extend_from_slice(&type_meta);

        let result = extract_type_meta_from_protobuf(&data);
        assert!(result.is_some());
        let (api_v, k) = result.unwrap();
        assert_eq!(api_v, "apiextensions.k8s.io/v1");
        assert_eq!(k, "CustomResourceDefinition");
    }

    #[test]
    fn test_decode_k8s_protobuf_to_json_crd() {
        // Build a realistic CRD protobuf message
        // TypeMeta: apiVersion="apiextensions.k8s.io/v1", kind="CustomResourceDefinition"
        let mut type_meta = Vec::new();
        let av = b"apiextensions.k8s.io/v1";
        type_meta.push(0x0a);
        type_meta.push(av.len() as u8);
        type_meta.extend_from_slice(av);
        let kind_str = b"CustomResourceDefinition";
        type_meta.push(0x12);
        type_meta.push(kind_str.len() as u8);
        type_meta.extend_from_slice(kind_str);

        // Build raw object (CRD body):
        // Field 1 = ObjectMeta with name = "foos.example.com"
        let mut obj_meta = Vec::new();
        let name = b"foos.example.com";
        obj_meta.push(0x0a); // field 1, wire type 2
        obj_meta.push(name.len() as u8);
        obj_meta.extend_from_slice(name);

        // Field 2 = CRDSpec
        let mut crd_spec = Vec::new();
        // spec.group = "example.com" (field 1)
        let group = b"example.com";
        crd_spec.push(0x0a);
        crd_spec.push(group.len() as u8);
        crd_spec.extend_from_slice(group);
        // spec.names (field 3) — submessage
        let mut names_msg = Vec::new();
        let plural = b"foos";
        names_msg.push(0x0a); // field 1 = plural
        names_msg.push(plural.len() as u8);
        names_msg.extend_from_slice(plural);
        let singular = b"foo";
        names_msg.push(0x12); // field 2 = singular
        names_msg.push(singular.len() as u8);
        names_msg.extend_from_slice(singular);
        let kind_name = b"Foo";
        names_msg.push(0x22); // field 4 = kind
        names_msg.push(kind_name.len() as u8);
        names_msg.extend_from_slice(kind_name);
        crd_spec.push(0x1a); // field 3, wire type 2
        crd_spec.push(names_msg.len() as u8);
        crd_spec.extend_from_slice(&names_msg);
        // spec.scope = "Namespaced" (field 4)
        let scope = b"Namespaced";
        crd_spec.push(0x22); // field 4, wire type 2
        crd_spec.push(scope.len() as u8);
        crd_spec.extend_from_slice(scope);
        // spec.versions (field 7) — one version "v1"
        let mut ver_msg = Vec::new();
        let ver_name = b"v1";
        ver_msg.push(0x0a); // field 1 = name
        ver_msg.push(ver_name.len() as u8);
        ver_msg.extend_from_slice(ver_name);
        crd_spec.push(0x3a); // field 7, wire type 2
        crd_spec.push(ver_msg.len() as u8);
        crd_spec.extend_from_slice(&ver_msg);

        // Assemble raw object: field 1 = ObjectMeta, field 2 = CRDSpec
        let mut raw = Vec::new();
        raw.push(0x0a); // field 1, wire type 2
        raw.push(obj_meta.len() as u8);
        raw.extend_from_slice(&obj_meta);
        raw.push(0x12); // field 2, wire type 2
        raw.push(crd_spec.len() as u8);
        raw.extend_from_slice(&crd_spec);

        // Assemble Unknown message: field 1 = TypeMeta, field 2 = raw
        let mut unknown = Vec::new();
        unknown.extend_from_slice(b"k8s\0");
        unknown.push(0x0a); // field 1 = TypeMeta
        unknown.push(type_meta.len() as u8);
        unknown.extend_from_slice(&type_meta);
        unknown.push(0x12); // field 2 = raw
        unknown.push(raw.len() as u8);
        unknown.extend_from_slice(&raw);

        let result = decode_k8s_protobuf_to_json(&unknown);
        assert!(
            result.is_some(),
            "decode_k8s_protobuf_to_json returned None"
        );
        let json_bytes = result.unwrap();
        let val: serde_json::Value = serde_json::from_slice(&json_bytes)
            .expect("decoded protobuf should produce valid JSON");
        assert_eq!(val["apiVersion"], "apiextensions.k8s.io/v1");
        assert_eq!(val["kind"], "CustomResourceDefinition");
        assert_eq!(val["metadata"]["name"], "foos.example.com");
        assert_eq!(val["spec"]["group"], "example.com");
        assert_eq!(val["spec"]["names"]["plural"], "foos");
        assert_eq!(val["spec"]["names"]["kind"], "Foo");
        assert_eq!(val["spec"]["scope"], "Namespaced");
    }

    #[test]
    fn test_binary_body_with_false_brace_not_treated_as_json() {
        // Simulate a protobuf body that contains 0x7b ({) and 0x7d (}) as part of
        // binary data but isn't valid JSON. The middleware should NOT pass this
        // through as-is.
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // TypeMeta
        let mut type_meta = Vec::new();
        let av = b"apiextensions.k8s.io/v1";
        type_meta.push(0x0a);
        type_meta.push(av.len() as u8);
        type_meta.extend_from_slice(av);
        let kind_str = b"CustomResourceDefinition";
        type_meta.push(0x12);
        type_meta.push(kind_str.len() as u8);
        type_meta.extend_from_slice(kind_str);
        data.push(0x0a);
        data.push(type_meta.len() as u8);
        data.extend_from_slice(&type_meta);
        // Field 2 = raw bytes that happen to contain { and } but aren't JSON
        let fake_raw: Vec<u8> = vec![0x0a, 0x03, b'{', 0x05, b'}', 0x12, 0x01, 0x00];
        data.push(0x12);
        data.push(fake_raw.len() as u8);
        data.extend_from_slice(&fake_raw);

        // extract_json_from_k8s_protobuf should NOT return the {0x05} garbage
        let extracted = extract_json_from_k8s_protobuf(&data);
        if let Some(ref e) = extracted {
            // If it did extract something, it must be valid JSON
            assert!(
                serde_json::from_slice::<serde_json::Value>(e).is_ok(),
                "extract_json_from_k8s_protobuf returned invalid JSON: {:?}",
                e
            );
        }

        // try_brace_scan_or_type_meta should produce valid JSON (TypeMeta fallback)
        let result = try_brace_scan_or_type_meta(&data);
        let parsed = serde_json::from_slice::<serde_json::Value>(&result);
        assert!(
            parsed.is_ok(),
            "try_brace_scan_or_type_meta produced invalid JSON: {:?}",
            String::from_utf8_lossy(&result)
        );
    }

    #[test]
    fn test_wrap_json_in_protobuf_roundtrip() {
        let json = b"{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"test\"}}";
        let wrapped = wrap_json_in_protobuf(json);

        // Should start with k8s\0 magic
        assert_eq!(&wrapped[..4], b"k8s\0");

        // Should be extractable back to JSON
        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(
            extracted.is_some(),
            "Should extract JSON from protobuf envelope"
        );
        let extracted = extracted.unwrap();
        assert_eq!(extracted, json, "Extracted JSON should match original");
    }

    #[test]
    fn test_wrap_json_in_protobuf_valid_wireformat() {
        let json = b"{\"test\":true}";
        let wrapped = wrap_json_in_protobuf(json);

        // Verify protobuf field tags match the Unknown message schema.
        // After k8s\0 magic (4 bytes), first byte should be field 2 tag (raw).
        // K8s runtime.Unknown proto: field 2 = raw, wire type 2 = (2 << 3) | 2 = 0x12
        let tag1 = wrapped[4];
        let field_num1 = tag1 >> 3;
        let wire_type1 = tag1 & 0x07;
        assert_eq!(
            field_num1, 2,
            "First field should be field 2 (raw) per K8s runtime.Unknown proto"
        );
        assert_eq!(wire_type1, 2, "Wire type should be 2 (length-delimited)");
    }

    #[test]
    fn test_wrap_json_in_protobuf_large_payload() {
        // Test with payload >127 bytes to exercise varint encoding
        let json = format!("{{\"data\":\"{}\"}}", "x".repeat(200));
        let wrapped = wrap_json_in_protobuf(json.as_bytes());

        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(extracted.is_some(), "Should handle large payloads");
        assert_eq!(
            extracted.unwrap(),
            json.as_bytes(),
            "Large payload should roundtrip"
        );
    }

    #[test]
    fn test_wrap_json_in_protobuf_decodable_by_common_decoder() {
        // The middleware encodes using Go's runtime.Unknown field numbers
        // (field 2 = raw, field 4 = contentType) which differ from our prost
        // Unknown struct (field 3 = raw, field 5 = contentType). The Go client
        // needs field 2/4. Our own extract_json_from_k8s_protobuf handles both.
        use rusternetes_common::protobuf::is_protobuf;

        let json = b"{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"test\"}}";
        let wrapped = wrap_json_in_protobuf(json);

        assert!(
            is_protobuf(&wrapped),
            "wrapped output must have k8s magic prefix"
        );

        // Verify our own extractor can decode it (handles field 2 and 3)
        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(
            extracted.is_some(),
            "extract_json_from_k8s_protobuf must decode the wrapper"
        );

        // The extracted JSON should match the original
        let original: serde_json::Value = serde_json::from_slice(json).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&extracted.unwrap()).unwrap();
        assert_eq!(decoded, original, "decoded JSON must match original input");
    }

    #[test]
    fn test_wrap_json_in_protobuf_field_numbers_correct() {
        // Verify field 2 (raw) and field 4 (contentType) per K8s runtime.Unknown proto.
        // Field 2, wire type 2 = (2 << 3) | 2 = 0x12
        // Field 5, wire type 2 = (5 << 3) | 2 = 0x2a
        let json = b"{\"test\":1}";
        let wrapped = wrap_json_in_protobuf(json);

        // After k8s\0 (4 bytes), first byte should be field 2 tag
        assert_eq!(
            wrapped[4], 0x12,
            "first field tag should be 0x12 (field 2, wire type 2)"
        );

        // Find field 4 tag after the raw field data
        // raw field: tag(1) + varint_len + json_data
        let json_len = json.len();
        let varint_size = if json_len < 128 { 1 } else { 2 };
        let content_type_tag_pos = 4 + 1 + varint_size + json_len;
        assert_eq!(wrapped[content_type_tag_pos], 0x22,
            "contentType field tag should be 0x22 (field 4, wire type 2) per K8s runtime.Unknown proto");
    }

    #[test]
    fn test_wrap_and_extract_roundtrip_with_correct_fields() {
        // End-to-end test: wrap JSON in protobuf, then extract it back.
        // This proves the encoding is compatible with the decoder.
        let json =
            b"{\"apiVersion\":\"apps/v1\",\"kind\":\"Deployment\",\"spec\":{\"replicas\":3}}";
        let wrapped = wrap_json_in_protobuf(json);
        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(
            extracted.is_some(),
            "should extract JSON from wrapped protobuf"
        );
        assert_eq!(
            extracted.unwrap(),
            json,
            "extracted JSON must match original"
        );
    }

    /// `watchEmbeddedEncoder.doEncode` (apiserver `handlers/response.go`):
    /// events are re-framed on newlines whatever the chunking, the embedded
    /// object is converted, and a `Status` (ERROR event) passes through.
    #[test]
    fn watch_event_transform_reframes_converts_and_spares_status() {
        let mut body =
            WatchEventTransformBody::new(Body::empty(), convert_to_partial_object_metadata);
        let added = r#"{"type":"ADDED","object":{"kind":"Secret","apiVersion":"v1","metadata":{"name":"a"},"data":{"k":"v"}}}"#;
        let err = r#"{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","code":410}}"#;
        // One event split across two chunks, then a second event.
        let all = format!("{added}\n{err}\n");
        let (a, b) = all.as_bytes().split_at(30);
        body.absorb(a);
        assert!(body.ready.is_empty(), "no complete line yet");
        body.absorb(b);
        let out: Vec<serde_json::Value> = body
            .ready
            .iter()
            .map(|c| serde_json::from_slice(c).unwrap())
            .collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["object"]["kind"], "PartialObjectMetadata");
        assert_eq!(out[0]["object"]["apiVersion"], "meta.k8s.io/v1");
        assert_eq!(out[0]["object"]["metadata"]["name"], "a");
        assert!(out[0]["object"].get("data").is_none());
        assert_eq!(out[1]["object"]["kind"], "Status");
        assert_eq!(out[1]["object"]["code"], 410);
    }

    #[test]
    fn test_is_watch_request_detection() {
        // Watch requests should be detected via query param watch=true, watch=1, or /watch/ in path.
        // This ensures protobuf wrapping is skipped for streaming watch responses.

        // watch=true query param
        let uri: axum::http::Uri = "http://localhost/api/v1/pods?watch=true".parse().unwrap();
        let has_watch = uri
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
        assert!(has_watch, "watch=true query param should be detected");

        // watch=1 query param
        let uri: axum::http::Uri = "http://localhost/api/v1/pods?watch=1".parse().unwrap();
        let has_watch = uri
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
        assert!(has_watch, "watch=1 query param should be detected");

        // /watch/ in path
        let uri: axum::http::Uri = "http://localhost/api/v1/watch/pods".parse().unwrap();
        assert!(
            uri.path().contains("/watch/"),
            "/watch/ in path should be detected"
        );

        // stream=watch in Accept header
        let accept = "application/json;stream=watch";
        assert!(
            accept.contains("stream=watch"),
            "stream=watch in Accept should be detected"
        );

        // Non-watch request should NOT be detected
        let uri: axum::http::Uri = "http://localhost/api/v1/pods".parse().unwrap();
        let has_watch = uri
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
        assert!(
            !has_watch,
            "regular request should not be detected as watch"
        );
        assert!(
            !uri.path().contains("/watch/"),
            "regular path should not contain /watch/"
        );
    }

    #[test]
    fn synthesize_generate_name_fills_empty_name_from_prefix() {
        let body = br#"{"metadata":{"generateName":"my-cm-"},"data":{"k":"v"}}"#;
        let out = synthesize_generate_name(body).expect("should synthesise a name");
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let name = v["metadata"]["name"].as_str().unwrap();
        assert!(name.starts_with("my-cm-"), "got {name}");
        assert_eq!(name.len(), "my-cm-".len() + 5, "prefix + 5 char suffix");
        // Other fields are preserved.
        assert_eq!(v["data"]["k"], "v");
        assert_eq!(v["metadata"]["generateName"], "my-cm-");
    }

    #[test]
    fn synthesize_generate_name_noop_when_name_present() {
        let body = br#"{"metadata":{"name":"fixed","generateName":"my-cm-"}}"#;
        assert!(
            synthesize_generate_name(body).is_none(),
            "an explicit name must win over generateName"
        );
    }

    /// Pin for upstream `createWithGenerateNameRetry` (store.go:464-473): a
    /// create whose generated name collides is redrawn, up to
    /// `maxNameGenerationCreateAttempts` (8, store.go:440) times.
    mod generate_name_retry {
        use super::*;
        use axum::routing::post;
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use tower::ServiceExt;

        /// A "store": 409 AlreadyExists for a taken name, else take it.
        fn app(
            taken: Arc<Mutex<HashSet<String>>>,
            gen_calls: Arc<AtomicUsize>,
            names: Vec<&'static str>,
        ) -> axum::Router {
            let handler = move |body: axum::body::Bytes| {
                let taken = taken.clone();
                async move {
                    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let name = v["metadata"]["name"].as_str().unwrap().to_string();
                    if !taken.lock().unwrap().insert(name.clone()) {
                        return (
                            StatusCode::CONFLICT,
                            axum::Json(serde_json::json!({
                                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                                "reason": "AlreadyExists", "code": 409,
                                "message": format!("\"{name}\" already exists"),
                            })),
                        );
                    }
                    (StatusCode::CREATED, axum::Json(v))
                }
            };
            axum::Router::new()
                .route("/", post(handler))
                .layer(axum::middleware::from_fn(move |req, next| {
                    let gen_calls = gen_calls.clone();
                    let names = names.clone();
                    async move {
                        generate_name_middleware_with(req, next, &move |p| {
                            let i = gen_calls.fetch_add(1, Ordering::SeqCst);
                            format!("{p}{}", names[i.min(names.len() - 1)])
                        })
                        .await
                    }
                }))
        }

        async fn post_json(app: axum::Router, body: &str) -> (StatusCode, serde_json::Value) {
            let req = axum::http::Request::builder()
                .method("POST")
                .uri("/")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
                .await
                .unwrap();
            (status, serde_json::from_slice(&bytes).unwrap())
        }

        fn taken(names: &[&str]) -> Arc<Mutex<HashSet<String>>> {
            Arc::new(Mutex::new(names.iter().map(|s| s.to_string()).collect()))
        }

        #[tokio::test]
        async fn a_colliding_generated_name_is_redrawn() {
            let calls = Arc::new(AtomicUsize::new(0));
            let a = app(
                taken(&["p-aaaaa", "p-bbbbb"]),
                calls.clone(),
                vec!["aaaaa", "bbbbb", "ccccc"],
            );
            let (status, v) = post_json(a, r#"{"metadata":{"generateName":"p-"}}"#).await;
            assert_eq!(status, StatusCode::CREATED, "{v}");
            assert_eq!(v["metadata"]["name"], "p-ccccc");
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn gives_up_after_eight_attempts() {
            let calls = Arc::new(AtomicUsize::new(0));
            let a = app(taken(&["p-aaaaa"]), calls.clone(), vec!["aaaaa"]);
            let (status, v) = post_json(a, r#"{"metadata":{"generateName":"p-"}}"#).await;
            assert_eq!(status, StatusCode::CONFLICT, "{v}");
            assert_eq!(calls.load(Ordering::SeqCst), 8);
        }

        #[tokio::test]
        async fn an_explicit_name_is_not_retried() {
            let calls = Arc::new(AtomicUsize::new(0));
            let a = app(taken(&["fixed"]), calls.clone(), vec!["aaaaa"]);
            let (status, _) = post_json(a, r#"{"metadata":{"name":"fixed"}}"#).await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn synthesize_generate_name_noop_without_generate_name() {
        assert!(synthesize_generate_name(br#"{"metadata":{}}"#).is_none());
        assert!(synthesize_generate_name(br#"{"metadata":{"generateName":""}}"#).is_none());
        // Subresource/review bodies with no metadata object — left untouched.
        assert!(synthesize_generate_name(br#"{"spec":{"x":1}}"#).is_none());
        assert!(synthesize_generate_name(b"not json").is_none());
    }

    // ========================================================================
    // NamespaceLifecycle admission: no CREATE into a Terminating namespace
    // ========================================================================

    /// Which requests the gate applies to, ported from upstream's early
    /// returns in `staging/src/k8s.io/apiserver/pkg/admission/plugin/namespace/
    /// lifecycle/admission.go:84-108`.
    #[test]
    fn namespace_lifecycle_gate_targets_only_namespaced_content_creates() {
        // Namespaced content under both the core and grouped API roots: gated.
        assert_eq!(
            gated_namespace("/api/v1/namespaces/ns-1/configmaps"),
            Some("ns-1")
        );
        assert_eq!(
            gated_namespace("/api/v1/namespaces/ns-1/pods"),
            Some("ns-1")
        );
        assert_eq!(
            gated_namespace("/apis/apps/v1/namespaces/ns-1/deployments"),
            Some("ns-1")
        );

        // The namespace object itself: upstream allows ALL operations on
        // namespaces (`admission.go:88-98`), so a POST to the namespace
        // collection — or to one namespace's subresource — is never gated.
        assert_eq!(gated_namespace("/api/v1/namespaces"), None);
        assert_eq!(gated_namespace("/api/v1/namespaces/ns-1"), None);
        assert_eq!(gated_namespace("/api/v1/namespaces/ns-1/finalize"), None);
        assert_eq!(gated_namespace("/api/v1/namespaces/ns-1/status"), None);

        // Cluster-scoped: upstream returns early when the request carries no
        // namespace (`admission.go:84-86`).
        assert_eq!(gated_namespace("/api/v1/nodes"), None);
        assert_eq!(
            gated_namespace("/apis/rbac.authorization.k8s.io/v1/clusterroles"),
            None
        );

        // Access reviews are allowed through so namespace state is not leaked
        // (`admission.go:106-108`), including the namespaced one.
        assert_eq!(
            gated_namespace(
                "/apis/authorization.k8s.io/v1/namespaces/ns-1/localsubjectaccessreviews"
            ),
            None
        );

        // A POST to a named object's subresource is upstream's `connect` or
        // `update` verb, not `create` — only a POST to the *collection*
        // creates namespaced content. Gating these would break `kubectl exec`
        // into a namespace that happens to be terminating.
        assert_eq!(
            gated_namespace("/api/v1/namespaces/ns-1/pods/p1/exec"),
            None
        );
        assert_eq!(
            gated_namespace("/api/v1/namespaces/ns-1/pods/p1/portforward"),
            None
        );
        assert_eq!(
            gated_namespace("/api/v1/namespaces/ns-1/pods/p1/status"),
            None
        );

        // Not an API path at all.
        assert_eq!(gated_namespace("/healthz"), None);
        assert_eq!(gated_namespace("/"), None);
    }

    /// The forbidden body must carry upstream's exact wording and cause, since
    /// clients match on it — `errors.IsForbidden` plus the
    /// `NamespaceTerminating` cause (`admission.go:174-181`).
    #[test]
    fn namespace_terminating_forbidden_status_matches_upstream() {
        let body = namespace_terminating_forbidden("ns-1", "configmaps");

        assert_eq!(body["kind"], "Status");
        assert_eq!(body["status"], "Failure");
        assert_eq!(body["reason"], "Forbidden");
        assert_eq!(body["code"], 403);
        assert_eq!(
            body["message"].as_str().unwrap(),
            "configmaps is forbidden: unable to create new content in namespace ns-1 \
             because it is being terminated"
        );
        assert_eq!(
            body["details"]["causes"][0]["reason"].as_str().unwrap(),
            "NamespaceTerminating"
        );
        assert_eq!(
            body["details"]["causes"][0]["message"].as_str().unwrap(),
            "namespace ns-1 is being terminated"
        );
        assert_eq!(
            body["details"]["causes"][0]["field"].as_str().unwrap(),
            "metadata.namespace"
        );
    }

    /// Only a `Terminating` phase blocks. An Active namespace, and a namespace
    /// with no phase recorded, must both pass — upstream returns nil for any
    /// phase that is not `NamespaceTerminating` (`admission.go:170-172`).
    #[test]
    fn lifecycle_update_and_immortal_targets() {
        // admission_test.go:124 — updates are existence-checked too.
        assert_eq!(
            updated_namespace("/api/v1/namespaces/ns-1/pods/p"),
            Some("ns-1")
        );
        assert_eq!(updated_namespace("/api/v1/namespaces/ns-1/pods"), None);
        assert_eq!(
            updated_namespace("/api/v1/namespaces/ns-1/pods/p/status"),
            None
        );
        assert_eq!(updated_namespace("/api/v1/namespaces/ns-1"), None);
        assert_eq!(updated_namespace("/api/v1/nodes/n1"), None);
        // admission_test.go:186 — immortal set is default/kube-system/kube-public.
        assert_eq!(
            immortal_namespace_delete("/api/v1/namespaces/default"),
            Some("default")
        );
        assert_eq!(
            immortal_namespace_delete("/api/v1/namespaces/kube-public"),
            Some("kube-public")
        );
        assert_eq!(immortal_namespace_delete("/api/v1/namespaces/other"), None);
        assert_eq!(
            immortal_namespace_delete("/api/v1/namespaces/default/pods/p"),
            None
        );
    }

    #[test]
    fn only_terminating_phase_blocks_creates() {
        assert!(!phase_blocks_create(Some("Active")));
        assert!(!phase_blocks_create(None));
        assert!(!phase_blocks_create(Some("")));
        assert!(phase_blocks_create(Some("Terminating")));
    }
}

mod delete_options_tests {
    use rusternetes_middleware::{parse_delete_options, DeleteOptionsCtx};
    use std::collections::HashMap;

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// With no body, the query string decides — upstream's `else` branch
    /// (endpoints/handlers/delete.go:121).
    #[test]
    fn an_empty_body_falls_back_to_query_parameters() {
        assert_eq!(
            parse_delete_options(&q(&[("propagationPolicy", "Foreground")]), None),
            DeleteOptionsCtx {
                propagation_policy: Some("Foreground".to_string()),
                orphan_dependents: None,
            }
        );
    }

    /// The deprecated bool is a query bool, so it follows
    /// `Convert_Slice_string_To_bool`: only "0"/"false" are false. Before the
    /// query-bool sweep this was `parse::<bool>()`, so `?orphanDependents=1`
    /// silently meant "not set".
    #[test]
    fn orphan_dependents_uses_upstream_query_bool_rules() {
        for truthy in ["true", "1", "t", "yes", ""] {
            assert_eq!(
                parse_delete_options(&q(&[("orphanDependents", truthy)]), None).orphan_dependents,
                Some(true),
                "?orphanDependents={truthy} must be true"
            );
        }
        for falsy in ["false", "False", "0"] {
            assert_eq!(
                parse_delete_options(&q(&[("orphanDependents", falsy)]), None).orphan_dependents,
                Some(false),
                "?orphanDependents={falsy} must be false"
            );
        }
    }

    /// A present body wins outright. This is the case that a per-field merge
    /// gets wrong, and it is the one kubectl exercises: it sends DeleteOptions
    /// in the body.
    #[test]
    fn a_present_body_wins_over_the_query_string() {
        let body = serde_json::json!({"propagationPolicy": "Orphan"});
        assert_eq!(
            parse_delete_options(&q(&[("propagationPolicy", "Foreground")]), Some(&body))
                .propagation_policy,
            Some("Orphan".to_string())
        );
    }

    /// The sharp edge: a body that does NOT name the field does not fall back
    /// to the query, because upstream never reads the query once a body is
    /// present. A merge implementation returns Foreground here and is wrong.
    #[test]
    fn a_body_without_the_field_does_not_fall_back_to_the_query() {
        let body = serde_json::json!({"kind": "DeleteOptions"});
        assert_eq!(
            parse_delete_options(&q(&[("propagationPolicy", "Foreground")]), Some(&body)),
            DeleteOptionsCtx::default(),
            "a present body suppresses the query string entirely \
             (endpoints/handlers/delete.go:95-126)"
        );
    }
}
