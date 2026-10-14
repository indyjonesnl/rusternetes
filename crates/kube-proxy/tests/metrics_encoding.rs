//! Exposition negotiation from upstream promhttp/http.go:193-212 and
//! prometheus/common/expfmt/encode.go:65-99.
use axum::{body::Body, http::Request};
use rusternetes_kube_proxy::serve_metrics::metrics_router;
use tower::ServiceExt;

async fn scrape(accept: &str, encoding: &str) -> (axum::http::HeaderMap, Vec<u8>) {
    let response = metrics_router("iptables")
        .oneshot(
            Request::builder()
                .uri("/metrics/slis")
                .header("accept", accept)
                .header("accept-encoding", encoding)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (headers, body.to_vec())
}

#[tokio::test]
async fn text_fallback_protobuf_and_gzip_negotiation() {
    let (headers, plain) = scrape("application/openmetrics-text", "").await;
    assert!(headers["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/plain; version=0.0.4"));
    assert!(!plain.ends_with(b"# EOF\n")); // OpenMetrics disabled in upstream opts
    assert!(headers.contains_key("process-start-time-unix"));
    let (headers, body) = scrape("application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited", "").await;
    assert!(headers["content-type"]
        .to_str()
        .unwrap()
        .contains("encoding=delimited"));
    let mut input = protobuf::CodedInputStream::from_bytes(&body);
    let family: prometheus::proto::MetricFamily = input.read_message().unwrap();
    assert_eq!(family.name(), "process_start_time_seconds");
    let (headers, compressed) = scrape("text/plain", "gzip").await;
    assert_eq!(headers["content-encoding"], "gzip");
    assert_eq!(&compressed[..3], &[0x1f, 0x8b, 8]);
    let end = compressed.len() - 8;
    let decoded = miniz_oxide::inflate::decompress_to_vec(&compressed[10..end]).unwrap();
    assert_eq!(decoded, plain);
    assert_eq!(
        u32::from_le_bytes(compressed[end..end + 4].try_into().unwrap()),
        crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(&decoded)
    );
    assert_eq!(
        u32::from_le_bytes(compressed[end + 4..].try_into().unwrap()),
        decoded.len() as u32
    );
    // NegotiateContentEncoding errors for q=0 are caught and fall back to
    // identity in promhttp/http.go:201-210, rather than making the scrape fail.
    for encoding in [
        "gzip;q=0",
        "identity;q=0,gzip;q=0",
        "identity;q=1,gzip;q=1",
        "br",
    ] {
        let (headers, body) = scrape("text/plain", encoding).await;
        assert!(!headers.contains_key("content-encoding"), "{encoding}");
        assert_eq!(body, plain);
    }
}

// gddo/httputil/header/header.go:64-67 reads every Accept-Encoding header.
#[tokio::test]
async fn repeated_encoding_headers_and_invalid_parameters() {
    let response = metrics_router("iptables")
        .oneshot(
            Request::builder()
                .uri("/metrics/slis")
                .header("accept-encoding", "br")
                .header("accept-encoding", "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-encoding")
            .map(|h| h.to_str().unwrap()),
        Some("gzip")
    );
    for encoding in ["gzip;level=2", "br;q=bad,gzip"] {
        let (headers, _) = scrape("text/plain", encoding).await;
        assert!(!headers.contains_key("content-encoding"), "{encoding}");
    }
}
