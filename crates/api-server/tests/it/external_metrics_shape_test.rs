//! #3390: external.metrics wire shape.
//!
//! Upstream: `k8s.io/metrics/pkg/apis/external_metrics/v1beta1/types.go`
//! (release-1.35): `MetricLabels map[string]string json:"metricLabels"` (no
//! omitempty, so a nil map is `null`) and `WindowSeconds *int64
//! json:"window,omitempty"` (an integer, not a duration string).
//! kubernetes-sigs/custom-metrics-apiserver @
//! ed62571de2338833370b3cf22e6ad91e323186c4
//! `test-adapter/provider/provider.go:312-327` returns one value per matching
//! series, each carrying that series' labels and no window.

use rusternetes_test_support::harness::TestApiServer;

const BASE: &str = "/apis/external.metrics.k8s.io/v1beta1/namespaces/default/qps";

#[tokio::test]
async fn external_value_has_metric_labels_key_and_no_string_window() {
    let api = TestApiServer::new();
    let (s, b) = api.get(BASE).await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(b["kind"], "ExternalMetricValueList", "{b}");
    let items = b["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "{b}");
    let item = &items[0];
    assert_eq!(item["metricName"], "qps", "{b}");
    // `metricLabels` has no omitempty: present even when the series has none.
    assert!(
        item.as_object().unwrap().contains_key("metricLabels"),
        "{b}"
    );
    // `window` is `*int64`: absent, or a number, never "60s".
    assert!(
        item.get("window").is_none_or(|w| w.is_i64()),
        "window must be an integer when present: {b}"
    );
}
