//! Tests for `src/audit_metrics.rs`: `ObserveEvent`, `ObservePolicyLevel`,
//! `HandlePluginError`'s counter and `ApiserverAuditDroppedCounter`
//! (`staging/src/k8s.io/apiserver/pkg/audit/metrics.go`). Values are read as
//! deltas since the counters are process-global.

use axum::{body::Body, routing::get, Router};
use rusternetes_api_server::audit::{with_audit, AuditConfig, Policy};
use rusternetes_api_server::audit_metrics::*;
use rusternetes_common::audit::{AuditBackend, AuditEvent, AuditLevel};
use std::sync::Arc;
use tower::ServiceExt;

fn value(name: &str, label: Option<(&str, &str)>) -> f64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.name() == name)
        .flat_map(|f| f.get_metric().iter())
        .filter(|m| {
            label.is_none_or(|(k, v)| {
                m.get_label()
                    .iter()
                    .any(|l| l.name() == k && l.value() == v)
            })
        })
        .map(|m| m.get_counter().value())
        .sum()
}

struct Sink(bool);

#[async_trait::async_trait]
impl AuditBackend for Sink {
    async fn log(&self, _: AuditEvent) -> Result<(), String> {
        if self.0 {
            Ok(())
        } else {
            Err("down".into())
        }
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

async fn serve(level: &str, ok_sink: bool) -> u16 {
    let cfg = Arc::new(AuditConfig {
        policy: Policy::from_yaml(&format!(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n- level: {level}\n"
        ))
        .unwrap(),
        sink: Arc::new(Sink(ok_sink)),
    });
    Router::new()
        .route("/api/v1/namespaces/ns/secrets/s", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(move |req, next| {
            let cfg = cfg.clone();
            async move { with_audit(cfg, req, next).await }
        }))
        .oneshot(
            axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[test]
fn plugin_error_adds_impacted_count_per_plugin() {
    let before = value("apiserver_audit_error_total", Some(("plugin", "t-plugin")));
    count_plugin_error("t-plugin", 3);
    count_plugin_error("t-plugin", 2);
    let after = value("apiserver_audit_error_total", Some(("plugin", "t-plugin")));
    assert_eq!(after - before, 5.0);
}

#[test]
fn level_label_is_the_level_string() {
    for (l, s) in [
        (AuditLevel::None, "None"),
        (AuditLevel::Metadata, "Metadata"),
        (AuditLevel::Request, "Request"),
        (AuditLevel::RequestResponse, "RequestResponse"),
    ] {
        let before = value("apiserver_audit_level_total", Some(("level", s)));
        observe_policy_level(&l);
        let after = value("apiserver_audit_level_total", Some(("level", s)));
        assert_eq!(after - before, 1.0, "{s}");
    }
}

#[tokio::test]
async fn filter_counts_level_and_events() {
    let lvl = value("apiserver_audit_level_total", Some(("level", "Metadata")));
    let ev = value("apiserver_audit_event_total", None);
    assert_eq!(serve("Metadata", true).await, 200);
    assert_eq!(
        value("apiserver_audit_level_total", Some(("level", "Metadata"))) - lvl,
        1.0
    );
    // RequestReceived and ResponseComplete.
    assert_eq!(value("apiserver_audit_event_total", None) - ev, 2.0);
}

#[tokio::test]
async fn level_none_is_observed_without_events() {
    let lvl = value("apiserver_audit_level_total", Some(("level", "None")));
    let ev = value("apiserver_audit_event_total", None);
    assert_eq!(serve("None", true).await, 200);
    assert_eq!(
        value("apiserver_audit_level_total", Some(("level", "None"))) - lvl,
        1.0
    );
    assert_eq!(value("apiserver_audit_event_total", None) - ev, 0.0);
}

#[tokio::test]
async fn sink_failure_rejects_request_and_counts_it() {
    let before = value("apiserver_audit_requests_rejected_total", None);
    assert_eq!(serve("Metadata", false).await, 500);
    assert_eq!(
        value("apiserver_audit_requests_rejected_total", None) - before,
        1.0
    );
}
