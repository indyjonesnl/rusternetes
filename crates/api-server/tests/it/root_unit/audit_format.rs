//! Tests for `src/audit_format.rs`: `EventString` (`pkg/audit/format.go`) and
//! the `HandlePluginError` message (`pkg/audit/metrics.go`). Upstream has no
//! unit test for either; these pin the format string.

use chrono::{TimeZone, Utc};
use rusternetes_api_server::audit_format::*;
use rusternetes_common::audit::{
    AuditEvent, AuditLevel, AuditStage, ObjectReference, ResponseStatus, UserInfo,
};

fn user(name: &str, groups: &[&str]) -> UserInfo {
    UserInfo {
        username: name.into(),
        uid: String::new(),
        groups: groups.iter().map(|g| g.to_string()).collect(),
        extra: None,
    }
}

fn event() -> AuditEvent {
    AuditEvent {
        api_version: "audit.k8s.io/v1".into(),
        kind: "Event".into(),
        level: AuditLevel::Metadata,
        impersonated_user: None,
        audit_id: "abc".into(),
        stage: AuditStage::ResponseComplete,
        request_uri: "/api/v1/pods".into(),
        verb: "list".into(),
        user: user("alice", &["g1", "g2"]),
        user_agent: Some("kubectl/1".into()),
        source_ips: vec!["10.0.0.1".into(), "10.0.0.2".into()],
        object_ref: Some(ObjectReference {
            namespace: Some("ns".into()),
            ..Default::default()
        }),
        response_status: Some(ResponseStatus {
            code: 200,
            ..Default::default()
        }),
        response_object: None,
        request_object: None,
        request_received_timestamp: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap()
            + chrono::Duration::nanoseconds(120_000_000),
        stage_timestamp: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 6).unwrap(),
        annotations: None,
    }
}

#[test]
fn full_event() {
    assert_eq!(
        event_string(&event()),
        "2026-01-02T03:04:05.12Z AUDIT: id=\"abc\" stage=\"ResponseComplete\" ip=\"10.0.0.1\" \
         method=\"list\" user=\"alice\" groups=\"\\\"g1\\\",\\\"g2\\\"\" as=\"<self>\" \
         asgroups=\"<lookup>\" user-agent=\"kubectl/1\" namespace=\"ns\" \
         uri=\"/api/v1/pods\" response=\"200\""
    );
}

#[test]
fn defaults_for_absent_fields() {
    let mut ev = event();
    ev.user = user("", &["ignored"]);
    ev.source_ips.clear();
    ev.object_ref = None;
    ev.response_status = None;
    ev.user_agent = None;
    ev.request_received_timestamp = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    assert_eq!(
        event_string(&ev),
        "2026-01-02T03:04:05Z AUDIT: id=\"abc\" stage=\"ResponseComplete\" ip=\"<unknown>\" \
         method=\"list\" user=\"<none>\" groups=\"<none>\" as=\"<self>\" asgroups=\"<lookup>\" \
         user-agent=\"\" namespace=\"<none>\" uri=\"/api/v1/pods\" response=\"<deferred>\""
    );
}

#[test]
fn impersonated_user_and_groups() {
    let mut ev = event();
    ev.impersonated_user = Some(user("bob", &["x"]));
    let s = event_string(&ev);
    assert!(s.contains("as=\"bob\" asgroups=\"\\\"x\\\"\""), "{s}");
}

#[test]
fn plugin_error_message_lists_every_impacted_event() {
    let a = event();
    let mut b = event();
    b.audit_id = "def".into();
    let msg = plugin_error_message("p", "boom", &[&a, &b]);
    assert_eq!(
        msg,
        format!(
            "Error in audit plugin 'p' affecting 2 audit events: boom\nImpacted events:\n{}\n{}\n",
            event_string(&a),
            event_string(&b)
        )
    );
}
