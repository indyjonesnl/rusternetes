//! One-line text form of an audit event.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/audit/format.go`
//! (`EventString`, `auditStringSlice`) and the message built by
//! `HandlePluginError` (`pkg/audit/metrics.go`).

use chrono::{DateTime, SecondsFormat, Utc};
use rusternetes_common::audit::AuditEvent;
use rusternetes_common::authz::reasons::quoted;

/// `time.RFC3339Nano`: UTC, trailing fractional zeros trimmed.
fn rfc3339_nano(t: &DateTime<Utc>) -> String {
    let s = t.to_rfc3339_opts(SecondsFormat::Nanos, true);
    match s.split_once('.') {
        Some((head, frac)) => {
            let digits = frac.trim_end_matches('Z').trim_end_matches('0');
            if digits.is_empty() {
                format!("{head}Z")
            } else {
                format!("{head}.{digits}Z")
            }
        }
        None => s,
    }
}

/// `auditStringSlice`: each element `%q`-quoted, joined by `,`.
fn audit_string_slice(list: &[String]) -> String {
    list.iter().map(|s| quoted(s)).collect::<Vec<_>>().join(",")
}

/// `EventString`: a 1-line text representation of an audit event, using a
/// subset of its fields.
///
/// Deviation: Go distinguishes a nil `ImpersonatedUser.Groups` (`<lookup>`)
/// from an empty one (empty string); our `UserInfo.groups` is a plain `Vec`,
/// so an empty list prints `<lookup>`.
pub fn event_string(ev: &AuditEvent) -> String {
    let mut username = "<none>".to_string();
    let mut groups = "<none>".to_string();
    if !ev.user.username.is_empty() {
        username = ev.user.username.clone();
        if !ev.user.groups.is_empty() {
            groups = audit_string_slice(&ev.user.groups);
        }
    }
    let mut asuser = "<self>".to_string();
    let mut asgroups = "<lookup>".to_string();
    if let Some(imp) = &ev.impersonated_user {
        asuser = imp.username.clone();
        if !imp.groups.is_empty() {
            asgroups = audit_string_slice(&imp.groups);
        }
    }
    let namespace = ev
        .object_ref
        .as_ref()
        .and_then(|r| r.namespace.as_deref())
        .filter(|n| !n.is_empty())
        .unwrap_or("<none>");
    let response = ev
        .response_status
        .as_ref()
        .map(|s| s.code.to_string())
        .unwrap_or_else(|| "<deferred>".to_string());
    let ip = ev
        .source_ips
        .first()
        .map(String::as_str)
        .unwrap_or("<unknown>");
    let stage = format!("{:?}", ev.stage);
    format!(
        "{} AUDIT: id={} stage={} ip={} method={} user={} groups={} as={} asgroups={} user-agent={} namespace={} uri={} response=\"{}\"",
        rfc3339_nano(&ev.request_received_timestamp),
        quoted(&ev.audit_id),
        quoted(&stage),
        quoted(ip),
        quoted(&ev.verb),
        quoted(&username),
        quoted(&groups),
        quoted(&asuser),
        quoted(&asgroups),
        quoted(ev.user_agent.as_deref().unwrap_or("")),
        quoted(namespace),
        quoted(&ev.request_uri),
        response,
    )
}

/// The message `HandlePluginError` (`pkg/audit/metrics.go`) logs at error
/// level: the plugin, the number of impacted events and the error, then one
/// `EventString` line per impacted event.
pub fn plugin_error_message(plugin: &str, err: &str, impacted: &[&AuditEvent]) -> String {
    let mut msg = format!(
        "Error in audit plugin '{plugin}' affecting {} audit events: {err}\nImpacted events:\n",
        impacted.len()
    );
    for ev in impacted {
        msg.push_str(&event_string(ev));
        msg.push('\n');
    }
    msg
}
