//! The audit pipeline: the policy (`--audit-policy-file`), the per-request
//! audit context, and the `WithAudit` filter that emits the events.
//!
//! Ported from `staging/src/k8s.io/apiserver` (release-1.35):
//! - `pkg/audit/policy/checker.go` (`EvaluatePolicyRule`, `ruleMatches*`),
//!   `pkg/audit/policy/reader.go` (`LoadPolicyFromBytes`),
//!   `pkg/apis/audit/validation/validation.go` (`ValidatePolicy`);
//! - `pkg/audit/context.go` (`AuditContext`, `ProcessEventStage`,
//!   `AddAuditAnnotations`), `pkg/audit/request.go` (`LogRequestMetadata`);
//! - `pkg/endpoints/filters/audit.go` (`WithAudit`);
//! - `pkg/endpoints/request/requestinfo.go` (`NewRequestInfo`).
//!
//! Not yet ported (tracked in the PR): Request / RequestResponse bodies,
//! impersonated users, the ResponseStarted -> ResponseComplete split of a
//! long-running request, the Panic stage, the webhook backend flags, log
//! rotation and the `legacy` format.

use axum::{
    extract::Request,
    http::{HeaderValue, Method},
    middleware::Next,
    response::Response,
};
use chrono::Utc;
use rusternetes_common::audit::{
    AuditBackend, AuditEvent, AuditLevel, AuditStage, ObjectReference, ResponseStatus, UserInfo,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::{error, warn};

/// `policy.DefaultAuditLevel` (checker.go:29): the level when no rule matches.
pub const DEFAULT_AUDIT_LEVEL: AuditLevel = AuditLevel::None;

/// `auditinternal.Level.Less` (apis/audit/helpers.go:32).
fn level_ord(l: &AuditLevel) -> u8 {
    match l {
        AuditLevel::None => 0,
        AuditLevel::Metadata => 1,
        AuditLevel::Request => 2,
        AuditLevel::RequestResponse => 3,
    }
}

// ---------------------------------------------------------------------
// Policy (audit.k8s.io/v1 Policy)
// ---------------------------------------------------------------------

/// `audit.GroupResources` (apis/audit/v1/types.go).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupResources {
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub resources: Vec<String>,
    #[serde(default)]
    pub resource_names: Vec<String>,
}

/// `audit.PolicyRule`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRule {
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub user_groups: Vec<String>,
    #[serde(default)]
    pub verbs: Vec<String>,
    #[serde(default)]
    pub resources: Vec<GroupResources>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default, rename = "nonResourceURLs")]
    pub non_resource_urls: Vec<String>,
    #[serde(default)]
    pub omit_stages: Vec<String>,
    #[serde(default)]
    pub omit_managed_fields: Option<bool>,
}

/// `audit.Policy`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub rules: Vec<PolicyRule>,
    #[serde(default)]
    pub omit_stages: Vec<String>,
    #[serde(default)]
    pub omit_managed_fields: bool,
}

/// `auditinternal.RequestAuditConfig`: what the policy decided for one request.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestAuditConfig {
    pub level: AuditLevel,
    pub omit_stages: Vec<String>,
    pub omit_managed_fields: bool,
}

fn parse_level(s: &str) -> Option<AuditLevel> {
    match s {
        "None" => Some(AuditLevel::None),
        "Metadata" => Some(AuditLevel::Metadata),
        "Request" => Some(AuditLevel::Request),
        "RequestResponse" => Some(AuditLevel::RequestResponse),
        _ => None,
    }
}

fn stage_name(s: &AuditStage) -> &'static str {
    match s {
        AuditStage::RequestReceived => "RequestReceived",
        AuditStage::ResponseStarted => "ResponseStarted",
        AuditStage::ResponseComplete => "ResponseComplete",
        AuditStage::Panic => "Panic",
    }
}

const VALID_OMIT_STAGES: [&str; 4] = [
    "RequestReceived",
    "ResponseStarted",
    "ResponseComplete",
    "Panic",
];

fn validate_omit_stages(stages: &[String], path: &str, errs: &mut Vec<String>) {
    // validateOmitStages (validation.go).
    for (i, s) in stages.iter().enumerate() {
        if !VALID_OMIT_STAGES.contains(&s.as_str()) {
            errs.push(format!(
                "{path}[{i}]: Invalid value: \"{s}\": allowed stages are {}",
                VALID_OMIT_STAGES.join(",")
            ));
        }
    }
}

impl Policy {
    /// `LoadPolicyFromBytes` (reader.go): decode, check the group-version,
    /// `ValidatePolicy`, and refuse a policy with no rules. Decoding is the
    /// lenient path (unknown fields ignored); upstream warns and falls back
    /// to it on a strict-decode error.
    pub fn from_yaml(yaml: &str) -> Result<Policy, String> {
        let mut policy: Policy =
            serde_yaml::from_str(yaml).map_err(|e| format!("failed decoding: {e}"))?;
        if policy.api_version != "audit.k8s.io/v1" || policy.kind != "Policy" {
            return Err(format!(
                "unknown group version field {}, Kind={} in policy",
                policy.api_version, policy.kind
            ));
        }
        policy.validate()?;
        if policy.rules.is_empty() {
            return Err("loaded illegal policy with 0 rules".to_string());
        }
        // NewPolicyRuleEvaluator (checker.go:33-38): each rule's OmitStages
        // becomes the union with the policy's.
        for rule in &mut policy.rules {
            let mut union: Vec<String> = Vec::new();
            for s in policy.omit_stages.iter().chain(rule.omit_stages.iter()) {
                if !union.contains(s) {
                    union.push(s.clone());
                }
            }
            rule.omit_stages = union;
        }
        Ok(policy)
    }

    /// `ValidatePolicy` / `validatePolicyRule` (validation.go).
    fn validate(&self) -> Result<(), String> {
        let mut errs = Vec::new();
        validate_omit_stages(&self.omit_stages, "omitStages", &mut errs);
        for (i, rule) in self.rules.iter().enumerate() {
            let p = format!("rules[{i}]");
            match rule.level.as_deref() {
                None | Some("") => errs.push(format!("{p}.level: Required value")),
                Some(l) if parse_level(l).is_none() => errs.push(format!(
                    "{p}.level: Unsupported value: \"{l}\": supported values: \"None\", \"Metadata\", \"Request\", \"RequestResponse\""
                )),
                _ => {}
            }
            for (j, url) in rule.non_resource_urls.iter().enumerate() {
                if url == "*" {
                    continue;
                }
                if !url.starts_with('/') {
                    errs.push(format!("{p}.nonResourceURLs[{j}]: Invalid value: \"{url}\": non-resource URL rules must begin with a '/' character"));
                }
                if !url.is_empty() && url[..url.len() - 1].contains('*') {
                    errs.push(format!("{p}.nonResourceURLs[{j}]: Invalid value: \"{url}\": non-resource URL wildcards '*' must be the final character of the rule"));
                }
            }
            for gr in &rule.resources {
                if !gr.resource_names.is_empty() && gr.resources.is_empty() {
                    errs.push(format!("{p}.resources.resourceNames: Invalid value: {:?}: using resourceNames requires at least one resource", gr.resource_names));
                }
            }
            validate_omit_stages(&rule.omit_stages, &format!("{p}.omitStages"), &mut errs);
            if !rule.non_resource_urls.is_empty()
                && (!rule.resources.is_empty() || !rule.namespaces.is_empty())
            {
                errs.push(format!("{p}.nonResourceURLs: Invalid value: {:?}: rules cannot apply to both regular resources and non-resource URLs", rule.non_resource_urls));
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(format!("[{}]", errs.join(", ")))
        }
    }

    /// `policyRuleEvaluator.EvaluatePolicyRule` (checker.go:64-84): the FIRST
    /// matching rule wins; none matching gives [`DEFAULT_AUDIT_LEVEL`].
    pub fn evaluate(&self, attrs: &Attributes) -> RequestAuditConfig {
        for rule in &self.rules {
            if rule_matches(rule, attrs) {
                return RequestAuditConfig {
                    level: rule
                        .level
                        .as_deref()
                        .and_then(parse_level)
                        .unwrap_or(DEFAULT_AUDIT_LEVEL),
                    omit_stages: rule.omit_stages.clone(),
                    // isOmitManagedFields (checker.go:86-92)
                    omit_managed_fields: rule
                        .omit_managed_fields
                        .unwrap_or(self.omit_managed_fields),
                };
            }
        }
        RequestAuditConfig {
            level: DEFAULT_AUDIT_LEVEL,
            omit_stages: self.omit_stages.clone(),
            omit_managed_fields: self.omit_managed_fields,
        }
    }
}

/// The slice of `authorizer.Attributes` a policy rule reads.
#[derive(Debug, Clone, Default)]
pub struct Attributes {
    pub user: Option<UserInfo>,
    pub verb: String,
    pub is_resource_request: bool,
    pub path: String,
    pub namespace: String,
    pub api_group: String,
    pub api_version: String,
    pub resource: String,
    pub subresource: String,
    pub name: String,
}

/// `ruleMatches` (checker.go:95-133).
fn rule_matches(r: &PolicyRule, attrs: &Attributes) -> bool {
    if !r.users.is_empty() {
        match &attrs.user {
            Some(u) if r.users.contains(&u.username) => {}
            _ => return false,
        }
    }
    if !r.user_groups.is_empty() {
        let Some(u) = &attrs.user else { return false };
        if !u.groups.iter().any(|g| r.user_groups.contains(g)) {
            return false;
        }
    }
    if !r.verbs.is_empty() && !r.verbs.contains(&attrs.verb) {
        return false;
    }
    if !r.namespaces.is_empty() || !r.resources.is_empty() {
        return rule_matches_resource(r, attrs);
    }
    if !r.non_resource_urls.is_empty() {
        return rule_matches_non_resource(r, attrs);
    }
    true
}

/// `ruleMatchesNonResource` (checker.go:135-149).
fn rule_matches_non_resource(r: &PolicyRule, attrs: &Attributes) -> bool {
    if attrs.is_resource_request {
        return false;
    }
    r.non_resource_urls
        .iter()
        .any(|spec| path_matches(&attrs.path, spec))
}

/// `pathMatches` (checker.go:151-166).
fn path_matches(path: &str, spec: &str) -> bool {
    if spec == "*" || spec == path {
        return true;
    }
    spec.ends_with('*') && path.starts_with(spec.trim_end_matches('*'))
}

/// `ruleMatchesResource` (checker.go:168-218).
fn rule_matches_resource(r: &PolicyRule, attrs: &Attributes) -> bool {
    if !attrs.is_resource_request {
        return false;
    }
    if !r.namespaces.is_empty() && !r.namespaces.contains(&attrs.namespace) {
        return false;
    }
    if r.resources.is_empty() {
        return true;
    }
    let combined = if attrs.subresource.is_empty() {
        attrs.resource.clone()
    } else {
        format!("{}/{}", attrs.resource, attrs.subresource)
    };
    for gr in &r.resources {
        if gr.group != attrs.api_group {
            continue;
        }
        if gr.resources.is_empty() {
            return true;
        }
        for res in &gr.resources {
            if gr.resource_names.is_empty() || gr.resource_names.contains(&attrs.name) {
                if *res == combined || res == "*" {
                    return true;
                }
                if !attrs.subresource.is_empty()
                    && res.starts_with("*/")
                    && attrs.subresource == res["*/".len()..]
                {
                    return true;
                }
                if let Some(prefix) = res.strip_suffix("/*") {
                    if attrs.resource == prefix {
                        return true;
                    }
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------
// RequestInfo
// ---------------------------------------------------------------------

/// `NewRequestInfo` (requestinfo.go:130-263), the part audit needs. APIPrefixes
/// are `api` and `apis`; `api` is groupless. The selector-bearing parts and the
/// `?watch=` query handling that decides list vs watch are included.
pub fn request_info(method: &Method, path: &str, query: Option<&str>) -> Attributes {
    let mut a = Attributes {
        verb: method.as_str().to_lowercase(),
        path: path.to_string(),
        ..Default::default()
    };
    let all: Vec<&str> = path.trim_matches('/').split('/').collect();
    let mut parts: &[&str] = if path.trim_matches('/').is_empty() {
        &[]
    } else {
        &all
    };
    if parts.len() < 3 {
        return a;
    }
    let prefix = parts[0];
    if prefix != "api" && prefix != "apis" {
        return a;
    }
    parts = &parts[1..];
    if prefix != "api" {
        if parts.len() < 3 {
            return a;
        }
        a.api_group = parts[0].to_string();
        parts = &parts[1..];
    }
    a.is_resource_request = true;
    a.api_version = parts[0].to_string();
    parts = &parts[1..];

    // specialVerbs = {"proxy", "watch"}; "proxy" allows no subresources.
    let mut no_subresources = false;
    if parts[0] == "proxy" || parts[0] == "watch" {
        if parts.len() < 2 {
            a.is_resource_request = false;
            return a;
        }
        a.verb = parts[0].to_string();
        no_subresources = parts[0] == "proxy";
        parts = &parts[1..];
    } else {
        a.verb = match *method {
            Method::POST => "create",
            Method::GET | Method::HEAD => "get",
            Method::PUT => "update",
            Method::PATCH => "patch",
            Method::DELETE => "delete",
            _ => "",
        }
        .to_string();
    }
    if parts[0] == "namespaces" && parts.len() > 1 {
        a.namespace = parts[1].to_string();
        // namespaceSubresources = {"status", "finalize"}
        if parts.len() > 2 && parts[2] != "status" && parts[2] != "finalize" {
            parts = &parts[2..];
        }
    }
    if parts.len() >= 3 && !no_subresources {
        a.subresource = parts[2].to_string();
    }
    if parts.len() >= 2 {
        a.name = parts[1].to_string();
    }
    if !parts.is_empty() {
        a.resource = parts[0].to_string();
    }
    if a.name.is_empty() && a.verb == "get" {
        let watch = query
            .into_iter()
            .flat_map(|q| q.split('&'))
            .filter_map(|kv| kv.split_once('='))
            .any(|(k, v)| k == "watch" && !matches!(v.to_lowercase().as_str(), "false" | "0"));
        a.verb = if watch { "watch" } else { "list" }.to_string();
    }
    if a.name.is_empty() && a.verb == "delete" {
        a.verb = "deletecollection".to_string();
    }
    a
}

// ---------------------------------------------------------------------
// AuditContext
// ---------------------------------------------------------------------

/// `audit.AuditContext` (context.go): the event of one request, shared by the
/// filter and everything the request passes through (admission adds
/// annotations to it).
pub struct AuditContext {
    inner: Mutex<AuditInner>,
    sink: Arc<dyn AuditBackend>,
}

struct AuditInner {
    event: AuditEvent,
    config: RequestAuditConfig,
}

tokio::task_local! {
    /// `audit.WithAuditContext` / `AuditContextFrom`: the context of the
    /// request being served.
    static AUDIT_CONTEXT: Arc<AuditContext>;
}

impl AuditContext {
    /// `ProcessEventStage` (context.go:107-133): an omitted stage is dropped;
    /// RequestReceived carries the received time as its stage time.
    pub async fn process_event_stage(&self, stage: AuditStage) -> bool {
        let event = {
            let Ok(mut g) = self.inner.lock() else {
                return false;
            };
            if g.config.omit_stages.iter().any(|o| o == stage_name(&stage)) {
                return true;
            }
            g.event.stage_timestamp = if stage == AuditStage::RequestReceived {
                g.event.request_received_timestamp
            } else {
                Utc::now()
            };
            g.event.stage = stage;
            g.event.clone()
        };
        match self.sink.log(event).await {
            Ok(()) => true,
            Err(e) => {
                error!("failed to process audit event: {e}");
                false
            }
        }
    }

    /// `AddAuditAnnotations` (context.go:312-327).
    fn add_annotations(&self, kvs: &[(String, String)]) {
        if let Ok(mut g) = self.inner.lock() {
            for (k, v) in kvs {
                add_annotation_locked(&mut g.event, k, v);
            }
        }
    }

    fn set_response_status(&self, code: u16) {
        if let Ok(mut g) = self.inner.lock() {
            g.event.response_status = Some(ResponseStatus {
                code,
                message: None,
                ..Default::default()
            });
        }
    }

    /// `GetEventLevel`.
    pub fn level(&self) -> AuditLevel {
        self.inner
            .lock()
            .map(|g| g.event.level.clone())
            .unwrap_or(AuditLevel::None)
    }
}

/// `addAuditAnnotationLocked` (context.go:346-356): the first value for a key
/// wins; a different later value is dropped with a warning.
fn add_annotation_locked(ev: &mut AuditEvent, key: &str, value: &str) {
    let ann = ev.annotations.get_or_insert_with(Default::default);
    if let Some(old) = ann.get(key) {
        if old != value {
            warn!(
                "Failed to set annotations[{key:?}] to {value:?} for audit:{:?}, it has already been set to {old:?}",
                ev.audit_id
            );
            return;
        }
    }
    ann.insert(key.to_string(), value.to_string());
}

/// `audit.AddAuditAnnotations(ctx, kv...)` (context.go:312): a no-op when the
/// request is not audited (`!ac.Enabled()`).
pub fn add_audit_annotations(kvs: &[(String, String)]) {
    let _ = AUDIT_CONTEXT.try_with(|ac| {
        if level_ord(&ac.level()) > 0 {
            ac.add_annotations(kvs)
        }
    });
}

/// `audit.AddAuditAnnotation(ctx, key, value)` (context.go:297).
#[allow(dead_code)]
pub fn add_audit_annotation(key: &str, value: &str) {
    add_audit_annotations(&[(key.to_string(), value.to_string())]);
}

// ---------------------------------------------------------------------
// WithAudit
// ---------------------------------------------------------------------

/// The configured audit pipeline: policy + sink (`WithAudit`'s `policy` and
/// `sink` arguments).
pub struct AuditConfig {
    pub policy: Policy,
    pub sink: Arc<dyn AuditBackend>,
}

static AUDIT: OnceLock<Arc<AuditConfig>> = OnceLock::new();

/// Install the process-wide audit pipeline, built once at startup from
/// `--audit-policy-file` and `--audit-log-path`.
pub fn install_audit(config: AuditConfig) {
    let _ = AUDIT.set(Arc::new(config));
}

/// The filter wired into the router: [`with_audit`] against the installed
/// pipeline. `WithAudit` returns the handler undecorated when the sink or
/// policy is nil (filters/audit.go:42-44); so does this.
pub async fn audit_middleware(req: Request, next: Next) -> Response {
    match AUDIT.get() {
        Some(cfg) => with_audit(cfg.clone(), req, next).await,
        None => next.run(req).await,
    }
}

/// `WithAudit` (filters/audit.go:41-117) +
/// `evaluatePolicyAndCreateAuditEvent` (:119-) + `audit.LogRequestMetadata`
/// (request.go:43-82).
pub async fn with_audit(cfg: Arc<AuditConfig>, req: Request, next: Next) -> Response {
    let user = req
        .extensions()
        .get::<rusternetes_middleware::AuthContext>()
        .map(|c| c.user.clone())
        .unwrap_or_else(rusternetes_common::auth::UserInfo::anonymous);
    let mut attrs = request_info(req.method(), req.uri().path(), req.uri().query());
    let user_info = UserInfo {
        username: user.username.clone(),
        uid: user.uid.clone(),
        groups: user.groups.clone(),
        extra: if user.extra.is_empty() {
            None
        } else {
            Some(user.extra.clone())
        },
    };
    attrs.user = Some(user_info.clone());

    let rac = cfg.policy.evaluate(&attrs);
    if rac.level == AuditLevel::None {
        // "Don't audit."
        return next.run(req).await;
    }

    let received = Utc::now();
    let audit_id = req
        .headers()
        .get("Audit-ID")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let event = AuditEvent {
        api_version: "audit.k8s.io/v1".to_string(),
        kind: "Event".to_string(),
        level: rac.level.clone(),
        audit_id: audit_id.clone(),
        stage: AuditStage::RequestReceived,
        request_uri: req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_default(),
        verb: attrs.verb.clone(),
        user: user_info,
        user_agent: req
            .headers()
            .get("User-Agent")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        source_ips: req
            .headers()
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default(),
        object_ref: attrs.is_resource_request.then(|| ObjectReference {
            resource: Some(attrs.resource.clone()),
            subresource: Some(attrs.subresource.clone()).filter(|s| !s.is_empty()),
            api_group: Some(attrs.api_group.clone()).filter(|s| !s.is_empty()),
            namespace: Some(attrs.namespace.clone()).filter(|s| !s.is_empty()),
            name: Some(attrs.name.clone()).filter(|s| !s.is_empty()),
            uid: None,
            api_version: Some(attrs.api_version.clone()),
            resource_version: None,
        }),
        response_status: None,
        request_received_timestamp: received,
        stage_timestamp: received,
        annotations: None,
    };
    let ac = Arc::new(AuditContext {
        inner: Mutex::new(AuditInner { event, config: rac }),
        sink: cfg.sink.clone(),
    });

    // Annotations authentication recorded before this filter existed
    // (e.g. `authentication.k8s.io/legacy-token*`, legacy.go:143-170).
    if let Some(a) = req
        .extensions()
        .get::<rusternetes_middleware::AuthAuditAnnotations>()
    {
        ac.add_annotations(&a.0);
    }

    if !ac.process_event_stage(AuditStage::RequestReceived).await {
        // "failed to store audit event" (audit.go:~62)
        return internal_error("failed to store audit event");
    }

    let long_running = attrs.verb == "watch" || attrs.verb == "proxy";
    let mut resp = AUDIT_CONTEXT.scope(ac.clone(), next.run(req)).await;
    if let Ok(v) = HeaderValue::from_str(&audit_id) {
        resp.headers_mut().insert("Audit-ID", v);
    }
    ac.set_response_status(resp.status().as_u16());
    if !resp.status().is_success() && !resp.status().is_informational() {
        resp = record_status_body(&ac, resp).await;
    }
    if long_running {
        ac.process_event_stage(AuditStage::ResponseStarted).await;
    }
    ac.process_event_stage(AuditStage::ResponseComplete).await;
    resp
}

/// Largest error body buffered to recover the Status (error responses are
/// small; a streamed body is never an error Status).
const MAX_STATUS_BODY: usize = 1 << 20;

/// `audit.LogResponseObject` (audit/request.go): at Metadata level and above
/// the `metav1.Status` the handler wrote becomes the event's `ResponseStatus`
/// (`ac.LogResponseObject` -> `ev.ResponseStatus = status`). Upstream hooks the
/// serializer; here the (non-2xx) body is read back and passed through
/// unchanged. Deviation: a 2xx Status (e.g. a delete's Success) is not
/// recorded, because buffering success bodies would break streaming.
async fn record_status_body(ac: &AuditContext, resp: Response) -> Response {
    let (parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, MAX_STATUS_BODY)
        .await
        .unwrap_or_default();
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        if v.get("kind").and_then(|k| k.as_str()) == Some("Status") {
            let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
            if let Ok(mut g) = ac.inner.lock() {
                g.event.response_status = Some(ResponseStatus {
                    status: s("status"),
                    message: s("message"),
                    reason: s("reason"),
                    details: v.get("details").cloned(),
                    code: parts.status.as_u16(),
                    ..Default::default()
                });
            }
        }
    }
    Response::from_parts(parts, axum::body::Body::from(bytes))
}

fn internal_error(msg: &str) -> Response {
    let mut r = Response::new(axum::body::Body::from(msg.to_string()));
    *r.status_mut() = axum::http::StatusCode::INTERNAL_SERVER_ERROR;
    r
}

/// `--audit-log-path` `-`: events to stdout (options/audit.go: "-" means
/// stdout).
pub struct StdoutAuditBackend;

#[async_trait::async_trait]
impl AuditBackend for StdoutAuditBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        let json = serde_json::to_string(&event).map_err(|e| e.to_string())?;
        println!("{json}");
        Ok(())
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get, Router};
    use tower::ServiceExt;

    struct Capture(tokio::sync::Mutex<Vec<AuditEvent>>);
    #[async_trait::async_trait]
    impl AuditBackend for Capture {
        async fn log(&self, e: AuditEvent) -> Result<(), String> {
            self.0.lock().await.push(e);
            Ok(())
        }
        async fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    const POLICY: &str = r#"
apiVersion: audit.k8s.io/v1
kind: Policy
omitStages: ["RequestReceived"]
rules:
- level: None
  users: ["system:kube-proxy"]
- level: None
  nonResourceURLs: ["/healthz*", "/version"]
- level: Metadata
  resources:
  - group: ""
    resources: ["secrets", "pods/log"]
- level: RequestResponse
  verbs: ["create"]
  namespaces: ["prod"]
- level: Request
  resources:
  - group: apps
    resources: ["*/scale"]
- level: Metadata
  omitStages: ["ResponseStarted"]
  resources:
  - group: ""
    resources: ["configmaps"]
    resourceNames: ["audited"]
"#;

    fn attrs(method: Method, path: &str) -> Attributes {
        let mut a = request_info(&method, path, None);
        a.user = Some(UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec!["devs".into()],
            extra: None,
        });
        a
    }

    #[test]
    fn request_info_matches_upstream_shapes() {
        let a = request_info(&Method::GET, "/api/v1/namespaces/ns/pods/p/log", None);
        assert_eq!(
            (a.verb.as_str(), a.namespace.as_str(), a.resource.as_str()),
            ("get", "ns", "pods")
        );
        assert_eq!((a.name.as_str(), a.subresource.as_str()), ("p", "log"));
        let a = request_info(&Method::GET, "/apis/apps/v1/deployments", None);
        assert_eq!((a.verb.as_str(), a.api_group.as_str()), ("list", "apps"));
        let a = request_info(&Method::GET, "/api/v1/pods", Some("watch=true"));
        assert_eq!(a.verb, "watch");
        let a = request_info(&Method::DELETE, "/api/v1/namespaces/ns/pods", None);
        assert_eq!(a.verb, "deletecollection");
        let a = request_info(&Method::GET, "/api/v1/namespaces/ns", None);
        assert_eq!((a.resource.as_str(), a.name.as_str()), ("namespaces", "ns"));
        let a = request_info(&Method::GET, "/healthz", None);
        assert!(!a.is_resource_request);
        assert_eq!(a.verb, "get");
    }

    #[test]
    fn policy_first_match_and_default() {
        let p = Policy::from_yaml(POLICY).unwrap();
        let lvl = |a: Attributes| p.evaluate(&a).level;
        // No rule matches: DefaultAuditLevel None.
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/services/s")),
            AuditLevel::None
        );
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/secrets/s")),
            AuditLevel::Metadata
        );
        // "pods/log" matches the subresource; "pods" alone does not.
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/pods/p/log")),
            AuditLevel::Metadata
        );
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/pods/p")),
            AuditLevel::None
        );
        // "*/scale"
        assert_eq!(
            lvl(attrs(
                Method::GET,
                "/apis/apps/v1/namespaces/ns/deployments/d/scale"
            )),
            AuditLevel::Request
        );
        // verbs + namespaces
        assert_eq!(
            lvl(attrs(Method::POST, "/api/v1/namespaces/prod/services")),
            AuditLevel::RequestResponse
        );
        assert_eq!(
            lvl(attrs(Method::POST, "/api/v1/namespaces/dev/services")),
            AuditLevel::None
        );
        // non-resource URLs, trailing * and exact
        assert_eq!(lvl(attrs(Method::GET, "/healthz/ping")), AuditLevel::None);
        // resourceNames
        assert_eq!(
            lvl(attrs(
                Method::GET,
                "/api/v1/namespaces/ns/configmaps/audited"
            )),
            AuditLevel::Metadata
        );
        assert_eq!(
            lvl(attrs(Method::GET, "/api/v1/namespaces/ns/configmaps/other")),
            AuditLevel::None
        );
    }

    #[test]
    fn policy_users_rule_and_omit_stage_union() {
        let p = Policy::from_yaml(POLICY).unwrap();
        let mut a = attrs(Method::GET, "/api/v1/namespaces/ns/secrets/s");
        a.user.as_mut().unwrap().username = "system:kube-proxy".into();
        assert_eq!(p.evaluate(&a).level, AuditLevel::None);
        // Policy.omitStages is unioned into the rule's (checker.go:33-38).
        let c = p.evaluate(&attrs(
            Method::GET,
            "/api/v1/namespaces/ns/configmaps/audited",
        ));
        let mut s = c.omit_stages.clone();
        s.sort();
        assert_eq!(s, vec!["RequestReceived", "ResponseStarted"]);
        // The default (no rule) config carries the policy's omitStages.
        let c = p.evaluate(&attrs(Method::GET, "/api/v1/namespaces/ns/services/s"));
        assert_eq!(c.omit_stages, vec!["RequestReceived"]);
    }

    #[test]
    fn policy_validation_matches_upstream() {
        let bad = |yaml: &str| Policy::from_yaml(yaml).unwrap_err();
        let hdr = "apiVersion: audit.k8s.io/v1\nkind: Policy\n";
        assert!(bad(&format!("{hdr}rules: []")).contains("0 rules"));
        assert!(bad(&format!("{hdr}rules:\n- verbs: [get]")).contains("rules[0].level: Required"));
        assert!(bad(&format!("{hdr}rules:\n- level: Bogus")).contains("Unsupported value"));
        assert!(bad(&format!(
            "{hdr}rules:\n- level: None\n  nonResourceURLs: [\"healthz\", \"/a*b\"]"
        ))
        .contains("must begin with a '/'"));
        assert!(bad(&format!(
            "{hdr}rules:\n- level: None\n  nonResourceURLs: [\"/a*b\"]"
        ))
        .contains("must be the final character"));
        assert!(bad(&format!(
            "{hdr}rules:\n- level: None\n  namespaces: [a]\n  nonResourceURLs: [\"/a\"]"
        ))
        .contains("cannot apply to both"));
        assert!(
            bad(&format!("{hdr}omitStages: [Nope]\nrules:\n- level: None"))
                .contains("allowed stages are")
        );
        assert!(bad("apiVersion: v1\nkind: Policy\nrules:\n- level: None")
            .contains("unknown group version"));
    }

    fn app(cfg: Arc<AuditConfig>) -> Router {
        Router::new()
            .route(
                "/api/v1/namespaces/:ns/secrets/:name",
                get(|| async {
                    add_audit_annotation("pod-security.kubernetes.io/enforce-policy", "x:latest");
                    "ok"
                }),
            )
            .route(
                "/api/v1/namespaces/:ns/services/:name",
                get(|| async {
                    add_audit_annotation("k.example/a", "b");
                    "ok"
                }),
            )
            .layer(axum::middleware::from_fn(move |req, next| {
                let cfg = cfg.clone();
                async move { with_audit(cfg, req, next).await }
            }))
    }

    #[tokio::test]
    async fn filter_emits_events_per_stage_with_annotations() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(&POLICY.replace("omitStages: [\"RequestReceived\"]\n", ""))
                .unwrap(),
            sink: cap.clone(),
        });
        let resp = app(cfg)
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                    .header("User-Agent", "kubectl/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let id = resp.headers()["Audit-ID"].to_str().unwrap().to_string();
        let ev = cap.0.lock().await;
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].stage, AuditStage::RequestReceived);
        assert_eq!(ev[0].stage_timestamp, ev[0].request_received_timestamp);
        assert!(ev[0].annotations.is_none());
        assert_eq!(ev[1].stage, AuditStage::ResponseComplete);
        assert_eq!(ev[1].audit_id, id);
        assert_eq!(ev[1].level, AuditLevel::Metadata);
        assert_eq!(ev[1].verb, "get");
        assert_eq!(ev[1].user_agent.as_deref(), Some("kubectl/1"));
        assert_eq!(ev[1].response_status.as_ref().unwrap().code, 200);
        let oref = ev[1].object_ref.as_ref().unwrap();
        assert_eq!(oref.resource.as_deref(), Some("secrets"));
        assert_eq!(oref.namespace.as_deref(), Some("ns"));
        assert_eq!(oref.name.as_deref(), Some("s"));
        assert_eq!(
            ev[1].annotations.as_ref().unwrap()["pod-security.kubernetes.io/enforce-policy"],
            "x:latest"
        );
    }

    /// Annotations authentication recorded (`AuthAuditAnnotations`, e.g.
    /// legacy.go:143-170) land on the audit event.
    #[tokio::test]
    async fn filter_applies_annotations_recorded_by_authentication() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(POLICY).unwrap(),
            sink: cap.clone(),
        });
        let mut req = axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(rusternetes_middleware::AuthAuditAnnotations(vec![(
                "authentication.k8s.io/legacy-token".to_string(),
                "system:serviceaccount:ns:sa".to_string(),
            )]));
        app(cfg).oneshot(req).await.unwrap();
        let ev = cap.0.lock().await;
        assert_eq!(
            ev[0].annotations.as_ref().unwrap()["authentication.k8s.io/legacy-token"],
            "system:serviceaccount:ns:sa"
        );
    }

    /// `LogResponseObject` (audit/request.go) stores the `metav1.Status` the
    /// handler wrote as the event's ResponseStatus, so a NotFound carries its
    /// status/reason/message, not just the code.
    #[tokio::test]
    async fn failed_response_status_body_is_recorded() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(&POLICY.replace("omitStages: [\"RequestReceived\"]\n", ""))
                .unwrap(),
            sink: cap.clone(),
        });
        let app = Router::new()
            .route(
                "/api/v1/namespaces/:ns/secrets/:name",
                get(|| async {
                    (
                        axum::http::StatusCode::NOT_FOUND,
                        [("content-type", "application/json")],
                        r#"{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"secrets \"s\" not found","reason":"NotFound","details":{"name":"s","kind":"secrets"},"code":404}"#,
                    )
                }),
            )
            .layer(axum::middleware::from_fn(move |req, next| {
                let cfg = cfg.clone();
                async move { with_audit(cfg, req, next).await }
            }));
        let resp = app
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        // The client still receives the body untouched.
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("NotFound"));
        let ev = cap.0.lock().await;
        let rs = ev[1].response_status.as_ref().unwrap();
        assert_eq!(rs.code, 404);
        assert_eq!(rs.status.as_deref(), Some("Failure"));
        assert_eq!(rs.reason.as_deref(), Some("NotFound"));
        assert_eq!(rs.message.as_deref(), Some("secrets \"s\" not found"));
        assert_eq!(rs.details.as_ref().unwrap()["name"], "s");
        let json = serde_json::to_value(rs).unwrap();
        assert_eq!(json["metadata"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn filter_honours_level_none_and_omit_stages() {
        let cap = Arc::new(Capture(Default::default()));
        let cfg = Arc::new(AuditConfig {
            policy: Policy::from_yaml(POLICY).unwrap(),
            sink: cap.clone(),
        });
        // Level None (no rule): nothing logged, no Audit-ID header.
        let r = app(cfg.clone())
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/services/s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(r.headers().get("Audit-ID").is_none());
        assert!(cap.0.lock().await.is_empty());
        // omitStages RequestReceived: only ResponseComplete.
        app(cfg)
            .oneshot(
                axum::http::Request::get("/api/v1/namespaces/ns/secrets/s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let ev = cap.0.lock().await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].stage, AuditStage::ResponseComplete);
    }

    fn test_ac() -> Arc<AuditContext> {
        let cap = Arc::new(Capture(Default::default()));
        Arc::new(AuditContext {
            inner: Mutex::new(AuditInner {
                event: AuditEvent {
                    api_version: "audit.k8s.io/v1".into(),
                    kind: "Event".into(),
                    level: AuditLevel::Metadata,
                    audit_id: "id".into(),
                    stage: AuditStage::RequestReceived,
                    request_uri: "/".into(),
                    verb: "get".into(),
                    user: UserInfo {
                        username: "u".into(),
                        uid: String::new(),
                        groups: vec![],
                        extra: None,
                    },
                    user_agent: None,
                    source_ips: vec![],
                    object_ref: None,
                    response_status: None,
                    request_received_timestamp: Utc::now(),
                    stage_timestamp: Utc::now(),
                    annotations: None,
                },
                config: RequestAuditConfig {
                    level: AuditLevel::Metadata,
                    omit_stages: vec![],
                    omit_managed_fields: false,
                },
            }),
            sink: cap,
        })
    }

    #[tokio::test]
    async fn annotation_conflict_keeps_first_and_unaudited_is_noop() {
        // Outside any request: AddAuditAnnotation with no AuditContext is a no-op.
        add_audit_annotation("a.example/k", "v");
        let ac = test_ac();
        AUDIT_CONTEXT
            .scope(ac.clone(), async {
                add_audit_annotation("a.example/k", "first");
                add_audit_annotation("a.example/k", "second");
            })
            .await;
        let g = ac.inner.lock().unwrap();
        assert_eq!(
            g.event.annotations.as_ref().unwrap()["a.example/k"],
            "first"
        );
    }

    #[tokio::test]
    async fn pod_security_outcome_is_recorded_with_the_psa_prefix() {
        let ac = test_ac();
        let outcome = crate::admission::PodSecurityOutcome {
            warnings: vec![],
            audit_annotations: std::collections::BTreeMap::from([
                ("enforce-policy".to_string(), "baseline:latest".to_string()),
                ("audit-violations".to_string(), "would violate".to_string()),
            ]),
        };
        AUDIT_CONTEXT
            .scope(ac.clone(), async {
                crate::admission::record_pod_security_audit(&outcome)
            })
            .await;
        let g = ac.inner.lock().unwrap();
        let a = g.event.annotations.as_ref().unwrap();
        assert_eq!(
            a["pod-security.kubernetes.io/enforce-policy"],
            "baseline:latest"
        );
        assert_eq!(
            a["pod-security.kubernetes.io/audit-violations"],
            "would violate"
        );
        assert_eq!(a.len(), 2);
    }
}
