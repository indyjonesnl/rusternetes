//! The audit pipeline: the policy (`--audit-policy-file`), the per-request
//! audit context, and the `WithAudit` filter that emits the events.
//!
//! Ported from `staging/src/k8s.io/apiserver` (release-1.35):
//! - `pkg/audit/policy/checker.go` (`EvaluatePolicyRule`, `ruleMatches*`),
//!   `pkg/audit/policy/reader.go` (`LoadPolicyFromBytes`),
//!   `pkg/apis/audit/validation/validation.go` (`ValidatePolicy`);
//! - `pkg/audit/context.go` (`AuditContext`, `ProcessEventStage`,
//!   `AddAuditAnnotations`), `pkg/audit/request.go` (`LogRequestMetadata`,
//!   `LogResponseObject`);
//! - `pkg/endpoints/filters/audit.go` (`WithAudit`);
//! - `pkg/endpoints/request/requestinfo.go` (`NewRequestInfo`).
//!
//! Not yet ported (tracked in the PR): the RequestObject body,
//! impersonated users, the ResponseStarted -> ResponseComplete split of a
//! long-running request, the Panic stage and the `legacy` format. The webhook
//! backend lives in `audit_webhook.rs`.

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
    /// `ValidatePolicy`, and refuse a policy with no rules. Decoding is
    /// strict first; a strict-only error (unknown fields) logs "Audit policy
    /// contains errors, falling back to lenient decoding" and keeps the
    /// lenient result, as `IsStrictDecodingError` does upstream.
    pub fn from_yaml(yaml: &str) -> Result<Policy, String> {
        let mut unknown: Vec<String> = Vec::new();
        let de = serde_yaml::Deserializer::from_str(yaml);
        let mut policy: Policy = serde_ignored::deserialize(de, |p| unknown.push(p.to_string()))
            .map_err(|e| format!("failed decoding: {e}"))?;
        if !unknown.is_empty() {
            let errs: Vec<String> = unknown
                .iter()
                .map(|f| format!("unknown field \"{f}\""))
                .collect();
            tracing::warn!(
                "Audit policy contains errors, falling back to lenient decoding: strict decoding error: {}",
                errs.join(", ")
            );
        }
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
    /// `RequestInfo.FieldSelector`: the unparsed `fieldSelector` query value.
    pub field_selector: String,
    /// `RequestInfo.LabelSelector`: the unparsed `labelSelector` query value.
    pub label_selector: String,
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
    let mut verb_via_path_prefix = false;
    if parts[0] == "proxy" || parts[0] == "watch" {
        if parts.len() < 2 {
            a.is_resource_request = false;
            return a;
        }
        a.verb = parts[0].to_string();
        verb_via_path_prefix = true;
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
        // `req.URL.Query()["watch"][0]` (requestinfo.go:225-239): the decoded
        // key, so `wat%63h=true` is `watch`; present with no value (`?watch`)
        // is true (`Convert_Slice_string_To_bool`, runtime/conversion.go:83:
        // only "0" and a case-insensitive "false" are false).
        let watch = query
            .and_then(|q| url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == "watch"))
            .is_some_and(|(_, v)| !(v == "0" || v.eq_ignore_ascii_case("false")));
        a.verb = if watch { "watch" } else { "list" }.to_string();
        // requestinfo.go:247-253: an exact `metadata.name` field selector names
        // the object.
        if let Some(name) = query
            .and_then(|q| field_selector_exact_match(q, "metadata.name"))
            .filter(|n| is_valid_path_segment_name(n))
        {
            a.name = name;
        }
    }
    if a.name.is_empty() && a.verb == "delete" {
        a.verb = "deletecollection".to_string();
    }
    // requestinfo.go:261-275 (`AuthorizeWithSelectors`, GA and locked on since
    // 1.34). Not supported on the deprecated verb-via-path mechanism; read
    // straight off the query so an undecodable sibling parameter cannot lose
    // them. `req.URL.Query()[k][0]` is the first occurrence.
    if !verb_via_path_prefix && matches!(a.verb.as_str(), "list" | "watch" | "deletecollection") {
        let first = |key: &str| {
            query.and_then(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.into_owned())
            })
        };
        a.field_selector = first("fieldSelector").unwrap_or_default();
        a.label_selector = first("labelSelector").unwrap_or_default();
    }
    a
}

/// `fieldSelector.RequiresExactMatch(field)` over the request's `fieldSelector`
/// query parameter (fields/selector.go `hasTerm`/`andTerm.RequiresExactMatch`):
/// the value of the first `field=value`/`field==value` term. An unparsable
/// selector yields `None`, as `DecodeParameters` failing leaves
/// `opts.FieldSelector` nil (requestinfo.go:225-232).
pub fn field_selector_exact_match(query: &str, field: &str) -> Option<String> {
    use rusternetes_common::field_selector::{FieldOperator, FieldSelector};
    let raw = url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "fieldSelector")?
        .1;
    let selector = FieldSelector::parse(&raw).ok()?;
    selector
        .requirements()
        .iter()
        .find(|r| r.operator() == FieldOperator::Equals && r.field() == field)
        .map(|r| r.value().to_string())
}

/// `path.IsValidPathSegmentName` (apimachinery/pkg/api/validation/path/name.go:31).
fn is_valid_path_segment_name(name: &str) -> bool {
    name != "." && name != ".." && !name.contains('/') && !name.contains('%')
}

// ---------------------------------------------------------------------
// AuditContext
// ---------------------------------------------------------------------

/// `audit.AuditContext` (context.go): the event of one request, shared by the
/// filter and everything the request passes through (admission adds
/// annotations to it).
pub struct AuditContext {
    pub inner: Mutex<AuditInner>,
    pub sink: Arc<dyn AuditBackend>,
}

pub struct AuditInner {
    pub event: AuditEvent,
    pub config: RequestAuditConfig,
}

tokio::task_local! {
    /// `audit.WithAuditContext` / `AuditContextFrom`: the context of the
    /// request being served.
    pub static AUDIT_CONTEXT: Arc<AuditContext>;
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

    /// `writeLatencyToAnnotation` (filters/audit.go:150-174): past 500ms,
    /// record each non-zero layer latency plus the request total.
    fn write_latency_to_annotation(&self) {
        const THRESHOLD: std::time::Duration = std::time::Duration::from_millis(500);
        let received = match self.inner.lock() {
            Ok(g) => g.event.request_received_timestamp,
            Err(_) => return,
        };
        let latency = (Utc::now() - received).to_std().unwrap_or_default();
        if latency <= THRESHOLD {
            return;
        }
        let mut layer =
            match rusternetes_common::latency_trackers::audit_annotations_from_latency_trackers() {
                Some(l) if !l.is_empty() => l,
                // "latency tracking is not enabled for this request"
                _ => return,
            };
        layer.push((
            "apiserver.latency.k8s.io/total".to_string(),
            rusternetes_common::go_duration::format_go_duration(latency.as_nanos() as i64),
        ));
        self.add_annotations(&layer);
    }

    /// `AddAuditAnnotations` (context.go:312-327).
    fn add_annotations(&self, kvs: &[(String, String)]) {
        if let Ok(mut g) = self.inner.lock() {
            for (k, v) in kvs {
                add_annotation_locked(&mut g.event, k, v);
            }
        }
    }

    /// `SetEventResponseStatusCode` (context.go:273-280): sets only the code,
    /// keeping any Status fields `LogResponseObject` already recorded.
    fn set_response_status(&self, code: u16) {
        if let Ok(mut g) = self.inner.lock() {
            g.event
                .response_status
                .get_or_insert_with(ResponseStatus::default)
                .code = code;
        }
    }

    fn level_ord(&self) -> u8 {
        self.inner
            .lock()
            .map(|g| level_ord(&g.event.level))
            .unwrap_or(0)
    }

    fn audit_id(&self) -> String {
        self.inner
            .lock()
            .map(|g| g.event.audit_id.clone())
            .unwrap_or_default()
    }

    /// `LogResponseObject` (audit/request.go:172-199) +
    /// `AuditContext.LogResponseObject` (context.go:158-175): below Metadata
    /// nothing is recorded; a `metav1.Status` has its bounded fields copied
    /// into `ResponseStatus`; at RequestResponse the object itself becomes
    /// `ResponseObject`, minus `managedFields` when the request's audit config
    /// omits them (`shouldOmitManagedFields`, request.go:289).
    fn log_response_object(&self, obj: &serde_json::Value) {
        let Ok(mut g) = self.inner.lock() else {
            return;
        };
        if level_ord(&g.event.level) < 1 {
            return;
        }
        if obj.get("kind").and_then(|k| k.as_str()) == Some("Status") {
            let s = |k: &str| obj.get(k).and_then(|x| x.as_str()).map(str::to_string);
            let code = obj
                .get("code")
                .and_then(|c| c.as_u64())
                .and_then(|c| u16::try_from(c).ok())
                .unwrap_or(0);
            // "selectively copy the bounded fields."
            g.event.response_status = Some(ResponseStatus {
                status: s("status"),
                message: s("message"),
                reason: s("reason"),
                details: obj.get("details").cloned(),
                code,
                ..Default::default()
            });
        }
        if level_ord(&g.event.level) < 3 {
            return;
        }
        let mut obj = obj.clone();
        if g.config.omit_managed_fields {
            remove_managed_fields(&mut obj);
        }
        g.event.response_object = Some(obj);
    }

    /// `GetEventLevel`.
    pub fn level(&self) -> AuditLevel {
        self.inner
            .lock()
            .map(|g| g.event.level.clone())
            .unwrap_or(AuditLevel::None)
    }
}

/// `copyWithoutManagedFields` (audit/request.go:235-276): the object's own
/// `metadata.managedFields`, those of a list's `items`, and those of a Table's
/// `rows[].object`.
fn remove_managed_fields(obj: &mut serde_json::Value) {
    fn strip(v: &mut serde_json::Value) {
        if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.remove("managedFields");
        }
    }
    strip(obj);
    if let Some(items) = obj.get_mut("items").and_then(|i| i.as_array_mut()) {
        items.iter_mut().for_each(strip);
    }
    if obj.get("kind").and_then(|k| k.as_str()) == Some("Table") {
        if let Some(rows) = obj.get_mut("rows").and_then(|r| r.as_array_mut()) {
            rows.iter_mut()
                .filter_map(|r| r.get_mut("object"))
                .for_each(strip);
        }
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

/// `audit.LogResponseObject(ctx, obj, gv, s)` (audit/request.go:172-199),
/// called by the response writers where the object is serialized
/// (`WriteObjectNegotiated`, responsewriters/writers.go:346). A no-op when the
/// request is not audited. The level is checked BEFORE the object is
/// serialized (request.go:174-180), so an unaudited or Metadata-level request
/// pays nothing for it.
pub fn log_response_object<B: serde::Serialize>(obj: &B) {
    let _ = AUDIT_CONTEXT.try_with(|ac| {
        if ac.level_ord() < 1 {
            return;
        }
        match serde_json::to_value(obj) {
            Ok(v) => ac.log_response_object(&v),
            Err(e) => error!(
                "encoding failed of response object (auditID {}): {e}",
                ac.audit_id()
            ),
        }
    });
}

/// A JSON 2xx body that records the object it serializes: `log_response_object`
/// then `Json(obj)`, the pair `WriteObjectNegotiated` performs
/// (responsewriters/writers.go:346). Handlers that return a typed list/review
/// directly use this instead of a bare `Json(..)`, which would bypass audit.
pub fn json_response<B: serde::Serialize>(obj: B) -> axum::Json<B> {
    log_response_object(&obj);
    axum::Json(obj)
}

/// `audit.AddAuditAnnotation(ctx, key, value)` (context.go:297).
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

/// The event half of `evaluatePolicyAndCreateAuditEvent`
/// (filters/audit.go:117-148): `ac.Init(rac, sink)` +
/// `audit.LogRequestMetadata` (request.go:43-82).
fn new_audit_context(
    cfg: &AuditConfig,
    uri: &axum::http::Uri,
    headers: &axum::http::HeaderMap,
    attrs: &Attributes,
    user_info: UserInfo,
    rac: RequestAuditConfig,
) -> Arc<AuditContext> {
    let received = Utc::now();
    let audit_id = headers
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
        request_uri: uri
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_default(),
        verb: attrs.verb.clone(),
        user: user_info,
        user_agent: headers
            .get("User-Agent")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        source_ips: headers
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
        response_object: None,
        request_received_timestamp: received,
        stage_timestamp: received,
        annotations: None,
    };
    Arc::new(AuditContext {
        inner: Mutex::new(AuditInner { event, config: rac }),
        sink: cfg.sink.clone(),
    })
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

    let ac = new_audit_context(&cfg, req.uri(), req.headers(), &attrs, user_info, rac);
    let audit_id = ac.audit_id();

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
    // The error writer (`Error::into_response`) cannot reach this crate's
    // audit context; it hands the Status it serialized over as a response
    // extension, recorded here exactly as `LogResponseObject` would have.
    if let Some(rusternetes_common::audit::AuditResponseObject(obj)) =
        resp.extensions_mut()
            .remove::<rusternetes_common::audit::AuditResponseObject>()
    {
        ac.log_response_object(&obj);
    }
    // `auditResponseWriter.processCode` (filters/audit.go:204-211).
    ac.set_response_status(resp.status().as_u16());
    if long_running {
        ac.process_event_stage(AuditStage::ResponseStarted).await;
    }
    // `writeLatencyToAnnotation` runs in `WithAudit`'s deferred func, which is
    // the audit filter's own context: the trackers are the outer filter's.
    ac.write_latency_to_annotation();
    ac.process_event_stage(AuditStage::ResponseComplete).await;
    resp
}

/// `withFailedRequestAudit` (request_deadline.go:106-133) against the
/// installed pipeline; a no-op without one (`sink == nil || policy == nil`,
/// :107-109).
pub async fn audit_failed_request(
    method: &Method,
    uri: &axum::http::Uri,
    headers: &axum::http::HeaderMap,
    status: &rusternetes_common::types::Status,
    failed: Response,
) -> Response {
    match AUDIT.get() {
        Some(cfg) => with_failed_request_audit(cfg, method, uri, headers, status, failed).await,
        None => failed,
    }
}

/// `withFailedRequestAudit` (request_deadline.go:106-133). It runs before
/// authentication, so the request has no user (`attribs.GetUser()` is nil,
/// request.go:61) and the policy is evaluated for an unauthenticated request.
/// The one event it writes is at stage ResponseStarted: no RequestReceived and
/// no ResponseComplete, because the failed handler is not wrapped by WithAudit.
pub async fn with_failed_request_audit(
    cfg: &AuditConfig,
    method: &Method,
    uri: &axum::http::Uri,
    headers: &axum::http::HeaderMap,
    status: &rusternetes_common::types::Status,
    mut failed: Response,
) -> Response {
    let mut attrs = request_info(method, uri.path(), uri.query());
    let user_info = UserInfo {
        username: String::new(),
        uid: String::new(),
        groups: vec![],
        extra: None,
    };
    attrs.user = Some(user_info.clone());
    let rac = cfg.policy.evaluate(&attrs);
    if rac.level == AuditLevel::None {
        // `!ac.Enabled()` (:118-121).
        return failed;
    }
    let ac = new_audit_context(cfg, uri, headers, &attrs, user_info, rac);
    // `respStatus.Message = statusErr.Error()` (:123-127).
    if let Ok(mut g) = ac.inner.lock() {
        g.event.response_status = Some(ResponseStatus {
            message: status.message.clone(),
            ..Default::default()
        });
    }
    // `ErrorNegotiated` -> `WriteObjectNegotiated` -> `audit.LogResponseObject`
    // (responsewriters/writers.go:346), then the audit writer's `processCode`
    // on the header write (filters/audit.go:205-211) with
    // `processResponseStartedStage` = true (:130).
    if let Ok(obj) = serde_json::to_value(status) {
        ac.log_response_object(&obj);
    }
    ac.set_response_status(failed.status().as_u16());
    ac.process_event_stage(AuditStage::ResponseStarted).await;
    // `WithAuditInit` sets the Audit-ID header ahead of this filter.
    if let Ok(v) = HeaderValue::from_str(&ac.audit_id()) {
        failed.headers_mut().insert("Audit-ID", v);
    }
    failed
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
