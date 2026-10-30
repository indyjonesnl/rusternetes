//! `--authorization-config`: the structured `AuthorizationConfiguration` (#3294).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/apis/apiserver/`
//! (`types.go` the internal types, `v1/defaults.go` + `v1{alpha1,beta1}`
//! defaulting, `load/load.go` `LoadFromData`, `validation/validation.go`
//! `ValidateAuthorizationConfiguration` :642 / `ValidateWebhookConfiguration`
//! :694) and `pkg/kubeapiserver/authorizer/config.go` `LoadAndValidateData`.

use crate::authorization_match_conditions::{CelMatcher, WebhookMatchCondition};
use crate::authorizer::{AUTHORIZATION_MODE_CHOICES, MODE_WEBHOOK};
use rusternetes_common::go_duration::{format_go_duration, parse_go_duration};
use rusternetes_common::validation::field::{BadValue, Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::metav1::is_dns1123_subdomain;
use std::collections::BTreeSet;

/// `AuthorizerType` `Webhook` (types.go).
pub const TYPE_WEBHOOK: &str = "Webhook";
pub const FAILURE_POLICY_NO_OPINION: &str = "NoOpinion";
pub const FAILURE_POLICY_DENY: &str = "Deny";
pub const CONNECTION_INFO_TYPE_KUBE_CONFIG_FILE: &str = "KubeConfigFile";
pub const CONNECTION_INFO_TYPE_IN_CLUSTER: &str = "InClusterConfig";

/// `apiserver.AuthorizationConfiguration` (internal type).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuthorizationConfiguration {
    pub authorizers: Vec<AuthorizerConfiguration>,
}

/// `apiserver.AuthorizerConfiguration`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuthorizerConfiguration {
    pub type_: String,
    pub name: String,
    pub webhook: Option<WebhookConfiguration>,
}

/// `apiserver.WebhookConfiguration`; durations are Go `time.Duration`
/// nanoseconds (`metav1.Duration`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WebhookConfiguration {
    pub authorized_ttl: i64,
    pub cache_authorized_requests: bool,
    pub unauthorized_ttl: i64,
    pub cache_unauthorized_requests: bool,
    pub timeout: i64,
    pub subject_access_review_version: String,
    pub match_condition_subject_access_review_version: String,
    pub failure_policy: String,
    pub connection_info: WebhookConnectionInfo,
    pub match_conditions: Vec<WebhookMatchCondition>,
}

/// `apiserver.WebhookConnectionInfo`. The internal type has no json tags, so
/// a bad value of this type marshals with the Go field names.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize)]
pub struct WebhookConnectionInfo {
    #[serde(rename = "Type")]
    pub type_: String,
    #[serde(rename = "KubeConfigFile")]
    pub kube_config_file: Option<String>,
}

/// `runtime.NewScheme()`'s name, `GetNameFromCallsite` of load.go:32
/// (`scheme = runtime.NewScheme()`), as it prints in kube-apiserver builds
/// (the `vendor/` prefix is trimmed).
const SCHEME_NAME: &str = "k8s.io/apiserver/pkg/apis/apiserver/load/load.go:32";
/// The group `install.Install` registers (`apiserver.config.k8s.io`) and its
/// versions (`v1alpha1`, `v1beta1`, `v1`; the three are field-for-field
/// identical, as are their defaulters).
const GROUP: &str = "apiserver.config.k8s.io";
const VERSIONS: [&str; 3] = ["v1alpha1", "v1beta1", "v1"];
const KIND: &str = "AuthorizationConfiguration";

/// The versioned wire shape (`v1alpha1`/`v1beta1`/`v1` `types.go`).
#[derive(Debug, Default, serde::Deserialize)]
struct ExtConfiguration {
    #[serde(default)]
    authorizers: Option<Vec<ExtAuthorizer>>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtAuthorizer {
    #[serde(default, rename = "type")]
    type_: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    webhook: Option<ExtWebhook>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtWebhook {
    #[serde(default, rename = "authorizedTTL")]
    authorized_ttl: Option<String>,
    #[serde(default, rename = "cacheAuthorizedRequests")]
    cache_authorized_requests: Option<bool>,
    #[serde(default, rename = "unauthorizedTTL")]
    unauthorized_ttl: Option<String>,
    #[serde(default, rename = "cacheUnauthorizedRequests")]
    cache_unauthorized_requests: Option<bool>,
    #[serde(default)]
    timeout: Option<String>,
    #[serde(default, rename = "subjectAccessReviewVersion")]
    subject_access_review_version: Option<String>,
    #[serde(default, rename = "matchConditionSubjectAccessReviewVersion")]
    match_condition_subject_access_review_version: Option<String>,
    #[serde(default, rename = "failurePolicy")]
    failure_policy: Option<String>,
    #[serde(default, rename = "connectionInfo")]
    connection_info: Option<ExtConnectionInfo>,
    #[serde(default, rename = "matchConditions")]
    match_conditions: Option<Vec<ExtMatchCondition>>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtConnectionInfo {
    #[serde(default, rename = "type")]
    type_: Option<String>,
    #[serde(default, rename = "kubeConfigFile")]
    kube_config_file: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtMatchCondition {
    #[serde(default)]
    expression: Option<String>,
}

/// Which json keys each object level accepts (the json tags above); the strict
/// decoder (`serializer.EnableStrict`, load.go:33) is case-sensitive.
const TOP_KEYS: [&str; 3] = ["apiVersion", "kind", "authorizers"];
const AUTHORIZER_KEYS: [&str; 3] = ["type", "name", "webhook"];
const WEBHOOK_KEYS: [&str; 10] = [
    "authorizedTTL",
    "cacheAuthorizedRequests",
    "unauthorizedTTL",
    "cacheUnauthorizedRequests",
    "timeout",
    "subjectAccessReviewVersion",
    "matchConditionSubjectAccessReviewVersion",
    "failurePolicy",
    "connectionInfo",
    "matchConditions",
];
const CONNECTION_KEYS: [&str; 2] = ["type", "kubeConfigFile"];
const MATCH_CONDITION_KEYS: [&str; 1] = ["expression"];

/// `unknown field "<path>"` for every key not in `known`, in document order;
/// `descend` is called for each known key's value.
pub(crate) fn unknown_fields(
    v: &serde_yaml::Value,
    path: &str,
    known: &[&str],
    out: &mut Vec<String>,
    mut descend: impl FnMut(&str, &serde_yaml::Value, &str, &mut Vec<String>),
) {
    let Some(map) = v.as_mapping() else { return };
    for (k, val) in map {
        let Some(key) = k.as_str() else { continue };
        let full = if path.is_empty() {
            key.to_string()
        } else {
            format!("{path}.{key}")
        };
        if known.contains(&key) {
            descend(key, val, &full, out);
        } else {
            out.push(format!("unknown field \"{full}\""));
        }
    }
}

pub(crate) fn each_item(
    v: &serde_yaml::Value,
    path: &str,
    out: &mut Vec<String>,
    mut each: impl FnMut(&serde_yaml::Value, &str, &mut Vec<String>),
) {
    if let Some(seq) = v.as_sequence() {
        for (i, item) in seq.iter().enumerate() {
            each(item, &format!("{path}[{i}]"), out);
        }
    }
}

fn strict_errors(doc: &serde_yaml::Value) -> Vec<String> {
    let mut out = Vec::new();
    unknown_fields(doc, "", &TOP_KEYS, &mut out, |key, val, full, out| {
        if key != "authorizers" {
            return;
        }
        each_item(val, full, out, |a, p, out| {
            unknown_fields(a, p, &AUTHORIZER_KEYS, out, |key, val, full, out| {
                if key != "webhook" {
                    return;
                }
                unknown_fields(
                    val,
                    full,
                    &WEBHOOK_KEYS,
                    out,
                    |key, val, full, out| match key {
                        "connectionInfo" => {
                            unknown_fields(val, full, &CONNECTION_KEYS, out, |_, _, _, _| {})
                        }
                        "matchConditions" => each_item(val, full, out, |m, p, out| {
                            unknown_fields(m, p, &MATCH_CONDITION_KEYS, out, |_, _, _, _| {})
                        }),
                        _ => {}
                    },
                );
            });
        });
    });
    out
}

/// `metav1.Duration.UnmarshalJSON`: the string is `time.ParseDuration`'d.
fn ext_duration(v: &Option<String>) -> Result<i64, String> {
    match v {
        None => Ok(0),
        Some(s) => parse_go_duration(s),
    }
}

const DEFAULT_AUTHORIZED_TTL: i64 = 5 * 60 * 1_000_000_000;
const DEFAULT_UNAUTHORIZED_TTL: i64 = 30 * 1_000_000_000;

/// `SetDefaults_WebhookConfiguration` (v1/defaults.go:54-67, identical in
/// v1alpha1/v1beta1) followed by the generated external-to-internal
/// conversion.
fn convert_webhook(w: ExtWebhook) -> Result<WebhookConfiguration, String> {
    let mut authorized_ttl = ext_duration(&w.authorized_ttl)?;
    let mut unauthorized_ttl = ext_duration(&w.unauthorized_ttl)?;
    if authorized_ttl == 0 {
        authorized_ttl = DEFAULT_AUTHORIZED_TTL;
    }
    if unauthorized_ttl == 0 {
        unauthorized_ttl = DEFAULT_UNAUTHORIZED_TTL;
    }
    let conn = w.connection_info.unwrap_or_default();
    Ok(WebhookConfiguration {
        authorized_ttl,
        cache_authorized_requests: w.cache_authorized_requests.unwrap_or(true),
        unauthorized_ttl,
        cache_unauthorized_requests: w.cache_unauthorized_requests.unwrap_or(true),
        timeout: ext_duration(&w.timeout)?,
        subject_access_review_version: w.subject_access_review_version.unwrap_or_default(),
        match_condition_subject_access_review_version: w
            .match_condition_subject_access_review_version
            .unwrap_or_default(),
        failure_policy: w.failure_policy.unwrap_or_default(),
        connection_info: WebhookConnectionInfo {
            type_: conn.type_.unwrap_or_default(),
            kube_config_file: conn.kube_config_file,
        },
        match_conditions: w
            .match_conditions
            .unwrap_or_default()
            .into_iter()
            .map(|m| WebhookMatchCondition {
                expression: m.expression.unwrap_or_default(),
            })
            .collect(),
    })
}

/// `load.LoadFromData` (load.go:61-82). Empty data is the default (empty)
/// configuration. Otherwise: kind and apiVersion are required (json.go:197-200,
/// kind first), the (group, version, kind) must be registered, unknown fields
/// are a `strict decoding error` (load.go:33), then the versioned defaults are
/// applied and the object converted to the internal type.
pub fn load_from_data(data: &[u8]) -> Result<AuthorizationConfiguration, String> {
    if data.is_empty() {
        return Ok(AuthorizationConfiguration::default());
    }
    let text = String::from_utf8_lossy(data);
    let doc: serde_yaml::Value = serde_yaml::from_str(&text)
        .map_err(|e| format!("couldn't get version/kind; json parse error: {e}"))?;
    let field = |name: &str| {
        doc.get(name)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let (kind, api_version) = (field("kind"), field("apiVersion"));
    if kind.is_empty() {
        return Err(format!("Object 'Kind' is missing in '{text}'"));
    }
    if api_version.is_empty() {
        return Err(format!("Object 'apiVersion' is missing in '{text}'"));
    }
    let registered = kind == KIND
        && VERSIONS
            .iter()
            .any(|v| api_version == format!("{GROUP}/{v}"));
    if !registered {
        return Err(format!(
            "no kind {kind:?} is registered for version {api_version:?} in scheme {SCHEME_NAME:?}"
        ));
    }
    let unknown = strict_errors(&doc);
    if !unknown.is_empty() {
        return Err(format!("strict decoding error: {}", unknown.join(", ")));
    }
    let ext: ExtConfiguration = serde_yaml::from_value(doc)
        .map_err(|e| format!("couldn't get version/kind; json parse error: {e}"))?;
    let mut authorizers = Vec::new();
    for a in ext.authorizers.unwrap_or_default() {
        authorizers.push(AuthorizerConfiguration {
            type_: a.type_.unwrap_or_default(),
            name: a.name.unwrap_or_default(),
            webhook: a.webhook.map(convert_webhook).transpose()?,
        });
    }
    Ok(AuthorizationConfiguration { authorizers })
}

/// Go's text for an `os` error (`syscall.Errno.Error()`), lowercased.
pub(crate) fn go_io_error(e: &std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        _ => {
            let s = e.to_string();
            let s = s.split(" (os error").next().unwrap_or(&s);
            let mut c = s.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
    }
}

/// `validation.ValidateAuthorizationConfiguration` (validation.go:642-692).
pub fn validate_authorization_configuration(
    fld_path: &Path,
    c: &AuthorizationConfiguration,
    known_types: &BTreeSet<String>,
    repeatable_types: &BTreeSet<String>,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if c.authorizers.is_empty() {
        errs.push(FieldError::required(
            &fld_path.child("authorizers"),
            "at least one authorization mode must be defined",
        ));
    }
    let mut seen_types = BTreeSet::new();
    let mut seen_names = BTreeSet::new();
    for (i, a) in c.authorizers.iter().enumerate() {
        let fld_path = fld_path.child("authorizers").index(i);
        let a_type = a.type_.as_str();
        if a_type.is_empty() {
            errs.push(FieldError::required(&fld_path.child("type"), ""));
            continue;
        }
        if !known_types.contains(a_type) {
            let known: Vec<&String> = known_types.iter().collect();
            errs.push(FieldError::not_supported(
                &fld_path.child("type"),
                a_type,
                &known,
            ));
            continue;
        }
        if seen_types.contains(a_type) && !repeatable_types.contains(a_type) {
            errs.push(FieldError::duplicate(&fld_path.child("type"), a_type));
            continue;
        }
        seen_types.insert(a_type.to_string());

        if a.name.is_empty() {
            errs.push(FieldError::required(&fld_path.child("name"), ""));
        } else if seen_names.contains(&a.name) {
            errs.push(FieldError::duplicate(
                &fld_path.child("name"),
                a.name.as_str(),
            ));
        } else {
            let name_errs = is_dns1123_subdomain(&a.name);
            if !name_errs.is_empty() {
                errs.push(FieldError::invalid(
                    &fld_path.child("name"),
                    a.name.as_str(),
                    format!("authorizer name is invalid: {}", name_errs.join(", ")),
                ));
            }
        }
        seen_names.insert(a.name.clone());

        if a.type_ == TYPE_WEBHOOK {
            match &a.webhook {
                None => {
                    errs.push(FieldError::required(
                        &fld_path.child("webhook"),
                        "required when type=Webhook",
                    ));
                    continue;
                }
                Some(w) => errs.extend(validate_webhook_configuration(&fld_path, w)),
            }
        } else if a.webhook.is_some() {
            errs.push(FieldError::invalid(
                &fld_path.child("webhook"),
                "non-null",
                "may only be specified when type=Webhook",
            ));
        }
    }
    errs
}

/// `validation.ValidateWebhookConfiguration` (validation.go:694-770). Like
/// upstream, `fld_path` is the AUTHORIZER's path (the caller does not descend
/// into `webhook`), so errors read `authorizers[0].timeout`.
pub fn validate_webhook_configuration(fld_path: &Path, c: &WebhookConfiguration) -> ErrorList {
    let mut errs = ErrorList::new();
    if c.timeout == 0 {
        errs.push(FieldError::required(&fld_path.child("timeout"), ""));
    } else if c.timeout > 30 * 1_000_000_000 || c.timeout < 0 {
        errs.push(FieldError::invalid(
            &fld_path.child("timeout"),
            format_go_duration(c.timeout),
            "must be > 0s and <= 30s",
        ));
    }
    if c.authorized_ttl == 0 {
        errs.push(FieldError::required(&fld_path.child("authorizedTTL"), ""));
    } else if c.authorized_ttl < 0 {
        errs.push(FieldError::invalid(
            &fld_path.child("authorizedTTL"),
            format_go_duration(c.authorized_ttl),
            "must be > 0s",
        ));
    }
    if c.unauthorized_ttl == 0 {
        errs.push(FieldError::required(&fld_path.child("unauthorizedTTL"), ""));
    } else if c.unauthorized_ttl < 0 {
        errs.push(FieldError::invalid(
            &fld_path.child("unauthorizedTTL"),
            format_go_duration(c.unauthorized_ttl),
            "must be > 0s",
        ));
    }
    let sar = fld_path.child("subjectAccessReviewVersion");
    match c.subject_access_review_version.as_str() {
        "" => errs.push(FieldError::required(&sar, "")),
        "v1" | "v1beta1" => {}
        v => errs.push(FieldError::not_supported(&sar, v, &["v1", "v1beta1"])),
    }
    let mc_sar = fld_path.child("matchConditionSubjectAccessReviewVersion");
    match c.match_condition_subject_access_review_version.as_str() {
        "" => {
            if !c.match_conditions.is_empty() {
                errs.push(FieldError::required(
                    &mc_sar,
                    "required if match conditions are specified",
                ));
            }
        }
        "v1" => {}
        v => errs.push(FieldError::not_supported(&mc_sar, v, &["v1"])),
    }
    let fp = fld_path.child("failurePolicy");
    match c.failure_policy.as_str() {
        "" => errs.push(FieldError::required(&fp, "")),
        FAILURE_POLICY_NO_OPINION | FAILURE_POLICY_DENY => {}
        v => errs.push(FieldError::not_supported(&fp, v, &["NoOpinion", "Deny"])),
    }
    let conn_type = fld_path.child("connectionInfo").child("type");
    let conn_file = fld_path.child("connectionInfo").child("kubeConfigFile");
    match c.connection_info.type_.as_str() {
        "" => errs.push(FieldError::required(&conn_type, "")),
        CONNECTION_INFO_TYPE_IN_CLUSTER => {
            if let Some(f) = &c.connection_info.kube_config_file {
                errs.push(FieldError::invalid(
                    &conn_file,
                    f.as_str(),
                    "can only be set when type=KubeConfigFile",
                ));
            }
        }
        CONNECTION_INFO_TYPE_KUBE_CONFIG_FILE => {
            match c.connection_info.kube_config_file.as_deref() {
                None | Some("") => errs.push(FieldError::required(&conn_file, "")),
                Some(f) if !std::path::Path::new(f).is_absolute() => errs.push(
                    FieldError::invalid(&conn_file, f, "must be an absolute path"),
                ),
                Some(f) => match std::fs::metadata(f) {
                    Err(e) => errs.push(FieldError::invalid(
                        &conn_file,
                        f,
                        format!("error loading file: stat {f}: {}", go_io_error(&e)),
                    )),
                    Ok(m) if !m.is_file() => {
                        errs.push(FieldError::invalid(&conn_file, f, "must be a regular file"))
                    }
                    Ok(_) => {}
                },
            }
        }
        _ => errs.push(FieldError::not_supported(
            &conn_type,
            BadValue::marshal(&c.connection_info),
            &[
                CONNECTION_INFO_TYPE_IN_CLUSTER,
                CONNECTION_INFO_TYPE_KUBE_CONFIG_FILE,
            ],
        )),
    }
    // `compileMatchConditions(..., StructuredAuthorizationConfiguration
    // enabled)` (:766): the gate is GA and locked on in 1.35.
    let (_, mc_errs) = CelMatcher::compile_at(&c.match_conditions, fld_path);
    errs.extend(mc_errs);
    errs
}

/// `utilerrors.NewAggregate(...).Error()` (errors.go: duplicates removed, one
/// message verbatim, several as `[a, b]`).
pub(crate) fn aggregate_error(msgs: Vec<String>) -> String {
    let mut seen = BTreeSet::new();
    let uniq: Vec<String> = msgs
        .into_iter()
        .filter(|m| seen.insert(m.clone()))
        .collect();
    if uniq.len() == 1 {
        uniq[0].clone()
    } else {
        format!("[{}]", uniq.join(", "))
    }
}

/// `GetNameForAuthorizerMode` (config.go): the lower-cased mode name.
pub fn name_for_authorizer_mode(mode: &str) -> String {
    mode.to_lowercase()
}

/// `authorizer.LoadAndValidateData` (config.go): load, validate with the known
/// modes and only `Webhook` repeatable, then require that each non-Webhook
/// authorizer is named after its type and that every
/// `require_non_webhook_types` entry is present.
pub fn load_and_validate_data(
    data: &[u8],
    require_non_webhook_types: &BTreeSet<String>,
) -> Result<AuthorizationConfiguration, String> {
    let config = load_from_data(data)
        .map_err(|e| format!("failed to load AuthorizationConfiguration from file: {e}"))?;
    let known: BTreeSet<String> = AUTHORIZATION_MODE_CHOICES
        .iter()
        .map(|s| s.to_string())
        .collect();
    let repeatable: BTreeSet<String> = [MODE_WEBHOOK.to_string()].into_iter().collect();
    let errors =
        validate_authorization_configuration(&Path::default(), &config, &known, &repeatable);
    if !errors.is_empty() {
        return Err(aggregate_error(
            errors.iter().map(|e| e.to_string()).collect(),
        ));
    }
    let mut all_errors = Vec::new();
    let mut seen_modes = BTreeSet::new();
    for a in &config.authorizers {
        if a.type_ == MODE_WEBHOOK {
            continue;
        }
        seen_modes.insert(a.type_.clone());
        let expected = name_for_authorizer_mode(&a.type_);
        if expected != a.name {
            all_errors.push(format!(
                "expected name {expected} for authorizer {} instead of {}",
                a.type_, a.name
            ));
        }
    }
    let missing: Vec<&str> = require_non_webhook_types
        .difference(&seen_modes)
        .map(|s| s.as_str())
        .collect();
    if !missing.is_empty() {
        all_errors.push(format!("missing required types: [{}]", missing.join(" ")));
    }
    if !all_errors.is_empty() {
        return Err(aggregate_error(all_errors));
    }
    Ok(config)
}

/// `authorizer.LoadAndValidateFile`: the file's contents are returned with the
/// configuration (`InitialAuthorizationConfigurationData`).
pub fn load_and_validate_file(
    path: &str,
    require_non_webhook_types: &BTreeSet<String>,
) -> Result<(AuthorizationConfiguration, String), String> {
    let data = std::fs::read(path).map_err(|e| format!("open {path}: {}", go_io_error(&e)))?;
    let config = load_and_validate_data(&data, require_non_webhook_types)?;
    Ok((config, String::from_utf8_lossy(&data).into_owned()))
}
