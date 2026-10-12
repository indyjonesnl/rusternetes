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
fn unknown_fields(
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

fn each_item(
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::validation::field::{ErrorType, Path};

    const SEC: i64 = 1_000_000_000;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ---- load_test.go TestLoadFromData ----------------------------------

    fn default_webhook() -> WebhookConfiguration {
        WebhookConfiguration {
            authorized_ttl: 5 * 60 * SEC,
            cache_authorized_requests: true,
            unauthorized_ttl: 30 * SEC,
            cache_unauthorized_requests: true,
            ..Default::default()
        }
    }

    fn bare_webhook_type() -> AuthorizationConfiguration {
        AuthorizationConfiguration {
            authorizers: vec![AuthorizerConfiguration {
                type_: "Webhook".into(),
                ..Default::default()
            }],
        }
    }

    fn defaulted_webhook() -> AuthorizationConfiguration {
        AuthorizationConfiguration {
            authorizers: vec![AuthorizerConfiguration {
                type_: "Webhook".into(),
                name: "default".into(),
                webhook: Some(default_webhook()),
            }],
        }
    }

    #[test]
    fn load_nil_and_empty_give_the_default_config() {
        assert_eq!(load_from_data(b"").unwrap(), Default::default());
    }

    #[test]
    fn load_every_version_json_defaults_and_yaml() {
        for v in ["v1alpha1", "v1beta1", "v1"] {
            let json = format!(
                r#"{{"apiVersion":"apiserver.config.k8s.io/{v}","kind":"AuthorizationConfiguration","authorizers":[{{"type":"Webhook"}}]}}"#
            );
            assert_eq!(
                load_from_data(json.as_bytes()).unwrap(),
                bare_webhook_type(),
                "{v} json"
            );
            let defaults = format!(
                r#"{{"apiVersion":"apiserver.config.k8s.io/{v}","kind":"AuthorizationConfiguration","authorizers":[{{"type":"Webhook","name":"default","webhook":{{}}}}]}}"#
            );
            assert_eq!(
                load_from_data(defaults.as_bytes()).unwrap(),
                defaulted_webhook(),
                "{v} defaults"
            );
            let yaml = format!(
                "\napiVersion: apiserver.config.k8s.io/{v}\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n"
            );
            assert_eq!(
                load_from_data(yaml.as_bytes()).unwrap(),
                bare_webhook_type(),
                "{v} yaml"
            );
        }
    }

    #[test]
    fn load_decodes_every_field() {
        let data = br#"
apiVersion: apiserver.config.k8s.io/v1
kind: AuthorizationConfiguration
authorizers:
- type: Webhook
  name: w1
  webhook:
    authorizedTTL: 1m
    cacheAuthorizedRequests: false
    unauthorizedTTL: 2s
    cacheUnauthorizedRequests: false
    timeout: 3s
    subjectAccessReviewVersion: v1
    matchConditionSubjectAccessReviewVersion: v1
    failurePolicy: Deny
    connectionInfo:
      type: KubeConfigFile
      kubeConfigFile: /etc/kubeconfig
    matchConditions:
    - expression: "request.user == 'admin'"
- type: RBAC
  name: rbac
"#;
        let c = load_from_data(data).unwrap();
        assert_eq!(c.authorizers.len(), 2);
        let w = c.authorizers[0].webhook.as_ref().unwrap();
        assert_eq!(w.authorized_ttl, 60 * SEC);
        assert!(!w.cache_authorized_requests);
        assert_eq!(w.unauthorized_ttl, 2 * SEC);
        assert!(!w.cache_unauthorized_requests);
        assert_eq!(w.timeout, 3 * SEC);
        assert_eq!(w.subject_access_review_version, "v1");
        assert_eq!(w.match_condition_subject_access_review_version, "v1");
        assert_eq!(w.failure_policy, "Deny");
        assert_eq!(w.connection_info.type_, "KubeConfigFile");
        assert_eq!(
            w.connection_info.kube_config_file.as_deref(),
            Some("/etc/kubeconfig")
        );
        assert_eq!(w.match_conditions[0].expression, "request.user == 'admin'");
        assert_eq!(c.authorizers[1].type_, "RBAC");
        assert!(c.authorizers[1].webhook.is_none());
    }

    #[test]
    fn load_errors() {
        let cases: [(&str, &str, &str); 6] = [
            (
                "missing apiVersion",
                r#"{"kind":"AuthorizationConfiguration"}"#,
                "'apiVersion' is missing",
            ),
            (
                "missing kind",
                r#"{"apiVersion":"apiserver.config.k8s.io/v1alpha1"}"#,
                "'Kind' is missing",
            ),
            (
                "unknown group",
                r#"{"apiVersion":"apps/v1alpha1","kind":"AuthorizationConfiguration"}"#,
                "apps/v1alpha1",
            ),
            (
                "unknown version",
                r#"{"apiVersion":"apiserver.config.k8s.io/v99","kind":"AuthorizationConfiguration"}"#,
                "apiserver.config.k8s.io/v99",
            ),
            (
                "unknown kind",
                r#"{"apiVersion":"apiserver.config.k8s.io/v1alpha1","kind":"SomeConfiguration"}"#,
                "SomeConfiguration",
            ),
            (
                "unknown field",
                r#"{"apiVersion":"apiserver.config.k8s.io/v1alpha1","kind":"AuthorizationConfiguration","authorzers":[{"type":"Webhook"}]}"#,
                r#"unknown field "authorzers""#,
            ),
        ];
        for (name, data, want) in cases {
            let err = load_from_data(data.as_bytes()).expect_err(name);
            assert!(err.contains(want), "{name}: {err}");
        }
    }

    #[test]
    fn load_exact_error_strings() {
        assert_eq!(
            load_from_data(br#"{"kind":"AuthorizationConfiguration"}"#).unwrap_err(),
            r#"Object 'apiVersion' is missing in '{"kind":"AuthorizationConfiguration"}'"#
        );
        assert_eq!(
            load_from_data(br#"{"apiVersion":"apiserver.config.k8s.io/v99","kind":"AuthorizationConfiguration"}"#)
                .unwrap_err(),
            r#"no kind "AuthorizationConfiguration" is registered for version "apiserver.config.k8s.io/v99" in scheme "k8s.io/apiserver/pkg/apis/apiserver/load/load.go:32""#
        );
        assert_eq!(
            load_from_data(br#"{"apiVersion":"apiserver.config.k8s.io/v1","kind":"AuthorizationConfiguration","authorizers":[{"type":"Webhook","webhok":{}}],"x":1}"#)
                .unwrap_err(),
            r#"strict decoding error: unknown field "authorizers[0].webhok", unknown field "x""#
        );
    }

    // ---- validation_test.go TestValidateAuthorizationConfiguration -------

    fn good_webhook() -> WebhookConfiguration {
        WebhookConfiguration {
            timeout: 5 * SEC,
            authorized_ttl: 5 * 60 * SEC,
            unauthorized_ttl: 30 * SEC,
            failure_policy: "NoOpinion".into(),
            subject_access_review_version: "v1".into(),
            match_condition_subject_access_review_version: "v1".into(),
            connection_info: WebhookConnectionInfo {
                type_: "InClusterConfig".into(),
                kube_config_file: None,
            },
            ..Default::default()
        }
    }

    fn one(
        type_: &str,
        name: &str,
        webhook: Option<WebhookConfiguration>,
    ) -> AuthorizationConfiguration {
        AuthorizationConfiguration {
            authorizers: vec![AuthorizerConfiguration {
                type_: type_.into(),
                name: name.into(),
                webhook,
            }],
        }
    }

    fn validate(
        c: &AuthorizationConfiguration,
        known: &[&str],
        repeatable: &[&str],
    ) -> Vec<String> {
        validate_authorization_configuration(&Path::default(), c, &set(known), &set(repeatable))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn webhook_errs(w: WebhookConfiguration) -> Vec<String> {
        validate(
            &one("Webhook", "default", Some(w)),
            &["Webhook"],
            &["Webhook"],
        )
    }

    #[test]
    fn at_least_one_authorizer() {
        assert_eq!(
            validate(&Default::default(), &[], &[]),
            vec!["authorizers: Required value: at least one authorization mode must be defined"]
        );
    }

    #[test]
    fn type_and_name_are_required() {
        assert_eq!(
            validate(&one("", "", None), &["Webhook"], &["Webhook"]),
            vec!["authorizers[0].type: Required value"]
        );
        assert_eq!(
            validate(&one("Foo", "", None), &["Foo"], &["Webhook"]),
            vec!["authorizers[0].name: Required value"]
        );
    }

    #[test]
    fn names_unique_and_dns1123() {
        let dup = AuthorizationConfiguration {
            authorizers: vec![
                AuthorizerConfiguration {
                    type_: "Foo".into(),
                    name: "foo".into(),
                    webhook: None,
                },
                AuthorizerConfiguration {
                    type_: "Bar".into(),
                    name: "foo".into(),
                    webhook: None,
                },
            ],
        };
        assert_eq!(
            validate(&dup, &["Foo", "Bar"], &["Webhook"]),
            vec![r#"authorizers[1].name: Duplicate value: "foo""#]
        );
        assert!(validate(&one("Foo", "myauthorizer", None), &["Foo"], &["Webhook"]).is_empty());
        assert!(validate(
            &one("Foo", "foo.example.domain", None),
            &["Foo"],
            &["Webhook"]
        )
        .is_empty());
        let bad = validate(
            &one("Foo", "FOO.example.domain", None),
            &["Foo"],
            &["Webhook"],
        );
        assert_eq!(bad.len(), 1);
        assert!(
            bad[0].starts_with(
                r#"authorizers[0].name: Invalid value: "FOO.example.domain": authorizer name is invalid: a lowercase RFC 1123 subdomain must consist of"#
            ),
            "{bad:?}"
        );
    }

    #[test]
    fn webhook_configurations_valid() {
        assert!(webhook_errs(good_webhook()).is_empty());
        let mut w = good_webhook();
        w.match_conditions = vec![
            WebhookMatchCondition {
                expression:
                    "has(request.resourceAttributes) && request.resourceAttributes.namespace == 'kube-system'"
                        .into(),
            },
            WebhookMatchCondition {
                expression: "request.user == 'admin'".into(),
            },
        ];
        assert!(webhook_errs(w).is_empty());
        let two = AuthorizationConfiguration {
            authorizers: vec![
                AuthorizerConfiguration {
                    type_: "Webhook".into(),
                    name: "default".into(),
                    webhook: Some(good_webhook()),
                },
                AuthorizerConfiguration {
                    type_: "Webhook".into(),
                    name: "second-webhook".into(),
                    webhook: Some(good_webhook()),
                },
            ],
        };
        assert!(validate(&two, &["Webhook"], &["Webhook"]).is_empty());
    }

    #[test]
    fn unknown_and_duplicate_types() {
        assert_eq!(
            validate(&one("Foo", "", None), &["Webhook"], &["Webhook"]),
            vec![r#"authorizers[0].type: Unsupported value: "Foo": supported values: "Webhook""#]
        );
        let twice = AuthorizationConfiguration {
            authorizers: vec![
                AuthorizerConfiguration {
                    type_: "Foo".into(),
                    name: "foo-1".into(),
                    webhook: None,
                },
                AuthorizerConfiguration {
                    type_: "Foo".into(),
                    name: "foo-2".into(),
                    webhook: None,
                },
            ],
        };
        assert_eq!(
            validate(&twice, &["Foo"], &["Webhook"]),
            vec![r#"authorizers[1].type: Duplicate value: "Foo""#]
        );
    }

    #[test]
    fn webhook_field_presence_by_type() {
        assert_eq!(
            validate(&one("Webhook", "default", None), &["Webhook"], &["Webhook"]),
            vec!["authorizers[0].webhook: Required value: required when type=Webhook"]
        );
        assert_eq!(
            validate(
                &one("Foo", "foo", Some(WebhookConfiguration::default())),
                &["Foo"],
                &["Webhook"]
            ),
            vec![
                r#"authorizers[0].webhook: Invalid value: "non-null": may only be specified when type=Webhook"#
            ]
        );
    }

    #[test]
    fn webhook_timeout() {
        let mut w = good_webhook();
        w.timeout = 0;
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].timeout: Required value"]
        );
        for (d, shown) in [(-30 * SEC, "-30s"), (60 * SEC, "1m0s")] {
            let mut w = good_webhook();
            w.timeout = d;
            assert_eq!(
                webhook_errs(w),
                vec![format!(
                    r#"authorizers[0].timeout: Invalid value: "{shown}": must be > 0s and <= 30s"#
                )]
            );
        }
    }

    #[test]
    fn webhook_ttls() {
        let mut w = good_webhook();
        w.authorized_ttl = 0;
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].authorizedTTL: Required value"]
        );
        let mut w = good_webhook();
        w.authorized_ttl = -30 * SEC;
        assert_eq!(
            webhook_errs(w),
            vec![r#"authorizers[0].authorizedTTL: Invalid value: "-30s": must be > 0s"#]
        );
        let mut w = good_webhook();
        w.unauthorized_ttl = 0;
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].unauthorizedTTL: Required value"]
        );
        let mut w = good_webhook();
        w.unauthorized_ttl = -30 * SEC;
        assert_eq!(
            webhook_errs(w),
            vec![r#"authorizers[0].unauthorizedTTL: Invalid value: "-30s": must be > 0s"#]
        );
    }

    #[test]
    fn webhook_sar_versions() {
        let mut w = good_webhook();
        w.subject_access_review_version = String::new();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].subjectAccessReviewVersion: Required value"]
        );
        let mut w = good_webhook();
        w.subject_access_review_version = "v2beta1".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].subjectAccessReviewVersion: Unsupported value: "v2beta1": supported values: "v1", "v1beta1""#
            ]
        );
        // v1beta1 is a valid subjectAccessReviewVersion.
        let mut w = good_webhook();
        w.subject_access_review_version = "v1beta1".into();
        assert!(webhook_errs(w).is_empty());

        let mut w = good_webhook();
        w.match_condition_subject_access_review_version = String::new();
        w.match_conditions = vec![WebhookMatchCondition {
            expression: "true".into(),
        }];
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].matchConditionSubjectAccessReviewVersion: Required value: required if match conditions are specified"]
        );
        // Empty is fine without match conditions.
        let mut w = good_webhook();
        w.match_condition_subject_access_review_version = String::new();
        assert!(webhook_errs(w).is_empty());
        let mut w = good_webhook();
        w.match_condition_subject_access_review_version = "v1beta1".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].matchConditionSubjectAccessReviewVersion: Unsupported value: "v1beta1": supported values: "v1""#
            ]
        );
    }

    #[test]
    fn webhook_failure_policy() {
        let mut w = good_webhook();
        w.failure_policy = String::new();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].failurePolicy: Required value"]
        );
        let mut w = good_webhook();
        w.failure_policy = "AlwaysAllow".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].failurePolicy: Unsupported value: "AlwaysAllow": supported values: "NoOpinion", "Deny""#
            ]
        );
        let mut w = good_webhook();
        w.failure_policy = "Deny".into();
        assert!(webhook_errs(w).is_empty());
    }

    #[test]
    fn webhook_connection_info() {
        let mut w = good_webhook();
        w.connection_info = WebhookConnectionInfo::default();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].connectionInfo.type: Required value"]
        );
        // validation.go:763 passes the whole struct as the bad value.
        let mut w = good_webhook();
        w.connection_info.type_ = "ExternalClusterConfig".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.type: Unsupported value: {"Type":"ExternalClusterConfig","KubeConfigFile":null}: supported values: "InClusterConfig", "KubeConfigFile""#
            ]
        );
        let mut w = good_webhook();
        w.connection_info.kube_config_file = Some(String::new());
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "": can only be set when type=KubeConfigFile"#
            ]
        );
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].connectionInfo.kubeConfigFile: Required value"]
        );
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some("../some/relative/path/kubeconfig".into());
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "../some/relative/path/kubeconfig": must be an absolute path"#
            ]
        );
        let f = tempfile_path("kc");
        std::fs::write(&f, "x").unwrap();
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some(f.clone());
        assert!(webhook_errs(w).is_empty());
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some("/nonexistent/kc".into());
        let e = webhook_errs(w);
        assert!(
            e[0].starts_with(
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "/nonexistent/kc": error loading file: "#
            ),
            "{e:?}"
        );
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some("/tmp".into());
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "/tmp": must be a regular file"#
            ]
        );
        let _ = std::fs::remove_file(f);
    }

    fn tempfile_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("authzcfg-{tag}-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn webhook_match_condition_paths_carry_the_authorizer_index() {
        let mut w = good_webhook();
        w.match_conditions = vec![WebhookMatchCondition {
            expression: "  ".into(),
        }];
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].matchConditions[0].expression: Required value"]
        );
        let mut w = good_webhook();
        let c = WebhookMatchCondition {
            expression: "request.user == 'admin'".into(),
        };
        w.match_conditions = vec![c.clone(), c];
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].matchConditions[1].expression: Duplicate value: "request.user == 'admin'""#
            ]
        );
    }

    #[test]
    fn error_types_are_field_error_types() {
        let errs = validate_authorization_configuration(
            &Path::default(),
            &Default::default(),
            &set(&[]),
            &set(&[]),
        );
        assert_eq!(errs[0].error_type, ErrorType::Required);
    }

    // ---- LoadAndValidateData (authorizer/config.go) ----------------------

    const HDR: &str = "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\n";

    #[test]
    fn load_and_validate_wraps_load_errors() {
        let e = load_and_validate_data(b"{\"kind\":\"AuthorizationConfiguration\"}", &set(&[]))
            .unwrap_err();
        assert!(
            e.starts_with("failed to load AuthorizationConfiguration from file: Object 'apiVersion' is missing"),
            "{e}"
        );
    }

    #[test]
    fn load_and_validate_empty_requires_an_authorizer() {
        assert_eq!(
            load_and_validate_data(b"", &set(&[])).unwrap_err(),
            "authorizers: Required value: at least one authorization mode must be defined"
        );
    }

    #[test]
    fn load_and_validate_builtin_authorizer_names_must_be_lowercased_type() {
        let ok =
            format!("{HDR}authorizers:\n- type: Node\n  name: node\n- type: RBAC\n  name: rbac\n");
        let c = load_and_validate_data(ok.as_bytes(), &set(&[])).unwrap();
        assert_eq!(c.authorizers.len(), 2);

        let bad = format!("{HDR}authorizers:\n- type: RBAC\n  name: foo\n");
        assert_eq!(
            load_and_validate_data(bad.as_bytes(), &set(&[])).unwrap_err(),
            "expected name rbac for authorizer RBAC instead of foo"
        );
        // Webhook names are free.
        let wh = format!(
            "{HDR}authorizers:\n- type: Webhook\n  name: whatever\n  webhook:\n    timeout: 5s\n    subjectAccessReviewVersion: v1\n    failurePolicy: Deny\n    connectionInfo:\n      type: InClusterConfig\n"
        );
        assert!(load_and_validate_data(wh.as_bytes(), &set(&[])).is_ok());
    }

    #[test]
    fn load_and_validate_unknown_type_and_missing_required_types() {
        let unknown = format!("{HDR}authorizers:\n- type: Bogus\n  name: bogus\n");
        let e = load_and_validate_data(unknown.as_bytes(), &set(&[])).unwrap_err();
        assert_eq!(
            e,
            r#"authorizers[0].type: Unsupported value: "Bogus": supported values: "ABAC", "AlwaysAllow", "AlwaysDeny", "Node", "RBAC", "Webhook""#
        );
        let only_rbac = format!("{HDR}authorizers:\n- type: RBAC\n  name: rbac\n");
        assert_eq!(
            load_and_validate_data(only_rbac.as_bytes(), &set(&["Node", "RBAC"])).unwrap_err(),
            "missing required types: [Node]"
        );
    }

    #[test]
    fn load_and_validate_aggregates_multiple_errors() {
        let two = format!("{HDR}authorizers:\n- type: RBAC\n  name: a\n- type: Node\n  name: b\n");
        assert_eq!(
            load_and_validate_data(two.as_bytes(), &set(&[])).unwrap_err(),
            "[expected name rbac for authorizer RBAC instead of a, expected name node for authorizer Node instead of b]"
        );
    }
}
