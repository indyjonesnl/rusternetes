//! ABAC (attribute-based access control) authorizer for `--authorization-mode=ABAC`.
//!
//! Ported from `pkg/auth/authorizer/abac/abac.go` (policy matching, `NewFromFile`,
//! `RulesFor`) and the policy API types under `pkg/apis/abac` (`types.go`,
//! `v0/{types,conversion}.go`, `v1beta1/{types,conversion}.go`). Like upstream
//! the policy file is read once at startup (no hot reload; `reload.go:118-123`
//! reuses the `PolicyList` loaded at initial startup).

use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use serde::Deserialize;
use std::path::Path;

/// `abac.GroupName` (`pkg/apis/abac/register.go:27`).
const GROUP_NAME: &str = "abac.authorization.kubernetes.io";

/// `user.AllAuthenticated` (v0/conversion.go:26, v1beta1/conversion.go:26).
const ALL_AUTHENTICATED: &str = "system:authenticated";

/// `abac.Policy` (internal type, `pkg/apis/abac/types.go:27-62`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Policy {
    pub spec: PolicySpec,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PolicySpec {
    pub user: String,
    pub group: String,
    pub readonly: bool,
    pub api_group: String,
    pub resource: String,
    pub namespace: String,
    pub non_resource_path: String,
}

/// `v0.Policy` (`pkg/apis/abac/v0/types.go`): the unversioned legacy format.
/// Keys are lowercased before decoding (Go's json matches case-insensitively).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct V0Policy {
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub user: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub group: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub readonly: bool,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub resource: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub namespace: String,
}

/// `v1beta1.Policy` (`pkg/apis/abac/v1beta1/types.go`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct V1beta1Policy {
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub spec: V1beta1PolicySpec,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct V1beta1PolicySpec {
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub user: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub group: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub readonly: bool,
    #[serde(
        rename = "apigroup",
        deserialize_with = "rusternetes_common::serde_helpers::null_default"
    )]
    pub api_group: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub resource: String,
    #[serde(deserialize_with = "rusternetes_common::serde_helpers::null_default")]
    pub namespace: String,
    #[serde(
        rename = "nonresourcepath",
        deserialize_with = "rusternetes_common::serde_helpers::null_default"
    )]
    pub non_resource_path: String,
}

/// `Convert_v0_Policy_To_abac_Policy` (v0/conversion.go:27-61).
pub fn v0_to_policy(input: V0Policy) -> Policy {
    let mut out = PolicySpec {
        user: input.user.clone(),
        group: input.group.clone(),
        namespace: input.namespace.clone(),
        resource: input.resource.clone(),
        readonly: input.readonly,
        ..Default::default()
    };

    // In v0, unspecified user and group matches all authenticated subjects
    if input.user.is_empty() && input.group.is_empty() {
        out.group = ALL_AUTHENTICATED.to_string();
    }
    // In v0, user or group of * matches all authenticated subjects
    if input.user == "*" || input.group == "*" {
        out.group = ALL_AUTHENTICATED.to_string();
        out.user = String::new();
    }

    // In v0, leaving namespace empty matches all namespaces
    if input.namespace.is_empty() {
        out.namespace = "*".to_string();
    }
    // In v0, leaving resource empty matches all resources
    if input.resource.is_empty() {
        out.resource = "*".to_string();
    }
    // Any rule in v0 should match all API groups
    out.api_group = "*".to_string();

    // In v0, leaving namespace and resource blank allows non-resource paths
    if input.namespace.is_empty() && input.resource.is_empty() {
        out.non_resource_path = "*".to_string();
    }

    Policy { spec: out }
}

/// `Convert_v1beta1_Policy_To_abac_Policy` (v1beta1/conversion.go:27-39); the
/// field copy is `autoConvert_v1beta1_Policy_To_abac_Policy`.
pub fn v1beta1_to_policy(input: V1beta1Policy) -> Policy {
    let s = input.spec;
    let mut out = PolicySpec {
        user: s.user.clone(),
        group: s.group.clone(),
        readonly: s.readonly,
        api_group: s.api_group,
        resource: s.resource,
        namespace: s.namespace,
        non_resource_path: s.non_resource_path,
    };
    // In v1beta1, * user or group maps to all authenticated subjects
    if s.user == "*" || s.group == "*" {
        out.group = ALL_AUTHENTICATED.to_string();
        out.user = String::new();
    }
    Policy { spec: out }
}

/// `policyLoadError` (abac.go:39-51), plus the raw `os.Open` error that
/// `NewFromFile` returns unwrapped (abac.go:62-65).
#[derive(Debug)]
pub enum PolicyLoadError {
    Open(std::io::Error),
    Read {
        path: String,
        /// 1-based line number; `None` for a scanner error (`line: -1`).
        line: Option<usize>,
        data: String,
        err: String,
    },
}

impl std::fmt::Display for PolicyLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyLoadError::Open(e) => write!(f, "{e}"),
            PolicyLoadError::Read {
                path,
                line: Some(line),
                data,
                err,
            } => write!(
                f,
                "error reading policy file {path}, line {line}: {data}: {err}"
            ),
            PolicyLoadError::Read {
                path,
                line: None,
                err,
                ..
            } => write!(f, "error reading policy file {path}: {err}"),
        }
    }
}

impl std::error::Error for PolicyLoadError {}

/// `PolicyList` (abac.go:53-54).
#[derive(Debug, Default)]
pub struct PolicyList(pub Vec<Policy>);

/// Decode one policy line the way `abac.Codecs.UniversalDecoder()` plus the
/// v0 fallback does (abac.go:86-102). Both v0 and v1beta1 register kind
/// `Policy` in group `abac.authorization.kubernetes.io`
/// (v0/register.go:30, v1beta1/register.go:30); a missing version, missing
/// kind or unregistered GVK (`IsMissingVersion`/`IsMissingKind`/
/// `IsNotRegisteredError`) is decoded as an unversioned v0 policy.
fn decode_line(line: &str) -> Result<Policy, String> {
    let value: serde_json::Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
    let serde_json::Value::Object(map) = value else {
        return Err("json: cannot unmarshal into Go value of type v0.Policy".to_string());
    };
    // Go's encoding/json matches keys case-insensitively.
    let mut norm = serde_json::Map::new();
    for (k, v) in map {
        let v = match (k.to_lowercase().as_str(), v) {
            ("spec", serde_json::Value::Object(spec)) => serde_json::Value::Object(
                spec.into_iter()
                    .map(|(k, v)| (k.to_lowercase(), v))
                    .collect(),
            ),
            (_, v) => v,
        };
        norm.insert(k.to_lowercase(), v);
    }
    let api_version = norm
        .get("apiversion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let kind = norm.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    let v1beta1 = format!("{GROUP_NAME}/v1beta1");
    let obj = serde_json::Value::Object(norm.clone());
    if kind == "Policy" && api_version == v1beta1 {
        let p: V1beta1Policy = serde_json::from_value(obj).map_err(|e| e.to_string())?;
        return Ok(v1beta1_to_policy(p));
    }
    // Registered v0 or the unversioned/unregistered fallback: same decode.
    let p: V0Policy = serde_json::from_value(obj).map_err(|e| e.to_string())?;
    Ok(v0_to_policy(p))
}

impl PolicyList {
    /// `NewFromFile` (abac.go:56-119). File format is one JSON object per
    /// line; blank lines and `#` comment lines are skipped.
    pub fn new_from_file(path: &Path) -> Result<Self, PolicyLoadError> {
        let contents = std::fs::read(path).map_err(PolicyLoadError::Open)?;
        let path_s = path.display().to_string();
        let text = String::from_utf8(contents).map_err(|e| PolicyLoadError::Read {
            path: path_s.clone(),
            line: None,
            data: String::new(),
            err: e.to_string(),
        })?;
        let mut pl = Vec::new();
        let mut unversioned = 0usize;
        for (idx, raw) in text.lines().enumerate() {
            let i = idx + 1;
            // skip comment lines and blank lines (abac.go:80-84)
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if !raw_has_registered_gvk(raw) {
                unversioned += 1;
            }
            let p = decode_line(raw).map_err(|err| PolicyLoadError::Read {
                path: path_s.clone(),
                line: Some(i),
                data: raw.to_string(),
                err,
            })?;
            pl.push(p);
        }
        if unversioned > 0 {
            tracing::warn!(
                "Policy file {path_s} contained unversioned rules. See docs/admin/authorization.md#abac-mode for ABAC file format details."
            );
        }
        Ok(PolicyList(pl))
    }
}

/// Whether a line carries a registered GVK (anything else counts toward
/// `unversionedLines`, abac.go:91).
fn raw_has_registered_gvk(line: &str) -> bool {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(line) else {
        return false;
    };
    let get = |name: &str| {
        map.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let av = get("apiVersion");
    get("kind") == "Policy"
        && (av == format!("{GROUP_NAME}/v1beta1") || av == format!("{GROUP_NAME}/v0"))
}

/// `matches` (abac.go:121-134).
pub fn matches(p: &Policy, a: &RequestAttributes) -> bool {
    if subject_matches(p, &a.user) && verb_matches(p, a) {
        // Resource and non-resource requests are mutually exclusive, at most
        // one will match a policy
        if resource_matches(p, a) {
            return true;
        }
        if non_resource_matches(p, a) {
            return true;
        }
    }
    false
}

/// `subjectMatches` (abac.go:136-177): true if the specified user and group
/// properties in the policy match the attributes.
pub fn subject_matches(p: &Policy, user: &UserInfo) -> bool {
    let mut matched = false;

    // If the policy specified a user, ensure it matches
    if !p.spec.user.is_empty() {
        if p.spec.user == "*" {
            matched = true;
        } else {
            matched = p.spec.user == user.username;
            if !matched {
                return false;
            }
        }
    }

    // If the policy specified a group, ensure it matches
    if !p.spec.group.is_empty() {
        if p.spec.group == "*" {
            matched = true;
        } else {
            matched = user.groups.contains(&p.spec.group);
            if !matched {
                return false;
            }
        }
    }

    matched
}

/// `IsReadOnly` (`authorizer.AttributesRecord`, `attributes.go`): get, list, watch.
fn is_read_only(verb: &str) -> bool {
    matches!(verb, "get" | "list" | "watch")
}

/// `verbMatches` (abac.go:179-193).
fn verb_matches(p: &Policy, a: &RequestAttributes) -> bool {
    // All policies allow read only requests
    if is_read_only(&a.verb) {
        return true;
    }
    // Allow if policy is not readonly
    !p.spec.readonly
}

/// `nonResourceMatches` (abac.go:195-212).
fn non_resource_matches(p: &Policy, a: &RequestAttributes) -> bool {
    // A non-resource policy cannot match a resource request
    if a.is_non_resource_request {
        let path = a.path.as_deref().unwrap_or("");
        let np = p.spec.non_resource_path.as_str();
        // Allow wildcard match
        if np == "*" {
            return true;
        }
        // Allow exact match
        if np == path {
            return true;
        }
        // Allow a trailing * subpath match
        if np.ends_with('*') && path.starts_with(np.trim_end_matches('*')) {
            return true;
        }
    }
    false
}

/// `resourceMatches` (abac.go:214-226).
fn resource_matches(p: &Policy, a: &RequestAttributes) -> bool {
    // A resource policy cannot match a non-resource request
    if !a.is_non_resource_request {
        let ns = a.namespace.as_deref().unwrap_or("");
        if (p.spec.namespace == "*" || p.spec.namespace == ns)
            && (p.spec.resource == "*" || p.spec.resource == a.resource)
            && (p.spec.api_group == "*" || p.spec.api_group == a.api_group)
        {
            return true;
        }
    }
    false
}

/// `getVerbs` (abac.go:274-279).
fn get_verbs(is_read_only: bool) -> Vec<String> {
    if is_read_only {
        vec!["get".into(), "list".into(), "watch".into()]
    } else {
        vec!["*".into()]
    }
}

#[async_trait::async_trait]
impl Authorizer for PolicyList {
    /// `PolicyList.Authorize` (abac.go:229-239). No match is
    /// `DecisionNoOpinion`; our [`Decision`] has no `NoOpinion` and
    /// `UnionAuthorizer` treats `Deny` as "fall through" (see
    /// `PrivilegedGroupAuthorizer`), so `Deny("No policy matched.")`.
    async fn authorize(
        &self,
        attrs: &RequestAttributes,
    ) -> rusternetes_common::error::Result<Decision> {
        for p in &self.0 {
            if matches(p, attrs) {
                return Ok(Decision::Allow);
            }
        }
        Ok(Decision::Deny("No policy matched.".to_string()))
    }

    // abac.go:229-235: allow reason is empty; abstention is exactly
    // "No policy matched.". Do not manufacture an ABAC allow description.
    async fn authorize_opinion(
        &self,
        attrs: &RequestAttributes,
    ) -> rusternetes_common::Result<Opinion> {
        Ok(match self.authorize(attrs).await? {
            Decision::Allow => Opinion::Allow(String::new()),
            Decision::Deny(reason) => Opinion::NoOpinion {
                reason,
                error: None,
            },
        })
    }

    /// `PolicyList.RulesFor` (abac.go:241-272).
    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> rusternetes_common::error::Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        let mut resource_rules = Vec::new();
        let mut non_resource_rules = Vec::new();
        for p in &self.0 {
            if subject_matches(p, user)
                && (p.spec.namespace == "*" || p.spec.namespace == namespace)
            {
                if !p.spec.resource.is_empty() {
                    resource_rules.push(ResourceRule {
                        verbs: get_verbs(p.spec.readonly),
                        api_groups: Some(vec![p.spec.api_group.clone()]),
                        resources: Some(vec![p.spec.resource.clone()]),
                        resource_names: None,
                    });
                }
                if !p.spec.non_resource_path.is_empty() {
                    non_resource_rules.push(NonResourceRule {
                        verbs: get_verbs(p.spec.readonly),
                        non_resource_urls: Some(vec![p.spec.non_resource_path.clone()]),
                    });
                }
            }
        }
        Ok((resource_rules, non_resource_rules))
    }
}
