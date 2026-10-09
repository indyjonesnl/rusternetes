//! ABAC (attribute-based access control) authorizer for `--authorization-mode=ABAC`.
//!
//! Ported from `pkg/auth/authorizer/abac/abac.go` (policy matching, `NewFromFile`,
//! `RulesFor`) and the policy API types under `pkg/apis/abac` (`types.go`,
//! `v0/{types,conversion}.go`, `v1beta1/{types,conversion}.go`). Like upstream
//! the policy file is read once at startup (no hot reload; `reload.go:118-123`
//! reuses the `PolicyList` loaded at initial startup).

use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use serde::{Deserialize, Deserializer};
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

/// Go's `encoding/json` treats `null` as a no-op, leaving the zero value.
fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// `v0.Policy` (`pkg/apis/abac/v0/types.go`): the unversioned legacy format.
/// Keys are lowercased before decoding (Go's json matches case-insensitively).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct V0Policy {
    #[serde(deserialize_with = "null_default")]
    pub user: String,
    #[serde(deserialize_with = "null_default")]
    pub group: String,
    #[serde(deserialize_with = "null_default")]
    pub readonly: bool,
    #[serde(deserialize_with = "null_default")]
    pub resource: String,
    #[serde(deserialize_with = "null_default")]
    pub namespace: String,
}

/// `v1beta1.Policy` (`pkg/apis/abac/v1beta1/types.go`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct V1beta1Policy {
    #[serde(deserialize_with = "null_default")]
    pub spec: V1beta1PolicySpec,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct V1beta1PolicySpec {
    #[serde(deserialize_with = "null_default")]
    pub user: String,
    #[serde(deserialize_with = "null_default")]
    pub group: String,
    #[serde(deserialize_with = "null_default")]
    pub readonly: bool,
    #[serde(rename = "apigroup", deserialize_with = "null_default")]
    pub api_group: String,
    #[serde(deserialize_with = "null_default")]
    pub resource: String,
    #[serde(deserialize_with = "null_default")]
    pub namespace: String,
    #[serde(rename = "nonresourcepath", deserialize_with = "null_default")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ALL_AUTHENTICATED: &str = "system:authenticated";

    /// `newWithContents` (abac_test.go, end of file).
    fn new_with_contents(contents: &str) -> Result<PolicyList, PolicyLoadError> {
        static N: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "abac_test_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::write(&path, contents).unwrap();
        let r = PolicyList::new_from_file(&path);
        let _ = std::fs::remove_file(&path);
        r
    }

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        }
    }

    /// The Go tests set `ResourceRequest: len(ns) > 0 || len(resource) > 0`.
    fn attrs(
        u: &UserInfo,
        verb: &str,
        resource: &str,
        ns: &str,
        group: &str,
        path: &str,
    ) -> RequestAttributes {
        let mut a = RequestAttributes::new(u.clone(), verb, resource);
        a.api_group = group.to_string();
        if !ns.is_empty() {
            a.namespace = Some(ns.to_string());
        }
        if ns.is_empty() && resource.is_empty() {
            a.is_non_resource_request = true;
            a.path = Some(path.to_string());
        }
        a
    }

    async fn decide(pl: &PolicyList, a: &RequestAttributes) -> bool {
        matches!(pl.authorize(a).await.unwrap(), Decision::Allow)
    }

    type Case<'a> = (
        &'a UserInfo,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        bool,
    );

    async fn run_cases(pl: &PolicyList, cases: Vec<Case<'_>>) {
        for (i, (u, verb, res, ns, grp, path, want)) in cases.into_iter().enumerate() {
            let a = attrs(u, verb, res, ns, grp, path);
            assert_eq!(
                decide(pl, &a).await,
                want,
                "case {i}: {} {verb} {res} ns={ns} path={path}",
                u.username
            );
        }
    }

    // TestEmptyFile
    #[test]
    fn empty_file() {
        assert!(new_with_contents("").unwrap().0.is_empty());
    }

    // TestOneLineFileNoNewLine
    #[test]
    fn one_line_file_no_new_line() {
        let pl = new_with_contents(
            r#"{"user":"scheduler",  "readonly": true, "resource": "pods", "namespace":"ns1"}"#,
        )
        .unwrap();
        assert_eq!(pl.0.len(), 1);
    }

    // TestTwoLineFile
    #[test]
    fn two_line_file() {
        let pl = new_with_contents(
            "{\"user\":\"scheduler\",  \"readonly\": true, \"resource\": \"pods\"}\n{\"user\":\"scheduler\",  \"readonly\": true, \"resource\": \"services\"}\n",
        )
        .unwrap();
        assert_eq!(pl.0.len(), 2);
    }

    // TestExampleFile (the file upstream points users at, copied verbatim).
    #[test]
    fn example_file() {
        let pl = new_with_contents(include_str!("abac_example_policy_file.jsonl")).unwrap();
        assert_eq!(pl.0.len(), 11);
    }

    // abac.go:80-84 skips blank and `#` lines; :86-109 errors carry the line.
    #[test]
    fn bad_line_reports_path_and_line_number() {
        let err = new_with_contents("# c\n\n{\"user\":\"a\"}\nnot json\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.starts_with("error reading policy file "), "{msg}");
        assert!(msg.contains(", line 4: not json: "), "{msg}");
    }

    #[test]
    fn missing_file_is_an_error() {
        assert!(
            PolicyList::new_from_file(std::path::Path::new("/nonexistent/abac.jsonl")).is_err()
        );
    }

    // TestAuthorizeV0 (abac_test.go:66-169)
    #[tokio::test]
    async fn authorize_v0() {
        let pl = new_with_contents(
            r#"{                    "readonly": true, "resource": "events"   }
{"user":"scheduler", "readonly": true, "resource": "pods"     }
{"user":"scheduler",                   "resource": "bindings" }
{"user":"kubelet",   "readonly": true, "resource": "bindings" }
{"user":"kubelet",                     "resource": "events"   }
{"user":"alice",                                              "namespace": "projectCaribou"}
{"user":"bob",       "readonly": true,                        "namespace": "projectCaribou"}
"#,
        )
        .unwrap();
        let auth = [ALL_AUTHENTICATED];
        let sched = user("scheduler", &auth);
        let alice = user("alice", &auth);
        let chuck = user("chuck", &auth);
        run_cases(
            &pl,
            vec![
                (&sched, "list", "pods", "ns1", "", "", true),
                (&sched, "list", "pods", "", "", "", true),
                (&sched, "create", "pods", "ns1", "", "", false),
                (&sched, "create", "pods", "", "", "", false),
                (&sched, "get", "bindings", "ns1", "", "", true),
                (&sched, "get", "bindings", "", "", "", true),
                (&alice, "get", "pods", "projectCaribou", "", "", true),
                (&alice, "get", "widgets", "projectCaribou", "", "", true),
                (&alice, "get", "", "projectCaribou", "", "", true),
                (&alice, "update", "pods", "projectCaribou", "", "", true),
                (&alice, "update", "widgets", "projectCaribou", "", "", true),
                (&alice, "update", "", "projectCaribou", "", "", true),
                (&alice, "update", "foo", "projectCaribou", "bar", "", true),
                (&alice, "get", "pods", "ns1", "", "", false),
                (&alice, "get", "widgets", "ns1", "", "", false),
                (&alice, "get", "", "ns1", "", "", false),
                (&chuck, "get", "events", "ns1", "", "", true),
                (&chuck, "get", "events", "", "", "", true),
                (&chuck, "update", "events", "ns1", "", "", false),
                (&chuck, "get", "pods", "ns1", "", "", false),
                (&chuck, "get", "floop", "ns1", "", "", false),
                (&chuck, "get", "", "", "", "/", false),
            ],
        )
        .await;
    }

    // TestAuthorizeV1beta1 (abac_test.go:344-462)
    #[tokio::test]
    async fn authorize_v1beta1() {
        let l = |spec: &str| {
            format!(
                r#"{{"apiVersion":"abac.authorization.kubernetes.io/v1beta1","kind":"Policy","spec":{spec}}}"#
            )
        };
        let contents = [
            "\t\t # Comment line, after a blank line".to_string(),
            l(r#"{"user":"*","readonly":true,"nonResourcePath":"/api"}"#),
            l(r#"{"user":"*","nonResourcePath":"/custom"}"#),
            l(r#"{"user":"*","nonResourcePath":"/root/*"}"#),
            l(r#"{"user":"noresource","nonResourcePath":"*"}"#),
            l(r#"{"user":"*","readonly":true,"resource":"events","namespace":"*"}"#),
            l(r#"{"user":"scheduler","readonly":true,"resource":"pods","namespace":"*"}"#),
            l(r#"{"user":"scheduler","resource":"bindings","namespace":"*"}"#),
            l(r#"{"user":"kubelet","readonly":true,"resource":"bindings","namespace":"*"}"#),
            l(r#"{"user":"kubelet","resource":"events","namespace":"*"}"#),
            l(r#"{"user":"alice","resource":"*","namespace":"projectCaribou"}"#),
            l(r#"{"user":"bob","readonly":true,"resource":"*","namespace":"projectCaribou"}"#),
            l(r#"{"user":"debbie","resource":"pods","namespace":"projectCaribou"}"#),
            l(
                r#"{"user":"apigroupuser","resource":"*","namespace":"projectAnyGroup","apiGroup":"*"}"#,
            ),
            l(
                r#"{"user":"apigroupuser","resource":"*","namespace":"projectEmptyGroup","apiGroup":""}"#,
            ),
            format!(
                "\t\t {}",
                l(
                    r#"{"user":"apigroupuser","resource":"*","namespace":"projectXGroup","apiGroup":"x"}"#
                )
            ),
        ]
        .join("\n");
        let pl = new_with_contents(&contents).unwrap();
        let auth = [ALL_AUTHENTICATED];
        let sched = user("scheduler", &auth);
        let alice = user("alice", &auth);
        let chuck = user("chuck", &auth);
        let debbie = user("debbie", &auth);
        let nores = user("noresource", &auth);
        let apig = user("apigroupuser", &auth);
        run_cases(
            &pl,
            vec![
                (&sched, "list", "pods", "ns1", "", "", true),
                (&sched, "list", "pods", "", "", "", true),
                (&sched, "create", "pods", "ns1", "", "", false),
                (&sched, "create", "pods", "", "", "", false),
                (&sched, "get", "bindings", "ns1", "", "", true),
                (&sched, "get", "bindings", "", "", "", true),
                (&alice, "get", "pods", "projectCaribou", "", "", true),
                (&alice, "get", "widgets", "projectCaribou", "", "", true),
                (&alice, "get", "", "projectCaribou", "", "", true),
                (&alice, "update", "pods", "projectCaribou", "", "", true),
                (&alice, "update", "widgets", "projectCaribou", "", "", true),
                (&alice, "update", "", "projectCaribou", "", "", true),
                (&alice, "get", "pods", "ns1", "", "", false),
                (&alice, "get", "widgets", "ns1", "", "", false),
                (&alice, "get", "", "ns1", "", "", false),
                (&debbie, "update", "pods", "projectCaribou", "", "", true),
                (&chuck, "get", "events", "ns1", "", "", true),
                (&chuck, "get", "events", "", "", "", true),
                (&chuck, "update", "events", "ns1", "", "", false),
                (&chuck, "get", "pods", "ns1", "", "", false),
                (&chuck, "get", "floop", "ns1", "", "", false),
                (&chuck, "get", "", "", "", "/", false),
                (&chuck, "get", "", "", "", "/api", true),
                (&chuck, "create", "", "", "", "/api", false),
                (&chuck, "update", "", "", "", "/custom", true),
                (&chuck, "get", "", "", "", "/root", false),
                (&chuck, "get", "", "", "", "/root/", true),
                (&chuck, "get", "", "", "", "/root/test/1/2/3", true),
                (&nores, "get", "", "", "", "", true),
                (&nores, "get", "", "", "", "/", true),
                (&nores, "get", "", "", "", "/foo/bar/baz", true),
                (&nores, "get", "", "bar", "", "/", false),
                (&nores, "get", "foo", "bar", "", "/foo/bar/baz", false),
                (&apig, "get", "foo", "projectAnyGroup", "x", "", true),
                (&apig, "get", "foo", "projectEmptyGroup", "x", "", false),
                (&apig, "get", "foo", "projectXGroup", "x", "", true),
            ],
        )
        .await;
    }

    /// An explicit `v0` apiVersion is a registered version (v0/register.go:30)
    /// and converts like an unversioned line; an unregistered version or a
    /// missing kind falls back to v0 (abac.go:88).
    #[tokio::test]
    async fn explicit_v0_and_unregistered_decode_as_v0() {
        let pl = new_with_contents(
            "{\"apiVersion\":\"abac.authorization.kubernetes.io/v0\",\"kind\":\"Policy\",\"user\":\"alice\"}\n{\"apiVersion\":\"example.com/v9\",\"kind\":\"Policy\",\"user\":\"bob\"}\n{\"kind\":\"Policy\",\"user\":\"carol\"}\n",
        )
        .unwrap();
        let auth = [ALL_AUTHENTICATED];
        for name in ["alice", "bob", "carol"] {
            let a = attrs(&user(name, &auth), "get", "pods", "ns", "g", "");
            assert!(decide(&pl, &a).await, "{name}");
        }
    }

    // TestRulesFor (abac_test.go:171-342)
    #[tokio::test]
    async fn rules_for() {
        let pl = new_with_contents(
            r#"
{                    "readonly": true, "resource": "events"   }
{"user":"scheduler", "readonly": true, "resource": "pods"     }
{"user":"scheduler",                   "resource": "bindings" }
{"user":"kubelet",   "readonly": true, "resource": "pods"     }
{"user":"kubelet",                     "resource": "events"   }
{"user":"alice",                                              "namespace": "projectCaribou"}
{"user":"bob",       "readonly": true,                        "namespace": "projectCaribou"}
{"user":"bob",       "readonly": true,                                                     "nonResourcePath": "*"}
{"group":"a",                          "resource": "bindings" }
{"group":"b",        "readonly": true,                                                     "nonResourcePath": "*"}
"#,
        )
        .unwrap();
        let ro: Vec<String> = ["get", "list", "watch"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let all: Vec<String> = vec!["*".to_string()];
        type Res = Vec<(Vec<String>, Vec<String>, Vec<String>)>;
        type NonRes = Vec<(Vec<String>, Vec<String>)>;
        let r =
            |v: &Vec<String>, res: &str| (v.clone(), vec!["*".to_string()], vec![res.to_string()]);
        let nr = |v: &Vec<String>| (v.clone(), vec!["*".to_string()]);
        let auth = [ALL_AUTHENTICATED];
        let cases: Vec<(UserInfo, &str, Res, NonRes)> = vec![
            (
                user("scheduler", &auth),
                "ns1",
                vec![r(&ro, "events"), r(&ro, "pods"), r(&all, "bindings")],
                vec![],
            ),
            (
                user("kubelet", &["a", "b"]),
                "ns1",
                vec![
                    r(&ro, "pods"),
                    r(&all, "events"),
                    r(&all, "bindings"),
                    r(&ro, "*"),
                ],
                vec![nr(&ro)],
            ),
            (
                user("alice", &auth),
                "projectCaribou",
                vec![r(&ro, "events"), r(&all, "*")],
                vec![],
            ),
            (
                user("bob", &auth),
                "projectCaribou",
                vec![r(&ro, "events"), r(&ro, "*"), r(&ro, "*")],
                vec![nr(&ro)],
            ),
            (
                user("chuck", &["a", "b"]),
                "ns1",
                vec![r(&all, "bindings"), r(&ro, "*")],
                vec![nr(&ro)],
            ),
        ];
        for (i, (u, ns, want_r, want_nr)) in cases.into_iter().enumerate() {
            let (rr, nrr) = pl.get_user_rules(&u, ns).await.unwrap();
            let got_r: Res = rr
                .into_iter()
                .map(|x| {
                    (
                        x.verbs,
                        x.api_groups.unwrap_or_default(),
                        x.resources.unwrap_or_default(),
                    )
                })
                .collect();
            let got_nr: NonRes = nrr
                .into_iter()
                .map(|x| (x.verbs, x.non_resource_urls.unwrap_or_default()))
                .collect();
            assert_eq!(got_r, want_r, "case {i} resource rules");
            assert_eq!(got_nr, want_nr, "case {i} non-resource rules");
        }
    }

    fn v0(user: &str, group: &str) -> Policy {
        v0_to_policy(V0Policy {
            user: user.into(),
            group: group.into(),
            ..Default::default()
        })
    }

    fn v1(user: &str, group: &str) -> Policy {
        v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: user.into(),
                group: group.into(),
                ..Default::default()
            },
        })
    }

    // TestSubjectMatches (abac_test.go:464-834), representative rows of each
    // v0/v1 group.
    #[test]
    fn subject_matches_table() {
        let anon = user("system:anonymous", &["system:unauthenticated"]);
        let foo = user("Foo", &[ALL_AUTHENTICATED]);
        let foo_groups = user("Foo", &["a", "b", ALL_AUTHENTICATED]);
        let cases: Vec<(&str, &UserInfo, Policy, bool)> = vec![
            ("v0 empty, unauthed", &anon, v0("", ""), false),
            ("v0 * user, unauthed", &anon, v0("*", ""), false),
            ("v0 * group, unauthed", &anon, v0("", "*"), false),
            ("v0 empty, authed", &foo, v0("", ""), true),
            ("v0 empty, authed w/ groups", &foo_groups, v0("", ""), true),
            ("v0 user, unauthed", &anon, v0("Foo", ""), false),
            ("v0 user case-sensitive", &foo, v0("foo", ""), false),
            ("v0 user match", &foo, v0("Foo", ""), true),
            ("v0 group match", &foo_groups, v0("", "a"), true),
            ("v0 group case-sensitive", &foo_groups, v0("", "A"), false),
            (
                "v0 user+group wrong user",
                &foo_groups,
                v0("Bar", "a"),
                false,
            ),
            (
                "v0 user+group wrong group",
                &foo_groups,
                v0("Foo", "c"),
                false,
            ),
            ("v0 user+group match", &foo_groups, v0("Foo", "a"), true),
            ("v1 empty, authed", &foo, v1("", ""), false),
            ("v1 empty, authed w/ groups", &foo_groups, v1("", ""), false),
            ("v1 * user, unauthed", &anon, v1("*", ""), false),
            ("v1 * group, unauthed", &anon, v1("", "*"), false),
            ("v1 * user, authed", &foo, v1("*", ""), true),
            ("v1 user match", &foo, v1("Foo", ""), true),
            ("v1 user substring", &foo, v1("Fo", ""), false),
            ("v1 group match", &foo_groups, v1("", "b"), true),
            (
                "v1 user+group wrong user",
                &foo_groups,
                v1("Bar", "a"),
                false,
            ),
            ("v1 user+group match", &foo_groups, v1("Foo", "a"), true),
        ];
        for (name, u, p, want) in cases {
            assert_eq!(subject_matches(&p, u), want, "{name}");
        }
    }

    // TestPolicy (abac_test.go:836-1253), representative rows.
    #[test]
    fn policy_matches_table() {
        let foo = user("foo", &[ALL_AUTHENTICATED]);
        let mk = |verb: &str, res: &str, ns: &str, path: Option<&str>| {
            let mut a = RequestAttributes::new(foo.clone(), verb, res);
            if !ns.is_empty() {
                a.namespace = Some(ns.to_string());
            }
            if let Some(p) = path {
                a.is_non_resource_request = true;
                a.path = Some(p.to_string());
            }
            a
        };
        let p = v0_to_policy(V0Policy {
            readonly: true,
            ..Default::default()
        });
        assert!(
            !matches(&p, &mk("create", "", "", None)),
            "v0 read-only mismatch"
        );
        assert!(matches(&p, &mk("get", "", "", None)), "v0 read-only match");
        let p = v0_to_policy(V0Policy {
            user: "foo".into(),
            ..Default::default()
        });
        assert!(matches(&p, &mk("get", "x", "y", None)), "v0 user match");
        let bar = user("bar", &[ALL_AUTHENTICATED]);
        assert!(
            !matches(
                &p,
                &RequestAttributes::new(bar, "get", "x").with_namespace("y")
            ),
            "v0 user mismatch"
        );
        let p = v0_to_policy(V0Policy {
            resource: "foo".into(),
            ..Default::default()
        });
        assert!(
            !matches(&p, &mk("get", "bar", "y", None)),
            "v0 resource mismatch"
        );
        assert!(
            matches(&p, &mk("get", "foo", "y", None)),
            "v0 resource match"
        );
        let p = v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: "foo".into(),
                non_resource_path: "/foo/*".into(),
                ..Default::default()
            },
        });
        assert!(
            matches(&p, &mk("get", "", "", Some("/foo/bar"))),
            "v1 subpath match"
        );
        assert!(
            !matches(&p, &mk("get", "", "", Some("/bar/foo"))),
            "v1 subpath mismatch"
        );
        let p = v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: "foo".into(),
                namespace: "ns".into(),
                resource: "*".into(),
                api_group: "*".into(),
                ..Default::default()
            },
        });
        assert!(
            !matches(&p, &mk("get", "pods", "other", None)),
            "v1 ns mismatch"
        );
        assert!(matches(&p, &mk("get", "pods", "ns", None)), "v1 ns match");
        let p = v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: "foo".into(),
                non_resource_path: "*".into(),
                ..Default::default()
            },
        });
        assert!(
            !matches(&p, &mk("get", "pods", "ns", None)),
            "non-resource policy never matches a resource request"
        );
    }
}
