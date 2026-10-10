//! The ResourceQuota admission plugin's configuration.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/`
//! `config.go` (`LoadConfiguration`), `apis/resourcequota/types.go`
//! (`Configuration`, `LimitedResource`) and `apis/resourcequota/validation`.

use rusternetes_common::resources::ScopedResourceSelectorRequirement;
use serde::Deserialize;

/// `resourcequotaapi.LimitedResource` (types.go:36-69): a resource whose
/// consumption is limited by default.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LimitedResource {
    #[serde(default)]
    pub api_group: String,
    #[serde(default)]
    pub resource: String,
    #[serde(default)]
    pub match_contains: Vec<String>,
    #[serde(default)]
    pub match_scopes: Vec<ScopedResourceSelectorRequirement>,
}

/// The decoded `Configuration` (types.go:26-34), strictly: an unknown or
/// duplicate field is a `strict decoding error` (config.go:48,
/// `serializer.EnableStrict`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Configuration {
    #[serde(default, rename = "kind")]
    _kind: String,
    #[serde(default, rename = "apiVersion")]
    _api_version: String,
    #[serde(default)]
    limited_resources: Vec<LimitedResource>,
}

/// The kinds `install.Install` registers (apis/resourcequota/install,
/// v1alpha1, v1beta1 and v1 `register.go`).
const REGISTERED: [(&str, &str); 3] = [
    ("Configuration", "resourcequota.admission.k8s.io/v1alpha1"),
    ("Configuration", "resourcequota.admission.k8s.io/v1beta1"),
    ("ResourceQuotaConfiguration", "apiserver.config.k8s.io/v1"),
];

/// `LoadConfiguration` (config.go:39-65) plus `ValidateConfiguration`
/// (validation.go:26-36).
pub fn load_configuration(yaml: &str) -> Result<Vec<LimitedResource>, String> {
    let strict = |e: serde_yaml::Error| format!("strict decoding error: {e}");
    let doc: serde_json::Value = serde_yaml::from_str(yaml).map_err(strict)?;
    let field = |name: &str| doc.get(name).and_then(|v| v.as_str()).unwrap_or("");
    let (kind, api_version) = (field("kind"), field("apiVersion"));
    if kind.is_empty() {
        return Err("Object 'Kind' is missing".to_string());
    }
    if !REGISTERED.contains(&(kind, api_version)) {
        return Err(format!(
            "no kind \"{kind}\" is registered for version \"{api_version}\""
        ));
    }
    let config: Configuration = serde_yaml::from_str(yaml).map_err(strict)?;

    // `ValidateConfiguration`: every limited resource names its resource.
    let mut errs = Vec::new();
    for (i, lr) in config.limited_resources.iter().enumerate() {
        if lr.resource.is_empty() {
            errs.push(format!("limitedResources[{i}].resource: Required value"));
        }
    }
    if !errs.is_empty() {
        return Err(errs.join(", "));
    }
    Ok(config.limited_resources)
}

/// The `ResourceQuota` entry of an `AdmissionConfiguration` document
/// (`--admission-control-config-file`), its inline `configuration` or the
/// file named by its `path`. No such
/// entry means nothing is limited (the plugin's default configuration).
pub fn from_admission_configuration(
    yaml: &str,
    config_file: &std::path::Path,
) -> Result<Vec<LimitedResource>, String> {
    match crate::admission::config::plugin_configuration_for(yaml, config_file, "ResourceQuota")? {
        Some(cfg) => load_configuration(&cfg),
        None => Ok(Vec::new()),
    }
}

static CONFIGURED: std::sync::OnceLock<Vec<LimitedResource>> = std::sync::OnceLock::new();

/// Install the process-wide configuration, read once at startup.
pub fn install_limited_resources(limited: Vec<LimitedResource>) {
    let _ = CONFIGURED.set(limited);
}

/// The installed `LimitedResources` (empty by default).
pub fn installed_limited_resources() -> &'static [LimitedResource] {
    CONFIGURED.get().map(Vec::as_slice).unwrap_or_default()
}
