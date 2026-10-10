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

/// `LoadConfiguration` (config.go:39-65) plus `ValidateConfiguration`.
pub fn load_configuration(_yaml: &str) -> Result<Vec<LimitedResource>, String> {
    Err("not implemented".to_string())
}

/// The `ResourceQuota` entry of an `AdmissionConfiguration` document.
pub fn from_admission_configuration(_yaml: &str) -> Result<Vec<LimitedResource>, String> {
    Err("not implemented".to_string())
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
