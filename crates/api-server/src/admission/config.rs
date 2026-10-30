//! Per-plugin lookup in an `AdmissionConfiguration` document
//! (`--admission-control-config-file`).
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/admission/config.go`:
//! `makeAbs` (:37-48), the relative-`path` rewrite in
//! `ReadAdmissionConfiguration` (:68-79), `GetAdmissionPluginConfigurationFor`
//! (:120-137) and `configProvider.ConfigFor` (:141-160).

use std::path::{Path, PathBuf};

/// `makeAbs` (config.go:37-48): an absolute path is kept; a relative one is
/// joined to `base`, which is the working directory when empty or ".".
fn make_abs(path: &str, base: &Path) -> std::io::Result<PathBuf> {
    let p = Path::new(path);
    if p.is_absolute() {
        return Ok(p.to_path_buf());
    }
    if base.as_os_str().is_empty() || base == Path::new(".") {
        return Ok(std::env::current_dir()?.join(p));
    }
    Ok(base.join(p))
}

/// `legacyPluginsWithUnversionedConfig` (config.go:112).
const LEGACY_UNVERSIONED_PLUGINS: &[&str] = &["ImagePolicyWebhook", "PodNodeSelector"];

/// The legacy fallback of `ReadAdmissionConfiguration` (config.go:92-110): the
/// document did not decode as an `AdmissionConfiguration` for lack of a
/// version, a kind or a registered type, and it carries one of the two legacy
/// top-level keys.
fn is_legacy_plugin_config(doc: &serde_json::Value) -> bool {
    let Some(obj) = doc.as_object() else {
        return false;
    };
    let s = |k: &str| obj.get(k).and_then(|v| v.as_str()).unwrap_or("");
    let decodes = !s("apiVersion").is_empty()
        && !s("kind").is_empty()
        && s("kind") == "AdmissionConfiguration"
        && matches!(
            s("apiVersion"),
            "apiserver.config.k8s.io/v1" | "apiserver.k8s.io/v1alpha1"
        );
    !decodes && (obj.contains_key("imagePolicy") || obj.contains_key("podNodeSelectorPluginConfig"))
}

/// `configProvider.ConfigFor` (config.go:141-160) +
/// `GetAdmissionPluginConfigurationFor` (:120-137): the configuration of the
/// first plugin entry named `plugin_name`, as YAML text.
///
/// An inline `configuration` is returned directly and wins over `path`
/// (:123-125); otherwise `path` is read, relative to the directory of
/// `config_file` (`path.Dir(configFilePath)`, :66, :72-76). No matching
/// entry, or one with neither, is `Ok(None)` (:128, :158).
pub fn plugin_configuration_for(
    yaml: &str,
    config_file: &Path,
    plugin_name: &str,
) -> Result<Option<String>, String> {
    let doc: serde_json::Value = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
    if is_legacy_plugin_config(&doc) {
        // `legacyPluginsWithUnversionedConfig` (config.go:113-123): the whole
        // file is the configuration of each enabled legacy plugin.
        let legacy = LEGACY_UNVERSIONED_PLUGINS.contains(&plugin_name)
            && super::plugins::enabled(plugin_name);
        return Ok(legacy.then(|| yaml.to_string()));
    }
    let Some(entry) = doc
        .get("plugins")
        .and_then(|p| p.as_array())
        .into_iter()
        .flatten()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some(plugin_name))
    else {
        return Ok(None);
    };
    if let Some(cfg) = entry.get("configuration").filter(|c| !c.is_null()) {
        return serde_yaml::to_string(cfg)
            .map(Some)
            .map_err(|e| e.to_string());
    }
    let path = entry.get("path").and_then(|p| p.as_str()).unwrap_or("");
    if path.is_empty() {
        return Ok(None);
    }
    let abs = make_abs(path, config_file.parent().unwrap_or_else(|| Path::new("")))
        .map_err(|e| e.to_string())?;
    std::fs::read_to_string(&abs).map(Some).map_err(|e| {
        format!(
            "Couldn't open admission plugin configuration {}: {e}",
            abs.display()
        )
    })
}
