//! Kubelet image credential provider plugin configuration.
//!
//! Ports `pkg/credentialprovider/plugin/config.go`
//! (`readCredentialProviderConfig`, `decode`, `validateCredentialProviderConfig`)
//! and the registration checks of `plugin.go` `RegisterCredentialProviderPlugins`
//! (bin dir and per-provider binary must exist; any failure aborts the
//! kubelet, `kuberuntime_manager.go:310-313`). The flags are
//! `--image-credential-provider-config` / `--image-credential-provider-bin-dir`.
//!
//! Deviations: strict decoding (`serializer.EnableStrict`) is a walk over the
//! known field names rather than the Go decoder, with the same
//! `strict decoding error: unknown field "providers[0].x"` text. The
//! `KubeletServiceAccountTokenForCredentialProviders` gate is not modelled by
//! this kubelet's feature gates; callers pass its (default, Beta on) value.
//! Not ported yet: running the plugins (`execPlugin`), the response cache and
//! the keyring (`pluginProvider.Provide`), and service account tokens.

use anyhow::{anyhow, Result};
use rusternetes_common::go_duration::{format_go_duration, parse_go_duration};
use rusternetes_common::validation::metav1::is_qualified_name;
use serde::Deserialize;
use serde_yaml::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// `credentialproviderv1.SchemeGroupVersion` and its siblings: the provider
/// `apiVersion`s the kubelet can speak (`plugin.go` `apiVersions`).
pub const CREDENTIAL_PROVIDER_API_V1: &str = "credentialprovider.kubelet.k8s.io/v1";
const CREDENTIAL_PROVIDER_API_VERSIONS: [&str; 3] = [
    "credentialprovider.kubelet.k8s.io/v1",
    "credentialprovider.kubelet.k8s.io/v1alpha1",
    "credentialprovider.kubelet.k8s.io/v1beta1",
];
const CONFIG_GROUP: &str = "kubelet.config.k8s.io";
const CONFIG_VERSIONS: [&str; 3] = ["v1alpha1", "v1beta1", "v1"];
const VALID_CACHE_TYPES: [&str; 2] = ["ServiceAccount", "Token"];

/// `kubeletconfig.ExecEnvVar`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ExecEnvVar {
    pub name: String,
    pub value: String,
}

/// `kubeletconfig.ServiceAccountTokenAttributes`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountTokenAttributes {
    #[serde(default)]
    pub service_account_token_audience: String,
    #[serde(default)]
    pub cache_type: String,
    pub require_service_account: Option<bool>,
    #[serde(default)]
    pub required_service_account_annotation_keys: Vec<String>,
    #[serde(default)]
    pub optional_service_account_annotation_keys: Vec<String>,
}

/// `kubeletconfig.CredentialProvider`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CredentialProvider {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub match_images: Vec<String>,
    /// `defaultCacheDuration` (a Go duration string) in nanoseconds.
    #[serde(default, deserialize_with = "de_duration")]
    pub default_cache_duration: Option<i64>,
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<ExecEnvVar>,
    pub token_attributes: Option<ServiceAccountTokenAttributes>,
}

fn de_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let s: Option<String> = Option::deserialize(d)?;
    s.map(|s| parse_go_duration(&s).map_err(serde::de::Error::custom))
        .transpose()
}

/// `kubeletconfig.CredentialProviderConfig` after merging.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CredentialProviderConfig {
    pub providers: Vec<CredentialProvider>,
}

#[derive(Deserialize)]
struct RawConfig {
    #[serde(default)]
    providers: Vec<CredentialProvider>,
}

fn unknown_field_check(value: &Value, version: &str) -> Result<()> {
    const TOP: [&str; 3] = ["apiVersion", "kind", "providers"];
    const PROVIDER: [&str; 7] = [
        "name",
        "matchImages",
        "defaultCacheDuration",
        "apiVersion",
        "args",
        "env",
        "tokenAttributes",
    ];
    const ENV: [&str; 2] = ["name", "value"];
    const TOKEN: [&str; 5] = [
        "serviceAccountTokenAudience",
        "cacheType",
        "requireServiceAccount",
        "requiredServiceAccountAnnotationKeys",
        "optionalServiceAccountAnnotationKeys",
    ];
    let unknown = |path: String| anyhow!("strict decoding error: unknown field \"{path}\"");
    let keys = |v: &Value| -> Vec<String> {
        v.as_mapping()
            .map(|m| {
                m.keys()
                    .filter_map(|k| k.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    for k in keys(value) {
        if !TOP.contains(&k.as_str()) {
            return Err(unknown(k));
        }
    }
    let Some(providers) = value.get("providers").and_then(Value::as_sequence) else {
        return Ok(());
    };
    for (i, p) in providers.iter().enumerate() {
        for k in keys(p) {
            // `tokenAttributes` only exists in the v1 config type.
            if !PROVIDER.contains(&k.as_str()) || (k == "tokenAttributes" && version != "v1") {
                return Err(unknown(format!("providers[{i}].{k}")));
            }
        }
        for (j, e) in p
            .get("env")
            .and_then(Value::as_sequence)
            .into_iter()
            .flatten()
            .enumerate()
        {
            for k in keys(e) {
                if !ENV.contains(&k.as_str()) {
                    return Err(unknown(format!("providers[{i}].env[{j}].{k}")));
                }
            }
        }
        if let Some(t) = p.get("tokenAttributes") {
            for k in keys(t) {
                if !TOKEN.contains(&k.as_str()) {
                    return Err(unknown(format!("providers[{i}].tokenAttributes.{k}")));
                }
            }
        }
    }
    Ok(())
}

/// `decode` (`config.go`): kind/group/version checks, strict field check.
pub fn decode(data: &[u8]) -> Result<CredentialProviderConfig> {
    let value: Value = serde_yaml::from_slice(data)?;
    let api_version = value
        .get("apiVersion")
        .and_then(Value::as_str)
        .unwrap_or("");
    let kind = value.get("kind").and_then(Value::as_str).unwrap_or("");
    let version = api_version
        .strip_prefix(CONFIG_GROUP)
        .and_then(|v| v.strip_prefix('/'))
        .filter(|v| CONFIG_VERSIONS.contains(v));
    let Some(version) = version else {
        return Err(anyhow!(
            "no kind \"{kind}\" is registered for version \"{api_version}\" in scheme"
        ));
    };
    if kind != "CredentialProviderConfig" {
        return Err(anyhow!(
            "no kind \"{kind}\" is registered for version \"{api_version}\" in scheme"
        ));
    }
    unknown_field_check(&value, version)?;
    let raw: RawConfig = serde_yaml::from_value(value)?;
    Ok(CredentialProviderConfig {
        providers: raw.providers,
    })
}

/// `readCredentialProviderConfig`: a file, or every `.json`/`.yaml`/`.yml`
/// file of a directory in lexical order, merged; plus the length-prefixed
/// sha256 of the file contents.
pub fn read_credential_provider_config(
    config_path: &str,
) -> Result<(CredentialProviderConfig, String)> {
    if config_path.is_empty() {
        return Err(anyhow!("credential provider config path is empty"));
    }
    let meta = std::fs::metadata(config_path)
        .map_err(|e| anyhow!("unable to access path {config_path:?}: {e}"))?;
    let mut files: Vec<PathBuf> = Vec::new();
    if meta.is_dir() {
        let entries = std::fs::read_dir(config_path)
            .map_err(|e| anyhow!("unable to read directory {config_path:?}: {e}"))?;
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if !path.is_dir() && matches!(ext, "json" | "yaml" | "yml") {
                files.push(path);
            }
        }
        files.sort();
        if files.is_empty() {
            return Err(anyhow!(
                "no configuration files found in directory {config_path:?}"
            ));
        }
    } else {
        files.push(PathBuf::from(config_path));
    }

    let mut hasher = Sha256::new();
    let mut merged = CredentialProviderConfig::default();
    let mut names = BTreeSet::new();
    for file in &files {
        let data = std::fs::read(file).map_err(|e| anyhow!("unable to read file {file:?}: {e}"))?;
        hasher.update((data.len() as u64).to_be_bytes());
        hasher.update(&data);
        let cfg = decode(&data).map_err(|e| anyhow!("error decoding config {file:?}: {e}"))?;
        for p in cfg.providers {
            if !names.insert(p.name.clone()) {
                return Err(anyhow!(
                    "duplicate provider name {:?} found in configuration file(s)",
                    p.name
                ));
            }
            merged.providers.push(p);
        }
    }
    Ok((merged, format!("sha256:{:x}", hasher.finalize())))
}

/// `credentialprovider.ParseSchemelessURL`: only the failure Go's `url.Parse`
/// reports for an invalid percent escape is detected.
fn parse_schemeless_url(s: &str) -> Result<(), String> {
    let full = format!("https://{s}");
    let b = full.as_bytes();
    for (i, c) in b.iter().enumerate() {
        if *c == b'%' {
            let ok = b.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && b.get(i + 2).is_some_and(u8::is_ascii_hexdigit);
            if !ok {
                let end = (i + 3).min(b.len());
                return Err(format!(
                    "parse {full:?}: invalid URL escape {:?}",
                    String::from_utf8_lossy(&b[i..end])
                ));
            }
        }
    }
    Ok(())
}

fn quote_list(items: &[&str]) -> String {
    items
        .iter()
        .map(|s| format!("{s:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_annotation_keys(path: &str, keys: &[String], errs: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    for k in keys {
        for msg in is_qualified_name(&k.to_lowercase()) {
            errs.push(format!("{path}: Invalid value: {k:?}: {msg}"));
        }
        if !seen.insert(k.clone()) {
            errs.push(format!("{path}: Duplicate value: {k:?}"));
        }
    }
}

/// `validateCredentialProviderConfig`: field errors as upstream formats them
/// (`path: Reason: detail`); empty when valid.
pub fn validate_credential_provider_config(
    config: &CredentialProviderConfig,
    sa_token_for_credential_providers: bool,
) -> Vec<String> {
    let mut errs = Vec::new();
    if config.providers.is_empty() {
        errs.push("providers: Required value: at least 1 item in plugins is required".into());
    }
    let mut seen = BTreeSet::new();
    for p in &config.providers {
        let name = &p.name;
        if name.contains('/') {
            errs.push(format!(
                "providers.name: Invalid value: {name:?}: provider name cannot contain '/'"
            ));
        }
        if name.contains(' ') {
            errs.push(format!(
                "providers.name: Invalid value: {name:?}: provider name cannot contain spaces"
            ));
        }
        if name == "." {
            errs.push(format!(
                "providers.name: Invalid value: {name:?}: provider name cannot be '.'"
            ));
        }
        if name == ".." {
            errs.push(format!(
                "providers.name: Invalid value: {name:?}: provider name cannot be '..'"
            ));
        }
        if !seen.insert(name.clone()) {
            errs.push(format!("providers.name: Duplicate value: {name:?}"));
        }
        if p.api_version.is_empty() {
            errs.push("providers.apiVersion: Required value".into());
        } else if !CREDENTIAL_PROVIDER_API_VERSIONS.contains(&p.api_version.as_str()) {
            errs.push(format!(
                "providers.apiVersion: Unsupported value: {:?}: supported values: {}",
                p.api_version,
                quote_list(&CREDENTIAL_PROVIDER_API_VERSIONS)
            ));
        }
        if p.match_images.is_empty() {
            errs.push(
                "providers.matchImages: Required value: at least 1 item in matchImages is required"
                    .into(),
            );
        }
        for m in &p.match_images {
            if let Err(e) = parse_schemeless_url(m) {
                errs.push(format!(
                    "providers.matchImages: Invalid value: {m:?}: match image is invalid: {e}"
                ));
            }
        }
        match p.default_cache_duration {
            None => errs.push("providers.defaultCacheDuration: Required value".into()),
            Some(d) if d < 0 => errs.push(format!(
                "providers.defaultCacheDuration: Invalid value: {:?}: must be greater than or equal to 0",
                format_go_duration(d)
            )),
            Some(_) => {}
        }
        if let Some(t) = &p.token_attributes {
            let f = "providers.tokenAttributes";
            if !sa_token_for_credential_providers {
                errs.push(format!("{f}: Forbidden: tokenAttributes is not supported when KubeletServiceAccountTokenForCredentialProviders feature gate is disabled"));
            }
            if t.service_account_token_audience.is_empty() {
                errs.push(format!("{f}.serviceAccountTokenAudience: Required value"));
            }
            if t.require_service_account.is_none() {
                errs.push(format!("{f}.requireServiceAccount: Required value"));
            }
            if p.api_version != CREDENTIAL_PROVIDER_API_V1 {
                errs.push(format!(
                    "{f}: Forbidden: tokenAttributes is only supported for {CREDENTIAL_PROVIDER_API_V1} API version"
                ));
            }
            if t.require_service_account == Some(false)
                && !t.required_service_account_annotation_keys.is_empty()
            {
                errs.push(format!("{f}.requiredServiceAccountAnnotationKeys: Forbidden: requireServiceAccount cannot be false when requiredServiceAccountAnnotationKeys is set"));
            }
            validate_annotation_keys(
                &format!("{f}.requiredServiceAccountAnnotationKeys"),
                &t.required_service_account_annotation_keys,
                &mut errs,
            );
            validate_annotation_keys(
                &format!("{f}.optionalServiceAccountAnnotationKeys"),
                &t.optional_service_account_annotation_keys,
                &mut errs,
            );
            let required: BTreeSet<&String> =
                t.required_service_account_annotation_keys.iter().collect();
            let both: Vec<String> = t
                .optional_service_account_annotation_keys
                .iter()
                .filter(|k| required.contains(k))
                .map(|k| format!("{k:?}"))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if !both.is_empty() {
                errs.push(format!(
                    "{f}: Invalid value: [{}]: annotation keys cannot be both required and optional",
                    both.join(", ")
                ));
            }
            if t.cache_type.is_empty() {
                errs.push(format!("{f}.cacheType: Required value: cacheType is required to be set when tokenAttributes is specified. Supported values are: {}", VALID_CACHE_TYPES.join(", ")));
            } else if !VALID_CACHE_TYPES.contains(&t.cache_type.as_str()) {
                errs.push(format!(
                    "{f}.cacheType: Unsupported value: {:?}: supported values: {}",
                    t.cache_type,
                    quote_list(&VALID_CACHE_TYPES)
                ));
            }
        }
    }
    errs
}

/// `RegisterCredentialProviderPlugins` (`plugin.go:143-190`) up to, but not
/// including, `newPluginProvider`: returns the validated providers and the
/// config hash. Every failure is fatal at the call site, as upstream.
pub fn register_credential_provider_plugins(
    config_path: &str,
    bin_dir: &str,
    sa_token_for_credential_providers: bool,
) -> Result<(CredentialProviderConfig, String)> {
    match std::fs::metadata(bin_dir) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(anyhow!("plugin binary directory {bin_dir} did not exist"))
        }
        Err(e) => return Err(anyhow!("error inspecting binary directory {bin_dir}: {e}")),
    }
    let (config, hash) = read_credential_provider_config(config_path)?;
    let errs = validate_credential_provider_config(&config, sa_token_for_credential_providers);
    if !errs.is_empty() {
        // `errs.ToAggregate()`: a lone error is its own message.
        let agg = if errs.len() == 1 {
            errs[0].clone()
        } else {
            format!("[{}]", errs.join(", "))
        };
        return Err(anyhow!(
            "failed to validate credential provider config: {agg}"
        ));
    }
    for p in &config.providers {
        let bin = Path::new(bin_dir).join(&p.name);
        match std::fs::metadata(&bin) {
            Ok(m)
                if m.is_file()
                    && std::os::unix::fs::PermissionsExt::mode(&m.permissions()) & 0o111 != 0 => {}
            Ok(_) => {
                return Err(anyhow!(
                    "error inspecting binary executable {}: permission denied",
                    bin.display()
                ))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(anyhow!(
                    "plugin binary executable {} did not exist",
                    bin.display()
                ))
            }
            Err(e) => {
                return Err(anyhow!(
                    "error inspecting binary executable {}: {e}",
                    bin.display()
                ))
            }
        }
    }
    Ok((config, hash))
}
