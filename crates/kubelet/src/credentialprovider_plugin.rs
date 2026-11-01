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
//! Also `plugin.go` `execPlugin` / `pluginProvider.provide` (matchImages,
//! response cache with per-response key type and expiry, singleflight, the
//! one minute exec timeout) and `plugins.go` (the process-wide provider
//! registry and `externalCredentialProviderKeyring`).
//!
//! Not ported yet: service account tokens (`serviceAccountProvider`, the
//! service-account part of the cache and singleflight keys) and the plugin
//! duration/error and config-hash metrics (`metrics.go`).

use crate::credentialprovider::{AuthConfig, DockerConfig};
use anyhow::{anyhow, Result};
use rusternetes_common::go_duration::{format_go_duration, parse_go_duration};
use rusternetes_common::validation::metav1::is_qualified_name;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

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

// ---- plugin.go: exec, cache, provide -------------------------------------

/// `credentialproviderapi.CredentialProviderResponse`, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialProviderResponse {
    pub cache_key_type: String,
    /// `CacheDuration` in nanoseconds; `None` when unset.
    pub cache_duration: Option<i64>,
    pub auth: BTreeMap<String, (String, String)>,
}

/// `Plugin` (`plugin.go`): the seam between `provide` and the exec.
#[async_trait::async_trait]
pub trait Plugin: Send + Sync {
    async fn exec_plugin(&self, image: &str) -> Result<CredentialProviderResponse, String>;
}

/// `execPlugin`'s timeout (`plugin.go`: `context.WithTimeout(ctx, 1*time.Minute)`).
const EXEC_PLUGIN_TIMEOUT: Duration = Duration::from_secs(60);

/// `execPlugin`.
pub struct ExecPlugin {
    pub name: String,
    pub api_version: String,
    pub plugin_bin_dir: PathBuf,
    pub args: Vec<String>,
    pub env_vars: Vec<ExecEnvVar>,
    pub timeout: Duration,
}

impl ExecPlugin {
    pub fn new(bin_dir: &str, p: &CredentialProvider) -> Self {
        Self {
            name: p.name.clone(),
            api_version: p.api_version.clone(),
            plugin_bin_dir: PathBuf::from(bin_dir),
            args: p.args.clone(),
            env_vars: p.env.clone(),
            timeout: EXEC_PLUGIN_TIMEOUT,
        }
    }

    /// `encodeRequest`: `CredentialProviderRequest` at the provider's version.
    fn encode_request(&self, image: &str) -> Vec<u8> {
        serde_json::json!({
            "kind": "CredentialProviderRequest",
            "apiVersion": self.api_version,
            "image": image,
        })
        .to_string()
        .into_bytes()
    }
}

/// `decodeResponse` (strict, like the scheme's `EnableStrict` codecs).
fn decode_response(data: &[u8]) -> Result<CredentialProviderResponse> {
    let v: JsonValue = serde_json::from_slice(data)?;
    let kind = v.get("kind").and_then(JsonValue::as_str).unwrap_or("");
    if kind != "CredentialProviderResponse" {
        return Err(anyhow!(
            "failed to decode CredentialProviderResponse, unexpected Kind: {kind:?}"
        ));
    }
    let group = v
        .get("apiVersion")
        .and_then(JsonValue::as_str)
        .and_then(|a| a.split_once('/'))
        .map_or("", |(g, _)| g);
    if group != "credentialprovider.kubelet.k8s.io" {
        return Err(anyhow!(
            "failed to decode CredentialProviderResponse, unexpected Group: {group}"
        ));
    }
    if let Some(m) = v.as_object() {
        for k in m.keys() {
            if ![
                "kind",
                "apiVersion",
                "cacheKeyType",
                "cacheDuration",
                "auth",
            ]
            .contains(&k.as_str())
            {
                return Err(anyhow!("strict decoding error: unknown field {k:?}"));
            }
        }
    }
    let cache_duration = match v.get("cacheDuration").and_then(JsonValue::as_str) {
        Some(d) => Some(parse_go_duration(d).map_err(|e| anyhow!(e))?),
        None => None,
    };
    let mut auth = BTreeMap::new();
    if let Some(m) = v.get("auth").and_then(JsonValue::as_object) {
        for (k, e) in m {
            let f = |n: &str| {
                e.get(n)
                    .and_then(JsonValue::as_str)
                    .unwrap_or("")
                    .to_string()
            };
            auth.insert(k.clone(), (f("username"), f("password")));
        }
    }
    Ok(CredentialProviderResponse {
        cache_key_type: v
            .get("cacheKeyType")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        cache_duration,
        auth,
    })
}

#[async_trait::async_trait]
impl Plugin for ExecPlugin {
    /// `execPlugin.ExecPlugin` + `runPlugin`.
    async fn exec_plugin(&self, image: &str) -> Result<CredentialProviderResponse, String> {
        use tokio::io::AsyncWriteExt;
        let data = self.encode_request(image);
        let mut cmd = tokio::process::Command::new(self.plugin_bin_dir.join(&self.name));
        cmd.args(&self.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // `mergeEnvVars(os.Environ(), configEnvVars)`: the configured vars are
        // appended, so they win over an inherited variable of the same name.
        for v in &self.env_vars {
            cmd.env(&v.name, &v.value);
        }
        let name = &self.name;
        let run = async {
            let mut child = cmd.spawn()?;
            if let Some(mut stdin) = child.stdin.take() {
                // The plugin may exit without reading; that surfaces as its status.
                let _ = stdin.write_all(&data).await;
            }
            child.wait_with_output().await
        };
        let out = match tokio::time::timeout(self.timeout, run).await {
            Err(_) => {
                return Err(format!(
                    "error execing credential provider plugin {name} for image {image}: context deadline exceeded: "
                ))
            }
            Ok(Err(e)) => {
                return Err(format!(
                    "error execing credential provider plugin {name} for image {image}: {e}: "
                ))
            }
            Ok(Ok(out)) => out,
        };
        if !out.status.success() {
            return Err(format!(
                "error execing credential provider plugin {name} for image {image}: {}: {}",
                exit_status_text(&out.status),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let v: JsonValue = serde_json::from_slice(&out.stdout)
            .map_err(|e| format!("error reading GVK from response: {e}"))?;
        let got = v
            .get("apiVersion")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        if got != self.api_version {
            return Err(format!(
                "apiVersion from credential plugin response did not match expected apiVersion:{}, actual apiVersion:{got}",
                self.api_version
            ));
        }
        decode_response(&out.stdout).map_err(|_| {
            "error decoding credential provider plugin response from stdout".to_string()
        })
    }
}

/// `exec.ExitError`'s text.
fn exit_status_text(s: &std::process::ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    match (s.code(), s.signal()) {
        (Some(c), _) => format!("exit status {c}"),
        (None, Some(sig)) => format!("signal: {sig}"),
        _ => s.to_string(),
    }
}

/// `parseRegistry`: the first `/`-separated part.
fn parse_registry(image: &str) -> &str {
    image.split('/').next().unwrap_or(image)
}

const GLOBAL_CACHE_KEY: &str = "<global>";
/// `cachePurgeInterval`.
const CACHE_PURGE_INTERVAL: Duration = Duration::from_secs(15 * 60);

struct CacheEntry {
    credentials: DockerConfig,
    expires_at: Instant,
}

type InFlight = Arc<tokio::sync::OnceCell<Result<CredentialProviderResponse, String>>>;

/// `pluginProvider`.
pub struct PluginProvider {
    pub name: String,
    clock: Arc<dyn Fn() -> Instant + Send + Sync>,
    match_images: Vec<String>,
    /// `cache.NewExpirationStore` keyed by cache key.
    cache: Mutex<HashMap<String, CacheEntry>>,
    default_cache_duration: Duration,
    last_cache_purge: Mutex<Instant>,
    plugin: Box<dyn Plugin>,
    /// `singleflight.Group`, keyed by image.
    in_flight: Mutex<HashMap<String, InFlight>>,
}

impl PluginProvider {
    /// `newPluginProvider`.
    pub fn new(bin_dir: &str, provider: &CredentialProvider) -> Self {
        Self::with_plugin(provider, Box::new(ExecPlugin::new(bin_dir, provider)))
    }

    pub fn with_plugin(provider: &CredentialProvider, plugin: Box<dyn Plugin>) -> Self {
        Self::with_plugin_and_clock(provider, plugin, Arc::new(Instant::now))
    }

    pub fn with_plugin_and_clock(
        provider: &CredentialProvider,
        plugin: Box<dyn Plugin>,
        clock: Arc<dyn Fn() -> Instant + Send + Sync>,
    ) -> Self {
        let now = clock();
        Self {
            name: provider.name.clone(),
            clock,
            match_images: provider.match_images.clone(),
            cache: Mutex::new(HashMap::new()),
            default_cache_duration: Duration::from_nanos(
                provider.default_cache_duration.unwrap_or(0).max(0) as u64,
            ),
            last_cache_purge: Mutex::new(now),
            plugin,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// The keys of the response cache (`cache.ListKeys`), sorted.
    pub fn cache_keys(&self) -> Vec<String> {
        let mut k: Vec<String> = self.cache.lock().unwrap().keys().cloned().collect();
        k.sort();
        k
    }

    /// `isImageAllowed`.
    fn is_image_allowed(&self, image: &str) -> bool {
        self.match_images
            .iter()
            .any(|m| crate::credentialprovider::urls_match_str(m, image))
    }

    /// `getCachedCredentials` (+ `lookupCredentialsInCache`): image, registry,
    /// then global key; an expired entry is a miss.
    fn get_cached_credentials(&self, image: &str) -> Option<DockerConfig> {
        let now = (self.clock)();
        {
            let mut last = self.last_cache_purge.lock().unwrap();
            if now > *last + CACHE_PURGE_INTERVAL {
                // `p.cache.List()` drops expired entries.
                self.cache
                    .lock()
                    .unwrap()
                    .retain(|_, e| now <= e.expires_at);
                *last = now;
            }
        }
        let cache = self.cache.lock().unwrap();
        [image, parse_registry(image), GLOBAL_CACHE_KEY]
            .iter()
            .find_map(|k| {
                cache
                    .get(*k)
                    .filter(|e| now <= e.expires_at)
                    .map(|e| e.credentials.clone())
            })
    }

    /// `pluginProvider.provide` without service accounts.
    pub async fn provide(&self, image: &str) -> Result<DockerConfig, String> {
        if !self.is_image_allowed(image) {
            return Ok(Vec::new());
        }
        if let Some(c) = self.get_cached_credentials(image) {
            return Ok(c);
        }
        // `p.group.Do(image, ExecPlugin)`: concurrent callers for one image
        // share a single exec.
        let cell: InFlight = self
            .in_flight
            .lock()
            .unwrap()
            .entry(image.to_string())
            .or_default()
            .clone();
        let res = cell
            .get_or_init(|| async { self.plugin.exec_plugin(image).await })
            .await
            .clone();
        self.in_flight.lock().unwrap().remove(image);
        let response = res.map_err(|e| {
            format!("failed to get credential from external registry credential provider: {e}")
        })?;

        let cache_key = match response.cache_key_type.as_str() {
            "Image" => image.to_string(),
            "Registry" => parse_registry(image).to_string(),
            "Global" => GLOBAL_CACHE_KEY.to_string(),
            other => {
                return Err(format!(
                    "credential provider plugin did not return a valid cacheKeyType: {other}"
                ))
            }
        };
        let docker_config: DockerConfig = response
            .auth
            .into_iter()
            .map(|(k, (username, password))| {
                (
                    k,
                    AuthConfig {
                        username,
                        password,
                        email: String::new(),
                    },
                )
            })
            .collect();

        // A zero response duration means "do not cache"; an unset one uses the
        // provider default, where zero also means do not cache.
        let ttl = match response.cache_duration {
            Some(0) => return Ok(docker_config),
            Some(d) => Duration::from_nanos(d.max(0) as u64),
            None if self.default_cache_duration.is_zero() => return Ok(docker_config),
            None => self.default_cache_duration,
        };
        self.cache.lock().unwrap().insert(
            cache_key,
            CacheEntry {
                credentials: docker_config.clone(),
                expires_at: (self.clock)() + ttl,
            },
        );
        Ok(docker_config)
    }

    /// `perPodPluginProvider.provideWithCoordinates`: errors are logged and
    /// yield an empty config.
    async fn provide_or_empty(&self, image: &str) -> DockerConfig {
        match self.provide(image).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    "Failed to provide credentials for image: {e} (provider {}, image {image})",
                    self.name
                );
                Vec::new()
            }
        }
    }
}

// ---- plugins.go: registry and keyring ------------------------------------

static PROVIDERS: RwLock<Vec<(String, Arc<PluginProvider>)>> = RwLock::new(Vec::new());

/// `registerCredentialProviderPlugin`: a name may be registered once.
pub fn register_credential_provider_plugin(name: &str, p: Arc<PluginProvider>) -> Result<()> {
    let mut providers = PROVIDERS.write().unwrap_or_else(|e| e.into_inner());
    if providers.iter().any(|(n, _)| n == name) {
        return Err(anyhow!("Credential provider {name:?} was registered twice"));
    }
    providers.push((name.to_string(), p));
    tracing::debug!("Registered credential provider {name}");
    Ok(())
}

/// The registration loop of `RegisterCredentialProviderPlugins`
/// (`newPluginProvider` + `registerCredentialProviderPlugin`), after
/// [`register_credential_provider_plugins`] validated the config.
pub fn register_validated_providers(
    bin_dir: &str,
    config: &CredentialProviderConfig,
) -> Result<()> {
    for p in &config.providers {
        register_credential_provider_plugin(&p.name, Arc::new(PluginProvider::new(bin_dir, p)))?;
    }
    Ok(())
}

/// `externalCredentialProviderKeyring`.
pub struct ExternalCredentialProviderKeyring {
    providers: Vec<Arc<PluginProvider>>,
}

/// `NewExternalCredentialProviderDockerKeyring` (without the per-pod service
/// account arguments): a keyring over every registered provider.
pub fn new_external_credential_provider_docker_keyring() -> ExternalCredentialProviderKeyring {
    ExternalCredentialProviderKeyring {
        providers: PROVIDERS
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(_, p)| p.clone())
            .collect(),
    }
}

impl ExternalCredentialProviderKeyring {
    pub fn from_providers(providers: Vec<Arc<PluginProvider>>) -> Self {
        Self { providers }
    }

    /// `externalCredentialProviderKeyring.Lookup`.
    pub async fn lookup(&self, image: &str) -> Vec<AuthConfig> {
        let mut keyring = crate::credentialprovider::BasicDockerKeyring::default();
        for p in &self.providers {
            keyring.add(p.provide_or_empty(image).await);
        }
        keyring.lookup(image)
    }
}
