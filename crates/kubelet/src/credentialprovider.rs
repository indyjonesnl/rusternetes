//! Docker registry credentials for image pulls.
//!
//! Ports `pkg/credentialprovider` (`config.go` `DockerConfigEntry`
//! unmarshalling, `keyring.go` `BasicDockerKeyring` / `URLsMatch` /
//! `isDefaultRegistryMatch`) and `pkg/credentialprovider/secrets/secrets.go`
//! `MakeDockerKeyring`, as consumed by `images/image_manager.go`
//! `makeLookupPullCredentialsFunc`.
//!
//! Not ported: `TrackedAuthConfig` source/hash tracking (only used with the
//! `KubeletEnsureSecretPulledImages` gate), node-level docker config files
//! and the external credential provider plugins.

use anyhow::{anyhow, Result};
use base64::Engine;
use rusternetes_common::resources::Secret;
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};

/// `credentialprovider.AuthConfig` (mirrors the CRI `AuthConfig`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
    pub email: String,
}

#[derive(Deserialize, Default)]
struct EntryWithAuth {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    auth: String,
}

/// `DockerConfigEntry.UnmarshalJSON`: a non-empty `auth` field
/// (`base64(username:password)`) overrides `username`/`password`.
fn entry_to_auth(e: EntryWithAuth) -> Result<AuthConfig> {
    let mut cfg = AuthConfig {
        username: e.username,
        password: e.password,
        email: e.email,
    };
    if e.auth.is_empty() {
        return Ok(cfg);
    }
    (cfg.username, cfg.password) = decode_docker_config_field_auth(&e.auth)?;
    Ok(cfg)
}

/// `decodeDockerConfigFieldAuth` (`config.go`): padded input uses the
/// standard alphabet with padding, unpadded the raw one.
fn decode_docker_config_field_auth(field: &str) -> Result<(String, String)> {
    let engine = if field.trim().ends_with('=') {
        base64::engine::general_purpose::STANDARD
    } else {
        base64::engine::general_purpose::STANDARD_NO_PAD
    };
    let decoded = engine.decode(field)?;
    let decoded = String::from_utf8_lossy(&decoded);
    match decoded.split_once(':') {
        Some((u, p)) => Ok((u.to_string(), p.to_string())),
        None => Err(anyhow!(
            "unable to parse auth field, must be formatted as base64(username:password)"
        )),
    }
}

/// `DockerConfig`: registry location to credentials.
pub type DockerConfig = Vec<(String, AuthConfig)>;

/// `DockerConfigJSON` (`.dockerconfigjson`): the `auths` map.
fn parse_docker_config_json(bytes: &[u8]) -> Result<DockerConfig> {
    #[derive(Deserialize)]
    struct Json {
        #[serde(default)]
        auths: HashMap<String, EntryWithAuth>,
    }
    let j: Json = serde_json::from_slice(bytes)?;
    j.auths
        .into_iter()
        .map(|(k, v)| Ok((k, entry_to_auth(v)?)))
        .collect()
}

/// `DockerConfig` (`.dockercfg`): the bare registry map.
fn parse_dockercfg(bytes: &[u8]) -> Result<DockerConfig> {
    let m: HashMap<String, EntryWithAuth> = serde_json::from_slice(bytes)?;
    m.into_iter()
        .map(|(k, v)| Ok((k, entry_to_auth(v)?)))
        .collect()
}

const DEFAULT_REGISTRY_HOST: &str = "index.docker.io";

/// Split a schemeless URL into `(host, path)` (`ParseSchemelessURL`).
fn split_host_path(url: &str) -> (&str, &str) {
    match url.find('/') {
        Some(i) => (&url[..i], &url[i..]),
        None => (url, ""),
    }
}

/// `SplitURL`: host labels and port (empty when absent).
fn split_host_port(host: &str) -> (Vec<&str>, &str) {
    match host.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => {
            (h.split('.').collect(), p)
        }
        _ => (host.split('.').collect(), ""),
    }
}

/// `filepath.Match` for a single host label (`*`, `?` and literals).
fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    match pattern.split_first() {
        None => s.is_empty(),
        Some((b'*', rest)) => (0..=s.len()).any(|i| glob_match(rest, &s[i..])),
        Some((b'?', rest)) => !s.is_empty() && glob_match(rest, &s[1..]),
        Some((c, rest)) => s.first() == Some(c) && glob_match(rest, &s[1..]),
    }
}

/// `URLsMatchStr` / `URLsMatch`: the glob may wildcard host labels; the
/// credential path must be a prefix of the target's.
pub fn urls_match_str(glob: &str, target: &str) -> bool {
    let (gh, gp) = split_host_path(glob);
    let (th, tp) = split_host_path(target);
    let ((gparts, gport), (tparts, tport)) = (split_host_port(gh), split_host_port(th));
    gport == tport
        && gparts.len() == tparts.len()
        && tp.starts_with(gp)
        && gparts
            .iter()
            .zip(&tparts)
            .all(|(g, t)| glob_match(g.as_bytes(), t.as_bytes()))
}

/// `isDefaultRegistryMatch`: does the image pull from DockerHub.
fn is_default_registry_match(image: &str) -> bool {
    let (first, rest) = match image.split_once('/') {
        Some((f, r)) => (f, Some(r)),
        None => (image, None),
    };
    if first.is_empty() {
        return false;
    }
    if rest.is_none() || first == "docker.io" || first == "index.docker.io" {
        return true;
    }
    !first.contains(['.', ':'])
}

/// `BasicDockerKeyring`.
#[derive(Debug, Default)]
pub struct BasicDockerKeyring {
    /// Reverse-sorted so more specific paths match first.
    index: Vec<String>,
    creds: HashMap<String, Vec<AuthConfig>>,
}

impl BasicDockerKeyring {
    /// `BasicDockerKeyring.Add`.
    pub fn add(&mut self, cfg: DockerConfig) {
        let mut keys: BTreeSet<String> = self.index.iter().cloned().collect();
        for (loc, creds) in cfg {
            let stripped = loc
                .strip_prefix("https://")
                .or_else(|| loc.strip_prefix("http://"))
                .unwrap_or(&loc);
            let (host, path) = split_host_path(stripped);
            // The docker client treats /v1/ and /v2/ as the hostname.
            let path = if path.starts_with("/v2/") || path.starts_with("/v1/") {
                &path[3..]
            } else {
                path
            };
            let key = if !path.is_empty() && path != "/" {
                format!("{host}{path}")
            } else {
                host.to_string()
            };
            self.creds.entry(key.clone()).or_default().push(creds);
            keys.insert(key);
        }
        self.index = keys.into_iter().rev().collect();
    }

    /// `BasicDockerKeyring.Lookup`: every matching credential, most specific
    /// registry first; DockerHub credentials for unqualified images.
    pub fn lookup(&self, image: &str) -> Vec<AuthConfig> {
        let ret: Vec<AuthConfig> = self
            .index
            .iter()
            .filter(|k| urls_match_str(k, image))
            .flat_map(|k| self.creds[k].iter().cloned())
            .collect();
        if !ret.is_empty() {
            return ret;
        }
        if is_default_registry_match(image) {
            if let Some(c) = self.creds.get(DEFAULT_REGISTRY_HOST) {
                return c.clone();
            }
        }
        Vec::new()
    }
}

/// `secretsToTrackedDockerConfigs` + `MakeDockerKeyring` (no node keyring):
/// `None` when no secret carries a docker config.
pub fn make_docker_keyring(secrets: &[Secret]) -> Result<Option<BasicDockerKeyring>> {
    let mut keyring = BasicDockerKeyring::default();
    let mut found = 0;
    for s in secrets {
        let data = |k: &str| {
            s.data
                .as_ref()
                .and_then(|d| d.get(k))
                .filter(|b| !b.is_empty())
        };
        let ty = s.secret_type.as_deref();
        if let (Some("kubernetes.io/dockerconfigjson"), Some(b)) = (ty, data(".dockerconfigjson")) {
            keyring.add(parse_docker_config_json(b)?);
            found += 1;
        } else if let (Some("kubernetes.io/dockercfg"), Some(b)) = (ty, data(".dockercfg")) {
            keyring.add(parse_dockercfg(b)?);
            found += 1;
        }
    }
    Ok((found > 0).then_some(keyring))
}

/// `parsers.ParseImageName` repository part: `ParseNormalizedNamed(image).Name()`
/// (default `docker.io` domain and `library/` namespace, tag and digest dropped).
pub fn parse_image_repo(image: &str) -> Result<String> {
    if image.is_empty() || image.contains(char::is_whitespace) {
        return Err(anyhow!("couldn't parse image name {image:?}"));
    }
    let name = image.split('@').next().unwrap_or(image);
    let last = name.rfind('/').map_or(0, |i| i + 1);
    let name = match name[last..].rfind(':') {
        Some(i) => &name[..last + i],
        None => name,
    };
    let (domain, remainder) = match name.split_once('/') {
        Some((d, r)) if d.contains(['.', ':']) || d == "localhost" => (d, r.to_string()),
        _ => ("docker.io", name.to_string()),
    };
    let domain = if domain == "index.docker.io" {
        "docker.io"
    } else {
        domain
    };
    let remainder = if domain == "docker.io" && !remainder.contains('/') {
        format!("library/{remainder}")
    } else {
        remainder
    };
    Ok(format!("{domain}/{remainder}"))
}
