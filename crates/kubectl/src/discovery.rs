use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::Value;
use std::collections::HashMap;

/// A resolved mapping from a resource (by any of its names) to the
/// information every kubectl verb needs to build an API path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMapping {
    pub group: String,   // "" for the core group
    pub version: String, // preferred version, e.g. "v1"
    pub kind: String,    // "Deployment"
    pub plural: String,  // "deployments"
    pub singular: String,
    pub namespaced: bool,
    pub verbs: Vec<String>,
    pub short_names: Vec<String>,
}

/// Parse an aggregated-discovery (`APIGroupDiscoveryList`) JSON document into a
/// flat list of resource mappings. Subresources (those whose `resource` name
/// contains '/') are skipped — they are not independently addressable kinds.
pub fn parse_aggregated_discovery(doc: &Value) -> Result<Vec<ResourceMapping>> {
    let mut out = Vec::new();
    let Some(items) = doc.get("items").and_then(|i| i.as_array()) else {
        return Ok(out);
    };
    for group in items {
        let group_name = group
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let Some(versions) = group.get("versions").and_then(|v| v.as_array()) else {
            continue;
        };
        for ver in versions {
            let version = ver.get("version").and_then(|v| v.as_str()).unwrap_or("");
            let Some(resources) = ver.get("resources").and_then(|r| r.as_array()) else {
                continue;
            };
            for r in resources {
                let plural = r.get("resource").and_then(|v| v.as_str()).unwrap_or("");
                if plural.is_empty() || plural.contains('/') {
                    continue; // skip subresources / malformed
                }
                let kind = r
                    .pointer("/responseKind/kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if kind.is_empty() {
                    continue; // skip entries with no kind (garbage mapping / empty API path)
                }
                let namespaced = r.get("scope").and_then(|v| v.as_str()) == Some("Namespaced");
                let singular = r
                    .get("singularResource")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .unwrap_or_else(|| kind.to_lowercase());
                let short_names = r
                    .get("shortNames")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let verbs = r
                    .get("verbs")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                out.push(ResourceMapping {
                    group: group_name.clone(),
                    version: version.to_string(),
                    kind,
                    plural: plural.to_string(),
                    singular,
                    namespaced,
                    verbs,
                    short_names,
                });
            }
        }
    }
    Ok(out)
}

/// Resolves a user-supplied resource reference (plural, singular, short name,
/// or Kind) to a single `ResourceMapping`.
pub struct RestMapper {
    mappings: Vec<ResourceMapping>,
    by_key: HashMap<String, usize>, // lowercased key -> index, first writer wins
}

impl RestMapper {
    pub fn new(mut mappings: Vec<ResourceMapping>) -> Self {
        // Order so the core group is preferred on key collisions: sort with
        // core ("") first. Insertion is first-writer-wins, so core entries
        // claim shared keys (e.g. "events", "Event") ahead of grouped ones.
        mappings.sort_by(|a, b| {
            (!a.group.is_empty())
                .cmp(&(!b.group.is_empty()))
                .then_with(|| a.group.cmp(&b.group))
                .then_with(|| a.plural.cmp(&b.plural))
        });
        let mut by_key = HashMap::new();
        for (i, m) in mappings.iter().enumerate() {
            let mut keys = vec![
                m.plural.to_lowercase(),
                m.singular.to_lowercase(),
                m.kind.to_lowercase(),
            ];
            keys.extend(m.short_names.iter().map(|s| s.to_lowercase()));
            for k in keys {
                by_key.entry(k).or_insert(i); // first writer (core) wins
            }
        }
        Self { mappings, by_key }
    }

    /// Resolve by plural / singular / short-name / Kind (case-insensitive).
    pub fn resolve(&self, reference: &str) -> Option<&ResourceMapping> {
        self.by_key
            .get(&reference.to_lowercase())
            .map(|&i| &self.mappings[i])
    }

    /// Returns all mappings in preference order (core group first).
    // Only the discovery parity test needs the full resolved set.
    pub fn all(&self) -> &[ResourceMapping] {
        &self.mappings
    }

    const AGG_ACCEPT: &'static str =
        "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json";

    /// Build a mapper from the api-server's aggregated discovery. Fetches both
    /// the core group (`/api`) and all named groups (`/apis`) — one HTTP call
    /// each — and merges them.
    pub async fn from_server(client: &ApiClient) -> anyhow::Result<Self> {
        let core = client
            .get_raw_with_accept("/api", Self::AGG_ACCEPT)
            .await
            .context("unable to fetch core API discovery")?;
        let apis = client
            .get_raw_with_accept("/apis", Self::AGG_ACCEPT)
            .await
            .context("unable to fetch API group discovery")?;
        let mut mappings = parse_aggregated_discovery(&core)?;
        mappings.extend(parse_aggregated_discovery(&apis)?);
        Ok(Self::new(mappings))
    }
}
