//! The `PodNodeSelector` and `PodTolerationRestriction` admission plugins.
//!
//! Ported from `plugin/pkg/admission/podnodeselector/admission.go`
//! (`Plugin.Admit`/`Validate`, `readConfig`, `getNodeSelectorMap`,
//! `isSubset`) and `plugin/pkg/admission/podtolerationrestriction/`
//! (`admission.go`: `Plugin.Admit`/`Validate`, `extractNSTolerations`;
//! `config.go`: `loadConfiguration`; `apis/.../validation/validation.go`:
//! `ValidateConfiguration`), with `pkg/util/tolerations/tolerations.go`
//! (`MergeTolerations`, `VerifyAgainstWhitelist`, `isSuperset`) and
//! `staging/src/k8s.io/apimachinery/pkg/labels` (`Conflicts`, `Merge`,
//! `ConvertSelectorToLabelsMap`), release-1.35.
//!
//! Upstream reads the namespace through an informer lister and falls back to a
//! live GET before answering NotFound; here the one read is the storage GET,
//! which is the live read, so the `WaitForReady` gate has nothing to wait on.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use rusternetes_common::qos::{get_pod_qos, QoSClass};
use rusternetes_common::resources::pod::Toleration;
use rusternetes_common::resources::{Namespace, Pod};
use rusternetes_common::validation::field::Path;
use rusternetes_common::validation::metav1::{is_qualified_name, is_valid_label_value};
use rusternetes_common::validation::pod::validate_tolerations_with_options;
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, Storage, StorageBackend};

use super::builtin_plugins::new_forbidden;
use super::plugins::enabled;

/// `NamespaceNodeSelectors` (podnodeselector/admission.go:41).
pub const NAMESPACE_NODE_SELECTORS: &[&str] = &["scheduler.alpha.kubernetes.io/node-selector"];

/// `NSDefaultTolerations` / `NSWLTolerations` (podtolerationrestriction/
/// admission.go:63-66).
pub const NS_DEFAULT_TOLERATIONS: &str = "scheduler.alpha.kubernetes.io/defaultTolerations";
pub const NS_WL_TOLERATIONS: &str = "scheduler.alpha.kubernetes.io/tolerationsWhitelist";

/// `v1.TaintNodeMemoryPressure`.
pub const TAINT_NODE_MEMORY_PRESSURE: &str = "node.kubernetes.io/memory-pressure";

/// A Go `labels.Set`; ordered so error text is deterministic.
pub type Set = BTreeMap<String, String>;

// ---------------------------------------------------------------------------
// labels helpers (apimachinery/pkg/labels/labels.go:106-183)
// ---------------------------------------------------------------------------

/// `labels.Conflicts`: a key present in both sets with different values.
pub fn conflicts(a: &Set, b: &Set) -> bool {
    let (small, big) = if b.len() < a.len() { (b, a) } else { (a, b) };
    small
        .iter()
        .any(|(k, v)| big.get(k).is_some_and(|val| val != v))
}

/// `labels.Merge`: the second set wins on conflict.
pub fn merge(a: &Set, b: &Set) -> Set {
    let mut m = a.clone();
    m.extend(b.iter().map(|(k, v)| (k.clone(), v.clone())));
    m
}

/// `labels.ConvertSelectorToLabelsMap` with no path options.
pub fn convert_selector_to_labels_map(selector: &str) -> std::result::Result<Set, String> {
    let mut out = Set::new();
    if selector.is_empty() {
        return Ok(out);
    }
    for label in selector.split(',') {
        let l: Vec<&str> = label.split('=').collect();
        if l.len() != 2 {
            // `fmt.Errorf("invalid selector: %s", l)` with `l` a []string.
            return Err(format!("invalid selector: [{}]", l.join(" ")));
        }
        let key = l[0].trim();
        let errs = is_qualified_name(key);
        if !errs.is_empty() {
            return Err(format!("Invalid value: \"{key}\": {}", errs.join("; ")));
        }
        let value = l[1].trim();
        let errs = is_valid_label_value(value);
        if !errs.is_empty() {
            return Err(format!("Invalid value: \"{value}\": {}", errs.join("; ")));
        }
        out.insert(key.to_string(), value.to_string());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// namespace lookup
// ---------------------------------------------------------------------------

/// `getNamespaceNodeSelectorMap` / `getNamespace`: the namespace, NotFound as
/// is, any other failure as an InternalError.
async fn get_namespace(storage: &StorageBackend, name: &str) -> Result<Namespace> {
    match storage
        .get::<Namespace>(&build_key("namespaces", None, name))
        .await
    {
        Ok(ns) => Ok(ns),
        Err(Error::NotFound(_)) => Err(Error::NotFound(format!("namespaces \"{name}\" not found"))),
        Err(e) => Err(Error::Internal(e.to_string())),
    }
}

// ---------------------------------------------------------------------------
// PodNodeSelector
// ---------------------------------------------------------------------------

/// `pluginConfig.PodNodeSelectorPluginConfig`: the cluster default selector
/// under `clusterDefaultNodeSelector` and per-namespace whitelists.
pub type NodeSelectorConfig = HashMap<String, String>;

/// `readConfig` (podnodeselector/admission.go:76-92). Upstream's decode loop
/// `continue`s past a failed document; a document that does not decode is
/// reported here instead of looped on.
pub fn read_node_selector_config(text: &str) -> std::result::Result<NodeSelectorConfig, String> {
    if text.trim().is_empty() {
        return Ok(NodeSelectorConfig::new());
    }
    let doc: serde_json::Value = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    // encoding/json matches field names case-insensitively.
    let Some(map) = doc.as_object().and_then(|o| {
        o.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("podNodeSelectorPluginConfig"))
            .map(|(_, v)| v)
    }) else {
        return Ok(NodeSelectorConfig::new());
    };
    if map.is_null() {
        return Ok(NodeSelectorConfig::new());
    }
    serde_json::from_value(map.clone()).map_err(|e| e.to_string())
}

static NODE_SELECTOR_CONFIG: OnceLock<NodeSelectorConfig> = OnceLock::new();
static TOLERATION_CONFIG: OnceLock<TolerationConfiguration> = OnceLock::new();

/// `getNodeSelectorMap` (podnodeselector/admission.go:207-236).
fn get_node_selector_map(
    ns: &Namespace,
    cluster: &NodeSelectorConfig,
) -> std::result::Result<Set, String> {
    let mut selector = Set::new();
    let mut found = false;
    if let Some(annotations) = ns.metadata.annotations.as_ref().filter(|a| !a.is_empty()) {
        for annotation in NAMESPACE_NODE_SELECTORS {
            if let Some(v) = annotations.get(*annotation) {
                let labels_map = convert_selector_to_labels_map(v)?;
                if conflicts(&selector, &labels_map) {
                    return Err(format!(
                        "{} annotations' node label selectors conflict",
                        ns.metadata.name
                    ));
                }
                selector = merge(&selector, &labels_map);
                found = true;
            }
        }
    }
    if !found {
        selector = convert_selector_to_labels_map(
            cluster
                .get("clusterDefaultNodeSelector")
                .map(String::as_str)
                .unwrap_or(""),
        )?;
    }
    Ok(selector)
}

/// `isSubset` (podnodeselector/admission.go:262-279).
pub fn is_subset(sub: &Set, sup: &Set) -> bool {
    if sup.is_empty() {
        return true;
    }
    sub.iter().all(|(k, v)| sup.get(k) == Some(v))
}

fn pod_node_selector(pod: &Pod) -> Set {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_selector.as_ref())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

/// `Plugin.Admit` (podnodeselector/admission.go:96-123): CREATE of a pod, the
/// namespace's selector merged into the pod's, then `Validate`.
pub async fn pod_node_selector_admit(
    storage: &StorageBackend,
    namespace: &str,
    pod: &mut Pod,
) -> Result<()> {
    pod_node_selector_admit_with(storage, namespace, pod, installed_node_selector_config()).await
}

pub async fn pod_node_selector_admit_with(
    storage: &StorageBackend,
    namespace: &str,
    pod: &mut Pod,
    cluster: &NodeSelectorConfig,
) -> Result<()> {
    let ns = get_namespace(storage, namespace).await?;
    let ns_selector = get_node_selector_map(&ns, cluster).map_err(Error::unknown_error)?;
    let pod_selector = pod_node_selector(pod);
    if conflicts(&ns_selector, &pod_selector) {
        return Err(new_forbidden(
            "pods",
            &pod.metadata.name,
            "pod node label selector conflicts with its namespace node label selector",
        ));
    }
    let merged = merge(&ns_selector, &pod_selector);
    if let Some(spec) = pod.spec.as_mut() {
        spec.node_selector = Some(merged.into_iter().collect());
    }
    pod_node_selector_validate_with(storage, namespace, pod, cluster).await
}

/// `Plugin.Validate` (podnodeselector/admission.go:125-155).
pub async fn pod_node_selector_validate(
    storage: &StorageBackend,
    namespace: &str,
    pod: &Pod,
) -> Result<()> {
    pod_node_selector_validate_with(storage, namespace, pod, installed_node_selector_config()).await
}

pub async fn pod_node_selector_validate_with(
    storage: &StorageBackend,
    namespace: &str,
    pod: &Pod,
    cluster: &NodeSelectorConfig,
) -> Result<()> {
    let ns = get_namespace(storage, namespace).await?;
    let ns_selector = get_node_selector_map(&ns, cluster).map_err(Error::unknown_error)?;
    let pod_selector = pod_node_selector(pod);
    if conflicts(&ns_selector, &pod_selector) {
        return Err(new_forbidden(
            "pods",
            &pod.metadata.name,
            "pod node label selector conflicts with its namespace node label selector",
        ));
    }
    // whitelist verification
    let whitelist =
        convert_selector_to_labels_map(cluster.get(namespace).map(String::as_str).unwrap_or(""))
            .map_err(Error::unknown_error)?;
    if !is_subset(&pod_selector, &whitelist) {
        return Err(new_forbidden(
            "pods",
            &pod.metadata.name,
            "pod node label selector labels conflict with its namespace whitelist",
        ));
    }
    Ok(())
}

fn installed_node_selector_config() -> &'static NodeSelectorConfig {
    static EMPTY: OnceLock<NodeSelectorConfig> = OnceLock::new();
    NODE_SELECTOR_CONFIG
        .get()
        .unwrap_or_else(|| EMPTY.get_or_init(NodeSelectorConfig::new))
}

// ---------------------------------------------------------------------------
// PodTolerationRestriction
// ---------------------------------------------------------------------------

/// The plugin's `Configuration` (apis/podtolerationrestriction/types.go,
/// v1alpha1): cluster level default tolerations and whitelist.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TolerationConfiguration {
    #[serde(default)]
    pub default: Vec<Toleration>,
    #[serde(default)]
    pub whitelist: Vec<Toleration>,
}

/// `loadConfiguration` (podtolerationrestriction/config.go:40-72) +
/// `ValidateConfiguration` (validation/validation.go:28-37). The document must
/// be a `Configuration` of `podtolerationrestriction.admission.k8s.io/v1alpha1`.
pub fn load_toleration_configuration(
    text: &str,
) -> std::result::Result<TolerationConfiguration, String> {
    let doc: serde_json::Value = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    let str_of = |k: &str| doc.get(k).and_then(|v| v.as_str()).unwrap_or("");
    let (api_version, kind) = (str_of("apiVersion"), str_of("kind"));
    if kind.is_empty() {
        return Err("Object 'Kind' is missing".to_string());
    }
    if api_version.is_empty() {
        return Err("Object 'apiVersion' is missing".to_string());
    }
    if api_version != "podtolerationrestriction.admission.k8s.io/v1alpha1"
        || kind != "Configuration"
    {
        return Err(format!(
            "no kind \"{kind}\" is registered for version \"{api_version}\""
        ));
    }
    let cfg: TolerationConfiguration = serde_json::from_value(doc).map_err(|e| e.to_string())?;
    let fld = Path::new("podtolerationrestriction");
    let mut errs = validate_tolerations_with_options(&cfg.default, &fld.child("default"), false);
    errs.extend(validate_tolerations_with_options(
        &cfg.whitelist,
        &fld.child("whitelist"),
        false,
    ));
    if !errs.is_empty() {
        return Err(format!("invalid config: {errs:?}"));
    }
    Ok(cfg)
}

fn installed_toleration_config() -> &'static TolerationConfiguration {
    static EMPTY: OnceLock<TolerationConfiguration> = OnceLock::new();
    TOLERATION_CONFIG
        .get()
        .unwrap_or_else(|| EMPTY.get_or_init(TolerationConfiguration::default))
}

/// Read both plugins' configuration from the `--admission-control-config-file`
/// document, once at startup. Each is read only when its plugin is enabled, as
/// upstream constructs a plugin (and so reads its config) only then; the legacy
/// unversioned `podNodeSelectorPluginConfig` file is routed to PodNodeSelector
/// by [`super::config::plugin_configuration_for`] (config.go:70-125).
pub fn install_from_admission_configuration(
    yaml: &str,
    config_file: &std::path::Path,
) -> std::result::Result<(), String> {
    if enabled("PodNodeSelector") {
        let cfg =
            match super::config::plugin_configuration_for(yaml, config_file, "PodNodeSelector")? {
                Some(text) => read_node_selector_config(&text)?,
                None => NodeSelectorConfig::new(),
            };
        let _ = NODE_SELECTOR_CONFIG.set(cfg);
    }
    if enabled("PodTolerationRestriction") {
        let cfg = match super::config::plugin_configuration_for(
            yaml,
            config_file,
            "PodTolerationRestriction",
        )? {
            Some(text) => load_toleration_configuration(&text)?,
            None => TolerationConfiguration::default(),
        };
        let _ = TOLERATION_CONFIG.set(cfg);
    }
    Ok(())
}

/// `apiequality.Semantic.DeepEqual` on two tolerations: an absent string and
/// an empty one are the same value after Go decoding.
fn tol_eq(a: &Toleration, b: &Toleration) -> bool {
    let s = |o: &Option<String>| o.clone().unwrap_or_default();
    s(&a.key) == s(&b.key)
        && s(&a.operator) == s(&b.operator)
        && s(&a.value) == s(&b.value)
        && s(&a.effect) == s(&b.effect)
        && a.toleration_seconds == b.toleration_seconds
}

/// `isSuperset` (tolerations.go:82-117): whether `ss` tolerates a superset of
/// what `t` does.
fn is_superset(ss: &Toleration, t: &Toleration) -> bool {
    if tol_eq(t, ss) {
        return true;
    }
    let s = |o: &Option<String>| o.as_deref().unwrap_or("").to_string();
    let (ss_key, ss_op, ss_effect) = (s(&ss.key), s(&ss.operator), s(&ss.effect));
    if s(&t.key) != ss_key
        // An empty key with Exists operator means match all keys & values.
        && (!ss_key.is_empty() || ss_op != "Exists")
    {
        return false;
    }
    // An empty effect means match all effects.
    if s(&t.effect) != ss_effect && !ss_effect.is_empty() {
        return false;
    }
    if ss_effect == "NoExecute" {
        if let Some(ss_secs) = ss.toleration_seconds {
            match t.toleration_seconds {
                None => return false,
                Some(t_secs) if t_secs > ss_secs => return false,
                _ => {}
            }
        }
    }
    match ss_op.as_str() {
        // empty operator means Equal
        "Equal" | "" => s(&t.operator) == "Equal" && s(&t.value) == s(&ss.value),
        "Exists" => true,
        _ => false,
    }
}

/// `VerifyAgainstWhitelist` (tolerations.go:30-49).
pub fn verify_against_whitelist(tolerations: &[Toleration], whitelist: &[Toleration]) -> bool {
    if whitelist.is_empty() || tolerations.is_empty() {
        return true;
    }
    tolerations
        .iter()
        .all(|t| whitelist.iter().any(|w| is_superset(w, t)))
}

/// `MergeTolerations` (tolerations.go:53-80): if one toleration is a superset
/// of another, only the superset is kept.
pub fn merge_tolerations(first: &[Toleration], second: &[Toleration]) -> Vec<Toleration> {
    let all: Vec<&Toleration> = first.iter().chain(second.iter()).collect();
    let mut merged: Vec<Toleration> = Vec::new();
    'next: for (i, t) in all.iter().enumerate() {
        for t2 in &merged {
            if is_superset(t2, t) {
                continue 'next; // t is redundant; ignore it
            }
        }
        for t2 in &all[i + 1..] {
            // If the tolerations are equal, prefer the first.
            if !tol_eq(t, t2) && is_superset(t2, t) {
                continue 'next;
            }
        }
        merged.push((*t).clone());
    }
    merged
}

/// `extractNSTolerations` (podtolerationrestriction/admission.go:236-276):
/// `None` when the annotation is unset, an empty list when set to the empty
/// string.
fn extract_ns_tolerations(
    ns: &Namespace,
    key: &str,
) -> std::result::Result<Option<Vec<Toleration>>, String> {
    let Some(v) = ns.metadata.annotations.as_ref().and_then(|a| a.get(key)) else {
        return Ok(None);
    };
    if v.is_empty() {
        return Ok(Some(Vec::new()));
    }
    // `json.Unmarshal` of `null` leaves a nil slice, which becomes an empty one.
    let ts: Option<Vec<Toleration>> = serde_json::from_str(v).map_err(|e| e.to_string())?;
    Ok(Some(ts.unwrap_or_default()))
}

fn pod_tolerations(pod: &Pod) -> Vec<Toleration> {
    pod.spec
        .as_ref()
        .and_then(|s| s.tolerations.clone())
        .unwrap_or_default()
}

/// `Plugin.Admit` (podtolerationrestriction/admission.go:96-137): on CREATE the
/// namespace's (else the cluster's) default tolerations, plus a memory-pressure
/// toleration for a non-BestEffort pod, merged into the pod's; then `Validate`.
pub async fn toleration_restriction_admit(
    storage: &StorageBackend,
    namespace: &str,
    create: bool,
    pod: &mut Pod,
) -> Result<()> {
    toleration_restriction_admit_with(
        storage,
        namespace,
        create,
        pod,
        installed_toleration_config(),
    )
    .await
}

pub async fn toleration_restriction_admit_with(
    storage: &StorageBackend,
    namespace: &str,
    create: bool,
    pod: &mut Pod,
    config: &TolerationConfiguration,
) -> Result<()> {
    let mut extra: Vec<Toleration> = Vec::new();
    if create {
        let ns = get_namespace(storage, namespace).await?;
        let ts =
            extract_ns_tolerations(&ns, NS_DEFAULT_TOLERATIONS).map_err(Error::unknown_error)?;
        // If the namespace has not specified its default tolerations, fall back
        // to cluster's default tolerations.
        extra = ts.unwrap_or_else(|| config.default.clone());
    }
    if get_pod_qos(pod) != QoSClass::BestEffort {
        extra.push(Toleration {
            key: Some(TAINT_NODE_MEMORY_PRESSURE.to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        });
    }
    // Final merge of tolerations irrespective of pod type.
    if !extra.is_empty() {
        if let Some(spec) = pod.spec.as_mut() {
            let current = spec.tolerations.clone().unwrap_or_default();
            spec.tolerations = Some(merge_tolerations(&current, &extra));
        }
    }
    toleration_restriction_validate_with(storage, namespace, pod, config).await
}

/// `Plugin.Validate` (podtolerationrestriction/admission.go:139-171): the pod's
/// tolerations must satisfy the namespace's, else the cluster's, whitelist.
pub async fn toleration_restriction_validate(
    storage: &StorageBackend,
    namespace: &str,
    pod: &Pod,
) -> Result<()> {
    toleration_restriction_validate_with(storage, namespace, pod, installed_toleration_config())
        .await
}

pub async fn toleration_restriction_validate_with(
    storage: &StorageBackend,
    namespace: &str,
    pod: &Pod,
    config: &TolerationConfiguration,
) -> Result<()> {
    let tolerations = pod_tolerations(pod);
    if tolerations.is_empty() {
        return Ok(());
    }
    let ns = get_namespace(storage, namespace).await?;
    let mut scope = "namespace";
    let mut whitelist =
        extract_ns_tolerations(&ns, NS_WL_TOLERATIONS).map_err(Error::unknown_error)?;
    // If the namespace has not specified its tolerations whitelist, fall back
    // to cluster's whitelist of tolerations.
    if whitelist.is_none() {
        whitelist = Some(config.whitelist.clone());
        scope = "cluster";
    }
    let whitelist = whitelist.unwrap_or_default();
    if !whitelist.is_empty() && !verify_against_whitelist(&tolerations, &whitelist) {
        // A plain `fmt.Errorf`: HTTP 500, reason Unknown.
        return Err(Error::unknown_error(format!(
            "pod tolerations (possibly merged with namespace default tolerations) conflict with its {scope} whitelist"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ExtendedResourceToleration
// ---------------------------------------------------------------------------

/// `ExtendedResourceToleration.Admit`
/// (plugin/pkg/admission/extendedresourcetoleration/admission.go:56-95): for
/// each extended resource a container (or init container) requests, add a
/// `<resource>:Exists:NoSchedule` toleration. The caller gates on Create/Update
/// of the `pods` resource with no subresource. Names are visited sorted
/// (`resources.List()`), and `AddOrUpdateTolerationInPod`
/// (pkg/apis/core/helper/helpers.go:411-433) replaces a toleration with the same
/// key/effect/operator/value, else appends.
pub fn extended_resource_toleration_admit(pod: &mut Pod) {
    let Some(spec) = pod.spec.as_mut() else {
        return;
    };
    let mut resources = std::collections::BTreeSet::new();
    for c in spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
    {
        if let Some(requests) = c.resources.as_ref().and_then(|r| r.requests.as_ref()) {
            for name in requests.keys() {
                if rusternetes_common::quota::is_extended_resource_name(name) {
                    resources.insert(name.clone());
                }
            }
        }
    }
    for resource in resources {
        let new = Toleration {
            key: Some(resource),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        };
        let tolerations = spec.tolerations.get_or_insert_with(Vec::new);
        let s = |o: &Option<String>| o.clone().unwrap_or_default();
        match tolerations.iter_mut().find(|t| {
            s(&t.key) == s(&new.key)
                && s(&t.effect) == s(&new.effect)
                && s(&t.operator) == s(&new.operator)
                && s(&t.value) == s(&new.value)
        }) {
            Some(existing) => *existing = new,
            None => tolerations.push(new),
        }
    }
}
