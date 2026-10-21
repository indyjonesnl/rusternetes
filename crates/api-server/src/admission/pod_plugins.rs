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
const TAINT_NODE_MEMORY_PRESSURE: &str = "node.kubernetes.io/memory-pressure";

/// A Go `labels.Set`; ordered so error text is deterministic.
type Set = BTreeMap<String, String>;

// ---------------------------------------------------------------------------
// labels helpers (apimachinery/pkg/labels/labels.go:106-183)
// ---------------------------------------------------------------------------

/// `labels.Conflicts`: a key present in both sets with different values.
fn conflicts(a: &Set, b: &Set) -> bool {
    let (small, big) = if b.len() < a.len() { (b, a) } else { (a, b) };
    small
        .iter()
        .any(|(k, v)| big.get(k).is_some_and(|val| val != v))
}

/// `labels.Merge`: the second set wins on conflict.
fn merge(a: &Set, b: &Set) -> Set {
    let mut m = a.clone();
    m.extend(b.iter().map(|(k, v)| (k.clone(), v.clone())));
    m
}

/// `labels.ConvertSelectorToLabelsMap` with no path options.
fn convert_selector_to_labels_map(selector: &str) -> std::result::Result<Set, String> {
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
fn is_subset(sub: &Set, sup: &Set) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;

    fn tol(key: &str, op: &str, value: &str, effect: &str, secs: Option<i64>) -> Toleration {
        let o = |s: &str| (!s.is_empty()).then(|| s.to_string());
        Toleration {
            key: o(key),
            operator: o(op),
            value: o(value),
            effect: o(effect),
            toleration_seconds: secs,
        }
    }

    // pkg/util/tolerations/tolerations_test.go TestVerifyAgainstWhitelist
    #[test]
    fn whitelist_verification() {
        let ok = |t: Vec<Toleration>, w: Vec<Toleration>| verify_against_whitelist(&t, &w);
        // empty whitelist / empty tolerations pass
        assert!(ok(
            vec![tol("foo", "Equal", "bar", "NoSchedule", None)],
            vec![]
        ));
        assert!(ok(
            vec![],
            vec![tol("foo", "Equal", "bar", "NoSchedule", None)]
        ));
        // exact match
        let t = tol("foo", "Equal", "bar", "NoSchedule", None);
        assert!(ok(vec![t.clone()], vec![t.clone()]));
        // different value
        assert!(!ok(
            vec![tol("foo", "Equal", "baz", "NoSchedule", None)],
            vec![t.clone()]
        ));
        // Exists on the whitelist covers every value of the key
        assert!(ok(
            vec![t.clone()],
            vec![tol("foo", "Exists", "", "NoSchedule", None)]
        ));
        // empty key + Exists matches all keys; empty effect matches all effects
        assert!(ok(vec![t.clone()], vec![tol("", "Exists", "", "", None)]));
        // whitelist NoExecute seconds bound the pod's
        let wl = tol("foo", "Exists", "", "NoExecute", Some(60));
        assert!(ok(
            vec![tol("foo", "Exists", "", "NoExecute", Some(30))],
            vec![wl.clone()]
        ));
        assert!(!ok(
            vec![tol("foo", "Exists", "", "NoExecute", Some(120))],
            vec![wl.clone()]
        ));
        assert!(!ok(
            vec![tol("foo", "Exists", "", "NoExecute", None)],
            vec![wl]
        ));
    }

    // TestMergeTolerations
    #[test]
    fn merge_keeps_supersets_only() {
        let narrow = tol("foo", "Equal", "bar", "NoSchedule", None);
        let wide = tol("foo", "Exists", "", "NoSchedule", None);
        let merged = merge_tolerations(std::slice::from_ref(&narrow), std::slice::from_ref(&wide));
        assert_eq!(merged, vec![wide.clone()]);
        let merged = merge_tolerations(std::slice::from_ref(&wide), &[narrow]);
        assert_eq!(merged, vec![wide.clone()]);
        // equal tolerations: the first is preferred, one copy kept
        assert_eq!(
            merge_tolerations(std::slice::from_ref(&wide), std::slice::from_ref(&wide)),
            vec![wide]
        );
    }

    #[test]
    fn selector_conversion() {
        assert_eq!(convert_selector_to_labels_map("").unwrap().len(), 0);
        let m = convert_selector_to_labels_map("a=b, c = d").unwrap();
        assert_eq!(m.get("a").unwrap(), "b");
        assert_eq!(m.get("c").unwrap(), "d");
        assert!(convert_selector_to_labels_map("a").is_err());
        assert!(convert_selector_to_labels_map("a=b=c").is_err());
        assert!(convert_selector_to_labels_map("a b=c").is_err());
    }

    fn set(pairs: &[(&str, &str)]) -> Set {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn labels_set_ops() {
        assert!(conflicts(&set(&[("a", "1")]), &set(&[("a", "2")])));
        assert!(!conflicts(&set(&[("a", "1")]), &set(&[("b", "2")])));
        assert_eq!(
            merge(&set(&[("a", "1")]), &set(&[("a", "2")])),
            set(&[("a", "2")])
        );
        assert!(is_subset(&set(&[("a", "1")]), &Set::new()));
        assert!(is_subset(
            &set(&[("a", "1")]),
            &set(&[("a", "1"), ("b", "2")])
        ));
        assert!(!is_subset(&set(&[("a", "1")]), &set(&[("a", "2")])));
        assert!(!is_subset(&set(&[("c", "1")]), &set(&[("a", "2")])));
    }

    async fn store_with_ns(annotations: &[(&str, &str)]) -> StorageBackend {
        let storage = StorageBackend::Memory(MemoryStorage::new().into());
        let mut ns = Namespace::new("testns");
        ns.metadata.annotations = Some(
            annotations
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        storage
            .create(&build_key("namespaces", None, "testns"), &ns)
            .await
            .unwrap();
        storage
    }

    fn pod(
        node_selector: &[(&str, &str)],
        tolerations: Vec<Toleration>,
        qos_guaranteed: bool,
    ) -> Pod {
        let mut container = serde_json::json!({"name": "c", "image": "i"});
        if qos_guaranteed {
            container["resources"] = serde_json::json!({
                "requests": {"cpu": "1", "memory": "1Gi"},
                "limits": {"cpu": "1", "memory": "1Gi"}
            });
        }
        let mut pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"},
            "spec": {"containers": [container]}
        }))
        .unwrap();
        let spec = pod.spec.as_mut().unwrap();
        if !node_selector.is_empty() {
            spec.node_selector = Some(
                node_selector
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        if !tolerations.is_empty() {
            spec.tolerations = Some(tolerations);
        }
        pod
    }

    const NS_SEL: &str = "scheduler.alpha.kubernetes.io/node-selector";

    // podnodeselector TestPodAdmission
    #[tokio::test]
    async fn node_selector_admission() {
        let cluster: NodeSelectorConfig = [(
            "clusterDefaultNodeSelector".to_string(),
            "env=dev".to_string(),
        )]
        .into_iter()
        .chain([("testns".to_string(), "env=test,infra=nginx".to_string())])
        .collect();

        // namespace annotation selector merged into the pod's
        let st = store_with_ns(&[(NS_SEL, "infra=nginx")]).await;
        let mut p = pod(&[("env", "test")], vec![], false);
        pod_node_selector_admit_with(&st, "testns", &mut p, &cluster)
            .await
            .unwrap();
        assert_eq!(
            p.spec.unwrap().node_selector.unwrap(),
            [("env", "test"), ("infra", "nginx")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>()
        );

        // conflict with the namespace selector
        let mut p = pod(&[("infra", "apache")], vec![], false);
        let err = pod_node_selector_admit_with(&st, "testns", &mut p, &cluster)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflicts with its namespace node label selector"));

        // outside the namespace whitelist
        let mut p = pod(&[("env", "prod")], vec![], false);
        let err = pod_node_selector_admit_with(&st, "testns", &mut p, &cluster)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflict with its namespace whitelist"));

        // no annotation: the cluster default is used
        let st = store_with_ns(&[]).await;
        let mut p = pod(&[], vec![], false);
        let cluster2: NodeSelectorConfig = [(
            "clusterDefaultNodeSelector".to_string(),
            "env=dev".to_string(),
        )]
        .into();
        pod_node_selector_admit_with(&st, "testns", &mut p, &cluster2)
            .await
            .unwrap();
        assert_eq!(p.spec.unwrap().node_selector.unwrap()["env"], "dev");

        // missing namespace is NotFound
        let err =
            pod_node_selector_admit_with(&st, "nope", &mut pod(&[], vec![], false), &cluster2)
                .await
                .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn node_selector_config_formats() {
        let c = read_node_selector_config(
            "podNodeSelectorPluginConfig:\n  clusterDefaultNodeSelector: a=b\n  ns1: c=d\n",
        )
        .unwrap();
        assert_eq!(c["clusterDefaultNodeSelector"], "a=b");
        assert_eq!(c["ns1"], "c=d");
        assert!(read_node_selector_config("").unwrap().is_empty());
        assert!(
            read_node_selector_config("{\"podNodeSelectorPluginConfig\":{\"x\":\"y=z\"}}")
                .unwrap()
                .contains_key("x")
        );
    }

    const DEF: &str = "scheduler.alpha.kubernetes.io/defaultTolerations";
    const WL: &str = "scheduler.alpha.kubernetes.io/tolerationsWhitelist";

    // podtolerationrestriction TestPodAdmission
    #[tokio::test]
    async fn toleration_admission() {
        let cfg = TolerationConfiguration::default();

        // namespace default tolerations are merged in
        let st = store_with_ns(&[(
            DEF,
            r#"[{"key":"testKey","operator":"Equal","value":"testValue","effect":"NoSchedule"}]"#,
        )])
        .await;
        let mut p = pod(&[], vec![], false);
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap();
        assert_eq!(
            p.spec.unwrap().tolerations.unwrap(),
            vec![tol("testKey", "Equal", "testValue", "NoSchedule", None)]
        );

        // cluster defaults when the namespace sets none
        let st = store_with_ns(&[]).await;
        let cfg2 = TolerationConfiguration {
            default: vec![tol("cluster", "Exists", "", "NoSchedule", None)],
            whitelist: vec![],
        };
        let mut p = pod(&[], vec![], false);
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg2)
            .await
            .unwrap();
        assert_eq!(p.spec.unwrap().tolerations.unwrap().len(), 1);

        // a non-BestEffort pod gets the memory-pressure toleration
        let mut p = pod(&[], vec![], true);
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap();
        assert_eq!(
            p.spec.unwrap().tolerations.unwrap(),
            vec![tol(
                TAINT_NODE_MEMORY_PRESSURE,
                "Exists",
                "",
                "NoSchedule",
                None
            )]
        );

        // namespace whitelist rejects a toleration outside it
        let st = store_with_ns(&[(
            WL,
            r#"[{"key":"allowed","operator":"Exists","effect":"NoSchedule"}]"#,
        )])
        .await;
        let mut p = pod(
            &[],
            vec![tol("other", "Exists", "", "NoSchedule", None)],
            false,
        );
        let err = toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflict with its namespace whitelist"));
        // and accepts one inside it
        let mut p = pod(
            &[],
            vec![tol("allowed", "Equal", "x", "NoSchedule", None)],
            false,
        );
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg)
            .await
            .unwrap();

        // empty namespace whitelist overrides the cluster whitelist
        let st = store_with_ns(&[(WL, "")]).await;
        let cfg3 = TolerationConfiguration {
            default: vec![],
            whitelist: vec![tol("allowed", "Exists", "", "", None)],
        };
        let mut p = pod(
            &[],
            vec![tol("other", "Exists", "", "NoSchedule", None)],
            false,
        );
        toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg3)
            .await
            .unwrap();

        // cluster whitelist applies when the namespace sets none
        let st = store_with_ns(&[]).await;
        let mut p = pod(
            &[],
            vec![tol("other", "Exists", "", "NoSchedule", None)],
            false,
        );
        let err = toleration_restriction_admit_with(&st, "testns", true, &mut p, &cfg3)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("conflict with its cluster whitelist"));

        // malformed annotation is an error
        let st = store_with_ns(&[(DEF, "not json")]).await;
        assert!(toleration_restriction_admit_with(
            &st,
            "testns",
            true,
            &mut pod(&[], vec![], false),
            &cfg
        )
        .await
        .is_err());

        // on UPDATE the namespace defaults are not applied
        let st = store_with_ns(&[(
            DEF,
            r#"[{"key":"testKey","operator":"Exists","effect":"NoSchedule"}]"#,
        )])
        .await;
        let mut p = pod(&[], vec![], false);
        toleration_restriction_admit_with(&st, "testns", false, &mut p, &cfg)
            .await
            .unwrap();
        assert!(p.spec.unwrap().tolerations.is_none());
    }

    // validation_test.go TestValidateConfiguration + config.go decoding
    #[test]
    fn toleration_configuration_loading() {
        let hdr =
            "apiVersion: podtolerationrestriction.admission.k8s.io/v1alpha1\nkind: Configuration\n";
        let ok = format!(
            "{hdr}default:\n- {{key: foo, operator: Exists, effect: NoExecute, tolerationSeconds: 60}}\n- {{operator: Exists, effect: NoSchedule}}\nwhitelist:\n- {{key: foo, value: bar, effect: NoSchedule}}\n"
        );
        let c = load_toleration_configuration(&ok).unwrap();
        assert_eq!(c.default.len(), 2);
        assert_eq!(c.whitelist.len(), 1);
        // Exists with a value is invalid
        let bad = format!(
            "{hdr}whitelist:\n- {{key: foo, operator: Exists, value: bar, effect: NoSchedule}}\n"
        );
        assert!(load_toleration_configuration(&bad).is_err());
        // empty key needs Exists
        let bad = format!("{hdr}default:\n- {{operator: Equal, value: bar, effect: NoSchedule}}\n");
        assert!(load_toleration_configuration(&bad).is_err());
        // wrong kind / missing kind
        assert!(load_toleration_configuration("kind: Other\napiVersion: v1\n").is_err());
        assert!(load_toleration_configuration("default: []\n").is_err());
    }
}
