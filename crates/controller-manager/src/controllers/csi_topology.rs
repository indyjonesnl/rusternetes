//! CSI topology for the external-provisioner (#3007), a port of
//! kubernetes-csi/external-provisioner `pkg/controller/topology.go` and the
//! in-memory `pvcNodeStore` of `pkg/controller/cache.go`.
//!
//! `GenerateAccessibilityRequirements` turns the StorageClass'
//! `allowedTopologies`, the selected node (delayed binding) and the
//! `CSINode`/`Node` objects into the `TopologyRequirement` passed to
//! `CreateVolume`; `GenerateVolumeNodeAffinity` turns the driver's
//! `accessible_topology` back into the PV's `nodeAffinity`.

use rusternetes_common::resources::csi::CSINode;
use rusternetes_common::resources::volume::{
    NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, TopologySelectorTerm,
    VolumeNodeAffinity,
};
use rusternetes_common::resources::Node;
use rusternetes_common::Error;
use rusternetes_csi::proto::{Topology, TopologyRequirement};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

/// `topologyTerm` (topology.go:41-46): `(key, value)` segments AND'd together.
/// Sort after construction for `compare`/`subset` to work.
pub type TopologyTerm = Vec<(String, String)>;

/// `GenerateVolumeNodeAffinity` (topology.go:54-80). Map iteration order is
/// random in Go; keys are sorted here so the PV is deterministic.
pub fn generate_volume_node_affinity(accessible: &[Topology]) -> Option<VolumeNodeAffinity> {
    if accessible.is_empty() {
        return None;
    }
    let mut terms = Vec::new();
    for topology in accessible {
        if topology.segments.is_empty() {
            continue;
        }
        let mut keys: Vec<&String> = topology.segments.keys().collect();
        keys.sort();
        let expressions = keys
            .into_iter()
            .map(|k| NodeSelectorRequirement {
                key: k.clone(),
                operator: "In".to_string(),
                values: Some(vec![topology.segments[k].clone()]),
            })
            .collect();
        terms.push(NodeSelectorTerm {
            match_expressions: Some(expressions),
            match_fields: None,
        });
    }
    Some(VolumeNodeAffinity {
        required: Some(NodeSelector {
            node_selector_terms: terms,
        }),
    })
}

/// `topologyTerm.sort` (topology.go:730-739).
fn sort_term(t: &mut TopologyTerm) {
    t.sort();
}

/// `topologyTerm.compare` (topology.go:741-757): shorter first, then by
/// segment.
fn compare(a: &TopologyTerm, b: &TopologyTerm) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// `topologyTerm.subset` (topology.go:759-779): every segment of `t` is in
/// `other`; both sorted.
fn subset(t: &TopologyTerm, other: &TopologyTerm) -> bool {
    if t.is_empty() {
        return true;
    }
    let mut j = 0;
    for (k2, v2) in other {
        let (k1, v1) = &t[j];
        if k1 != k2 {
            continue;
        }
        if v1 != v2 {
            return false;
        }
        j += 1;
        if j == t.len() {
            return true;
        }
    }
    false
}

/// `toCSITopology` (topology.go:781-796).
fn to_csi_topology(terms: &[TopologyTerm]) -> Vec<Topology> {
    terms
        .iter()
        .filter(|t| !t.is_empty())
        .map(|t| Topology {
            segments: t.iter().cloned().collect(),
        })
        .collect()
}

/// `flatten` (topology.go:570-604): distribute the OR over values across the
/// AND of requirements of every term.
pub fn flatten(allowed: &[TopologySelectorTerm]) -> Vec<TopologyTerm> {
    let mut final_terms: Vec<TopologyTerm> = Vec::new();
    for selector_term in allowed {
        let mut old_terms: Vec<TopologyTerm> = Vec::new();
        for expr in selector_term
            .match_label_expressions
            .as_deref()
            .unwrap_or_default()
        {
            let mut new_terms = Vec::new();
            for v in &expr.values {
                if old_terms.is_empty() {
                    new_terms.push(vec![(expr.key.clone(), v.clone())]);
                } else {
                    for old in &old_terms {
                        let mut t = old.clone();
                        t.push((expr.key.clone(), v.clone()));
                        new_terms.push(t);
                    }
                }
            }
            old_terms = new_terms;
        }
        final_terms.extend(old_terms);
    }
    for t in &mut final_terms {
        sort_term(t);
    }
    final_terms
}

/// `getPVCNameHashAndIndexOffset` (topology.go:798-844). `fnv.New32` is
/// FNV-1 (multiply, then xor).
pub fn pvc_name_hash_and_index_offset(pvc_name: &str) -> (u32, u32) {
    if pvc_name.is_empty() {
        return (rand::random::<u32>(), 0);
    }
    let mut index = 0u32;
    let mut hash_string = pvc_name;
    if let Some(last_dash) = pvc_name.rfind('-') {
        if let Ok(id) = pvc_name[last_dash + 1..].parse::<u32>() {
            index = id;
            hash_string = &pvc_name[..last_dash];
            if let Some(last_dash) = hash_string.rfind('-') {
                hash_string = &hash_string[last_dash + 1..];
            }
        }
    }
    let mut h: u32 = 2166136261;
    for b in hash_string.bytes() {
        h = h.wrapping_mul(16777619);
        h ^= b as u32;
    }
    (h, index)
}

/// `TopologyInfo` (cache.go): what the provisioner remembers about one PVC
/// between retries.
#[derive(Clone, Debug, Default)]
pub struct TopologyInfo {
    pub node_labels: HashMap<String, String>,
    pub topology_keys: Vec<String>,
    pub requisite_terms: Vec<TopologyTerm>,
}

/// `InMemoryStore` (cache.go:31-162), keyed by PVC UID. "The entry is deleted
/// when provision succeeds or returns a final error."
#[derive(Default)]
pub struct TopologyStore {
    data: Mutex<HashMap<String, TopologyInfo>>,
}

impl TopologyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, uid: &str) -> Option<TopologyInfo> {
        self.data.lock().unwrap().get(uid).cloned()
    }

    pub fn delete(&self, uid: &str) {
        self.data.lock().unwrap().remove(uid);
    }

    pub fn update_node_labels(&self, uid: &str, labels: HashMap<String, String>) {
        self.data
            .lock()
            .unwrap()
            .entry(uid.to_string())
            .or_default()
            .node_labels = labels;
    }

    pub fn update_topology_keys(&self, uid: &str, keys: Vec<String>) {
        self.data
            .lock()
            .unwrap()
            .entry(uid.to_string())
            .or_default()
            .topology_keys = keys;
    }

    pub fn update_requisite_terms(&self, uid: &str, terms: Vec<TopologyTerm>) {
        self.data
            .lock()
            .unwrap()
            .entry(uid.to_string())
            .or_default()
            .requisite_terms = terms;
    }
}

/// An error out of `GenerateAccessibilityRequirements`. `NotFound` is
/// `apierrors.IsNotFound` in `prepareProvision` (controller.go:733-735): the
/// CSINode of the selected node is gone, so ask the scheduler to reschedule.
#[derive(Debug)]
pub enum TopologyError {
    NotFound(String),
    Other(String),
}

impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopologyError::NotFound(m) | TopologyError::Other(m) => f.write_str(m),
        }
    }
}

fn other<T>(m: String) -> Result<T, TopologyError> {
    Err(TopologyError::Other(m))
}

/// `getTopologyKeys` (topology.go:640-647).
fn topology_keys_of(csi_node: &CSINode, driver: &str) -> Vec<String> {
    csi_node
        .spec
        .drivers
        .iter()
        .find(|d| d.name == driver)
        .and_then(|d| d.topology_keys.clone())
        .unwrap_or_default()
}

/// `distinctTopologyKeySets` (topology.go:349-376).
pub fn distinct_topology_key_sets(csi_nodes: &[CSINode], driver: &str) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    let mut sets: Vec<Vec<String>> = Vec::new();
    for n in csi_nodes {
        let mut keys = topology_keys_of(n, driver);
        if keys.is_empty() {
            continue;
        }
        keys.sort();
        if seen.insert(keys.join("\x00")) {
            sets.push(keys);
        }
    }
    sets.sort();
    sets
}

/// `registeredTopologyKeys` (topology.go:378-391).
pub fn registered_topology_keys(
    csi_nodes: &[CSINode],
    driver: &str,
) -> HashMap<String, BTreeSet<String>> {
    csi_nodes
        .iter()
        .filter_map(|n| {
            let keys = topology_keys_of(n, driver);
            (!keys.is_empty()).then(|| (n.metadata.name.clone(), keys.into_iter().collect()))
        })
        .collect()
}

/// `extractTopologyTerm` (topology.go:649-660); `None` is `isMissingKey`.
fn extract_topology_term(
    labels: &HashMap<String, String>,
    keys: &[String],
) -> Option<TopologyTerm> {
    let mut term = Vec::new();
    for k in keys {
        term.push((k.clone(), labels.get(k)?.clone()));
    }
    sort_term(&mut term);
    Some(term)
}

/// What `GenerateAccessibilityRequirements` reads. `storage` is the
/// informer-backed `nodeLister` / `csiNodeLister` upstream.
pub struct TopologyInputs<'a, S: Storage> {
    pub storage: &'a S,
    pub store: &'a TopologyStore,
    pub driver_name: &'a str,
    pub pvc_uid: &'a str,
    pub pvc_name: &'a str,
    pub allowed_topologies: &'a [TopologySelectorTerm],
    pub selected_node_name: &'a str,
    pub strict_topology: bool,
    pub immediate_topology: bool,
}

impl<S: Storage> TopologyInputs<'_, S> {
    /// `getSelectedCSINode` (topology.go:321-347): any error, including
    /// NotFound, is returned rather than falling back to the wrong topology.
    async fn selected_csi_node(&self) -> Result<CSINode, TopologyError> {
        self.storage
            .get::<CSINode>(&build_key("csinodes", None, self.selected_node_name))
            .await
            .map_err(|e| match e {
                Error::NotFound(_) => TopologyError::NotFound(format!(
                    "failed to get selected CSINode {}: csinodes.storage.k8s.io \"{}\" not found",
                    self.selected_node_name, self.selected_node_name
                )),
                e => TopologyError::Other(format!(
                    "failed to get selected CSINode {}: {e}",
                    self.selected_node_name
                )),
            })
    }

    /// `topologyKeysLookup` (topology.go:109-136).
    async fn topology_keys_lookup(&self) -> Result<Vec<String>, TopologyError> {
        if let Some(info) = self.store.get(self.pvc_uid) {
            if !info.topology_keys.is_empty() {
                return Ok(info.topology_keys);
            }
        }
        let csi_node = self.selected_csi_node().await?;
        let keys = topology_keys_of(&csi_node, self.driver_name);
        if !keys.is_empty() {
            self.store.update_topology_keys(self.pvc_uid, keys.clone());
        }
        Ok(keys)
    }

    /// `getTopologyFromNodeName` (topology.go:662-693); `Err(labels)` is
    /// `isMissingKey`.
    async fn topology_from_node_name(
        &self,
        keys: &[String],
    ) -> Result<TopologyTerm, HashMap<String, String>> {
        // Read from the cache first.
        let cached = self
            .store
            .get(self.pvc_uid)
            .map(|i| i.node_labels)
            .unwrap_or_default();
        if !cached.is_empty() {
            if let Some(t) = extract_topology_term(&cached, keys) {
                return Ok(t);
            }
            // "refresh node labels from nodeLister to avoid using stale or
            // incomplete entries from pvcNodeStore."
        }
        let node: Node = match self
            .storage
            .get(&build_key("nodes", None, self.selected_node_name))
            .await
        {
            Ok(n) => n,
            // "Any error, including NotFound, results in us not being able to
            // determine topology."
            Err(_) => return Err(cached),
        };
        let labels = node.metadata.labels.unwrap_or_default();
        if !labels.is_empty() {
            self.store.update_node_labels(self.pvc_uid, labels.clone());
        }
        extract_topology_term(&labels, keys).ok_or(labels)
    }

    /// `aggregateTopologiesForKeys` (topology.go:399-422): one term per node
    /// carrying all `keys` whose own driver registration covers them.
    async fn aggregate_for_keys(
        &self,
        keys: &[String],
        registered: &HashMap<String, BTreeSet<String>>,
    ) -> Result<Vec<TopologyTerm>, TopologyError> {
        let nodes: Vec<Node> = self
            .storage
            .list(&build_prefix("nodes", None))
            .await
            .map_err(|e| TopologyError::Other(format!("error listing nodes: {e}")))?;
        let mut terms = Vec::new();
        for node in nodes {
            // `buildTopologyKeySelector`: every key must exist as a label.
            let labels = node.metadata.labels.clone().unwrap_or_default();
            if !keys.iter().all(|k| labels.contains_key(k)) {
                continue;
            }
            match registered.get(&node.metadata.name) {
                Some(reg) if keys.iter().all(|k| reg.contains(k)) => {}
                _ => continue,
            }
            if let Some(term) = extract_topology_term(&labels, keys) {
                if !term.is_empty() {
                    terms.push(term);
                }
            }
        }
        Ok(terms)
    }

    async fn list_csi_nodes(&self) -> Result<Vec<CSINode>, TopologyError> {
        self.storage
            .list(&build_prefix("csinodes", None))
            .await
            .map_err(|e| TopologyError::Other(format!("error listing CSINodes: {e}")))
    }

    /// `aggregateTopologies` (topology.go:424-568).
    async fn aggregate_topologies(
        &self,
        selected_keys: &[String],
    ) -> Result<Vec<TopologyTerm>, TopologyError> {
        if self.selected_node_name.is_empty() {
            // Immediate binding.
            if let Some(info) = self.store.get(self.pvc_uid) {
                if !info.requisite_terms.is_empty() {
                    return Ok(info.requisite_terms);
                }
            }
            let csi_nodes = self.list_csi_nodes().await?;
            let key_sets = distinct_topology_key_sets(&csi_nodes, self.driver_name);
            if key_sets.is_empty() {
                // "The driver supports topology but no nodes have registered
                // any topology keys."
                tracing::warn!("No topology keys found on any node");
                return Ok(Vec::new());
            }
            let registered = registered_topology_keys(&csi_nodes, self.driver_name);
            let mut terms = Vec::new();
            for keys in &key_sets {
                terms.extend(self.aggregate_for_keys(keys, &registered).await?);
            }
            if terms.is_empty() {
                return other(format!(
                    "topologyKeys {key_sets:?} were not found on any nodes"
                ));
            }
            self.store
                .update_requisite_terms(self.pvc_uid, terms.clone());
            return Ok(terms);
        }

        // Delayed binding; use the topology keys of the selected node.
        if selected_keys.is_empty() {
            return other(format!(
                "no topology key found on CSINode {}",
                self.selected_node_name
            ));
        }
        if let Some(info) = self.store.get(self.pvc_uid) {
            if !info.requisite_terms.is_empty() {
                return Ok(info.requisite_terms);
            }
        }
        let csi_nodes = self.list_csi_nodes().await?;
        let terms = self
            .aggregate_for_keys(
                selected_keys,
                &registered_topology_keys(&csi_nodes, self.driver_name),
            )
            .await?;
        if terms.is_empty() {
            return other(format!(
                "topologyKeys {selected_keys:?} were not found on any nodes"
            ));
        }
        self.store
            .update_requisite_terms(self.pvc_uid, terms.clone());
        Ok(terms)
    }

    /// `GenerateAccessibilityRequirements` (topology.go:177-318).
    pub async fn generate_accessibility_requirements(
        &self,
    ) -> Result<Option<TopologyRequirement>, TopologyError> {
        let selected = !self.selected_node_name.is_empty();
        let mut selected_topology: TopologyTerm = Vec::new();
        let mut topology_keys: Vec<String> = Vec::new();
        let mut requisite: Vec<TopologyTerm> = Vec::new();

        // 1. Get topology keys for the selected node.
        if selected {
            topology_keys = self.topology_keys_lookup().await?;
            if topology_keys.is_empty() {
                // The scheduler selected a node with no topology information.
                return other(format!(
                    "no topology key found for node {}",
                    self.selected_node_name
                ));
            }
            selected_topology = match self.topology_from_node_name(&topology_keys).await {
                Ok(t) => t,
                Err(labels) => {
                    return other(format!(
                        "topology labels from selected node {labels:?} does not match topology keys from CSINode {topology_keys:?}"
                    ))
                }
            };
            if self.strict_topology {
                // Make sure that selected node topology is in allowed topologies list
                if !self.allowed_topologies.is_empty() {
                    let flat = flatten(self.allowed_topologies);
                    if !flat.iter().any(|t| subset(t, &selected_topology)) {
                        return other(format!(
                            "selected node '{:?}' topology '{selected_topology:?}' is not in allowed topologies: {flat:?}",
                            self.selected_node_name
                        ));
                    }
                }
                // Only pass topology of selected node.
                requisite.push(selected_topology.clone());
            }
        }

        // 2. Generate CSI Requisite Terms.
        if requisite.is_empty() {
            if !self.allowed_topologies.is_empty() {
                // Distribute out one of the OR layers in allowedTopologies.
                requisite = flatten(self.allowed_topologies);
            } else {
                if !selected && !self.immediate_topology {
                    // Don't specify any topology requirements.
                    return Ok(None);
                }
                // Aggregate existing topologies in nodes across the cluster.
                requisite = self.aggregate_topologies(&topology_keys).await?;
                if requisite.is_empty() {
                    // The driver has not registered on any nodes: wait.
                    return other("no available topology found".into());
                }
            }
        }

        // allowedTopologies had empty entries: "topology disabled" behaviour.
        if requisite.is_empty() {
            return Ok(None);
        }

        requisite.sort_by(compare);
        requisite.dedup();

        let mut requirement = TopologyRequirement {
            requisite: to_csi_topology(&requisite),
            preferred: Vec::new(),
        };

        // 3. Generate CSI Preferred Terms.
        let preferred: Vec<TopologyTerm>;
        if !selected {
            // Immediate binding: statefulset spreading hash.
            let (hash, index) = pvc_name_hash_and_index_offset(self.pvc_name);
            let i = (hash.wrapping_add(index) % requisite.len() as u32) as usize;
            preferred = rotate(&requisite, i);
        } else if self.strict_topology {
            // In case of strict topology, preferred = requisite.
            preferred = requisite;
        } else {
            // Delayed binding, use topology from that node.
            match requisite.iter().position(|t| subset(t, &selected_topology)) {
                Some(i) => preferred = rotate(&requisite, i),
                None => {
                    return other(format!(
                        "topology {selected_topology:?} from selected node {:?} is not in requisite: {requisite:?}",
                        self.selected_node_name
                    ))
                }
            }
        }
        requirement.preferred = to_csi_topology(&preferred);
        Ok(Some(requirement))
    }
}

/// `append(terms[i:], terms[:i]...)`.
fn rotate(terms: &[TopologyTerm], i: usize) -> Vec<TopologyTerm> {
    let mut v = terms[i..].to_vec();
    v.extend_from_slice(&terms[..i]);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::csi::{CSINodeDriver, CSINodeSpec};
    use rusternetes_common::resources::volume::TopologySelectorLabelRequirement;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;

    const DRIVER: &str = "com.example.csi/driver";
    const ZONE: &str = "com.example.csi/zone";
    const RACK: &str = "com.example.csi/rack";

    fn seg(pairs: &[(&str, &str)]) -> Topology {
        Topology {
            segments: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn term(pairs: &[(&str, &str)]) -> TopologyTerm {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn allowed(exprs: &[(&str, &[&str])]) -> TopologySelectorTerm {
        TopologySelectorTerm {
            match_label_expressions: Some(
                exprs
                    .iter()
                    .map(|(k, vs)| TopologySelectorLabelRequirement {
                        key: k.to_string(),
                        values: vs.iter().map(|v| v.to_string()).collect(),
                    })
                    .collect(),
            ),
        }
    }

    // ---- TestGenerateVolumeNodeAffinity (topology_test.go:47) ----

    #[test]
    fn node_affinity_of_no_topology_is_nil() {
        assert!(generate_volume_node_affinity(&[]).is_none());
    }

    #[test]
    fn node_affinity_has_one_term_per_topology_and_one_in_expression_per_segment() {
        let a = generate_volume_node_affinity(&[
            seg(&[(ZONE, "zone1"), (RACK, "rack2")]),
            seg(&[(ZONE, "zone2")]),
        ])
        .unwrap();
        let terms = a.required.unwrap().node_selector_terms;
        assert_eq!(terms.len(), 2);
        let e = terms[0].match_expressions.as_ref().unwrap();
        assert_eq!(e.len(), 2);
        assert_eq!((e[0].key.as_str(), e[0].operator.as_str()), (RACK, "In"));
        assert_eq!(e[1].values.as_deref(), Some(&["zone1".to_string()][..]));
        assert_eq!(
            terms[1].match_expressions.as_ref().unwrap()[0]
                .values
                .as_deref(),
            Some(&["zone2".to_string()][..])
        );
    }

    #[test]
    fn node_affinity_skips_empty_segments() {
        let a = generate_volume_node_affinity(&[seg(&[])]).unwrap();
        assert!(a.required.unwrap().node_selector_terms.is_empty());
    }

    // ---- TestTopologyTermSort / Compare / Subset (topology_test.go:2464-2619) ----

    #[test]
    fn terms_compare_by_length_then_segments() {
        let a = term(&[("a", "1")]);
        let b = term(&[("a", "1"), ("b", "1")]);
        assert_eq!(compare(&a, &b), Ordering::Less);
        assert_eq!(compare(&b, &a), Ordering::Greater);
        assert_eq!(compare(&a, &a), Ordering::Equal);
        assert_eq!(
            compare(&term(&[("a", "1")]), &term(&[("a", "2")])),
            Ordering::Less
        );
        assert_eq!(
            compare(&term(&[("a", "9")]), &term(&[("b", "1")])),
            Ordering::Less
        );
    }

    #[test]
    fn subset_follows_topology_term_subset() {
        let sel = term(&[("rack", "r1"), ("zone", "z1")]);
        assert!(subset(&term(&[]), &sel));
        assert!(subset(&term(&[("zone", "z1")]), &sel));
        assert!(subset(&sel, &sel));
        assert!(!subset(&term(&[("zone", "z2")]), &sel));
        assert!(!subset(&term(&[("host", "h")]), &sel));
        assert!(!subset(&term(&[("rack", "r1"), ("host", "h")]), &sel));
    }

    // ---- TestAllowedTopologies (topology_test.go:465): flatten ----

    #[test]
    fn flatten_distributes_or_over_and() {
        let f = flatten(&[allowed(&[(ZONE, &["zone1"]), (RACK, &["rackA", "rackB"])])]);
        assert_eq!(
            f,
            vec![
                term(&[(RACK, "rackA"), (ZONE, "zone1")]),
                term(&[(RACK, "rackB"), (ZONE, "zone1")]),
            ]
        );
        let f = flatten(&[allowed(&[(ZONE, &["z1", "z2"]), (RACK, &["rA", "rB"])])]);
        assert_eq!(f.len(), 4);
        // several allowed terms are OR'd.
        let f = flatten(&[allowed(&[(ZONE, &["z1"])]), allowed(&[(ZONE, &["z2"])])]);
        assert_eq!(f, vec![term(&[(ZONE, "z1")]), term(&[(ZONE, "z2")])]);
    }

    // ---- TestStatefulSetSpreading (topology_test.go:163) ----

    #[test]
    fn fnv1_32_matches_go_hash_fnv() {
        // Go: fnv.New32() over "a" / "testset" / "foobar".
        let (h, i) = pvc_name_hash_and_index_offset("a");
        assert_eq!((h, i), (84696446, 0));
        let (h, i) = pvc_name_hash_and_index_offset("foobar");
        assert_eq!((h, i), (837857890, 0));
        // `ClaimName-StatefulSetName-Id`: only StatefulSetName is hashed.
        let (h, i) = pvc_name_hash_and_index_offset("testpvcA-testset-1");
        assert_eq!((h, i), (1633848515, 1));
        let (h2, _) = pvc_name_hash_and_index_offset("testpvcB-testset-7");
        assert_eq!(h2, 1633848515);
        // `Name-Id` hashes `Name`.
        let (h, i) = pvc_name_hash_and_index_offset("a-3");
        assert_eq!((h, i), (84696446, 3));
    }

    // ---- GenerateAccessibilityRequirements over a MemoryStorage ----

    struct Cluster {
        storage: MemoryStorage,
        store: TopologyStore,
    }

    impl Cluster {
        fn new() -> Self {
            Self {
                storage: MemoryStorage::new(),
                store: TopologyStore::new(),
            }
        }

        /// A node with `labels`; the driver is registered with `keys` on its
        /// CSINode when `keys` is `Some`.
        async fn node(&self, name: &str, labels: &[(&str, &str)], keys: Option<&[&str]>) {
            let mut n = Node::new(name);
            n.metadata.labels = Some(
                labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
            self.storage
                .create(&build_key("nodes", None, name), &n)
                .await
                .unwrap();
            if let Some(keys) = keys {
                let cn = CSINode {
                    type_meta: TypeMeta {
                        kind: "CSINode".into(),
                        api_version: "storage.k8s.io/v1".into(),
                    },
                    metadata: ObjectMeta::new(name),
                    spec: CSINodeSpec {
                        drivers: vec![CSINodeDriver {
                            name: DRIVER.into(),
                            node_id: name.into(),
                            topology_keys: Some(keys.iter().map(|k| k.to_string()).collect()),
                            allocatable: None,
                        }],
                    },
                };
                self.storage
                    .create(&build_key("csinodes", None, name), &cn)
                    .await
                    .unwrap();
            }
        }

        async fn generate(
            &self,
            pvc_name: &str,
            allowed: &[TopologySelectorTerm],
            selected: &str,
            strict: bool,
            immediate: bool,
        ) -> Result<Option<TopologyRequirement>, TopologyError> {
            TopologyInputs {
                storage: &self.storage,
                store: &self.store,
                driver_name: DRIVER,
                pvc_uid: "uid-1",
                pvc_name,
                allowed_topologies: allowed,
                selected_node_name: selected,
                strict_topology: strict,
                immediate_topology: immediate,
            }
            .generate_accessibility_requirements()
            .await
        }
    }

    const BOTH: &[&str] = &[ZONE, RACK];

    async fn four_nodes() -> Cluster {
        let c = Cluster::new();
        for (i, (z, r)) in [
            ("zone1", "rackA"),
            ("zone2", "rackB"),
            ("zone3", "rackC"),
            ("zone4", "rackD"),
        ]
        .iter()
        .enumerate()
        {
            c.node(&format!("node{i}"), &[(ZONE, z), (RACK, r)], Some(BOTH))
                .await;
        }
        c
    }

    #[tokio::test]
    async fn statefulset_pvcs_rotate_the_preferred_terms_by_their_ordinal() {
        let c = four_nodes().await;
        // hash("testset") = 1633848515, 1633848515 % 4 == 3.
        let r = c
            .generate("testpvcA-testset-0", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite.len(), 4);
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone4"), (RACK, "rackD")]));
        assert_eq!(r.preferred[1], seg(&[(ZONE, "zone1"), (RACK, "rackA")]));
        // ordinal 1 shifts by one: (3 + 1) % 4 == 0.
        c.store.delete("uid-1");
        let r = c
            .generate("testpvcA-testset-1", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.preferred, r.requisite);
        // The other claim of the same StatefulSet member lands in the same place.
        c.store.delete("uid-1");
        let r2 = c
            .generate("testpvcB-testset-1", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2.preferred, r.preferred);
    }

    #[tokio::test]
    async fn allowed_topologies_become_the_requisite_terms() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone2", "zone1"])])];
        let r = c
            .generate("p-0", &a, "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.requisite,
            vec![seg(&[(ZONE, "zone1")]), seg(&[(ZONE, "zone2")])]
        );
        assert_eq!(r.preferred.len(), 2);
    }

    #[tokio::test]
    async fn immediate_binding_without_immediate_topology_sends_nothing() {
        let c = four_nodes().await;
        assert!(c
            .generate("p-0", &[], "", false, false)
            .await
            .unwrap()
            .is_none());
        // allowedTopologies still apply.
        let a = [allowed(&[(ZONE, &["zone1"])])];
        assert!(c
            .generate("p-0", &a, "", false, false)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn aggregation_only_reports_nodes_whose_driver_registered_the_keys() {
        let c = Cluster::new();
        c.node("a", &[(ZONE, "z1")], Some(&[ZONE])).await;
        // Labelled but the driver is not registered there.
        c.node("b", &[(ZONE, "z2")], None).await;
        // Registered but without the label.
        c.node("c", &[], Some(&[ZONE])).await;
        let r = c
            .generate("p-0", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite, vec![seg(&[(ZONE, "z1")])]);
    }

    #[tokio::test]
    async fn aggregation_covers_every_distinct_key_set() {
        // TestTopologyAggregation: nodes may report different granularities.
        let c = Cluster::new();
        c.node("a", &[(ZONE, "z1")], Some(&[ZONE])).await;
        c.node("b", &[(ZONE, "z2"), (RACK, "r1")], Some(BOTH)).await;
        let r = c
            .generate("p-0", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        // Sorted shortest first: the coarse term, then the finer one.
        assert_eq!(
            r.requisite,
            vec![
                seg(&[(ZONE, "z1")]),
                seg(&[(ZONE, "z2")]),
                seg(&[(ZONE, "z2"), (RACK, "r1")]),
            ]
        );
    }

    #[tokio::test]
    async fn no_registered_topology_keys_means_no_available_topology() {
        let c = Cluster::new();
        c.node("a", &[(ZONE, "z1")], None).await;
        let e = c.generate("p-0", &[], "", false, true).await.unwrap_err();
        assert_eq!(e.to_string(), "no available topology found");
    }

    #[tokio::test]
    async fn keys_registered_but_labels_absent_fail() {
        let c = Cluster::new();
        c.node("a", &[], Some(&[ZONE])).await;
        let e = c.generate("p-0", &[], "", false, true).await.unwrap_err();
        assert!(e.to_string().contains("were not found on any nodes"), "{e}");
    }

    // ---- TestPreferredTopologies (topology_test.go:1493) ----

    #[tokio::test]
    async fn delayed_binding_prefers_the_selected_node_and_wraps_around() {
        let c = four_nodes().await;
        let r = c
            .generate("p", &[], "node2", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite.len(), 4);
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone3"), (RACK, "rackC")]));
        assert_eq!(r.preferred[1], seg(&[(ZONE, "zone4"), (RACK, "rackD")]));
        assert_eq!(r.preferred[2], seg(&[(ZONE, "zone1"), (RACK, "rackA")]));
    }

    #[tokio::test]
    async fn strict_topology_passes_only_the_selected_node() {
        let c = four_nodes().await;
        let r = c
            .generate("p", &[], "node1", true, true)
            .await
            .unwrap()
            .unwrap();
        let only = vec![seg(&[(ZONE, "zone2"), (RACK, "rackB")])];
        assert_eq!(r.requisite, only);
        assert_eq!(r.preferred, only);
    }

    #[tokio::test]
    async fn strict_topology_rejects_a_node_outside_allowed_topologies() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone1"])])];
        let e = c.generate("p", &a, "node1", true, true).await.unwrap_err();
        assert!(
            e.to_string().contains("is not in allowed topologies"),
            "{e}"
        );
        // ... and accepts one inside it.
        // (a final error drops the PVC's cache entry in the provisioner)
        c.store.delete("uid-1");
        let r = c
            .generate("p", &a, "node0", true, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite.len(), 1);
    }

    #[tokio::test]
    async fn non_strict_delayed_binding_with_allowed_topologies_uses_them() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone1", "zone2"])])];
        let r = c
            .generate("p", &a, "node1", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.requisite,
            vec![seg(&[(ZONE, "zone1")]), seg(&[(ZONE, "zone2")])]
        );
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone2")]));
    }

    #[tokio::test]
    async fn a_selected_node_outside_requisite_is_an_error() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone1"])])];
        let e = c.generate("p", &a, "node3", false, true).await.unwrap_err();
        assert!(e.to_string().contains("from selected node"), "{e}");
    }

    // ---- error cases of GenerateAccessibilityRequirements ----

    #[tokio::test]
    async fn a_missing_csinode_is_not_found_so_the_claim_reschedules() {
        let c = four_nodes().await;
        let e = c
            .generate("p", &[], "ghost", false, true)
            .await
            .unwrap_err();
        assert!(matches!(e, TopologyError::NotFound(_)), "{e}");
    }

    #[tokio::test]
    async fn a_csinode_without_topology_keys_is_an_error() {
        let c = Cluster::new();
        c.node("n", &[(ZONE, "z")], Some(&[])).await;
        let e = c.generate("p", &[], "n", false, true).await.unwrap_err();
        assert_eq!(e.to_string(), "no topology key found for node n");
    }

    #[tokio::test]
    async fn a_node_missing_a_registered_label_is_an_error() {
        // TestNodeTopologyLabelFallback.
        let c = Cluster::new();
        c.node("n", &[(ZONE, "z")], Some(BOTH)).await;
        let e = c.generate("p", &[], "n", false, true).await.unwrap_err();
        assert!(
            e.to_string()
                .contains("does not match topology keys from CSINode"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn a_deleted_node_is_served_from_the_cache_until_the_entry_is_dropped() {
        // TestProvisionWithDeletedNodeFromCache.
        let c = four_nodes().await;
        c.generate("p", &[], "node0", false, true).await.unwrap();
        c.storage
            .delete(&build_key("nodes", None, "node0"))
            .await
            .unwrap();
        let r = c
            .generate("p", &[], "node0", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone1"), (RACK, "rackA")]));
        c.store.delete("uid-1");
        assert!(c.generate("p", &[], "node0", false, true).await.is_err());
    }

    #[test]
    fn distinct_key_sets_are_sorted_and_deduplicated() {
        // TestDistinctTopologyKeySets.
        let mk = |name: &str, keys: Option<&[&str]>| CSINode {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new(name),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: DRIVER.into(),
                    node_id: String::new(),
                    topology_keys: keys.map(|k| k.iter().map(|s| s.to_string()).collect()),
                    allocatable: None,
                }],
            },
        };
        let nodes = [
            mk("a", Some(&["z", "r"])),
            mk("b", Some(&["r", "z"])),
            mk("c", Some(&["z"])),
            mk("d", None),
        ];
        assert_eq!(
            distinct_topology_key_sets(&nodes, DRIVER),
            vec![
                vec!["r".to_string(), "z".to_string()],
                vec!["z".to_string()]
            ]
        );
        let reg = registered_topology_keys(&nodes, DRIVER);
        assert_eq!(reg.len(), 3);
        assert!(!reg.contains_key("d"));
    }
}
