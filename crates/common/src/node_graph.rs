//! The Node authorizer's relationship graph.
//!
//! Ported from upstream `plugin/pkg/auth/authorizer/node/graph.go` (the graph,
//! its destination-edge index and the `Add*`/`Delete*` mutators),
//! `graph_populator.go` (the event handlers that decide when a mutator runs) and
//! `intset.go`, together with the visitors those mutators call:
//! `pkg/api/v1/pod/util.go` `VisitPodSecretNames` / `VisitPodConfigmapNames`,
//! `pkg/api/v1/persistentvolume/util.go` `VisitPVSecretNames`,
//! `component-helpers/storage/ephemeral` `VolumeClaimName` and
//! `dynamic-resource-allocation/resourceclaim` `Name` / `PodStatusEqual`.
//!
//! Upstream builds the graph on `third_party/forked/gonum/graph/simple`
//! `DirectedAcyclicGraph`, which stores ONE edge per vertex pair (an undirected
//! store that remembers each edge's direction). [`Inner`] keeps that shape: an
//! adjacency map holding the same [`Edge`] under both endpoints, so that
//! `Degree` (the number of adjacent vertices) and the "replace the edge between
//! this pair" behaviour of `SetEdge` match.
//!
//! Objects arrive as the JSON the API server stores (the Kubernetes wire shape),
//! not as typed structs, because the visitors must see every volume source the
//! upstream visitors see whether or not this repository models it.

use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// `vertexType` (graph.go).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VertexType {
    ConfigMap,
    Slice,
    Node,
    Pod,
    Pvc,
    Pv,
    ResourceClaim,
    Secret,
    Va,
    ServiceAccount,
    Pcr,
}

impl VertexType {
    /// `vertexTypes` (graph.go).
    pub fn name(self) -> &'static str {
        match self {
            VertexType::ConfigMap => "configmap",
            VertexType::Slice => "resourceslice",
            VertexType::Node => "node",
            VertexType::Pod => "pod",
            VertexType::Pvc => "pvc",
            VertexType::Pv => "pv",
            VertexType::ResourceClaim => "resourceclaim",
            VertexType::Secret => "secret",
            VertexType::Va => "volumeattachment",
            VertexType::ServiceAccount => "serviceAccount",
            VertexType::Pcr => "podcertificaterequest",
        }
    }

    /// `vertexTypeWithAuthoritativeIndex` (graph.go).
    fn has_authoritative_index(self) -> bool {
        matches!(
            self,
            VertexType::ConfigMap
                | VertexType::Slice
                | VertexType::Pod
                | VertexType::Pvc
                | VertexType::ResourceClaim
                | VertexType::Va
                | VertexType::ServiceAccount
                | VertexType::Pcr
        )
    }
}

/// `Graph.destinationEdgeThreshold` default (graph.go `NewGraph`).
const DEFAULT_DESTINATION_EDGE_THRESHOLD: usize = 200;

/// `corev1.MirrorPodAnnotationKey`.
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";

type VertexId = usize;

#[derive(Debug, Clone)]
struct NamedVertex {
    vertex_type: VertexType,
    namespace: String,
    name: String,
}

/// `simple.Edge` / `destinationEdge`: `destination` is `Some` for a
/// destination edge (`destinationEdge.Destination`).
#[derive(Debug, Clone, Copy)]
struct Edge {
    from: VertexId,
    to: VertexId,
    destination: Option<VertexId>,
}

/// `intSet` (intset.go): ids with refcounts.
#[derive(Debug, Default, Clone)]
struct IntSet {
    members: HashMap<VertexId, usize>,
}

impl IntSet {
    fn has(&self, i: VertexId) -> bool {
        self.members.get(&i).copied().unwrap_or(0) > 0
    }
    fn reset(&mut self) {
        self.members.clear();
    }
    fn increment(&mut self, i: VertexId) {
        *self.members.entry(i).or_insert(0) += 1;
    }
    /// Removes the id once the refcount would reach zero; never tracks below zero.
    fn decrement(&mut self, i: VertexId) {
        match self.members.get(&i).copied().unwrap_or(0) {
            0 | 1 => {
                self.members.remove(&i);
            }
            n => {
                self.members.insert(i, n - 1);
            }
        }
    }
}

#[derive(Debug)]
struct Inner {
    next_id: VertexId,
    nodes: HashMap<VertexId, NamedVertex>,
    /// `Graph.vertices`: type -> namespace -> name -> id.
    vertices: HashMap<VertexType, HashMap<String, HashMap<String, VertexId>>>,
    /// The vertex-pair edge store; each edge is present under both endpoints.
    adj: HashMap<VertexId, HashMap<VertexId, Edge>>,
    /// `Graph.destinationEdgeIndex`.
    destination_edge_index: HashMap<VertexId, IntSet>,
    destination_edge_threshold: usize,
}

/// `Graph` (graph.go).
#[derive(Debug)]
pub struct NodeGraph {
    inner: RwLock<Inner>,
}

impl Default for NodeGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeGraph {
    /// `NewGraph`.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                next_id: 0,
                nodes: HashMap::new(),
                vertices: HashMap::new(),
                adj: HashMap::new(),
                destination_edge_index: HashMap::new(),
                destination_edge_threshold: DEFAULT_DESTINATION_EDGE_THRESHOLD,
            }),
        }
    }

    /// Test hook for `Graph.destinationEdgeThreshold`.
    pub fn set_destination_edge_threshold(&self, threshold: usize) {
        self.inner.write().unwrap().destination_edge_threshold = threshold;
    }

    /// `hasPathFrom` (node_authorizer.go:499-553): is there a directed path from
    /// the specified type/namespace/name to the node. The `Err` strings are
    /// upstream's error texts.
    pub fn has_path_from(
        &self,
        node_name: &str,
        starting_type: VertexType,
        starting_namespace: &str,
        starting_name: &str,
    ) -> Result<bool, String> {
        let g = self.inner.read().unwrap();
        let type_name = starting_type.name();

        let Some(node_vertex) = g.get_vertex(VertexType::Node, "", node_name) else {
            return Err(format!(
                "unknown node '{node_name}' cannot get {type_name} {starting_namespace}/{starting_name}"
            ));
        };
        let Some(starting_vertex) = g.get_vertex(starting_type, starting_namespace, starting_name)
        else {
            return Err(format!(
                "node '{node_name}' cannot get unknown {type_name} {starting_namespace}/{starting_name}"
            ));
        };

        let no_relationship = || {
            format!(
                "node '{node_name}' cannot get {type_name} {starting_namespace}/{starting_name}, no relationship to this object was found in the node authorizer graph"
            )
        };

        if let Some(index) = g.destination_edge_index.get(&starting_vertex) {
            // Fast check to see if we know of a destination edge.
            if index.has(node_vertex) {
                return Ok(true);
            }
            // For some types of vertices, the destination edge index is
            // authoritative (as long as it exists), hence fail fast instead of
            // running a potentially costly DFS.
            if starting_type.has_authoritative_index() {
                return Err(no_relationship());
            }
        }

        // `traverse.VisitingDepthFirst` with the upstream `EdgeFilter`: follow
        // directed edges From(t); a destination edge leading to another node is
        // not followed, one leading to this node means found.
        let mut found = false;
        let mut visited: HashSet<VertexId> = HashSet::new();
        let mut stack = vec![starting_vertex];
        visited.insert(starting_vertex);
        if starting_vertex == node_vertex {
            found = true;
        }
        while !found {
            let Some(t) = stack.pop() else { break };
            let Some(edges) = g.adj.get(&t) else { continue };
            for (n, edge) in edges {
                if edge.from != t {
                    continue; // VisitFrom only follows edges leaving t.
                }
                if let Some(dest) = edge.destination {
                    if dest != node_vertex {
                        continue; // Don't follow edges leading to other nodes.
                    }
                    found = true; // We found an edge leading to the node we want.
                }
                if visited.insert(*n) {
                    stack.push(*n);
                    if *n == node_vertex {
                        found = true;
                    }
                }
                if found {
                    break;
                }
            }
        }
        if !found {
            return Err(no_relationship());
        }
        Ok(true)
    }

    /// `Graph.AddPod`.
    pub fn add_pod(&self, pod: &Value) {
        let mut g = self.inner.write().unwrap();
        let ns = meta_str(pod, "namespace");
        let name = meta_str(pod, "name");
        let node_name = str_at(pod, &["spec", "nodeName"]);

        g.delete_vertex(VertexType::Pod, ns, name);
        let pod_vertex = g.get_or_create_vertex(VertexType::Pod, ns, name);
        let node_vertex = g.get_or_create_vertex(VertexType::Node, "", node_name);
        g.add_edge(pod_vertex, node_vertex, Some(node_vertex));

        if get(pod, &["metadata", "annotations"])
            .and_then(|a| a.get(MIRROR_POD_ANNOTATION_KEY))
            .is_some()
        {
            return;
        }

        let sa = str_at(pod, &["spec", "serviceAccountName"]);
        if !sa.is_empty() {
            let sa_vertex = g.get_or_create_vertex(VertexType::ServiceAccount, ns, sa);
            g.add_edge(sa_vertex, pod_vertex, Some(node_vertex));
        }

        for secret in visit_pod_secret_names(pod) {
            let v = g.get_or_create_vertex(VertexType::Secret, ns, &secret);
            g.add_edge(v, pod_vertex, Some(node_vertex));
        }

        for cm in visit_pod_configmap_names(pod) {
            let v = g.get_or_create_vertex(VertexType::ConfigMap, ns, &cm);
            g.add_edge(v, pod_vertex, Some(node_vertex));
        }

        for vol in array_at(pod, &["spec", "volumes"]) {
            let mut claim_name = String::new();
            if let Some(pvc) = get(vol, &["persistentVolumeClaim"]) {
                claim_name = str_at(pvc, &["claimName"]).to_string();
            } else if get(vol, &["ephemeral"]).is_some() {
                // ephemeral.VolumeClaimName
                claim_name = format!("{}-{}", name, str_at(vol, &["name"]));
            }
            if !claim_name.is_empty() {
                let v = g.get_or_create_vertex(VertexType::Pvc, ns, &claim_name);
                g.add_edge(v, pod_vertex, Some(node_vertex));
            }
        }

        for prc in array_at(pod, &["spec", "resourceClaims"]) {
            if let Some(claim_name) = resource_claim_name(pod, prc) {
                let v = g.get_or_create_vertex(VertexType::ResourceClaim, ns, &claim_name);
                g.add_edge(v, pod_vertex, Some(node_vertex));
            }
        }

        let extended = str_at(
            pod,
            &["status", "extendedResourceClaimStatus", "resourceClaimName"],
        );
        if !extended.is_empty() {
            let v = g.get_or_create_vertex(VertexType::ResourceClaim, ns, extended);
            g.add_edge(v, pod_vertex, Some(node_vertex));
        }
    }

    /// `Graph.DeletePod`.
    pub fn delete_pod(&self, name: &str, namespace: &str) {
        self.inner
            .write()
            .unwrap()
            .delete_vertex(VertexType::Pod, namespace, name);
    }

    /// `Graph.AddPodCertificateRequest`.
    pub fn add_pod_certificate_request(&self, pcr: &Value) {
        let mut g = self.inner.write().unwrap();
        let ns = meta_str(pcr, "namespace");
        let name = meta_str(pcr, "name");
        g.delete_vertex(VertexType::Pcr, ns, name);
        let pcr_vertex = g.get_or_create_vertex(VertexType::Pcr, ns, name);
        let node_vertex =
            g.get_or_create_vertex(VertexType::Node, "", str_at(pcr, &["spec", "nodeName"]));
        g.add_edge(pcr_vertex, node_vertex, Some(node_vertex));
    }

    /// `Graph.DeletePodCertificateRequest`.
    pub fn delete_pod_certificate_request(&self, namespace: &str, name: &str) {
        self.inner
            .write()
            .unwrap()
            .delete_vertex(VertexType::Pcr, namespace, name);
    }

    /// `Graph.AddPV`.
    pub fn add_pv(&self, pv: &Value) {
        let mut g = self.inner.write().unwrap();
        let name = meta_str(pv, "name");
        g.delete_vertex(VertexType::Pv, "", name);

        // Skip adding edges to the PV if it does not have a claim: the secrets
        // are only reachable through the PV <- PVC <- pod relationship.
        if let Some(claim_ref) = get(pv, &["spec", "claimRef"]) {
            let pv_vertex = g.get_or_create_vertex(VertexType::Pv, "", name);
            let pvc_vertex = g.get_or_create_vertex(
                VertexType::Pvc,
                str_at(claim_ref, &["namespace"]),
                str_at(claim_ref, &["name"]),
            );
            g.add_edge(pv_vertex, pvc_vertex, None);
            for (namespace, secret, kubelet_visible) in visit_pv_secret_names(pv) {
                if kubelet_visible {
                    let sv = g.get_or_create_vertex(VertexType::Secret, &namespace, &secret);
                    g.add_edge(sv, pv_vertex, None);
                }
            }
        }
    }

    /// `Graph.DeletePV`.
    pub fn delete_pv(&self, name: &str) {
        self.inner
            .write()
            .unwrap()
            .delete_vertex(VertexType::Pv, "", name);
    }

    /// `Graph.AddVolumeAttachment`.
    pub fn add_volume_attachment(&self, attachment_name: &str, node_name: &str) {
        let mut g = self.inner.write().unwrap();
        g.delete_vertex(VertexType::Va, "", attachment_name);
        if !node_name.is_empty() {
            let va = g.get_or_create_vertex(VertexType::Va, "", attachment_name);
            let node = g.get_or_create_vertex(VertexType::Node, "", node_name);
            g.add_edge(va, node, Some(node));
        }
    }

    /// `Graph.DeleteVolumeAttachment`.
    pub fn delete_volume_attachment(&self, name: &str) {
        self.inner
            .write()
            .unwrap()
            .delete_vertex(VertexType::Va, "", name);
    }

    /// `Graph.AddResourceSlice`.
    pub fn add_resource_slice(&self, slice_name: &str, node_name: &str) {
        let mut g = self.inner.write().unwrap();
        g.delete_vertex(VertexType::Slice, "", slice_name);
        if !node_name.is_empty() {
            let slice = g.get_or_create_vertex(VertexType::Slice, "", slice_name);
            let node = g.get_or_create_vertex(VertexType::Node, "", node_name);
            g.add_edge(slice, node, Some(node));
        }
    }

    /// `Graph.DeleteResourceSlice`.
    pub fn delete_resource_slice(&self, slice_name: &str) {
        self.inner
            .write()
            .unwrap()
            .delete_vertex(VertexType::Slice, "", slice_name);
    }

    /// `deleteEdgesLocked` (graph.go), kept for its upstream test.
    #[cfg(test)]
    fn delete_edges(
        &self,
        from_type: VertexType,
        to_type: VertexType,
        to_namespace: &str,
        to_name: &str,
    ) {
        self.inner
            .write()
            .unwrap()
            .delete_edges(from_type, to_type, to_namespace, to_name);
    }
}

impl Inner {
    /// `getVertexRLocked`.
    fn get_vertex(&self, t: VertexType, ns: &str, name: &str) -> Option<VertexId> {
        self.vertices.get(&t)?.get(ns)?.get(name).copied()
    }

    /// `getOrCreateVertexLocked`.
    fn get_or_create_vertex(&mut self, t: VertexType, ns: &str, name: &str) -> VertexId {
        if let Some(id) = self.get_vertex(t, ns, name) {
            return id;
        }
        self.create_vertex(t, ns, name)
    }

    /// `createVertexLocked`.
    fn create_vertex(&mut self, t: VertexType, ns: &str, name: &str) -> VertexId {
        let id = self.next_id;
        self.next_id += 1;
        self.vertices
            .entry(t)
            .or_default()
            .entry(ns.to_string())
            .or_default()
            .insert(name.to_string(), id);
        self.nodes.insert(
            id,
            NamedVertex {
                vertex_type: t,
                namespace: ns.to_string(),
                name: name.to_string(),
            },
        );
        self.adj.entry(id).or_default();
        id
    }

    /// `graph.Degree`: the number of adjacent vertices.
    fn degree(&self, id: VertexId) -> usize {
        self.adj.get(&id).map_or(0, HashMap::len)
    }

    /// `simple.DirectedAcyclicGraph.SetEdge`: replaces any edge between the pair.
    fn set_edge(&mut self, e: Edge) {
        assert!(e.from != e.to, "node authorizer graph: self edge");
        self.adj.entry(e.from).or_default().insert(e.to, e);
        self.adj.entry(e.to).or_default().insert(e.from, e);
    }

    #[cfg(test)]
    fn remove_edge(&mut self, from: VertexId, to: VertexId) {
        if let Some(m) = self.adj.get_mut(&from) {
            m.remove(&to);
        }
        if let Some(m) = self.adj.get_mut(&to) {
            m.remove(&from);
        }
    }

    /// `graph.RemoveNode`: drops the node and every incident edge.
    fn remove_node(&mut self, id: VertexId) {
        if let Some(neighbors) = self.adj.remove(&id) {
            for n in neighbors.keys() {
                if let Some(m) = self.adj.get_mut(n) {
                    m.remove(&id);
                }
            }
        }
        self.nodes.remove(&id);
    }

    /// `addEdgeLocked`.
    fn add_edge(&mut self, from: VertexId, to: VertexId, destination: Option<VertexId>) {
        if destination.is_some() {
            let e = Edge {
                from,
                to,
                destination,
            };
            self.set_edge(e);
            self.add_edge_to_destination_index(e);
            return;
        }
        let from_type = self.nodes[&from].vertex_type;
        // Upstream panics; a malformed edge here is a programming error, but the
        // API server must not abort on it.
        debug_assert!(
            !from_type.has_authoritative_index(),
            "vertex of type {:?} must have destination edges only",
            from_type.name()
        );
        self.set_edge(Edge {
            from,
            to,
            destination: None,
        });
    }

    /// `removeVertexLocked`.
    fn remove_vertex(&mut self, id: VertexId) {
        let Some(v) = self.nodes.get(&id).cloned() else {
            return;
        };
        self.remove_node(id);
        self.destination_edge_index.remove(&id);
        if let Some(by_ns) = self.vertices.get_mut(&v.vertex_type) {
            if let Some(by_name) = by_ns.get_mut(&v.namespace) {
                by_name.remove(&v.name);
                if by_name.is_empty() {
                    by_ns.remove(&v.namespace);
                }
            }
        }
    }

    /// `deleteVertexLocked`: removes the vertex and any neighbor left orphaned,
    /// and keeps the destination index in step.
    fn delete_vertex(&mut self, t: VertexType, ns: &str, name: &str) {
        let Some(vertex) = self.get_vertex(t, ns, name) else {
            return;
        };

        let mut neighbors_to_remove: Vec<VertexId> = Vec::new();
        let mut edges_to_remove_from_indexes: Vec<Edge> = Vec::new();
        let edges: Vec<(VertexId, Edge)> = self
            .adj
            .get(&vertex)
            .map(|m| m.iter().map(|(n, e)| (*n, *e)).collect())
            .unwrap_or_default();
        // VisitFrom
        for (neighbor, e) in &edges {
            if e.from == vertex && self.degree(*neighbor) == 1 {
                neighbors_to_remove.push(*neighbor);
            }
        }
        // VisitTo
        for (neighbor, e) in &edges {
            if e.to == vertex {
                if self.degree(*neighbor) == 1 {
                    neighbors_to_remove.push(*neighbor);
                } else {
                    edges_to_remove_from_indexes.push(*e);
                }
            }
        }

        self.remove_vertex(vertex);
        for n in neighbors_to_remove {
            self.remove_vertex(n);
        }
        for e in edges_to_remove_from_indexes {
            self.remove_edge_from_destination_index(e);
        }
    }

    /// `deleteEdgesLocked`.
    #[cfg(test)]
    fn delete_edges(
        &mut self,
        from_type: VertexType,
        to_type: VertexType,
        to_namespace: &str,
        to_name: &str,
    ) {
        let Some(to_vert) = self.get_vertex(to_type, to_namespace, to_name) else {
            return;
        };
        let mut neighbors_to_remove = Vec::new();
        let mut edges_to_remove = Vec::new();
        let edges: Vec<(VertexId, Edge)> = self
            .adj
            .get(&to_vert)
            .map(|m| m.iter().map(|(n, e)| (*n, *e)).collect())
            .unwrap_or_default();
        for (from, e) in edges {
            if e.to != to_vert {
                continue;
            }
            if self.nodes[&from].vertex_type != from_type {
                continue;
            }
            if self.degree(from) == 1 {
                neighbors_to_remove.push(from);
            } else {
                edges_to_remove.push(e);
            }
        }
        for v in neighbors_to_remove {
            self.remove_vertex(v);
        }
        for e in edges_to_remove {
            self.remove_edge(e.from, e.to);
            self.remove_edge_from_destination_index(e);
        }
    }

    /// `removeEdgeFromDestinationIndexLocked`.
    fn remove_edge_from_destination_index(&mut self, e: Edge) {
        let n = e.from;
        if self.degree(n) < self.destination_edge_threshold {
            self.destination_edge_index.remove(&n);
            return;
        }
        let Some(index) = self.destination_edge_index.get_mut(&n) else {
            return;
        };
        if let Some(dest) = e.destination {
            index.decrement(dest);
        }
    }

    /// `addEdgeToDestinationIndexLocked`.
    fn add_edge_to_destination_index(&mut self, e: Edge) {
        let n = e.from;
        if !self.destination_edge_index.contains_key(&n) {
            self.recompute_destination_index(n);
            return;
        }
        if let (Some(dest), Some(index)) = (e.destination, self.destination_edge_index.get_mut(&n))
        {
            index.increment(dest);
        }
    }

    /// `recomputeDestinationIndexLocked`.
    fn recompute_destination_index(&mut self, n: VertexId) {
        if self.degree(n) < self.destination_edge_threshold {
            self.destination_edge_index.remove(&n);
            return;
        }
        let mut index = self.destination_edge_index.remove(&n).unwrap_or_default();
        index.reset();
        if let Some(edges) = self.adj.get(&n) {
            for e in edges.values() {
                // VisitFrom(n): edges leaving n.
                if e.from == n {
                    if let Some(dest) = e.destination {
                        index.increment(dest);
                    }
                }
            }
        }
        self.destination_edge_index.insert(n, index);
    }
}

// ---------------------------------------------------------------------------
// JSON access helpers
// ---------------------------------------------------------------------------

/// Walks `path`, treating JSON `null` as absent (a Go nil pointer / slice).
fn get<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = v;
    for key in path {
        cur = cur.get(*key)?;
    }
    if cur.is_null() {
        None
    } else {
        Some(cur)
    }
}

fn str_at<'a>(v: &'a Value, path: &[&str]) -> &'a str {
    get(v, path).and_then(Value::as_str).unwrap_or("")
}

fn meta_str<'a>(v: &'a Value, key: &str) -> &'a str {
    str_at(v, &["metadata", key])
}

fn array_at<'a>(v: &'a Value, path: &[&str]) -> impl Iterator<Item = &'a Value> {
    get(v, path)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|x| !x.is_null())
}

// ---------------------------------------------------------------------------
// Visitors
// ---------------------------------------------------------------------------

/// `ContainerIter` with `AllContainers`: init containers, containers, then
/// ephemeral containers (pkg/api/v1/pod/util.go).
fn all_containers(pod: &Value) -> impl Iterator<Item = &Value> {
    array_at(pod, &["spec", "initContainers"])
        .chain(array_at(pod, &["spec", "containers"]))
        .chain(array_at(pod, &["spec", "ephemeralContainers"]))
}

/// `VisitPodSecretNames` (pkg/api/v1/pod/util.go): every secret the pod spec
/// references, empty names skipped (`skipEmptyNames`). Transitive references
/// (pod -> pvc -> pv -> secret) are not visited.
pub fn visit_pod_secret_names(pod: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut visit = |n: &str| {
        if !n.is_empty() {
            out.push(n.to_string());
        }
    };
    for r in array_at(pod, &["spec", "imagePullSecrets"]) {
        visit(str_at(r, &["name"]));
    }
    for c in all_containers(pod) {
        // visitContainerSecretNames
        for env in array_at(c, &["envFrom"]) {
            if get(env, &["secretRef"]).is_some() {
                visit(str_at(env, &["secretRef", "name"]));
            }
        }
        for env in array_at(c, &["env"]) {
            if get(env, &["valueFrom", "secretKeyRef"]).is_some() {
                visit(str_at(env, &["valueFrom", "secretKeyRef", "name"]));
            }
        }
    }
    for vol in array_at(pod, &["spec", "volumes"]) {
        // The upstream `switch` takes the first non-nil source, in this order.
        if get(vol, &["azureFile"]).is_some() {
            visit(str_at(vol, &["azureFile", "secretName"]));
        } else if get(vol, &["cephfs"]).is_some() {
            if get(vol, &["cephfs", "secretRef"]).is_some() {
                visit(str_at(vol, &["cephfs", "secretRef", "name"]));
            }
        } else if get(vol, &["cinder"]).is_some() {
            if get(vol, &["cinder", "secretRef"]).is_some() {
                visit(str_at(vol, &["cinder", "secretRef", "name"]));
            }
        } else if get(vol, &["flexVolume"]).is_some() {
            if get(vol, &["flexVolume", "secretRef"]).is_some() {
                visit(str_at(vol, &["flexVolume", "secretRef", "name"]));
            }
        } else if get(vol, &["projected"]).is_some() {
            for s in array_at(vol, &["projected", "sources"]) {
                if get(s, &["secret"]).is_some() {
                    visit(str_at(s, &["secret", "name"]));
                }
            }
        } else if get(vol, &["rbd"]).is_some() {
            if get(vol, &["rbd", "secretRef"]).is_some() {
                visit(str_at(vol, &["rbd", "secretRef", "name"]));
            }
        } else if get(vol, &["secret"]).is_some() {
            visit(str_at(vol, &["secret", "secretName"]));
        } else if get(vol, &["scaleIO"]).is_some() {
            if get(vol, &["scaleIO", "secretRef"]).is_some() {
                visit(str_at(vol, &["scaleIO", "secretRef", "name"]));
            }
        } else if get(vol, &["iscsi"]).is_some() {
            if get(vol, &["iscsi", "secretRef"]).is_some() {
                visit(str_at(vol, &["iscsi", "secretRef", "name"]));
            }
        } else if get(vol, &["storageos"]).is_some() {
            if get(vol, &["storageos", "secretRef"]).is_some() {
                visit(str_at(vol, &["storageos", "secretRef", "name"]));
            }
        } else if get(vol, &["csi"]).is_some()
            && get(vol, &["csi", "nodePublishSecretRef"]).is_some()
        {
            visit(str_at(vol, &["csi", "nodePublishSecretRef", "name"]));
        }
    }
    out
}

/// `VisitPodConfigmapNames` (pkg/api/v1/pod/util.go).
pub fn visit_pod_configmap_names(pod: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut visit = |n: &str| {
        if !n.is_empty() {
            out.push(n.to_string());
        }
    };
    for c in all_containers(pod) {
        // visitContainerConfigmapNames
        for env in array_at(c, &["envFrom"]) {
            if get(env, &["configMapRef"]).is_some() {
                visit(str_at(env, &["configMapRef", "name"]));
            }
        }
        for env in array_at(c, &["env"]) {
            if get(env, &["valueFrom", "configMapKeyRef"]).is_some() {
                visit(str_at(env, &["valueFrom", "configMapKeyRef", "name"]));
            }
        }
    }
    for vol in array_at(pod, &["spec", "volumes"]) {
        if get(vol, &["projected"]).is_some() {
            for s in array_at(vol, &["projected", "sources"]) {
                if get(s, &["configMap"]).is_some() {
                    visit(str_at(s, &["configMap", "name"]));
                }
            }
        } else if get(vol, &["configMap"]).is_some() {
            visit(str_at(vol, &["configMap", "name"]));
        }
    }
    out
}

/// `VisitPVSecretNames` (pkg/api/v1/persistentvolume/util.go): `(namespace,
/// name, kubeletVisible)` for every secret the PV references, empty names
/// skipped.
pub fn visit_pv_secret_names(pv: &Value) -> Vec<(String, String, bool)> {
    let claim_ref_ns = str_at(pv, &["spec", "claimRef", "namespace"]);
    let mut out = Vec::new();
    let mut visit = |ns: &str, name: &str, kubelet_visible: bool| {
        if !name.is_empty() {
            out.push((ns.to_string(), name.to_string(), kubelet_visible));
        }
    };
    let src = |k: &str| get(pv, &["spec", k]);
    // A secretRef whose namespace falls back to the claimRef namespace.
    let ref_or_claim_ns = |k: &str| -> Option<(String, String)> {
        let r = get(pv, &["spec", k, "secretRef"])?;
        let ns = str_at(r, &["namespace"]);
        let ns = if ns.is_empty() { claim_ref_ns } else { ns };
        Some((ns.to_string(), str_at(r, &["name"]).to_string()))
    };
    let ref_plain = |k: &str| -> Option<(String, String)> {
        let r = get(pv, &["spec", k, "secretRef"])?;
        Some((
            str_at(r, &["namespace"]).to_string(),
            str_at(r, &["name"]).to_string(),
        ))
    };

    if src("azureFile").is_some() {
        let name = str_at(pv, &["spec", "azureFile", "secretName"]);
        let ns = str_at(pv, &["spec", "azureFile", "secretNamespace"]);
        let ns = if ns.is_empty() { claim_ref_ns } else { ns };
        visit(ns, name, true);
    } else if src("cephfs").is_some() {
        if let Some((ns, n)) = ref_or_claim_ns("cephfs") {
            visit(&ns, &n, true);
        }
    } else if src("cinder").is_some() {
        if let Some((ns, n)) = ref_plain("cinder") {
            visit(&ns, &n, true);
        }
    } else if src("flexVolume").is_some() {
        if let Some((ns, n)) = ref_or_claim_ns("flexVolume") {
            visit(&ns, &n, true);
        }
    } else if src("rbd").is_some() {
        if let Some((ns, n)) = ref_or_claim_ns("rbd") {
            visit(&ns, &n, true);
        }
    } else if src("scaleIO").is_some() {
        if let Some((ns, n)) = ref_or_claim_ns("scaleIO") {
            visit(&ns, &n, true);
        }
    } else if src("iscsi").is_some() {
        if let Some((ns, n)) = ref_or_claim_ns("iscsi") {
            visit(&ns, &n, true);
        }
    } else if src("storageos").is_some() {
        if let Some((ns, n)) = ref_plain("storageos") {
            visit(&ns, &n, true);
        }
    } else if src("csi").is_some() {
        for (field, kubelet_visible) in [
            ("controllerPublishSecretRef", false),
            ("controllerExpandSecretRef", false),
            ("nodePublishSecretRef", true),
            ("nodeStageSecretRef", true),
            ("nodeExpandSecretRef", true),
        ] {
            if let Some(r) = get(pv, &["spec", "csi", field]) {
                visit(
                    str_at(r, &["namespace"]),
                    str_at(r, &["name"]),
                    kubelet_visible,
                );
            }
        }
    }
    out
}

/// `resourceclaim.Name` (dynamic-resource-allocation/resourceclaim/resourceclaim.go:65):
/// the ResourceClaim a `PodResourceClaim` refers to, or `None` when the
/// upstream call returns a nil name or an error (both are skipped by the
/// graph's caller: `err == nil && claimName != nil`).
fn resource_claim_name(pod: &Value, pod_claim: &Value) -> Option<String> {
    if let Some(n) = get(pod_claim, &["resourceClaimName"]).and_then(Value::as_str) {
        return Some(n.to_string());
    }
    if get(pod_claim, &["resourceClaimTemplateName"]).is_some() {
        let claim = str_at(pod_claim, &["name"]);
        for status in array_at(pod, &["status", "resourceClaimStatuses"]) {
            if str_at(status, &["name"]) == claim {
                return get(status, &["resourceClaimName"])
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
        }
    }
    None
}

/// `resourceclaim.PodStatusEqual` (resourceclaim/pod.go:34).
fn pod_resource_claim_status_equal(a: &Value, b: &Value) -> bool {
    let a: Vec<&Value> = array_at(a, &["status", "resourceClaimStatuses"]).collect();
    let b: Vec<&Value> = array_at(b, &["status", "resourceClaimStatuses"]).collect();
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| {
        str_at(x, &["name"]) == str_at(y, &["name"])
            && get(x, &["resourceClaimName"]) == get(y, &["resourceClaimName"])
    })
}

// ---------------------------------------------------------------------------
// graph_populator.go: the event handlers
// ---------------------------------------------------------------------------

/// `graphPopulator.updatePod` (addPod is `updatePod(nil, obj)`).
pub fn populator_update_pod(graph: &NodeGraph, old: Option<&Value>, pod: &Value) {
    if str_at(pod, &["spec", "nodeName"]).is_empty() {
        return;
    }
    if let Some(old) = old {
        let has_new_ephemeral_containers = array_at(pod, &["spec", "ephemeralContainers"]).count()
            > array_at(old, &["spec", "ephemeralContainers"]).count();
        if str_at(pod, &["spec", "nodeName"]) == str_at(old, &["spec", "nodeName"])
            && str_at(pod, &["metadata", "uid"]) == str_at(old, &["metadata", "uid"])
            && !has_new_ephemeral_containers
            && pod_resource_claim_status_equal(old, pod)
        {
            return; // node unchanged
        }
    }
    graph.add_pod(pod);
}

/// `graphPopulator.deletePod`.
pub fn populator_delete_pod(graph: &NodeGraph, pod: &Value) {
    if str_at(pod, &["spec", "nodeName"]).is_empty() {
        return;
    }
    graph.delete_pod(meta_str(pod, "name"), meta_str(pod, "namespace"));
}

/// `graphPopulator.updatePV` / `addPV`.
pub fn populator_update_pv(graph: &NodeGraph, pv: &Value) {
    graph.add_pv(pv);
}

/// `graphPopulator.deletePV`.
pub fn populator_delete_pv(graph: &NodeGraph, pv: &Value) {
    graph.delete_pv(meta_str(pv, "name"));
}

/// `graphPopulator.updateVolumeAttachment`.
pub fn populator_update_volume_attachment(graph: &NodeGraph, old: Option<&Value>, va: &Value) {
    if let Some(old) = old {
        if str_at(old, &["spec", "nodeName"]) == str_at(va, &["spec", "nodeName"]) {
            return;
        }
    }
    graph.add_volume_attachment(meta_str(va, "name"), str_at(va, &["spec", "nodeName"]));
}

/// `graphPopulator.deleteVolumeAttachment`.
pub fn populator_delete_volume_attachment(graph: &NodeGraph, va: &Value) {
    graph.delete_volume_attachment(meta_str(va, "name"));
}

/// `graphPopulator.addResourceSlice` (no update handler: NodeName is immutable).
pub fn populator_add_resource_slice(graph: &NodeGraph, slice: &Value) {
    graph.add_resource_slice(
        meta_str(slice, "name"),
        str_at(slice, &["spec", "nodeName"]),
    );
}

/// `graphPopulator.deleteResourceSlice`.
pub fn populator_delete_resource_slice(graph: &NodeGraph, slice: &Value) {
    graph.delete_resource_slice(meta_str(slice, "name"));
}

/// `graphPopulator.addPCR` (no update handler: spec fields are immutable).
pub fn populator_add_pcr(graph: &NodeGraph, pcr: &Value) {
    graph.add_pod_certificate_request(pcr);
}

/// `graphPopulator.deletePCR`.
pub fn populator_delete_pcr(graph: &NodeGraph, pcr: &Value) {
    graph.delete_pod_certificate_request(meta_str(pcr, "namespace"), meta_str(pcr, "name"));
}

#[cfg(test)]
mod tests;
