//! The Node authorizer's relationship graph.
//!
//! Ported from `plugin/pkg/auth/authorizer/node/graph.go` (`Graph`, `AddPod`,
//! `AddPV`, `AddVolumeAttachment`, `AddResourceSlice`,
//! `AddPodCertificateRequest`, the destination edge index) and `intset.go`
//! (`intSet`), together with the storage of the forked gonum
//! `simple.DirectedAcyclicGraph` they sit on
//! (`third_party/forked/gonum/graph/simple/{undirected,directed_acyclic}.go`):
//! one edge per vertex pair, direction recorded on the edge, node IDs reused
//! lowest-first.
//!
//! All edges point toward the vertices representing Kubernetes nodes:
//!
//! ```text
//! node <- pod
//! pod  <- secret,configmap,pvc,serviceaccount,resourceclaim
//! pvc  <- pv
//! pv   <- secret
//! ```
//!
//! The populator that feeds the graph (`graph_populator.go`) is in
//! [`GraphPopulator`]; the decisions made on it are in `node_authorizer.rs`.

use crate::resources::podcertificaterequest::PodCertificateRequest;
use crate::resources::{PersistentVolume, Pod};
use prometheus::{register, HistogramOpts, HistogramVec};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{LazyLock, RwLock};
use std::time::Instant;

/// `corev1.MirrorPodAnnotationKey`.
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";

/// `destinationEdgeThreshold` default (graph.go `NewGraph`): "experimentally
/// determined to be the point at which iteration adds an order of magnitude to
/// the authz check".
const DEFAULT_DESTINATION_EDGE_THRESHOLD: usize = 200;

// ---------------------------------------------------------------------------
// metrics.go
// ---------------------------------------------------------------------------

/// `node_authorizer_graph_actions_duration_seconds` (metrics.go:28-37):
/// `ExponentialBuckets(0.0001, 2, 12)`, label `operation`.
static GRAPH_ACTIONS_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "node_authorizer_graph_actions_duration_seconds",
            "[ALPHA] Histogram of duration of graph actions in node authorizer.",
        )
        .buckets((0..12).map(|i| 0.0001 * 2f64.powi(i)).collect()),
        &["operation"],
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
});

/// `RegisterMetrics` (metrics.go:43-48).
pub fn register_metrics() {
    LazyLock::force(&GRAPH_ACTIONS_DURATION);
}

fn observe(operation: &str, start: Instant) {
    GRAPH_ACTIONS_DURATION
        .with_label_values(&[operation])
        .observe(start.elapsed().as_secs_f64());
}

// ---------------------------------------------------------------------------
// intset.go
// ---------------------------------------------------------------------------

/// `intSet` (intset.go): a map of id to refcounts.
#[derive(Debug, Default)]
pub struct IntSet {
    pub members: HashMap<usize, usize>,
}

impl IntSet {
    /// `has`: true if the id has a positive refcount.
    pub fn has(&self, i: usize) -> bool {
        self.members.get(&i).copied().unwrap_or(0) > 0
    }

    /// `reset`: removes all ids.
    pub fn reset(&mut self) {
        self.members.clear();
    }

    /// `increment`: adds one to the refcount of the id.
    pub fn increment(&mut self, i: usize) {
        *self.members.entry(i).or_insert(0) += 1;
    }

    /// `decrement`: removes one from the refcount, removing the id when the
    /// result is <= 0. Refcounts below zero are not tracked.
    pub fn decrement(&mut self, i: usize) {
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

// ---------------------------------------------------------------------------
// graph.go
// ---------------------------------------------------------------------------

/// `vertexType`: the type of the API object a vertex represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    /// `vertexTypes` (graph.go): the name used in `namedVertex.String()` and
    /// in the relationship error messages.
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

    /// `vertexTypeWithAuthoritativeIndex` (graph.go): vertex types whose
    /// destination edge index, if it exists, always holds every Node
    /// reachable from the vertex.
    pub fn has_authoritative_index(self) -> bool {
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

/// `namedVertex`: remembers the type, namespace, and name of its API object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedVertex {
    pub(crate) vertex_type: VertexType,
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) id: usize,
}

impl NamedVertex {
    /// `namedVertex.String`.
    pub fn describe(&self) -> String {
        if self.namespace.is_empty() {
            format!("{}:{}", self.vertex_type.name(), self.name)
        } else {
            format!(
                "{}:{}/{}",
                self.vertex_type.name(),
                self.namespace,
                self.name
            )
        }
    }
}

/// A directed edge. `destination` is `Some` for a `destinationEdge`, "a graph
/// edge that includes a denormalized reference to the final destination
/// vertex. This should only be used when there is a single leaf vertex
/// reachable from T." (graph.go `destinationEdge`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Edge {
    pub from: usize,
    pub to: usize,
    pub destination: Option<usize>,
}

/// The graph state behind [`NodeGraph`]'s lock. The `*_locked` methods are
/// upstream's "must be called under (write) lock" methods.
#[derive(Debug)]
pub struct GraphInner {
    // simple.UndirectedGraph storage.
    pub nodes: HashMap<usize, NamedVertex>,
    /// node id -> neighbor id -> the single edge between them (stored under
    /// both endpoints, as `UndirectedGraph.edges` does).
    pub edges: HashMap<usize, HashMap<usize, Edge>>,
    pub free_ids: BTreeSet<usize>,
    pub used_ids: BTreeSet<usize>,

    /// `vertices`: type -> namespace -> name -> vertex id.
    pub vertices: HashMap<VertexType, HashMap<String, HashMap<String, usize>>>,
    /// `destinationEdgeIndex`: vertex -> set of destination IDs.
    pub destination_edge_index: HashMap<usize, IntSet>,
    /// `destinationEdgeThreshold`: the minimum number of distinct destination
    /// IDs at which to maintain an index.
    pub destination_edge_threshold: usize,
}

impl GraphInner {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            free_ids: BTreeSet::new(),
            used_ids: BTreeSet::new(),
            vertices: HashMap::new(),
            destination_edge_index: HashMap::new(),
            destination_edge_threshold: DEFAULT_DESTINATION_EDGE_THRESHOLD,
        }
    }

    // --- simple.UndirectedGraph / DirectedAcyclicGraph -------------------

    /// `UndirectedGraph.NewNodeID`.
    fn new_node_id(&mut self) -> usize {
        if self.nodes.is_empty() {
            return 0;
        }
        if let Some(id) = self.free_ids.pop_first() {
            return id;
        }
        self.used_ids.last().map_or(0, |max| max + 1)
    }

    /// `UndirectedGraph.AddNode`.
    fn add_node(&mut self, vertex: NamedVertex) {
        let id = vertex.id;
        assert!(
            !self.nodes.contains_key(&id),
            "simple: node ID collision: {id}"
        );
        self.nodes.insert(id, vertex);
        self.edges.insert(id, HashMap::new());
        self.free_ids.remove(&id);
        self.used_ids.insert(id);
    }

    /// `UndirectedGraph.RemoveNode`.
    fn remove_node(&mut self, id: usize) {
        if self.nodes.remove(&id).is_none() {
            return;
        }
        if let Some(neighbors) = self.edges.remove(&id) {
            for neighbor in neighbors.keys() {
                if let Some(held) = self.edges.get_mut(neighbor) {
                    held.remove(&id);
                }
            }
        }
        self.free_ids.insert(id);
        self.used_ids.remove(&id);
    }

    /// `UndirectedGraph.SetEdge`.
    fn set_edge(&mut self, e: Edge) {
        assert!(e.from != e.to, "simple: adding self edge");
        for id in [e.from, e.to] {
            assert!(self.nodes.contains_key(&id), "simple: edge to absent node");
        }
        self.edges.entry(e.from).or_default().insert(e.to, e);
        self.edges.entry(e.to).or_default().insert(e.from, e);
    }

    /// `UndirectedGraph.RemoveEdge`.
    pub fn remove_edge(&mut self, e: Edge) {
        if !self.nodes.contains_key(&e.from) || !self.nodes.contains_key(&e.to) {
            return;
        }
        if let Some(held) = self.edges.get_mut(&e.from) {
            held.remove(&e.to);
        }
        if let Some(held) = self.edges.get_mut(&e.to) {
            held.remove(&e.from);
        }
    }

    /// `UndirectedGraph.Degree`.
    fn degree(&self, id: usize) -> usize {
        self.edges.get(&id).map_or(0, HashMap::len)
    }

    /// `UndirectedGraph.EdgeBetween`.
    fn edge_between(&self, a: usize, b: usize) -> Option<Edge> {
        self.edges.get(&a).and_then(|m| m.get(&b)).copied()
    }

    /// `DirectedAcyclicGraph.VisitFrom`: neighbors reachable by an edge
    /// directed away from `id`.
    pub fn neighbors_from(&self, id: usize) -> Vec<usize> {
        self.edges.get(&id).map_or_else(Vec::new, |m| {
            m.values().filter(|e| e.from == id).map(|e| e.to).collect()
        })
    }

    /// `DirectedAcyclicGraph.VisitTo`: neighbors with an edge directed at `id`.
    fn neighbors_to(&self, id: usize) -> Vec<usize> {
        self.edges.get(&id).map_or_else(Vec::new, |m| {
            m.values().filter(|e| e.to == id).map(|e| e.from).collect()
        })
    }

    // --- Graph vertex bookkeeping -----------------------------------------

    /// `getVertexRLocked`.
    pub(crate) fn get_vertex(
        &self,
        vertex_type: VertexType,
        namespace: &str,
        name: &str,
    ) -> Option<&NamedVertex> {
        let id = self.vertices.get(&vertex_type)?.get(namespace)?.get(name)?;
        self.nodes.get(id)
    }

    fn get_vertex_id(&self, vertex_type: VertexType, namespace: &str, name: &str) -> Option<usize> {
        self.get_vertex(vertex_type, namespace, name).map(|v| v.id)
    }

    /// `getOrCreateVertexLocked`.
    pub fn get_or_create_vertex(
        &mut self,
        vertex_type: VertexType,
        namespace: &str,
        name: &str,
    ) -> usize {
        if let Some(id) = self.get_vertex_id(vertex_type, namespace, name) {
            return id;
        }
        self.create_vertex(vertex_type, namespace, name)
    }

    /// `createVertexLocked`.
    fn create_vertex(&mut self, vertex_type: VertexType, namespace: &str, name: &str) -> usize {
        let id = self.new_node_id();
        self.vertices
            .entry(vertex_type)
            .or_default()
            .entry(namespace.to_string())
            .or_default()
            .insert(name.to_string(), id);
        self.add_node(NamedVertex {
            vertex_type,
            namespace: namespace.to_string(),
            name: name.to_string(),
            id,
        });
        id
    }

    /// `deleteVertexLocked`.
    fn delete_vertex(&mut self, vertex_type: VertexType, namespace: &str, name: &str) {
        let Some(vertex) = self.get_vertex_id(vertex_type, namespace, name) else {
            return;
        };

        // find existing neighbors with a single edge (meaning we are their only neighbor)
        let mut neighbors_to_remove = Vec::new();
        let mut edges_to_remove_from_indexes = Vec::new();
        for neighbor in self.neighbors_from(vertex) {
            // this downstream neighbor has only one edge (which must be from us), so remove them as well
            if self.degree(neighbor) == 1 {
                neighbors_to_remove.push(neighbor);
            }
        }
        for neighbor in self.neighbors_to(vertex) {
            if self.degree(neighbor) == 1 {
                // this upstream neighbor has only one edge (which must be to us), so remove them as well
                neighbors_to_remove.push(neighbor);
            } else if let Some(edge) = self.edge_between(vertex, neighbor) {
                // decrement the destination edge index on this neighbor if the edge between us was a destination edge
                edges_to_remove_from_indexes.push(edge);
            }
        }

        // remove the vertex
        self.remove_vertex(vertex);

        // remove neighbors that are now edgeless
        for neighbor in neighbors_to_remove {
            self.remove_vertex(neighbor);
        }

        // remove edges from destination indexes for neighbors that dropped outbound edges
        for edge in edges_to_remove_from_indexes {
            self.remove_edge_from_destination_index(edge);
        }
    }

    /// `deleteEdgesLocked`: deletes edges from a given vertex type to a
    /// specific vertex; deletes each orphaned "from" vertex, but never the
    /// "to" vertex.
    pub fn delete_edges(
        &mut self,
        from_type: VertexType,
        to_type: VertexType,
        to_namespace: &str,
        to_name: &str,
    ) {
        let Some(to_vert) = self.get_vertex_id(to_type, to_namespace, to_name) else {
            return;
        };

        // delete all edges between vertices of fromType and toVert
        let mut neighbors_to_remove = Vec::new();
        let mut edges_to_remove = Vec::new();
        for from in self.neighbors_to(to_vert) {
            if self.nodes[&from].vertex_type != from_type {
                continue;
            }
            // this neighbor has only one edge (which must be to us), so remove them as well
            if self.degree(from) == 1 {
                neighbors_to_remove.push(from);
            } else if let Some(edge) = self.edge_between(from, to_vert) {
                edges_to_remove.push(edge);
            }
        }

        // clean up orphaned verts
        for v in neighbors_to_remove {
            self.remove_vertex(v);
        }

        // remove edges and decrement destination indexes for neighbors that dropped outbound edges
        for edge in edges_to_remove {
            self.remove_edge(edge);
            self.remove_edge_from_destination_index(edge);
        }
    }

    /// `removeVertexLocked`: removes the vertex from the graph and from the
    /// maintained indices. Does nothing to indexes of neighbor vertices.
    fn remove_vertex(&mut self, id: usize) {
        let Some(vertex) = self.nodes.get(&id).cloned() else {
            return;
        };
        self.remove_node(id);
        self.destination_edge_index.remove(&id);
        if let Some(by_namespace) = self.vertices.get_mut(&vertex.vertex_type) {
            if let Some(by_name) = by_namespace.get_mut(&vertex.namespace) {
                by_name.remove(&vertex.name);
                if by_name.is_empty() {
                    by_namespace.remove(&vertex.namespace);
                }
            }
        }
    }

    /// `removeEdgeFromDestinationIndexLocked`: a fastpath for
    /// `recomputeDestinationIndexLocked` for the "removing edge" case.
    fn remove_edge_from_destination_index(&mut self, e: Edge) {
        let n = e.from;
        // don't maintain indices for nodes with few edges
        if self.degree(n) < self.destination_edge_threshold {
            self.destination_edge_index.remove(&n);
            return;
        }
        // decrement the nodeID->destinationID refcount in the index, if the index exists
        let Some(index) = self.destination_edge_index.get_mut(&n) else {
            return;
        };
        if let Some(destination) = e.destination {
            index.decrement(destination);
        }
    }

    /// `addEdgeToDestinationIndexLocked`: a fastpath for
    /// `recomputeDestinationIndexLocked` for the "adding edge" case.
    fn add_edge_to_destination_index(&mut self, e: Edge) {
        let n = e.from;
        let Some(index) = self.destination_edge_index.get_mut(&n) else {
            // There is no index, use the full index computation method
            self.recompute_destination_index(n);
            return;
        };
        // fast-add the new edge to an existing index
        if let Some(destination) = e.destination {
            index.increment(destination);
        }
    }

    /// `recomputeDestinationIndexLocked`: recomputes the index of destination
    /// ids for the specified vertex.
    fn recompute_destination_index(&mut self, n: usize) {
        // don't maintain indices for nodes with few edges
        if self.degree(n) < self.destination_edge_threshold {
            self.destination_edge_index.remove(&n);
            return;
        }

        // get or create the index
        let mut index = self.destination_edge_index.remove(&n).unwrap_or_default();
        index.reset();

        // populate the index
        for dest in self.neighbors_from(n) {
            if let Some(Edge {
                destination: Some(destination),
                ..
            }) = self.edge_between(n, dest)
            {
                index.increment(destination);
            }
        }
        self.destination_edge_index.insert(n, index);
    }

    /// `addEdgeLocked`: all edge adds must be handled by this method rather
    /// than by calling `set_edge` directly. If `from` belongs to
    /// `vertexTypeWithAuthoritativeIndex`, `destination` must be non-nil.
    pub fn add_edge(&mut self, from: usize, to: usize, destination: Option<usize>) {
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

        // We must not create edges without a Node label from a vertex that is
        // supposed to hold authoritative destination edge index only.
        // Entering this branch would mean there's a bug in the Node authorizer.
        let from_type = self.nodes[&from].vertex_type;
        assert!(
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

    /// `hasPathFrom`'s body, under the read lock.
    pub fn has_path_from(
        &self,
        node_name: &str,
        starting_type: VertexType,
        starting_namespace: &str,
        starting_name: &str,
    ) -> Result<bool, String> {
        let Some(node_vertex) = self.get_vertex_id(VertexType::Node, "", node_name) else {
            return Err(format!(
                "unknown node '{}' cannot get {} {}/{}",
                node_name,
                starting_type.name(),
                starting_namespace,
                starting_name
            ));
        };

        let Some(starting_vertex) =
            self.get_vertex_id(starting_type, starting_namespace, starting_name)
        else {
            return Err(format!(
                "node '{}' cannot get unknown {} {}/{}",
                node_name,
                starting_type.name(),
                starting_namespace,
                starting_name
            ));
        };

        if let Some(index) = self.destination_edge_index.get(&starting_vertex) {
            // Fast check to see if we know of a destination edge
            if index.has(node_vertex) {
                return Ok(true);
            }
            // For some types of vertices, the destination edge index is authoritative
            // (as long as it exists), hence we can fail-fast here instead of running
            // a potentially costly DFS below.
            if starting_type.has_authoritative_index() {
                return Err(format!(
                    "node '{}' cannot get {} {}/{}, no relationship to this object was found in the node authorizer graph",
                    node_name,
                    starting_type.name(),
                    starting_namespace,
                    starting_name
                ));
            }
        }

        // traverse.VisitingDepthFirst{EdgeFilter: ...}.Walk
        // (third_party/forked/gonum/graph/traverse/visit_depth_first.go).
        let mut found = starting_vertex == node_vertex;
        let mut visited: HashSet<usize> = HashSet::new();
        let mut stack = vec![starting_vertex];
        visited.insert(starting_vertex);
        'walk: while !found {
            let Some(t) = stack.pop() else { break };
            for n in self.neighbors_from(t) {
                if let Some(edge) = self.edge_between(t, n) {
                    if let Some(destination) = edge.destination {
                        if destination != node_vertex {
                            // Don't follow edges leading to other nodes
                            continue;
                        }
                        // We found an edge leading to the node we want
                        found = true;
                    }
                }
                if visited.contains(&n) {
                    if found {
                        break 'walk;
                    }
                    continue;
                }
                visited.insert(n);
                stack.push(n);
                // Stop visiting if we've found the node we want
                if found || n == node_vertex {
                    found = true;
                    break 'walk;
                }
            }
        }
        if !found {
            return Err(format!(
                "node '{}' cannot get {} {}/{}, no relationship to this object was found in the node authorizer graph",
                node_name,
                starting_type.name(),
                starting_namespace,
                starting_name
            ));
        }
        Ok(true)
    }
}

/// `Graph`: holds graph vertices and a way to look up a vertex for a
/// particular API type/namespace/name.
#[derive(Debug)]
pub struct NodeGraph {
    inner: RwLock<GraphInner>,
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
            inner: RwLock::new(GraphInner::new()),
        }
    }

    /// Test hook for `g.destinationEdgeThreshold = n`.
    pub fn set_destination_edge_threshold(&self, threshold: usize) {
        self.write().destination_edge_threshold = threshold;
    }

    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, GraphInner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, GraphInner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    /// `hasPathFrom`: true if there is a directed path from the specified
    /// type/namespace/name to the specified Node.
    pub fn has_path_from(
        &self,
        node_name: &str,
        starting_type: VertexType,
        starting_namespace: &str,
        starting_name: &str,
    ) -> Result<bool, String> {
        self.read()
            .has_path_from(node_name, starting_type, starting_namespace, starting_name)
    }

    /// `AddPod` should only be called once spec.NodeName is populated.
    /// It sets up edges for the following relationships (which are immutable
    /// for a pod once bound to a node):
    ///
    /// ```text
    /// pod       -> node
    /// secret    -> pod
    /// configmap -> pod
    /// pvc       -> pod
    /// svcacct   -> pod
    /// ```
    pub fn add_pod(&self, pod: &Pod) {
        let start = Instant::now();
        {
            let mut g = self.write();
            let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
            let name = pod.metadata.name.as_str();
            let spec = pod.spec.as_ref();
            let node_name = spec.and_then(|s| s.node_name.as_deref()).unwrap_or("");

            g.delete_vertex(VertexType::Pod, namespace, name);
            let pod_vertex = g.get_or_create_vertex(VertexType::Pod, namespace, name);
            let node_vertex = g.get_or_create_vertex(VertexType::Node, "", node_name);
            g.add_edge(pod_vertex, node_vertex, Some(node_vertex));

            // Short-circuit adding edges to other resources for mirror pods.
            // A node must never be able to create a pod that grants them permissions on other API objects.
            // The NodeRestriction admission plugin prevents creation of such pods, but short-circuiting here gives us defense in depth.
            if pod
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
            {
                drop(g);
                observe("AddPod", start);
                return;
            }

            let Some(spec) = spec else {
                drop(g);
                observe("AddPod", start);
                return;
            };

            // The pod unconditionally gets access to the pod's service account.  In the
            // future, this access could be restricted based on whether or not the pod
            // actually mounts a service account token, or has a podcertificate volume.
            if let Some(sa) = spec
                .service_account_name
                .as_deref()
                .filter(|s| !s.is_empty())
            {
                let sa_vertex = g.get_or_create_vertex(VertexType::ServiceAccount, namespace, sa);
                g.add_edge(sa_vertex, pod_vertex, Some(node_vertex));
            }

            for secret in visit_pod_secret_names(pod) {
                let v = g.get_or_create_vertex(VertexType::Secret, namespace, &secret);
                g.add_edge(v, pod_vertex, Some(node_vertex));
            }

            for configmap in visit_pod_configmap_names(pod) {
                let v = g.get_or_create_vertex(VertexType::ConfigMap, namespace, &configmap);
                g.add_edge(v, pod_vertex, Some(node_vertex));
            }

            for volume in spec.volumes.iter().flatten() {
                let claim_name = if let Some(pvc) = &volume.persistent_volume_claim {
                    pvc.claim_name.clone()
                } else if volume.ephemeral.is_some() {
                    // `ephemeral.VolumeClaimName`.
                    format!("{}-{}", pod.metadata.name, volume.name)
                } else {
                    String::new()
                };
                if !claim_name.is_empty() {
                    let v = g.get_or_create_vertex(VertexType::Pvc, namespace, &claim_name);
                    g.add_edge(v, pod_vertex, Some(node_vertex));
                }
            }

            for pod_resource_claim in spec.resource_claims.iter().flatten() {
                // Do we have a valid claim name? If yes, add an edge that grants
                // kubelet access to that claim. An error indicates that a claim
                // still needs to be created, nil that intentionally no claim
                // was created and never will be because it isn't needed.
                if let Some(claim_name) = resource_claim_name(pod, pod_resource_claim) {
                    let v =
                        g.get_or_create_vertex(VertexType::ResourceClaim, namespace, &claim_name);
                    g.add_edge(v, pod_vertex, Some(node_vertex));
                }
            }

            if let Some(extended) = pod
                .status
                .as_ref()
                .and_then(|s| s.extended_resource_claim_status.as_ref())
                .filter(|e| !e.resource_claim_name.is_empty())
            {
                let v = g.get_or_create_vertex(
                    VertexType::ResourceClaim,
                    namespace,
                    &extended.resource_claim_name,
                );
                g.add_edge(v, pod_vertex, Some(node_vertex));
            }
        }
        observe("AddPod", start);
    }

    /// `DeletePod`.
    pub fn delete_pod(&self, name: &str, namespace: &str) {
        let start = Instant::now();
        self.write().delete_vertex(VertexType::Pod, namespace, name);
        observe("DeletePod", start);
    }

    /// `AddPodCertificateRequest`.
    ///
    /// PCRs technically have two valid edges: PCR -> Pod (-> Node) and
    /// PCR -> Node. Only the direct PCR -> Node edge is added, since that is
    /// enough to perform the authorization, and it's a shorter graph
    /// traversal. The noderestriction admission plugin ensures that all PCRs
    /// created have a valid node, serviceaccount, and pod combination that
    /// actually exists in the cluster.
    pub fn add_pod_certificate_request(&self, pcr: &PodCertificateRequest) {
        let start = Instant::now();
        {
            let mut g = self.write();
            let namespace = pcr.metadata.namespace.as_deref().unwrap_or("");
            g.delete_vertex(VertexType::Pcr, namespace, &pcr.metadata.name);
            let pcr_vertex = g.get_or_create_vertex(VertexType::Pcr, namespace, &pcr.metadata.name);
            let node_vertex = g.get_or_create_vertex(VertexType::Node, "", &pcr.spec.node_name);
            g.add_edge(pcr_vertex, node_vertex, Some(node_vertex));
        }
        observe("AddPodCertificateRequest", start);
    }

    /// `DeletePodCertificateRequest`.
    pub fn delete_pod_certificate_request(&self, pcr: &PodCertificateRequest) {
        self.delete_pcr_by_key(
            pcr.metadata.namespace.as_deref().unwrap_or(""),
            &pcr.metadata.name,
        );
    }

    pub(crate) fn delete_pcr_by_key(&self, namespace: &str, name: &str) {
        let start = Instant::now();
        self.write().delete_vertex(VertexType::Pcr, namespace, name);
        observe("DeletePodCertificateRequest", start);
    }

    /// `AddPV` sets up edges for the following relationships:
    ///
    /// ```text
    /// secret -> pv
    /// pv -> pvc
    /// ```
    pub fn add_pv(&self, pv: &PersistentVolume) {
        let start = Instant::now();
        {
            let mut g = self.write();

            // clear existing edges
            g.delete_vertex(VertexType::Pv, "", &pv.metadata.name);

            // if we have a pvc, establish new edges
            if let Some(claim_ref) = &pv.spec.claim_ref {
                let pv_vertex = g.get_or_create_vertex(VertexType::Pv, "", &pv.metadata.name);

                // since we don't know the other end of the pvc -> pod -> node chain (or it may not even exist yet), we can't decorate these edges with kubernetes node info
                let pvc_vertex = g.get_or_create_vertex(
                    VertexType::Pvc,
                    claim_ref.namespace.as_deref().unwrap_or(""),
                    claim_ref.name.as_deref().unwrap_or(""),
                );
                g.add_edge(pv_vertex, pvc_vertex, None);
                for (namespace, secret, kubelet_visible) in visit_pv_secret_names(pv) {
                    // This grants access to the named secret in the same namespace as the bound PVC
                    if kubelet_visible {
                        let secret_vertex =
                            g.get_or_create_vertex(VertexType::Secret, &namespace, &secret);
                        g.add_edge(secret_vertex, pv_vertex, None);
                    }
                }
            }
        }
        observe("AddPV", start);
    }

    /// `DeletePV`.
    pub fn delete_pv(&self, name: &str) {
        let start = Instant::now();
        self.write().delete_vertex(VertexType::Pv, "", name);
        observe("DeletePV", start);
    }

    /// `AddVolumeAttachment` sets up edges for: volume attachment -> node.
    pub fn add_volume_attachment(&self, attachment_name: &str, node_name: &str) {
        let start = Instant::now();
        {
            let mut g = self.write();
            // clear existing edges
            g.delete_vertex(VertexType::Va, "", attachment_name);
            // if we have a node, establish new edges
            if !node_name.is_empty() {
                let va = g.get_or_create_vertex(VertexType::Va, "", attachment_name);
                let node = g.get_or_create_vertex(VertexType::Node, "", node_name);
                g.add_edge(va, node, Some(node));
            }
        }
        observe("AddVolumeAttachment", start);
    }

    /// `DeleteVolumeAttachment`.
    pub fn delete_volume_attachment(&self, name: &str) {
        let start = Instant::now();
        self.write().delete_vertex(VertexType::Va, "", name);
        observe("DeleteVolumeAttachment", start);
    }

    /// `AddResourceSlice` sets up edges for: node resource slice -> node.
    pub fn add_resource_slice(&self, slice_name: &str, node_name: &str) {
        let start = Instant::now();
        {
            let mut g = self.write();
            // clear existing edges
            g.delete_vertex(VertexType::Slice, "", slice_name);
            // if we have a node, establish new edges
            if !node_name.is_empty() {
                let slice = g.get_or_create_vertex(VertexType::Slice, "", slice_name);
                let node = g.get_or_create_vertex(VertexType::Node, "", node_name);
                g.add_edge(slice, node, Some(node));
            }
        }
        observe("AddResourceSlice", start);
    }

    /// `DeleteResourceSlice`.
    pub fn delete_resource_slice(&self, slice_name: &str) {
        let start = Instant::now();
        self.write()
            .delete_vertex(VertexType::Slice, "", slice_name);
        observe("DeleteResourceSlice", start);
    }
}

// ---------------------------------------------------------------------------
// Pod / PV reference visitors
// ---------------------------------------------------------------------------

/// `resourceclaim.Name` (staging/src/k8s.io/dynamic-resource-allocation/
/// resourceclaim/resourceclaim.go:65-79), reduced to what `AddPod` consumes:
/// `Some(name)` only for `err == nil && claimName != nil`.
fn resource_claim_name(
    pod: &Pod,
    pod_claim: &crate::resources::pod::PodResourceClaim,
) -> Option<String> {
    if let Some(name) = &pod_claim.resource_claim_name {
        return Some(name.clone());
    }
    if pod_claim.resource_claim_template_name.is_some() {
        return pod
            .status
            .as_ref()
            .and_then(|s| s.resource_claim_statuses.as_ref())
            .into_iter()
            .flatten()
            .find(|status| status.name == pod_claim.name)
            .and_then(|status| status.resource_claim_name.clone());
    }
    // ErrAPIUnsupported
    None
}

/// `VisitContainers(&pod.Spec, AllContainers, ...)`: init containers, regular
/// containers, then ephemeral containers (pkg/api/v1/pod/util.go:68-107).
fn visit_container_env(
    pod: &Pod,
    mut visit: impl FnMut(
        Option<&Vec<crate::resources::pod::EnvVar>>,
        Option<&Vec<crate::resources::pod::EnvFromSource>>,
    ),
) {
    let Some(spec) = pod.spec.as_ref() else {
        return;
    };
    for c in spec.init_containers.iter().flatten() {
        visit(c.env.as_ref(), c.env_from.as_ref());
    }
    for c in &spec.containers {
        visit(c.env.as_ref(), c.env_from.as_ref());
    }
    for c in spec.ephemeral_containers.iter().flatten() {
        visit(c.env.as_ref(), c.env_from.as_ref());
    }
}

fn json_str<'a>(v: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cur = v;
    for p in path {
        cur = cur.get(*p)?;
    }
    cur.as_str()
}

fn json_present<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    v.get(key).filter(|x| !x.is_null())
}

/// `podutil.VisitPodSecretNames` (pkg/api/v1/pod/util.go:113-176): the name
/// of every secret referenced by the pod spec; empty names skipped.
/// Transitive references (pod -> pvc -> pv -> secret) are not visited.
pub fn visit_pod_secret_names(pod: &Pod) -> Vec<String> {
    let mut names = Vec::new();
    let mut push = |n: &str| {
        if !n.is_empty() {
            names.push(n.to_string());
        }
    };
    let Some(spec) = pod.spec.as_ref() else {
        return names;
    };
    for reference in spec.image_pull_secrets.iter().flatten() {
        push(&reference.name);
    }
    // visitContainerSecretNames: envFrom first, then env.
    visit_container_env(pod, |env, env_from| {
        for e in env_from.into_iter().flatten() {
            if let Some(s) = &e.secret_ref {
                push(&s.name);
            }
        }
        for e in env.into_iter().flatten() {
            if let Some(s) = e
                .value_from
                .as_ref()
                .and_then(|v| v.secret_key_ref.as_ref())
            {
                push(&s.name);
            }
        }
    });
    for volume in spec.volumes.iter().flatten() {
        // The Go `switch` takes the first populated source; volume sources
        // are serialized so the legacy (untyped) ones can be read uniformly.
        let Ok(source) = serde_json::to_value(volume) else {
            continue;
        };
        if let Some(s) = json_present(&source, "azureFile") {
            push(json_str(s, &["secretName"]).unwrap_or(""));
        } else if let Some(s) = json_present(&source, "cephfs") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "cinder") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "flexVolume") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "projected") {
            for src in s
                .get("sources")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(secret) = json_present(src, "secret") {
                    push(json_str(secret, &["name"]).unwrap_or(""));
                }
            }
        } else if let Some(s) = json_present(&source, "rbd") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "secret") {
            push(json_str(s, &["secretName"]).unwrap_or(""));
        } else if let Some(s) = json_present(&source, "scaleIO") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "iscsi") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "storageos") {
            if let Some(n) = json_str(s, &["secretRef", "name"]) {
                push(n);
            }
        } else if let Some(s) = json_present(&source, "csi") {
            if let Some(n) = json_str(s, &["nodePublishSecretRef", "name"]) {
                push(n);
            }
        }
    }
    names
}

/// `podutil.VisitPodConfigmapNames` (pkg/api/v1/pod/util.go:196-214).
pub fn visit_pod_configmap_names(pod: &Pod) -> Vec<String> {
    let mut names = Vec::new();
    let mut push = |n: &str| {
        if !n.is_empty() {
            names.push(n.to_string());
        }
    };
    let Some(spec) = pod.spec.as_ref() else {
        return names;
    };
    visit_container_env(pod, |env, env_from| {
        for e in env_from.into_iter().flatten() {
            if let Some(c) = &e.config_map_ref {
                push(&c.name);
            }
        }
        for e in env.into_iter().flatten() {
            if let Some(c) = e
                .value_from
                .as_ref()
                .and_then(|v| v.config_map_key_ref.as_ref())
            {
                push(&c.name);
            }
        }
    });
    for volume in spec.volumes.iter().flatten() {
        if let Some(projected) = &volume.projected {
            for src in projected.sources.iter().flatten() {
                if let Some(c) = &src.config_map {
                    push(c.name.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(c) = &volume.config_map {
            push(c.name.as_deref().unwrap_or(""));
        }
    }
    names
}

/// `pvutil.VisitPVSecretNames` (pkg/api/v1/persistentvolume/util.go:47-157):
/// `(namespace, name, kubeletVisible)` for every secret referenced by the PV
/// source; empty names skipped.
pub fn visit_pv_secret_names(pv: &PersistentVolume) -> Vec<(String, String, bool)> {
    let mut out: Vec<(String, String, bool)> = Vec::new();
    let mut push = |ns: &str, name: &str, kubelet_visible: bool| {
        if !name.is_empty() {
            out.push((ns.to_string(), name.to_string(), kubelet_visible));
        }
    };
    let Ok(spec) = serde_json::to_value(&pv.spec) else {
        return out;
    };
    // getClaimRefNamespace
    let claim_ns = pv
        .spec
        .claim_ref
        .as_ref()
        .and_then(|c| c.namespace.as_deref())
        .unwrap_or("");
    // secretRef with "use the secret namespace if namespace is set"
    let ns_defaulting_to_claim = |s: &Value| -> (String, String) {
        let ns = json_str(s, &["secretRef", "namespace"])
            .filter(|n| !n.is_empty())
            .unwrap_or(claim_ns);
        (
            ns.to_string(),
            json_str(s, &["secretRef", "name"])
                .unwrap_or("")
                .to_string(),
        )
    };

    if let Some(s) = json_present(&spec, "azureFile") {
        let secret_name = json_str(s, &["secretName"]).unwrap_or("");
        match json_str(s, &["secretNamespace"]).filter(|n| !n.is_empty()) {
            Some(ns) => push(ns, secret_name, true),
            None => push(claim_ns, secret_name, true),
        }
    } else if let Some(s) = json_present(&spec, "cephfs") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            let (ns, name) = ns_defaulting_to_claim(s);
            push(&ns, &name, true);
        }
    } else if let Some(s) = json_present(&spec, "cinder") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            push(
                json_str(s, &["secretRef", "namespace"]).unwrap_or(""),
                json_str(s, &["secretRef", "name"]).unwrap_or(""),
                true,
            );
        }
    } else if let Some(s) = json_present(&spec, "flexVolume") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            let (ns, name) = ns_defaulting_to_claim(s);
            push(&ns, &name, true);
        }
    } else if let Some(s) = json_present(&spec, "rbd") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            let (ns, name) = ns_defaulting_to_claim(s);
            push(&ns, &name, true);
        }
    } else if let Some(s) = json_present(&spec, "scaleIO") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            let (ns, name) = ns_defaulting_to_claim(s);
            push(&ns, &name, true);
        }
    } else if let Some(s) = json_present(&spec, "iscsi") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            let (ns, name) = ns_defaulting_to_claim(s);
            push(&ns, &name, true);
        }
    } else if let Some(s) = json_present(&spec, "storageos") {
        if s.get("secretRef").is_some_and(|r| !r.is_null()) {
            push(
                json_str(s, &["secretRef", "namespace"]).unwrap_or(""),
                json_str(s, &["secretRef", "name"]).unwrap_or(""),
                true,
            );
        }
    } else if let Some(s) = json_present(&spec, "csi") {
        for (field, kubelet_visible) in [
            ("controllerPublishSecretRef", false),
            ("controllerExpandSecretRef", false),
            ("nodePublishSecretRef", true),
            ("nodeStageSecretRef", true),
            ("nodeExpandSecretRef", true),
        ] {
            if let Some(r) = json_present(s, field) {
                push(
                    json_str(r, &["namespace"]).unwrap_or(""),
                    json_str(r, &["name"]).unwrap_or(""),
                    kubelet_visible,
                );
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// graph_populator.go
// ---------------------------------------------------------------------------

/// `graphPopulator` (graph_populator.go:37-39): turns object events into
/// graph updates. The informer's old/new pair is a [`GraphPopulator::update_pod`]
/// argument; the `apply_*` forms keep the few fields the skip checks compare
/// instead of whole objects, for a watch that does not carry the old object.
pub struct GraphPopulator {
    graph: std::sync::Arc<NodeGraph>,
    pods: std::sync::Mutex<HashMap<(String, String), PodFingerprint>>,
    attachments: std::sync::Mutex<HashMap<String, String>>,
}

/// What `updatePod` compares between the old and the new pod
/// (graph_populator.go:100-111).
#[derive(Debug, Clone, PartialEq, Eq)]
struct PodFingerprint {
    node_name: String,
    uid: String,
    ephemeral_containers: usize,
    /// `resourceclaim.PodStatusEqual` input: ordered (name, claimName).
    claim_statuses: Vec<(String, Option<String>)>,
}

impl PodFingerprint {
    fn of(pod: &Pod) -> Self {
        Self {
            node_name: pod_node_name(pod).to_string(),
            uid: pod.metadata.uid.clone(),
            ephemeral_containers: pod
                .spec
                .as_ref()
                .and_then(|s| s.ephemeral_containers.as_ref())
                .map_or(0, Vec::len),
            claim_statuses: pod
                .status
                .as_ref()
                .and_then(|s| s.resource_claim_statuses.as_ref())
                .into_iter()
                .flatten()
                .map(|c| (c.name.clone(), c.resource_claim_name.clone()))
                .collect(),
        }
    }

    /// The skip condition of `updatePod`: "Node and uid are unchanged, all
    /// object references in the pod spec are immutable respectively
    /// unmodified (claim statuses)". "Ephemeral containers can add new secret
    /// or config map references to the pod."
    fn unchanged_from(&self, old: &PodFingerprint) -> bool {
        let has_new_ephemeral_containers = self.ephemeral_containers > old.ephemeral_containers;
        self.node_name == old.node_name
            && self.uid == old.uid
            && !has_new_ephemeral_containers
            && self.claim_statuses == old.claim_statuses
    }
}

fn pod_node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

impl GraphPopulator {
    pub fn new(graph: std::sync::Arc<NodeGraph>) -> Self {
        Self {
            graph,
            pods: Default::default(),
            attachments: Default::default(),
        }
    }

    /// The graph this populator feeds.
    pub fn graph(&self) -> &std::sync::Arc<NodeGraph> {
        &self.graph
    }

    /// `addPod`.
    pub fn add_pod(&self, pod: &Pod) {
        self.update_pod(None, pod);
    }

    /// `updatePod`.
    pub fn update_pod(&self, old: Option<&Pod>, pod: &Pod) {
        if pod_node_name(pod).is_empty() {
            // No node assigned
            return;
        }
        if let Some(old) = old {
            if PodFingerprint::of(pod).unchanged_from(&PodFingerprint::of(old)) {
                return;
            }
        }
        self.graph.add_pod(pod);
    }

    /// `updatePod` for a source that has no old object: the previous
    /// fingerprint is the one recorded by the last `apply_pod`.
    pub fn apply_pod(&self, pod: &Pod) {
        if pod_node_name(pod).is_empty() {
            return;
        }
        let key = (
            pod.metadata.namespace.clone().unwrap_or_default(),
            pod.metadata.name.clone(),
        );
        let fingerprint = PodFingerprint::of(pod);
        {
            let mut pods = self.pods.lock().unwrap_or_else(|e| e.into_inner());
            if pods
                .get(&key)
                .is_some_and(|old| fingerprint.unchanged_from(old))
            {
                return;
            }
            pods.insert(key, fingerprint);
        }
        self.graph.add_pod(pod);
    }

    /// `deletePod`.
    pub fn delete_pod(&self, pod: &Pod) {
        if pod_node_name(pod).is_empty() {
            return;
        }
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
        self.pods
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(namespace.to_string(), pod.metadata.name.clone()));
        self.graph.delete_pod(&pod.metadata.name, namespace);
    }

    /// `deletePod` for a source that only has the object's key (a resync that
    /// finds a pod gone). A pod that never had a node has no vertex, so the
    /// "no node" early return of `deletePod` is unnecessary.
    pub fn delete_pod_by_key(&self, namespace: &str, name: &str) {
        self.pods
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(namespace.to_string(), name.to_string()));
        self.graph.delete_pod(name, namespace);
    }

    /// `deletePCR` for a source that only has the object's key.
    pub fn delete_pcr_by_key(&self, namespace: &str, name: &str) {
        self.graph.delete_pcr_by_key(namespace, name);
    }

    /// `addPV` / `updatePV`. (Upstream: "TODO: skip add if uid, pvc, and
    /// secrets are all identical between old and new".)
    pub fn add_pv(&self, pv: &PersistentVolume) {
        self.graph.add_pv(pv);
    }

    /// `deletePV`.
    pub fn delete_pv(&self, pv: &PersistentVolume) {
        self.graph.delete_pv(&pv.metadata.name);
    }

    /// `updateVolumeAttachment`: "skip add if node name is identical".
    pub fn update_volume_attachment(
        &self,
        old_node_name: Option<&str>,
        name: &str,
        node_name: &str,
    ) {
        if old_node_name == Some(node_name) {
            return;
        }
        self.graph.add_volume_attachment(name, node_name);
    }

    /// `updateVolumeAttachment` for a source with no old object.
    pub fn apply_volume_attachment(&self, name: &str, node_name: &str) {
        {
            let mut attachments = self.attachments.lock().unwrap_or_else(|e| e.into_inner());
            if attachments.get(name).map(String::as_str) == Some(node_name) {
                return;
            }
            attachments.insert(name.to_string(), node_name.to_string());
        }
        self.graph.add_volume_attachment(name, node_name);
    }

    /// `deleteVolumeAttachment`.
    pub fn delete_volume_attachment(&self, name: &str) {
        self.attachments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
        self.graph.delete_volume_attachment(name);
    }

    /// `addResourceSlice` (`UpdateFunc` is nil: "Not needed, NodeName is immutable").
    pub fn add_resource_slice(&self, slice: &crate::resources::dra::ResourceSlice) {
        self.graph.add_resource_slice(
            &slice.metadata.name,
            slice.spec.node_name.as_deref().unwrap_or(""),
        );
    }

    /// `deleteResourceSlice`.
    pub fn delete_resource_slice(&self, slice: &crate::resources::dra::ResourceSlice) {
        self.graph.delete_resource_slice(&slice.metadata.name);
    }

    /// `addPCR` (`UpdateFunc` is nil: "Not needed, spec fields are immutable").
    pub fn add_pcr(&self, pcr: &PodCertificateRequest) {
        self.graph.add_pod_certificate_request(pcr);
    }

    /// `deletePCR`.
    pub fn delete_pcr(&self, pcr: &PodCertificateRequest) {
        self.graph.delete_pod_certificate_request(pcr);
    }
}
