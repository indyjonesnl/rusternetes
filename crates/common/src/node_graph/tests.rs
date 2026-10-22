//! Ports of `plugin/pkg/auth/authorizer/node/graph_test.go`
//! (`TestDeleteEdgesLocked`, `TestIndex`, `TestIndex2`) and the
//! graph_populator.go behaviour that `node_authorizer_test.go` exercises.
use super::*;
use serde_json::json;
use std::collections::BTreeMap;

fn label(g: &Inner, id: VertexId) -> String {
    let v = &g.nodes[&id];
    if v.namespace.is_empty() {
        format!("{}:{}", v.vertex_type.name(), v.name)
    } else {
        format!("{}:{}/{}", v.vertex_type.name(), v.namespace, v.name)
    }
}

/// `expectGraph`'s actual: vertex -> sorted `From` neighbors.
fn graph_dump(g: &NodeGraph) -> BTreeMap<String, Vec<String>> {
    let g = g.inner.read().unwrap();
    let mut out = BTreeMap::new();
    for id in g.nodes.keys() {
        let mut to: Vec<String> = g.adj[id]
            .iter()
            .filter(|(_, e)| e.from == *id)
            .map(|(n, _)| label(&g, *n))
            .collect();
        to.sort();
        out.insert(label(&g, *id), to);
    }
    out
}

/// `expectIndex`'s actual.
fn index_dump(g: &NodeGraph) -> BTreeMap<String, Vec<String>> {
    let g = g.inner.read().unwrap();
    let mut out = BTreeMap::new();
    for (from, set) in &g.destination_edge_index {
        let mut vals: Vec<String> = set
            .members
            .iter()
            .map(|(m, c)| format!("{}={}", label(&g, *m), c))
            .collect();
        vals.sort();
        out.insert(label(&g, *from), vals);
    }
    out
}

fn map(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
        .collect()
}

fn test_graph() -> NodeGraph {
    let g = NodeGraph::new();
    g.set_destination_edge_threshold(3);
    g
}

fn pod(name: &str, node: &str, sa: &str, volumes: Value) -> Value {
    let mut p = json!({
        "metadata": {"name": name, "namespace": "ns", "uid": format!("pod{name}uid")},
        "spec": {"nodeName": node},
    });
    if !sa.is_empty() {
        p["spec"]["serviceAccountName"] = json!(sa);
    }
    if !volumes.is_null() {
        p["spec"]["volumes"] = volumes;
    }
    p
}

fn cm_vol(n: &str) -> Value {
    json!({"name": n, "configMap": {"name": n}})
}
fn secret_vol(n: &str) -> Value {
    json!({"name": n, "secret": {"secretName": n}})
}
fn pvc_vol(n: &str) -> Value {
    json!({"name": n, "persistentVolumeClaim": {"claimName": n}})
}
fn pod_with_sa_and_cms(name: &str, node: &str) -> Value {
    pod(
        name,
        node,
        "sa1",
        json!([cm_vol("cm1"), cm_vol("cm2"), cm_vol("cm3")]),
    )
}
fn pod_with_secrets(name: &str, node: &str) -> Value {
    pod(
        name,
        node,
        "",
        json!([secret_vol("s1"), secret_vol("s2"), secret_vol("s3")]),
    )
}
fn pod_with_pvcs(name: &str, node: &str) -> Value {
    pod(
        name,
        node,
        "",
        json!([pvc_vol("pvc1"), pvc_vol("pvc2"), pvc_vol("pvc3")]),
    )
}
fn pod_with_resource_claims(name: &str, node: &str) -> Value {
    let mut p = pod(name, node, "", Value::Null);
    p["spec"]["resourceClaims"] = json!([
        {"resourceClaimName": "rc1"}, {"resourceClaimName": "rc2"}, {"resourceClaimName": "rc3"}
    ]);
    p["status"] = json!({"extendedResourceClaimStatus": {"resourceClaimName": "rc4"}});
    p
}
fn pv(name: &str, pvc: &str, secret: &str) -> Value {
    let mut v = json!({
        "metadata": {"name": name, "uid": format!("pv{name}uid")},
        "spec": {"claimRef": {"name": pvc, "namespace": "ns"}},
    });
    if !secret.is_empty() {
        v["spec"]["csi"] = json!({"nodePublishSecretRef": {"name": secret, "namespace": "ns"}});
    }
    v
}
fn pcr(ns: &str, name: &str, node: &str) -> Value {
    json!({"metadata": {"namespace": ns, "name": name}, "spec": {"nodeName": node}})
}

/// TestDeleteEdgesLocked
#[test]
fn delete_edges_locked() {
    // edges and source orphans are deleted, destination orphans are preserved
    let g = NodeGraph::new();
    {
        let mut i = g.inner.write().unwrap();
        i.get_or_create_vertex(VertexType::ConfigMap, "namespace1", "configmap2");
        let node = i.get_or_create_vertex(VertexType::Node, "", "node1");
        let cm = i.get_or_create_vertex(VertexType::ConfigMap, "namespace1", "configmap1");
        i.add_edge(cm, node, Some(node));
    }
    g.delete_edges(VertexType::ConfigMap, VertexType::Node, "", "node1");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("configmap:namespace1/configmap2", &[]),
            ("node:node1", &[])
        ])
    );

    // non-orphans and destination orphans are preserved
    let g = NodeGraph::new();
    {
        let mut i = g.inner.write().unwrap();
        let n1 = i.get_or_create_vertex(VertexType::Node, "", "node1");
        let n2 = i.get_or_create_vertex(VertexType::Node, "", "node2");
        let cm = i.get_or_create_vertex(VertexType::ConfigMap, "namespace1", "configmap1");
        i.add_edge(cm, n1, Some(n1));
        i.add_edge(cm, n2, Some(n2));
    }
    g.delete_edges(VertexType::ConfigMap, VertexType::Node, "", "node2");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node1", &[]),
            ("node:node2", &[]),
            ("configmap:namespace1/configmap1", &["node:node1"]),
        ])
    );

    // no edges / destination missing / source type missing: untouched
    let g = NodeGraph::new();
    {
        let mut i = g.inner.write().unwrap();
        i.get_or_create_vertex(VertexType::Node, "", "node1");
        i.get_or_create_vertex(VertexType::ConfigMap, "namespace1", "configmap1");
    }
    g.delete_edges(VertexType::ConfigMap, VertexType::Node, "", "node1");
    assert_eq!(graph_dump(&g).len(), 2);
    let g = NodeGraph::new();
    g.inner.write().unwrap().get_or_create_vertex(
        VertexType::ConfigMap,
        "namespace1",
        "configmap1",
    );
    g.delete_edges(VertexType::ConfigMap, VertexType::Node, "", "node1");
    assert_eq!(graph_dump(&g).len(), 1);
}

/// TestIndex
#[test]
fn index() {
    let g = test_graph();
    let add_pod = |pod_number: usize, node_number: usize| {
        let node = format!("node{node_number}");
        let p = json!({
            "metadata": {"name": format!("pod{pod_number}"), "namespace": "ns",
                         "uid": format!("pod{pod_number}uid1")},
            "spec": {"nodeName": node, "serviceAccountName": "sa1", "serviceAccount": "sa1",
                     "volumes": [cm_vol("cm1"), cm_vol("cm2"), cm_vol("cm3")]},
        });
        g.add_pod(&p);
        assert_eq!(
            g.has_path_from(&node, VertexType::ConfigMap, "ns", "cm1"),
            Ok(true)
        );
    };

    for i in 1..=3 {
        add_pod(i, i);
        if i < 3 {
            // if we're under the threshold, no index expected
            assert_eq!(index_dump(&g), map(&[]));
        }
    }
    let three = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3"];
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node1", &[]),
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("pod:ns/pod1", &["node:node1"]),
            ("pod:ns/pod2", &["node:node2"]),
            ("pod:ns/pod3", &["node:node3"]),
            ("configmap:ns/cm1", &three),
            ("configmap:ns/cm2", &three),
            ("configmap:ns/cm3", &three),
            ("serviceAccount:ns/sa1", &three),
        ])
    );
    let idx3 = ["node:node1=1", "node:node2=1", "node:node3=1"];
    assert_eq!(
        index_dump(&g),
        map(&[
            ("configmap:ns/cm1", &idx3),
            ("configmap:ns/cm2", &idx3),
            ("configmap:ns/cm3", &idx3),
            ("serviceAccount:ns/sa1", &idx3),
        ])
    );

    // delete one to drop below the threshold
    g.delete_pod("pod1", "ns");
    let two = ["pod:ns/pod2", "pod:ns/pod3"];
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("pod:ns/pod2", &["node:node2"]),
            ("pod:ns/pod3", &["node:node3"]),
            ("configmap:ns/cm1", &two),
            ("configmap:ns/cm2", &two),
            ("configmap:ns/cm3", &two),
            ("serviceAccount:ns/sa1", &two),
        ])
    );
    assert_eq!(index_dump(&g), map(&[]));

    // add two to get above the threshold
    add_pod(1, 1);
    add_pod(4, 1);
    let four = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"];
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node1", &[]),
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("pod:ns/pod1", &["node:node1"]),
            ("pod:ns/pod2", &["node:node2"]),
            ("pod:ns/pod3", &["node:node3"]),
            ("pod:ns/pod4", &["node:node1"]),
            ("configmap:ns/cm1", &four),
            ("configmap:ns/cm2", &four),
            ("configmap:ns/cm3", &four),
            ("serviceAccount:ns/sa1", &four),
        ])
    );
    let idx = ["node:node1=2", "node:node2=1", "node:node3=1"];
    assert_eq!(
        index_dump(&g),
        map(&[
            ("configmap:ns/cm1", &idx),
            ("configmap:ns/cm2", &idx),
            ("configmap:ns/cm3", &idx),
            ("serviceAccount:ns/sa1", &idx),
        ])
    );

    // delete one to remain above the threshold
    g.delete_pod("pod1", "ns");
    assert_eq!(
        index_dump(&g),
        map(&[
            ("configmap:ns/cm1", &idx3),
            ("configmap:ns/cm2", &idx3),
            ("configmap:ns/cm3", &idx3),
            ("serviceAccount:ns/sa1", &idx3),
        ])
    );
}

/// TestIndex2
#[test]
fn index2() {
    let idx3 = ["node:node1=1", "node:node2=1", "node:node3=1"];
    let three = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3"];

    // outdeg below destination edge index threshold
    let g = test_graph();
    g.add_pod(&pod_with_sa_and_cms("pod1", "node1"));
    g.add_pod(&pod_with_sa_and_cms("pod2", "node2"));
    assert_eq!(index_dump(&g), map(&[]));
    assert_eq!(graph_dump(&g).len(), 8);

    // index built for configmaps and serviceaccounts
    g.add_pod(&pod_with_sa_and_cms("pod3", "node3"));
    assert_eq!(
        index_dump(&g),
        map(&[
            ("configmap:ns/cm1", &idx3),
            ("configmap:ns/cm2", &idx3),
            ("configmap:ns/cm3", &idx3),
            ("serviceAccount:ns/sa1", &idx3),
        ])
    );

    // index built / dropped for secrets, pvcs, resourceclaims
    type PodFn = fn(&str, &str) -> Value;
    let cases: [(PodFn, &str, &[&str]); 3] = [
        (pod_with_secrets, "secret:ns/", &["s1", "s2", "s3"]),
        (pod_with_pvcs, "pvc:ns/", &["pvc1", "pvc2", "pvc3"]),
        (
            pod_with_resource_claims,
            "resourceclaim:ns/",
            &["rc1", "rc2", "rc3", "rc4"],
        ),
    ];
    for (mk, prefix, names) in cases {
        let g = test_graph();
        g.add_pod(&mk("pod1", "node1"));
        g.add_pod(&mk("pod2", "node2"));
        assert_eq!(index_dump(&g), map(&[]));
        g.add_pod(&mk("pod3", "node3"));
        let mut expect_graph = map(&[
            ("node:node1", &[]),
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("pod:ns/pod1", &["node:node1"]),
            ("pod:ns/pod2", &["node:node2"]),
            ("pod:ns/pod3", &["node:node3"]),
        ]);
        let mut expect_index = BTreeMap::new();
        for n in names {
            expect_graph.insert(
                format!("{prefix}{n}"),
                three.iter().map(|s| s.to_string()).collect(),
            );
            expect_index.insert(
                format!("{prefix}{n}"),
                idx3.iter().map(|s| s.to_string()).collect(),
            );
        }
        assert_eq!(graph_dump(&g), expect_graph, "{prefix}");
        assert_eq!(index_dump(&g), expect_index, "{prefix}");

        // dropping below threshold
        g.delete_pod("pod1", "ns");
        assert_eq!(index_dump(&g), map(&[]), "{prefix}");
    }

    // removing a pod but staying above threshold
    let g = test_graph();
    for (p, n) in [
        ("pod1", "node1"),
        ("pod2", "node2"),
        ("pod3", "node3"),
        ("pod4", "node1"),
    ] {
        g.add_pod(&pod_with_sa_and_cms(p, n));
    }
    g.delete_pod("pod1", "ns");
    assert_eq!(
        index_dump(&g),
        map(&[
            ("configmap:ns/cm1", &idx3),
            ("configmap:ns/cm2", &idx3),
            ("configmap:ns/cm3", &idx3),
            ("serviceAccount:ns/sa1", &idx3),
        ])
    );

    // resourceslices
    let g = test_graph();
    g.add_resource_slice("s1", "node1");
    g.add_resource_slice("s2", "node2");
    g.add_resource_slice("s3", "node3");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node1", &[]),
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("resourceslice:s1", &["node:node1"]),
            ("resourceslice:s2", &["node:node2"]),
            ("resourceslice:s3", &["node:node3"]),
        ])
    );
    g.delete_resource_slice("s1");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("resourceslice:s2", &["node:node2"]),
            ("resourceslice:s3", &["node:node3"]),
        ])
    );

    // volumeattachments
    let g = test_graph();
    g.add_volume_attachment("va1", "node1");
    g.add_volume_attachment("va2", "node2");
    g.delete_volume_attachment("va1");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node2", &[]),
            ("volumeattachment:va2", &["node:node2"])
        ])
    );

    // persistentvolumes, with and without secrets
    let g = test_graph();
    g.add_pv(&pv("pv1", "pvc1", ""));
    g.add_pv(&pv("pv2", "pvc2", ""));
    g.delete_pv("pv1");
    assert_eq!(
        graph_dump(&g),
        map(&[("pv:pv2", &["pvc:ns/pvc2"]), ("pvc:ns/pvc2", &[])])
    );
    let g = test_graph();
    g.add_pv(&pv("pv1", "pvc1", "s1"));
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("pv:pv1", &["pvc:ns/pvc1"]),
            ("pvc:ns/pvc1", &[]),
            ("secret:ns/s1", &["pv:pv1"]),
        ])
    );
    assert_eq!(index_dump(&g), map(&[]));

    // podcertificaterequests (including namespace/name ordering on delete)
    let g = test_graph();
    g.add_pod_certificate_request(&pcr("foo", "pcr1", "node1"));
    g.add_pod_certificate_request(&pcr("foo", "pcr3", "node1"));
    g.add_pod_certificate_request(&pcr("foo", "pcr4", "node2"));
    g.delete_pod_certificate_request("foo", "pcr3");
    g.delete_pod_certificate_request("foo", "pcr4");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node1", &[]),
            ("podcertificaterequest:foo/pcr1", &["node:node1"]),
        ])
    );
    let g = test_graph();
    g.add_pod_certificate_request(&pcr("foo", "bar", "node1"));
    g.add_pod_certificate_request(&pcr("bar", "foo", "node2"));
    g.delete_pod_certificate_request("foo", "bar");
    assert_eq!(
        graph_dump(&g),
        map(&[
            ("node:node2", &[]),
            ("podcertificaterequest:bar/foo", &["node:node2"]),
        ])
    );
}

/// `TestNodeAuthorizerAddEphemeralContainers`: an update that adds an ephemeral
/// container is not skipped as "node unchanged", so its secret/configmap
/// references join the graph.
#[test]
fn populator_update_adds_ephemeral_container_references() {
    let g = NodeGraph::new();
    let p = pod("pod1-node1", "node1", "", json!([secret_vol("node1-only")]));
    populator_update_pod(&g, None, &p);
    assert_eq!(
        g.has_path_from("node1", VertexType::Secret, "ns", "node1-only"),
        Ok(true)
    );
    assert!(g
        .has_path_from("node1", VertexType::Secret, "ns", "new-secret")
        .is_err());

    let mut with_secret = p.clone();
    with_secret["spec"]["ephemeralContainers"] = json!([{
        "name": "eph", "image": "imageURL",
        "envFrom": [{"secretRef": {"name": "new-secret"}}],
    }]);
    populator_update_pod(&g, Some(&p), &with_secret);
    assert_eq!(
        g.has_path_from("node1", VertexType::Secret, "ns", "new-secret"),
        Ok(true)
    );

    let mut with_cm = p.clone();
    with_cm["spec"]["ephemeralContainers"] = json!([{
        "name": "eph", "image": "imageURL",
        "envFrom": [{"configMapRef": {"name": "new-config-map"}}],
    }]);
    populator_update_pod(&g, Some(&p), &with_cm);
    assert_eq!(
        g.has_path_from("node1", VertexType::ConfigMap, "ns", "new-config-map"),
        Ok(true)
    );
}

/// An unchanged update (same node, uid, no new ephemeral containers, same
/// claim statuses) is skipped: a changed volume list is NOT re-read.
#[test]
fn populator_update_unchanged_node_is_skipped() {
    let g = NodeGraph::new();
    let p = pod("p", "node1", "", json!([secret_vol("a")]));
    populator_update_pod(&g, None, &p);
    let p2 = pod("p", "node1", "", json!([secret_vol("b")]));
    populator_update_pod(&g, Some(&p), &p2);
    assert!(g
        .has_path_from("node1", VertexType::Secret, "ns", "b")
        .is_err());
    // a node change is applied
    let p3 = pod("p", "node2", "", json!([secret_vol("b")]));
    populator_update_pod(&g, Some(&p), &p3);
    assert_eq!(
        g.has_path_from("node2", VertexType::Secret, "ns", "b"),
        Ok(true)
    );
    assert!(g
        .has_path_from("node1", VertexType::Secret, "ns", "a")
        .is_err());
    // a pod with no node is ignored entirely
    let unbound = pod("q", "", "", json!([secret_vol("z")]));
    populator_update_pod(&g, None, &unbound);
    assert!(g
        .has_path_from("node2", VertexType::Secret, "ns", "z")
        .is_err());
}

#[test]
fn has_path_from_error_texts() {
    let g = NodeGraph::new();
    assert_eq!(
        g.has_path_from("n", VertexType::Secret, "ns", "s"),
        Err("unknown node 'n' cannot get secret ns/s".to_string())
    );
    g.add_pod(&pod("p", "n", "", json!([secret_vol("s")])));
    assert_eq!(
        g.has_path_from("n", VertexType::Secret, "ns", "other"),
        Err("node 'n' cannot get unknown secret ns/other".to_string())
    );
    g.add_pod(&pod("p2", "m", "", json!([secret_vol("t")])));
    assert_eq!(
        g.has_path_from("n", VertexType::Secret, "ns", "t"),
        Err("node 'n' cannot get secret ns/t, no relationship to this object was found in the node authorizer graph".to_string())
    );
}

/// A mirror pod gets only its node edge: none of its references.
#[test]
fn mirror_pod_has_no_reference_edges() {
    let g = NodeGraph::new();
    let mut p = pod("m", "n", "sa", json!([secret_vol("s")]));
    p["metadata"]["annotations"] = json!({"kubernetes.io/config.mirror": "hash"});
    g.add_pod(&p);
    assert_eq!(g.has_path_from("n", VertexType::Pod, "ns", "m"), Ok(true));
    assert!(g.has_path_from("n", VertexType::Secret, "ns", "s").is_err());
    assert!(g
        .has_path_from("n", VertexType::ServiceAccount, "ns", "sa")
        .is_err());
}

/// pod -> pvc -> pv -> secret, the transitive chain from the authorizer's
/// doc comment; a PV with no claimRef contributes nothing.
#[test]
fn pv_chain_and_unclaimed_pv() {
    let g = NodeGraph::new();
    g.add_pod(&pod("p", "n", "", json!([pvc_vol("c1")])));
    g.add_pv(&pv("pv1", "c1", "pvsecret"));
    assert_eq!(g.has_path_from("n", VertexType::Pvc, "ns", "c1"), Ok(true));
    assert_eq!(g.has_path_from("n", VertexType::Pv, "", "pv1"), Ok(true));
    assert_eq!(
        g.has_path_from("n", VertexType::Secret, "ns", "pvsecret"),
        Ok(true)
    );

    let unclaimed = json!({"metadata": {"name": "free"}, "spec": {"csi": {"nodePublishSecretRef": {"name": "x", "namespace": "ns"}}}});
    g.add_pv(&unclaimed);
    assert!(g.has_path_from("n", VertexType::Pv, "", "free").is_err());
    // controllerPublishSecretRef is not kubelet visible
    let mut ctrl = pv("pv2", "c1", "");
    ctrl["spec"]["csi"] = json!({"controllerPublishSecretRef": {"name": "ctl", "namespace": "ns"}});
    g.add_pv(&ctrl);
    assert!(g
        .has_path_from("n", VertexType::Secret, "ns", "ctl")
        .is_err());
}

/// The resource claim name rules of `resourceclaim.Name`.
#[test]
fn resource_claim_names() {
    let g = NodeGraph::new();
    let mut p = pod("p", "n", "", Value::Null);
    p["spec"]["resourceClaims"] = json!([
        {"name": "direct", "resourceClaimName": "claim-a"},
        {"name": "tmpl-pending", "resourceClaimTemplateName": "t1"},
        {"name": "tmpl-made", "resourceClaimTemplateName": "t2"},
        {"name": "neither"},
    ]);
    p["status"] =
        json!({"resourceClaimStatuses": [{"name": "tmpl-made", "resourceClaimName": "generated"}]});
    g.add_pod(&p);
    assert_eq!(
        g.has_path_from("n", VertexType::ResourceClaim, "ns", "claim-a"),
        Ok(true)
    );
    assert_eq!(
        g.has_path_from("n", VertexType::ResourceClaim, "ns", "generated"),
        Ok(true)
    );
    assert!(g
        .has_path_from("n", VertexType::ResourceClaim, "ns", "t1")
        .is_err());
    assert!(g
        .has_path_from("n", VertexType::ResourceClaim, "ns", "t2")
        .is_err());
}

/// Every pod-volume secret/configmap source the upstream visitors list.
#[test]
fn visitors_cover_upstream_sources() {
    let p = json!({"spec": {
    "imagePullSecrets": [{"name": "pull"}, {"name": ""}],
    "initContainers": [{"envFrom": [{"secretRef": {"name": "init-env"}}]}],
    "containers": [{"env": [
        {"valueFrom": {"secretKeyRef": {"name": "k"}}},
        {"valueFrom": {"configMapKeyRef": {"name": "cmk"}}}],
        "envFrom": [{"configMapRef": {"name": "cmenv"}}]}],
    "volumes": [
        {"azureFile": {"secretName": "az"}},
        {"cephfs": {"secretRef": {"name": "ceph"}}},
        {"cinder": {"secretRef": {"name": "cinder"}}},
        {"flexVolume": {"secretRef": {"name": "flex"}}},
        {"projected": {"sources": [{"secret": {"name": "proj-s"}}, {"configMap": {"name": "proj-c"}}]}},
        {"rbd": {"secretRef": {"name": "rbd"}}},
        {"secret": {"secretName": "plain"}},
        {"scaleIO": {"secretRef": {"name": "sio"}}},
        {"iscsi": {"secretRef": {"name": "iscsi"}}},
        {"storageos": {"secretRef": {"name": "sos"}}},
        {"csi": {"nodePublishSecretRef": {"name": "csi"}}},
        {"configMap": {"name": "cmvol"}},
    ]}});
    let mut s = visit_pod_secret_names(&p);
    s.sort();
    let mut want = vec![
        "pull", "init-env", "k", "az", "ceph", "cinder", "flex", "proj-s", "rbd", "plain", "sio",
        "iscsi", "sos", "csi",
    ];
    want.sort();
    assert_eq!(s, want);
    let mut c = visit_pod_configmap_names(&p);
    c.sort();
    assert_eq!(c, vec!["cmenv", "cmk", "cmvol", "proj-c"]);
}

#[test]
fn pv_secret_namespaces() {
    let p = json!({"spec": {"claimRef": {"name": "c", "namespace": "claimns"},
        "rbd": {"secretRef": {"name": "a"}}}});
    assert_eq!(
        visit_pv_secret_names(&p),
        vec![("claimns".into(), "a".into(), true)]
    );
    let p = json!({"spec": {"claimRef": {"name": "c", "namespace": "claimns"},
        "rbd": {"secretRef": {"name": "a", "namespace": "own"}}}});
    assert_eq!(
        visit_pv_secret_names(&p),
        vec![("own".into(), "a".into(), true)]
    );
    let p = json!({"spec": {"claimRef": {"name": "c", "namespace": "claimns"},
        "azureFile": {"secretName": "az", "secretNamespace": "azns"}}});
    assert_eq!(
        visit_pv_secret_names(&p),
        vec![("azns".into(), "az".into(), true)]
    );
    let p = json!({"spec": {"claimRef": {"name": "c", "namespace": "claimns"},
        "azureFile": {"secretName": "az"}}});
    assert_eq!(
        visit_pv_secret_names(&p),
        vec![("claimns".into(), "az".into(), true)]
    );
    // storageos / cinder do not default the namespace
    let p = json!({"spec": {"claimRef": {"name": "c", "namespace": "claimns"},
        "cinder": {"secretRef": {"name": "ci"}}}});
    assert_eq!(
        visit_pv_secret_names(&p),
        vec![("".into(), "ci".into(), true)]
    );
}
