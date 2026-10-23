//! Ports of `plugin/pkg/auth/authorizer/node/node_authorizer_test.go`:
//! `TestNodeAuthorizer`, `TestNodeAuthorizerSharedResources`, plus the
//! identity rules of `pkg/auth/nodeidentifier/default.go`.
//!
//! The `*_selector_disabled` variants are not ported: `AuthorizeNodeWithSelectors`
//! is GA and locked on in 1.35 (kube_features.go:1146-1150), so the permissive
//! pre-selector behaviour no longer exists upstream.
//!
//! One deliberate deviation, `pods/eviction`: upstream allows `create` for any
//! pod (NodeRestriction narrows it); here it stays denied because no
//! NodeRestriction path covers evictions. See `NodeAuthorizer::authorize_pod`.
use super::*;
use crate::field_selector::FieldSelector;
use crate::node_graph::{
    populator_add_pcr, populator_add_resource_slice, populator_update_pod, populator_update_pv,
    populator_update_volume_attachment,
};
use serde_json::{json, Value};

fn user(name: &str) -> UserInfo {
    UserInfo {
        username: name.to_string(),
        uid: String::new(),
        groups: vec!["system:nodes".to_string()],
        extra: std::collections::HashMap::new(),
    }
}

fn req(node: &str, verb: &str, group: &str, resource: &str) -> RequestAttributes {
    RequestAttributes::new(user(&format!("system:node:{node}")), verb, resource)
        .with_api_group(group)
}

fn with_selector(mut a: RequestAttributes, sel: &str) -> RequestAttributes {
    let parsed = FieldSelector::parse(sel).unwrap();
    a.field_selector_requirements = parsed.requirements().to_vec();
    a
}

/// `generate(opts)` for the options `TestNodeAuthorizer` uses; names encode the
/// relationships: `secret0-pod0-node0` is referenced by pod0 on node0.
fn populated() -> NodeAuthorizer {
    let graph = Arc::new(NodeGraph::new());
    for n in 0..2 {
        let node = format!("node{n}");
        for p in 0..2 {
            let name = format!("pod{p}-{node}");
            let ns = format!("ns{}", p % 2);
            let pv_name = format!("pv0-{name}-{ns}");
            let pvc_name = format!("pvc0-{name}");
            let mut pod = json!({
                "metadata": {"name": name, "namespace": ns},
                "spec": {
                    "nodeName": node,
                    "serviceAccountName": format!("svcacct{p}-{node}"),
                    "volumes": [
                        {"name": "a", "secret": {"secretName": format!("secret0-{name}")}},
                        {"name": "b", "secret": {"secretName": "secret0-shared"}},
                        {"name": "c", "configMap": {"name": format!("configmap0-{name}")}},
                        {"name": "d", "persistentVolumeClaim": {"claimName": pvc_name}},
                    ],
                    "resourceClaims": [
                        {"name": "claim0", "resourceClaimName": format!("claim0-{name}-{ns}")},
                        {"name": "claimtemplate0", "resourceClaimTemplateName": format!("claimtemplate0-{name}-{ns}")},
                        {"name": "claimtemplate-with-claim0", "resourceClaimTemplateName": format!("claimtemplate0-{name}-{ns}")},
                    ],
                },
                "status": {"resourceClaimStatuses": [
                    {"name": "claimtemplate-with-claim0", "resourceClaimName": format!("generated-claim-{name}-{ns}-0")}
                ]},
            });
            pod["metadata"]["uid"] = json!("");
            populator_update_pod(&graph, None, &pod);
            populator_update_pv(
                &graph,
                &json!({
                    "metadata": {"name": pv_name},
                    "spec": {
                        "flexVolume": {"secretRef": {"name": format!("secret-{pv_name}")}},
                        "claimRef": {"name": pvc_name, "namespace": ns},
                    },
                }),
            );
            for i in 0..2 {
                populator_add_pcr(
                    &graph,
                    &json!({
                        "metadata": {"namespace": ns, "name": format!("pcr{i}-{name}")},
                        "spec": {"nodeName": node, "podName": name},
                    }),
                );
            }
        }
        populator_update_volume_attachment(
            &graph,
            None,
            &json!({"metadata": {"name": format!("attachment0-{node}")}, "spec": {"nodeName": node}}),
        );
        for s in 0..=2 {
            populator_add_resource_slice(
                &graph,
                &json!({"metadata": {"name": format!("slice{s}-{node}")}, "spec": {"nodeName": node}}),
            );
        }
    }
    NodeAuthorizer::new(graph)
}

const NO_REL: &str = "no relationship found between node 'node0' and this object";

fn check(az: &NodeAuthorizer, name: &str, attrs: RequestAttributes, allow: bool, reason: &str) {
    let got = az.evaluate(&attrs);
    match (allow, got) {
        (true, Opinion::Allow(r)) => assert_eq!(r, "", "{name}"),
        (false, Opinion::NoOpinion { reason: r, error }) => {
            assert_eq!(r, reason, "{name}");
            assert_eq!(error, None, "{name}");
        }
        (_, got) => panic!("{name}: expected allow={allow}, got {got:?}"),
    }
}

#[test]
fn node_authorizer_table() {
    let az = populated();
    let sub = |a: RequestAttributes, s: &str| a.with_subresource(s);
    let ns = |a: RequestAttributes, n: &str| a.with_namespace(n);
    let nm = |a: RequestAttributes, n: &str| a.with_name(n);
    let get = |g: &str, r: &str| req("node0", "get", g, r);

    // secrets / configmaps / pvc / pv / resourceclaims / attachments
    check(
        &az,
        "allowed configmap",
        nm(ns(get("", "configmaps"), "ns0"), "configmap0-pod0-node0"),
        true,
        "",
    );
    check(
        &az,
        "allowed secret via pod",
        nm(ns(get("", "secrets"), "ns0"), "secret0-pod0-node0"),
        true,
        "",
    );
    for verb in ["list", "watch"] {
        check(
            &az,
            "allowed read via pod",
            nm(
                ns(req("node0", verb, "", "secrets"), "ns0"),
                "secret0-pod0-node0",
            ),
            true,
            "",
        );
        check(
            &az,
            "disallowed many",
            ns(req("node0", verb, "", "secrets"), "ns0"),
            false,
            "No Object name found",
        );
    }
    check(
        &az,
        "list all namespaces with name",
        nm(req("node0", "list", "", "secrets"), "secret0-pod0-node0"),
        false,
        "can only read namespaced object of this type",
    );
    check(
        &az,
        "shared secret via pod",
        nm(ns(get("", "secrets"), "ns0"), "secret0-shared"),
        true,
        "",
    );
    check(
        &az,
        "secret via pvc via pv",
        nm(ns(get("", "secrets"), "ns0"), "secret-pv0-pod0-node0-ns0"),
        true,
        "",
    );
    check(
        &az,
        "allowed pvc",
        nm(
            ns(get("", "persistentvolumeclaims"), "ns0"),
            "pvc0-pod0-node0",
        ),
        true,
        "",
    );
    check(
        &az,
        "allowed resource claim",
        nm(
            ns(get("resource.k8s.io", "resourceclaims"), "ns0"),
            "claim0-pod0-node0-ns0",
        ),
        true,
        "",
    );
    check(
        &az,
        "allowed resource claim with template",
        nm(
            ns(get("resource.k8s.io", "resourceclaims"), "ns0"),
            "generated-claim-pod0-node0-ns0-0",
        ),
        true,
        "",
    );
    check(
        &az,
        "allowed pv",
        nm(get("", "persistentvolumes"), "pv0-pod0-node0-ns0"),
        true,
        "",
    );
    check(
        &az,
        "disallowed configmap",
        nm(ns(get("", "configmaps"), "ns0"), "configmap0-pod0-node1"),
        false,
        NO_REL,
    );
    check(
        &az,
        "disallowed secret via pod",
        nm(ns(get("", "secrets"), "ns0"), "secret0-pod0-node1"),
        false,
        NO_REL,
    );
    check(
        &az,
        "disallowed shared secret via pvc",
        nm(ns(get("", "secrets"), "ns0"), "secret-pv0-pod0-node1-ns0"),
        false,
        NO_REL,
    );
    check(
        &az,
        "disallowed pvc",
        nm(
            ns(get("", "persistentvolumeclaims"), "ns0"),
            "pvc0-pod0-node1",
        ),
        false,
        NO_REL,
    );
    check(
        &az,
        "disallowed resource claim, other node",
        nm(
            ns(get("resource.k8s.io", "resourceclaims"), "ns0"),
            "claim0-pod0-node1-ns0",
        ),
        false,
        NO_REL,
    );
    check(
        &az,
        "disallowed resource claim with template",
        nm(
            ns(get("resource.k8s.io", "resourceclaims"), "ns0"),
            "pod0-node1-claimtemplate0",
        ),
        false,
        NO_REL,
    );
    check(
        &az,
        "disallowed pv",
        nm(get("", "persistentvolumes"), "pv0-pod0-node1-ns0"),
        false,
        NO_REL,
    );
    check(
        &az,
        "allowed attachment",
        nm(
            get("storage.k8s.io", "volumeattachments"),
            "attachment0-node0",
        ),
        true,
        "",
    );
    check(
        &az,
        "attachment of another node",
        nm(
            get("storage.k8s.io", "volumeattachments"),
            "attachment0-node1",
        ),
        false,
        NO_REL,
    );
    // authorizeGet / authorizeStatusUpdate / authorizeReadNamespacedObject reasons
    check(
        &az,
        "pv list",
        nm(
            req("node0", "list", "", "persistentvolumes"),
            "pv0-pod0-node0-ns0",
        ),
        false,
        "can only get individual resources of this type",
    );
    check(
        &az,
        "pv subresource",
        sub(
            nm(get("", "persistentvolumes"), "pv0-pod0-node0-ns0"),
            "status",
        ),
        false,
        "cannot get subresource",
    );
    check(
        &az,
        "secret delete",
        nm(
            ns(req("node0", "delete", "", "secrets"), "ns0"),
            "secret0-pod0-node0",
        ),
        false,
        "can only read resources of this type",
    );
    check(
        &az,
        "secret subresource",
        sub(nm(ns(get("", "secrets"), "ns0"), "secret0-pod0-node0"), "x"),
        false,
        "cannot read subresource",
    );
    check(
        &az,
        "pvc status update related",
        sub(
            nm(
                ns(req("node0", "update", "", "persistentvolumeclaims"), "ns0"),
                "pvc0-pod0-node0",
            ),
            "status",
        ),
        true,
        "",
    );
    check(
        &az,
        "pvc status update unrelated",
        sub(
            nm(
                ns(req("node0", "patch", "", "persistentvolumeclaims"), "ns0"),
                "pvc0-pod0-node1",
            ),
            "status",
        ),
        false,
        NO_REL,
    );
    check(
        &az,
        "pvc status get",
        sub(
            nm(
                ns(get("", "persistentvolumeclaims"), "ns0"),
                "pvc0-pod0-node0",
            ),
            "status",
        ),
        false,
        "can only get/update/patch this type",
    );
    // an unregistered node has no vertex in the graph
    check(
        &az,
        "unknown node",
        nm(
            ns(req("nodeunregistered", "get", "", "secrets"), "ns0"),
            "secret0-pod0-node0",
        ),
        false,
        "no relationship found between node 'nodeunregistered' and this object",
    );

    // service accounts
    let tok = |name: &str| {
        sub(
            nm(
                ns(req("node0", "create", "", "serviceaccounts"), "ns0"),
                name,
            ),
            "token",
        )
    };
    check(&az, "svcacct token create", tok("svcacct0-node0"), true, "");
    check(
        &az,
        "svcacct token create unrelated",
        tok("svcacct0-node1"),
        false,
        NO_REL,
    );
    check(
        &az,
        "svcacct token no subresource",
        nm(
            ns(req("node0", "create", "", "serviceaccounts"), "ns0"),
            "svcacct0-node0",
        ),
        false,
        "can only create token subresource of serviceaccount",
    );
    check(
        &az,
        "svcacct token non create",
        sub(
            nm(
                ns(req("node0", "update", "", "serviceaccounts"), "ns0"),
                "svcacct0-node0",
            ),
            "token",
        ),
        false,
        "can only create tokens for individual service accounts",
    );
    check(
        &az,
        "get svcacct via pod",
        nm(ns(get("", "serviceaccounts"), "ns0"), "svcacct0-node0"),
        true,
        "",
    );
    let off = az.clone().with_features(NodeAuthorizerFeatures {
        kubelet_service_account_token_for_credential_providers: false,
        pod_certificate_request: false,
    });
    check(
        &off,
        "get svcacct feature disabled",
        nm(ns(get("", "serviceaccounts"), "ns0"), "svcacct0-node0"),
        false,
        "not allowed to get service accounts",
    );
    for verb in ["list", "watch"] {
        check(
            &off,
            "list svcacct feature disabled",
            nm(
                ns(req("node0", verb, "", "serviceaccounts"), "ns0"),
                "svcacct0-node0",
            ),
            false,
            "can only create tokens for individual service accounts",
        );
    }
    let pcr_only = az.clone().with_features(NodeAuthorizerFeatures {
        kubelet_service_account_token_for_credential_providers: false,
        pod_certificate_request: true,
    });
    check(
        &pcr_only,
        "svcacct get when PCR enabled",
        nm(ns(get("", "serviceaccounts"), "ns0"), "svcacct0-node0"),
        true,
        "",
    );

    // podcertificaterequests
    let pcr = |verb: &str| {
        nm(
            ns(
                req(
                    "node0",
                    verb,
                    "certificates.k8s.io",
                    "podcertificaterequests",
                ),
                "ns0",
            ),
            "pcr0-pod0-node0",
        )
    };
    let on = az.clone().with_features(NodeAuthorizerFeatures {
        kubelet_service_account_token_for_credential_providers: true,
        pod_certificate_request: true,
    });
    for verb in ["create", "get", "list", "watch"] {
        check(&az, "pcr gate off", pcr(verb), false, "");
    }
    check(&on, "pcr create", pcr("create"), true, "");
    check(&on, "pcr get", pcr("get"), true, "");
    let want = "can only list/watch podcertificaterequests with nodeName field selector";
    for verb in ["list", "watch"] {
        check(&on, "pcr unfiltered", pcr(verb), false, want);
        check(
            &on,
            "pcr other node",
            with_selector(pcr(verb), "spec.nodeName=node1"),
            false,
            want,
        );
        check(
            &on,
            "pcr own node",
            with_selector(pcr(verb), "spec.nodeName=node0"),
            true,
            "",
        );
    }
    check(
        &on,
        "pcr unrelated get",
        nm(
            ns(
                req(
                    "node0",
                    "get",
                    "certificates.k8s.io",
                    "podcertificaterequests",
                ),
                "ns0",
            ),
            "pcr0-pod0-node1",
        ),
        false,
        NO_REL,
    );
    check(
        &on,
        "pcr delete",
        pcr("delete"),
        false,
        "nodes may not delete podcertificaterequests",
    );
    check(
        &on,
        "pcr status",
        sub(pcr("get"), "status"),
        false,
        "nodes may not access the status subresource of PodCertificateRequests",
    );

    // leases
    let lease = |verb: &str, ns_: &str, name: &str| {
        let mut a = req("node0", verb, "coordination.k8s.io", "leases").with_namespace(ns_);
        if !name.is_empty() {
            a = a.with_name(name);
        }
        a
    };
    for verb in ["get", "create", "update", "patch", "delete"] {
        check(
            &az,
            "lease other ns",
            lease(verb, "foo", "node0"),
            false,
            "can only access leases in the \"kube-node-lease\" system namespace",
        );
        check(
            &az,
            "own lease",
            lease(verb, "kube-node-lease", "node0"),
            true,
            "",
        );
        if verb != "create" {
            check(
                &az,
                "other lease",
                lease(verb, "kube-node-lease", "node1"),
                false,
                "can only access node lease with the same name as the requesting node",
            );
        }
    }
    for verb in ["list", "watch"] {
        check(
            &az,
            "list leases",
            lease(verb, "kube-node-lease", ""),
            false,
            "can only get, create, update, patch, or delete a node lease",
        );
    }

    // csinodes
    let csi = |verb: &str, name: &str| {
        let mut a = req("node0", verb, "storage.k8s.io", "csinodes");
        if !name.is_empty() {
            a = a.with_name(name);
        }
        a
    };
    check(
        &az,
        "csinode subresource",
        sub(csi("get", "node0"), "csiDrivers"),
        false,
        "cannot authorize CSINode subresources",
    );
    for verb in ["get", "update", "patch", "delete"] {
        check(
            &az,
            "other csinode",
            csi(verb, "node1"),
            false,
            "can only access CSINode with the same name as the requesting node",
        );
    }
    for verb in ["list", "watch"] {
        check(
            &az,
            "list csinodes",
            csi(verb, ""),
            false,
            "can only get, create, update, patch, or delete a CSINode",
        );
    }
    for verb in ["get", "create", "update", "patch", "delete"] {
        check(&az, "own csinode", csi(verb, "node0"), true, "");
    }

    // resourceslices
    let slice = |verb: &str, name: &str| {
        let mut a = req("node0", verb, "resource.k8s.io", "resourceslices");
        if !name.is_empty() {
            a = a.with_name(name);
        }
        a
    };
    check(
        &az,
        "slice subresource",
        sub(slice("get", "slice0-node0"), "status"),
        false,
        "cannot authorize ResourceSlice subresources",
    );
    for verb in ["get", "update", "patch", "delete"] {
        check(
            &az,
            "other slice",
            slice(verb, "slice0-node1"),
            false,
            NO_REL,
        );
        check(&az, "own slice", slice(verb, "slice0-node0"), true, "");
    }
    check(
        &az,
        "create slice",
        slice("create", "slice0-node0"),
        true,
        "",
    );
    for verb in ["list", "watch", "deletecollection"] {
        check(
            &az,
            "filtered slice list",
            with_selector(slice(verb, ""), "spec.nodeName==node0"),
            true,
            "",
        );
        check(
            &az,
            "unfiltered slice list",
            slice(verb, ""),
            false,
            "can only list/watch/deletecollection resourceslices with nodeName field selector",
        );
    }
    check(&az, "slice proxy verb", slice("proxy", ""), false, "only the following verbs are allowed for a ResourceSlice: get, watch, list, create, update, patch, delete, deletecollection");

    // pods
    let pod = |verb: &str, name: &str| {
        let mut a = req("node0", verb, "", "pods").with_namespace("ns0");
        if !name.is_empty() {
            a = a.with_name(name);
        }
        a
    };
    check(&az, "get related pod", pod("get", "pod0-node0"), true, "");
    check(
        &az,
        "get unrelated pod",
        pod("get", "pod0-node1"),
        false,
        NO_REL,
    );
    for verb in ["list", "watch"] {
        for sel in ["spec.nodeName=node0", "spec.nodeName==node0"] {
            check(
                &az,
                "related pods",
                with_selector(req("node0", verb, "", "pods"), sel),
                true,
                "",
            );
        }
        check(&az, "single related pod", pod(verb, "pod0-node0"), true, "");
        check(
            &az,
            "unrelated pods",
            req("node0", verb, "", "pods"),
            false,
            "can only list/watch pods with spec.nodeName field selector",
        );
        check(
            &az,
            "other node's selector",
            with_selector(req("node0", verb, "", "pods"), "spec.nodeName=node1"),
            false,
            "can only list/watch pods with spec.nodeName field selector",
        );
        check(
            &az,
            "single unrelated pod",
            pod(verb, "pod0-node1"),
            false,
            NO_REL,
        );
    }
    check(&az, "create unnamed pod", pod("create", ""), true, "");
    check(
        &az,
        "create pod proxy forbidden",
        sub(pod("create", ""), "proxy"),
        false,
        "",
    );
    check(
        &az,
        "delete related pod",
        pod("delete", "pod0-node0"),
        true,
        "",
    );
    check(
        &az,
        "delete unrelated pod",
        pod("delete", "pod0-node1"),
        true,
        "",
    ); // NodeRestriction
    check(
        &az,
        "delete pod proxy forbidden",
        sub(pod("delete", "pod0-node0"), "proxy"),
        false,
        "",
    );
    for verb in ["update", "patch"] {
        check(
            &az,
            "pod status",
            sub(pod(verb, "pod0-node0"), "status"),
            true,
            "",
        );
        check(
            &az,
            "unrelated pod status",
            sub(pod(verb, "pod0-node1"), "status"),
            true,
            "",
        ); // NodeRestriction
    }
    check(
        &az,
        "get pod status",
        sub(pod("get", "pod0-node0"), "status"),
        false,
        "",
    );
    // deviation: upstream allows pods/eviction create; see module docs
    check(
        &az,
        "pod eviction (deviation)",
        sub(pod("create", "pod0-node0"), "eviction"),
        false,
        "",
    );

    // nodes
    let node = |who: &str, verb: &str, name: &str| {
        let mut a = req(who, verb, "", "nodes");
        if !name.is_empty() {
            a = a.with_name(name);
        }
        a
    };
    check(
        &az,
        "get related unregistered node",
        node("nodeunregistered", "get", "nodeunregistered"),
        true,
        "",
    );
    for verb in ["get", "list", "watch"] {
        check(&az, "own node", node("node0", verb, "node0"), true, "");
        check(
            &az,
            "other node",
            node("node0", verb, "node1"),
            false,
            "node 'node0' cannot read 'node1', only its own Node object",
        );
    }
    for verb in ["list", "watch"] {
        check(
            &az,
            "all nodes",
            node("node0", verb, ""),
            false,
            "node 'node0' cannot read all nodes, only its own Node object",
        );
    }
    check(&az, "create node", node("node0", "create", ""), true, "");
    for verb in ["update", "patch"] {
        check(
            &az,
            "own node write",
            node("node0", verb, "node0"),
            true,
            "",
        );
        check(
            &az,
            "other node write",
            node("node0", verb, "node1"),
            true,
            "",
        ); // NodeRestriction
        check(
            &az,
            "node status",
            sub(node("node0", verb, "node1"), "status"),
            true,
            "",
        ); // NodeRestriction
    }
    check(
        &az,
        "delete node",
        node("node0", "delete", "node0"),
        false,
        "",
    );
    check(
        &az,
        "get node status",
        sub(node("node0", "get", "node0"), "status"),
        false,
        "",
    );
    // delete nodes/status is no longer allowed (was permissive before the port)
    check(
        &az,
        "delete node status",
        sub(node("node0", "delete", "node0"), "status"),
        false,
        "",
    );
}

/// node_authorizer.go:109-118 plus nodeidentifier/default.go:42-68.
#[test]
fn identity_and_static_rules() {
    let az = populated();
    let mut a = req("node0", "get", "", "services");
    // non-node: empty reason
    a.user.groups.clear();
    check(&az, "no group", a.clone(), false, "");
    a.user = user("alice");
    check(&az, "not a node", a.clone(), false, "");
    // prefix only, no node name
    check(
        &az,
        "empty node name",
        req("", "get", "", "services"),
        false,
        "unknown node for user \"system:node:\"",
    );
    // static rules for resources that are not subdivided
    check(
        &az,
        "services",
        req("node0", "list", "", "services"),
        true,
        "",
    );
    check(
        &az,
        "delete services",
        req("node0", "delete", "", "services"),
        false,
        "",
    );
    check(
        &az,
        "events",
        req("node0", "create", "", "events"),
        true,
        "",
    );
    check(
        &az,
        "tokenreviews",
        req("node0", "create", "authentication.k8s.io", "tokenreviews"),
        true,
        "",
    );
    // endpoints/get is in NodeRules, list is not
    check(
        &az,
        "endpoints get",
        req("node0", "get", "", "endpoints"),
        true,
        "",
    );
    check(
        &az,
        "endpoints list",
        req("node0", "list", "", "endpoints"),
        false,
        "",
    );
    // non-resource requests only see the static rules
    let mut nr = RequestAttributes::new_non_resource(user("system:node:node0"), "get", "/healthz");
    nr.resource = "secrets".into();
    check(&az, "non resource", nr, false, "");
}

/// `TestNodeAuthorizerSharedResources`.
#[test]
fn shared_resources() {
    let graph = Arc::new(NodeGraph::new());
    graph.set_destination_edge_threshold(1);
    let az = NodeAuthorizer::new(graph.clone());
    let mk = |name: &str, node: &str, secrets: &[&str]| -> Value {
        json!({
            "metadata": {"name": name, "namespace": "ns1"},
            "spec": {"nodeName": node,
                     "volumes": secrets.iter().map(|s| json!({"secret": {"secretName": s}})).collect::<Vec<_>>()},
        })
    };
    graph.add_pod(&mk(
        "pod1-node1",
        "node1",
        &["node1-only", "node1-node2-only", "shared-all"],
    ));
    graph.add_pod(&mk(
        "pod2-node2",
        "node2",
        &["node1-node2-only", "shared-all"],
    ));
    let pod3 = mk("pod3-node3", "node3", &["shared-all"]);
    graph.add_pod(&pod3);

    let cases = [
        ("node1", "node1-only", true),
        ("node1", "node1-node2-only", true),
        ("node1", "shared-all", true),
        ("node2", "node1-only", false),
        ("node2", "node1-node2-only", true),
        ("node2", "shared-all", true),
        ("node3", "node1-only", false),
        ("node3", "node1-node2-only", false),
        ("node3", "shared-all", true),
    ];
    for (node, secret, allow) in cases {
        let a = req(node, "get", "", "secrets")
            .with_namespace("ns1")
            .with_name(secret);
        assert_eq!(
            matches!(az.evaluate(&a), Opinion::Allow(_)),
            allow,
            "{node} {secret}"
        );
    }

    // should trigger recalculation of the shared secret index
    let a = req("node3", "get", "", "secrets")
        .with_namespace("ns1")
        .with_name("shared-all");
    assert!(matches!(az.evaluate(&a), Opinion::Allow(_)));
    graph.add_pod(&mk("pod3-node3", "node3", &[]));
    assert!(!matches!(az.evaluate(&a), Opinion::Allow(_)));
}

/// The trait entry points agree with `evaluate`, and `authorize` maps a
/// no-opinion to a legacy `Deny` carrying the upstream reason.
#[tokio::test]
async fn trait_entry_points() {
    let az = populated();
    let a = nm_get("secret0-pod0-node1");
    assert_eq!(
        az.authorize(&a).await.unwrap(),
        Decision::Deny(NO_REL.to_string())
    );
    assert_eq!(
        az.authorize_opinion(&a).await.unwrap(),
        Opinion::NoOpinion {
            reason: NO_REL.to_string(),
            error: None
        }
    );
    let a = nm_get("secret0-pod0-node0");
    assert_eq!(az.authorize(&a).await.unwrap(), Decision::Allow);
}

fn nm_get(name: &str) -> RequestAttributes {
    req("node0", "get", "", "secrets")
        .with_namespace("ns0")
        .with_name(name)
}
