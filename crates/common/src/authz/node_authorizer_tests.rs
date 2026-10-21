//! Ported from `plugin/pkg/auth/authorizer/node/node_authorizer_test.go`:
//! `TestNodeAuthorizer`, `TestNodeAuthorizerSharedResources` and
//! `TestNodeAuthorizerAddEphemeralContainers`, with `generate` /
//! `generatePod` / `populate` as the fixtures.
//!
//! One deliberate difference: with `AuthorizeNodeWithSelectors` off, pods fall
//! through to [`super::super::NODE_RULES`], which (unlike upstream's
//! `bootstrappolicy.NodeRules()`) omits `create pods/eviction` because there is
//! no NodeRestriction admission for evictions yet (see the comment on
//! `NODE_RULES`). Those rows are marked `EVICTION_GAP` below.

use super::super::node_graph::tests::{pod_from, pv_from};
use super::super::node_graph::{GraphPopulator, NodeGraph};
use super::*;
use crate::field_selector::FieldSelector;
use crate::resources::Pod;
use serde_json::{json, Value};

fn node_user(name: &str) -> UserInfo {
    UserInfo {
        username: format!("system:node:{name}"),
        uid: String::new(),
        groups: vec!["system:nodes".to_string()],
        extra: Default::default(),
    }
}

/// `sampleDataOpts` for `TestNodeAuthorizer`.
fn populate_sample_data(g: &Arc<NodeGraph>) {
    let pop = GraphPopulator::new(g.clone());
    let (nodes, namespaces, pods_per_node) = (2, 2, 2);
    for n in 0..nodes {
        let node_name = format!("node{n}");
        for p in 0..pods_per_node {
            let name = format!("pod{p}-{node_name}");
            let namespace = format!("ns{}", p % namespaces);
            let sa = format!("svcacct{p}-{node_name}");
            generate_pod(&pop, &name, &namespace, &node_name, &sa);
        }
        // attachmentsPerNode: 1
        pop.update_volume_attachment(None, &format!("attachment0-{node_name}"), &node_name);
        // nodeResourceSlicesPerNode: 2 (the loop is `p <= opts.nodeResourceSlicesPerNode`)
        for p in 0..=2 {
            pop.add_resource_slice(
                &serde_json::from_value(json!({
                    "metadata": {"name": format!("slice{p}-{node_name}")},
                    "spec": {"nodeName": node_name, "driver": "d", "pool": {"name": "p", "generation": 1, "resourceSliceCount": 1}},
                }))
                .expect("slice"),
            );
        }
    }
}

/// `generatePod`: the references are named to indicate the connections between
/// the objects (secret0-pod0-node0 is a secret referenced by pod0 bound to node0).
fn generate_pod(pop: &GraphPopulator, name: &str, namespace: &str, node: &str, sa: &str) {
    let mut volumes: Vec<Value> = Vec::new();
    let mut claims: Vec<Value> = Vec::new();
    let mut statuses: Vec<Value> = Vec::new();

    // uniqueSecretsPerPod: 1, sharedSecretsPerPod: 1 (-> secret0-shared)
    volumes.push(json!({"name": "s0", "secret": {"secretName": format!("secret0-{name}")}}));
    volumes.push(json!({"name": "s1", "secret": {"secretName": "secret0-shared"}}));
    // uniqueConfigMapsPerPod: 1
    volumes.push(json!({"name": "c0", "configMap": {"name": format!("configmap0-{name}")}}));
    // uniquePVCsPerPod: 1
    let pv_name = format!("pv0-{name}-{namespace}");
    let pvc_name = format!("pvc0-{name}");
    pop.add_pv(&pv_from(json!({
        "metadata": {"name": pv_name},
        "spec": {
            "flexVolume": {"driver": "d", "secretRef": {"name": format!("secret-{pv_name}")}},
            "claimRef": {"name": pvc_name, "namespace": namespace},
        },
    })));
    volumes.push(json!({"name": "p0", "persistentVolumeClaim": {"claimName": pvc_name}}));
    // uniqueResourceClaimsPerPod: 1
    claims
        .push(json!({"name": "claim0", "resourceClaimName": format!("claim0-{name}-{namespace}")}));
    // uniqueResourceClaimTemplatesPerPod: 1 (no status: no claim yet)
    claims.push(json!({"name": "claimtemplate0", "resourceClaimTemplateName": format!("claimtemplate0-{name}-{namespace}")}));
    // uniqueResourceClaimTemplatesWithClaimPerPod: 1
    claims.push(json!({"name": "claimtemplate-with-claim0", "resourceClaimTemplateName": format!("claimtemplate0-{name}-{namespace}")}));
    statuses.push(json!({
        "name": "claimtemplate-with-claim0",
        "resourceClaimName": format!("generated-claim-{name}-{namespace}-0"),
    }));

    pop.add_pod(&pod_from(json!({
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "nodeName": node,
            "serviceAccountName": sa,
            "containers": [],
            "volumes": volumes,
            "resourceClaims": claims,
        },
        "status": {"resourceClaimStatuses": statuses},
    })));

    // podCertificateRequestsPerPod: 2
    for i in 0..2 {
        pop.add_pcr(
            &serde_json::from_value(json!({
                "metadata": {"namespace": namespace, "name": format!("pcr{i}-{name}")},
                "spec": {"podName": name, "serviceAccountName": sa, "nodeName": node},
            }))
            .expect("pcr"),
        );
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Variant {
    SelectorDisabled,
    SelectorEnabled,
    SaTokenDisabled,
    SaTokenEnabled,
    PodCertEnabled,
    PodCertDisabled,
}

impl Variant {
    fn features(self) -> NodeAuthorizerFeatures {
        let default = NodeAuthorizerFeatures {
            authorize_node_with_selectors: true,
            kubelet_service_account_token_for_credential_providers: true,
            pod_certificate_request: false,
        };
        match self {
            Variant::SelectorDisabled => NodeAuthorizerFeatures {
                authorize_node_with_selectors: false,
                kubelet_service_account_token_for_credential_providers: false,
                ..default
            },
            Variant::SelectorEnabled => default,
            Variant::SaTokenDisabled => NodeAuthorizerFeatures {
                kubelet_service_account_token_for_credential_providers: false,
                pod_certificate_request: false,
                ..default
            },
            Variant::SaTokenEnabled => default,
            Variant::PodCertEnabled => NodeAuthorizerFeatures {
                pod_certificate_request: true,
                ..default
            },
            Variant::PodCertDisabled => default,
        }
    }
}

struct Row {
    name: &'static str,
    attrs: RequestAttributes,
    allow: bool,
    reason: &'static str,
    features: Option<Variant>,
    /// Row-specific expectation for the selector-disabled variant of an
    /// untagged row.
    disabled_allow: Option<bool>,
}

#[allow(clippy::too_many_arguments)]
fn req(
    user: &UserInfo,
    verb: &str,
    group: &str,
    resource: &str,
    sub: &str,
    name: &str,
    ns: &str,
    fields: &str,
) -> RequestAttributes {
    let mut a = RequestAttributes::new(user.clone(), verb, resource).with_api_group(group);
    if !sub.is_empty() {
        a = a.with_subresource(sub);
    }
    if !name.is_empty() {
        a = a.with_name(name);
    }
    if !ns.is_empty() {
        a = a.with_namespace(ns);
    }
    if !fields.is_empty() {
        a.field_selector_requirements = FieldSelector::parse(fields)
            .expect("field selector")
            .requirements()
            .to_vec();
    }
    a
}

fn allow(name: &'static str, attrs: RequestAttributes) -> Row {
    Row {
        name,
        attrs,
        allow: true,
        reason: "",
        features: None,
        disabled_allow: None,
    }
}

fn deny(name: &'static str, attrs: RequestAttributes) -> Row {
    Row {
        allow: false,
        ..allow(name, attrs)
    }
}

impl Row {
    fn with(mut self, features: Variant, reason: &'static str) -> Self {
        self.features = Some(features);
        self.reason = reason;
        self
    }

    /// `EVICTION_GAP`.
    fn disabled_denies(mut self) -> Self {
        self.disabled_allow = Some(false);
        self
    }
}

#[tokio::test]
async fn node_authorizer() {
    let g = Arc::new(NodeGraph::new());
    populate_sample_data(&g);

    let node0 = node_user("node0");
    let nodeunregistered = node_user("nodeunregistered");
    use Variant::*;

    let r = |verb: &str, group: &str, resource: &str, sub: &str, name: &str, ns: &str| {
        req(&node0, verb, group, resource, sub, name, ns, "")
    };
    let rf = |verb: &str, group: &str, resource: &str, name: &str, ns: &str, fields: &str| {
        req(&node0, verb, group, resource, "", name, ns, fields)
    };
    let lease_ns = "kube-node-lease";
    let rs = "resource.k8s.io";
    let storage = "storage.k8s.io";
    let coord = "coordination.k8s.io";
    let certs = "certificates.k8s.io";

    let rows = vec![
        allow("allowed configmap", r("get", "", "configmaps", "", "configmap0-pod0-node0", "ns0")),
        allow("allowed secret via pod", r("get", "", "secrets", "", "secret0-pod0-node0", "ns0")),
        allow("list allowed secret via pod", r("list", "", "secrets", "", "secret0-pod0-node0", "ns0")),
        allow("watch allowed secret via pod", r("watch", "", "secrets", "", "secret0-pod0-node0", "ns0")),
        deny("disallowed list many secrets", r("list", "", "secrets", "", "", "ns0")),
        deny("disallowed watch many secrets", r("watch", "", "secrets", "", "", "ns0")),
        deny(
            "disallowed list secrets from all namespaces with name",
            r("list", "", "secrets", "", "secret0-pod0-node0", ""),
        ),
        allow("allowed shared secret via pod", r("get", "", "secrets", "", "secret0-shared", "ns0")),
        allow(
            "allowed shared secret via pvc",
            r("get", "", "secrets", "", "secret-pv0-pod0-node0-ns0", "ns0"),
        ),
        allow("allowed pvc", r("get", "", "persistentvolumeclaims", "", "pvc0-pod0-node0", "ns0")),
        allow(
            "allowed resource claim",
            r("get", rs, "resourceclaims", "", "claim0-pod0-node0-ns0", "ns0"),
        ),
        allow(
            "allowed resource claim with template",
            r("get", rs, "resourceclaims", "", "generated-claim-pod0-node0-ns0-0", "ns0"),
        ),
        allow("allowed pv", r("get", "", "persistentvolumes", "", "pv0-pod0-node0-ns0", "")),
        deny("disallowed configmap", r("get", "", "configmaps", "", "configmap0-pod0-node1", "ns0")),
        deny("disallowed secret via pod", r("get", "", "secrets", "", "secret0-pod0-node1", "ns0")),
        deny(
            "disallowed shared secret via pvc",
            r("get", "", "secrets", "", "secret-pv0-pod0-node1-ns0", "ns0"),
        ),
        deny("disallowed pvc", r("get", "", "persistentvolumeclaims", "", "pvc0-pod0-node1", "ns0")),
        deny(
            "disallowed resource claim, other node",
            r("get", rs, "resourceclaims", "", "claim0-pod0-node1-ns0", "ns0"),
        ),
        deny(
            "disallowed resource claim with template",
            r("get", rs, "resourceclaims", "", "pod0-node1-claimtemplate0", "ns0"),
        ),
        deny("disallowed pv", r("get", "", "persistentvolumes", "", "pv0-pod0-node1-ns0", "")),
        allow("allowed attachment", r("get", storage, "volumeattachments", "", "attachment0-node0", "")),
        allow(
            "allowed svcacct token create",
            r("create", "", "serviceaccounts", "token", "svcacct0-node0", "ns0"),
        ),
        deny(
            "disallowed svcacct token create - serviceaccount not attached to node",
            r("create", "", "serviceaccounts", "token", "svcacct0-node1", "ns0"),
        ),
        deny(
            "disallowed svcacct token create - no subresource",
            r("create", "", "serviceaccounts", "", "svcacct0-node0", "ns0"),
        ),
        deny(
            "disallowed svcacct token create - non create",
            r("update", "", "serviceaccounts", "token", "svcacct0-node0", "ns0"),
        ),
        allow(
            "get allowed svcacct via pod - feature enabled",
            r("get", "", "serviceaccounts", "", "svcacct0-node0", "ns0"),
        )
        .with(SaTokenEnabled, ""),
        deny(
            "disallowed get svcacct via pod - feature disabled",
            r("get", "", "serviceaccounts", "", "svcacct0-node0", "ns0"),
        )
        .with(SaTokenDisabled, "not allowed to get service accounts"),
        deny(
            "disallowed list svcacct via pod - feature disabled",
            r("list", "", "serviceaccounts", "", "svcacct0-node0", "ns0"),
        )
        .with(SaTokenDisabled, "can only create tokens for individual service accounts"),
        deny(
            "disallowed watch svcacct via pod - feature disabled",
            r("watch", "", "serviceaccounts", "", "svcacct0-node0", "ns0"),
        )
        .with(SaTokenDisabled, "can only create tokens for individual service accounts"),
        allow(
            "allowed svcacct token create when PodCertificateProjection is enabled",
            r("create", "", "serviceaccounts", "token", "svcacct0-node0", "ns0"),
        )
        .with(PodCertEnabled, ""),
        allow(
            "allowed svcacct get when PodCertificateProjection is enabled",
            r("get", "", "serviceaccounts", "", "svcacct0-node0", "ns0"),
        )
        .with(PodCertEnabled, ""),
        deny(
            "disallowed pcr create when PodCertificateProjection is disabled",
            r("create", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(PodCertDisabled, ""),
        allow(
            "allowed pcr create when PodCertificateProjection is enabled",
            r("create", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(PodCertEnabled, ""),
        deny(
            "disallowed pcr get when PodCertificateProjection is disabled",
            r("get", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(PodCertDisabled, ""),
        allow(
            "allowed pcr get when PodCertificateProjection is enabled",
            r("get", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(PodCertEnabled, ""),
        deny(
            "disallowed pcr list when PodCertificateProjection is disabled",
            r("list", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(PodCertDisabled, ""),
        deny(
            "disallowed pcr list (un-filtered) when PodCertificateProjection is enabled",
            r("list", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(
            PodCertEnabled,
            "can only list/watch podcertificaterequests with nodeName field selector",
        ),
        deny(
            "disallowed pcr list (filtered to other node) when PodCertificateProjection is enabled",
            rf("list", certs, "podcertificaterequests", "pcr0-pod0-node0", "ns0", "spec.nodeName=othernode"),
        )
        .with(
            PodCertEnabled,
            "can only list/watch podcertificaterequests with nodeName field selector",
        ),
        allow(
            "allowed pcr list (filtered to correct node) when PodCertificateProjection is enabled",
            rf("list", certs, "podcertificaterequests", "pcr0-pod0-node0", "ns0", "spec.nodeName=node0"),
        )
        .with(PodCertEnabled, ""),
        deny(
            "disallowed pcr watch when PodCertificateProjection is disabled",
            r("watch", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(PodCertDisabled, ""),
        deny(
            "disallowed pcr watch (un-filtered) when PodCertificateProjection is enabled",
            r("watch", certs, "podcertificaterequests", "", "pcr0-pod0-node0", "ns0"),
        )
        .with(
            PodCertEnabled,
            "can only list/watch podcertificaterequests with nodeName field selector",
        ),
        deny(
            "disallowed pcr watch (filtered to other node) when PodCertificateProjection is enabled",
            rf("watch", certs, "podcertificaterequests", "pcr0-pod0-node0", "ns0", "spec.nodeName=othernode"),
        )
        .with(
            PodCertEnabled,
            "can only list/watch podcertificaterequests with nodeName field selector",
        ),
        allow(
            "allowed pcr watch (filtered to correct node) when PodCertificateProjection is enabled",
            rf("watch", certs, "podcertificaterequests", "pcr0-pod0-node0", "ns0", "spec.nodeName=node0"),
        )
        .with(PodCertEnabled, ""),
    ];

    // Leases: every verb in the wrong namespace / for another node.
    let mut rows = rows;
    for verb in ["get", "create", "update", "patch", "delete"] {
        rows.push(deny(
            "disallowed lease in namespace other than kube-node-lease",
            r(verb, coord, "leases", "", "node0", "foo"),
        ));
    }
    for verb in ["get", "update", "patch", "delete"] {
        rows.push(deny(
            "disallowed another node's lease",
            r(verb, coord, "leases", "", "node1", lease_ns),
        ));
    }
    for verb in ["list", "watch"] {
        rows.push(deny(
            "disallowed list/watch node leases",
            r(verb, coord, "leases", "", "", lease_ns),
        ));
    }
    for verb in ["get", "create", "update", "patch", "delete"] {
        rows.push(allow(
            "allowed node lease",
            r(verb, coord, "leases", "", "node0", lease_ns),
        ));
    }

    // CSINode
    rows.push(deny(
        "disallowed CSINode with subresource",
        r("get", storage, "csinodes", "csiDrivers", "node0", ""),
    ));
    for verb in ["get", "update", "patch", "delete"] {
        rows.push(deny(
            "disallowed another node's CSINode",
            r(verb, storage, "csinodes", "", "node1", ""),
        ));
    }
    for verb in ["list", "watch"] {
        rows.push(deny(
            "disallowed list/watch CSINodes",
            r(verb, storage, "csinodes", "", "", ""),
        ));
    }
    for verb in ["get", "create", "update", "patch", "delete"] {
        rows.push(allow(
            "allowed CSINode",
            r(verb, storage, "csinodes", "", "node0", ""),
        ));
    }

    // ResourceSlice
    rows.push(deny(
        "disallowed ResourceSlice with subresource",
        r("get", rs, "resourceslices", "status", "slice0-node0", ""),
    ));
    for verb in ["get", "update", "patch", "delete"] {
        rows.push(deny(
            "disallowed another node's ResourceSlice",
            r(verb, rs, "resourceslices", "", "slice0-node1", ""),
        ));
    }
    for verb in ["list", "watch"] {
        rows.push(allow(
            "allowed filtered list/watch ResourceSlices",
            rf(verb, rs, "resourceslices", "", "", "spec.nodeName==node0"),
        ));
        rows.push(
            allow(
                "allowed unfiltered list/watch ResourceSlices - selector authz disabled",
                r(verb, rs, "resourceslices", "", "", ""),
            )
            .with(SelectorDisabled, ""),
        );
        rows.push(
            deny(
                "disallowed unfiltered list/watch ResourceSlices - selector authz enabled",
                r(verb, rs, "resourceslices", "", "", ""),
            )
            .with(
                SelectorEnabled,
                "can only list/watch/deletecollection resourceslices with nodeName field selector",
            ),
        );
    }
    for verb in ["get", "create", "update", "patch", "delete"] {
        rows.push(allow(
            "allowed ResourceSlice",
            r(verb, rs, "resourceslices", "", "slice0-node0", ""),
        ));
    }

    // pods
    rows.push(allow(
        "get related pod",
        r("get", "", "pods", "", "pod0-node0", "ns0"),
    ));
    rows.push(
        allow(
            "get unrelated pod - selector disabled",
            r("get", "", "pods", "", "pod0-node1", "ns0"),
        )
        .with(SelectorDisabled, ""),
    );
    rows.push(
        deny(
            "get unrelated pod - selector enabled",
            r("get", "", "pods", "", "pod0-node1", "ns0"),
        )
        .with(
            SelectorEnabled,
            "no relationship found between node 'node0' and this object",
        ),
    );
    for verb in ["list", "watch"] {
        rows.push(allow(
            "related pods",
            rf(verb, "", "pods", "", "", "spec.nodeName=node0"),
        ));
        rows.push(allow(
            "related pods - alternate selector",
            rf(verb, "", "pods", "", "", "spec.nodeName==node0"),
        ));
        rows.push(allow(
            "single related pod",
            r(verb, "", "pods", "", "pod0-node0", "ns0"),
        ));
        rows.push(
            allow(
                "unrelated pods - selector disabled",
                r(verb, "", "pods", "", "", ""),
            )
            .with(SelectorDisabled, ""),
        );
        rows.push(
            deny(
                "unrelated pods - selector enabled",
                r(verb, "", "pods", "", "", ""),
            )
            .with(
                SelectorEnabled,
                "can only list/watch pods with spec.nodeName field selector",
            ),
        );
    }
    rows.push(allow(
        "create unnamed pod",
        r("create", "", "pods", "", "", "ns0"),
    ));
    rows.push(deny(
        "create pod proxy subresource forbidden",
        r("create", "", "pods", "proxy", "", "ns0"),
    ));
    rows.push(allow(
        "delete related pod",
        r("delete", "", "pods", "", "pod0-node0", "ns0"),
    ));
    rows.push(allow(
        "delete unrelated pod",
        r("delete", "", "pods", "", "pod0-node1", "ns0"),
    ));
    rows.push(deny(
        "delete pod proxy subresource forbidden",
        r("delete", "", "pods", "proxy", "pod0-node0", "ns0"),
    ));
    for verb in ["update", "patch"] {
        rows.push(allow(
            "related pod status",
            r(verb, "", "pods", "status", "pod0-node0", "ns0"),
        ));
        rows.push(allow(
            "unrelated pod status",
            r(verb, "", "pods", "status", "pod0-node1", "ns0"),
        ));
    }
    rows.push(deny(
        "get pod status",
        r("get", "", "pods", "status", "pod0-node0", "ns0"),
    ));
    rows.push(
        allow(
            "create related pod eviction",
            r("create", "", "pods", "eviction", "pod0-node0", "ns0"),
        )
        .disabled_denies(),
    );
    rows.push(
        allow(
            "create unrelated pod eviction",
            r("create", "", "pods", "eviction", "pod0-node1", "ns0"),
        )
        .disabled_denies(),
    );

    // nodes
    rows.push(allow(
        "get related unregistered node",
        req(
            &nodeunregistered,
            "get",
            "",
            "nodes",
            "",
            "nodeunregistered",
            "",
            "",
        ),
    ));
    rows.push(allow(
        "get related node",
        r("get", "", "nodes", "", "node0", ""),
    ));
    for verb in ["get", "list", "watch"] {
        rows.push(
            allow(
                "unrelated node - selector disabled",
                r(verb, "", "nodes", "", "node1", ""),
            )
            .with(SelectorDisabled, ""),
        );
        rows.push(
            deny(
                "unrelated node - selector enabled",
                r(verb, "", "nodes", "", "node1", ""),
            )
            .with(
                SelectorEnabled,
                "node 'node0' cannot read 'node1', only its own Node object",
            ),
        );
    }
    for verb in ["list", "watch"] {
        rows.push(allow(
            "single related node",
            r(verb, "", "nodes", "", "node0", ""),
        ));
        rows.push(
            allow(
                "all nodes - selector disabled",
                r(verb, "", "nodes", "", "", ""),
            )
            .with(SelectorDisabled, ""),
        );
        rows.push(
            deny(
                "all nodes - selector enabled",
                r(verb, "", "nodes", "", "", ""),
            )
            .with(
                SelectorEnabled,
                "node 'node0' cannot read all nodes, only its own Node object",
            ),
        );
    }
    rows.push(allow("create node", r("create", "", "nodes", "", "", "")));
    for verb in ["update", "patch"] {
        rows.push(allow("related node", r(verb, "", "nodes", "", "node0", "")));
        rows.push(allow(
            "unrelated node",
            r(verb, "", "nodes", "", "node1", ""),
        ));
        rows.push(allow(
            "related node status",
            r(verb, "", "nodes", "status", "node0", ""),
        ));
        rows.push(allow(
            "unrelated node status",
            r(verb, "", "nodes", "status", "node1", ""),
        ));
    }

    for row in rows {
        let variants: Vec<(String, Variant, bool)> = match row.features {
            Some(v) => vec![(row.name.to_string(), v, row.allow)],
            None => vec![
                (
                    format!("{}_selector_disabled", row.name),
                    SelectorDisabled,
                    row.disabled_allow.unwrap_or(row.allow),
                ),
                (
                    format!("{}_selector_enabled", row.name),
                    SelectorEnabled,
                    row.allow,
                ),
            ],
        };
        for (name, variant, expect_allow) in variants {
            let authz = NodeAuthorizer::new(g.clone()).with_features(variant.features());
            let opinion = authz.authorize_opinion(&row.attrs).await.unwrap();
            let got_allow = matches!(opinion, Opinion::Allow(_));
            assert_eq!(
                got_allow, expect_allow,
                "{name}: expected allow={expect_allow}, got {opinion:?}"
            );
            if row.features.is_some() {
                match opinion {
                    Opinion::Allow(reason) => assert_eq!(reason, row.reason, "{name}"),
                    Opinion::NoOpinion { reason, error } => {
                        assert_eq!(reason, row.reason, "{name}");
                        assert_eq!(error, None, "{name}");
                    }
                    Opinion::Deny(_) => panic!("{name}: node authorizer never denies"),
                }
            }
        }
    }
}

/// An unknown node and an unknown object are distinct `hasPathFrom` errors
/// but one rejection reason (node_authorizer.go:206-215).
#[tokio::test]
async fn unknown_node_and_unknown_object_share_the_relationship_reason() {
    let g = Arc::new(NodeGraph::new());
    populate_sample_data(&g);
    let authz = NodeAuthorizer::new(g.clone());
    for (user, name) in [
        (node_user("node0"), "no-such-secret"),
        (node_user("never-registered"), "secret0-shared"),
    ] {
        let attrs = req(&user, "get", "", "secrets", "", name, "ns0", "");
        let node = user.username.trim_start_matches("system:node:").to_string();
        assert_eq!(
            authz.authorize_opinion(&attrs).await.unwrap(),
            Opinion::NoOpinion {
                reason: format!("no relationship found between node '{node}' and this object"),
                error: None
            }
        );
    }
    let g = g.read();
    assert_eq!(
        g.has_path_from("never-registered", VertexType::Secret, "ns0", "x"),
        Err("unknown node 'never-registered' cannot get secret ns0/x".to_string())
    );
    assert_eq!(
        g.has_path_from("node0", VertexType::Secret, "ns0", "x"),
        Err("node 'node0' cannot get unknown secret ns0/x".to_string())
    );
    assert_eq!(
        g.has_path_from("node1", VertexType::Secret, "ns0", "secret0-pod0-node0"),
        Err("node 'node1' cannot get secret ns0/secret0-pod0-node0, no relationship to this object was found in the node authorizer graph".to_string())
    );
}

#[tokio::test]
async fn unidentifiable_node_gets_the_quoted_identity_reason() {
    let authz = NodeAuthorizer::new(Arc::new(NodeGraph::new()));
    let attrs = req(&node_user(""), "get", "", "secrets", "", "s", "ns", "");
    assert_eq!(
        authz.authorize_opinion(&attrs).await.unwrap(),
        Opinion::NoOpinion {
            reason: "unknown node for user \"system:node:\"".into(),
            error: None
        }
    );
}

#[tokio::test]
async fn node_authorizer_shared_resources() {
    let g = Arc::new(NodeGraph::new());
    g.set_destination_edge_threshold(1);
    let authz = NodeAuthorizer::new(g.clone());

    let pod = |name: &str, node: &str, secrets: &[&str]| {
        pod_from(json!({
            "metadata": {"name": name, "namespace": "ns1"},
            "spec": {
                "nodeName": node,
                "containers": [],
                "volumes": secrets
                    .iter()
                    .map(|s| json!({"name": s, "secret": {"secretName": s}}))
                    .collect::<Vec<_>>(),
            },
        }))
    };
    g.add_pod(&pod(
        "pod1-node1",
        "node1",
        &["node1-only", "node1-node2-only", "shared-all"],
    ));
    g.add_pod(&pod(
        "pod2-node2",
        "node2",
        &["node1-node2-only", "shared-all"],
    ));
    g.add_pod(&pod("pod3-node3", "node3", &["shared-all"]));

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
    for (i, (node, secret, expect)) in cases.into_iter().enumerate() {
        let attrs = req(
            &node_user(node),
            "get",
            "",
            "secrets",
            "",
            secret,
            "ns1",
            "",
        );
        let got = matches!(
            authz.authorize_opinion(&attrs).await.unwrap(),
            Opinion::Allow(_)
        );
        assert_eq!(got, expect, "{i}: {node} {secret}");
    }

    let node3_shared = req(
        &node_user("node3"),
        "get",
        "",
        "secrets",
        "",
        "shared-all",
        "ns1",
        "",
    );
    assert!(matches!(
        authz.authorize_opinion(&node3_shared).await.unwrap(),
        Opinion::Allow(_)
    ));

    // should trigger recalculation of the shared secret index
    g.add_pod(&pod("pod3-node3", "node3", &[]));

    assert!(
        !matches!(
            authz.authorize_opinion(&node3_shared).await.unwrap(),
            Opinion::Allow(_)
        ),
        "unexpectedly allowed"
    );
}

#[tokio::test]
async fn node_authorizer_add_ephemeral_containers() {
    let g = Arc::new(NodeGraph::new());
    g.set_destination_edge_threshold(1);
    let authz = NodeAuthorizer::new(g.clone());
    let pop = GraphPopulator::new(g.clone());

    let base = json!({
        "metadata": {"name": "pod1-node1", "namespace": "ns1"},
        "spec": {
            "nodeName": "node1",
            "containers": [],
            "volumes": [{"name": "v", "secret": {"secretName": "node1-only"}}],
        },
    });
    let old = pod_from(base.clone());
    pop.add_pod(&old);

    let eph = |env_from: Value| {
        let mut p = base.clone();
        p["spec"]["ephemeralContainers"] = json!([{
            "name": "eph",
            "image": "imageURL",
            "command": ["command"],
            "targetContainerName": "targetContainerName",
            "envFrom": [env_from],
            "securityContext": {"privileged": true},
        }]);
        pod_from(p)
    };
    let new_secret = eph(json!({"secretRef": {"name": "new-secret"}}));
    let new_config_map = eph(json!({"configMapRef": {"name": "new-config-map"}}));

    // (resource, name, expected, ephemeral container update applied first)
    let cases: Vec<(&str, &str, bool, Option<&Pod>)> = vec![
        ("secrets", "node1-only", true, None),
        ("secrets", "new-secret", false, None),
        ("secrets", "new-secret", true, Some(&new_secret)),
        ("configmaps", "new-config-map", false, None),
        ("configmaps", "new-config-map", true, Some(&new_config_map)),
    ];
    for (i, (resource, name, expect, update)) in cases.into_iter().enumerate() {
        if let Some(new_pod) = update {
            pop.update_pod(Some(&old), new_pod);
        }
        let attrs = req(
            &node_user("node1"),
            "get",
            "",
            resource,
            "",
            name,
            "ns1",
            "",
        );
        let got = matches!(
            authz.authorize_opinion(&attrs).await.unwrap(),
            Opinion::Allow(_)
        );
        assert_eq!(got, expect, "{i}: {resource} {name}");
    }
}
