use anyhow::{Context, Result};
use rusternetes_common::resources::{EndpointSlice, Endpoints};
use rusternetes_storage::Storage;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

#[cfg(test)]
const CLUSTER_ADMIN_ROLE_KEY: &str = "/registry/clusterroles/cluster-admin";
#[cfg(test)]
const CLUSTER_ADMIN_BINDING_KEY: &str = "/registry/clusterrolebindings/cluster-admin";

/// Upstream's bootstrap policy, vendored verbatim from
/// `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/testdata/` (release-1.35).
/// Those files are the serialised output of `ClusterRoles()`,
/// `ClusterRoleBindings()`, `ControllerRoles()`, `ControllerRoleBindings()`,
/// `NamespaceRoles()` and `NamespaceRoleBindings()` in `policy.go` /
/// `controller_policy.go` / `namespace_policy.go`, so every role carries the
/// `kubernetes.io/bootstrapping=rbac-defaults` label and the
/// `rbac.authorization.kubernetes.io/autoupdate=true` annotation exactly as
/// `addClusterRoleLabel` (policy.go:48-60) stamps them. These are the output
/// with every feature gate at its v1.35 default; what the non-default gates add
/// is applied by [`apply_feature_gated_policy`] (the
/// `cluster-roles-featuregates.yaml` variant is vendored for the test only).
const BOOTSTRAP_POLICY: &[&str] = &[
    include_str!("bootstrap_policy/cluster-roles.yaml"),
    include_str!("bootstrap_policy/controller-roles.yaml"),
    include_str!("bootstrap_policy/namespace-roles.yaml"),
    include_str!("bootstrap_policy/cluster-role-bindings.yaml"),
    include_str!("bootstrap_policy/controller-role-bindings.yaml"),
    include_str!("bootstrap_policy/namespace-role-bindings.yaml"),
];

use crate::registry::rbac::reconciliation::{
    compute_reconciled_role, compute_reconciled_role_binding, ReconcileOperation,
};

#[cfg(test)]
use crate::registry::rbac::reconciliation::AUTOUPDATE_ANNOTATION;

/// Decode one vendored `v1.List` into its items. Mirrors nothing upstream (Go
/// builds the objects in code); this is only the Rust-side loader for the
/// vendored testdata. A role's `rules: null` becomes `[]` so it decodes as a
/// typed `ClusterRole`.
fn load_policy_items(yaml: &str) -> Result<Vec<serde_json::Value>> {
    let list: serde_json::Value =
        serde_yaml::from_str(yaml).context("bootstrap policy YAML must parse")?;
    let mut items = list["items"].as_array().cloned().unwrap_or_default();
    for item in &mut items {
        if item["kind"].as_str().is_some_and(|k| k.ends_with("Role")) && item["rules"].is_null() {
            item["rules"] = serde_json::json!([]);
        }
    }
    Ok(items)
}

fn policy_key(item: &serde_json::Value) -> Option<(String, String)> {
    let plural = match item["kind"].as_str()? {
        "ClusterRole" => "clusterroles",
        "ClusterRoleBinding" => "clusterrolebindings",
        "Role" => "roles",
        "RoleBinding" => "rolebindings",
        _ => return None,
    };
    let name = item["metadata"]["name"].as_str()?;
    let ns = item["metadata"]["namespace"].as_str();
    Some((
        rusternetes_storage::build_key(plural, ns, name),
        item["kind"].as_str()?.to_string(),
    ))
}

/// `NewRule(...).RuleOrDie()` sorts the verbs, which is why the testdata reads
/// `create, get, list, watch` for `podcertificaterequests`.
fn rule(api_groups: &[&str], resources: &[&str], verbs: &[&str]) -> serde_json::Value {
    serde_json::json!({"apiGroups": api_groups, "resources": resources, "verbs": verbs})
}

fn url_rule(url: &str) -> serde_json::Value {
    serde_json::json!({"nonResourceURLs": [url], "verbs": ["get"]})
}

fn push_rules(items: &mut [serde_json::Value], role: &str, rules: Vec<serde_json::Value>) {
    let target = items
        .iter_mut()
        .find(|i| i["kind"] == "ClusterRole" && i["metadata"]["name"] == role)
        .unwrap_or_else(|| panic!("vendored policy has no ClusterRole {role}"));
    let list = target["rules"].as_array_mut().expect("rules is an array");
    list.extend(rules);
}

/// Insert `rules` just before the rule that grants `get` on core
/// `serviceaccounts`, where `NodeRules` places the `ClusterTrustBundle` rule
/// (`policy.go:270-278`: it precedes the credential-provider
/// ServiceAccount rule, which is on by default and so already vendored).
fn insert_before_node_serviceaccounts_rule(
    items: &mut [serde_json::Value],
    rules: Vec<serde_json::Value>,
) {
    let target = items
        .iter_mut()
        .find(|i| i["kind"] == "ClusterRole" && i["metadata"]["name"] == "system:node")
        .expect("vendored policy has system:node");
    let list = target["rules"].as_array_mut().expect("rules is an array");
    let at = list
        .iter()
        .position(|r| r["resources"] == serde_json::json!(["serviceaccounts"]))
        .unwrap_or(list.len());
    list.splice(at..at, rules);
}

/// The feature-gated part of the ClusterRoles / ClusterRoleBindings. The
/// vendored YAML is `ClusterRoles()` with every gate at its default; this adds
/// what the branches `if utilfeature.DefaultFeatureGate.Enabled(...)` append
/// when a gate is switched on, in upstream's order:
/// * `system:monitoring`: `ComponentFlagz` `/flagz`, `ComponentStatusz`
///   `/statusz` (`policy.go:301-307`)
/// * `system:node` (`NodeRules`): `ClusterTrustBundle`, `PodCertificateRequest`
///   (`policy.go:271-283`; `KubeletServiceAccountTokenForCredentialProviders`
///   is on by default so it is in the vendored YAML)
/// * `system:kube-scheduler`: `DRAExtendedResource`, `DRADeviceTaintRules`
///   (under `DynamicResourceAllocation`, GA and on in v1.35) then
///   `GenericWorkload` (`policy.go:644-655`)
/// * `ClusterTrustBundle`: the `system:cluster-trust-bundle-discovery`
///   ClusterRole (`policy.go:663-672`) and its binding to
///   `system:serviceaccounts` (`policy.go:713-715`)
///
/// * `ControllerRoles()` (`controller_policy.go`), see
///   [`apply_feature_gated_controller_roles`]
///
/// Gates the vendored default output already covers (`DynamicResourceAllocation`,
/// `MultiCIDRServiceAllocator`, `KubeletFineGrainedAuthz`) are GA/on in v1.35
/// and so need no branch here.
fn apply_feature_gated_policy(items: &mut Vec<serde_json::Value>) {
    use rusternetes_common::feature_gates::{enabled, Feature};
    const READ: [&str; 3] = ["get", "list", "watch"];
    const CERTS: [&str; 1] = ["certificates.k8s.io"];

    if enabled(Feature::ComponentFlagz) {
        push_rules(items, "system:monitoring", vec![url_rule("/flagz")]);
    }
    if enabled(Feature::ComponentStatusz) {
        push_rules(items, "system:monitoring", vec![url_rule("/statusz")]);
    }

    if enabled(Feature::ClusterTrustBundle) {
        insert_before_node_serviceaccounts_rule(
            items,
            vec![rule(&CERTS, &["clustertrustbundles"], &READ)],
        );
    }
    if enabled(Feature::PodCertificateRequest) {
        push_rules(
            items,
            "system:node",
            vec![rule(
                &CERTS,
                &["podcertificaterequests"],
                &["create", "get", "list", "watch"],
            )],
        );
    }

    let mut scheduler = Vec::new();
    if enabled(Feature::DRAExtendedResource) {
        scheduler.push(rule(
            &["resource.k8s.io"],
            &["resourceclaims"],
            &["create", "delete"],
        ));
    }
    if enabled(Feature::DRADeviceTaintRules) {
        scheduler.push(rule(&["resource.k8s.io"], &["devicetaintrules"], &READ));
    }
    if enabled(Feature::GenericWorkload) {
        scheduler.push(rule(&["scheduling.k8s.io"], &["workloads"], &READ));
    }
    push_rules(items, "system:kube-scheduler", scheduler);

    if enabled(Feature::ClusterTrustBundle) {
        let meta = |name: &str| {
            serde_json::json!({
                "name": name,
                "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
                "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"},
            })
        };
        let mut role = serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "rules": [rule(&CERTS, &["clustertrustbundles"], &READ)],
        });
        role["metadata"] = meta("system:cluster-trust-bundle-discovery");
        let mut binding = serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "system:cluster-trust-bundle-discovery",
            },
            "subjects": [{
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Group",
                "name": "system:serviceaccounts",
            }],
        });
        binding["metadata"] = meta("system:cluster-trust-bundle-discovery");
        items.push(role);
        items.push(binding);
    }

    apply_feature_gated_controller_roles(items);
}

/// `eventsRule()` (`controller_policy.go:55-57`).
fn events_rule() -> serde_json::Value {
    rule(
        &["", "events.k8s.io"],
        &["events"],
        &["create", "patch", "update"],
    )
}

/// `addControllerRole` (`controller_policy.go:36-52`): the
/// `system:controller:<name>` ClusterRole plus its ClusterRoleBinding to the
/// `kube-system/<name>` ServiceAccount, both with the bootstrapping label and
/// autoupdate annotation (`addClusterRoleLabel` / `addClusterRoleBindingLabel`).
fn add_controller_role(
    items: &mut Vec<serde_json::Value>,
    short: &str,
    rules: Vec<serde_json::Value>,
) {
    let name = format!("system:controller:{short}");
    // upstream `klog.Fatalf("role %q was already registered")`
    assert!(
        !items
            .iter()
            .any(|i| i["kind"] == "ClusterRole" && i["metadata"]["name"] == name.as_str()),
        "role {name:?} was already registered"
    );
    let meta = serde_json::json!({
        "name": name,
        "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
        "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"},
    });
    items.push(serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": meta,
        "rules": rules,
    }));
    items.push(serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": meta,
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": name,
        },
        "subjects": [{"kind": "ServiceAccount", "name": short, "namespace": "kube-system"}],
    }));
}

/// The `if utilfeature.DefaultFeatureGate.Enabled(...)` branches of
/// `buildControllerRoles` (`controller_policy.go`) whose gate is off in v1.35,
/// so the vendored `controller-roles.yaml` (default gates) lacks them:
/// `DRADeviceTaints` (+ `DRADeviceTaintRules`) under `DynamicResourceAllocation`
/// (`:206-231`), `PodCertificateRequest` (`:440-446`), `ClusterTrustBundle`
/// (`:490-498`), `StorageVersionAPI` && `APIServerIdentity` (`:512-523`),
/// `StorageVersionMigrator` (`:534-546`). The on-by-default branches
/// (`DynamicResourceAllocation`, `MultiCIDRServiceAllocator`,
/// `VolumeAttributesClass`, `SELinuxChangePolicy`) are in the YAML already.
fn apply_feature_gated_controller_roles(items: &mut Vec<serde_json::Value>) {
    use rusternetes_common::feature_gates::{enabled, Feature};
    const READ: [&str; 3] = ["get", "list", "watch"];
    const CERTS: [&str; 1] = ["certificates.k8s.io"];
    const DRA: [&str; 1] = ["resource.k8s.io"];

    // DynamicResourceAllocation is GA and on in v1.35 (the YAML carries
    // resource-claim-controller), so only the nested gates need a branch.
    if enabled(Feature::DRADeviceTaints) {
        let mut rules = vec![
            // Deletes pods to evict them.
            rule(&[""], &["pods"], &["get", "list", "watch", "delete"]),
            // Sets pod conditions.
            rule(&[""], &["pods/status"], &["update", "patch"]),
            // The rest is read-only.
            rule(&DRA, &["resourceclaims"], &READ),
            rule(&DRA, &["resourceslices"], &READ),
            rule(&DRA, &["deviceclasses"], &READ),
            events_rule(),
        ];
        if enabled(Feature::DRADeviceTaintRules) {
            // Sets DeviceTaintRule conditions.
            rules.push(rule(
                &DRA,
                &["devicetaintrules/status"],
                &["update", "patch"],
            ));
            // Read-only for spec.
            rules.push(rule(&DRA, &["devicetaintrules"], &READ));
        }
        add_controller_role(
            items,
            "device-taint-eviction-controller",
            sorted_verbs(rules),
        );
    }
    if enabled(Feature::PodCertificateRequest) {
        add_controller_role(
            items,
            "podcertificaterequestcleaner",
            vec![rule(
                &CERTS,
                &["podcertificaterequests"],
                &["delete", "get", "list", "watch"],
            )],
        );
    }
    if enabled(Feature::ClusterTrustBundle) {
        let mut attest = rule(&CERTS, &["signers"], &["attest"]);
        attest["resourceNames"] = serde_json::json!(["kubernetes.io/kube-apiserver-serving"]);
        add_controller_role(
            items,
            "kube-apiserver-serving-clustertrustbundle-publisher",
            vec![
                attest,
                rule(
                    &CERTS,
                    &["clustertrustbundles"],
                    &["create", "delete", "list", "update", "watch"],
                ),
                events_rule(),
            ],
        );
    }
    if enabled(Feature::StorageVersionAPI) && enabled(Feature::APIServerIdentity) {
        add_controller_role(
            items,
            "storage-version-garbage-collector",
            vec![
                rule(&["coordination.k8s.io"], &["leases"], &READ),
                rule(
                    &["internal.apiserver.k8s.io"],
                    &["storageversions"],
                    &["delete", "get", "list", "patch", "update", "watch"],
                ),
                rule(
                    &["internal.apiserver.k8s.io"],
                    &["storageversions/status"],
                    &["get", "patch", "update"],
                ),
            ],
        );
    }
    if enabled(Feature::StorageVersionMigrator) {
        add_controller_role(
            items,
            "storage-version-migrator-controller",
            vec![
                // need list to get current RV for any resource
                // need patch for SSA of any resource
                // need create because SSA of a deleted resource will be
                // interpreted as a create request (always a conflict: UID set)
                rule(&["*"], &["*"], &["create", "list", "patch"]),
                rule(
                    &["storagemigration.k8s.io"],
                    &["storageversionmigrations/status"],
                    &["update"],
                ),
            ],
        );
    }
}

/// `NewRule(...).RuleOrDie()` sorts each rule's verbs.
fn sorted_verbs(mut rules: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    for r in &mut rules {
        if let Some(v) = r["verbs"].as_array_mut() {
            v.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        }
    }
    rules
}

/// Every bootstrap policy object with the feature gates applied.
fn bootstrap_policy_items() -> Result<Vec<serde_json::Value>> {
    let mut items = Vec::new();
    for yaml in BOOTSTRAP_POLICY {
        items.extend(load_policy_items(yaml)?);
    }
    apply_feature_gated_policy(&mut items);
    Ok(items)
}

/// Seed the full upstream RBAC bootstrap policy (#1659, #1753), mirroring
/// `EnsureRBACPolicy` (`pkg/registry/rbac/rest/storage_rbac.go:269-331`, run
/// from the `rbac/bootstrap-roles` post-start hook): every bootstrap
/// ClusterRole/ClusterRoleBinding/Role/RoleBinding is created if missing, and
/// an existing one is reconciled unless it opted out with
/// `rbac.authorization.kubernetes.io/autoupdate: "false"`
/// (`component-helpers/auth/rbac/reconciliation/reconcile_role.go`,
/// `reconcile_rolebindings.go`: add missing rules / subjects, never remove).
/// Namespaced objects get their namespace created first (`tryEnsureNamespace`,
/// `namespace.go:31-45`).
///
/// This includes `cluster-admin` bound to `system:masters`: without it a
/// freshly bootstrapped (empty) store denies the cluster admin — kubeadm's
/// `CN=kubernetes-admin, O=system:masters` client cert — every request (#1659).
/// The superuser effect comes from a real RBAC rule, so the
/// privilege-escalation check stays rule-based (NOT an authorizer
/// short-circuit). Idempotent.
///
/// Reconciliation is `registry/rbac/reconciliation.rs`
/// (`computeReconciledRole`, `computeReconciledRoleBinding`). `ClusterRole`s
/// with an `aggregationRule` are seeded without rules, as upstream; the
/// clusterroleaggregation controller (controller-manager) fills them.
pub async fn bootstrap_default_rbac(storage: Arc<StorageBackend>) -> Result<()> {
    let mut namespaces = std::collections::BTreeSet::new();

    {
        for mut item in bootstrap_policy_items()? {
            let Some((key, kind)) = policy_key(&item) else {
                continue;
            };
            if let Some(ns) = item["metadata"]["namespace"].as_str() {
                if namespaces.insert(ns.to_string()) {
                    create_namespace_if_needed(storage.as_ref(), ns).await?;
                }
            }
            match storage.get::<serde_json::Value>(&key).await {
                Ok(existing) => {
                    let result = if kind.ends_with("Binding") {
                        compute_reconciled_role_binding(&existing, &item)
                    } else {
                        compute_reconciled_role(&existing, &item)
                    };
                    if result.protected {
                        if result.operation != ReconcileOperation::None {
                            warn!("skipped reconcile-protected RBAC object {key}");
                        }
                        continue;
                    }
                    match result.operation {
                        ReconcileOperation::None => {}
                        ReconcileOperation::Update => {
                            storage
                                .update(&key, &result.object)
                                .await
                                .with_context(|| format!("reconcile {key}"))?;
                            info!("Reconciled bootstrap RBAC object {key}");
                        }
                        // reconcile_rolebindings.go:104-118: delete, then create.
                        ReconcileOperation::Recreate => {
                            let _ = storage.delete(&key).await;
                            let mut fresh = result.object;
                            fresh["metadata"]["uid"] = uuid::Uuid::new_v4().to_string().into();
                            fresh["metadata"]["creationTimestamp"] = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                                .into();
                            storage
                                .create(&key, &fresh)
                                .await
                                .with_context(|| format!("recreate {key}"))?;
                            info!("Recreated bootstrap RBAC binding {key}");
                        }
                    }
                }
                Err(_) => {
                    item["metadata"]["uid"] = uuid::Uuid::new_v4().to_string().into();
                    item["metadata"]["creationTimestamp"] = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                        .into();
                    match storage.create(&key, &item).await {
                        Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => {}
                        Err(e) => {
                            return Err(anyhow::Error::from(e))
                                .with_context(|| format!("create {key}"))
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// How often the `kubernetes` Service endpoint is re-asserted to the live
/// api-server IP. Mirrors upstream's `DefaultEndpointReconcilerInterval`
/// (`pkg/controlplane/instance.go`, 10s).
pub const ENDPOINT_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

const SERVICE_KEY: &str = "/registry/services/default/kubernetes";

/// The `kubernetes` Service address under the default range. At runtime it is
/// `ServiceIpRanges::api_server_service_ip` (the first address of the primary
/// `--service-cluster-ip-range`, as upstream derives it).
#[cfg(test)]
const KUBERNETES_SERVICE_IP: &str = "10.96.0.1";

#[cfg(test)]
fn test_service_ip() -> std::net::IpAddr {
    KUBERNETES_SERVICE_IP.parse().unwrap()
}

const ENDPOINTS_KEY: &str = "/registry/endpoints/default/kubernetes";
const ENDPOINTSLICE_KEY: &str = "/registry/endpointslices/default/kubernetes";

/// Upstream `discovery.LabelSkipMirror`. Set on the `kubernetes` Endpoints so
/// the EndpointSlice mirroring controller leaves it alone — the apiserver owns
/// the slice directly (see `crates/controller-manager/.../endpointslice.rs`,
/// which honors this label). Matches `setSkipMirrorTrue`
/// (`pkg/controlplane/reconcilers/endpointsadapter.go`).
const SKIP_MIRROR_LABEL: &str = "endpointslice.kubernetes.io/skip-mirror";

/// Get the API server's IP address from the network interface.
/// This discovers the container's IP on the Docker/Podman network.
fn get_api_server_ip() -> Result<String> {
    // Try to get IP from network interfaces.
    // Look for non-loopback IPv4 addresses.
    let interfaces = match get_if_addrs::get_if_addrs() {
        Ok(addrs) => addrs,
        Err(e) => {
            warn!("Failed to get network interfaces: {}", e);
            return Err(anyhow::anyhow!("Failed to get network interfaces: {}", e));
        }
    };

    // Find the first non-loopback IPv4 address.
    for iface in interfaces {
        if !iface.is_loopback() {
            if let get_if_addrs::IfAddr::V4(addr) = iface.addr {
                let ip = addr.ip.to_string();
                info!(
                    "Discovered API server IP: {} (interface: {})",
                    ip, iface.name
                );
                return Ok(ip);
            }
        }
    }

    Err(anyhow::anyhow!("No non-loopback IPv4 address found"))
}

/// The single canonical `EndpointSubset` the `kubernetes` Endpoints must hold:
/// exactly the api-server's own `ip:port` over the `https`/TCP port. This is the
/// `masterCount == 1` shape of upstream's `masterCountEndpointReconciler`
/// (`pkg/controlplane/reconcilers/instancecount.go`) — we always force the
/// endpoint to our own address.
fn desired_subsets(ip: &str, port: u16) -> Vec<rusternetes_common::resources::EndpointSubset> {
    use rusternetes_common::resources::{EndpointAddress, EndpointPort, EndpointSubset};
    vec![EndpointSubset {
        addresses: Some(vec![EndpointAddress {
            ip: ip.to_string(),
            hostname: None,
            node_name: None,
            target_ref: None,
        }]),
        not_ready_addresses: None,
        ports: Some(vec![EndpointPort {
            name: Some("https".to_string()),
            port,
            protocol: "TCP".to_string(),
            app_protocol: None,
        }]),
    }]
}

/// Ensure the skip-mirror label is `"true"`. Returns whether the labels changed
/// (so the caller knows a write is needed).
fn ensure_skip_mirror(metadata: &mut rusternetes_common::types::ObjectMeta) -> bool {
    let labels = metadata.labels.get_or_insert_with(Default::default);
    if labels.get(SKIP_MIRROR_LABEL).map(String::as_str) == Some("true") {
        false
    } else {
        labels.insert(SKIP_MIRROR_LABEL.to_string(), "true".to_string());
        true
    }
}

/// Reconcile the `kubernetes` Endpoints to point at exactly `ip:port`. Idempotent:
/// only writes when the stored object diverges from the desired shape (so a
/// steady-state cluster does not churn resourceVersions every interval).
async fn reconcile_endpoints<S: Storage + ?Sized>(storage: &S, ip: &str, port: u16) -> Result<()> {
    let desired = desired_subsets(ip, port);

    match storage.get::<Endpoints>(ENDPOINTS_KEY).await {
        Ok(mut endpoints) => {
            let label_changed = ensure_skip_mirror(&mut endpoints.metadata);
            if endpoints.subsets != desired || label_changed {
                let old = endpoints
                    .subsets
                    .first()
                    .and_then(|s| s.addresses.as_ref())
                    .and_then(|a| a.first())
                    .map(|a| a.ip.clone())
                    .unwrap_or_default();
                endpoints.subsets = desired;
                storage
                    .update(ENDPOINTS_KEY, &endpoints)
                    .await
                    .context("Failed to update kubernetes Endpoints")?;
                info!("Reconciled kubernetes Endpoints: {} -> {}", old, ip);
            }
        }
        Err(_) => {
            use rusternetes_common::types::{ObjectMeta, TypeMeta};

            let mut metadata = ObjectMeta::new("kubernetes");
            metadata.namespace = Some("default".to_string());
            metadata.ensure_uid();
            metadata.ensure_creation_timestamp();
            ensure_skip_mirror(&mut metadata);

            let endpoints = Endpoints {
                type_meta: TypeMeta {
                    kind: "Endpoints".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata,
                subsets: desired,
            };

            storage
                .create(ENDPOINTS_KEY, &endpoints)
                .await
                .context("Failed to create kubernetes Endpoints")?;
            info!("Created kubernetes Endpoints with IP: {}", ip);
        }
    }
    Ok(())
}

/// Reconcile the `kubernetes` EndpointSlice to mirror the Endpoints. The
/// conformance test "should have Endpoints and EndpointSlices pointing to API
/// Server" (apiserver.go) expects BOTH to exist; the apiserver owns the slice
/// directly (the mirroring controller skips it via the skip-mirror label).
async fn reconcile_endpointslice<S: Storage + ?Sized>(
    storage: &S,
    ip: &str,
    port: u16,
) -> Result<()> {
    use rusternetes_common::resources::endpointslice::{
        Endpoint, EndpointConditions, EndpointPort,
    };

    let desired_endpoints = vec![Endpoint {
        addresses: vec![ip.to_string()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(true),
            terminating: Some(false),
        }),
        hostname: None,
        target_ref: None,
        node_name: None,
        zone: None,
        hints: None,
        deprecated_topology: None,
    }];
    let desired_ports = vec![EndpointPort {
        name: Some("https".to_string()),
        port: Some(port as i32),
        protocol: "TCP".to_string(),
        app_protocol: None,
    }];

    match storage.get::<EndpointSlice>(ENDPOINTSLICE_KEY).await {
        Ok(mut es) => {
            if es.endpoints != desired_endpoints || es.ports != desired_ports {
                es.endpoints = desired_endpoints;
                es.ports = desired_ports;
                storage
                    .update(ENDPOINTSLICE_KEY, &es)
                    .await
                    .context("Failed to update kubernetes EndpointSlice")?;
                info!("Reconciled kubernetes EndpointSlice to IP: {}", ip);
            }
        }
        Err(_) => {
            use rusternetes_common::types::{ObjectMeta, TypeMeta};

            let mut metadata = ObjectMeta::new("kubernetes");
            metadata.namespace = Some("default".to_string());
            let mut labels = std::collections::HashMap::new();
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                "kubernetes".to_string(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-mirroring-controller.k8s.io".to_string(),
            );
            metadata.labels = Some(labels);
            metadata.ensure_uid();
            metadata.ensure_creation_timestamp();

            let es = EndpointSlice {
                type_meta: TypeMeta {
                    kind: "EndpointSlice".to_string(),
                    api_version: "discovery.k8s.io/v1".to_string(),
                },
                metadata,
                address_type: "IPv4".to_string(),
                endpoints: desired_endpoints,
                ports: desired_ports,
            };

            storage
                .create(ENDPOINTSLICE_KEY, &es)
                .await
                .context("Failed to create kubernetes EndpointSlice")?;
            info!("Created kubernetes EndpointSlice with IP: {}", ip);
        }
    }
    Ok(())
}

/// Reconcile both the `kubernetes` Endpoints and EndpointSlice to `ip:port`.
/// Idempotent and safe to call repeatedly (the interval reconciler does).
/// Create the `default/kubernetes` Service if it is missing.
///
/// Ports upstream's kubernetesservice controller
/// (`pkg/controlplane/instance.go:349` -> `kubernetesservice.New(...)`), which
/// creates and repairs this Service on every reconcile tick. The api-server owns
/// it because in-cluster clients depend on it existing: the kubelet derives
/// `KUBERNETES_SERVICE_HOST` / `KUBERNETES_SERVICE_PORT` from it, so without it
/// every client-go `InClusterConfig()` fails with
/// "unable to load in-cluster configuration".
///
/// Previously only the Endpoints were reconciled here and the Service came from
/// `bootstrap-cluster.yaml` — fine for the compose stack, but any cluster that
/// does not run `scripts/bootstrap-cluster.sh` had no Service at all. That is what
/// aborted the vanilla-swap api-server leg's conformance suite in 1ms (#1667).
///
/// Idempotent: an existing Service is left untouched (its ClusterIP is immutable
/// and may have been allocated by whoever created it first).
pub async fn reconcile_kubernetes_service<S: Storage + ?Sized>(
    storage: &S,
    api_server_port: u16,
    service_ip: std::net::IpAddr,
) -> Result<()> {
    use rusternetes_common::resources::policy::IntOrString;
    use rusternetes_common::resources::{Service, ServicePort, ServiceSpec, ServiceType};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};

    if storage.get::<Service>(SERVICE_KEY).await.is_ok() {
        return Ok(());
    }

    // `cp.ServiceIPRange` (pkg/controlplane/apiserver/options/options.go:
    // 378-382): the first address of the primary range; the family follows.
    let service_ip_str = service_ip.to_string();
    let family = if service_ip.is_ipv6() {
        rusternetes_common::resources::service::IPFamily::IPv6
    } else {
        rusternetes_common::resources::service::IPFamily::IPv4
    };
    let mut metadata = ObjectMeta::new("kubernetes");
    metadata.namespace = Some("default".to_string());
    metadata.ensure_uid();
    metadata.ensure_creation_timestamp();
    // Upstream labels (`pkg/controlplane/controller/kubernetesservice`).
    let mut labels = std::collections::HashMap::new();
    labels.insert("component".to_string(), "apiserver".to_string());
    labels.insert("provider".to_string(), "kubernetes".to_string());
    metadata.labels = Some(labels);

    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: ServiceSpec {
            cluster_ip: Some(service_ip_str.clone()),
            ports: vec![ServicePort {
                name: Some("https".to_string()),
                port: 443,
                target_port: Some(IntOrString::Int(api_server_port as i32)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            // No selector: the api-server maintains the Endpoints itself.
            selector: None,
            // kube-proxy resolves a Service to its EndpointSlices BY IP FAMILY.
            // Without ipFamilies it matches no slice, decides the Service has
            // no endpoints, and installs a REJECT for the ClusterIP:
            //
            //   -A KUBE-SERVICES -d 10.96.0.1/32 -p tcp \
            //      --comment "default/kubernetes:https has no endpoints" -j REJECT
            //
            // so 10.96.0.1 is unroutable even with a present, ready
            // EndpointSlice. Observed live during the api-server swap:
            // kube-system/kube-dns had ipFamilies/ipFamilyPolicy/clusterIPs and
            // programmed 6 working DNAT rules; this Service had none of them
            // and got the REJECT above.
            //
            // Upstream sets ipFamilyPolicy and sessionAffinity explicitly when
            // it creates this Service
            // (pkg/controlplane/controller/kubernetesservice/controller.go:225-239)
            // and its registry allocator fills clusterIPs/ipFamilies on the way
            // in (pkg/registry/core/service/storage/alloc.go:239). We write
            // straight to storage, bypassing that allocator, so they have to be
            // set here or nothing sets them at all.
            cluster_ips: Some(vec![service_ip_str.clone()]),
            ip_families: Some(vec![family]),
            ip_family_policy: Some(
                rusternetes_common::resources::service::IPFamilyPolicy::SingleStack,
            ),
            session_affinity: Some("None".to_string()),
            ..Default::default()
        },
        status: None,
    };

    storage
        .create(SERVICE_KEY, &service)
        .await
        .context("Failed to create kubernetes Service")?;
    info!(
        "Created default/kubernetes Service ({} :443 -> :{})",
        service_ip_str, api_server_port
    );
    Ok(())
}

pub async fn reconcile_kubernetes_endpoint<S: Storage + ?Sized>(
    storage: &S,
    ip: &str,
    port: u16,
) -> Result<()> {
    reconcile_endpoints(storage, ip, port).await?;
    reconcile_endpointslice(storage, ip, port).await?;
    Ok(())
}

/// Bootstrap the `kubernetes` Service Endpoints + EndpointSlice in the default
/// namespace, pointing them at this api-server's discovered IP. Run once at
/// startup so the endpoint is correct immediately; [`spawn_endpoint_reconciler`]
/// then keeps it correct across restarts / IP changes.
pub async fn bootstrap_kubernetes_service(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
    service_ip: std::net::IpAddr,
) -> Result<()> {
    info!("Bootstrapping kubernetes Service and Endpoints");
    let api_server_ip = get_api_server_ip().context("Failed to discover API server IP address")?;
    info!(
        "API server IP: {}, Port: {}",
        api_server_ip, api_server_port
    );
    reconcile_kubernetes_service(storage.as_ref(), api_server_port, service_ip).await?;
    reconcile_kubernetes_endpoint(storage.as_ref(), &api_server_ip, api_server_port).await
}

/// Spawn the background reconciler that re-asserts the `kubernetes` endpoint to
/// the live api-server IP every [`ENDPOINT_RECONCILE_INTERVAL`]. Ports upstream's
/// `masterCountEndpointReconciler` run loop (`Controller.Run` ->
/// `wait.NonSlidingUntil(UpdateKubernetesService, EndpointInterval)`): the
/// endpoint self-heals after a container recreate (new bridge IP), a stale
/// write, or a clobber, without depending on the controller-manager.
///
/// Registered as the `bootstrap-controller` post-start hook
/// (pkg/controlplane/instance.go:360-363: `Start` the controller, `return nil`
/// -- never fatal).
pub fn spawn_endpoint_reconciler(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
    service_ip: std::net::IpAddr,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(BOOTSTRAP_CONTROLLER_HOOK, move || {
        spawn_endpoint_reconciler_loop(storage, api_server_port, service_ip);
    })
}

fn spawn_endpoint_reconciler_loop(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
    service_ip: std::net::IpAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ENDPOINT_RECONCILE_INTERVAL);
        // Skip the immediate first tick — startup bootstrap already ran.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let ip = match get_api_server_ip() {
                Ok(ip) => ip,
                Err(e) => {
                    warn!(
                        "endpoint reconciler: could not discover api-server IP: {}",
                        e
                    );
                    continue;
                }
            };
            if let Err(e) =
                reconcile_kubernetes_service(storage.as_ref(), api_server_port, service_ip).await
            {
                warn!("endpoint reconciler: service reconcile failed: {}", e);
            }
            if let Err(e) =
                reconcile_kubernetes_endpoint(storage.as_ref(), &ip, api_server_port).await
            {
                warn!("endpoint reconciler: reconcile failed: {}", e);
            }
        }
    })
}

/// Spawn the APIService availability controller **inside the api-server**.
///
/// Upstream runs the aggregator's availability controller as part of
/// kube-apiserver (`kube-aggregator/pkg/controllers/status`), NOT in
/// kube-controller-manager. In the vanilla-module-swap the controller-manager
/// is the stock KCM, which does not run this controller, so a remote
/// `APIService` would never get an `Available` condition at all — the create
/// path deliberately writes none (upstream `PrepareForCreate`) — and every
/// aggregation client (`e2e Aggregator`, `kubectl get apiservices`) would hang
/// waiting for one. Running it here matches upstream placement and works
/// regardless of which controller-manager is deployed.
///
/// Upstream registers the local and the remote availability controllers as
/// two post-start hooks (kube-aggregator/pkg/apiserver/apiserver.go:339-343,
/// :361-366), each `go ...Run(...)` then `return nil`. One controller covers
/// both here, so both `poststarthook/` checks are registered and finish
/// together.
pub fn spawn_apiservice_availability_controller(
    storage: Arc<StorageBackend>,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(APISERVICE_LOCAL_AVAILABLE_HOOK, || {});
    crate::post_start_hooks::spawn_starting_hook(APISERVICE_REMOTE_AVAILABLE_HOOK, move || {
        spawn_apiservice_availability_loop(storage);
    })
}

fn spawn_apiservice_availability_loop(storage: Arc<StorageBackend>) -> tokio::task::JoinHandle<()> {
    use rusternetes_controller_manager::controllers::apiservice::APIServiceAvailabilityController;
    tokio::spawn(async move {
        let controller = Arc::new(APIServiceAvailabilityController::new(storage));
        if let Err(e) = controller.run().await {
            warn!("APIService availability controller exited: {}", e);
        }
    })
}

// ---------------------------------------------------------------------------
// Default ServiceCIDR controller
// ---------------------------------------------------------------------------

/// Upstream `defaultservicecidr.DefaultServiceCIDRName`
/// (`pkg/controlplane/controller/defaultservicecidr/default_servicecidr_controller.go:47`).
pub const DEFAULT_SERVICE_CIDR_NAME: &str = "kubernetes";

/// Upstream `controllerName` (`default_servicecidr_controller.go:46`), used as
/// the event source component.
const DEFAULT_SERVICE_CIDR_CONTROLLER: &str = "kubernetes-service-cidr-controller";

/// The default `--service-cluster-ip-range`; the flag itself is
/// [`crate::registry::core::service::ipranges::ServiceIpRanges`]. The
/// `kubernetes` ServiceCIDR is seeded from the configured range, and
/// ClusterIPs are allocated from the ServiceCIDRs. Must stay in step with
/// the `kubernetes` Service address (the range's first address).
#[cfg(test)]
const DEFAULT_SERVICE_CIDRS: &[&str] = &["10.96.0.0/12"];

/// Upstream's controller interval (`default_servicecidr_controller.go:61`,
/// "same as DefaultEndpointReconcilerInterval", 10s).
pub const DEFAULT_SERVICE_CIDR_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

/// Upstream `default_servicecidr_controller.go:236`. Applied with **no reason**
/// — the controller builds the condition without one, and ServiceCIDR status is
/// not condition-validated (`ValidateServiceCIDRStatusUpdate`,
/// `pkg/apis/networking/validation/validation.go:883-886`).
pub const DEFAULT_SERVICE_CIDR_READY_MESSAGE: &str = "Kubernetes default Service CIDR is ready";

/// Port of upstream's `kubernetes-service-cidr-controller`
/// (`pkg/controlplane/controller/defaultservicecidr`), which owns the
/// `kubernetes` ServiceCIDR and lives in the **apiserver**, not the KCM — it is
/// the only component that knows this process's service range.
///
/// Replaces the create-if-absent seed that used to be duplicated inline in
/// `main.rs` and `lib.rs`. Unlike that seed this reconciles: it upgrades
/// single-stack to dual-stack when the configured ranges grow, warns (once) via
/// an Event when the persisted CIDRs disagree with this api-server's
/// configuration, and applies `Ready=True` only when they agree.
///
/// The *deletion* half of ServiceCIDR lifecycle — the protection finalizer, the
/// `Ready=False`/`Terminating` condition, `canDeleteCIDR` — belongs to the
/// separate `service-cidr-controller` in the controller-manager
/// (`crates/controller-manager/src/controllers/servicecidr.rs`), exactly as
/// upstream splits it. This controller deliberately never touches the status of
/// a ServiceCIDR that is being deleted (`syncStatus`, `:188-193`).
pub struct DefaultServiceCIDRController<S: Storage + ?Sized> {
    storage: Arc<S>,
    /// Order matters: the first CIDR defines the default IP family.
    cidrs: Vec<String>,
    recorder: rusternetes_storage::EventRecorder<S>,
    reported_mismatched_cidrs: bool,
    reported_not_ready_condition: bool,
}

impl<S: Storage + ?Sized> DefaultServiceCIDRController<S> {
    pub fn new(storage: Arc<S>, cidrs: Vec<String>) -> Self {
        Self {
            recorder: rusternetes_storage::EventRecorder::new(Arc::clone(&storage)),
            storage,
            cidrs,
            reported_mismatched_cidrs: false,
            reported_not_ready_condition: false,
        }
    }

    fn key() -> String {
        rusternetes_storage::build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME)
    }

    fn object_ref(
        sc: &rusternetes_common::resources::ServiceCIDR,
    ) -> rusternetes_common::resources::ObjectReference {
        rusternetes_common::resources::ObjectReference {
            kind: Some("ServiceCIDR".to_string()),
            namespace: None,
            name: Some(sc.metadata.name.clone()),
            uid: Some(sc.metadata.uid.clone()),
            api_version: Some("networking.k8s.io/v1".to_string()),
            resource_version: sc.metadata.resource_version.clone(),
            field_path: None,
        }
    }

    async fn warn_event(
        &self,
        sc: &rusternetes_common::resources::ServiceCIDR,
        reason: &str,
        message: &str,
    ) {
        let source = rusternetes_common::resources::EventSource {
            component: DEFAULT_SERVICE_CIDR_CONTROLLER.to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(
                &Self::object_ref(sc),
                &source,
                rusternetes_common::resources::EventType::Warning,
                reason,
                message,
            )
            .await
        {
            warn!(
                "default ServiceCIDR: could not record {} event: {}",
                reason, e
            );
        }
    }

    /// Upstream `sync` (`default_servicecidr_controller.go:142-185`).
    pub async fn sync(&mut self) -> Result<()> {
        use rusternetes_common::resources::{ServiceCIDR, ServiceCIDRSpec};
        use rusternetes_common::types::{ObjectMeta, TypeMeta};

        let key = Self::key();
        match self.storage.get::<ServiceCIDR>(&key).await {
            Ok(existing) => {
                let existing_cidrs = existing
                    .spec
                    .as_ref()
                    .map(|s| s.cidrs.clone())
                    .unwrap_or_default();
                // Single-stack -> dual-stack upgrade (`:148-156`).
                if self.cidrs.len() == 2
                    && existing_cidrs.len() == 1
                    && self.cidrs[0] == existing_cidrs[0]
                {
                    info!(
                        "Updating default ServiceCIDR from single-stack ({:?}) to dual-stack ({:?})",
                        existing_cidrs, self.cidrs
                    );
                    let mut updated = existing.clone();
                    updated.spec = Some(ServiceCIDRSpec {
                        cidrs: self.cidrs.clone(),
                    });
                    if let Err(e) = self.storage.update(&key, &updated).await {
                        warn!(
                            "The default ServiceCIDR can not be updated from {} to dual stack {:?}: {}",
                            self.cidrs[0], self.cidrs, e
                        );
                        self.warn_event(
                            &existing,
                            "KubernetesDefaultServiceCIDRError",
                            &format!(
                                "The default ServiceCIDR can not be upgraded from {} to dual stack {:?} : {}",
                                self.cidrs[0], self.cidrs, e
                            ),
                        )
                        .await;
                    }
                } else {
                    self.sync_status(&existing).await;
                }
                return Ok(());
            }
            Err(rusternetes_common::Error::NotFound(_)) => {}
            // Unknown error: retry on the next tick rather than racing a create
            // against a backend that is merely unreachable.
            Err(e) => return Err(e.into()),
        }

        // The default ServiceCIDR does not exist yet.
        info!("Creating default ServiceCIDR with CIDRs: {:?}", self.cidrs);
        let mut metadata = ObjectMeta::new(DEFAULT_SERVICE_CIDR_NAME);
        metadata.ensure_uid();
        metadata.ensure_creation_timestamp();
        let service_cidr = ServiceCIDR {
            type_meta: TypeMeta {
                kind: "ServiceCIDR".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata,
            spec: Some(ServiceCIDRSpec {
                cidrs: self.cidrs.clone(),
            }),
            // No status on create — upstream's registry strategy clears it
            // (`pkg/registry/networking/servicecidr/strategy.go:67-71`) and
            // `syncStatus` below is what applies `Ready`.
            status: None,
        };
        let created = match self.storage.create(&key, &service_cidr).await {
            Ok(created) => created,
            // Another api-server replica won the race; fall through to status.
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                self.storage.get::<ServiceCIDR>(&key).await?
            }
            Err(e) => {
                self.warn_event(
                    &service_cidr,
                    "KubernetesDefaultServiceCIDRError",
                    "The default ServiceCIDR can not be created",
                )
                .await;
                return Err(e.into());
            }
        };
        self.sync_status(&created).await;
        Ok(())
    }

    /// Upstream `syncStatus` (`default_servicecidr_controller.go:187-245`).
    async fn sync_status(&mut self, sc: &rusternetes_common::resources::ServiceCIDR) {
        use rusternetes_common::resources::{ServiceCIDRCondition, ServiceCIDRStatus};

        // A ServiceCIDR being deleted belongs to the controller-manager's
        // service-cidr-controller; never fight it over the condition.
        if sc.metadata.deletion_timestamp.is_some() {
            return;
        }

        let spec_cidrs = sc
            .spec
            .as_ref()
            .map(|s| s.cidrs.clone())
            .unwrap_or_default();
        let same_config = spec_cidrs == self.cidrs;
        let ready = sc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|c| c.iter().find(|c| c.condition_type == "Ready"))
            .cloned();

        if !same_config {
            if !self.reported_mismatched_cidrs {
                warn!(
                    "Inconsistent ServiceCIDR status for {}, controller configuration: {:?}, ServiceCIDR configuration: {:?}. Configure the flags to match current ServiceCIDR or manually delete it.",
                    sc.metadata.name, self.cidrs, spec_cidrs
                );
                self.warn_event(
                    sc,
                    "KubernetesDefaultServiceCIDRInconsistent",
                    &format!(
                        "The default ServiceCIDR {:?} does not match the controller flag configurations {:?}",
                        spec_cidrs, self.cidrs
                    ),
                )
                .await;
                self.reported_mismatched_cidrs = true;
            }
            // Inconsistent config is a problem regardless of the current Ready
            // condition; don't try to change it.
            return;
        }

        match ready {
            // Ready=False with matching config should not happen, and is not
            // ours to overwrite — the service-cidr-controller owns the
            // Terminating case. Report once and leave it for an operator.
            Some(c) if c.status == "False" => {
                if !self.reported_not_ready_condition {
                    warn!(
                        "Default ServiceCIDR {} condition Ready is False, but controller configuration matches. Please validate your cluster's network configuration. reason={} message={}",
                        sc.metadata.name, c.reason, c.message
                    );
                    let reason = if c.reason.is_empty() {
                        "KubernetesDefaultServiceCIDRError"
                    } else {
                        c.reason.as_str()
                    };
                    self.warn_event(
                        sc,
                        reason,
                        &format!("Configuration matches, but {}", c.message),
                    )
                    .await;
                    self.reported_not_ready_condition = true;
                }
            }
            // Already Ready=True and the config matches: nothing to do.
            Some(c) if c.status == "True" => {}
            // Missing or Unknown: this is ours to set.
            _ => {
                info!("Setting default ServiceCIDR condition Ready to True");
                let mut updated = sc.clone();
                updated.status = Some(ServiceCIDRStatus {
                    conditions: Some(vec![ServiceCIDRCondition {
                        condition_type: "Ready".to_string(),
                        status: "True".to_string(),
                        observed_generation: sc.metadata.generation,
                        last_transition_time: Some(
                            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        ),
                        reason: String::new(),
                        message: DEFAULT_SERVICE_CIDR_READY_MESSAGE.to_string(),
                    }]),
                });
                if let Err(e) = self.storage.update_status(&Self::key(), &updated).await {
                    warn!("error updating default ServiceCIDR status: {}", e);
                    self.warn_event(
                        sc,
                        "KubernetesDefaultServiceCIDRError",
                        "The default ServiceCIDR Status can not be set to Ready=True",
                    )
                    .await;
                }
            }
        }
    }
}

/// Run the default-ServiceCIDR controller: one synchronous sync so the
/// `kubernetes` ServiceCIDR exists before this api-server starts serving, then
/// a background reconcile every [`DEFAULT_SERVICE_CIDR_RECONCILE_INTERVAL`].
/// Mirrors upstream `Controller.Start` (`default_servicecidr_controller.go:101-140`),
/// which likewise blocks on a first successful sync before returning.
///
/// Registered as the `start-kubernetes-service-cidr-controller` post-start hook
/// (pkg/controlplane/instance.go:370-381): the hook returns `nil` once `Start`
/// has made its first sync attempt, so the `poststarthook/` check only passes
/// after the default ServiceCIDR was attempted. Deviation: upstream polls the
/// first sync until it succeeds; here a failed first sync is logged and the
/// background loop retries.
pub async fn start_default_servicecidr_controller(
    storage: Arc<StorageBackend>,
    cidrs: Vec<String>,
) -> tokio::task::JoinHandle<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let hook = crate::post_start_hooks::spawn_hook(SERVICE_CIDR_CONTROLLER_HOOK, async move {
        let handle = start_default_servicecidr_controller_inner(storage, cidrs).await;
        let _ = tx.send(handle);
        Ok::<(), std::convert::Infallible>(())
    });
    let _ = hook.await;
    rx.await.expect("service CIDR hook sends its handle")
}

async fn start_default_servicecidr_controller_inner(
    storage: Arc<StorageBackend>,
    cidrs: Vec<String>,
) -> tokio::task::JoinHandle<()> {
    let mut controller = DefaultServiceCIDRController::new(storage, cidrs);
    if let Err(e) = controller.sync().await {
        warn!("error initializing the default ServiceCIDR: {}", e);
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(DEFAULT_SERVICE_CIDR_RECONCILE_INTERVAL);
        ticker.tick().await; // startup sync already ran
        loop {
            ticker.tick().await;
            if let Err(e) = controller.sync().await {
                warn!("error trying to sync the default ServiceCIDR: {}", e);
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Service NodePort repair
// ---------------------------------------------------------------------------

/// How long startup waits for the first successful repair pass before it
/// gives up (storage_core.go:535-539).
const INITIAL_REPAIR_TIMEOUT: Duration = Duration::from_secs(60);

/// One NodePort repair pass over `state`'s allocator — what the
/// `start-service-ip-repair-controllers` post-start hook waits for before
/// the api-server reports ready. Until it has run, the allocation snapshot
/// may not exist and no NodePort can be allocated
/// (allocator/storage/storage.go:154-157).
// Used by the test harness (test_support), not by the binary.
#[allow(dead_code)]
pub async fn repair_service_node_ports_once(
    state: &crate::state::ApiServerState,
) -> rusternetes_common::Result<()> {
    use crate::registry::core::service::portallocator::repair::Repair;
    Repair::new(
        state.storage.clone(),
        state.node_port_allocator.port_range(),
        state.node_port_registry.clone(),
    )
    .run_once()
    .await
}

/// One ClusterIP repair pass (`RepairIPAddress.runOnce`), for the test
/// harness: it creates the IPAddresses of Services stored without one.
// Used by the test harness (test_support), not by the binary.
#[allow(dead_code)]
pub async fn repair_service_cluster_ips_once(
    state: &crate::state::ApiServerState,
) -> rusternetes_common::Result<()> {
    use crate::registry::core::service::ipallocator::repair::RepairIpAddress;
    RepairIpAddress::new(state.storage.clone()).run_once().await
}

/// Run `pass` now and every `REPAIR_INTERVAL` after, signalling the first
/// success (the `onFirstSuccess` callback of both `RunUntil`s).
fn spawn_repair_loop<F, Fut>(
    name: &'static str,
    mut pass: F,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = rusternetes_common::Result<()>> + Send,
{
    use crate::registry::core::service::portallocator::repair::REPAIR_INTERVAL;
    let (first_success, first) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut first_success = Some(first_success);
        loop {
            match pass().await {
                Ok(()) => {
                    if let Some(tx) = first_success.take() {
                        let _ = tx.send(());
                    }
                }
                Err(e) => warn!("{name}: {e}"),
            }
            tokio::time::sleep(REPAIR_INTERVAL).await;
        }
    });
    (handle, first)
}

/// The `start-service-ip-repair-controllers` post-start hook
/// (pkg/registry/core/rest/storage_core.go:504-540): run the ClusterIP
/// repair (`RepairIPAddress`, storage_core.go:142-148) and the NodePort
/// repair (`portallocator/controller.Repair`, :128) every
/// `REPAIR_INTERVAL`, and fail startup unless both first passes succeed
/// within one minute.
pub async fn start_service_ip_repair_controllers(
    state: &crate::state::ApiServerState,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    use crate::registry::core::service::ipallocator::repair::RepairIpAddress;
    use crate::registry::core::service::portallocator::repair::Repair;

    let ip_repair = Arc::new(RepairIpAddress::new(state.storage.clone()));
    let ip_repair_workers = ip_repair.clone();
    let (ip_handle, ip_first) = spawn_repair_loop("service ClusterIP repair", move || {
        let ip_repair = ip_repair.clone();
        async move {
            // Wait for the default ServiceCIDR (repairip.go:196-206).
            while !ip_repair.default_service_cidr_exists().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            ip_repair.run_once().await
        }
    });

    let port_repair = Arc::new(tokio::sync::Mutex::new(Repair::new(
        state.storage.clone(),
        state.node_port_allocator.port_range(),
        state.node_port_registry.clone(),
    )));
    let (port_handle, port_first) = spawn_repair_loop("service NodePort repair", move || {
        let port_repair = port_repair.clone();
        async move { port_repair.lock().await.run_once().await }
    });

    let both = async {
        let (a, b) = tokio::join!(ip_first, port_first);
        a.is_ok() && b.is_ok()
    };
    match tokio::time::timeout(INITIAL_REPAIR_TIMEOUT, both).await {
        Ok(true) => {
            // RunUntil starts the event-driven workers once the first
            // pass succeeded (repairip.go:208-216).
            let mut handles = vec![ip_handle, port_handle];
            handles.extend(ip_repair_workers.spawn_workers());
            Ok(handles)
        }
        _ => {
            ip_handle.abort();
            port_handle.abort();
            Err(anyhow::anyhow!(
                "unable to perform initial IP and Port allocation check"
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterAuthenticationTrust controller
// ---------------------------------------------------------------------------

/// Upstream's resync: `wait.PollImmediateUntil(1*time.Minute, ...)`
/// (cluster_authentication_trust_controller.go:470-476).
const CLUSTER_AUTHENTICATION_TRUST_RESYNC: Duration = Duration::from_secs(60);

/// The CA bundle this api-server publishes, resolved the way the Namespace
/// create handler resolved it before this controller existed: the
/// well-known filesystem paths, then the serving CA.
fn resolve_cluster_ca(ca_cert_pem: Option<&str>) -> Option<String> {
    [
        "/etc/kubernetes/pki/ca.crt",
        "/etc/kubernetes/pki/api-server.crt",
        "/root/.rusternetes/certs/ca.crt",
    ]
    .iter()
    .find_map(|p| std::fs::read_to_string(p).ok())
    .or_else(|| ca_cert_pem.map(str::to_string))
    .filter(|ca| !ca.is_empty())
}

/// This api-server's `ClusterAuthenticationInfo`
/// (pkg/controlplane/apiserver/config.go:71). Upstream fills it from
/// `--client-ca-file` and the `--requestheader-*` flags, which this
/// api-server does not take yet (#1577). Until it does, the effective
/// configuration is published: the cluster CA for both client and front
/// proxy, and the identity headers the aggregator proxy sends
/// (`handlers::aggregator::build_proxy_headers`).
pub fn cluster_authentication_info(
    ca_cert_pem: Option<&str>,
) -> rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
    let ca = resolve_cluster_ca(ca_cert_pem);
    rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
        client_ca: ca.clone(),
        request_header_username_headers: Some(vec!["X-Remote-User".to_string()]),
        request_header_uid_headers: None,
        request_header_group_headers: Some(vec!["X-Remote-Group".to_string()]),
        request_header_extra_header_prefixes: Some(vec!["X-Remote-Extra-".to_string()]),
        request_header_allowed_names: Some(Vec::new()),
        request_header_ca: ca,
    }
}

/// `systemnamespaces.Controller.sync` / `createNamespaceIfNeeded`
/// (`pkg/controlplane/controller/systemnamespaces/system_namespaces_controller.go:78-100`)
/// over [`SYSTEM_NAMESPACES`]; upstream creates through the API, this writes
/// the object the Namespace strategy would have stored (phase Active, the
/// `kubernetes` finalizer, the `kubernetes.io/metadata.name` label).
/// NamespaceLifecycle answers NotFound for a create in a missing namespace
/// (#2533), so these must exist before the apiserver serves. One pass;
/// idempotent.
pub async fn bootstrap_system_namespaces(storage: &StorageBackend) -> Result<()> {
    for ns in SYSTEM_NAMESPACES {
        create_namespace_if_needed(storage, ns).await?;
    }
    Ok(())
}

async fn create_namespace_if_needed(storage: &StorageBackend, ns: &str) -> Result<()> {
    use rusternetes_common::resources::Namespace;
    let key = rusternetes_storage::build_key("namespaces", None, ns);
    if storage.get::<Namespace>(&key).await.is_ok() {
        return Ok(());
    }
    let namespace = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": ns,
            "uid": uuid::Uuid::new_v4().to_string(),
            "creationTimestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "labels": {"kubernetes.io/metadata.name": ns}
        },
        "spec": {"finalizers": ["kubernetes"]},
        "status": {"phase": "Active"}
    });
    match storage.create(&key, &namespace).await {
        Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// `ServerRunOptions.SystemNamespaces`: `{kube-system, kube-public, default}`
/// (pkg/controlplane/apiserver/options/options.go:131) plus `kube-node-lease`
/// appended by cmd/kube-apiserver/app/options/options.go:94.
pub const SYSTEM_NAMESPACES: [&str; 4] =
    ["kube-system", "kube-public", "default", "kube-node-lease"];

/// Post-start hook name (pkg/controlplane/apiserver/server.go:146).
pub const SYSTEM_NAMESPACES_HOOK: &str = "start-system-namespaces-controller";

/// The controller's resync period (`interval := 1 * time.Minute`,
/// system_namespaces_controller.go NewController).
const SYSTEM_NAMESPACES_INTERVAL: Duration = Duration::from_secs(60);

/// `Controller.sync` (system_namespaces_controller.go): create each system
/// namespace that does not exist (`createNamespaceIfNeeded`: existing ->
/// no-op, AlreadyExists -> ok). Per-namespace failures are logged (upstream
/// `utilruntime.HandleError`) and returned, and do not stop the loop.
pub async fn sync_system_namespaces(storage: &StorageBackend, namespaces: &[&str]) -> Vec<String> {
    let mut errors = Vec::new();
    for ns in namespaces {
        if let Err(e) = create_namespace_if_needed(storage, ns).await {
            let msg = format!("unable to create required kubernetes system Namespace {ns}: {e}");
            warn!("{msg}");
            errors.push(msg);
        }
    }
    errors
}

/// `start-system-namespaces-controller` (server.go:145-150): the hook spawns
/// `Run` and returns nil at once, so it is never fatal. `Run` is
/// `wait.Until(c.sync, 1m, stopCh)`: sync now, then every interval. (No
/// informer-sync wait: there is no informer cache here -- `sync` reads
/// storage directly.)
pub fn spawn_system_namespaces_controller(storage: Arc<StorageBackend>) {
    crate::post_start_hooks::spawn_hook(SYSTEM_NAMESPACES_HOOK, async move {
        tokio::spawn(async move {
            loop {
                sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
                tokio::time::sleep(SYSTEM_NAMESPACES_INTERVAL).await;
            }
        });
        Ok::<(), anyhow::Error>(())
    });
}

/// `syncConfigMap` (cluster_authentication_trust_controller.go:140-179) with
/// `writeConfigMap` (:199-220): update, or create when absent.
pub async fn sync_cluster_authentication_trust(
    storage: &StorageBackend,
    required: &rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo,
) -> Result<()> {
    use rusternetes_common::clusterauthenticationtrust::{
        sync_config_map_data, CONFIG_MAP_NAME, CONFIG_MAP_NAMESPACE,
    };
    use rusternetes_common::resources::ConfigMap;

    let key =
        rusternetes_storage::build_key("configmaps", Some(CONFIG_MAP_NAMESPACE), CONFIG_MAP_NAME);
    let existing = match storage.get::<ConfigMap>(&key).await {
        Ok(cm) => Some(cm),
        Err(rusternetes_common::Error::NotFound(_)) => None,
        Err(e) => return Err(e.into()),
    };
    let existing_data = existing
        .as_ref()
        .map(|cm| cm.data.clone().unwrap_or_default());
    // `RemoteRequestHeaderUID` is beta and on by default (kube_features.go:2059-2062).
    let Some(data) = sync_config_map_data(
        existing_data.as_ref(),
        required,
        true,
        chrono::Utc::now().timestamp(),
    )
    .map_err(|e| anyhow::anyhow!("{CONFIG_MAP_NAMESPACE}/{CONFIG_MAP_NAME}: {e}"))?
    else {
        return Ok(());
    };
    info!("writing updated authentication info to {CONFIG_MAP_NAMESPACE} configmaps/{CONFIG_MAP_NAME}");
    create_namespace_if_needed(storage, CONFIG_MAP_NAMESPACE).await?;
    match existing {
        Some(mut cm) => {
            cm.data = Some(data);
            storage.update(&key, &cm).await?;
        }
        None => {
            let mut cm = ConfigMap {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "ConfigMap".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: rusternetes_common::types::ObjectMeta::new(CONFIG_MAP_NAME)
                    .with_namespace(CONFIG_MAP_NAMESPACE.to_string()),
                data: Some(data),
                binary_data: None,
                immutable: None,
            };
            cm.metadata.ensure_uid();
            cm.metadata.ensure_creation_timestamp();
            storage.create(&key, &cm).await?;
        }
    }
    Ok(())
}

/// The ClusterAuthenticationTrust controller, which kube-apiserver runs as
/// the `start-cluster-authentication-info-controller` post-start hook
/// (pkg/controlplane/apiserver/server.go:248-282): keep
/// `kube-system/extension-apiserver-authentication` holding how aggregated
/// api-servers should authenticate this one. It syncs at once, on every
/// add or delete of that ConfigMap (:117-135), and every minute (:470-476).
pub fn spawn_cluster_authentication_trust_controller(
    storage: Arc<StorageBackend>,
    required: rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(CLUSTER_AUTHENTICATION_INFO_HOOK, move || {
        spawn_cluster_authentication_trust_loop(storage, required);
    })
}

fn spawn_cluster_authentication_trust_loop(
    storage: Arc<StorageBackend>,
    required: rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo,
) -> tokio::task::JoinHandle<()> {
    use futures::StreamExt;
    use rusternetes_common::clusterauthenticationtrust::{CONFIG_MAP_NAME, CONFIG_MAP_NAMESPACE};
    use rusternetes_storage::WatchEvent;

    tokio::spawn(async move {
        let key = rusternetes_storage::build_key(
            "configmaps",
            Some(CONFIG_MAP_NAMESPACE),
            CONFIG_MAP_NAME,
        );
        let mut ticker = tokio::time::interval(CLUSTER_AUTHENTICATION_TRUST_RESYNC);
        let mut watch = None;
        loop {
            if watch.is_none() {
                watch = storage.watch(&key).await.ok();
            }
            let event = async {
                match watch.as_mut() {
                    Some(w) => w.next().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = ticker.tick() => {}
                ev = event => match ev {
                    Some(Ok(WatchEvent::Added(k, _) | WatchEvent::Deleted(k, _))) if k == key => {}
                    Some(Ok(_)) => continue,
                    // A broken watch is re-established on the next pass.
                    Some(Err(_)) | None => {
                        watch = None;
                        continue;
                    }
                },
            }
            if let Err(e) = sync_cluster_authentication_trust(storage.as_ref(), &required).await {
                warn!("cluster_authentication_trust_controller: {e}");
            }
        }
    })
}

/// The `kube-system` Role and RoleBinding of upstream's bootstrap namespace
/// policy that concern `extension-apiserver-authentication`
/// (plugin/pkg/auth/authorizer/rbac/bootstrappolicy/namespace_policy.go:75-82,
/// 124-126): read access to the ConfigMap, granted to the controller-manager
/// and the scheduler. `EnsureRBACPolicy` (pkg/registry/rbac/rest/
/// storage_rbac.go:269-331) creates each, creating the namespace first
/// (`tryEnsureNamespace`, component-helpers/auth/rbac/reconciliation/
/// namespace.go:31-45). Only the create half is ported, and only these two
/// objects. The rest of the bootstrap policy is `bootstrap_default_rbac`.
pub async fn bootstrap_extension_apiserver_authentication_rbac(
    storage: Arc<StorageBackend>,
) -> Result<()> {
    use rusternetes_common::clusterauthenticationtrust::{CONFIG_MAP_NAME, CONFIG_MAP_NAMESPACE};

    const ROLE: &str = "extension-apiserver-authentication-reader";
    const BINDING: &str = "system::extension-apiserver-authentication-reader";
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let objects = [
        (
            rusternetes_storage::build_key("roles", Some(CONFIG_MAP_NAMESPACE), ROLE),
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {
                    "name": ROLE,
                    "namespace": CONFIG_MAP_NAMESPACE,
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": now,
                    "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
                    "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"}
                },
                "rules": [{
                    "apiGroups": [""],
                    "resources": ["configmaps"],
                    "resourceNames": [CONFIG_MAP_NAME],
                    "verbs": ["get", "list", "watch"]
                }]
            }),
        ),
        (
            rusternetes_storage::build_key("rolebindings", Some(CONFIG_MAP_NAMESPACE), BINDING),
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": BINDING,
                    "namespace": CONFIG_MAP_NAMESPACE,
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": now,
                    "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
                    "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"}
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": ROLE
                },
                "subjects": [
                    {"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "system:kube-controller-manager"},
                    {"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "system:kube-scheduler"}
                ]
            }),
        ),
    ];
    create_namespace_if_needed(storage.as_ref(), CONFIG_MAP_NAMESPACE).await?;
    for (key, obj) in objects {
        if storage.get::<serde_json::Value>(&key).await.is_ok() {
            continue;
        }
        match storage.create(&key, &obj).await {
            Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// `SystemPriorityClasses()` (pkg/apis/scheduling/v1/helpers.go:29-54): the
/// classes the api-server seeds. `SystemCriticalPriority` is 2 * 10^9
/// (pkg/apis/scheduling/types.go); node-critical adds 1000.
fn system_priority_classes() -> [rusternetes_common::resources::PriorityClass; 2] {
    use rusternetes_common::resources::PriorityClass;
    let mut node = PriorityClass::new("system-node-critical", 2_000_001_000);
    node.description = Some(
        "Used for system critical pods that must not be moved from their current node.".into(),
    );
    let mut cluster = PriorityClass::new("system-cluster-critical", 2_000_000_000);
    cluster.description = Some(
        "Used for system critical pods that must run in the cluster, but can be moved to another node if necessary."
            .into(),
    );
    [node, cluster]
}

/// One pass of `AddSystemPriorityClasses` (pkg/registry/scheduling/rest/
/// storage_scheduling.go:100-139): create each system PriorityClass that is
/// missing; an existing one is left alone. The created object carries what
/// the PriorityClass strategy gives a create (`generation` 1,
/// `PrepareForCreate` strategy.go:46-50; `preemptionPolicy` default,
/// `SetDefaults_PriorityClass`).
pub async fn bootstrap_system_priority_classes<S: Storage + ?Sized>(storage: &S) -> Result<()> {
    for mut pc in system_priority_classes() {
        let key = rusternetes_storage::build_key("priorityclasses", None, &pc.metadata.name);
        match storage
            .get::<rusternetes_common::resources::PriorityClass>(&key)
            .await
        {
            Ok(_) => continue,
            Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        pc.metadata.generation = Some(1);
        pc.preemption_policy = Some("PreemptLowerPriority".to_string());
        match storage.create(&key, &pc).await {
            Ok(_) => info!(
                "created PriorityClass {} with value {}",
                pc.metadata.name, pc.value
            ),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// `instance.go:360`.
pub const BOOTSTRAP_CONTROLLER_HOOK: &str = "bootstrap-controller";
/// `pkg/controlplane/instance.go:370`.
pub const SERVICE_CIDR_CONTROLLER_HOOK: &str = "start-kubernetes-service-cidr-controller";
/// `kube-aggregator/pkg/apiserver/apiserver.go:339` and `:361`. One controller
/// here covers both the local and the remote availability checks.
pub const APISERVICE_LOCAL_AVAILABLE_HOOK: &str = "apiservice-status-local-available-controller";
pub const APISERVICE_REMOTE_AVAILABLE_HOOK: &str = "apiservice-status-remote-available-controller";
/// `pkg/controlplane/apiserver/server.go:249`.
pub const CLUSTER_AUTHENTICATION_INFO_HOOK: &str = "start-cluster-authentication-info-controller";
/// `pkg/registry/rbac/rest/storage_rbac.go:59` (`PostStartHookName`).
pub const RBAC_BOOTSTRAP_ROLES_HOOK: &str = "rbac/bootstrap-roles";
/// `apiextensions-apiserver/pkg/apiserver/apiserver.go:228`.
pub const APIEXTENSIONS_CONTROLLERS_HOOK: &str = "start-apiextensions-controllers";

/// `PostStartHookName` (storage_scheduling.go:41).
pub const SYSTEM_PRIORITY_CLASSES_HOOK: &str = "scheduling/bootstrap-system-priority-classes";

/// `wait.Poll(interval, timeout, ...)` (storage_scheduling.go:104): retry
/// `attempt` every `interval` until it succeeds or `timeout` elapses; the last
/// error is wrapped as upstream does (:140-143,
/// "unable to add default system priority classes: %v").
pub async fn poll_until<F, Fut>(timeout: Duration, interval: Duration, attempt: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    poll_until_msg(
        "unable to add default system priority classes",
        timeout,
        interval,
        attempt,
    )
    .await
}

/// [`poll_until`] with the caller's upstream error prefix
/// (`wait.Poll(1s, 30s, ...)` then `fmt.Errorf("<prefix>: %v", err)`).
pub async fn poll_until_msg<F, Fut>(
    prefix: &str,
    timeout: Duration,
    interval: Duration,
    mut attempt: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match attempt().await {
            Ok(()) => return Ok(()),
            Err(e) if tokio::time::Instant::now() >= deadline => {
                return Err(anyhow::anyhow!("{prefix}: {e}"))
            }
            Err(e) => warn!("unable to create system priority classes: {e}. Retrying..."),
        }
        tokio::time::sleep(interval).await;
    }
}

/// The `scheduling/bootstrap-system-priority-classes` PostStartHook
/// (storage_scheduling.go:96-145): poll 1s/30s until
/// [`bootstrap_system_priority_classes`] succeeds. A failure is returned
/// (:140-143) and the shared runner ([`crate::post_start_hooks::spawn_hook`])
/// treats it as fatal (hooks.go:204 `klog.Fatalf`); until it finishes the
/// `poststarthook/<name>` check fails (hooks.go:239-246).
pub fn spawn_system_priority_classes_hook<S: Storage + 'static>(storage: Arc<S>) {
    crate::post_start_hooks::spawn_hook(SYSTEM_PRIORITY_CLASSES_HOOK, async move {
        poll_until(Duration::from_secs(30), Duration::from_secs(1), || {
            let storage = storage.clone();
            async move { bootstrap_system_priority_classes(storage.as_ref()).await }
        })
        .await?;
        info!("all system priority classes are created successfully or already exist.");
        Ok::<(), anyhow::Error>(())
    });
}

/// The `rbac/bootstrap-roles` PostStartHook (storage_rbac.go:131-140,
/// `EnsureRBACPolicy` :162-179): poll 1s/30s until the bootstrap policy is
/// reconciled, then fail with "unable to initialize roles: %v", which the
/// shared runner makes fatal (hooks.go:204) -- "if we're never able to make it
/// through initialization, kill the API server". Until it finishes,
/// `poststarthook/rbac/bootstrap-roles` fails.
///
/// Deviation: upstream's hook runs after the server starts; callers here await
/// the returned handle before serving so a fresh store is never reachable
/// with an empty RBAC policy (#1659: no `system:masters` authorizer shortcut).
pub fn spawn_rbac_bootstrap_roles_hook(
    storage: Arc<StorageBackend>,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_hook(RBAC_BOOTSTRAP_ROLES_HOOK, async move {
        poll_until_msg(
            "unable to initialize roles",
            Duration::from_secs(30),
            Duration::from_secs(1),
            || {
                let storage = storage.clone();
                async move { bootstrap_default_rbac(storage).await }
            },
        )
        .await
    })
}

#[cfg(test)]
mod system_priority_class_tests {
    use super::*;
    use rusternetes_common::resources::PriorityClass;
    use rusternetes_storage::memory::MemoryStorage;

    /// `AddSystemPriorityClasses` seeds `SystemPriorityClasses()`.
    #[tokio::test]
    async fn seeds_the_two_system_priority_classes() {
        let storage = MemoryStorage::new();
        bootstrap_system_priority_classes(&storage).await.unwrap();
        let node: PriorityClass = storage
            .get("/registry/priorityclasses/system-node-critical")
            .await
            .unwrap();
        assert_eq!(node.value, 2_000_001_000);
        assert_eq!(node.global_default, None);
        assert_eq!(node.metadata.generation, Some(1));
        assert_eq!(
            node.preemption_policy.as_deref(),
            Some("PreemptLowerPriority")
        );
        let cluster: PriorityClass = storage
            .get("/registry/priorityclasses/system-cluster-critical")
            .await
            .unwrap();
        assert_eq!(cluster.value, 2_000_000_000);
    }

    /// Upstream returns the error once the 30s poll expires
    /// (storage_scheduling.go:140-143) instead of logging and finishing.
    #[tokio::test(start_paused = true)]
    async fn poll_returns_error_at_deadline() {
        let mut calls = 0;
        let r = poll_until(Duration::from_secs(30), Duration::from_secs(1), || {
            calls += 1;
            async { Err::<(), _>(anyhow::anyhow!("storage down")) }
        })
        .await;
        let msg = r.unwrap_err().to_string();
        assert!(
            msg.contains("unable to add default system priority classes: storage down"),
            "{msg}"
        );
        assert!(calls > 1);
    }

    #[tokio::test(start_paused = true)]
    async fn poll_succeeds_after_retries() {
        let mut calls = 0;
        let r = poll_until(Duration::from_secs(30), Duration::from_secs(1), || {
            calls += 1;
            let ok = calls >= 3;
            async move {
                if ok {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("not yet"))
                }
            }
        })
        .await;
        assert!(r.is_ok());
    }

    /// The hook only creates what is missing: an existing class is left alone.
    #[tokio::test]
    async fn leaves_an_existing_class_alone() {
        let storage = MemoryStorage::new();
        let mut pc = PriorityClass::new("system-node-critical", 2_000_001_000);
        pc.description = Some("custom".into());
        storage
            .create("/registry/priorityclasses/system-node-critical", &pc)
            .await
            .unwrap();
        bootstrap_system_priority_classes(&storage).await.unwrap();
        let got: PriorityClass = storage
            .get("/registry/priorityclasses/system-node-critical")
            .await
            .unwrap();
        assert_eq!(got.description.as_deref(), Some("custom"));
        assert!(storage
            .get::<PriorityClass>("/registry/priorityclasses/system-cluster-critical")
            .await
            .is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::rbac::{ClusterRole, ClusterRoleBinding};
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn creates_endpoints_and_slice_when_missing() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "10.89.0.5", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(ep.subsets[0].addresses.as_ref().unwrap()[0].ip, "10.89.0.5");
        assert_eq!(ep.subsets[0].ports.as_ref().unwrap()[0].port, 6443);
        // Mirroring is suppressed; the apiserver owns the slice.
        assert_eq!(
            ep.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(SKIP_MIRROR_LABEL)
                .map(String::as_str),
            Some("true")
        );

        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.endpoints[0].addresses, vec!["10.89.0.5".to_string()]);
        assert_eq!(es.ports[0].port, Some(6443));
    }

    /// #2533: the systemnamespaces controller's pass creates default,
    /// kube-system and kube-public, and is idempotent.
    #[tokio::test]
    async fn seeds_system_namespaces() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_system_namespaces(storage.as_ref()).await.unwrap();
        bootstrap_system_namespaces(storage.as_ref()).await.unwrap();
        for ns in ["default", "kube-system", "kube-public"] {
            let key = rusternetes_storage::build_key("namespaces", None, ns);
            let got: serde_json::Value = storage.get(&key).await.unwrap();
            assert_eq!(got["status"]["phase"], "Active", "{ns}");
        }
    }

    /// #1659: bootstrap seeds the cluster-admin ClusterRole + a binding to the
    /// system:masters group on an empty store, and is idempotent. This is what
    /// authorizes kubeadm's `O=system:masters` admin cert before any RBAC is
    /// applied (via a real rule, so the escalation check stays rule-based).
    #[tokio::test]
    async fn seeds_cluster_admin_for_system_masters() {
        let storage = Arc::new(StorageBackend::new_memory());

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        // Idempotent: a second run must not error or duplicate.
        bootstrap_default_rbac(storage.clone()).await.unwrap();

        let role: ClusterRole = storage.get(CLUSTER_ADMIN_ROLE_KEY).await.unwrap();
        assert!(
            role.rules.iter().any(|r| r.verbs.contains(&"*".to_string())
                && r.resources
                    .as_ref()
                    .is_some_and(|x| x.contains(&"*".to_string()))),
            "cluster-admin must grant wildcard resource access"
        );

        let binding: ClusterRoleBinding = storage.get(CLUSTER_ADMIN_BINDING_KEY).await.unwrap();
        assert_eq!(binding.role_ref.name, "cluster-admin");
        assert!(
            binding
                .subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:masters"),
            "binding must target the system:masters group"
        );
    }

    /// `computeReconciledRole` (reconcile_role.go:195-232): a bootstrap rule that
    /// an existing rule already `Covers` (here a wildcard) is not appended;
    /// only the uncovered atomic rules are, and labels are merged with the
    /// existing value winning.
    #[tokio::test]
    async fn reconcile_uses_covers_not_verbatim_match() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let key = "/registry/clusterroles/system:basic-user";
        let mut role: serde_json::Value = storage.get(key).await.unwrap();
        // Wildcard covers every bootstrap rule of the role.
        role["rules"] = serde_json::json!([{"apiGroups":["*"],"resources":["*"],"verbs":["*"]}]);
        role["metadata"]["labels"]["extra"] = "kept".into();
        role["metadata"]["labels"]["kubernetes.io/bootstrapping"] = "mine".into();
        storage.update(key, &role).await.unwrap();

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert_eq!(after["rules"].as_array().unwrap().len(), 1, "{after}");
        assert_eq!(after["metadata"]["labels"]["extra"], "kept");
        assert_eq!(
            after["metadata"]["labels"]["kubernetes.io/bootstrapping"],
            "mine"
        );

        // A rule that is NOT covered is appended as atomic rules.
        let mut role = after.clone();
        role["rules"] = serde_json::json!([]);
        storage.update(key, &role).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert!(!after["rules"].as_array().unwrap().is_empty());
    }

    /// `computeReconciledRoleBinding` (reconcile_rolebindings.go:152-160): a
    /// changed roleRef is immutable, so the binding is deleted and recreated
    /// (`ReconcileRecreate`), dropping the stale subjects.
    #[tokio::test]
    async fn reconcile_recreates_binding_when_role_ref_differs() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let key = CLUSTER_ADMIN_BINDING_KEY;
        let mut b: serde_json::Value = storage.get(key).await.unwrap();
        b["roleRef"]["name"] = "view".into();
        b["subjects"] =
            serde_json::json!([{"apiGroup":"rbac.authorization.k8s.io","kind":"User","name":"x"}]);
        storage.update(key, &b).await.unwrap();

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert_eq!(after["roleRef"]["name"], "cluster-admin");
        assert_eq!(after["subjects"].as_array().unwrap().len(), 1);
        assert_eq!(after["subjects"][0]["name"], "system:masters");
    }

    /// A protected (`autoupdate: "false"`) object is left alone, even with a
    /// differing roleRef (reconcile_role.go:121-123 returns before the switch).
    #[tokio::test]
    async fn reconcile_leaves_protected_binding_alone() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let key = CLUSTER_ADMIN_BINDING_KEY;
        let mut b: serde_json::Value = storage.get(key).await.unwrap();
        b["roleRef"]["name"] = "view".into();
        b["metadata"]["annotations"][AUTOUPDATE_ANNOTATION] = "false".into();
        storage.update(key, &b).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        let after: serde_json::Value = storage.get(key).await.unwrap();
        assert_eq!(after["roleRef"]["name"], "view");
    }

    /// `ClusterRoles()` only: not the `system:controller:*` roles (`ControllerRoles()`).
    fn is_cluster_roles_item(i: &serde_json::Value) -> bool {
        i["kind"] == "ClusterRole"
            && !i["metadata"]["name"]
                .as_str()
                .unwrap()
                .starts_with("system:controller:")
    }

    /// Upstream `TestBootstrapClusterRolesWithFeatureGatesEnabled`
    /// (`bootstrappolicy/policy_test.go:180-200`): with the gates on, the
    /// ClusterRoles equal `testdata/cluster-roles-featuregates.yaml`. Upstream
    /// flips `AllAlpha`/`AllBeta`; we flip each gate that changes a ClusterRole.
    #[test]
    #[serial_test::serial]
    fn cluster_roles_with_feature_gates_enabled_match_upstream_testdata() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _g: Vec<_> = [
            Feature::ClusterTrustBundle,
            Feature::PodCertificateRequest,
            Feature::DRAExtendedResource,
            Feature::DRADeviceTaintRules,
            Feature::GenericWorkload,
            Feature::ComponentFlagz,
            Feature::ComponentStatusz,
        ]
        .into_iter()
        .map(|f| with_feature(f, true))
        .collect();
        let mut got: Vec<_> = bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(is_cluster_roles_item)
            .collect();
        got.sort_by_key(|i| i["metadata"]["name"].as_str().unwrap().to_string());
        let want = load_policy_items(include_str!(
            "bootstrap_policy/cluster-roles-featuregates.yaml"
        ))
        .unwrap();
        assert_eq!(got.len(), want.len(), "role set differs");
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(g, w, "ClusterRole {} differs", w["metadata"]["name"]);
        }
    }

    /// With the gates at their v1.35 defaults the policy is exactly the
    /// vendored `cluster-roles.yaml` (`TestBootstrapClusterRoles`).
    #[test]
    #[serial_test::serial]
    fn cluster_roles_with_default_gates_match_upstream_testdata() {
        rusternetes_common::feature_gates::reset_to_defaults();
        let mut got: Vec<_> = bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(is_cluster_roles_item)
            .collect();
        got.sort_by_key(|i| i["metadata"]["name"].as_str().unwrap().to_string());
        let want = load_policy_items(include_str!("bootstrap_policy/cluster-roles.yaml")).unwrap();
        assert_eq!(got, want);
    }

    /// `policy.go:713-715`: the trust-bundle discovery binding exists only
    /// with `ClusterTrustBundle` on.
    #[test]
    #[serial_test::serial]
    fn cluster_trust_bundle_discovery_binding_follows_the_gate() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let has = || {
            bootstrap_policy_items().unwrap().iter().any(|i| {
                i["kind"] == "ClusterRoleBinding"
                    && i["metadata"]["name"] == "system:cluster-trust-bundle-discovery"
            })
        };
        assert!(!has());
        let _g = with_feature(Feature::ClusterTrustBundle, true);
        assert!(has());
    }

    fn find_item(kind: &str, name: &str) -> Option<serde_json::Value> {
        bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .find(|i| i["kind"] == kind && i["metadata"]["name"] == name)
    }

    fn events() -> serde_json::Value {
        // `eventsRule()` (controller_policy.go:55-57)
        serde_json::json!({"apiGroups": ["", "events.k8s.io"], "resources": ["events"],
            "verbs": ["create", "patch", "update"]})
    }

    /// Asserts the role exists with exactly `rules` and that its
    /// `addControllerRole` binding (`controller_policy.go:36-52`) exists too.
    fn assert_controller_role(short: &str, rules: Vec<serde_json::Value>) {
        let name = format!("system:controller:{short}");
        let role =
            find_item("ClusterRole", &name).unwrap_or_else(|| panic!("no ClusterRole {name}"));
        assert_eq!(role["rules"], serde_json::Value::Array(rules), "{name}");
        assert_eq!(
            role["metadata"]["labels"]["kubernetes.io/bootstrapping"],
            "rbac-defaults"
        );
        let b = find_item("ClusterRoleBinding", &name)
            .unwrap_or_else(|| panic!("no ClusterRoleBinding {name}"));
        assert_eq!(b["roleRef"]["name"], name.as_str());
        assert_eq!(b["subjects"][0]["kind"], "ServiceAccount");
        assert_eq!(b["subjects"][0]["name"], short);
        assert_eq!(b["subjects"][0]["namespace"], "kube-system");
    }

    fn assert_no_controller_role(short: &str) {
        let name = format!("system:controller:{short}");
        assert!(find_item("ClusterRole", &name).is_none(), "{name} role");
        assert!(
            find_item("ClusterRoleBinding", &name).is_none(),
            "{name} binding"
        );
    }

    /// `buildControllerRoles` (`controller_policy.go:206-231`): the DRA
    /// device-taint-eviction role needs `DRADeviceTaints`; `DRADeviceTaintRules`
    /// appends the DeviceTaintRule rules.
    #[test]
    #[serial_test::serial]
    fn device_taint_eviction_controller_role_follows_dra_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        const R: &str = "device-taint-eviction-controller";
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role(R);
        let _a = with_feature(Feature::DRADeviceTaints, true);
        let mut rules = vec![
            serde_json::json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["delete", "get", "list", "watch"]}),
            serde_json::json!({"apiGroups": [""], "resources": ["pods/status"], "verbs": ["patch", "update"]}),
            serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["resourceclaims"], "verbs": ["get", "list", "watch"]}),
            serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["resourceslices"], "verbs": ["get", "list", "watch"]}),
            serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["deviceclasses"], "verbs": ["get", "list", "watch"]}),
            events(),
        ];
        assert_controller_role(R, rules.clone());
        let _b = with_feature(Feature::DRADeviceTaintRules, true);
        rules.push(serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["devicetaintrules/status"], "verbs": ["patch", "update"]}));
        rules.push(serde_json::json!({"apiGroups": ["resource.k8s.io"], "resources": ["devicetaintrules"], "verbs": ["get", "list", "watch"]}));
        assert_controller_role(R, rules);
    }

    /// `controller_policy.go:440-446` (`PodCertificateRequest`) and `:490-498`
    /// (`ClusterTrustBundle`).
    #[test]
    #[serial_test::serial]
    fn certificate_controller_roles_follow_their_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role("podcertificaterequestcleaner");
        assert_no_controller_role("kube-apiserver-serving-clustertrustbundle-publisher");
        let _a = with_feature(Feature::PodCertificateRequest, true);
        assert_controller_role(
            "podcertificaterequestcleaner",
            vec![
                serde_json::json!({"apiGroups": ["certificates.k8s.io"], "resources": ["podcertificaterequests"], "verbs": ["delete", "get", "list", "watch"]}),
            ],
        );
        assert_no_controller_role("kube-apiserver-serving-clustertrustbundle-publisher");
        let _b = with_feature(Feature::ClusterTrustBundle, true);
        assert_controller_role(
            "kube-apiserver-serving-clustertrustbundle-publisher",
            vec![
                serde_json::json!({"apiGroups": ["certificates.k8s.io"], "resourceNames": ["kubernetes.io/kube-apiserver-serving"], "resources": ["signers"], "verbs": ["attest"]}),
                serde_json::json!({"apiGroups": ["certificates.k8s.io"], "resources": ["clustertrustbundles"], "verbs": ["create", "delete", "list", "update", "watch"]}),
                events(),
            ],
        );
    }

    /// `controller_policy.go:512-523`: needs `StorageVersionAPI` AND
    /// `APIServerIdentity` (on by default).
    #[test]
    #[serial_test::serial]
    fn storage_version_gc_role_needs_both_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        const R: &str = "storage-version-garbage-collector";
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role(R);
        {
            let _a = with_feature(Feature::StorageVersionAPI, true);
            let _b = with_feature(Feature::APIServerIdentity, false);
            assert_no_controller_role(R);
        }
        let _a = with_feature(Feature::StorageVersionAPI, true);
        assert_controller_role(
            R,
            vec![
                serde_json::json!({"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get", "list", "watch"]}),
                serde_json::json!({"apiGroups": ["internal.apiserver.k8s.io"], "resources": ["storageversions"], "verbs": ["delete", "get", "list", "patch", "update", "watch"]}),
                serde_json::json!({"apiGroups": ["internal.apiserver.k8s.io"], "resources": ["storageversions/status"], "verbs": ["get", "patch", "update"]}),
            ],
        );
    }

    /// `controller_policy.go:534-546`.
    #[test]
    #[serial_test::serial]
    fn storage_version_migrator_role_follows_the_gate() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        const R: &str = "storage-version-migrator-controller";
        rusternetes_common::feature_gates::reset_to_defaults();
        assert_no_controller_role(R);
        let _a = with_feature(Feature::StorageVersionMigrator, true);
        assert_controller_role(
            R,
            vec![
                serde_json::json!({"apiGroups": ["*"], "resources": ["*"], "verbs": ["create", "list", "patch"]}),
                serde_json::json!({"apiGroups": ["storagemigration.k8s.io"], "resources": ["storageversionmigrations/status"], "verbs": ["update"]}),
            ],
        );
    }

    /// Gates that are on in v1.35 (`DynamicResourceAllocation`,
    /// `MultiCIDRServiceAllocator`, `VolumeAttributesClass`, `SELinuxChangePolicy`)
    /// are already in the vendored `controller-roles.yaml`; switching one off
    /// removes its role (`controller_policy.go:206,386,463,549`).
    #[test]
    #[serial_test::serial]
    fn default_on_controller_roles_are_present() {
        rusternetes_common::feature_gates::reset_to_defaults();
        for r in [
            "resource-claim-controller",
            "service-cidrs-controller",
            "volumeattributesclass-protection-controller",
            "selinux-warning-controller",
        ] {
            assert!(
                find_item("ClusterRole", &format!("system:controller:{r}")).is_some(),
                "{r}"
            );
        }
    }

    /// Default-gate output equals the vendored `controller-roles.yaml` /
    /// `controller-role-bindings.yaml` (`TestBootstrapControllerRoles`,
    /// `policy_test.go:212-248`).
    #[test]
    #[serial_test::serial]
    fn controller_roles_with_default_gates_match_upstream_testdata() {
        rusternetes_common::feature_gates::reset_to_defaults();
        for (kind, yaml) in [
            (
                "ClusterRole",
                include_str!("bootstrap_policy/controller-roles.yaml"),
            ),
            (
                "ClusterRoleBinding",
                include_str!("bootstrap_policy/controller-role-bindings.yaml"),
            ),
        ] {
            let mut got: Vec<_> = bootstrap_policy_items()
                .unwrap()
                .into_iter()
                .filter(|i| {
                    i["kind"] == kind
                        && i["metadata"]["name"]
                            .as_str()
                            .unwrap()
                            .starts_with("system:controller:")
                })
                .collect();
            got.sort_by_key(|i| i["metadata"]["name"].as_str().unwrap().to_string());
            assert_eq!(got, load_policy_items(yaml).unwrap(), "{kind}");
        }
    }

    /// #1753: the whole upstream bootstrap policy is seeded, not only
    /// cluster-admin. Names are those of upstream's
    /// `bootstrappolicy/testdata/{cluster-roles,cluster-role-bindings,
    /// namespace-roles}.yaml` (release-1.35).
    #[tokio::test]
    async fn seeds_upstream_bootstrap_policy() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();

        let role: ClusterRole = storage
            .get("/registry/clusterroles/system:service-account-issuer-discovery")
            .await
            .expect("issuer-discovery ClusterRole (policy.go:555-566)");
        assert_eq!(role.rules.len(), 1);
        let urls = role.rules[0].non_resource_urls.clone().unwrap();
        assert_eq!(
            urls,
            [
                "/.well-known/openid-configuration",
                "/.well-known/openid-configuration/",
                "/openid/v1/jwks",
                "/openid/v1/jwks/"
            ]
        );
        assert_eq!(role.rules[0].verbs, ["get"]);
        let labels = role.metadata.labels.clone().unwrap();
        assert_eq!(
            labels
                .get("kubernetes.io/bootstrapping")
                .map(String::as_str),
            Some("rbac-defaults")
        );
        assert_eq!(
            role.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("rbac.authorization.kubernetes.io/autoupdate"))
                .map(String::as_str),
            Some("true")
        );

        let binding: ClusterRoleBinding = storage
            .get("/registry/clusterrolebindings/system:service-account-issuer-discovery")
            .await
            .expect("issuer-discovery binding (policy.go:709)");
        assert_eq!(binding.subjects[0].name, "system:serviceaccounts");

        for name in [
            "admin",
            "edit",
            "view",
            "system:discovery",
            "system:basic-user",
            "system:public-info-viewer",
            "system:node",
            "system:node-proxier",
            "system:kube-scheduler",
            "system:kube-controller-manager",
            "system:controller:replicaset-controller",
            "system:controller:generic-garbage-collector",
        ] {
            storage
                .get::<ClusterRole>(&format!("/registry/clusterroles/{name}"))
                .await
                .unwrap_or_else(|e| panic!("ClusterRole {name} missing: {e}"));
        }
        for name in [
            "system:discovery",
            "system:basic-user",
            "system:public-info-viewer",
            "system:node-proxier",
            "system:kube-scheduler",
            "system:kube-controller-manager",
            "system:controller:replicaset-controller",
        ] {
            storage
                .get::<ClusterRoleBinding>(&format!("/registry/clusterrolebindings/{name}"))
                .await
                .unwrap_or_else(|e| panic!("ClusterRoleBinding {name} missing: {e}"));
        }
        // namespace-roles.yaml
        storage
            .get::<serde_json::Value>(
                "/registry/roles/kube-system/system::leader-locking-kube-scheduler",
            )
            .await
            .expect("kube-system leader-locking Role");
        // admin/edit/view are seeded with an aggregationRule and no rules, as
        // upstream; the clusterroleaggregation controller fills them from the
        // aggregate-to-* roles.
        let admin: ClusterRole = storage.get("/registry/clusterroles/admin").await.unwrap();
        assert!(admin.aggregation_rule.is_some());
        assert!(admin.rules.is_empty(), "seeded without rules");
        rusternetes_controller_manager::controllers::clusterrole_aggregation::ClusterRoleAggregationController::new(storage.clone())
            .sync_all()
            .await
            .unwrap();
        for name in ["admin", "edit", "view"] {
            let role: ClusterRole = storage
                .get(&format!("/registry/clusterroles/{name}"))
                .await
                .unwrap();
            assert!(
                !role.rules.is_empty(),
                "{name} aggregated by the controller"
            );
        }
    }

    /// An IPv6-primary range: the `kubernetes` Service takes the range's first
    /// address (`cp.ServiceIPRange`, options.go:378-382) and its family.
    #[tokio::test]
    async fn kubernetes_service_follows_an_ipv6_primary_range() {
        use rusternetes_common::resources::service::IPFamily;

        let storage = MemoryStorage::new();
        let ip =
            crate::registry::core::service::ipranges::ServiceIpRanges::parse("fd00:10:96::/112")
                .unwrap()
                .api_server_service_ip();
        reconcile_kubernetes_service(&storage, 6443, ip)
            .await
            .unwrap();
        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some("fd00:10:96::1"));
        assert_eq!(
            svc.spec.cluster_ips.as_deref(),
            Some(["fd00:10:96::1".to_string()].as_slice())
        );
        assert_eq!(
            svc.spec.ip_families.as_deref(),
            Some([IPFamily::IPv6].as_slice())
        );
    }

    /// The api-server must own the `default/kubernetes` Service, as upstream's
    /// kubernetesservice controller does (`pkg/controlplane/instance.go:349`,
    /// which creates AND repairs it on every reconcile tick).
    ///
    /// Ours only ever reconciled the Endpoints. In the compose stack the Service
    /// comes from bootstrap-cluster.yaml, so nothing was visibly broken — but on a
    /// cluster that does not run that script (the vanilla-swap api-server leg, and
    /// any real mixed deployment) there is no Service at all, so the kubelet
    /// injects no KUBERNETES_SERVICE_HOST/PORT and every in-cluster client dies:
    ///
    /// ```text
    /// Error loading client: error creating client: unable to load in-cluster
    /// configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined
    /// ```
    ///
    /// which aborted the whole conformance suite in 1ms (#1667).
    #[tokio::test]
    async fn creates_the_kubernetes_service_when_absent() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();

        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some(KUBERNETES_SERVICE_IP));
        let ports = &svc.spec.ports;
        assert_eq!(ports[0].port, 443);
        assert_eq!(
            ports[0].target_port.as_ref().map(|t| format!("{t:?}")),
            Some(format!(
                "{:?}",
                rusternetes_common::resources::policy::IntOrString::Int(6443)
            ))
        );
        let labels = svc.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("component").map(String::as_str),
            Some("apiserver")
        );
        assert_eq!(
            labels.get("provider").map(String::as_str),
            Some("kubernetes")
        );
    }

    /// kube-proxy resolves a Service to its EndpointSlices BY IP FAMILY, so a
    /// `kubernetes` Service without `ipFamilies` yields no usable endpoints and
    /// kube-proxy installs a REJECT rule for 10.96.0.1 with the comment
    /// "default/kubernetes:https has no endpoints" — the ClusterIP is then
    /// unroutable even though the EndpointSlice is present and ready.
    ///
    /// Upstream sets `IPFamilyPolicy: SingleStack` and `SessionAffinity: None`
    /// explicitly when it creates this Service
    /// (`pkg/controlplane/controller/kubernetesservice/controller.go:225-239`),
    /// and the registry allocator fills `ClusterIPs`/`IPFamilies` on the way in
    /// (`pkg/registry/core/service/storage/alloc.go:239`). We write straight to
    /// storage and so bypass that allocator, which is why they must be set
    /// here. Observed live: `kube-dns` had all three and worked; this Service
    /// had none of them and did not.
    #[tokio::test]
    async fn kubernetes_service_carries_the_ip_family_fields_kube_proxy_needs() {
        use rusternetes_common::resources::service::{IPFamily, IPFamilyPolicy};

        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");

        assert_eq!(
            svc.spec.cluster_ips.as_deref(),
            Some([KUBERNETES_SERVICE_IP.to_string()].as_slice()),
            "clusterIPs must mirror clusterIP — the registry allocator would have \
             set it, and kube-proxy reads it"
        );
        assert_eq!(
            svc.spec.ip_families.as_deref(),
            Some([IPFamily::IPv4].as_slice()),
            "ipFamilies must be [IPv4]; without it kube-proxy matches no \
             EndpointSlice and REJECTs the ClusterIP"
        );
        assert_eq!(
            svc.spec.ip_family_policy,
            Some(IPFamilyPolicy::SingleStack),
            "upstream sets IPFamilyPolicy: SingleStack explicitly"
        );
        assert_eq!(
            svc.spec.session_affinity.as_deref(),
            Some("None"),
            "upstream sets SessionAffinity: None explicitly"
        );
    }

    /// Reconcile runs every tick, so it must be idempotent.
    #[tokio::test]
    async fn reconciling_the_service_twice_is_stable() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let first: rusternetes_common::resources::Service = storage.get(SERVICE_KEY).await.unwrap();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let second: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.unwrap();
        assert_eq!(first.metadata.uid, second.metadata.uid, "must not recreate");
        assert_eq!(
            second.spec.cluster_ip.as_deref(),
            Some(KUBERNETES_SERVICE_IP)
        );
    }

    /// And it must self-heal: upstream recreates the Service if it is deleted.
    #[tokio::test]
    async fn recreates_the_service_after_deletion() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        storage.delete(SERVICE_KEY).await.unwrap();
        reconcile_kubernetes_service(&storage, 6443, test_service_ip())
            .await
            .unwrap();
        let svc: rusternetes_common::resources::Service = storage
            .get(SERVICE_KEY)
            .await
            .expect("Service must be recreated after deletion");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some(KUBERNETES_SERVICE_IP));
    }

    #[tokio::test]
    async fn updates_ip_on_recreate() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "172.18.0.2", 6443)
            .await
            .unwrap();
        // Simulate api-server container recreate with a new bridge IP.
        reconcile_kubernetes_endpoint(&storage, "172.18.0.9", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(
            ep.subsets[0].addresses.as_ref().unwrap()[0].ip,
            "172.18.0.9"
        );
        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.endpoints[0].addresses, vec!["172.18.0.9".to_string()]);
    }

    #[tokio::test]
    async fn repairs_malformed_empty_subsets() {
        let storage = MemoryStorage::new();
        // An endpoint stuck with no subsets — the failure mode the old one-shot
        // bootstrap could not repair (its update path only touched an existing
        // first address).
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        let mut metadata = ObjectMeta::new("kubernetes");
        metadata.namespace = Some("default".to_string());
        let broken = Endpoints {
            type_meta: TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            subsets: vec![],
        };
        storage.create(ENDPOINTS_KEY, &broken).await.unwrap();

        reconcile_kubernetes_endpoint(&storage, "10.1.2.3", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(ep.subsets[0].addresses.as_ref().unwrap()[0].ip, "10.1.2.3");
        assert_eq!(
            ep.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(SKIP_MIRROR_LABEL)
                .map(String::as_str),
            Some("true")
        );
    }

    // --- default ServiceCIDR controller -----------------------------------

    use rusternetes_common::resources::{ServiceCIDR, ServiceCIDRCondition, ServiceCIDRStatus};

    fn default_cidrs() -> Vec<String> {
        DEFAULT_SERVICE_CIDRS
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    fn sc_key() -> String {
        rusternetes_storage::build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME)
    }

    fn controller(
        storage: &Arc<MemoryStorage>,
        cidrs: Vec<String>,
    ) -> DefaultServiceCIDRController<MemoryStorage> {
        DefaultServiceCIDRController::new(Arc::clone(storage), cidrs)
    }

    async fn stored(storage: &MemoryStorage) -> ServiceCIDR {
        storage
            .get::<ServiceCIDR>(&sc_key())
            .await
            .expect("default ServiceCIDR exists")
    }

    fn ready(sc: &ServiceCIDR) -> Option<ServiceCIDRCondition> {
        sc.status
            .as_ref()?
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.condition_type == "Ready")
            .cloned()
    }

    #[tokio::test]
    async fn creates_the_default_servicecidr_and_marks_it_ready() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        let sc = stored(&storage).await;
        assert_eq!(sc.spec.as_ref().unwrap().cidrs, default_cidrs());
        let cond = ready(&sc).expect("Ready condition applied by syncStatus");
        assert_eq!(cond.status, "True");
        assert_eq!(cond.message, DEFAULT_SERVICE_CIDR_READY_MESSAGE);
        assert_eq!(
            cond.reason, "",
            "upstream applies Ready=True with no reason"
        );
    }

    #[tokio::test]
    async fn sync_is_idempotent() {
        let storage = Arc::new(MemoryStorage::new());
        let mut c = controller(&storage, default_cidrs());
        c.sync().await.unwrap();
        let first = ready(&stored(&storage).await).unwrap();
        c.sync().await.unwrap();
        let second = ready(&stored(&storage).await).unwrap();
        assert_eq!(first.last_transition_time, second.last_transition_time);
    }

    #[tokio::test]
    async fn upgrades_single_stack_to_dual_stack() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, vec!["10.96.0.0/12".into()])
            .sync()
            .await
            .unwrap();

        let dual = vec!["10.96.0.0/12".to_string(), "2001:db8::/112".to_string()];
        controller(&storage, dual.clone()).sync().await.unwrap();

        assert_eq!(stored(&storage).await.spec.unwrap().cidrs, dual);
    }

    #[tokio::test]
    async fn mismatched_cidrs_leave_the_condition_alone() {
        let storage = Arc::new(MemoryStorage::new());
        // Persisted range disagrees with this api-server's configuration.
        controller(&storage, vec!["10.0.0.0/16".into()])
            .sync()
            .await
            .unwrap();
        let mut sc = stored(&storage).await;
        sc.status = None;
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, vec!["10.96.0.0/12".into()])
            .sync()
            .await
            .unwrap();

        let sc = stored(&storage).await;
        assert_eq!(
            sc.spec.as_ref().unwrap().cidrs,
            vec!["10.0.0.0/16".to_string()],
            "a mismatch is reported, never silently rewritten"
        );
        assert!(
            ready(&sc).is_none(),
            "inconsistent config must not be marked Ready"
        );
    }

    #[tokio::test]
    async fn never_touches_the_status_of_a_terminating_servicecidr() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        // The controller-manager owns the terminating path; clear Ready and
        // mark the object deleting the way a DELETE + finalizer would.
        let mut sc = stored(&storage).await;
        sc.status = Some(ServiceCIDRStatus { conditions: None });
        sc.metadata.deletion_timestamp = Some(chrono::Utc::now());
        sc.metadata.finalizers = Some(vec!["networking.k8s.io/service-cidr-finalizer".into()]);
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, default_cidrs()).sync().await.unwrap();

        assert!(
            ready(&stored(&storage).await).is_none(),
            "a deleting ServiceCIDR must not be marked Ready again"
        );
    }

    #[tokio::test]
    async fn does_not_overwrite_a_ready_false_condition() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        let mut sc = stored(&storage).await;
        sc.status = Some(ServiceCIDRStatus {
            conditions: Some(vec![ServiceCIDRCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                observed_generation: None,
                last_transition_time: None,
                reason: "Terminating".to_string(),
                message: "blocked".to_string(),
            }]),
        });
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, default_cidrs()).sync().await.unwrap();

        let cond = ready(&stored(&storage).await).unwrap();
        assert_eq!(
            cond.status, "False",
            "Ready=False is another component's to clear, not ours"
        );
        assert_eq!(cond.reason, "Terminating");
    }
}

#[cfg(test)]
mod post_start_hook_registration_tests {
    use super::*;
    use crate::post_start_hooks::global;

    async fn finished(name: &str) -> bool {
        for _ in 0..200 {
            if global().check(name) == Some(Ok(())) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// Each controller upstream registers as a `poststarthook/<name>` check
    /// that passes once the hook has started it (instance.go:360,
    /// apiserver.go:339/361, server.go:249).
    #[tokio::test]
    async fn controller_hooks_register_and_finish() {
        let storage = Arc::new(StorageBackend::new_memory());
        spawn_endpoint_reconciler(storage.clone(), 6443, super::test_service_ip());
        spawn_apiservice_availability_controller(storage.clone());
        crate::registry::apiextensions::customresourcedefinition::spawn_resync(storage.clone());
        spawn_cluster_authentication_trust_controller(
            storage.clone(),
            cluster_authentication_info(None),
        );
        start_default_servicecidr_controller(
            storage.clone(),
            DEFAULT_SERVICE_CIDRS
                .iter()
                .map(|c| c.to_string())
                .collect(),
        )
        .await;
        for name in [
            BOOTSTRAP_CONTROLLER_HOOK,
            APISERVICE_LOCAL_AVAILABLE_HOOK,
            APISERVICE_REMOTE_AVAILABLE_HOOK,
            APIEXTENSIONS_CONTROLLERS_HOOK,
            CLUSTER_AUTHENTICATION_INFO_HOOK,
            SERVICE_CIDR_CONTROLLER_HOOK,
        ] {
            assert!(finished(name).await, "{name} not registered/finished");
        }
    }

    /// storage_rbac.go:162-179: poll 1s/30s, then fail with
    /// "unable to initialize roles: %v" -- which the runner makes fatal.
    #[tokio::test(start_paused = true)]
    async fn rbac_hook_poll_fails_with_upstream_message() {
        let r = poll_until_msg(
            "unable to initialize roles",
            Duration::from_secs(30),
            Duration::from_secs(1),
            || async { Err::<(), _>(anyhow::anyhow!("etcd down")) },
        )
        .await;
        assert_eq!(
            r.unwrap_err().to_string(),
            "unable to initialize roles: etcd down"
        );
    }

    #[tokio::test]
    async fn rbac_hook_seeds_policy_and_finishes() {
        let storage = Arc::new(StorageBackend::new_memory());
        spawn_rbac_bootstrap_roles_hook(storage.clone())
            .await
            .unwrap();
        assert_eq!(global().check(RBAC_BOOTSTRAP_ROLES_HOOK), Some(Ok(())));
        let _: serde_json::Value = storage
            .get("/registry/clusterroles/cluster-admin")
            .await
            .unwrap();
    }
}

/// Ported from `pkg/controlplane/controller/systemnamespaces/
/// system_namespaces_controller_test.go` `Test_Controller`: `sync` creates
/// exactly the missing system namespaces and leaves existing ones alone.
#[cfg(test)]
mod system_namespaces_tests {
    use super::*;
    use rusternetes_common::resources::Namespace;

    async fn seed(storage: &StorageBackend, ns: &str) -> String {
        create_namespace_if_needed(storage, ns).await.unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, ns);
        storage
            .get::<Namespace>(&key)
            .await
            .unwrap()
            .metadata
            .uid
            .clone()
    }

    /// options.go:131 `{kube-system, kube-public, default}` plus
    /// cmd/kube-apiserver/app/options/options.go:94 `kube-node-lease`.
    #[test]
    fn the_four_system_namespaces() {
        assert_eq!(
            SYSTEM_NAMESPACES,
            ["kube-system", "kube-public", "default", "kube-node-lease"]
        );
        assert_eq!(SYSTEM_NAMESPACES_HOOK, "start-system-namespaces-controller");
    }

    #[tokio::test]
    async fn creates_every_missing_system_namespace() {
        for pre in [
            &["foo", "bar"][..],
            &["kube-system"],
            &["kube-system", "kube-public"],
        ] {
            let storage = StorageBackend::new_memory();
            for ns in pre {
                seed(&storage, ns).await;
            }
            let errs = sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
            assert!(errs.is_empty(), "{errs:?}");
            for ns in SYSTEM_NAMESPACES {
                let key = rusternetes_storage::build_key("namespaces", None, ns);
                let got: Namespace = storage.get(&key).await.unwrap();
                assert_eq!(got.metadata.name, ns);
            }
        }
    }

    /// "the four namespaces" case: no create at all, so uids are unchanged.
    #[tokio::test]
    async fn existing_namespaces_are_not_recreated() {
        let storage = StorageBackend::new_memory();
        let before = seed(&storage, "kube-system").await;
        sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
        sync_system_namespaces(&storage, &SYSTEM_NAMESPACES).await;
        let key = rusternetes_storage::build_key("namespaces", None, "kube-system");
        let got: Namespace = storage.get(&key).await.unwrap();
        assert_eq!(got.metadata.uid, before);
    }
}
