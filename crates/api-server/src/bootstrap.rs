use anyhow::{Context, Result};
use rusternetes_common::resources::{EndpointSlice, Endpoints};
use rusternetes_storage::Storage;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub const CLUSTER_ADMIN_ROLE_KEY: &str = "/registry/clusterroles/cluster-admin";
pub const CLUSTER_ADMIN_BINDING_KEY: &str = "/registry/clusterrolebindings/cluster-admin";

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

/// Decode one vendored `v1.List` into its items. Mirrors nothing upstream (Go
/// builds the objects in code); this is only the Rust-side loader for the
/// vendored testdata. A role's `rules: null` becomes `[]` so it decodes as a
/// typed `ClusterRole`.
pub fn load_policy_items(yaml: &str) -> Result<Vec<serde_json::Value>> {
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
pub fn bootstrap_policy_items() -> Result<Vec<serde_json::Value>> {
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

pub const SERVICE_KEY: &str = "/registry/services/default/kubernetes";

/// The `kubernetes` Service address under the default range. At runtime it is
/// `ServiceIpRanges::api_server_service_ip` (the first address of the primary
/// `--service-cluster-ip-range`, as upstream derives it).
pub const KUBERNETES_SERVICE_IP: &str = "10.96.0.1";

pub fn test_service_ip() -> std::net::IpAddr {
    KUBERNETES_SERVICE_IP.parse().unwrap()
}

pub const ENDPOINTS_KEY: &str = "/registry/endpoints/default/kubernetes";
pub const ENDPOINTSLICE_KEY: &str = "/registry/endpointslices/default/kubernetes";

/// Upstream `discovery.LabelSkipMirror`. Set on the `kubernetes` Endpoints so
/// the EndpointSlice mirroring controller leaves it alone — the apiserver owns
/// the slice directly (see `crates/controller-manager/.../endpointslice.rs`,
/// which honors this label). Matches `setSkipMirrorTrue`
/// (`pkg/controlplane/reconcilers/endpointsadapter.go`).
pub const SKIP_MIRROR_LABEL: &str = "endpointslice.kubernetes.io/skip-mirror";

/// Address family, mirroring `utilnet.AddressFamily`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AddrFamily {
    V4,
    V6,
}

/// `utilnet.preferIPv4` / `preferIPv6`
/// (`staging/src/k8s.io/apimachinery/pkg/util/net/interface.go:43-44`).
pub const PREFER_IPV4: [AddrFamily; 2] = [AddrFamily::V4, AddrFamily::V6];
pub const PREFER_IPV6: [AddrFamily; 2] = [AddrFamily::V6, AddrFamily::V4];

/// Go's `net.IP.IsGlobalUnicast`: not unspecified, loopback, multicast,
/// link-local unicast, or the IPv4 limited broadcast address.
fn is_global_unicast(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            !(v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_loopback()
                || v4.is_multicast()
                || v4.is_link_local())
        }
        std::net::IpAddr::V6(v6) => {
            !(v6.is_unspecified()
                || v6.is_loopback()
                || v6.is_multicast()
                // fe80::/10, Go's IsLinkLocalUnicast.
                || (v6.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// Port of `utilnet.chooseIPFromHostInterfaces`
/// (`apimachinery/pkg/util/net/interface.go:312-358`): for each preferred
/// family in order, the first global unicast address of that family on a
/// non-loopback interface.
///
/// `get_if_addrs` exposes neither interface up/down nor point-to-point flags
/// (`isInterfaceUp` / `isLoopbackOrPointToPoint`), so only the loopback skip is
/// ported; the default-route based selection (`chooseHostInterfaceFromRoute`)
/// is not ported either.
pub fn choose_ip_from_host_interfaces(
    addrs: &[(String, std::net::IpAddr)],
    families: [AddrFamily; 2],
) -> Result<std::net::IpAddr> {
    if addrs.is_empty() {
        return Err(anyhow::anyhow!("no interfaces found on host."));
    }
    for family in families {
        for (name, ip) in addrs {
            let member = match ip {
                std::net::IpAddr::V4(_) => family == AddrFamily::V4,
                std::net::IpAddr::V6(_) => family == AddrFamily::V6,
            };
            if !member || !is_global_unicast(ip) {
                continue;
            }
            info!("Discovered API server IP: {} (interface: {})", ip, name);
            return Ok(*ip);
        }
    }
    Err(anyhow::anyhow!(
        "no acceptable interface with global unicast address found on host"
    ))
}

fn host_interface_addrs() -> Result<Vec<(String, std::net::IpAddr)>> {
    let interfaces = get_if_addrs::get_if_addrs().map_err(|e| {
        warn!("Failed to get network interfaces: {}", e);
        anyhow::anyhow!("Failed to get network interfaces: {}", e)
    })?;
    Ok(interfaces
        .into_iter()
        .filter(|i| !i.is_loopback())
        .map(|i| (i.name.clone(), i.ip()))
        .collect())
}

/// Port of `utilnet.ResolveBindAddress`
/// (`apimachinery/pkg/util/net/interface.go:468-480`): an unspecified or
/// loopback bind address (or none) resolves to the host's default address of
/// the bind address's family (IPv4-preferred when it has none); anything else
/// is returned as is.
fn resolve_bind_address(bind: Option<std::net::IpAddr>) -> Result<std::net::IpAddr> {
    resolve_bind_address_with(bind, host_interface_addrs)
}

pub fn resolve_bind_address_with(
    bind: Option<std::net::IpAddr>,
    addrs: impl FnOnce() -> Result<Vec<(String, std::net::IpAddr)>>,
) -> Result<std::net::IpAddr> {
    let families = match bind {
        Some(std::net::IpAddr::V6(_)) => PREFER_IPV6,
        _ => PREFER_IPV4,
    };
    match bind {
        Some(ip) if !ip.is_unspecified() && !ip.is_loopback() => Ok(ip),
        _ => choose_ip_from_host_interfaces(&addrs()?, families),
    }
}

/// Where the api-server's own address for the `kubernetes` Endpoints comes
/// from. Upstream sets `GenericConfig.PublicAddress` from `--advertise-address`
/// and, when that is unset or unspecified, from
/// `SecureServingOptions.DefaultExternalAddress` ->
/// `utilnet.ResolveBindAddress(--bind-address)`
/// (`apiserver/pkg/server/options/server_run_options.go:172-187`,
/// `serving.go:123-128`); `pkg/controlplane/instance.go:350` hands it to the
/// reconciler as `PublicIP`. So the family follows the bind address, not the
/// service range.
#[derive(Clone, Copy, Debug, Default)]
pub struct AdvertiseAddress {
    /// `--advertise-address`.
    pub advertise: Option<std::net::IpAddr>,
    /// The IP of `--bind-address`.
    pub bind: Option<std::net::IpAddr>,
}

impl AdvertiseAddress {
    /// `bind_address` is the `host:port` listen string.
    pub fn new(advertise: Option<std::net::IpAddr>, bind_address: &str) -> Self {
        Self {
            advertise,
            bind: bind_address
                .parse::<std::net::SocketAddr>()
                .ok()
                .map(|a| a.ip()),
        }
    }

    pub fn resolve(&self) -> Result<std::net::IpAddr> {
        match self.advertise {
            Some(ip) if !ip.is_unspecified() => Ok(ip),
            _ => resolve_bind_address(self.bind),
        }
    }
}

/// `Config.ExternalAddress` as `Complete` derives it (`server/config.go:706-721`):
/// `--external-hostname` when set, else the public (advertise) address; the
/// secure serving port is appended when the value carries none
/// (`net.SplitHostPort` fails -> `net.JoinHostPort`, IPv6 bracketed).
pub fn external_address(
    external_hostname: Option<&str>,
    advertise: AdvertiseAddress,
    bind_address: &str,
) -> Result<String> {
    let port = bind_address
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6443);
    let host = match external_hostname.filter(|h| !h.is_empty()) {
        Some(h) => h.to_string(),
        None => advertise.resolve()?.to_string(),
    };
    if split_host_port(&host) {
        return Ok(host);
    }
    // JoinHostPort brackets a host that contains a colon.
    Ok(if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

/// Whether `net.SplitHostPort` accepts `s`: `host:port` or `[host]:port`.
fn split_host_port(s: &str) -> bool {
    if let Some(rest) = s.strip_prefix('[') {
        return rest
            .split_once("]:")
            .is_some_and(|(h, p)| !h.contains(['[', ']']) && !p.contains([':', '[', ']']));
    }
    match s.split_once(':') {
        Some((h, p)) => !h.contains(['[', ']']) && !p.contains([':', '[', ']']),
        None => false,
    }
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

    // `endpointSliceFromEndpoints`: IPv4 unless every address is IPv6
    // (`pkg/controlplane/reconcilers/endpointsadapter.go:126,138-140`).
    let address_type = if ip.parse::<std::net::Ipv6Addr>().is_ok() {
        "IPv6"
    } else {
        "IPv4"
    };

    match storage.get::<EndpointSlice>(ENDPOINTSLICE_KEY).await {
        Ok(es) if es.address_type != address_type => {
            // `EnsureEndpointSliceFromEndpoints` (:96-104): an address-type
            // change cannot be updated in place, so delete and re-create.
            storage
                .delete(ENDPOINTSLICE_KEY)
                .await
                .context("Failed to delete kubernetes EndpointSlice")?;
            create_kubernetes_endpointslice(
                storage,
                address_type,
                desired_endpoints,
                desired_ports,
            )
            .await?;
            info!(
                "Re-created kubernetes EndpointSlice ({}) with IP: {}",
                address_type, ip
            );
        }
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
            create_kubernetes_endpointslice(
                storage,
                address_type,
                desired_endpoints,
                desired_ports,
            )
            .await?;
            info!("Created kubernetes EndpointSlice with IP: {}", ip);
        }
    }
    Ok(())
}

async fn create_kubernetes_endpointslice<S: Storage + ?Sized>(
    storage: &S,
    address_type: &str,
    desired_endpoints: Vec<rusternetes_common::resources::endpointslice::Endpoint>,
    desired_ports: Vec<rusternetes_common::resources::endpointslice::EndpointPort>,
) -> Result<()> {
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
        address_type: address_type.to_string(),
        endpoints: desired_endpoints,
        ports: desired_ports,
    };

    storage
        .create(ENDPOINTSLICE_KEY, &es)
        .await
        .context("Failed to create kubernetes EndpointSlice")?;
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
    advertise: AdvertiseAddress,
) -> Result<()> {
    info!("Bootstrapping kubernetes Service and Endpoints");
    let api_server_ip = advertise
        .resolve()
        .context("Failed to discover API server IP address")?
        .to_string();
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
    advertise: AdvertiseAddress,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(BOOTSTRAP_CONTROLLER_HOOK, move || {
        spawn_endpoint_reconciler_loop(storage, api_server_port, service_ip, advertise);
    })
}

fn spawn_endpoint_reconciler_loop(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
    service_ip: std::net::IpAddr,
    advertise: AdvertiseAddress,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ENDPOINT_RECONCILE_INTERVAL);
        // Skip the immediate first tick — startup bootstrap already ran.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let ip = match advertise.resolve() {
                Ok(ip) => ip.to_string(),
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
    use rusternetes_controller_core::apiservice::APIServiceAvailabilityController;
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
pub const DEFAULT_SERVICE_CIDRS: &[&str] = &["10.96.0.0/12"];

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
pub async fn repair_service_cluster_ips_once(
    state: &Arc<crate::state::ApiServerState>,
) -> rusternetes_common::Result<()> {
    use crate::registry::core::service::ipallocator::repair::RepairIpAddress;
    RepairIpAddress::new(state.storage.clone(), ip_address_loopback(state))
        .run_once()
        .await
}

/// The loopback client `RepairIPAddress` writes IPAddresses through
/// (`NewRepairIPAddress(..., client, ...)`,
/// pkg/registry/core/rest/storage_core.go:142-148), the same one the
/// ClusterIP allocators use.
fn ip_address_loopback(
    state: &Arc<crate::state::ApiServerState>,
) -> Arc<dyn crate::registry::core::service::ipallocator::IpAddressClient> {
    Arc::new(crate::registry::networking::ipaddress::Loopback::new(state))
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
    state: &Arc<crate::state::ApiServerState>,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    use crate::registry::core::service::ipallocator::repair::RepairIpAddress;
    use crate::registry::core::service::portallocator::repair::Repair;

    let ip_repair = Arc::new(RepairIpAddress::new(
        state.storage.clone(),
        ip_address_loopback(state),
    ));
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
/// `--client-ca-file` (config.go:344) and, when `--requestheader-client-ca-file`
/// is set, from the `--requestheader-*` flags (config.go:346-357); that is
/// what is published here when the flags are given (#3414).
///
/// Without `--requestheader-client-ca-file` upstream publishes no
/// request-header keys (the `requestHeaderConfig != nil` guard at config.go:346),
/// and neither does this: the bring-up passes the front-proxy flags
/// (`scripts/generate-certs.sh` emits the front-proxy CA + client cert, as
/// kubeadm's `KubeadmCertFrontProxyCA` / `KubeadmCertFrontProxyClient` do), so
/// the former "effective config" fallback is gone (#3423).
///
/// Deviation: `ClientCA` falls back to the cluster CA when `--client-ca-file`
/// is unset, where upstream publishes nothing; extension API servers verify the
/// proxied caller's identity chain against it.
pub fn cluster_authentication_info(
    ca_cert_pem: Option<&str>,
    client_ca_file: Option<&str>,
    request_header: Option<&crate::request_header_authentication::RequestHeaderTrust>,
) -> rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
    let ca = resolve_cluster_ca(ca_cert_pem);
    let client_ca = client_ca_file
        .and_then(|p| std::fs::read_to_string(p).ok())
        .filter(|c| !c.is_empty())
        .or_else(|| ca.clone());
    if let Some(trust) = request_header {
        let cfg = &trust.config;
        return rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
            client_ca,
            request_header_username_headers: Some(cfg.username_headers.clone()),
            request_header_uid_headers: Some(cfg.uid_headers.clone()),
            request_header_group_headers: Some(cfg.group_headers.clone()),
            request_header_extra_header_prefixes: Some(cfg.extra_header_prefixes.clone()),
            request_header_allowed_names: Some(cfg.allowed_names.clone()),
            request_header_ca: std::fs::read_to_string(&trust.client_ca_file)
                .ok()
                .filter(|c| !c.is_empty()),
        };
    }
    rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
        client_ca,
        ..Default::default()
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

pub async fn create_namespace_if_needed(storage: &StorageBackend, ns: &str) -> Result<()> {
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
/// `apiextensions-apiserver/pkg/apiserver/apiserver.go:263`.
pub const CRD_INFORMER_SYNCED_HOOK: &str = "crd-informer-synced";

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
