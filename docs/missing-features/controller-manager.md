# Missing Features — controller-manager

Per-module comparison of `crates/controller-manager` (Rust, Rusternetes) against
upstream Kubernetes' `cmd/kube-controller-manager` + `pkg/controller/*` (Go).
Source of truth for upstream:
[kubernetes/kubernetes/tree/master/pkg/controller](https://github.com/kubernetes/kubernetes/tree/master/pkg/controller).

## Scope

This document covers everything that runs under the `kube-controller-manager`
binary upstream. Cloud-specific controllers (`route`, `service` LB sync,
`node` route, node addresses) live in a separate
`cloud-controller-manager` binary upstream since v1.21 — Rusternetes folds the
cloud LB sync into its own `LoadBalancerController` (see
`crates/controller-manager/src/controllers/loadbalancer.rs`) and uses an
optional `Arc<dyn CloudProvider>` injected at startup. The `cloud-controller-manager`
binary itself is **not** a separate Rust crate in Rusternetes.

Out of scope: `kube-scheduler` (own crate, `crates/scheduler`),
`kubelet` (own crate), `kube-proxy` (own crate). These are documented
separately under `docs/missing-features/`.

## Current Rusternetes state

- 30 controllers spawned by `controller-manager::run()` (see
  `crates/controller-manager/src/lib.rs:46-288`). Each is a `tokio::spawn`'d
  task with a struct of the form
  `pub struct FooController<S: Storage> { storage: Arc<S>, interval: Duration }`.
- The `ResourceClaimController` is implemented at
  `crates/controller-manager/src/controllers/resourceclaim.rs` but is **not
  wired into `lib.rs::run()`** — it has no `tokio::spawn` block in `lib.rs`
  (verified by `grep ResourceClaimController crates/controller-manager/src/lib.rs`
  returning no hits).
- No leader election: `crates/controller-manager/src/lib.rs:41` —
  `// No leader election in all-in-one mode — single instance`. Multi-replica
  controller-manager deployments are not supported; a second instance would
  duplicate every reconcile.
- Some controllers (e.g. `apiservice.rs:24-67`, `ttl_controller.rs:32-60`,
  `resourceclaim.rs:31-55`) have migrated to a watch + `WorkQueue`-based loop
  with periodic resync. Many older controllers still use a fixed
  `interval`-based `loop { reconcile_all; sleep }` pattern (e.g.
  `deployment.rs:62-90`, `replicaset.rs`, `hpa.rs:132+`). Upstream uses
  shared informers + per-controller rate-limited workqueues universally.
- No shared informer cache — every controller does its own
  `storage.list("/registry/<kind>/")` per reconcile, generating O(N) API
  reads per cycle. Upstream resolves this via `client-go` shared informers
  driven by a single watch per (kind, namespace) tuple.
- No metrics endpoint emitted by the controller-manager crate (upstream
  exposes `workqueue_*`, `controller_reconcile_*`, `leader_election_*` on
  `/metrics`).

## Parity matrix

Upstream column maps to a directory under
[pkg/controller/](https://github.com/kubernetes/kubernetes/tree/master/pkg/controller)
or a file under
[cmd/kube-controller-manager/app/](https://github.com/kubernetes/kubernetes/tree/master/cmd/kube-controller-manager/app).
"Wired" = present in `crates/controller-manager/src/lib.rs::run()` task list.

| Upstream controller | Rusternetes file | Wired | Status / notes |
|---|---|---|---|
| `deployment` | `deployment.rs` | Yes (`lib.rs:60`) | Revision/rollback annotations present (`deployment.rs:344-417`); no progress deadline reconcile, paused-deployment handling thin |
| `replicaset` | `replicaset.rs` | Yes (`lib.rs:76`) | Sync-based; no expectations cache (upstream `pkg/controller/replicaset/expectations.go`) |
| `replication` (ReplicationController) | `replicationcontroller.rs` | Yes (`lib.rs:68`) | Separate controller as upstream |
| `statefulset` | `statefulset.rs` | Yes (`lib.rs:84`) | Single-instance, no shared informer; OrderedReady / Parallel pod-management partially implemented |
| `daemon` (DaemonSet) | `daemonset.rs` | Yes (`lib.rs:92`) | No rolling-update surge, no node-affinity update predicate |
| `job` | `job.rs` | Yes (`lib.rs:100`) | Indexed-job + suspended-job support partial; no backoffLimitPerIndex |
| `cronjob` | `cronjob.rs` | Yes (`lib.rs:108`) | v2 fast-path (workqueue-driven) not implemented; uses interval |
| `endpoint` | `endpoints.rs` | Yes (`lib.rs:148`) | Legacy v1 Endpoints |
| `endpointslice` | `endpointslice.rs` | Yes (`lib.rs:156`) | Reconciler logic simpler than upstream `pkg/controller/endpointslice/reconciler.go` |
| `endpointslicemirroring` | — | — | **Missing.** Manually-managed Endpoints → EndpointSlice mirror; relevant for legacy external integrations |
| `service` (in-cluster) | `service.rs` | Yes (`lib.rs:268`) | ClusterIP allocation lives in api-server; controller does status reconcile |
| Cloud `service` LB | `loadbalancer.rs` | Yes (`lib.rs:47`) | Takes `Option<Arc<dyn CloudProvider>>`; warns and no-ops when absent (`loadbalancer.rs:45-47`) |
| `nodeipam` | — | — | **Missing.** No per-node `podCIDR` allocation; `node.rs` does not call any IPAM allocator. Pods get IPs from kubelet's CNI plugin without a central range. |
| `nodelifecycle` | `node.rs` + `taint_eviction.rs` | Yes (`lib.rs:252,276`) | Heartbeat/Lease-based readiness done (`node.rs:183,228-253`); NoExecute eviction done (`taint_eviction.rs`); zone-aware rate limits, secondary-eviction queue, and unreachable-node taint application all absent |
| `route` (cloud) | — | — | **Missing.** Cloud route programming for non-overlay CNIs |
| `bootstrap` (bootstrap-signer + token-cleaner) | — | — | **Missing.** No kubeadm bootstrap-token signing of `cluster-info` ConfigMap, no expiration GC of bootstrap-token Secrets |
| `clusterroleaggregation` | — | — | **Missing.** No aggregation of ClusterRoles via `aggregationRule.clusterRoleSelectors` — RBAC composition is manual |
| `certificates/approver` | `certificate_signing_request.rs` | Yes (`lib.rs:228`) | Auto-approves kubelet certs (`certificate_signing_request.rs:24-31`); generic approver policy absent |
| `certificates/signer` (kubelet-serving, kubelet-client, apiserver-client, legacy-unknown) | — | — | **Missing.** Comment at `certificate_signing_request.rs:19` defers signing to external signers; no built-in signer-CA loop |
| `certificates/authority` | — | — | **Missing.** No internal CA management for built-in signers |
| `certificates/cleaner` | — | — | **Missing.** No GC of expired/denied CSRs |
| `certificates/rootcacertpublisher` | `namespace.rs:160-201` | Yes (folded into namespace) | `kube-root-ca.crt` ConfigMap created per namespace; behavior matches upstream rootcacertpublisher |
| `certificates/clustertrustbundlepublisher` | — | — | **Missing.** No ClusterTrustBundle publication |
| `serviceaccount` (controller) | `serviceaccount.rs` | Yes (`lib.rs:260`) | Default SA creation + token Secret creation present |
| `serviceaccount/tokens` (TokenRequest path) | partial — `serviceaccount.rs:3,67-110` | folded | Local JWT signing via `jsonwebtoken` crate; bound-token TokenRequest API path lives in api-server. No projected-volume audience binding logic |
| `serviceaccount/legacyserviceaccounttokencleaner` | — | — | **Missing.** No cleanup of unused auto-generated SA token Secrets (KEP-2799) |
| `garbagecollector` | `garbage_collector.rs` | Yes (`lib.rs:178`) | Owner-ref graph walk via grace period instead of informer cache (`garbage_collector.rs:33,332`); cascading delete works for tested cases |
| `namespace` | `namespace.rs` | Yes (`lib.rs:244`) | Finalizer drain implemented (`namespace.rs:211-276`); large kinds (CRs from CRDs) drain via storage list — no discovery-driven dynamic deleter |
| `node` (NodeController) | `node.rs` | Yes (`lib.rs:276`) | See nodelifecycle row for split |
| `podautoscaler` (HPA) | `hpa.rs` | Yes (`lib.rs:184`) | Custom-metric path queries Prometheus directly; **TODO `hpa.rs:452`** — does not call `metrics.k8s.io/v1beta1`. No behavior-window stabilization, no `HPAScalingPolicy` rate enforcement |
| `podautoscaler` VPA | `vpa.rs` | Yes (`lib.rs:192`) | **VPA is not in upstream `pkg/controller`** — it's an addon (`kubernetes/autoscaler`). Rusternetes ships it in-tree. Tracked here for completeness. |
| `disruption` (PDB) | `pod_disruption_budget.rs` | Yes (`lib.rs:204`) | `currentHealthy`/`desiredHealthy` computed; **TODO `pod_disruption_budget.rs:306`** — `matchExpressions` selector not supported; `Eviction` subresource enforcement in api-server is separate; no `unhealthyPodEvictionPolicy` evaluation |
| `resourcequota` | `resource_quota.rs` | Yes (`lib.rs:170`) | Scope selectors and priority-class scope partial; no quota-monitor lazy recompute |
| `ttlafterfinished` (Job TTL) | `ttl_controller.rs` | Yes (`lib.rs:198`) | Drives `ttlSecondsAfterFinished` (`ttl_controller.rs:186-330`) |
| `ttl` (node-condition TTL annotator) | — | — | **Missing.** No `ttl.kubernetes.io/...` annotation publisher for kubelet client cache hints |
| `podgc` | partial in `garbage_collector.rs:140` | — | **Missing as standalone.** No pod-GC by terminated-pod-threshold; no orphan-pod cleanup on node deletion outside cascading delete |
| `volume/persistentvolume` (PV binder) | `pv_binder.rs` | Yes (`lib.rs:116`) | Static binding works |
| `volume/persistentvolume` (provisioner) | `dynamic_provisioner.rs` | Yes (`lib.rs:124`) | In-tree provisioner stub; no CSI external-provisioner sidecar pattern needed since storage-class plugins are direct |
| `volume/attachdetach` | — | — | **Missing.** No `VolumeAttachment` reconcile loop; kubelet handles attach via bollard mount, no detach-on-eviction flow |
| `volume/expand` | `volume_expansion.rs` | Yes (`lib.rs:140`) | Online expand handled |
| `volume/ephemeral` | — | — | **Missing.** No automatic PVC creation for generic-ephemeral pod volumes (`PodSpec.Volumes[].Ephemeral.VolumeClaimTemplate`) |
| `volume/pvcprotection` | — | — | **Missing.** No `kubernetes.io/pvc-protection` finalizer add/remove |
| `volume/pvprotection` | — | — | **Missing.** No `kubernetes.io/pv-protection` finalizer add/remove |
| `volume/vacprotection` | — | — | **Missing.** No `VolumeAttributesClass` protection (KEP-3751) |
| `volume/selinuxwarning` | — | — | **Missing.** No SELinux mismatch warning emission |
| `volume/events` | — | — | **Missing.** No volume-attached/detached Event emitter |
| `events` (event-aggregator) | `events.rs` | Yes (`lib.rs:164`) | Hour-based TTL window (`events.rs:351`); upstream aggregates via `events.k8s.io/v1` series-aggregation — not implemented |
| `validatingadmissionpolicystatus` | — | — | **Missing.** No status reconcile for `ValidatingAdmissionPolicy` (KEP-3488). VAP enforcement is in api-server admission chain, but status condition publishing is not run |
| `storageversion` | — | — | **Missing.** No `StorageVersion` API publication |
| `storageversiongc` | — | — | **Missing.** No GC of `StorageVersion` records on api-server lease loss |
| `storageversionmigrator` | — | — | **Missing.** No automated re-write of stored objects after a `StorageVersionMigration` is requested (KEP-2855) |
| `servicecidrs` | — | — | **Missing.** No `ServiceCIDR`/`IPAddress` (beta) reconcile — Rusternetes still does static ClusterIP allocation from a single configured CIDR in api-server |
| `resourceclaim` (DRA) | `resourceclaim.rs` | **No** (not in `lib.rs`) | Allocation logic exists but is dead code at runtime; **TODO `resourceclaim.rs:346`** — CEL device-selector evaluation not implemented |
| `devicetainteviction` | — | — | **Missing.** Dynamic Resource Allocation device-taint eviction (KEP-5055) |
| `resourcepoolstatusrequest` | — | — | **Missing.** Upstream beta controller for DRA pool status |
| `apis/discovery` (APIService availability) | `apiservice.rs` | Yes (`lib.rs:284`) | Mirrors upstream `kube-aggregator` remote-availability controller per comment at `apiservice.rs:6` |
| `crd` (CRD establishment) | `crd.rs` | Yes (`lib.rs:236`) | Drives `Established` condition |
| `ingress` | `ingress.rs` | Yes (`lib.rs:220`) | Not part of upstream `kube-controller-manager` — upstream leaves Ingress to third-party controllers. Rusternetes bundles a minimal one. |
| `networkpolicy` | `network_policy.rs` | Yes (`lib.rs:212`) | Likewise not in upstream `kube-controller-manager`; enforcement lives in the CNI |
| `volumesnapshot` | `volume_snapshot.rs` | Yes (`lib.rs:132`) | External addon upstream (`external-snapshotter`), in-tree here |
| `kube-controller-manager-leader-election` | — | — | **Missing.** See `lib.rs:41` |

## Missing features

### 1. Leader election

- **Upstream:** `vendor/k8s.io/client-go/tools/leaderelection` + `Lease`
  objects in `kube-system`. Two replicas of `kube-controller-manager`
  contend on one `Lease`; the loser sleeps.
- **Why it matters:** Multi-replica HA. Today, running two
  Rusternetes controller-managers would double-reconcile every object,
  cause `resourceVersion` conflict storms, and double-fire Events.
- **Effort:** Medium. Needs a `Lease`-based primitive in `common`/`storage`
  with periodic renew + observe, and a wrapper around each `tokio::spawn`
  block in `lib.rs` that yields when the lease is lost.

### 2. Shared informer cache

- **Upstream:** A single `client-go` SharedInformerFactory hands every
  controller a watch-backed in-memory cache keyed by GVR.
- **Why:** Each Rusternetes controller does `storage.list(...)` per
  reconcile, scaling read load linearly with controller count × object
  count. Hot-path latency under load is dominated by storage I/O.
- **Effort:** Large. Requires a reflector layer in `rusternetes-common`
  (or a new `rusternetes-informers` crate) plus migrating every
  `reconcile_all` to read from cache.

### 3. NodeIPAM (`pkg/controller/nodeipam`)

- **Upstream:** Allocates a `Node.spec.podCIDR` and
  `Node.spec.podCIDRs` slice from a cluster-wide CIDR, supporting
  IPv4, IPv6, and dual-stack.
- **Why:** Required for any non-overlay CNI that consumes
  `podCIDR`. Conformance test
  `[sig-network] Networking should provide unchanging, static URL paths`
  and the routed-CNI E2E suites read this field.
- **Effort:** Medium. CIDR allocator state in `NodeController` keyed
  on `node.metadata.name`.

### 4. ServiceCIDR / IPAddress beta controller

- **Upstream:** `pkg/controller/servicecidrs` — multi-`ServiceCIDR`
  support, evacuation of in-use addresses on CIDR deletion
  (KEP-1880 beta in 1.31).
- **Why:** Newer conformance tests, upgrade-path for clusters needing
  larger Service ranges without a restart.
- **Effort:** Medium. Closely tied to api-server `clusterip` allocator.

### 5. Bootstrap signer + token cleaner (`pkg/controller/bootstrap`)

- **Upstream:** Signs `cluster-info` ConfigMap with bootstrap-token
  HMAC; deletes expired bootstrap-token Secrets.
- **Why:** Required for `kubeadm join` flows.
- **Effort:** Small. ~250 lines per controller; cryptographic primitives
  already present in `serviceaccount.rs` deps (`jsonwebtoken`,
  `hmac`/`sha2` via transitive deps).

### 6. ClusterRoleAggregation

- **Upstream:** Watches ClusterRoles with `aggregationRule` and
  composes their `rules` from selector-matched ClusterRoles.
- **Why:** Standard idiom for operator RBAC bundling (e.g.
  `system:aggregate-to-admin`); some conformance and addon manifests
  depend on it.
- **Effort:** Small. Pure logic on existing rbac.authorization.k8s.io
  resources.

### 7. Built-in CSR signers (`pkg/controller/certificates/signer`)

- **Upstream:** Four signers — `kubelet-serving`, `kubelet-client`,
  `kube-apiserver-client`, `legacy-unknown` — sign approved CSRs
  using a cluster CA key.
- **Why:** Without this, kubelets that submit CSRs (kubeadm flow,
  `--rotate-certificates`) never receive a signed cert. The current
  `certificate_signing_request.rs` only auto-approves; signing is
  deferred to "external signers" per the doc comment at
  `certificate_signing_request.rs:19`.
- **Effort:** Medium. CA key plumbing + x509 signing via `rcgen`
  or `rustls-pki-types`.

### 8. CSR cleaner

- **Upstream:** GCs CSRs in `Approved` (>1h since signed),
  `Denied`, or `Failed` state to bound storage growth.
- **Why:** Long-running clusters accumulate CSRs; etcd watch overhead
  grows.
- **Effort:** Small.

### 9. Pod GC controller (`pkg/controller/podgc`)

- **Upstream:** Deletes pods stuck in `Pending` on missing nodes,
  pods bound to non-existent nodes, and trims terminated pods to
  `--terminated-pod-gc-threshold`.
- **Why:** Orphan-pod accumulation after node deletion. The current
  `garbage_collector.rs` handles owner-ref cascade but explicitly
  excludes Node-scoped pod cleanup
  (`garbage_collector.rs:140` notes that NamespaceController handles
  namespace-deletion cascade, but neither path covers node-deleted pods).
- **Effort:** Small-medium.

### 10. EndpointSlice mirroring (`pkg/controller/endpointslicemirroring`)

- **Upstream:** When users manually create `Endpoints` (no Service
  selector), mirror them into `EndpointSlice` objects so dual-stack
  proxy clients work.
- **Why:** Legacy headless-service workflows. kube-proxy in
  Rusternetes already reads both — but without the mirror, manually
  created `Endpoints` will not appear in `EndpointSlice` lists.
- **Effort:** Small.

### 11. VolumeAttachment / attach-detach controller (`pkg/controller/volume/attachdetach`)

- **Upstream:** Drives the `VolumeAttachment` API to coordinate CSI
  attach/detach with kubelet; ensures volumes detach on pod deletion or
  node failure.
- **Why:** Required for any CSI driver running outside in-tree volume
  paths. Rusternetes kubelet currently mounts volumes directly via
  bollard; CSI drivers won't function until this controller plus the
  api-server `VolumeAttachment` handler exist.
- **Effort:** Large.

### 12. PVC/PV protection finalizers

- **Upstream:** `volume/pvcprotection` + `volume/pvprotection` add
  `kubernetes.io/pvc-protection`/`pv-protection` finalizers to block
  deletion while in use by a pod / bound.
- **Why:** Data-loss protection. Without it, a `kubectl delete pvc`
  on an in-use PVC succeeds and the bound PV is reclaimed even though
  pods still reference it.
- **Effort:** Small.

### 13. Generic ephemeral volumes (`volume/ephemeral`)

- **Upstream:** Auto-creates a PVC named `<pod>-<volume>` from
  `PodSpec.Volumes[].Ephemeral.VolumeClaimTemplate` with the pod as
  owner.
- **Why:** Conformance + addon (e.g. CSI ephemeral driver) support.
- **Effort:** Small.

### 14. HPA metrics-API client (resolves `hpa.rs:452` TODO)

- **Upstream:** Queries `metrics.k8s.io/v1beta1` (resource metrics)
  and `custom.metrics.k8s.io` (custom) via the API aggregator.
  Aggregates pod-level metrics → per-container utilisation.
- **Current:** `hpa.rs:425-460` returns placeholder values; the
  controller's scaling decisions cannot be trusted under load.
- **Effort:** Medium. Needs a metrics-server (or equivalent) plus
  client logic. Rusternetes ships a Prometheus adapter today but
  it's read at controller scope, not via the aggregated API.

### 15. StorageVersionMigrator (KEP-2855)

- **Upstream:** Re-writes stored objects after a
  `StorageVersionMigration` CR is created; required for safe etcd
  version skews.
- **Why:** Without it, deprecated API versions stay in storage and
  break on apiserver upgrade.
- **Effort:** Medium.

### 16. ValidatingAdmissionPolicyStatus

- **Upstream:** Reconciles the `status.typeChecking` block of every
  `ValidatingAdmissionPolicy` by compiling its CEL expressions.
- **Why:** Operators rely on `Status.TypeChecking.ExpressionWarnings`
  to debug policies. Rusternetes' VAP enforcement (in api-server) runs,
  but policy authors get no compile-time feedback.
- **Effort:** Medium. Requires a CEL evaluator (same dependency as
  the `resourceclaim.rs:346` TODO).

## Partial / stubbed

- `crates/controller-manager/src/controllers/hpa.rs:452` — Resource-metric
  query returns placeholder; only Prometheus-custom-metric branch is real
  (`hpa.rs:387,425-460`).
- `crates/controller-manager/src/controllers/pod_disruption_budget.rs:306` —
  `matchExpressions` label-selector branch is a TODO; only `matchLabels`
  is honored.
- `crates/controller-manager/src/controllers/resourceclaim.rs:346` — CEL
  evaluation of device selectors is a TODO; ResourceClaim allocation
  cannot respect `selectors[].cel` expressions yet. The controller is
  also **not spawned** from `lib.rs` (no `tokio::spawn` block exists for
  it).
- `crates/controller-manager/src/controllers/certificate_signing_request.rs:19,24,201-208`
  — Auto-approval works, but signing is "typically handled by external
  signers"; awaiting-manual-approval branch at line 208 logs and exits.
- `crates/controller-manager/src/controllers/loadbalancer.rs:45-47` —
  Logs a warning and no-ops when no `CloudProvider` is injected; this
  is the only path in the current all-in-one binary
  (`lib.rs:42` always passes `None`).
- `crates/controller-manager/src/controllers/garbage_collector.rs:33,332`
  — GC explicitly trades the informer cache for a "grace period"
  heuristic; risk of false-positive cascading deletes during high write
  churn.
- `crates/controller-manager/src/controllers/node.rs:228-253` — Lease /
  heartbeat readiness logic implemented, but no taint application
  (`node.kubernetes.io/unreachable`, `not-ready`) on a missed heartbeat.
  TaintEvictionController (`taint_eviction.rs`) only consumes taints; it
  does not place them on unreachable nodes.

## Known in-code TODOs

```
crates/controller-manager/src/controllers/hpa.rs:452
    // TODO: Query actual metrics from metrics API

crates/controller-manager/src/controllers/pod_disruption_budget.rs:306
    // TODO: Implement match_expressions support

crates/controller-manager/src/controllers/resourceclaim.rs:346
    // TODO: Implement CEL evaluation using cel-interpreter crate
```

(Verified by `grep -nE 'TODO|FIXME|HACK' crates/controller-manager/src/controllers/*.rs`.)

## References

- Upstream tree:
  https://github.com/kubernetes/kubernetes/tree/master/pkg/controller
- Upstream entrypoint:
  https://github.com/kubernetes/kubernetes/tree/master/cmd/kube-controller-manager/app
- Leader election:
  https://github.com/kubernetes/client-go/tree/master/tools/leaderelection
- KEP-1880 (ServiceCIDR beta):
  https://github.com/kubernetes/enhancements/tree/master/keps/sig-network/1880-multiple-service-cidrs
- KEP-2855 (StorageVersionMigrator):
  https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/2855-storage-version-migrator
- KEP-2799 (Bound SA token cleanup):
  https://github.com/kubernetes/enhancements/tree/master/keps/sig-auth/2799-reduction-of-secret-based-service-account-token
- KEP-3488 (ValidatingAdmissionPolicy):
  https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3488-cel-admission-control
- KEP-3751 (VolumeAttributesClass):
  https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/3751-volume-attributes-class
- KEP-5055 (DRA device taints):
  https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/5055-dra-device-taints-and-tolerations
- Rusternetes controller-manager entrypoint:
  `crates/controller-manager/src/lib.rs`
- Rusternetes controllers directory:
  `crates/controller-manager/src/controllers/`
