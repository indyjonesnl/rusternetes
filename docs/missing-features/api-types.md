# Missing Features — API types (`common` crate resources)

Per-module comparison of `crates/common/src/resources/` (Rust, Rusternetes)
against upstream Kubernetes' `staging/src/k8s.io/api/*` (Go). Source of truth
for upstream:
[kubernetes/kubernetes/tree/master/staging/src/k8s.io/api](https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/api).

## Scope

This document covers only the Rust **type definitions** that model
Kubernetes API objects — the structs/enums under
`crates/common/src/resources/`. Handlers (routing, validation, conversion),
storage layout, controllers, kubelet runtime behavior, scheduler plugins,
RBAC enforcement, admission machinery and other behavior that consumes
these types is covered in sibling documents under
`docs/missing-features/`:

- `api-server.md` — handler routes, subresources, conversion, admission wiring
- `controller-manager.md` — reconcile loops that observe/produce these types
- `kubelet.md`, `scheduler.md`, `kube-proxy.md`, `storage.md` — runtime
  consumers
- `cluster-bootstrap.md` — bootstrap resources

What this doc tracks: which K8s API **kinds** Rusternetes models, which
ones are missing entirely, which fields on existing kinds drift from
upstream, and where struct shape diverges from upstream Go.

Out of scope for this file: serialization plumbing (`k8s_time`,
`micro_time`, `IntOrString`), `ObjectMeta` / `ListMeta` / `Condition`
helpers in `crates/common/src/types.rs` (all present and reasonably
complete; see `docs/api-gap-analysis.md` for low-level field drift), and
the OpenAPI / discovery aggregation in
`crates/api-server/src/openapi.rs`.

The exhaustive priority-ranked field drift list lives at
`docs/api-gap-analysis.md` — that file is the running ledger of
swagger-audited gaps. This document is the higher-level "what's not
modeled at all" companion organized by API group.

## Current Rusternetes state

- 36 files under `crates/common/src/resources/` (verified via
  `ls crates/common/src/resources/`). Re-exports gated through
  `crates/common/src/resources.rs:1-36` (`pub mod ...`).
- ~518 `pub struct`/`pub enum` declarations across the directory
  (`grep -cE "^pub (struct|enum)" crates/common/src/resources/*.rs`).
- Resource kinds span 14 K8s API groups. Each resource type's
  `apiVersion` is hard-coded in its `new()` constructor — see e.g.
  `crates/common/src/resources/runtimeclass.rs:46`
  (`api_version: "node.k8s.io/v1".to_string()`).
- Every struct uses `#[serde(rename_all = "camelCase")]` and flattens
  `TypeMeta` via `#[serde(flatten)] pub type_meta: TypeMeta`. K8s
  abbreviation conventions (`podIP`, `hostIP`, `clusterIP`) are honored
  via explicit `#[serde(rename = ...)]` overrides
  (`crates/common/src/resources/service.rs:45`,
  `crates/common/src/resources/pod.rs:1158-1173`).
- The largest type file is `pod.rs` at 2828 lines — covers `Pod`,
  `PodSpec`, `Container`, `EphemeralContainer`, `Probe`, `Volume`,
  `VolumeMount`, `Lifecycle`, `Affinity`, `SecurityContext`,
  `Toleration`, all volume sources, all projection sources, container
  status types, etc.
- The `Event` struct
  (`crates/common/src/resources/event.rs:122-214`) is a unified type
  that carries both `core/v1.Event` fields (`firstTimestamp`,
  `lastTimestamp`, `source`, `involvedObject`, `count`) **and**
  `events.k8s.io/v1.Event` fields (`eventTime`, `note`, `regarding`,
  `reportingController` via `alias`). The struct never serializes with
  `apiVersion: "events.k8s.io/v1"` — see the constructor at
  `event.rs:228` which hard-codes `"v1"`. Effectively
  `events.k8s.io/v1.Event` is only **partially modeled** — no dedicated
  type, no separate API-group registration, deprecated/non-deprecated
  field separation is not enforced.
- `VolumeAttributesClass` (KEP-3751) is declared in
  `crates/common/src/resources/csi.rs:257-268` but tracks
  `storage.k8s.io/v1beta1` — no api-version constant, no `new()`
  constructor, not re-exported as a primary kind. The PVC spec/status
  side of the KEP (`volumeAttributesClassName`,
  `currentVolumeAttributesClassName`, `modifyVolumeStatus`) is wired
  on `PersistentVolumeClaimSpec` / `PersistentVolumeClaimStatus`
  (`volume.rs:258-260`, `volume.rs:332-339`).

## Parity matrix — API groups × resources

Upstream column maps to a directory under
[staging/src/k8s.io/api/](https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/api).
"State" values: **OK** = struct present and field-complete enough for
core conformance; **Partial** = struct present but missing recent KEP
fields (cross-reference `docs/api-gap-analysis.md`); **Missing** = no
struct in `crates/common/src/resources/`; **Stub** = symbol present
but never populated end-to-end.

| API group / version | Kind | Rust file | State |
|---|---|---|---|
| `core/v1` | Pod | `pod.rs:45` | Partial (no `restartPolicyRules`, no `hostnameOverride`, no `schedulingGroup`) |
| `core/v1` | PodSpec | `pod.rs:72` | Partial (see Pod row) |
| `core/v1` | PodTemplate | `workloads.rs:807` | OK |
| `core/v1` | PodTemplateSpec | `workloads.rs:837` | OK |
| `core/v1` | Service | `service.rs:9` | OK (incl. `trafficDistribution`, dual-stack) |
| `core/v1` | Endpoints | `endpoints.rs:8` | OK |
| `core/v1` | Node | `node.rs:9` | Partial (no `Node.spec.configSource` deprecated; no alpha `swap`/`features` wholly populated) |
| `core/v1` | Namespace | `namespace.rs:8` | OK |
| `core/v1` | ConfigMap | `config_and_secret.rs:8` | OK |
| `core/v1` | Secret | `config_and_secret.rs:163` | OK |
| `core/v1` | ServiceAccount | `service_account.rs:7` | OK |
| `core/v1` | PersistentVolume | `volume.rs:10` | OK |
| `core/v1` | PersistentVolumeClaim | `volume.rs:214` | OK |
| `core/v1` | Event | `event.rs:122` | Partial (unified core+events.k8s.io shape, not two distinct kinds) |
| `core/v1` | ReplicationController | `workloads.rs:11` | OK |
| `core/v1` | LimitRange | `policy.rs:95` | OK |
| `core/v1` | ResourceQuota | `policy.rs:9` | OK |
| `core/v1` | ComponentStatus | `componentstatus.rs:7` | OK (deprecated upstream) |
| `core/v1` | Binding | `binding.rs:7` | OK |
| `apps/v1` | Deployment | `deployment.rs:9` | OK |
| `apps/v1` | ReplicaSet | `workloads.rs:117` | OK |
| `apps/v1` | StatefulSet | `workloads.rs:228` | OK (incl. `ordinals`) |
| `apps/v1` | DaemonSet | `workloads.rs:413` | OK |
| `apps/v1` | ControllerRevision | `controllerrevision.rs:9` | OK |
| `batch/v1` | Job | `workloads.rs:557` | Partial (`podFailurePolicy` and `successPolicy` typed as `serde_json::Value` — `workloads.rs:631,635`) |
| `batch/v1` | CronJob | `workloads.rs:716` | OK |
| `networking.k8s.io/v1` | NetworkPolicy | `networking.rs:7` | Partial (`NetworkPolicy.spec.policyTypes` typed as `Option<Vec<String>>` rather than enum; no `cluster-wide` extensions) |
| `networking.k8s.io/v1` | Ingress | `ingress.rs:7` | OK |
| `networking.k8s.io/v1` | IngressClass | `ingressclass.rs:9` | OK |
| `networking.k8s.io/v1` | IPAddress | `ipaddress.rs:8` | Partial (no `Status` subresource modeled) |
| `networking.k8s.io/v1` | ServiceCIDR | `servicecidr.rs:8` | OK |
| `networking.k8s.io/v1beta1` | ClusterCIDR | — | **Missing** (KEP-2593; deprecated upstream in favor of ServiceCIDR but still alpha/beta in some channels) |
| `rbac.authorization.k8s.io/v1` | Role | `rbac.rs:7` | OK |
| `rbac.authorization.k8s.io/v1` | RoleBinding | `rbac.rs:128` | OK |
| `rbac.authorization.k8s.io/v1` | ClusterRole | `rbac.rs:38` | OK (incl. `aggregationRule`) |
| `rbac.authorization.k8s.io/v1` | ClusterRoleBinding | `rbac.rs:173` | OK |
| `storage.k8s.io/v1` | StorageClass | `volume.rs:398` | OK |
| `storage.k8s.io/v1` | VolumeAttachment | `csi.rs:129` | OK |
| `storage.k8s.io/v1` | CSIDriver | `csi.rs:10` | OK |
| `storage.k8s.io/v1` | CSINode | `csi.rs:83` | OK |
| `storage.k8s.io/v1` | CSIStorageCapacity | `csi.rs:233` | OK |
| `storage.k8s.io/v1beta1` | VolumeAttributesClass | `csi.rs:257` | Partial (struct exists; no `new()`, no controller wiring) |
| `scheduling.k8s.io/v1` | PriorityClass | `policy.rs:162` | OK |
| `coordination.k8s.io/v1` | Lease | `coordination.rs:51` | OK (incl. `preferredHolder`, `strategy`) |
| `coordination.k8s.io/v1alpha2` | LeaseCandidate | — | **Missing** (KEP-3960 coordinated leader election) |
| `discovery.k8s.io/v1` | EndpointSlice | `endpointslice.rs:8` | OK (incl. `EndpointHints.forNodes`) |
| `events.k8s.io/v1` | Event | (folded into `event.rs:122`) | Stub — see "Missing kinds" |
| `policy/v1` | PodDisruptionBudget | `policy.rs:222` | OK |
| `policy/v1` | Eviction (subresource) | — | **Missing** (no `Eviction` request kind; eviction handled in handler but no shared struct) |
| `autoscaling/v1` | HorizontalPodAutoscaler | `autoscaling.rs:8` | OK |
| `autoscaling/v2` | HorizontalPodAutoscaler | `autoscaling.rs:8` | OK (incl. `behavior`, `containerResource`) |
| `autoscaling.k8s.io/v1` | VerticalPodAutoscaler | `autoscaling.rs:407` | OK (out-of-tree CRD upstream) |
| `certificates.k8s.io/v1` | CertificateSigningRequest | `certificates.rs:8` | OK |
| `certificates.k8s.io/v1alpha1` | ClusterTrustBundle | — | **Missing** as a top-level kind (KEP-3257). `ClusterTrustBundleProjection` exists at `pod.rs:982` but the CRUD resource type is absent |
| `node.k8s.io/v1` | RuntimeClass | `runtimeclass.rs:12` | OK |
| `apiextensions.k8s.io/v1` | CustomResourceDefinition | `crd.rs:36` | OK |
| `apiregistration.k8s.io/v1` | APIService | — | **Missing** struct type (discovery handler refers to `kind: "APIService"` at `crates/api-server/src/handlers/discovery.rs:3682` but no Rust struct exists) |
| `admissionregistration.k8s.io/v1` | ValidatingWebhookConfiguration | `admission_webhook.rs:13` | OK |
| `admissionregistration.k8s.io/v1` | MutatingWebhookConfiguration | `admission_webhook.rs:79` | OK |
| `admissionregistration.k8s.io/v1` | ValidatingAdmissionPolicy | `validating_admission_policy.rs:13` | OK (incl. `matchConditions`, `paramKind`, `variables`) |
| `admissionregistration.k8s.io/v1` | ValidatingAdmissionPolicyBinding | `validating_admission_policy.rs:314` | OK |
| `admissionregistration.k8s.io/v1alpha1` | MutatingAdmissionPolicy | — | **Missing** (KEP-3962, alpha in 1.32) |
| `admissionregistration.k8s.io/v1alpha1` | MutatingAdmissionPolicyBinding | — | **Missing** (KEP-3962) |
| `flowcontrol.apiserver.k8s.io/v1` | FlowSchema | `flowcontrol.rs:112` | OK |
| `flowcontrol.apiserver.k8s.io/v1` | PriorityLevelConfiguration | `flowcontrol.rs:7` | OK |
| `authentication.k8s.io/v1` | TokenReview | `authentication.rs:13` | OK |
| `authentication.k8s.io/v1` | TokenRequest | `authentication.rs:106` | OK |
| `authentication.k8s.io/v1` | SelfSubjectReview | `authentication.rs:192` | OK |
| `authorization.k8s.io/v1` | SubjectAccessReview | `authorization.rs:13` | OK |
| `authorization.k8s.io/v1` | SelfSubjectAccessReview | `authorization.rs:198` | OK |
| `authorization.k8s.io/v1` | LocalSubjectAccessReview | `authorization.rs:237` | OK |
| `authorization.k8s.io/v1` | SelfSubjectRulesReview | `authorization.rs:263` | OK |
| `resource.k8s.io/v1beta1` | ResourceClaim | `dra.rs:19` | OK |
| `resource.k8s.io/v1beta1` | ResourceClaimTemplate | `dra.rs:371` | OK |
| `resource.k8s.io/v1beta1` | ResourceSlice | `dra.rs:455` | OK |
| `resource.k8s.io/v1beta1` | DeviceClass | `dra.rs:405` | OK |
| `resource.k8s.io/v1alpha3` | ResourceClaimParameters | — | **Missing** (older alpha shape; superseded by inline `DeviceClaim` in v1beta1, retained upstream for skew) |
| `imagepolicy.k8s.io/v1alpha1` | ImageReview | — | **Missing** (admission image-policy webhook, niche) |
| `internal.apiserver.k8s.io/v1alpha1` | StorageVersion | — | **Missing** (used by API aggregation upgrades) |
| `snapshot.storage.k8s.io/v1` | VolumeSnapshot | `volume.rs:455` | OK |
| `snapshot.storage.k8s.io/v1` | VolumeSnapshotClass | `volume.rs:522` | OK |
| `snapshot.storage.k8s.io/v1` | VolumeSnapshotContent | `volume.rs:547` | OK |
| `metrics.k8s.io/v1beta1` | NodeMetrics | `metrics.rs:9` | OK |
| `metrics.k8s.io/v1beta1` | PodMetrics | `metrics.rs:29` | OK |
| `custom.metrics.k8s.io/v1beta2` | MetricValue / List | `custom_metrics.rs:9,41` | OK |
| `external.metrics.k8s.io/v1beta1` | ExternalMetricValueList | — | **Missing** as a top-level kind (used by HPA external metric source) |

## Missing kinds (no Rust struct in `crates/common/src/resources/`)

1. **`events.k8s.io/v1.Event`** — `crates/common/src/resources/event.rs:122` is
   a unified struct that carries both `core/v1.Event` and
   `events.k8s.io/v1.Event` field sets, but the constructor hard-codes
   `api_version: "v1"` (`event.rs:228`). There is no dedicated kind, no
   handler under `crates/api-server/src/handlers/` for the modern API
   group, and no separation of the deprecated-`v1`-only fields
   (`deprecatedCount`, `deprecatedFirstTimestamp`,
   `deprecatedLastTimestamp`, `deprecatedSource`). This is the highest-
   value missing kind because `kubectl events`, the
   event-recorder library, and many controllers default to
   `events.k8s.io/v1` since 1.25. Tracked at
   `docs/api-gap-analysis.md` "NEW-3" as P1.

2. **`apiregistration.k8s.io/v1.APIService`** — referenced in
   `crates/api-server/src/handlers/discovery.rs:3676-3719` as a
   discoverable resource (returned in the `APIResourceList`) but no
   `pub struct APIService { ... }` exists in `crates/common/src/resources/`
   (verified by
   `grep -rn "pub struct APIService" crates/` which returns no hits).
   Without the type, the discovery endpoint advertises a kind that
   cannot actually be CRUD-served. Required for `kube-aggregator` and
   metrics-server-style API aggregation.

3. **`admissionregistration.k8s.io/v1alpha1.MutatingAdmissionPolicy`**
   and **`MutatingAdmissionPolicyBinding`** (KEP-3962). Validating
   counterparts are fully implemented at
   `crates/common/src/resources/validating_admission_policy.rs:13` and
   `:314` — the mutating side is a parallel surface that should reuse
   the same `MatchResources`, `ParamKind`, `MatchCondition`, and
   `Variable` helper types. Alpha in 1.32, expected beta soon.

4. **`certificates.k8s.io/v1alpha1.ClusterTrustBundle`** (KEP-3257).
   Only `pod.rs:982-996` (`ClusterTrustBundleProjection`) and the
   `Volume.projected` plumbing exist; the top-level resource that holds
   the trust anchor PEMs is absent. Without it, `clusterTrustBundle`
   projections in PodSpec reference a kind that has no storage backing.

5. **`coordination.k8s.io/v1alpha2.LeaseCandidate`** (KEP-3960
   coordinated leader election). Companion to `Lease`. `Lease.spec.strategy`
   and `Lease.spec.preferredHolder` are already present
   (`crates/common/src/resources/coordination.rs:116,122`), but the
   `LeaseCandidate` resource the coordinator reads is missing.

6. **`networking.k8s.io/v1beta1.ClusterCIDR`** (KEP-2593). Upstream
   transitioned this to `ServiceCIDR` for Service IP allocation, but
   the pod-CIDR-range allocator surface is a separate kind and still
   present in upstream API groups. Rusternetes only has `ServiceCIDR`
   (`servicecidr.rs:8`) and `IPAddress` (`ipaddress.rs:8`).

7. **`imagepolicy.k8s.io/v1alpha1.ImageReview`** — admission
   image-policy review request. Niche, only used by
   `ImagePolicyWebhook` admission plugin (which Rusternetes does not
   implement).

8. **`internal.apiserver.k8s.io/v1alpha1.StorageVersion`** — used by
   the API server to coordinate storage migration across versions
   during HA rollouts. Required for production-grade upgrade flows.

9. **`resource.k8s.io/v1alpha3.ResourceClaimParameters`** — older
   shape from the pre-`v1beta1` DRA API. Upstream still carries it for
   API-group skew; Rusternetes only models the `v1beta1` flattened
   `DeviceClaim` shape (`dra.rs:48`).

10. **`policy/v1.Eviction` request kind** — the body shape for the
    `/api/v1/namespaces/{ns}/pods/{name}/eviction` subresource. The
    eviction handler exists in `crates/api-server/src/handlers/` (see
    PDB integration) but consumes an ad-hoc inline shape rather than a
    dedicated `pub struct Eviction { ... }`. Code that consumes the
    type from outside the api-server crate cannot share a definition.

11. **`external.metrics.k8s.io/v1beta1.ExternalMetricValueList`** —
    response shape for the External Metrics API (HPA external metric
    source). `MetricValueList` exists in `custom_metrics.rs:41` but is
    typed as a custom-metrics shape; the external-metrics counterpart
    has a different selector field layout.

12. **`PodCertificateRequest`** / `PodCertificateProjection` (alpha,
    KEP-4317) — neither the request kind nor the projection are
    modeled. The pod-level `volumes.projected.sources.podCertificate`
    projection exists upstream behind an alpha gate; Rusternetes'
    `VolumeProjection` (`pod.rs:919-937`) does not include this
    variant.

## Missing fields (recent KEPs adding fields to existing types)

Cross-reference: `docs/api-gap-analysis.md` carries the priority-ranked,
swagger-audited list. The entries below are the ones still tracked as
"Phase 4 — P3" or otherwise not closed in that audit, summarized for
quick scanning.

1. **`PodSpec.hostnameOverride`** (`pod.rs:72-221`) — alpha KEP. The
   field is documented as still-missing in
   `docs/api-gap-analysis.md` line 146.

2. **`PodSpec.schedulingGroup`** (`pod.rs:72-221`) — alpha
   `PodSchedulingGroup` group-based scheduling.
   `docs/api-gap-analysis.md` line 145.

3. **`Container.restartPolicyRules`** and
   **`EphemeralContainer.restartPolicyRules`** (`pod.rs:474`, `:374`) —
   alpha KEP-753 fine-grained container restart rules. Listed as Phase
   4 in the audit. The simpler `restartPolicy` field on
   Container/EphemeralContainer (sidecar pattern) is implemented at
   `pod.rs:474+` / `pod.rs:374+`.

4. **`PodStatus.allocatedResources`** (`map[string]Quantity`) and
   **`PodStatus.extendedResourceClaimStatus`** (`pod.rs:1198`) —
   `docs/api-gap-analysis.md` lines 184-186 (P3). The
   `ContainerStatus.allocatedResources` is implemented;
   the pod-level mirror is not.

5. **`VolumeProjection.podCertificate`** (`pod.rs:919-937`) — alpha
   per KEP-4317; no variant in the projection enum.

6. **`NodeSpec.externalID`** (deprecated, `node.rs:34`) — still part of
   upstream PodSpec for skew. Not modeled. Listed as P3 (deprecated).

7. **`NodeStatus.phase`** (deprecated, `node.rs:73`) — still emitted
   by some legacy controllers/clients. Not modeled.

8. **Legacy `PersistentVolumeSpec` volume backends** — `awsElasticBlockStore`,
   `azureDisk`, `azureFile`, `cinder`, `flexVolume`, `gcePersistentDisk`,
   `glusterfs`, `photonPersistentDisk`, `portworxVolume`, `quobyte`,
   `rbd`, `scaleIO`, `storageos`, `vsphereVolume`, `cephfs`,
   `flocker` — all deprecated upstream, none modeled in
   `volume.rs:21`. Modern PV creation in conformance tests does not
   exercise these; legacy import paths will fail.
   `docs/api-gap-analysis.md` Phase 4.

9. **`NodeSwapStatus` populated end-to-end** —
   `crates/common/src/resources/node.rs:222` defines the type, and
   `NodeSystemInfo.swap` is wired, but the kubelet does not populate
   it (covered in `kubelet.md`).

10. **`Job.spec.podFailurePolicy` / `Job.spec.successPolicy` typing** —
    both are `Option<serde_json::Value>` rather than typed structs
    (`workloads.rs:631,635`). Round-trips raw JSON; loses
    compile-time field validation and cannot drive controller logic
    without re-parsing.

11. **`NetworkPolicyPort.port` typing** — typed as
    `Option<serde_json::Value>` rather than `IntOrString`
    (`networking.rs:89`). Similar pattern to the Job fields above; the
    proper `IntOrString` enum exists at `policy.rs:276` and should be
    reused.

12. **`PersistentVolumeClaimCondition.r#type`** uses the raw escaped
    identifier (`volume.rs:365`); upstream `metav1.Condition` uses
    `Type` consistently. Newer code paths in Rusternetes use
    `Condition` from `crates/common/src/types.rs`; the PVC-specific
    condition type predates that and has not been migrated.

13. **`ServiceCIDR` lacks `Status` subresource constructor** —
    `servicecidr.rs:46` defines `ServiceCIDRStatus` but `new()` at
    `servicecidr.rs:18` constructs the object with `status: None` and
    no helper to initialize the `Ready` condition. Controllers must
    manually populate.

14. **`IPAddress` lacks `Status`** — `ipaddress.rs:8-15` has no
    `status` field at all; upstream defines an `IPAddressStatus` with
    `conditions` (1.34+).

15. **`PriorityClass.preemptionPolicy` typed as `Option<String>`** —
    `policy.rs:185`. Upstream uses a typed enum (`PreemptLowerPriority`,
    `Never`). Same string-typed-enum drift on `PodSpec.preemption_policy`
    (`pod.rs:192`) and `PodSpec.restart_policy` (`pod.rs:88`).

16. **`Toleration.operator` and `Toleration.effect` typed as
    `Option<String>`** (`pod.rs:1499-1523`) — upstream has typed enums.

17. **Condition time fields use `Option<String>` instead of
    `Option<DateTime<Utc>>`** —
    `policy.rs:341` (`PodDisruptionBudgetCondition.last_transition_time`),
    `servicecidr.rs:69-70` and
    `volume.rs:367-368`. The `chrono::DateTime<chrono::Utc>` pattern is
    used elsewhere (`workloads.rs:700`); inconsistent.

## Partial / stubbed

- **Event API duality.** `crates/common/src/resources/event.rs:122` is
  a "kitchen sink" type that contains every field from
  `core/v1.Event` plus every field from `events.k8s.io/v1.Event`. The
  `apiVersion` is fixed to `"v1"` at construction
  (`event.rs:228`). Round-tripping an `events.k8s.io/v1.Event` JSON
  blob through this type is lossy on `apiVersion` (becomes `"v1"`)
  and silently mixes deprecated and non-deprecated field names. A
  follow-up should split into two structs that share helper types.
- **`VolumeAttributesClass`** (`csi.rs:257-268`) — declared but
  inert. No `new()` constructor, no `TypeMeta` defaulting,
  `parameters` is `Option<HashMap>` (upstream is non-optional). The
  consumer-side wiring (PVCSpec.`volumeAttributesClassName`,
  PVCStatus.`currentVolumeAttributesClassName`,
  `modifyVolumeStatus`) is fully present, but the resource that holds
  the actual attributes set is a half-implementation.
- **`PodResourceClaim`** flattening was done in commit `1383ff8`
  (referenced in `docs/api-gap-analysis.md` line 152). The
  `ClaimSource` indirection was removed in favor of inline
  `resourceClaimName` / `resourceClaimTemplateName` on
  `PodResourceClaim` (`pod.rs:226-237`). Field-level OK; upstream
  retains `ClaimSource` for skew, so re-introducing it as an alias
  may be needed for older clients.
- **`CSIVolumeSource` (inline)** — both `pod.rs:855-908` (`Volume`)
  and `volume.rs:123-135` (`volume.rs` standalone) define `CSIVolumeSource`
  but the inline pod-volume variant on `Volume` may diverge from the
  PV-spec variant. `docs/api-gap-analysis.md` line 285 lists this as
  "Still missing" but earlier entries in the audit say it was added —
  worth a fresh field-level audit.
- **`ContainerStatus.user`** (`pod.rs:1662-1678`) — `ContainerUser`
  and `LinuxContainerUser` exist. Upstream adds a `WindowsContainerUser`
  shape that is not modeled.
- **`crd.rs::CustomResource`** (`crd.rs:550`) — present but its serde
  shape and the CRD validation pipeline are documented in
  `docs/CRD_IMPLEMENTATION.md`; not strictly an API-types gap.

## Known in-code TODOs / panics / suspect typings

- `grep -nE "TODO|FIXME|unimplemented|HACK|XXX"
  crates/common/src/resources/*.rs` returns **no hits** across the
  resource files at HEAD (`3e06b846`). The resource modules are
  notably free of leftover markers. Field-level gaps are tracked
  separately in `docs/api-gap-analysis.md`.
- `serde_json::Value` escape hatches indicate fields where the typed
  shape was not modeled. Three instances:
  - `workloads.rs:631` — `Job.spec.podFailurePolicy: Option<serde_json::Value>`
  - `workloads.rs:635` — `Job.spec.successPolicy: Option<serde_json::Value>`
  - `networking.rs:89` — `NetworkPolicyPort.port: Option<serde_json::Value>`
  These bypass Rust's type system and should be replaced with the
  upstream-equivalent typed structs (`PodFailurePolicy`,
  `SuccessPolicy`, `IntOrString`).
- `pod.rs:684` carries a deliberate comment about mutually exclusive
  fields and serialization edge cases — not a TODO, just a non-
  obvious invariant that future edits must preserve.
- `Event.extra: Option<HashMap<String, serde_json::Value>>`
  (`event.rs:212-213`) catches unknown fields via `#[serde(flatten)]`.
  Useful for conformance tolerance but masks genuine schema drift —
  worth periodically auditing what shows up here.
- Several `Condition` shapes still use `Option<String>` for time
  fields rather than `Option<DateTime<Utc>>` (see "Missing fields"
  items 12 and 17). The audit at `docs/api-gap-analysis.md` line 517
  flags this as a generic correction needed.
- `crd.rs:550` — `CustomResource` deserializes arbitrary JSON via
  `serde_json::Value`. Necessary for CRDs but means CRD field-level
  validation is enforced only by `schema_validation.rs`, not the
  type system.

## References

- Upstream API root:
  [kubernetes/kubernetes/staging/src/k8s.io/api](https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/api)
- `docs/api-gap-analysis.md` — running ledger of field-level drift,
  swagger-audited, priority-ranked. Source of truth for "what fields
  are missing" inside an already-modeled kind.
- `docs/ADVANCED_API_FEATURES.md` — narrative coverage of CEL,
  ValidatingAdmissionPolicy, SSA, etc.
- `docs/CRD_IMPLEMENTATION.md` — CRD validation and conversion
  pipeline.
- KEPs referenced inline:
  - KEP-127 user namespaces (`hostUsers`)
  - KEP-753 sidecar containers / container `restartPolicy`
  - KEP-2593 `ClusterCIDR`
  - KEP-3257 `ClusterTrustBundle`
  - KEP-3335 StatefulSet start ordinals
  - KEP-3521 PodSchedulingGate
  - KEP-3751 `VolumeAttributesClass`
  - KEP-3939 Job pod replacement policy
  - KEP-3960 coordinated leader election (`LeaseCandidate`)
  - KEP-3962 MutatingAdmissionPolicy
  - KEP-3998 Job success policy
  - KEP-4317 PodCertificateRequest / PodCertificateProjection
  - KEP-4368 Job `managedBy`
  - KEP-4444 Service `trafficDistribution`
  - KEP-4639 Image volume source
- Sibling per-module docs:
  - `docs/missing-features/api-server.md`
  - `docs/missing-features/controller-manager.md`
  - `docs/missing-features/kubelet.md`
  - `docs/missing-features/kubectl.md`
  - `docs/missing-features/scheduler.md`
  - `docs/missing-features/storage.md`
  - `docs/missing-features/kube-proxy.md`
  - `docs/missing-features/cloud-providers.md`
  - `docs/missing-features/cluster-bootstrap.md`
