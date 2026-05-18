# Missing Features — scheduler

Comparison of `crates/scheduler` (Rusternetes) against upstream Kubernetes
`cmd/kube-scheduler` + `pkg/scheduler/` (master, 2026-05). The Rusternetes
scheduler is a watch-driven loop that filters, scores, and binds one pod per
cycle; the framework abstraction in `framework.rs` is implemented but not
wired into the production binary.

## Scope

- In: pod-to-node scheduling decisions, filter/score plugins, preemption,
  binding, scheduler configuration, and the Scheduling Framework extension
  points.
- Out: the kubelet eviction manager, controller-manager descheduler-like
  loops, kube-controller-manager taint manager (covered by its own doc),
  cluster autoscaler (out-of-tree upstream).

## Current Rusternetes state

Files (line totals from `wc -l`):

- `crates/scheduler/src/scheduler.rs` — 1934 lines. The production
  scheduler. `Scheduler { storage, interval, scheduler_name }` struct
  (`scheduler.rs:17-22`). `run()` (`scheduler.rs:68`) installs a watch on
  `pods/*`, fans events into a `WorkQueue`, and a `worker()` task
  (`scheduler.rs:130`) calls `try_schedule_pod()` per key. The legacy
  per-cycle `schedule_pending_pods()` (`scheduler.rs:266`) is retained
  `#[allow(dead_code)]` for tests.
- `crates/scheduler/src/advanced.rs` — 1832 lines. All filter/score logic:
  `check_taints_tolerations` (`advanced.rs:16`), `check_node_affinity`
  (`advanced.rs:96`), `check_pod_affinity` (`advanced.rs:130`),
  `check_pod_anti_affinity` (`advanced.rs:175`),
  `check_host_port_conflicts` (`advanced.rs:429`),
  `calculate_resource_score_with_pods` (`advanced.rs:536`),
  `check_topology_spread_constraints` (`advanced.rs:1226`),
  `check_preemption_with_pdbs` (`advanced.rs:826`).
- `crates/scheduler/src/framework.rs` — 652 lines. Defines the upstream
  extension-point traits (`PreFilterPlugin`, `FilterPlugin`,
  `PostFilterPlugin`, `PreScorePlugin`, `ScorePlugin`, `ReservePlugin`,
  `PermitPlugin`, `PreBindPlugin`, `BindPlugin`, `PostBindPlugin`) plus a
  `PluginRegistry`, `CycleState`, `FrameworkHandle`, and `Framework`
  orchestrator. Marked `#[allow(dead_code)]` at `lib.rs:3` and `main.rs:3`
  because it is not used by the binary.
- `crates/scheduler/src/plugins.rs` — 508 lines. Built-in plugin wrappers
  (`NodeUnschedulablePlugin`, `TaintTolerationPlugin`, `NodeSelectorPlugin`,
  `NodeAffinityPlugin`, `PodAffinityPlugin`, `PodAntiAffinityPlugin`,
  `TopologySpreadConstraintsPlugin`, `HostPortPlugin`,
  `NodeResourcesFitPlugin`, four `*ScoringPlugin` variants). Returned by
  `get_default_plugins()` (`plugins.rs:464`). Also `#[allow(dead_code)]`.
- `crates/scheduler/src/main.rs` — 166 lines. CLI binary entry point with
  optional etcd leader election (`main.rs:115`).
- `crates/scheduler/src/lib.rs` — 28 lines. All-in-one embedding entry
  point.

What the production path does inside `select_node()` (`scheduler.rs:572`):
filter for `spec.unschedulable=false`, taints/tolerations, node selector,
DRA device availability (`scheduler.rs:1242`), host-port conflicts, node
affinity, pod affinity / anti-affinity, topology-spread, then score with a
hard-coded weighted sum (`scheduler.rs:724-729`):

- resource (25%) + node-affinity (20%) + pod-affinity (18%)
- priority (15%) − anti-affinity penalty (12%) − topology penalty (10%)

Preemption (`try_preempt`, `scheduler.rs:952`) uses the PDB-aware
`check_preemption_with_pdbs` (`advanced.rs:826`) with the upstream
"remove all, then reprieve" algorithm and immediate re-bind to the nominated
node when freed resources fit.

## Parity matrix

Per upstream plugin / cross-cutting feature; “Rusternetes status” is one of
`done`, `partial`, `missing`.

| Upstream component | Upstream package | Rusternetes status | Notes |
| --- | --- | --- | --- |
| NodeUnschedulable (Filter) | `plugins/nodeunschedulable` | done | `plugins.rs:78`; production path uses inline check `scheduler.rs:589-597`. |
| TaintToleration (Filter+Score) | `plugins/tainttoleration` | partial | Filter at `advanced.rs:16`. NoExecute eviction lives in controller-manager, not scheduler. No PreferNoSchedule scoring (`advanced.rs:51` returns `true` and discards score). |
| NodeName (Filter) | `plugins/nodename` | missing | No early-return when `spec.nodeName` is set; Rusternetes assumes a populated `nodeName` means already scheduled (`scheduler.rs:193-200`). Doesn't validate the named node exists or matches constraints. |
| NodeAffinity (Filter+Score) | `plugins/nodeaffinity` | partial | `requiredDuringScheduling` + `preferredDuringScheduling` In/NotIn/Exists/DoesNotExist/Gt/Lt all implemented (`advanced.rs:259-313`). `matchFields` limited to `metadata.name` + `metadata.namespace` (`advanced.rs:316-322`). No `RequiredDuringSchedulingRequiredDuringExecution` (long-deprecated alpha). |
| NodePorts / HostPort (Filter) | `plugins/nodeports` | done | `advanced.rs:429-527` with wildcard hostIP overlap detection. |
| NodeResourcesFit (Filter+Score) | `plugins/noderesources` | partial | Filter + LeastAllocated scoring at `advanced.rs:530-740`. Pod overhead handled (`scheduler.rs:1082`). Missing: MostAllocated, RequestedToCapacityRatio, scoring-strategy configuration, per-resource weights, separate handling for huge-pages and ephemeral-storage usage tracking (`advanced.rs:615-625` skips `ephemeral-storage`). |
| NodeResourcesBalancedAllocation (Score) | `plugins/noderesources` | missing | No CPU/memory balance heuristic. Single `LeastAllocated` style mean of `cpu_score` and `memory_score` is used. |
| ImageLocality (Score) | `plugins/imagelocality` | missing | No reading of `node.status.images` to bias toward nodes with the pod's images pre-pulled. |
| InterPodAffinity (Filter+Score) | `plugins/interpodaffinity` | partial | `check_pod_affinity` / `check_pod_anti_affinity` at `advanced.rs:130-222`. `matches_pod_affinity_term` (`advanced.rs:375-423`) has a known TODO at `advanced.rs:416`: it does NOT verify the matching pod is on a node with the same topology-key value. No `namespaceSelector`, no `matchLabelKeys`/`mismatchLabelKeys` (KEP-3633). Hard required terms always treated as conflicts on any matching pod, which over-rejects. |
| PodTopologySpread (Filter+Score) | `plugins/podtopologyspread` | partial | `check_topology_spread_constraints` at `advanced.rs:1226`. Implements `maxSkew` + `topologyKey` + `whenUnsatisfiable`. Missing: `minDomains` (KEP-3022), `nodeAffinityPolicy` and `nodeTaintsPolicy` (KEP-3094), `matchLabelKeys` (KEP-3243), defaulting constraints from `kube-system` defaults. |
| VolumeBinding (Filter+Score+Reserve+PreBind) | `plugins/volumebinding` | missing | No PV provisioning/binding logic in the scheduler. Storage classes, volume topology, `WaitForFirstConsumer`, late binding, capacity tracking (KEP-1472) — none of it is consulted. |
| VolumeRestrictions (Filter) | `plugins/volumerestrictions` | missing | Pods with single-writer block-mode volumes can stack on the same node without error. |
| VolumeZone (Filter) | `plugins/volumezone` | missing | No `topology.kubernetes.io/zone` matching between PV labels and node labels. |
| EBSLimits / AzureDiskLimits / GCEPDLimits / CSILimits (Filter) | `plugins/nodevolumelimits` | missing | No per-CSI-driver / per-cloud volume-attach-limit enforcement. |
| DefaultBinder (Bind) | `plugins/defaultbinder` | partial | `bind_pod_to_node` (`scheduler.rs:795`) writes `spec.nodeName` directly via `storage.update` instead of issuing a `POST /api/v1/namespaces/{ns}/pods/{name}/binding` against the apiserver. This bypasses the proper Binding subresource. |
| DefaultPreemption (PostFilter) | `plugins/defaultpreemption` | partial | `check_preemption_with_pdbs` at `advanced.rs:826` implements remove-all-then-reprieve + PDB ordering. Missing: cross-node dry-run that picks the best candidate node by victim count/priority/age; `nominatedNodeName` is set per-pod but the legacy queue does not snapshot the nominated node into the scheduling cache for subsequent cycles. |
| DynamicResources (DRA) | `plugins/dynamicresources` | partial | `check_dra_device_availability` (`scheduler.rs:1242`) verifies an allocated `ResourceClaim`’s node selector and looks up a `ResourceSlice`. No structured-parameter selection (KEP-3902), no per-node device picking by the scheduler (KEP-4381), no `ResourceClaimTemplate` instantiation — see TODO at `scheduler.rs:1267`. |
| SchedulingGates | `plugins/schedulinggates` | missing | `spec.schedulingGates` is never inspected; gated pods are scheduled immediately, violating KEP-3521 semantics. |
| SelectorSpread (legacy Score) | `plugins/selectorspread` | n/a | Deprecated in favor of `PodTopologySpread`; intentionally omitted upstream by default since v1.32. |
| Extender webhooks | `pkg/scheduler/extender` | missing | No HTTP extender support; scheduler config does not accept `--config` profile with extenders. |
| MultiPoint plugin config | `KubeSchedulerConfiguration` v1 | missing | Plugin set is hard-coded in `get_default_plugins()` (`plugins.rs:464`); no `KubeSchedulerConfiguration` parsing, no scheduler profiles. |
| Multiple scheduler profiles | KEP-1451 | missing | Single `scheduler_name` per process. To run a second scheduler one must run a second binary. |
| Scheduling queue (active/backoff/unschedulable) | `pkg/scheduler/internal/queue` | partial | Uses `WorkQueue` from `rusternetes_storage`; rate-limited re-queue exists (`scheduler.rs:139`), but the three-queue split, per-pod backoff with exponential ceiling, and unschedulable→active movement on relevant cluster events are absent. |
| EnqueueExtensions / ClusterEventsForPlugin | `pkg/scheduler/framework` | missing | Watch is keyed on `pods/*` only. New/updated nodes, pvcs, services, storage-classes do not nudge unschedulable pods. A failing pod re-enqueues on a fixed rate-limited delay rather than on the relevant cluster event. |
| Scheduler snapshot/cache | `pkg/scheduler/internal/cache` | missing | Each scheduling decision re-runs `storage.list("nodes")` + `storage.list("pods")` (`scheduler.rs:219-223`). No in-memory snapshot, no incremental updates. |
| NominatedNodeName | KEP-3094 | partial | Set after preemption (`scheduler.rs:476-481`). Not used as an early-bind hint on subsequent cycles; immediate re-bind path mitigates the gap but bypasses proper status reconciliation. |
| Pod priority (PriorityClass resolution) | `priority-and-fairness` | done | `load_priority_classes` + `get_pod_priority_sync` (`scheduler.rs:1065-1236`). |
| Leader election | `client-go/leaderelection` | partial | etcd-based via `LeaderElector` (`main.rs:115-158`); only works against the etcd backend. SQLite/Redis backends silently downgrade to single-instance. |
| Bind via apiserver `/binding` subresource | `client-go bindings` | missing | Writes go straight to storage; the api-server has no `/binding` handler invoked by the scheduler. |
| Scheduler observability — `scheduler_pending_pods`, `scheduler_pod_scheduling_duration_seconds`, `scheduling_attempt_duration_seconds`, `scheduler_unschedulable_pods` | `pkg/scheduler/metrics` | partial | `metrics_port` is opened (`main.rs:108-112`) with a generic `MetricsRegistry::with_scheduler_metrics`. The standard upstream metric names/labels are not all present. |

## Missing features (detailed)

### 1. Scheduling Framework not wired in

`framework.rs` defines the entire plugin lifecycle (PreFilter → Filter →
PostFilter → PreScore → Score → NormalizeScore → Reserve → Permit → PreBind
→ Bind → PostBind) and `plugins.rs` builds a registry of typed plugin
implementations, but `main.rs:144,161` constructs `Scheduler::new(...)` and
calls the production `run()` path, which uses the inline `select_node()` in
`scheduler.rs:572`. Both `lib.rs:3-5` and `main.rs:3-5` annotate the
modules `#[allow(dead_code)]`. Consequence: there is no extension surface;
adding a new policy requires editing `select_node()` directly. The
Framework's `run_scheduling_cycle()` (`framework.rs:424`) even acknowledges
the gap — "PostFilter plugin {} succeeded, but re-filtering not yet
implemented" (`framework.rs:472`).

### 2. No VolumeBinding plugin or PV-aware scheduling

The scheduler never inspects `spec.volumes[].persistentVolumeClaim`, the
referenced PVC, the PV's `nodeAffinity`, or the storage class's
`volumeBindingMode`. A pod that requires a zonal EBS PV will be scheduled
to any node and the kubelet will fail to mount. Upstream's
`pkg/scheduler/framework/plugins/volumebinding` runs a PreFilter that
builds a `BindingInfo` set, a Filter that rejects nodes whose topology
can't satisfy the PV, a Reserve that holds the binding, and a PreBind that
fires the actual PVC/PV binding API call. None of this exists.

### 3. SchedulingGates ignored

Pods with `spec.schedulingGates` set should remain `Pending`/Unschedulable
until all gates are removed (KEP-3521 / v1.30+). Rusternetes treats them
as fully schedulable; `try_schedule_pod` (`scheduler.rs:179`) only checks
nodeName, pending-phase, and `schedulerName`. There is no early return for
gates, no PreEnqueue plugin equivalent.

### 4. No scheduler queue with backoff

Upstream's queue (`pkg/scheduler/internal/queue/scheduling_queue.go`) has
three sub-queues: active, podBackoffQ (failed once, backing off), and
unschedulableQ (failed and waiting on cluster events). Backoff is per-pod,
exponential with a 10s start and 10m ceiling. Cluster events
(NodeAdd, NodeUpdate, PvAdd, etc.) move pods from unschedulableQ back to
active. Rusternetes uses a single `WorkQueue` with `requeue_rate_limited`
(`scheduler.rs:139`). It will re-attempt an unschedulable pod every few
seconds forever, generating churn against storage and the api-server. It
also won't react quickly when a new node is added — the next attempt
fires on the rate-limited interval, not on the NodeAdd event.

### 5. No EnqueueExtensions / event-driven re-queuing

Each plugin upstream declares the cluster events it cares about via
`EnqueueExtensions.EventsToRegister`. The scheduler watches all those event
types and selectively re-queues only the affected pods. Rusternetes
watches `pods/*` only (`scheduler.rs:88`). Adding capacity (NodeAdd),
deleting a blocking pod (PodDelete), updating a storage class — none
re-enqueue waiting pods on the event itself, so scheduling latency is
bounded only by the resync interval.

### 6. Binding goes through storage, not `/binding` subresource

`bind_pod_to_node` (`scheduler.rs:795`) calls `self.storage.update(&key,
&pod)` with the full pod object, mutating `spec.nodeName` and rewriting
`status.conditions`. Upstream issues `POST /api/v1/namespaces/{ns}/pods/{n}/binding`
with a `Binding` object — the apiserver enforces that `spec.nodeName` can
only be set this way (the apiserver rejects direct pod updates to
`spec.nodeName` for non-system clients). Bypassing the subresource means
the apiserver's binding admission (storage validation, audit) is skipped.

### 7. No KubeSchedulerConfiguration / profiles

Upstream parses `--config kube-scheduler.yaml` to load a
`KubeSchedulerConfiguration` with multiple `profiles[]`, each with its own
`plugins` block (enable/disable/weight), `pluginConfig`, and
`schedulerName`. This is how operators run, e.g., a `gang-scheduler` and a
`default-scheduler` in one binary. Rusternetes only has CLI flags
(`main.rs:22-62`) and a hard-coded plugin set. No per-pod profile lookup.

### 8. No scheduler cache snapshot

Each `try_schedule_pod` call (`scheduler.rs:179`) issues two `list()`
calls against storage — all pods and all nodes (`scheduler.rs:219,223`).
Upstream maintains an in-memory `Snapshot` of all node info plus the
pods that are scheduled there, kept current via the
`SharedInformerFactory`. The snapshot lets each scheduling cycle run
filter/score in pure CPU without hitting storage. On a 5,000-pod cluster
the current implementation will saturate storage long before it saturates
the scheduler.

### 9. Pod affinity does not check topology-key match

The TODO at `advanced.rs:416` is load-bearing: `matches_pod_affinity_term`
finds any pod matching the label selector and returns `true` without
verifying it lives on a node whose topology-key value equals the candidate
node's topology-key value. Pod affinity therefore behaves like a global
"is there any matching pod" test, not the per-topology-domain test
specified by the API.

### 10. DRA: structured parameters and per-node selection

`check_dra_device_availability` (`scheduler.rs:1242`) only verifies an
already-allocated `ResourceClaim`. Upstream's `dynamicresources` plugin
implements the full structured-parameter model from KEP-3902 (the
scheduler picks the device, writes the allocation result back to the
claim) and KEP-4381 (per-node devices selectable by the scheduler). Even
`ResourceClaimTemplate` instantiation is stubbed (`scheduler.rs:1267`).

### 11. No extender webhook support

`pkg/scheduler/extender/http.go` lets operators wire external HTTP
filters/scorers via `KubeSchedulerConfiguration.extenders[]`. Rusternetes
has no extender support and no configuration surface to add one.

### 12. Multiple profiles / multiple `schedulerName` values in one process

Rusternetes stores a single `scheduler_name` on the struct
(`scheduler.rs:21`) and rejects pods whose `spec.schedulerName` differs
(`scheduler.rs:214`). To run multiple schedulers you must launch multiple
binary instances, each with their own etcd leader-election lock.

## Partial / stubbed

- TaintToleration: `PreferNoSchedule` is tolerated unconditionally
  (`advanced.rs:52-53`) instead of contributing a preferred-score penalty.
  NoExecute-time-based eviction is not handled in the scheduler (it lives
  in the controller-manager taint-manager — that's correct, but the
  upstream `tainttoleration` Score plugin still scores NoExecute
  tolerations).
- NodeAffinity: `matchFields` operators are restricted to `metadata.name`
  and `metadata.namespace` (`advanced.rs:316-322`). Upstream uses generic
  field-path lookup against the Node object.
- PodTopologySpread: missing `minDomains` (KEP-3022),
  `nodeAffinityPolicy`/`nodeTaintsPolicy` (KEP-3094),
  `matchLabelKeys`/`mismatchLabelKeys` (KEP-3243), defaulting global
  constraints, ignoring terminating pods consistently with the upstream
  filter.
- DefaultPreemption: `check_preemption_with_pdbs` (`advanced.rs:826`)
  implements per-node remove-all-then-reprieve with PDB ordering, but
  there is no inter-node candidate ranking (upstream picks the node where
  preemption evicts the fewest highest-priority victims). `try_preempt`
  (`scheduler.rs:974`) iterates nodes in storage-list order and returns
  the first that works.
- DefaultBinder: writes `spec.nodeName` directly to storage rather than
  issuing the Binding subresource POST.
- Scheduler queue: rate-limited single queue, no active/backoff/unsched
  split, no per-event re-queue.
- NodeResourcesFit: only one strategy (LeastAllocated), no per-resource
  weights, no `RequestedToCapacityRatio`, no `MostAllocated`, no
  ephemeral-storage tracking in pod-usage accounting
  (`advanced.rs:615-625`).
- Leader election: works only when storage backend is etcd (`main.rs:136`).
  SQLite/Redis users have no HA story; the leader-election crate path
  silently drops back to single-instance.

## Known in-code TODOs

- `crates/scheduler/src/advanced.rs:416` —
  `// TODO: Check if the pod is on a node with matching topology value`.
  Affects every InterPodAffinity decision.
- `crates/scheduler/src/scheduler.rs:1267` — `ResourceClaimTemplate`
  instantiation is faked by using the template name as the claim name.

## References

- Upstream sources (compared against `kubernetes/kubernetes`, master,
  2026-05):
  - `cmd/kube-scheduler/` — binary entrypoint and CLI flags.
  - `pkg/scheduler/framework/` — Framework runtime, extension-point
    interfaces, cycle state.
  - `pkg/scheduler/framework/plugins/` — every built-in plugin
    (`nodeaffinity`, `nodeports`, `nodename`, `noderesources`,
    `nodeunschedulable`, `nodevolumelimits`, `podtopologyspread`,
    `tainttoleration`, `interpodaffinity`, `imagelocality`,
    `volumebinding`, `volumerestrictions`, `volumezone`, `defaultbinder`,
    `defaultpreemption`, `dynamicresources`, `schedulinggates`,
    `selectorspread`).
  - `pkg/scheduler/internal/cache/` — Snapshot/Cache.
  - `pkg/scheduler/internal/queue/` — Active/backoff/unschedulable queue.
  - `pkg/scheduler/extender/` — Extender webhook client.
  - `staging/src/k8s.io/kube-scheduler/config/` —
    `KubeSchedulerConfiguration` v1 schema.
- Relevant KEPs: 624 (Framework), 1451 (multiple profiles), 1472 (CSI
  capacity tracking), 3022 (minDomains), 3094 (nominatedNodeName,
  nodeAffinityPolicy/nodeTaintsPolicy), 3243 (matchLabelKeys for spread),
  3521 (PodSchedulingReadiness / SchedulingGates), 3633 (matchLabelKeys
  for PodAffinity), 3902 (DRA structured parameters), 4381 (DRA per-node
  selection).
- Rusternetes sources cited above with absolute path
  `crates/scheduler/src/{scheduler,advanced,framework,plugins,main,lib}.rs`.
