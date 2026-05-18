# Missing Features — kubelet

## Scope

This document compares `crates/kubelet/` (≈21k LOC across 8 files) against
upstream `cmd/kubelet/` + `pkg/kubelet/*` (Go, ≈150k LOC across ~40
sub-packages). It enumerates kubelet capabilities that Kubernetes ships and
that Rusternetes either omits, stubs, or implements only partially.

The kubelet is the single largest crate in the workspace and also the one with
the largest delta against upstream — many of the gaps listed below would be
multi-week or multi-month projects in their own right (CRI, device plugins,
CPU/memory/topology managers, the SPDY/WebSocket streaming server, the volume
manager plugin model).

Only Linux-relevant gaps are listed. Windows-only kubelet code
(`pkg/kubelet/winstats/`, `pkg/kubelet/cm/cpumanager/topology/...`) is out of
scope.

## Current Rusternetes state (cite path:line)

Source files in `crates/kubelet/src/`:

| File                            | LOC    | Purpose                                                                                                  |
|---------------------------------|-------:|----------------------------------------------------------------------------------------------------------|
| `runtime.rs`                    | 12094  | Container lifecycle over bollard/Docker; volumes; probes (exec/HTTPGet/TCPSocket/gRPC); CNI orchestration |
| `kubelet.rs`                    |  4732  | Pod state machine (SyncPod / TerminatingPod / TerminatedPod), per-pod workers, sync loop, GC, node reg.   |
| `eviction.rs`                   |   850  | EvictionSignals (Memory, NodeFs, PIDs), thresholds (soft+hard), QoS ordering                              |
| `config.rs`                     |   563  | `KubeletConfiguration` v1beta1 (subset)                                                                   |
| `cni/`                          |  ~1100 | CNI v1.0.0 spec impl: config, plugin, result, runtime                                                    |
| `lifecycle.rs`                  |   379  | preStop timing; grace period accounting; `MINIMUM_GRACE_PERIOD_SECS = 2`                                 |
| `downward_api.rs`               |   358  | `fieldRef` + `resourceFieldRef` resolution                                                               |
| `main.rs`                       |   342  | Binary entrypoint; tiny `/metrics`, `/configz`, `/exec/:container_id` HTTP server                        |

Key citations:

- Pod state machine — `crates/kubelet/src/kubelet.rs:34-41` (`PodWorkerState`
  enum: `SyncPod | TerminatingPod | TerminatedPod`).
- Per-pod worker dispatch — `crates/kubelet/src/kubelet.rs:1166-1273`
  (`ensure_pod_worker`).
- In-place vertical resize (KEP-1287) — partial; status flow recognised at
  `crates/kubelet/src/kubelet.rs:2431-2509`.
- Node Lease heartbeat (since K8s 1.14) — implemented at
  `crates/kubelet/src/kubelet.rs:219-310`.
- CSI ephemeral inline volume — **placeholder only**, see
  `crates/kubelet/src/runtime.rs:3083-3092`.
- SA token projection — fallback placeholder if generation fails,
  `crates/kubelet/src/runtime.rs:3467-3470`.
- ImageFs eviction signal — recognised but stat returns `0.0`, see
  `crates/kubelet/src/eviction.rs:213-214` (`// Not implemented`).
- Privileged / capabilities are parsed but only `privileged` is forwarded to
  Docker — `crates/kubelet/src/runtime.rs:4818-4829`.
- Cluster DNS discovery via `kube-dns` Service —
  `crates/kubelet/src/main.rs:165-189`.
- HTTP API surface is `/metrics`, `/configz`, `/exec/:container_id` only —
  `crates/kubelet/src/main.rs:223-234`. No `/attach`, no `/portForward`, no
  `/stats/summary`, no `/pods`, no `/healthz`, no `/run`, no
  `/containerLogs/`.
- `runtime_class_name` ignored — set to `None` in every constructed Pod in the
  crate, e.g. `crates/kubelet/src/runtime.rs:8549,9822,10008,10713`.
- Only one in-tree CSI usage is the placeholder noted above; no
  `plugins_registry` discovery, no plugin manager, no node staging /
  publishing.

The only in-source `TODO` is at `crates/kubelet/src/kubelet.rs:827`
("Remove once per-pod workers are proven stable").

## Parity matrix

Symbols: ✓ implemented, ◐ partial / stubbed, ✗ missing.

| Area / feature                                                  | Upstream package                          | Rust | Notes (path:line where useful)                                                          |
|-----------------------------------------------------------------|-------------------------------------------|:----:|------------------------------------------------------------------------------------------|
| CRI (Container Runtime Interface, gRPC to containerd/CRI-O)     | `pkg/kubelet/cri/`                        |  ✗   | Direct bollard/Docker calls; dockershim removed upstream in 1.24                         |
| Multi-runtime selection via RuntimeClass.handler                 | `pkg/kubelet/runtimeclass/`               |  ✗   | `runtime_class_name: None` everywhere (`runtime.rs:8549`)                                |
| Image manager (registry, deduped pulls, parallel limit)         | `pkg/kubelet/images/`                     |  ◐   | Per-image pull + retry in `runtime.rs:873-911`; no concurrency cap, no cache index       |
| Image GC (LRU on disk usage)                                    | `pkg/kubelet/images/image_gc_manager.go`  |  ◐   | Container GC only (`kubelet.rs:1127`); image LRU eviction absent                         |
| Pod state machine (sync/terminating/terminated)                 | `pkg/kubelet/pod_workers.go`              |  ✓   | `kubelet.rs:34-41`                                                                       |
| Per-pod goroutine / worker                                      | `pkg/kubelet/pod_workers.go`              |  ✓   | mpsc per UID, `kubelet.rs:1166-1273`                                                     |
| Node Lease heartbeat (coordination.k8s.io/v1)                   | `pkg/kubelet/nodelease/`                  |  ✓   | `kubelet.rs:219-310`                                                                     |
| Node registration                                               | `pkg/kubelet/kubelet_node_status.go`      |  ✓   | `kubelet.rs:373-478`                                                                     |
| Container lifecycle (PreStart/PostStart/PreStop)                | `pkg/kubelet/lifecycle/`                  |  ◐   | preStop done; postStart partial (exec/http); no PreStart hooks at the API tier           |
| Probes — startup / readiness / liveness (exec/http/tcp/grpc)    | `pkg/kubelet/prober/`                     |  ✓   | `runtime.rs:6395-6953`                                                                   |
| In-place pod vertical scaling (KEP-1287)                        | `pkg/kubelet/kuberuntime/`                |  ◐   | Resize Proposed→InProgress→cleared, `kubelet.rs:2431-2509`; no `Deferred`/`Infeasible`   |
| Pod resize policy (`resizePolicy: NotRequired/RestartContainer`) | KEP-1287                                  |  ✗   | `resize_policy: None` (`eviction.rs:755`)                                                |
| Ephemeral containers (`/ephemeralcontainers` exercised)         | `pkg/kubelet/kuberuntime/`                |  ◐   | Status reported (`runtime.rs:5995`); container creation path not wired                   |
| Sidecar containers (init w/ `restartPolicy: Always`, KEP-753)   | `pkg/kubelet/container/`                  |  ✓   | Init ordering w/ restartable inits handled                                               |
| Init container ordering / restart                               | `pkg/kubelet/kuberuntime/`                |  ✓   | `decide_next_init_action`, `runtime.rs:457`                                              |
| Termination grace period + `MINIMUM_GRACE_PERIOD_SECS`          | `pkg/kubelet/kuberuntime/kuberuntime_container.go` | ✓ | `lifecycle.rs:31`                                                                  |
| Pod overhead (RuntimeClass.overhead) deduction                  | `pkg/kubelet/cm/`                         |  ✗   | Spec field ignored; cgroup sizing uses raw requests/limits                                |
| QoS class (`Guaranteed/Burstable/BestEffort`)                   | `pkg/kubelet/qos/`                        |  ✓   | `kubelet.rs:3862`                                                                        |
| Eviction signals — Memory                                       | `pkg/kubelet/eviction/`                   |  ✓   | `eviction.rs`                                                                            |
| Eviction signals — NodeFs avail / inodes                        | `pkg/kubelet/eviction/`                   |  ✓   | `eviction.rs`                                                                            |
| Eviction signals — PIDs                                         | `pkg/kubelet/eviction/`                   |  ✓   | `eviction.rs`                                                                            |
| Eviction signals — ImageFs avail / inodes                       | `pkg/kubelet/eviction/`                   |  ◐   | Enum present, stats hard-coded to `0.0` — `eviction.rs:213-214`                          |
| Eviction signals — ContainerFs                                  | `pkg/kubelet/eviction/`                   |  ✗   | Absent                                                                                   |
| Eviction signals — Allocatable enforcement                      | `pkg/kubelet/eviction/`                   |  ✗   | No node-allocatable cgroup tree                                                          |
| Soft + hard eviction with grace                                 | `pkg/kubelet/eviction/`                   |  ✓   | `eviction.rs:36-46`                                                                      |
| QoS-ordered eviction (BestEffort→Burstable→Guaranteed)          | `pkg/kubelet/eviction/`                   |  ✓   | `eviction.rs:58-66`                                                                      |
| Container Manager — CPU manager (static, distribute-across-numa)| `pkg/kubelet/cm/cpumanager/`              |  ✗   | No cpuset pinning, no NUMA awareness                                                     |
| Container Manager — Memory manager (KEP-1769)                   | `pkg/kubelet/cm/memorymanager/`           |  ✗   | Absent                                                                                   |
| Container Manager — Topology manager (KEP-693)                  | `pkg/kubelet/cm/topologymanager/`         |  ✗   | Absent                                                                                   |
| Container Manager — Device manager / device plugins (KEP-368)   | `pkg/kubelet/cm/devicemanager/`           |  ✗   | No gRPC listsocket, no resource discovery, no resource allocation                        |
| Container Manager — DRA driver (KEP-3063 / 4381)                | `pkg/kubelet/cm/dra/`                     |  ✗   | Absent                                                                                   |
| Container Manager — node shutdown handler (KEP-2000, systemd inhibit) | `pkg/kubelet/cm/nodeshutdown/`     |  ✗   | Absent — kubelet does not block shutdown or pre-drain pods                               |
| Container Manager — cgroup driver (`cgroupfs`/`systemd`)        | `pkg/kubelet/cm/`                         |  ✗   | Cgroups are whatever Docker / bollard sets up                                            |
| Hugepages allocation + downward API                             | `pkg/kubelet/cm/`                         |  ◐   | DownwardAPI scales hugepages units (`downward_api.rs:129`); no allocation enforcement    |
| Swap support (KEP-2400 swap accounting, KEP-3673 swap on)       | `pkg/kubelet/cm/`                         |  ✗   | `memory_swap = memory` hard-set (`runtime.rs:8061-8070`)                                 |
| User namespaces (KEP-127)                                       | `pkg/kubelet/userns/`                     |  ✗   | Absent                                                                                   |
| Pod Security — seccomp                                          | `pkg/kubelet/`                            |  ✗   | `seccomp_profile: None` everywhere (`runtime.rs:9777,9998`)                              |
| Pod Security — AppArmor                                         | `pkg/kubelet/`                            |  ✗   | No `apparmor.security.beta.kubernetes.io/*` enforcement                                  |
| Pod Security — Capabilities add/drop                            | `pkg/kubelet/`                            |  ◐   | Read at `runtime.rs:4818-4823` but not threaded to bollard create call                   |
| Pod Security — privileged                                       | `pkg/kubelet/`                            |  ◐   | Forwarded to Docker (`runtime.rs:4826-4829`); no admit-side enforcement                  |
| Pod Security — `runAsUser/runAsGroup/runAsNonRoot/fsGroup`      | `pkg/kubelet/`                            |  ◐   | fsGroup applied to emptyDir; non-root admit absent                                       |
| Sysctls (safe/unsafe whitelist)                                 | `pkg/kubelet/sysctl/`                     |  ◐   | Forwarded to Docker (`runtime.rs:1664-1725`); no whitelist gate                          |
| Volume manager loop / desired+actual world reconciliation       | `pkg/kubelet/volumemanager/`              |  ✗   | Direct per-pod mount in `runtime.rs:create_pod_volumes`; no `populator + reconciler`     |
| CSI plugin discovery (`/var/lib/kubelet/plugins_registry`)      | `pkg/kubelet/pluginmanager/`              |  ✗   | No socket discovery, no `NodeRegistrar` handshake                                        |
| CSI volume staging (`NodeStageVolume` / `NodePublishVolume`)    | `pkg/kubelet/volumemanager/reconciler/`   |  ✗   | Placeholder dir only (`runtime.rs:3083-3092`)                                            |
| Block-mode volumes (`volumeDevices`)                            | `pkg/kubelet/volumemanager/`              |  ✗   | Filesystem mode only                                                                     |
| FlexVolume                                                       | `pkg/volume/flexvolume/`                  |  ✗   | Absent                                                                                   |
| Network volume plugins (iSCSI/RBD/NFS/Cinder/Glusterfs)         | `pkg/volume/*`                            |  ✗   | `iscsi: None` (`runtime.rs:1020`); only HostPath / EmptyDir / PVC→HostPath used          |
| CSI inline ephemeral (KEP-596)                                  | `pkg/kubelet/volumemanager/`              |  ◐   | Placeholder, see above                                                                   |
| GenericEphemeral volumes (KEP-1698, auto-PVC from template)     | `pkg/kubelet/volumemanager/`              |  ✓   | `runtime.rs:3098-3100`                                                                   |
| Projected volumes (SA token, downwardAPI, configMap, secret)    | `pkg/volume/projected/`                   |  ✓   | Implemented; SA token has placeholder fallback                                           |
| SA token projection w/ audience + expiration (TokenRequest API) | `pkg/kubelet/token/`                      |  ◐   | Token generated via TokenRequest; falls back to literal placeholder on failure           |
| ConfigMap + Secret atomic write w/ symlink swap                 | `pkg/volume/configmap/` `secret/`         |  ◐   | Files written; no atomic-rotate symlink dance                                            |
| HostPath with `type:` enforcement                               | `pkg/volume/host_path/`                   |  ✓   | `check_host_path_type` (`runtime.rs:259`)                                                |
| EmptyDir (memory medium / sizeLimit)                            | `pkg/volume/empty_dir/`                   |  ◐   | Disk EmptyDir only; `medium: Memory` and `sizeLimit` not enforced                        |
| DNS — ClusterFirst                                              | `pkg/kubelet/network/dns/`                |  ✓   | `runtime.rs:9724`                                                                        |
| DNS — ClusterFirstWithHostNet                                   | `pkg/kubelet/network/dns/`                |  ✓   | `runtime.rs:9742`                                                                        |
| DNS — `dnsConfig` (nameservers / searches / options)            | `pkg/kubelet/network/dns/`                |  ✓   | `runtime.rs:9611-9689`                                                                   |
| Pod `hostname` / `subdomain` (FQDN, `/etc/hosts`)               | `pkg/kubelet/kuberuntime/`                |  ✓   | `runtime.rs:1497-1530`                                                                   |
| CNI v1.0.0                                                      | `pkg/kubelet/dockershim/network/cni/`     |  ✓   | `crates/kubelet/src/cni/`                                                                |
| Network namespace (pause-container model)                       | `pkg/kubelet/kuberuntime/`                |  ✓   | `start_pause_container` (`runtime.rs:1531`)                                              |
| Stats — `/stats/summary` (cAdvisor-style)                       | `pkg/kubelet/server/stats/`               |  ✗   | Only `/metrics` (Prometheus) is served                                                   |
| HTTP /healthz, /pods, /logs, /run, /containerLogs/              | `pkg/kubelet/server/`                     |  ✗   | None served                                                                              |
| HTTP /exec (SPDY)                                               | `pkg/kubelet/server/`                     |  ◐   | A `/exec/:container_id` POST exists but is JSON-batched, not streaming                   |
| HTTP /exec, /attach, /portforward over WebSockets (KEP-4006)    | `pkg/kubelet/cri/streaming/`              |  ✗   | Absent                                                                                   |
| HTTP /attach                                                    | `pkg/kubelet/server/`                     |  ✗   | Absent                                                                                   |
| HTTP /portforward                                               | `pkg/kubelet/server/`                     |  ✗   | Absent                                                                                   |
| HTTP /debug/pprof, /debug/flags                                 | `pkg/kubelet/server/`                     |  ✗   | Absent                                                                                   |
| Dynamic kubelet config via ConfigMap (deprecated, removed 1.26) | `pkg/kubelet/kubeletconfig/`              |  ✗   | Static YAML only (`config.rs`)                                                           |
| Checkpoint / restore (KEP-2008, `/checkpoint/`)                 | `pkg/kubelet/checkpointmanager/`          |  ✗   | Absent                                                                                   |
| Pod admission handlers chain (predicate, sysctl, eviction)      | `pkg/kubelet/lifecycle/`                  |  ◐   | Eviction triggers `handle_eviction` (`kubelet.rs:3971`); no pluggable admit pipeline     |
| Critical-pod preemption                                         | `pkg/kubelet/preemption/`                 |  ✗   | Absent                                                                                   |
| ClusterTrustBundle projection (KEP-3257)                        | `pkg/kubelet/clustertrustbundle/`         |  ✗   | Absent                                                                                   |
| Image pull credential providers (out-of-tree)                   | `pkg/credentialprovider/`                 |  ✗   | Only Pod-attached `imagePullSecrets` is honoured                                          |
| Container logs rotation                                         | `pkg/kubelet/logs/`                       |  ✗   | Docker handles rotation; kubelet has no logrotate / `kubelet-log-rotation`               |
| PLEG (pod lifecycle event generator) relist                     | `pkg/kubelet/pleg/`                       |  ◐   | Mentioned in code (`kubelet.rs:2902`); achieved via watch + sync loop rather than relist |
| Eviction-driven memcg notification                              | `pkg/kubelet/eviction/threshold_notifier_linux.go` | ✗ | Polling-based only                                                                  |
| Cordon / unschedulable status propagation                       | `pkg/kubelet/kubelet_node_status.go`      |  ◐   | `unschedulable` honoured via API; kubelet itself does not flip the bit                   |
| Graceful node shutdown (systemd inhibit)                         | `pkg/kubelet/cm/nodeshutdown/`            |  ✗   | Absent                                                                                   |

## Missing features (detailed entries)

The kubelet has by far the largest gap surface in the project. The entries
below are roughly ordered by impact on conformance / operability.

### 1. Container Runtime Interface (CRI)

Upstream: `pkg/kubelet/cri/remote/` defines the kubelet's only runtime contract
since v1.24 — gRPC service definitions in
[k8s.io/cri-api](https://github.com/kubernetes/cri-api) cover
`RuntimeService` and `ImageService`. Kubelet talks to containerd or CRI-O over
a Unix socket (`/run/containerd/containerd.sock`), and the runtime owns the
sandbox (pod) and container lifecycle.

Rusternetes: Direct bollard ([Docker Engine API
v1.43](https://docs.docker.com/engine/api/v1.43/)) calls throughout
`runtime.rs`. No CRI client, no gRPC stubs, no sandbox abstraction. Pod
sandbox is implemented in-process by `start_pause_container`
(`runtime.rs:1531`).

Consequences: cannot run against containerd / CRI-O / `crun`-only nodes;
cannot honour user-supplied `RuntimeClass.handler`; tied to dockershim-era
container model.

### 2. RuntimeClass selection / handler resolution

Upstream: `pkg/kubelet/runtimeclass/manager.go` resolves
`Pod.spec.runtimeClassName` to the configured handler name and passes it on
the CRI `RunPodSandboxRequest`. RuntimeClass.overhead is added to pod
requests/limits for cgroup sizing (`pkg/kubelet/cm/pod_container_manager_linux.go`).

Rusternetes: every constructed pod sets `runtime_class_name: None`
(`runtime.rs:8549,9822,10008,10713`). Field is parsed in
`rusternetes-common` but never read. No handler resolver, no overhead
deduction, no gVisor / Kata-aware code path.

### 3. Device plugin framework (KEP-368)

Upstream: `pkg/kubelet/cm/devicemanager/` exposes a gRPC server on
`/var/lib/kubelet/device-plugins/kubelet.sock`. Plugins register at
`Registration.Register` and advertise extended resources
(`nvidia.com/gpu`). The kubelet then handles `Allocate`, `PreStartContainer`,
and `GetPreferredAllocation` calls during sync.

Rusternetes: no device-plugins socket, no registration loop, no extended
resource accounting at the kubelet level. The scheduler crate can match
on extended resources, but no node ever advertises any — there is no producer.

### 4. CPU manager, memory manager, topology manager

Upstream:
- `pkg/kubelet/cm/cpumanager/` — `none` and `static` policies; KEP-2625 adds
  distribute-cpus-across-numa.
- `pkg/kubelet/cm/memorymanager/` (KEP-1769) — `None` / `Static` policies for
  NUMA-pinned hugepage / memory allocations.
- `pkg/kubelet/cm/topologymanager/` (KEP-693) — `none` / `best-effort` /
  `restricted` / `single-numa-node` hint admit policies.

Rusternetes: none of these exist. Every container is sized by Docker cgroup
shares/quota only; there is no cpuset pinning, no NUMA awareness, no
single-numa-node admit. Pods with `Guaranteed` QoS still float across all CPUs.

### 5. Volume manager loop + plugin model

Upstream: `pkg/kubelet/volumemanager/` runs a `desired-state-of-world` and an
`actual-state-of-world` populator, with a reconciler that issues
`SetUp` / `TearDown` / `MountDevice` / `UnmountDevice`. Plugins implement the
`volume.VolumePlugin` interface and are discovered via the plugin manager at
`/var/lib/kubelet/plugins_registry`.

Rusternetes: `runtime.rs:create_pod_volumes` walks the pod's volume list
and creates each one inline — no desired-state vs. actual-state, no
mount/unmount queues, no plugin registry, no `NodeRegistrar` handshake.
Adding a new volume type means editing `runtime.rs`.

### 6. CSI staging / publishing

Upstream: kubelet calls `NodeStageVolume` (per-node global mount) and
`NodePublishVolume` (per-pod bind mount) on the CSI gRPC socket. Plugins
advertise capabilities (`STAGE_UNSTAGE_VOLUME`, `RPC_VOLUME_EXPANSION`) and
unique paths under `/var/lib/kubelet/plugins/<driver>`.

Rusternetes: CSI inline ephemeral volumes are stubbed —
`runtime.rs:3083-3092` literally creates an empty directory and notes
"managed by CSI driver" without ever speaking to one. PVCs backed by CSI
PersistentVolumes are not supported (the kubelet's PV resolution code only
handles HostPath-backed PVs).

### 7. Block-mode volumes (`volumeDevices`)

Upstream: a PV with `volumeMode: Block` is mounted as a raw block device into
the container; the kubelet creates the symlink under
`/var/lib/kubelet/pods/<uid>/volumeDevices/`.

Rusternetes: filesystem-mode only; the `volumeDevices` array on Container is
parsed by `rusternetes-common` but never honoured by the runtime.

### 8. Network volume plugins (iSCSI / RBD / NFS / Cinder / FlexVolume)

Upstream `pkg/volume/{iscsi,rbd,nfs,cinder,glusterfs,flexvolume}/`.

Rusternetes: `iscsi: None`, `nfs: None`, ... by construction
(`runtime.rs:1016-1020`). The "fallback empty directory" branch at
`runtime.rs:3499-3514` swallows every unknown volume type.

### 9. Streaming kubelet API (`/exec`, `/attach`, `/portForward`)

Upstream: `pkg/kubelet/cri/streaming/` serves SPDY (and, since KEP-4006, also
WebSockets) for `/exec`, `/attach`, `/portForward`. The api-server proxies
these through `kubelet --tls-cert-file`. Multi-channel stdin/stdout/stderr
with a separate error channel.

Rusternetes: `main.rs:255-342` exposes a non-streaming `POST
/exec/:container_id` that buffers stdout/stderr into a JSON blob. No SPDY
upgrade, no WebSocket, no resize channel, no `/attach`, no `/portForward`. The
api-server has an exec proxy (`docs/WEBSOCKET_EXEC_IMPLEMENTATION.md`) but it
talks to the JSON blob endpoint, not to a streaming kubelet.

### 10. Stats / cAdvisor `/stats/summary`

Upstream: `pkg/kubelet/server/stats/` and embedded cAdvisor expose
`/stats/summary` (kubectl top node, kubectl top pod, HPA via metrics-server
relies on it). Plus `/metrics/cadvisor`, `/metrics/resource`,
`/metrics/probes`.

Rusternetes: only Prometheus `/metrics`. `kubectl top` requires
`metrics-server`, which scrapes `/stats/summary` — therefore unsupported.

### 11. Other HTTP endpoints

Upstream: `/healthz`, `/pods`, `/run/{podNamespace}/{podID}/{containerName}`,
`/containerLogs/{podNamespace}/{podID}/{containerName}`, `/logs/`,
`/debug/pprof/`, `/debug/flags`, `/configz`.

Rusternetes: `/configz` exists; everything else listed is missing.
`/containerLogs` is particularly load-bearing — without it, the api-server's
`GET /pods/.../log` proxy has nothing to call, so log streaming is wired
elsewhere via Docker directly.

### 12. seccomp / AppArmor enforcement

Upstream: kubelet applies `seccompProfile` per-container by passing
`SeccompProfilePath` over CRI; AppArmor profile is set via the
`container.apparmor.security.beta.kubernetes.io/<containerName>` annotation
and applied by the OCI runtime.

Rusternetes: `seccomp_profile: None` is hard-coded everywhere
(`runtime.rs:9777,9998`). AppArmor annotations are not parsed at the kubelet
level. `capabilities` is read into a local at `runtime.rs:4818-4823` but the
extracted value is not forwarded into the bollard `HostConfig` call.

### 13. Graceful node shutdown (KEP-2000)

Upstream: `pkg/kubelet/cm/nodeshutdown/` registers a systemd
`Inhibit("shutdown:delay")` lock, then on shutdown signal drains pods in
priority order (system-critical last) within `shutdownGracePeriod` /
`shutdownGracePeriodCriticalPods`.

Rusternetes: absent. SIGTERM to the kubelet binary tears down per-pod
workers but does not coordinate with systemd, and pods are not pre-evicted
with grace on host reboot.

### 14. User namespaces (KEP-127)

Upstream: `pkg/kubelet/userns/` allocates UID/GID ranges per-pod, maps them
on the pod sandbox, and applies them to volumes (`fsGroup` interaction).

Rusternetes: absent. `Pod.spec.hostUsers` is parsed but ignored by the
kubelet. Pods always share the host user namespace.

### 15. In-place pod vertical scaling — full KEP-1287

Upstream: the kubelet listens to the `resize` subresource, may set
`status.resize` to `Proposed`, `InProgress`, `Deferred`, or `Infeasible`,
checks node allocatable, deducts pod overhead, applies the new requests, and
updates `status.containerStatuses[].resources.allocated{Resources,Requests}`.

Rusternetes: `Proposed → InProgress → ""` happy path only
(`kubelet.rs:2431-2509`). `Deferred` and `Infeasible` states are not emitted;
the resize is always applied unconditionally, with no node-allocatable
admission. `resizePolicy: NotRequired` vs `RestartContainer` is ignored — the
runtime never recreates a container even when policy demands it.

### 16. Pod overhead (RuntimeClass.overhead)

Upstream: the overhead from the selected RuntimeClass is added to the pod's
cgroup sizing and to scheduler accounting.

Rusternetes: not deducted — cgroup sizes match raw requests/limits exactly.
The field on `Pod.spec.overhead` is parsed but never read in the kubelet.

### 17. Hugepages allocation

Upstream: `pkg/kubelet/cm/` reserves hugepages from the
`hugepages.kubernetes.io/2Mi` and `1Gi` resources; downward API exposes them.

Rusternetes: downward API recognises hugepage units
(`downward_api.rs:129`, `runtime.rs:7965`) but the kubelet does not reserve
hugepages from kernel pools, does not advertise them as extended resources on
Node, and does not enforce limits.

### 18. Swap support (KEP-2400 / KEP-3673)

Upstream: post-KEP-2400, kubelet can run with swap enabled (`failSwapOn:
false`) and apply swap limits via cgroup v2. KEP-3673 adds a `swapBehavior`
config (`LimitedSwap` / `UnlimitedSwap`).

Rusternetes: hardcoded `memory_swap = memory` (`runtime.rs:8061-8070`),
explicitly disabling swap. No `failSwapOn`, no `swapBehavior`.

### 19. Pod admission chain

Upstream: `pkg/kubelet/lifecycle/` chains admit handlers — predicate
(node-allocatable), sysctl whitelist, eviction admit, AppArmor admit. Each
returns `Admitted/Reject(reason, message)`.

Rusternetes: no pluggable admit pipeline. Eviction triggers ad-hoc pod
deletion in `handle_eviction` (`kubelet.rs:3971`), but there is no chained
admit phase before a pod is even attempted.

### 20. Critical-pod preemption

Upstream: `pkg/kubelet/preemption/` evicts non-critical pods on a saturated
node to make room for `system-cluster-critical` / `system-node-critical`
pods (the kubelet's *own* preemption, distinct from scheduler preemption).

Rusternetes: absent. A critical pod on a full node simply waits.

### 21. ClusterTrustBundle projection (KEP-3257)

Upstream: `pkg/kubelet/clustertrustbundle/` watches
`ClusterTrustBundle` objects and projects matching bundles into pods via the
`clusterTrustBundle` projected-volume source (PEM-formatted CA roots).

Rusternetes: not parsed; projected volumes only handle
`{configMap, secret, downwardAPI, serviceAccountToken}`.

### 22. Image pull credential providers

Upstream: `pkg/credentialprovider/plugin/` defines an exec-based credential
provider plugin (ECR, GCR, ACR helpers ship out-of-tree). Configured via
`--image-credential-provider-config`.

Rusternetes: only the pod-attached `imagePullSecrets` are honoured. There is
no `/etc/kubernetes/image-credential-provider*.yaml` config path and no
exec-plugin invocation.

### 23. Dynamic kubelet config via ConfigMap

Upstream feature removed in 1.26 but is one of the historical interactions.
`pkg/kubelet/kubeletconfig/` allowed picking up `kubelet.config.k8s.io`
configuration from a Node-referenced ConfigMap.

Rusternetes: static `KubeletConfiguration` from a YAML file via `--config`.
Acceptable, but worth flagging.

### 24. Container log rotation

Upstream: kubelet truncates / rotates `/var/log/containers/*.log` via
`pkg/kubelet/logs/container_log_manager.go` when not using Docker (Docker
handled it for us). With CRI runtimes, the kubelet *owns* rotation.

Rusternetes: relies on Docker's `json-file` driver rotation. Once on CRI,
this will need to be reimplemented.

### 25. Checkpoint / restore (KEP-2008)

Upstream: `pkg/kubelet/checkpointmanager/` plus the
`/checkpoint/{podNamespace}/{podName}/{containerName}` endpoint allow CRIU-
based forensic checkpoints.

Rusternetes: absent.

### 26. PLEG (pod lifecycle event generator)

Upstream: `pkg/kubelet/pleg/` relists running containers every second and
emits container-add/update/remove events. This is *the* trigger for
`syncPod` outside of API watch events.

Rusternetes: there is no PLEG. The sync loop is driven by the API watch and
the 10s tick (`kubelet.rs:2902` mentions PLEG semantics without implementing
the loop). On a container that exits without an API event, recovery latency
is up to one sync interval.

## Partial / stubbed

- **CSI inline ephemeral volumes** — `runtime.rs:3083-3092` creates an empty
  directory and emits a log line. No CSI driver is contacted.
- **SA token projection** — `runtime.rs:3467-3470` falls back to a literal
  unsigned-JWT placeholder
  `"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.placeholder"` if TokenRequest
  generation fails.
- **In-place pod resize** — happy path only; `Deferred` and `Infeasible`
  status values are never written.
- **Ephemeral containers** — pod status reports any ephemeral container statuses
  from the runtime (`runtime.rs:5995`), but the `/ephemeralcontainers`
  subresource flow that *adds* the container is not exercised through this
  kubelet.
- **Capabilities** — read at `runtime.rs:4818-4823` but the extracted
  `add/drop` vector is not forwarded into the bollard `HostConfig` call site
  on the next pages.
- **ImageFs eviction** — enum variants exist but the signal value is
  hard-coded `0.0` (`eviction.rs:213-214`).
- **`/exec` endpoint** — exists but is a JSON-buffered single-shot endpoint,
  not a SPDY/WebSocket stream. Cannot serve interactive shells.
- **Sysctls** — values from the pod are passed straight to Docker
  (`runtime.rs:1664-1725`); no `safe` vs `unsafe` whitelist gate.
- **`fsGroup`** — applied to emptyDir but not to all volume types, and no
  `fsGroupChangePolicy: OnRootMismatch` optimisation.
- **Image GC** — container GC at `kubelet.rs:1127` runs every 60s; an LRU
  image-disk-usage GC matching upstream is absent.

## Known in-code TODOs

There is exactly one TODO in the kubelet sources:

- `crates/kubelet/src/kubelet.rs:827` — `// TODO: Remove once per-pod workers
  are proven stable.` (this guards a fallback branch in the sync loop that
  re-syncs pods even when their per-pod worker channel was already signalled).

All other gaps are implicit (silent fallthrough branches, `None`-typed fields,
or absent modules).

## References

- Upstream kubelet tree: <https://github.com/kubernetes/kubernetes/tree/master/pkg/kubelet>
- Upstream kubelet entrypoint: <https://github.com/kubernetes/kubernetes/tree/master/cmd/kubelet>
- CRI API: <https://github.com/kubernetes/cri-api>
- KEPs referenced:
  - KEP-127 — User Namespaces — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/127-user-namespaces>
  - KEP-368 — Device Plugins — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/368-device-plugin-framework>
  - KEP-596 — CSI Inline Ephemeral Volumes — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/596-csi-inline-volumes>
  - KEP-693 — Topology Manager — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/693-topology-manager>
  - KEP-753 — Sidecar Containers — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/753-sidecar-containers>
  - KEP-1287 — In-place Pod Resize — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/1287-in-place-update-pod-resources>
  - KEP-1698 — Generic Ephemeral Volumes — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1698-generic-ephemeral-volumes>
  - KEP-1769 — Memory Manager — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/1769-memory-manager>
  - KEP-2000 — Graceful Node Shutdown — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2000-graceful-node-shutdown>
  - KEP-2008 — Container Checkpoint / Restore — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2008-forensic-container-checkpointing>
  - KEP-2400 — Swap Accounting — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2400-node-swap>
  - KEP-2625 — Distribute CPUs Across NUMA — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2625-cpumanager-policies-thread-placement>
  - KEP-3063 — DRA — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/3063-dynamic-resource-allocation>
  - KEP-3257 — ClusterTrustBundles — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-auth/3257-cluster-trust-bundles>
  - KEP-3673 — Kubelet Limited Swap — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/3673-kubelet-swap>
  - KEP-4006 — WebSockets for Exec/Attach/PortForward — <https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/4006-transition-spdy-to-websockets>
- Internal:
  - `docs/CONFORMANCE.md`
  - `docs/CNI_GUIDE.md` / `docs/CNI_IMPLEMENTATION_SUMMARY.md`
  - `docs/KUBELET_CONFIGURATION.md`
  - `docs/WEBSOCKET_EXEC_IMPLEMENTATION.md`
