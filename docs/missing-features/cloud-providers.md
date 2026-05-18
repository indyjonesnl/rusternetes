# Missing Features — cloud-providers

## Scope

This document compares the Rusternetes `cloud-providers` crate
(`crates/cloud-providers/`) against the upstream Kubernetes
cloud-controller-manager (CCM) — specifically `cmd/cloud-controller-manager`
and the cloud-provider SPI in `staging/src/k8s.io/cloud-provider/`.

Out of scope:

- CSI volume drivers (`crates/storage-provisioner` covers HostPath only; cloud
  block-storage CSI drivers are tracked in a separate doc).
- Ingress controllers (e.g. AWS ALB Ingress, GCE Ingress) — those are
  out-of-tree addons even upstream.
- CNI integrations that happen to use cloud APIs (VPC CNI, Azure CNI).

In scope: the cloud-provider SPI (`LoadBalancer`, `Instances`, `InstancesV2`,
`Zones`, `Routes`, `Clusters`), the four upstream CCM controllers
(`cloud-node`, `cloud-node-lifecycle`, `service`, `route`), and per-provider
LB/IPAM behaviour.

## Current Rusternetes state

The crate ships **three providers** wired through a feature-gated factory plus
**one consumer controller** in `controller-manager`:

- `crates/cloud-providers/src/lib.rs` (~209 LOC, including tests) — factory
  `create_provider()` + env-based `detect_cloud_provider()`. Recognises
  `aws`, `gcp`, `azure`, `none`; falls back to a generic
  `Error::Internal("... provider not available. Compile with --features X")`
  when the matching feature flag is off.
- `crates/cloud-providers/src/aws.rs` (~615 LOC) — only **fully-functional**
  provider. Uses `aws-sdk-elasticloadbalancingv2` to create an NLB per Service
  of type LoadBalancer. Auto-tags `kubernetes.io/cluster=<name>` and
  `managed-by=rusternetes`. Pulls VPC + subnet IDs from
  `AWS_VPC_ID` / `AWS_SUBNET_IDS` env (not real EC2 IMDS yet — see
  `detect_vpc_and_subnets()` at line 65, which has a "placeholder" TODO).
  Honours `service.beta.kubernetes.io/aws-load-balancer-internal=true`.
  Targets are registered as **IP** targets pointing at node addresses (so the
  controller treats every node as a target and never trims terminating nodes).
- `crates/cloud-providers/src/gcp.rs` (~83 LOC) — **stub**. Constructor stores
  `_project_id`, `_region`, `_cluster_name` (all underscore-prefixed because
  unused). `ensure_load_balancer()` logs a warning and returns
  `Error::Internal("GCP LoadBalancer provider not yet implemented")`. TODO
  list at lines 40–45 outlines forwarding rule, backend service, health
  check, instance group, external IP.
- `crates/cloud-providers/src/azure.rs` (~92 LOC) — **stub**. Same pattern as
  GCP. TODO list at lines 47–54 outlines public IP, LB, backend pool, health
  probe, LB rules, VM registration.
- `crates/common/src/cloud_provider.rs` — defines `CloudProvider` trait and
  the `LoadBalancerService`, `LoadBalancerPort`, `LoadBalancerStatus`,
  `LoadBalancerIngress`, `LoadBalancerConfig`, `CloudProviderType` types.
  The trait only contains four methods, all about LB: `ensure_load_balancer`,
  `delete_load_balancer`, `get_load_balancer_status`, `name`. There is **no**
  surface for Instances, Zones, Routes, Clusters.
- `crates/controller-manager/src/controllers/loadbalancer.rs` (~613 LOC) —
  watches Services with `type=LoadBalancer`, builds a `CloudLBService`,
  delegates to the configured provider, and writes the result to
  `service.status.loadBalancer.ingress`. Reconcile loop with 30s resync,
  watch-driven enqueue. No annotation pass-through beyond what the AWS
  provider reads directly.

Other consumers of cloud APIs are absent. `crates/controller-manager` does
**not** ship a `cloud-node` controller, a `cloud-node-lifecycle` controller,
or a `route-controller`. The `Node.spec.providerID` field is defined
(`crates/common/src/resources/node.rs:42`) but **no code ever sets it** — every
producer (kubelet at `crates/kubelet/src/kubelet.rs:392`, the
`controller-manager` node helpers at `crates/controller-manager/src/controllers/node.rs:452`,
all daemonset/scheduler/api-server tests) hard-codes `provider_id: None`.

## Parity matrix

Legend: ✓ implemented, ◐ partial / stubbed, ✗ missing.

| Capability | Upstream interface | AWS | GCP | Azure |
| --- | --- | --- | --- | --- |
| `LoadBalancer.EnsureLoadBalancer` (create) | cloud-provider/cloud.go | ✓ NLB v2 only | ✗ stub | ✗ stub |
| `LoadBalancer.EnsureLoadBalancer` (update on node-set change) | cloud-provider/cloud.go | ◐ only full reconcile; no incremental `RegisterTargets`/`DeregisterTargets` | ✗ | ✗ |
| `LoadBalancer.UpdateLoadBalancer` | cloud-provider/cloud.go | ✗ (always falls through to ensure) | ✗ | ✗ |
| `LoadBalancer.GetLoadBalancer` | cloud-provider/cloud.go | ✓ `get_load_balancer_status` returns hostname only | ✗ | ✗ |
| `LoadBalancer.GetLoadBalancerName` | cloud-provider/cloud.go | ◐ baked into `lb_name`, not exposed via trait | ✗ | ✗ |
| `LoadBalancer.EnsureLoadBalancerDeleted` | cloud-provider/cloud.go | ✓ but leaves orphan target groups behind (only deletes the LB itself) | ✗ | ✗ |
| Classic ELB (CLB) support | cloud-provider-aws | ✗ | n/a | n/a |
| ALB (L7) for Service | cloud-provider-aws (annotation `aws-load-balancer-type=external`) | ✗ | ✗ | ✗ |
| Internal LB scheme | annotation `*-internal` | ✓ AWS (`-internal=true`) | ✗ | ✗ |
| HTTPS / TLS listener (`aws-load-balancer-ssl-cert`, Azure cert ref) | annotations | ✗ | ✗ | ✗ |
| Backend Protocol HTTP/TCP/UDP/SCTP | KEP-3866 multi-protocol | ✗ TCP only | ✗ | ✗ |
| `LoadBalancerClass` selector | KEP-1959 | ✗ provider is global, ignores `spec.loadBalancerClass` | ✗ | ✗ |
| `allocateLoadBalancerNodePorts=false` | KEP-1860 | ✗ NLB always targets NodePort | n/a | n/a |
| Source-range firewalling (`loadBalancerSourceRanges`) | core Service | ✗ AWS sec-group not managed | ✗ | ✗ |
| Health-check policy (`externalTrafficPolicy=Local`, healthCheckNodePort) | KEP-1672 | ✗ all-nodes targeting; ignores Local | ✗ | ✗ |
| `Instances.NodeAddresses` / `InstancesV2.InstanceMetadata` | cloud-provider/cloud.go | ✗ | ✗ | ✗ |
| `Instances.InstanceID` / providerID population | cloud-provider/cloud.go | ✗ never sets `Node.spec.providerID` | ✗ | ✗ |
| `Instances.InstanceType` → `node.kubernetes.io/instance-type` label | well-known labels | ✗ | ✗ | ✗ |
| `Instances.InstanceShutdownByProviderID` | cloud-provider/cloud.go | ✗ | ✗ | ✗ |
| `Instances.AddSSHKeyToAllInstances` | cloud-provider/cloud.go | ✗ | ✗ | ✗ |
| `Zones.GetZone` → `topology.kubernetes.io/zone` label | well-known labels | ✗ | ✗ | ✗ |
| `Zones.GetZoneByProviderID` / `GetZoneByNodeName` | cloud-provider/cloud.go | ✗ | ✗ | ✗ |
| Region label `topology.kubernetes.io/region` | well-known labels | ✗ | ✗ | ✗ |
| `Routes.ListRoutes` / `CreateRoute` / `DeleteRoute` (kubenet pod-CIDR routes) | cloud-provider/cloud.go | ✗ | ✗ | ✗ |
| `Clusters.ListClusters` / `Master` | cloud-provider/cloud.go | ✗ | ✗ | ✗ |
| `HasClusterID` / `Initialize(clientBuilder, stop)` | cloud-provider/cloud.go | ✗ no trait surface | ✗ | ✗ |
| `cloud-node` controller (init node taint + labels + addresses) | upstream CCM | ✗ no such controller | ✗ | ✗ |
| `cloud-node-lifecycle` controller (delete node when instance gone) | upstream CCM | ✗ | ✗ | ✗ |
| `route-controller` (program VPC route table for pod CIDR) | upstream CCM | ✗ | ✗ | ✗ |
| Uninitialized-node taint `node.cloudprovider.kubernetes.io/uninitialized` | upstream CCM | ✗ kubelet never applies it, controller never removes it | ✗ | ✗ |
| Feature-gated, in-process integration (vs external CCM binary) | KEP-2392 split | ◐ in-tree only, no `--cloud-provider=external` mode | ◐ | ◐ |
| Leader election among CCM replicas | upstream CCM | ◐ controller-manager binary supports `--enable-leader-election`, but cloud-provider tasks share the controller-manager lock rather than electing CCM-specific leadership | ◐ | ◐ |
| `--allow-untagged-cloud` semantics | upstream CCM | ✗ | ✗ | ✗ |

## Missing features

### 1. Cloud-node controller (provider-driven node initialization)

Upstream `cloud-node-controller` runs on every newly-registered Node and is
responsible for:

- Removing the `node.cloudprovider.kubernetes.io/uninitialized:NoSchedule`
  taint that kubelet applies on startup when `--cloud-provider=external`.
- Setting `Node.spec.providerID` from `InstanceID` / `InstanceMetadata.ProviderID`.
- Populating `Node.status.addresses` (InternalIP, ExternalIP, Hostname,
  InternalDNS, ExternalDNS) from cloud metadata.
- Setting standard labels:
  - `node.kubernetes.io/instance-type=<m5.large|n1-standard-4|...>`
  - `topology.kubernetes.io/zone=<us-east-1a|...>`
  - `topology.kubernetes.io/region=<us-east-1|...>`
  - `failure-domain.beta.kubernetes.io/zone` (legacy alias, still required by
    some workloads)
  - `failure-domain.beta.kubernetes.io/region` (legacy alias)

Rusternetes has **no equivalent controller**. The `providerID` field is
defined in the resource type but never written. Cloud-aware scheduling
predicates (zone-aware volume binding, topology-spread on `topology.kubernetes.io/zone`)
therefore cannot work on AWS, GCP, or Azure clusters spun up by Rusternetes
even when the LB provider is wired correctly.

### 2. Cloud-node-lifecycle controller

Upstream `cloud-node-lifecycle-controller` polls
`Instances.InstanceExistsByProviderID` / `InstancesV2.InstanceExists` and, when
the cloud reports a node has been terminated:

- Marks the Node `Ready=False` with reason `NodeStatusNeverUpdated`.
- Calls `InstancesV2.InstanceShutdown` to differentiate stop-vs-terminate.
- Deletes the Node object once shutdown is confirmed, which triggers pod
  eviction via the GC controller.

Without this controller, a terminated EC2 instance leaves a stale Node object
in Rusternetes forever, and the scheduler keeps trying to place pods on it.
The existing `crates/controller-manager/src/controllers/node.rs` only handles
heartbeat-based `Ready` flipping (kubelet-driven), never cloud-driven
deletion.

### 3. Route controller

Upstream `route-controller` watches Nodes and, for each one, calls
`Routes.CreateRoute(podCIDR -> instance)` so that pods on node A can reach
pods on node B through the cloud VPC routing table (used by **kubenet** and
flannel-host-gw style CNIs). Without it, every cluster needs an overlay
(VXLAN, IP-in-IP) to route pod traffic.

Rusternetes does not have a route controller, nor any `Routes`-style trait
methods. The current Docker-compose deployments paper over this by relying on
the host bridge / `kube-proxy` iptables. Multi-node deployments on real cloud
VPCs would not work.

### 4. External-mode CCM (KEP-2392)

Upstream graduated KEP-2392 to GA in 1.31: the cloud-provider code lives in
**per-provider repos** (`kubernetes/cloud-provider-aws`,
`kubernetes-sigs/cloud-provider-azure`, `kubernetes/cloud-provider-gcp`,
plus vSphere, OpenStack, etc.) and runs as a separate `cloud-controller-manager`
binary. The in-tree providers (the original `pkg/cloudprovider/providers/*`)
were removed in 1.29.

Rusternetes' approach is structurally different:

- All three providers compile into a single `cloud-providers` crate via
  `#[cfg(feature = "aws")]` flags.
- They are linked directly into `controller-manager` (and the all-in-one
  binary) — there is no separate CCM process, no `--cloud-provider=external`
  on kubelet, no `node.cloudprovider.kubernetes.io/uninitialized` taint
  protocol, no leader election between CCM replicas.

Either Rusternetes commits to the in-tree style (and accepts the divergence
from upstream's GA architecture) or grows a separate `cloud-controller-manager`
binary. Today there is no decision documented either way.

### 5. AWS provider: target group lifecycle and orphan resources

`AwsProvider::delete_load_balancer` deletes the LB but **does not delete the
underlying target groups** created by `ensure_target_group`. Each LB port
gets a TG named `<cluster>-<ns>-<svc>-<port>`. After repeated create/delete
cycles those TGs accumulate and eventually hit the per-region TG quota (3000
by default). Upstream cloud-provider-aws walks the TGs by tag and deletes
orphans on every reconcile.

Related gaps in the AWS code path:

- `register_targets` only fires during initial TG creation
  (`aws.rs:196–226`). If nodes are added / removed after the LB exists, the
  target set is never updated. Should be a diff against
  `describe_target_health` on every reconcile.
- VPC / subnet discovery is fake: `detect_vpc_and_subnets()` (line 65)
  returns env-vars and "placeholder" strings. Production deployments need
  IMDSv2 lookup of the current instance's VPC, plus subnet enumeration tagged
  with `kubernetes.io/role/elb=1` (public) or `kubernetes.io/role/internal-elb=1`
  (internal) — both are upstream conventions.
- Security groups: NLB does not allocate a security group, but the **node
  security groups** must be amended to allow traffic from the LB subnet
  CIDRs on the target NodePort range. Rusternetes does not touch SGs.
- Cross-zone load balancing attribute (`load_balancing.cross_zone.enabled`)
  is never set; AWS default is per-LB scheme.
- Connection idle timeout, deletion protection, access logs: none of the
  upstream `service.beta.kubernetes.io/aws-load-balancer-*` annotations are
  honoured except `-internal`.

### 6. ELB Classic (CLB) backend

Upstream cloud-provider-aws supports two LB types via annotation:
`service.beta.kubernetes.io/aws-load-balancer-type=nlb` (NLB, current
default) and the absence of the annotation (Classic ELB, the historical
default). Rusternetes only ever creates NLBs. Workloads that depend on the
CLB's HTTP/HTTPS termination, X-Forwarded-Proto headers, or the legacy ELB
listener model cannot be migrated.

### 7. GCP provider: forwarding rule + backend service stack

`gcp.rs` is a stub. A real implementation would need:

- `compute.googleapis.com` SDK (no `google-cloud-compute` crate is in
  `Cargo.toml` yet — the constructor doesn't even build a client).
- For external L4 LBs: target pool + forwarding rule + static external IP
  + health check (TCP or HTTPS). Modern variant is **regional** backend
  service + forwarding rule, which is what `service.beta.kubernetes.io/gcp-load-balancer-type=Internal`
  requires.
- Instance group management — every node must be a member of an instance
  group that the backend service references.
- Firewall rules permitting GCP health-check source ranges
  (`130.211.0.0/22`, `35.191.0.0/16`).
- TODOs spelled out at `gcp.rs:40–45` cover steps 1–5 but not instance
  group lifecycle or firewall rules.

### 8. Azure provider: Standard SKU LB + Public IP + Probe

`azure.rs` is a stub. A real implementation would need:

- `azure_mgmt_network` / `azure_mgmt_compute` SDKs.
- Public IP allocation (Standard SKU, static, zone-redundant by default per
  cloud-provider-azure).
- Frontend IP config + backend pool + LB rules + health probe (TCP/HTTP).
- NIC-level backend pool membership for every Node VM (or VMSS-level for
  scale sets — Rusternetes would need to detect which).
- Network Security Group rules permitting LB-to-Node traffic on each
  NodePort.
- The TODO list at `azure.rs:47–54` enumerates steps 1–7 but omits NSG
  changes and zone-redundancy.
- Azure also has its own `LoadBalancerClass` `service.beta.kubernetes.io/azure-load-balancer-mode`
  (Auto / VMSS-aware) — none of that exists.

### 9. `LoadBalancerClass` selection (KEP-1959, GA in 1.24)

`Service.spec.loadBalancerClass` lets a cluster run multiple LB
implementations side-by-side (e.g. AWS NLB **and** the OpenELB on-prem class).
Today's controller picks **one** provider at startup from `CLOUD_PROVIDER` env
and applies it to every type=LoadBalancer Service. The
`crates/controller-manager/src/controllers/loadbalancer.rs` reconcile path
should:

1. Read `service.spec.load_balancer_class`.
2. If `Some(class)`, only proceed if a provider has registered for that
   class; otherwise leave the Service un-provisioned (upstream behaviour).
3. If `None`, fall back to the cluster-default class (or the env-configured
   provider for back-compat).

### 10. `externalTrafficPolicy=Local` / health-check NodePort

Upstream Service spec exposes `externalTrafficPolicy: Local`, which tells
the LB to:

- Only forward traffic to nodes that have a local pod backing the Service
  (preserves the client source IP).
- Probe a separate `healthCheckNodePort` (`service.spec.healthCheckNodePort`)
  rather than the data NodePort, so nodes without a local pod are marked
  unhealthy by the LB.

The Rusternetes AWS provider always targets all nodes and always health-checks
the data NodePort. Source IP preservation does not work.

### 11. KEP-3866 LoadBalancer multi-protocol

Since 1.26 (GA), a single Service of type LoadBalancer can mix TCP, UDP, and
SCTP ports. Cloud providers must create the correct listener type per port.
The AWS code path hard-codes `ProtocolEnum::Tcp` (`aws.rs:177`,
`aws.rs:347`). UDP and SCTP Service ports silently fall through to TCP — at
best half-broken, at worst a silent data-plane mismatch.

### 12. KEP-1860 `allocateLoadBalancerNodePorts=false`

For LB classes that target pods directly (e.g. AWS NLB **IP target type**,
which is exactly what `aws.rs:181` configures), the NodePort allocation is
wasted. Upstream exposes `service.spec.allocateLoadBalancerNodePorts: false`
to skip NodePort assignment for those classes. Rusternetes always allocates
a NodePort even though the AWS provider then ignores it and targets pod IPs
directly. The controller should respect the flag and the api-server
allocator should honour it.

### 13. KEP-3458 KubeProxyDrainingTerminatingNodes

When a node is being drained, kube-proxy can keep terminating endpoints
serving in-flight connections while the LB removes the node from rotation.
Upstream coordinates this through `Endpoint.conditions.terminating` and the
`node.kubernetes.io/exclude-from-external-load-balancers` taint. Rusternetes'
LB controller does not read either signal — drained nodes stay in the LB
target pool until the next reconcile.

### 14. `--cloud-provider` flag on kubelet + uninitialized taint

Upstream kubelet, when invoked with `--cloud-provider=external`, applies the
`node.cloudprovider.kubernetes.io/uninitialized:NoSchedule` taint at startup
and waits for the CCM to remove it (after the cloud-node controller runs).
Rusternetes' kubelet (`crates/kubelet/src/kubelet.rs`) does not take a cloud
provider flag and does not apply the taint, so there is no coordination
between kubelet start-up and provider-driven node initialization. This
matters when the cloud provider is what supplies the node's IP addresses
(e.g. ExternalIP allocation).

### 15. Other providers (vSphere, OpenStack, IBM Cloud, AlibabaCloud, DigitalOcean)

Upstream maintains community CCMs for at least:

- vSphere (`kubernetes/cloud-provider-vsphere`)
- OpenStack (`kubernetes/cloud-provider-openstack`)
- IBM Cloud (`kubernetes-sigs/cloud-provider-ibmcloud`)
- AlibabaCloud (`kubernetes/cloud-provider-alibaba-cloud`)
- DigitalOcean (`digitalocean/digitalocean-cloud-controller-manager`)
- Equinix Metal / Packet, Hetzner, Linode (community)

The Rusternetes `CloudProviderType` enum has exactly four variants
(`AWS`, `GCP`, `Azure`, `None`) — extending it would touch the factory plus
every match expression in `controller-manager` and `rusternetes` binary
plumbing. There's no plugin mechanism (à la Go's `RegisterCloudProvider`
init-time registration) to add a provider without rebuilding the workspace.

## Partial / stubbed

- **AWS provider end-to-end** — works for the happy path of "create one NLB
  per Service, leave it alone". Falls down on update (no node-set diff), on
  delete (TG leaks), and on every annotation beyond `-internal=true`. See
  Missing #5.
- **GCP provider** — `gcp.rs` only defines the struct and trait stubs that
  return `Internal` errors. No SDK dependency. Compiles, doesn't function.
- **Azure provider** — same shape as GCP. Stub.
- **VPC / subnet detection** — `detect_vpc_and_subnets()` (`aws.rs:65`) reads
  `AWS_VPC_ID` and `AWS_SUBNET_IDS` env-vars and otherwise returns
  `"vpc-placeholder"`, `"subnet-placeholder-1"`. Comment at line 67 calls it
  out: "In production, this should query EC2 instance metadata service".
- **Tags** — only two tags are written
  (`kubernetes.io/cluster=<name>`, `managed-by=rusternetes`). Upstream uses
  `kubernetes.io/cluster/<name>=owned` (note the slash and `=owned` value),
  plus `kubernetes.io/service-name=<ns>/<name>`. Migration tooling that
  scans for upstream tags won't recognise Rusternetes LBs.
- **`CloudProviderType` enum** — closed enum, no `Custom(String)` variant
  and no out-of-tree registration. Adding a fifth provider is a workspace
  rebuild.
- **`detect_cloud_provider()`** — env-var based only
  (`AWS_REGION`, `GCP_PROJECT`, `AZURE_SUBSCRIPTION_ID`). Does not query
  IMDS / GCE metadata server / Azure IMDS, which upstream does.

## Known in-code TODOs

Grep across the crate (`grep -rn "TODO\|FIXME\|placeholder" crates/cloud-providers/`)
surfaces:

- `aws.rs:67` — "In production, this should query EC2 instance metadata service"
- `aws.rs:69` — `"vpc-placeholder"`
- `aws.rs:72` — `"subnet-placeholder-1,subnet-placeholder-2"`
- `gcp.rs:1` — "TODO: Implement GCP Cloud Load Balancing integration"
- `gcp.rs:40` — "TODO: Implement using Google Cloud SDK" + 5-step plan
- `gcp.rs:62` — "TODO: Implement deletion of forwarding rule, backend service, health check"
- `azure.rs:1` — "TODO: Implement Azure Load Balancer integration"
- `azure.rs:47` — "TODO: Implement using Azure SDK" + 7-step plan
- `azure.rs:71` — "TODO: Implement deletion of load balancer, public IP, backend pool"

The controller side (`controllers/loadbalancer.rs`) has none of its own
TODOs; gaps there are silent (no annotation handling, no class selection, no
finalizer beyond what the Service controller in api-server provides).

## References

### Upstream Kubernetes

- `staging/src/k8s.io/cloud-provider/cloud.go` — the SPI: `Interface`,
  `LoadBalancer`, `Instances`, `InstancesV2`, `Zones`, `Clusters`, `Routes`.
- `cmd/cloud-controller-manager/` — the binary scaffold. Real logic lives
  in `pkg/controller/cloud/` and per-provider repos.
- `pkg/controller/cloud/node_controller.go` — cloud-node controller
  (Missing #1).
- `pkg/controller/cloud/node_lifecycle_controller.go` — cloud-node-lifecycle
  (Missing #2).
- `pkg/controller/route/route_controller.go` — route controller
  (Missing #3).
- `pkg/controller/service/controller.go` — service controller, the upstream
  counterpart of `crates/controller-manager/src/controllers/loadbalancer.rs`.

### KEPs

- KEP-2392 — graduating cloud-controller-manager to GA (the in-tree → out-of-tree
  split). 1.31.
- KEP-2395 — removing in-tree cloud providers (AWS, Azure, GCE, vSphere,
  OpenStack). 1.29.
- KEP-1959 — `Service.spec.loadBalancerClass`. GA in 1.24.
- KEP-1860 — `allocateLoadBalancerNodePorts`. GA in 1.24.
- KEP-1672 — `externalTrafficPolicy=Local` and health-check NodePort
  semantics. GA in 1.27 (cluster-internal traffic policy).
- KEP-3458 — `KubeProxyDrainingTerminatingNodes`. Graduated 1.31.
- KEP-3866 — LoadBalancer multi-protocol (TCP + UDP + SCTP on one Service).
  GA in 1.26.

### Per-provider repos

- `kubernetes/cloud-provider-aws` — `service.beta.kubernetes.io/aws-load-balancer-*`
  annotation reference; NLB / CLB / target-group reconcile semantics.
- `kubernetes-sigs/cloud-provider-azure` — Standard SKU LB design doc; NSG /
  zone-redundancy semantics.
- `kubernetes/cloud-provider-gcp` — forwarding rule / backend service stack;
  ILB vs external scheme; firewall-rule programming.

### Rusternetes paths cited

- `crates/cloud-providers/src/lib.rs`
- `crates/cloud-providers/src/aws.rs`
- `crates/cloud-providers/src/gcp.rs`
- `crates/cloud-providers/src/azure.rs`
- `crates/common/src/cloud_provider.rs`
- `crates/common/src/resources/node.rs` (line 42 — `providerID` field)
- `crates/controller-manager/src/controllers/loadbalancer.rs`
- `crates/controller-manager/src/controllers/node.rs` (heartbeat-based, not
  cloud-driven)
- `crates/kubelet/src/kubelet.rs` (line 392 — `provider_id: None`)
