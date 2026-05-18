# Missing Features — kube-proxy

Comparison of Rusternetes' `crates/kube-proxy` (~2552 LOC across 4 files) against
upstream Kubernetes [`pkg/proxy/`](https://github.com/kubernetes/kubernetes/tree/master/pkg/proxy).
The aim is to enumerate what is **not yet** implemented and where the gaps sit,
not to litigate code quality.

## Scope

Upstream `pkg/proxy/` is roughly an order of magnitude larger than the
Rusternetes crate and ships **four** proxy backends — `iptables/`, `ipvs/`,
`nftables/`, `winkernel/` — plus shared infrastructure (`endpointslicecache.go`,
`servicechangetracker.go`, `topology.go`, `healthcheck/`, `conntrack/`,
`metaproxier/`, `metrics/`). Rusternetes implements **one** of those backends
(iptables, legacy command), and only a portion of its feature surface.

This document covers:

1. The state of the iptables backend in Rusternetes.
2. The proxy modes (IPVS, native nftables, winkernel) that have **no** Rusternetes
   counterpart.
3. Per-service features (ExternalIPs, externalTrafficPolicy, terminating
   endpoints, healthCheckNodePort, LoadBalancer firewall, dual-stack,
   topology-aware routing) that the iptables backend itself is missing.

E2E conformance for kube-proxy lives in
`crates/kube-proxy/tests/conformance_network_services_proxy.rs` and
`crates/kube-proxy/tests/conformance_network_endpointslices_dns_headless.rs`.

## Current Rusternetes state (cite path:line)

- `crates/kube-proxy/src/lib.rs:11-22` — `KubeProxyConfig` exposes
  `node_name`, `sync_interval`, `cluster_cidr`, `nodeport_range`. No mode
  selector (`--proxy-mode`), no `--ipvs-scheduler`, no `--metrics-bind-address`,
  no `--cluster-cidr` list for dual-stack.
- `crates/kube-proxy/src/lib.rs:106-145` — watches `services`, `endpoints`,
  `endpointslices`, `pods`. No watch on `Node` (needed for topology hints) or
  `NetworkPolicy` (kube-proxy doesn't enforce NPs, just noting we don't pull the
  signal). No leader election; on every node only kube-proxy itself runs.
- `crates/kube-proxy/src/lib.rs:148-149` — periodic 10s safety-net resync;
  upstream `minSyncPeriod` / `syncPeriod` are configurable and default to
  `1s` / `30s`.
- `crates/kube-proxy/src/lib.rs:230-242` — `check_iptables` hardcodes
  `/usr/sbin/iptables-legacy`; the detection-via-`detect_iptables_cmd` in
  `iptables.rs:9-25` chooses between `iptables` (nft userspace) and
  `iptables-legacy`, but neither code path uses the native `nft` binary.
- `crates/kube-proxy/src/main.rs:11-50` — CLI accepts `--storage-backend`,
  `--node-name`, `--sync-interval`, `--cluster-cidr`, `--node-port-range`.
  Missing: `--proxy-mode`, `--masquerade-all`, `--masquerade-bit`,
  `--nodeport-addresses`, `--healthz-bind-address`, `--metrics-bind-address`,
  `--detect-local-mode`, `--bind-address`, `--feature-gates`.
- `crates/kube-proxy/src/proxy.rs:11-21` — `KubeProxy` struct holds storage +
  one `IptablesManager`. No `Proxier` trait, no dual-stack/MetaProxier
  splitting of v4 vs v6.
- `crates/kube-proxy/src/proxy.rs:42-271` — `sync()` reads ALL services, ALL
  endpoints, ALL endpointslices, ALL pods on every reconcile. Upstream uses
  `EndpointSliceCache` + `ServiceChangeTracker` for incremental updates and
  `partialSync` to touch only changed services.
- `crates/kube-proxy/src/proxy.rs:111-114` — EndpointSlice filtering only
  inspects `conditions.ready == Some(false)`. `conditions.serving` and
  `conditions.terminating` are ignored, so the
  `ProxyTerminatingEndpoints` / `KubeProxyDrainingTerminatingNodes` fallback
  (KEP-1669, KEP-3458) is **not** implemented.
- `crates/kube-proxy/src/proxy.rs:298-302` — `ExternalName` is skipped; no DNAT
  programming. (Upstream also doesn't program iptables for ExternalName, so
  this is parity.)
- `crates/kube-proxy/src/proxy.rs:351-360` — session affinity `ClientIP`
  honored with timeout from `sessionAffinityConfig.clientIP.timeoutSeconds`,
  default 10800s.
- `crates/kube-proxy/src/iptables.rs:86-87` — `DEFAULT_CLUSTER_CIDR =
  10.96.0.0/12`, `DEFAULT_NODEPORT_RANGE = 30000:32767`.
- `crates/kube-proxy/src/iptables.rs:89-114` — `IptablesManager` defines only
  three chains: `RUSTERNETES-SERVICES`, `RUSTERNETES-NODEPORTS`,
  `KUBE-HOSTPORTS`. Upstream uses `KUBE-SERVICES`, `KUBE-EXTERNAL-SERVICES`,
  `KUBE-NODEPORTS`, `KUBE-POSTROUTING`, `KUBE-MARK-MASQ`, `KUBE-FORWARD`,
  `KUBE-PROXY-FIREWALL`, `KUBE-PROXY-CANARY`, plus per-service `KUBE-SVC-*`,
  `KUBE-SVL-*`, `KUBE-EXT-*`, `KUBE-FW-*`, and per-endpoint `KUBE-SEP-*`.
- `crates/kube-proxy/src/iptables.rs:1651-1959` — `build_nat_rules` writes the
  whole `*nat` table in iptables-restore format, including KUBE-SEP chains for
  session-affinity services. No `*filter` or `*mangle` table writes (kube-proxy
  upstream uses `*mangle` for masquerade-mark plus `*filter` for
  KUBE-PROXY-FIREWALL drop rules and KUBE-FORWARD).
- `crates/kube-proxy/src/iptables.rs:1975-2059` — `KUBE-HOSTPORTS` is
  populated from the pod list; upstream's hostport plumbing lives in the
  kubelet, not kube-proxy. This is **extra**, not a gap (note for future
  refactor: should arguably move to `kubelet`).
- `crates/kube-proxy/src/iptables.rs:2061+` — `apply_nat_rules_atomic` uses
  `iptables-restore --noflush`; mirrors upstream's atomic write path.
- `crates/kube-proxy/src/iptables.rs:1458-` — `cleanup()` removes the
  Rusternetes chains on Drop; no equivalent for the iptables-cleanup CLI
  (`kube-proxy --cleanup`).
- No `prometheus`/`metrics` integration — no rule-sync duration histogram, no
  `sync_proxy_rules_iptables_total`, no per-service or per-endpoint counters
  exported.
- No source-grep hits for any of: `IPVS`, `ipvs`, `nftables`, `NFTables`,
  `nft `, `internalTrafficPolicy`, `internal_traffic_policy`,
  `externalTrafficPolicy`, `external_traffic_policy`, `external_ips`,
  `topology_aware`, `topology_keys`, `health_check_node_port` outside test
  fixtures.

## Parity matrix

| Feature | Upstream `pkg/proxy/` | Rusternetes `crates/kube-proxy/` |
| --- | --- | --- |
| Proxy mode: userspace (legacy) | Removed (since 1.26) | n/a |
| Proxy mode: iptables (default 1.x) | `pkg/proxy/iptables/proxier.go` | **Implemented** (`iptables.rs`) |
| Proxy mode: nftables (KEP-3866, beta 1.31, GA target 1.33) | `pkg/proxy/nftables/` | **Missing** |
| Proxy mode: IPVS | `pkg/proxy/ipvs/proxier.go` | **Missing** |
| Proxy mode: winkernel (Windows HNS) | `pkg/proxy/winkernel/` | **Missing** (and out of scope) |
| MetaProxier (dual-stack coord) | `pkg/proxy/metaproxier/` | **Missing** |
| Service type ClusterIP | KUBE-SVC-* + KUBE-SEP-* | **Implemented** (`proxy.rs:414-423`) |
| Service type NodePort | KUBE-NODEPORTS + KUBE-SVC-* | **Implemented** (`proxy.rs:426-439`) |
| Service type LoadBalancer (LB VIP DNAT) | KUBE-EXT-*, KUBE-FW-* | **Partial** (LB treated as NodePort; no VIP-specific chain) |
| Service type ExternalName | DNS-only, no iptables | **Implemented** (skip path, `proxy.rs:298-302`) |
| `spec.externalIPs` DNAT | KUBE-EXT-* + KUBE-PROXY-FIREWALL | **Missing** |
| `spec.loadBalancerSourceRanges` (LB firewall) | KUBE-FW-* / KUBE-PROXY-FIREWALL | **Missing** |
| `externalTrafficPolicy=Local` (preserve source IP) | KUBE-XLB-* / KUBE-EXT-* | **Missing** |
| `internalTrafficPolicy=Local` (KEP-2086) | KUBE-SVL-* | **Missing** |
| Terminating-endpoint fallback (KEP-1669 `ProxyTerminatingEndpoints`) | `serving`/`terminating` filter | **Missing** (only `ready==false` filtered, `proxy.rs:111-114`) |
| KubeProxyDrainingTerminatingNodes (KEP-3458) | Node-level | **Missing** |
| Session affinity ClientIP | `recent` match in KUBE-SVC-* | **Implemented** with `xt_recent` (`iptables.rs:1743-1770`) and direct-DNAT fallback when xt_recent unavailable (`iptables.rs:1795-1819`) |
| Session affinity timeout (`timeoutSeconds`) | per-service | **Implemented** (`proxy.rs:354-360`, default 10800) |
| Endpoints API (v1) | `EndpointsCache` | **Implemented** (`proxy.rs:62-67`) |
| EndpointSlice API (`discovery.k8s.io/v1`) | `EndpointSliceCache` | **Implemented** (`proxy.rs:86-143`) |
| EndpointSlice port-name aware routing | yes | **Implemented** (`proxy.rs:374-411`) |
| Topology-aware routing / Topology Hints (KEP-2433) | `hints.forZones` | **Missing** |
| Dual-stack IPv4 + IPv6 (parallel proxiers) | MetaProxier + dual `Proxier` | **Missing** (only IPv4 rule generation) |
| Multi-CIDR Service CIDR (KEP-3552/1880) | Watches `ServiceCIDR`/`IPAddress` | **Missing** |
| MASQUERADE for hairpin (pod-to-its-own-VIP) | KUBE-POSTROUTING with mark | **Implemented** for container-bridge hairpin (`iptables.rs:299-352`) |
| MASQUERADE for off-cluster traffic | `--masquerade-all` flag + KUBE-MARK-MASQ | **Partial** (no `--masquerade-all`; conntrack-aware) |
| `--masquerade-bit` configuration | yes | **Missing** |
| `randomFully` MASQUERADE (better port allocation) | yes (since 1.19) | **Missing** |
| `--nodeport-addresses` (limit NodePort bind) | yes | **Missing** (binds 0.0.0.0) |
| `--detect-local-mode` (ClusterCIDR / NodeCIDR / iface / bridge) | yes | **Partial** (bridge-only auto-detect, `iptables.rs:30-81`) |
| `KUBE-PROXY-FIREWALL` (LB source ranges, ExternalIP filter) | yes | **Missing** |
| `KUBE-PROXY-CANARY` (rule-sync monitor) | yes | **Missing** |
| `healthCheckNodePort` HTTP listener | `healthcheck/` package | **Missing** |
| `/healthz` + `/livez` HTTP servers | yes | **Missing** |
| Prometheus metrics endpoint | `metrics/` | **Missing** |
| Conntrack flush on UDP svc/endpoint deletion | yes | **Missing** |
| Conntrack tuning (`max-per-core`, `min`, `tcp-timeout-established`, `tcp-timeout-close-wait`, `udp-timeout`) | yes | **Missing** |
| `bridge-nf-call-iptables` enable | yes | **Implemented** (`iptables.rs:218-`) |
| Atomic restore via `iptables-restore --noflush` | yes | **Implemented** (`iptables.rs:2061+`) |
| Large-cluster comment-elision (≥1k endpoints) | yes | **Missing** (always emits comments) |
| EndpointSliceCache merge of multiple slices per svc | yes | **Partial** (looped per slice, no merge into single per-svc cache entry) |
| Service change tracker (delta-driven sync) | `ServiceChangeTracker` | **Missing** (full-list reconcile each tick) |
| IPVS schedulers (`rr`, `lc`, `dh`, `sh`, `sed`, `nq`) | yes (in `ipvs/`) | **Missing** (no IPVS at all) |
| `ipset` integration | yes (IPVS mode) | **Missing** |
| `kube-proxy --cleanup` CLI | yes | **Missing** (Drop-time cleanup only) |
| Leader election | n/a (DaemonSet) | n/a |
| ConfigMap-based dynamic config (`KubeProxyConfiguration`) | yes | **Missing** (only CLI flags) |

## Missing features (detailed)

### 1. IPVS mode

`pkg/proxy/ipvs/proxier.go` programs a dummy interface (`kube-ipvs0`), binds
every ClusterIP to it, and installs IPVS virtual servers via netlink with one
of six schedulers (`rr`, `wrr`, `lc`, `wlc`, `dh`, `sh`, `sed`, `nq`). It uses
`ipset` for bulk-matching ExternalIPs / LoadBalancer IPs / NodePort addresses
to keep iptables rule count O(1) regardless of service count. Rusternetes has
no IPVS code path; large clusters (>5k services) will see kernel-rule walk
times dominate sync latency.

**Cost to add:** large. Requires netlink bindings (`rsnetlink` / `neli`),
ipset bindings, a parallel `Proxier` trait, and a CLI flag (`--proxy-mode ipvs`,
`--ipvs-scheduler`, `--ipvs-min-sync-period`, `--ipvs-sync-period`,
`--ipvs-tcp-timeout`, `--ipvs-tcpfin-timeout`, `--ipvs-udp-timeout`,
`--ipvs-exclude-cidrs`, `--ipvs-strict-arp`).

### 2. nftables mode (KEP-3866)

Upstream nftables backend graduated to beta in 1.31 and is the long-term
replacement for the iptables backend (see KEP-3705 "Cleanup IPTables Mode by
default"). It uses a single `kube-proxy` nft table with named sets / maps
(`service-ips`, `service-nodeports`, `cluster-ips`, `nodeport-ips`) and
`vmap` for O(1) endpoint dispatch. Rusternetes invokes the **legacy** iptables
binary; even when `detect_iptables_cmd` picks `/usr/sbin/iptables`
(`iptables.rs:9-25`), the rule format is still iptables, never native nft.

**Cost to add:** large. Either shell out to `nft -f -` with a templated nftables
script, or use `libnftnl` via FFI (no mature pure-Rust crate). New `Proxier`
impl; chain mapping is fundamentally different from the iptables one.

### 3. `externalTrafficPolicy=Local`

Upstream's KUBE-EXT-*/KUBE-XLB-* chains preserve the client source IP on
NodePort and LoadBalancer traffic by dropping packets that arrive on a node
with no local backend instead of SNAT-then-forwarding to a remote backend. The
Rusternetes NodePort code path (`proxy.rs:426-439`, `iptables.rs:1824-1955`)
load-balances across **all** endpoints regardless of node locality. Pods using
`spec.externalTrafficPolicy: Local` will not see their client IP preserved,
and traffic on nodes with no local backend won't be dropped (they'll proxy to
a remote pod, breaking the source-IP guarantee).

**Cost to add:** medium. Needs (a) per-endpoint node-name tracking from
EndpointSlice (`endpoint.nodeName` is already in the resource — see
`Endpoint::node_name` in `common/src/resources/endpointslice.rs`), (b) a
KUBE-XLB-equivalent chain that filters endpoints by `node_name == self.node_name`,
(c) a fallback "no local endpoints, drop" rule. Also needs a per-service
`healthCheckNodePort` HTTP listener (see item 9).

### 4. `internalTrafficPolicy=Local` (KEP-2086)

Same locality filter as #3 but applied to ClusterIP traffic from in-cluster
clients. Upstream uses `KUBE-SVL-*` chains. Rusternetes has neither the chain
nor a knob to drive it.

**Cost to add:** medium. Builds on #3's local-endpoint-set extraction; the
chain plumbing is a near-mirror of `add_clusterip_rules`.

### 5. Terminating endpoint fallback (KEP-1669)

When all `ready` endpoints for a service are gone but some endpoints are
`serving=true, terminating=true` (the pod has received SIGTERM but is still
draining connections), upstream proxies to the terminating set instead of
NXDOMAIN-ing. This is critical for zero-downtime rolling deploys on services
with very fast endpoint rotation.

Rusternetes (`proxy.rs:108-119`) only filters `conditions.ready == Some(false)`
— `serving` and `terminating` are read into the struct but never consulted.
A pod in `TerminationGracePeriod` with `ready=false, serving=true,
terminating=true` is dropped from the backend pool immediately.

**Cost to add:** small. Filter logic in `sync()` already loops over
`endpoint.conditions`; need a two-tier "ready-or-fallback-to-serving" pool
construction plus the matching match-by-`serving`-not-`terminating` selection
in `build_nat_rules`.

### 6. Topology-aware routing / Topology Hints (KEP-2433)

When EndpointSlice carries `hints.forZones=[{name: "us-east-1a"}]`, upstream
prefers in-zone endpoints. Rusternetes ignores `Endpoint.hints` entirely
(`proxy.rs:108-143` reads `endpoint.conditions` and `endpoint.addresses` only).

**Cost to add:** medium. Needs (a) self-zone discovery — read the local
`Node` object's `topology.kubernetes.io/zone` label, (b) hint-aware endpoint
filtering with the documented "always use all endpoints if any hint is
missing/insufficient" safety fallback, (c) a watch on `Node` since the local
node label can change.

### 7. Dual-stack IPv4 + IPv6

Rusternetes builds **one** rule set assuming IPv4 (`iptables.rs:1670-1818`
uses `-d {}/32`, never `/128`; `cluster_cidr` is parsed as a single string;
`detect_bridge_network` matches only IPv4 private ranges). A dual-stack
service with `.spec.clusterIPs=["10.96.0.10","fd00::1"]` will only get the v4
ClusterIP wired up.

**Cost to add:** medium. Mirror the existing IptablesManager for `ip6tables`
(/`ip6tables-restore`), thread two cluster-CIDRs, and split MetaProxier-style
on `clusterIPs`/`ipFamilies`. The trickiest piece is bridging through
upstream's `MetaProxier` API surface — Rusternetes doesn't need that exact
API but does need both v4 and v6 syncs to be independent (a v6 iptables-restore
error must not stop the v4 sync).

### 8. ExternalIPs + LoadBalancerSourceRanges (KUBE-FW / KUBE-PROXY-FIREWALL)

`spec.externalIPs[]` lets users assign cluster-routable VIPs from outside the
service CIDR; upstream wires them into KUBE-EXT-*. `spec.loadBalancerSourceRanges`
allows-lists client CIDRs at the LB VIP, enforced by KUBE-FW-* (and on modern
kube-proxy, KUBE-PROXY-FIREWALL on the INPUT chain). Rusternetes has neither
chain; LoadBalancer is treated as NodePort with no VIP-specific DNAT and no
source-range filter.

**Cost to add:** small for externalIPs (mirror NodePort path with a `-d <eip>`
match), medium for source-range firewall (needs a new `*filter` table write,
which the current code does not emit at all — only `*nat`).

### 9. healthCheckNodePort HTTP listener

For `Service.spec.type=LoadBalancer` with `externalTrafficPolicy=Local`,
upstream serves an HTTP endpoint on `spec.healthCheckNodePort` that returns
`200 OK` when the node has ≥1 ready local endpoint and `503` otherwise. This
is how external load balancers (AWS NLB, GCP TCP/SSL LB) decide which nodes
to send traffic to. Rusternetes has no HTTP listener at all in kube-proxy.

**Cost to add:** small. Standalone `axum` server bound to the configured
NodePort range; checks the local-endpoint count out of the EndpointSlice
cache.

### 10. `/metrics`, `/healthz`, `/livez`

Upstream exposes Prometheus metrics
(`sync_proxy_rules_duration_seconds`, `sync_proxy_rules_endpoint_changes_total`,
`sync_proxy_rules_iptables_total`, `kubernetes_build_info`, …) and HTTP
healthz/livez probes on `--healthz-bind-address` and `--metrics-bind-address`.
Rusternetes only logs via `tracing` — no Prometheus, no readiness signal for
the kubelet/static pod that runs it.

**Cost to add:** small. The `prometheus` crate is already a dependency
elsewhere in the workspace; wire up a counter/histogram set in `KubeProxy::sync`
and a one-file axum HTTP layer.

### 11. Conntrack flush on UDP service / endpoint deletion

DNS, syslog, NTP, NFS — all sit on UDP services where the conntrack table
caches the post-DNAT 5-tuple. When an endpoint is removed (pod deletion), the
existing conntrack entries point at the dead pod IP and the next client
packet is silently black-holed until the conntrack entry expires (default 30s
for unanswered UDP). Upstream calls `conntrack -D --orig-dst <svcIP> -p udp`
on every UDP service/endpoint deletion. Rusternetes never touches conntrack.

**Cost to add:** small. `conntrack` binary or `nfnetlink_conntrack` netlink
call. Tricky bit is detecting deletions in the change tracker (which doesn't
exist yet — see #14).

### 12. `randomFully` MASQUERADE + masquerade-bit / masquerade-all

Upstream emits MASQUERADE with `--random-fully` (since 1.19) to fix the
SNAT-port-exhaustion bug that caused 5-second connection stalls on busy
clusters (the well-known "iptables --random-fully" CoreDNS bug). It also
supports `--masquerade-all` (mark every cluster-bound packet for SNAT, used
on multi-NIC nodes where the in-cluster route isn't the default) and
`--masquerade-bit` (which fwmark bit identifies "needs SNAT" — defaults to
14). Rusternetes' MASQUERADE rule (`iptables.rs:299-352`) has none of these:
no `--random-fully`, no mark-based architecture (it conditions directly on
src/dst CIDRs), no flag to force masquerade-all.

**Cost to add:** small. Append `--random-fully` to the MASQUERADE statement
when the kernel supports it; add CLI flags and gate the broader-mask rule.

### 13. `--nodeport-addresses` filter

By default upstream listens on `0.0.0.0` (NodePorts answer on every node IP).
`--nodeport-addresses=10.0.0.0/8,192.168.0.0/16` restricts the bind to a set
of CIDRs, which is critical on dual-NIC nodes where you don't want NodePorts
exposed on the public NIC. Rusternetes has no equivalent flag — every node IP
answers every NodePort.

**Cost to add:** small. Filter the per-NodePort rule with `-d <cidr>` for
each configured address range.

### 14. Service change tracker / EndpointSliceCache (delta-driven sync)

Every Rusternetes `sync()` does a full `list("/registry/services/")`,
`list("/registry/endpoints/")`, `list("/registry/endpointslices/")`,
`list("/registry/pods/")` (`proxy.rs:42-89`) and then computes an
order-independent XOR hash to short-circuit if nothing changed
(`proxy.rs:152-229`). Upstream's `ServiceChangeTracker` and
`EndpointSliceCache` accumulate per-resource deltas across watch events and
emit only the changed service set into the proxier; a 10k-service cluster
sees this as the difference between O(N) and O(Δ).

**Cost to add:** medium-large. Touches lib.rs's watch loop (already there),
the proxy.rs sync entry point, and the iptables.rs rule builder (currently
"write everything"). The hash-skip optimization already helps for **no**-op
syncs but does nothing for "one service changed, redraw 10k rules".

### 15. Per-rule metric counters / large-cluster comment-elision

Upstream emits per-rule `-m comment --comment "<ns>/<name>:<port> cluster IP"`
in non-large mode, and **elides** comments past 1000 endpoints because the
comment-walking overhead in kernel matchers becomes the bottleneck.
Rusternetes always emits comments (e.g.
`"rusternetes service hairpin masquerade"` in `iptables.rs:312`, the per-rule
comments at `iptables.rs:1814-1815`), with no scale-out threshold.

**Cost to add:** trivial. A `services.len() < 1000` check around the comment
formatter.

## Partial / stubbed

- **LoadBalancer Service type** — programmed as a NodePort + ClusterIP, but
  there is no DNAT-on-VIP for `status.loadBalancer.ingress[].ip`. If an
  external LB is set up to point at a node and arrives with `dst=VIP`, the
  packet won't match any rule and will be dropped. In real clusters
  Rusternetes-managed LoadBalancers expose only NodePort, never the VIP.
- **Session affinity** — fully working with `xt_recent` (`iptables.rs:1743-`),
  and a direct-DNAT fallback when the module isn't loaded
  (`iptables.rs:1795-`). The fallback **doesn't actually preserve session
  affinity** — it just does plain round-robin DNAT. That's correct for
  liveness but a silent correctness gap if a user expects sticky sessions on
  a host where xt_recent is unavailable. Upstream's iptables backend has the
  same limitation (it logs and continues); upstream IPVS uses the IPVS
  persistence flag instead.
- **`detect_local_mode`** — Rusternetes has a bridge-network detector
  (`iptables.rs:30-81`) which approximates `--detect-local-mode=Bridge-Interface`.
  Other upstream modes (ClusterCIDR, NodeCIDR, InterfaceNamePrefix) are not
  implemented.
- **Atomic restore** — implemented (`iptables.rs:2061+`), with a flush+rebuild
  fallback (`proxy.rs:250-267`). The fallback has the classic "no rules
  during rebuild" window upstream's never had since the iptables-restore
  switchover (~1.11). Worth keeping the fallback for now since
  iptables-restore is the most likely thing to fail in CI sandboxes, but it
  should be loud (`error!`) and rare.
- **Periodic resync** — 10s constant (`lib.rs:148`). Upstream is configurable
  (`--sync-period`, `--min-sync-period`).

## Known in-code TODOs

`grep -n "TODO\|FIXME\|XXX\|HACK" crates/kube-proxy/src/*.rs` returns no
matches. The crate has no in-source TODO markers for the gaps above — they
are documented here for the first time, in this file.

## References

- Upstream package root —
  <https://github.com/kubernetes/kubernetes/tree/master/pkg/proxy>
- iptables proxier —
  <https://github.com/kubernetes/kubernetes/blob/master/pkg/proxy/iptables/proxier.go>
- IPVS proxier —
  <https://github.com/kubernetes/kubernetes/blob/master/pkg/proxy/ipvs/proxier.go>
- nftables proxier —
  <https://github.com/kubernetes/kubernetes/tree/master/pkg/proxy/nftables>
- winkernel proxier —
  <https://github.com/kubernetes/kubernetes/tree/master/pkg/proxy/winkernel>
- MetaProxier —
  <https://github.com/kubernetes/kubernetes/tree/master/pkg/proxy/metaproxier>
- KEP-1669 — `ProxyTerminatingEndpoints` (graceful termination fallback)
- KEP-1672 — DSR + internal traffic policy precursor
- KEP-2086 — `Service.spec.internalTrafficPolicy`
- KEP-2433 — Topology Aware Hints (`hints.forZones`)
- KEP-3458 — `KubeProxyDrainingTerminatingNodes` (node-level draining)
- KEP-3552 / KEP-1880 — Multi-CIDR Service CIDR / Multiple ClusterCIDRs
- KEP-3705 — Deprecate the iptables proxy mode in favor of nftables
- KEP-3866 — nftables backend
- Upstream issue tracker discussion of the `--random-fully` MASQUERADE fix —
  <https://github.com/kubernetes/kubernetes/issues/76699>
- Rusternetes inventory used to compile this doc:
  - `crates/kube-proxy/src/lib.rs`
  - `crates/kube-proxy/src/main.rs`
  - `crates/kube-proxy/src/proxy.rs`
  - `crates/kube-proxy/src/iptables.rs`
