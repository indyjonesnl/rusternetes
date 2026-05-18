# Missing Features — cluster bootstrap / kubeadm-equivalent

## Scope

This document compares the Rusternetes cluster bootstrap surface — the
all-in-one binary (`crates/rusternetes/src/main.rs`), the helper shell
scripts (`scripts/bootstrap-cluster.sh`, `scripts/generate-certs.sh`,
`scripts/generate-default-serviceaccounts.sh`,
`scripts/generate-sa-signing-key.sh`), the cluster manifest
(`bootstrap-cluster.yaml`), and the compose topologies
(`compose.yml`, `compose.sqlite.yml`, `compose.redis.yml`,
`compose.all-in-one.yml`, `compose.all-in-one-redis.yml`,
`compose.ha.yml`) — against upstream Kubernetes' kubeadm
(`cmd/kubeadm/app/`) and the bootstrap-token / TLS-bootstrap flow
(`pkg/controller/bootstrap`, `pkg/controller/certificates`).

Out of scope: individual component flags (covered by their own gap
docs), CNI plugin installation (kubeadm explicitly does not install
one; the CNI gap doc owns this), self-hosting (deprecated upstream),
and the storage encryption-at-rest pipeline (covered by
`docs/missing-features/storage.md`). In scope: everything between
"I have N nodes with a container runtime and a Rusternetes binary"
and "I have a multi-node cluster with rotating certificates,
joinable workers, and an upgrade path."

## Current Rusternetes state

### All-in-one binary

`crates/rusternetes/src/main.rs:127-281` is the entire bootstrap
sequence: parse CLI args, build a single `StorageBackend`
(SQLite default, `main.rs:145-176`), then `tokio::spawn` five
long-lived tasks — api-server (`main.rs:181-198`), scheduler
(`204-212`), controller-manager (`215-223`), kubelet (`226-245`),
and kube-proxy (`248-264`, optional via `--disable-proxy`).

CLI knobs (`main.rs:17-125`): storage backend
(`sqlite`/`etcd`/`redis`) + connection args; `--bind-address
0.0.0.0:6443`; single hard-coded `--node-name node-1`;
`--cluster-dns 10.96.0.10`; TLS via `--tls` + `--tls-cert-file`
/ `--tls-key-file` / `--tls-san localhost,127.0.0.1`
(auto-generates a self-signed cert if `--tls` set without files);
sync intervals; `--cluster-cidr 10.0.0.0/24`,
`--node-port-range 30000-32767`; `--skip-auth true` (insecure
default); `--disable-proxy`, `--console-dir`,
`--kubernetes-service-host`, `--client-ca-file`.

There is no `init` / `join` / `reset` / `upgrade` / `token` /
`certs` / `config` subcommand — the binary is a daemon, not a
bootstrap tool.

### Helper scripts

- `scripts/generate-certs.sh` — produces `api-server.{crt,key}`
  (P-256 ECDSA, 10-year validity, single cert acting as its own
  CA via `basicConstraints = critical, CA:TRUE` and EKU =
  `serverAuth, clientAuth`,
  `generate-certs.sh:80-117`) plus an RSA-2048 `sa.{key,pub}`
  pair for ServiceAccount-token signing. SANs are hard-coded
  (`localhost`, `kubernetes.default.svc.cluster.local`,
  `10.96.0.1`) plus auto-detected from the
  `rusternetes-network` bridge (`generate-certs.sh:48-108`). The
  cert is copied to `ca.crt` and into the CoreDNS volume
  (`generate-certs.sh:122-129`).
- `scripts/generate-default-serviceaccounts.sh` — writes
  `.rusternetes/default-serviceaccounts.yaml` with
  `openssl rand -base64 64` tokens for the `default` SA in
  `default` and `kube-system`. Externally generated because no
  TokenController auto-populates these on first boot
  (script comments at lines 41-42).
- `scripts/bootstrap-cluster.sh` — orchestrator: detect runtime,
  run SA generator, `kubectl apply` the SA YAML, wipe leftover
  CoreDNS state, `kubectl apply bootstrap-cluster.yaml`, poll
  for CoreDNS `Running`.

### Cluster manifest (`bootstrap-cluster.yaml`, 153 lines)

Defines `Namespace/{default,kube-system}`,
`Service/default/kubernetes` pinned to `10.96.0.1:443→6443`,
`PriorityClass/system-{node,cluster}-critical`,
`Pod/kube-system/coredns` (image `coredns/coredns:1.14.3`,
`restartPolicy: Always`, NOT a Deployment),
`Service/kube-system/kube-dns` pinned to `10.96.0.10`, and the
CoreDNS Corefile ConfigMap (which talks to the api-server at
`https://api-server:6443` using the copied CA cert).

### Compose topologies

Multi-container (`compose.yml`, `compose.sqlite.yml`,
`compose.redis.yml`): one storage container
(`etcd`/`rhino`/`redis`), one api-server (`6443/TLS`), one
scheduler, one controller-manager, two kubelets (`node-1` on
metrics 10250 and `node-2` on 10251 — both talking to the api
via `KUBERNETES_SERVICE_HOST_OVERRIDE=api-server`), one
kube-proxy in host-network mode with `NET_ADMIN` /
`NET_RAW` / `SYS_ADMIN`.

All-in-one (`compose.all-in-one.yml`,
`compose.all-in-one-redis.yml`): single container running the
`rusternetes` binary (plus a `redis` sidecar in the latter). No
kube-proxy (iptables generally unavailable inside a container).

HA (`compose.ha.yml`, 356 lines): 3-node etcd quorum, 3
api-servers behind `haproxy`, 2 schedulers and 2
controller-managers using Lease-based leader election
(`crates/controller-manager/src/main.rs:66-185`,
`crates/common/src/resources/coordination.rs:118`). No
upload-certs / control-plane-join flow — the topology is
static.

### What's already in the codebase

- `CertificateSigningRequest` REST handler
  (`crates/api-server/src/handlers/certificates.rs`, 372 lines)
  + `kubectl certificate approve|deny`
  (`crates/kubectl/src/commands/certificate.rs`).
- `CertificateSigningRequestController`
  (`crates/controller-manager/src/controllers/certificate_signing_request.rs`,
  620 lines) — auto-approves CSRs with `signerName` of
  `kubernetes.io/kube-apiserver-client-kubelet` or
  `kubernetes.io/kubelet-serving` (lines 216-242), then writes
  the `Approved` condition with `status.certificate = None` and
  the comment "External signer will add the certificate"
  (lines 244-283).
- `BootstrapToken` + `BootstrapTokenManager`
  (`crates/common/src/auth.rs:252-518`) — parses
  `token-id.token-secret`, validates expiry, emits a
  `UserInfo { username: "system:bootstrap:<id>", groups:
  ["system:bootstrappers"] }`. Wired into auth middleware
  (`crates/api-server/src/middleware.rs:52-77`).

### What's *not* in the codebase

No `kubeadm`-equivalent CLI; no CSR signer (only approval); no
controller seeding `BootstrapTokenManager` from
`bootstrap.kubernetes.io/token` Secrets; no kubelet-side TLS
bootstrap (CSR submission + cert pivot); no `kubeadm-config` or
`kubelet-config-N.N` ConfigMap; no certificate rotation; no
multi-CA PKI tree (one cert acts as both leaf and CA); no static
Pod manifests in `/etc/kubernetes/manifests`; no upgrade flow;
no `kube-public/cluster-info` discovery doc; no
`ClusterTrustBundle` distribution.

## Parity matrix

| Feature | Upstream (kubeadm / KEP) | Rusternetes | Notes |
| --- | --- | --- | --- |
| `init` subcommand | `kubeadm init` with 14 phases | NO | Bootstrap is shell + `tokio::spawn` |
| Phase: preflight | port-bind / swap / cgroup / kernel-module checks | NO | no preflight validation |
| Phase: certs | 11-cert PKI tree | partial — single leaf+CA cert | see Missing #1 |
| Phase: kubeconfig | `admin.conf`, `super-admin.conf`, `kubelet.conf`, `controller-manager.conf`, `scheduler.conf` | NO — only `kubeconfig.example.yaml` template | see Missing #2 |
| Phase: etcd | local stacked etcd via static Pod | external (compose service) | architectural choice |
| Phase: control-plane | static Pod manifests in `/etc/kubernetes/manifests` | NO — components run as containers / tasks | architectural choice |
| Phase: kubelet-start | writes `kubelet.conf` + `kubelet-config.yaml`, starts kubelet | NO | see Missing #3 |
| Phase: upload-config | `kubeadm-config` + `kubelet-config-N.N` ConfigMaps | NO | see Missing #4 |
| Phase: upload-certs | encrypted `kubeadm-certs` Secret | NO | needed for HA control-plane join |
| Phase: mark-control-plane | `node-role.kubernetes.io/control-plane:NoSchedule` taint + label | NO | node labels static / unset |
| Phase: bootstrap-token | default bootstrap-token Secret (24h TTL) | NO — only in-memory tokens | see Missing #5 |
| Phase: kubelet-finalize | client-cert pivot after bootstrap | NO | see Missing #3 |
| Phase: addon coredns | 2-replica Deployment + RBAC | partial — single Pod, no RBAC | see Missing #6 |
| Phase: addon kube-proxy | DaemonSet + ConfigMap | NO — kube-proxy is a compose service | see Missing #6 |
| `join` subcommand | `kubeadm join` with `discovery-token-ca-cert-hash` | NO | nodes added by editing compose |
| `join` discovery | TLS-pinned via `cluster-info` ConfigMap in `kube-public` | NO | no `kube-public` namespace |
| `reset` subcommand | wipes a node (mounts, manifests, iptables, kubelet config) | NO — `unbreak-host-dns.sh` only | see Missing #9 |
| `upgrade` subcommand | `plan / apply / node / diff` | NO | see Missing #10 |
| `certs renew` / `check-expiration` | renew + report NotAfter per cert | NO | 10-year validity, manual `rm` |
| `token create/list/delete` | manages bootstrap-token Secrets | NO | only programmatic `add_token` |
| `config print init-defaults` / `migrate` | versioned `ClusterConfiguration` v1beta3 → v1beta4 | NO | no config-as-API |
| Bootstrap-token Secret type (KEP-1003) | `bootstrap.kubernetes.io/token` Secret | NO | see Missing #5 |
| TLS bootstrap flow (KEP-2453) | kubelet CSR + approver + signer + pivot | partial — CSR + approver only | see Missing #7 |
| `RotateKubeletClientCertificate` | kubelet auto-renews client cert | NO | kubelet hard-codes auth |
| `RotateKubeletServerCertificate` | kubelet auto-renews serving cert | NO | no kubelet serving cert |
| CSR signers (`kubelet-client`, `kubelet-serving`, `kube-apiserver-client`, `legacy-unknown`) | `csrsigning` controller | NO | approver-only, no signer |
| `csrapproving` controller | RBAC-gated auto-approval | partial — hard-coded signer-name check, no RBAC gate | `certificate_signing_request.rs:216-242` |
| Front-proxy CA (aggregation layer) | separate `front-proxy-ca` + `front-proxy-client` | NO | aggregation API not implemented |
| etcd PKI (peer + server + client) | separate `etcd-ca` + `apiserver-etcd-client` | NO — plaintext HTTP on the bridge | data path unencrypted |
| ServiceAccount key rotation | KMS-driven, JWKS at `/openid/v1/jwks` | partial — JWKS served, no rotation | issuer-discovery handler exists |
| `kube-public/cluster-info` ConfigMap | discovery doc with CA hash | NO | requires `--insecure-skip-tls-verify` today |
| ClusterTrustBundle (KEP-3257) | signer-scoped trust bundle distribution | NO | see Missing #11 |
| HA control-plane join | upload-certs + control-plane phase of `join` | NO — static topology in `compose.ha.yml` | see Missing #8 |
| Leader election | Lease-based via `coordination.k8s.io/v1` | partial — controller-manager only | scheduler has no `--leader-elect` |
| Encryption-at-rest (`EncryptionConfiguration`) | aescbc / aesgcm / secretbox / kms-v2 transformers | NO | see Missing #12 |

## Missing features

### 1. Real PKI hierarchy

Upstream `kubeadm init phase certs` produces eleven cert / key
files in `/etc/kubernetes/pki/`: `ca.{crt,key}`,
`front-proxy-ca.{crt,key}`, `front-proxy-client.{crt,key}`,
`etcd/{ca,server,peer,healthcheck-client}.{crt,key}`,
`apiserver-etcd-client.{crt,key}`,
`apiserver-kubelet-client.{crt,key}`, `apiserver.{crt,key}`,
and `sa.{pub,key}`.

Rusternetes produces three files: `api-server.{crt,key}` and
`sa.{key,pub}` (`generate-certs.sh:14-23, 38-117`). The single
`api-server.crt` acts as its own CA, is reused as the client
cert any in-cluster component would need, and is copied into
`ca.crt` (`generate-certs.sh:128-129`).

Consequences: the aggregation layer
(`apiservices.apiregistration.k8s.io`) cannot be secured with
no front-proxy CA; etcd is exposed on `0.0.0.0:2379` over
plaintext HTTP (`compose.yml:7-11`); api-server-to-kubelet
calls (`exec`, `logs`) are not TLS-authenticated by a separate
client cert; and rotation has nothing to rotate against — a
single 10-year cert with no CA means rotation = wipe-and-regenerate.

Landing this means a Rust subcommand (e.g.
`rusternetes pki init` / `pki renew`) that emits the full tree
with kubeadm's file names so existing tooling
(`kubeadm certs check-expiration`-style audit) is portable.

### 2. Component kubeconfigs

Upstream writes `admin.conf`, `super-admin.conf` (since 1.29),
`kubelet.conf` (`system:node:<name>`, group `system:nodes`),
`controller-manager.conf` (`system:kube-controller-manager`),
and `scheduler.conf` (`system:kube-scheduler`) to
`/etc/kubernetes/`.

Rusternetes ships `kubeconfig.example.yaml` as a template. No
in-cluster component has its own kubeconfig — they either rely
on the default `--skip-auth=true` (`main.rs:106-107`) or, in
the all-in-one binary, share process memory via the `Storage`
trait without an HTTP round-trip. When `--skip-auth=false` is
set, controllers and the scheduler have no way to
authenticate.

Landing this means each `*.conf` file is signed by `ca.key`
during `pki init`, and each component CLI grows a
`--kubeconfig` flag that takes precedence over `--skip-auth`.

### 3. Kubelet TLS bootstrap

Upstream flow (KEP-2453): provisioner places
`/etc/kubernetes/bootstrap-kubelet.conf` with a
`bootstrap.kubernetes.io/token` token (24h TTL) → kubelet
starts, generates a keypair, submits a CSR with
`signerName: kubernetes.io/kube-apiserver-client-kubelet`,
subject `CN=system:node:<nodename>, O=system:nodes` →
`csrapproving` approves (RBAC-gated) → `csrsigning` signs with
the cluster CA → kubelet polls `status.certificate`, pivots to
`kubelet-client-current.pem` + writes
`/etc/kubernetes/kubelet.conf` → near expiry, kubelet repeats
to rotate (`RotateKubeletClientCertificate`, GA since 1.19;
`RotateKubeletServerCertificate` does the same for serving
certs via `kubelet-serving`).

Rusternetes implements steps 2 and 3 on the api-server side
(CSR REST resource + auto-approver). Steps 4-6 are missing:

- Step 4 — no signer; the approver writes `Approved` and
  leaves `status.certificate = None`
  (`certificate_signing_request.rs:270`).
- Step 5 — `crates/kubelet/` has no bootstrap-kubeconfig
  loader, no CSR submission, no file pivot.
- Step 6 — no rotation logic anywhere.

Without this flow, every kubelet either uses `--skip-auth` or
shares the single self-signed cert from `generate-certs.sh`,
which provides no per-node identity.

### 4. Cluster config persistence

Upstream stores `init` args as a versioned
`ClusterConfiguration` ConfigMap (`kubeadm-config` in
`kube-system`) and the rendered kubelet config as
`kubelet-config-N.N` (one per minor version). `kubeadm upgrade
plan` reads both.

Rusternetes has no equivalent. The all-in-one binary's args
live only in the process arglist; restarting with different
flags silently changes behavior. The compose files encode the
args, but there's no in-cluster ConfigMap an operator can read
to know how the cluster was bootstrapped.

Landing this means a `kubeadm-config`-style ConfigMap written
by `rusternetes init` carrying `ClusterConfiguration`,
`InitConfiguration`, `KubeletConfiguration`,
`KubeProxyConfiguration` documents, with each component
preferring the in-cluster ConfigMap to its own CLI flags
(like upstream's `kubelet --config`).

### 5. Persistent bootstrap tokens

`BootstrapToken` + `BootstrapTokenManager`
(`crates/common/src/auth.rs:252-518`) is an in-memory
`HashMap<token_id, BootstrapToken>` keyed by id. The middleware
validates bearer tokens against it (`middleware.rs:75-77`).
Missing:

- No controller that watches `kube-system` Secrets of type
  `bootstrap.kubernetes.io/token` and seeds the manager
  (upstream's `bootstrap.TokenCleaner` / `tokensManager` in
  `pkg/controller/bootstrap/`).
- No way to `kubectl create -f bootstrap-token.yaml` and have
  it actually authenticate.
- No `kubeadm token list / create / delete` UX.
- No TTL enforcement: `BootstrapToken::is_expired()` exists
  (test at `auth.rs:915-930`) but no controller deletes
  expired tokens.

Until a Secret-backed populator lands, the entire bootstrap-token
machinery is dead code.

### 6. Addon manifests are not idiomatic

`bootstrap-cluster.yaml` ships CoreDNS as a single `Pod` (not a
`Deployment`), with no `ServiceAccount/coredns` + RBAC
(it runs as `default` SA in `kube-system`), and the image
`coredns/coredns:1.14.3` is unpinned by digest. A node failure
permanently loses cluster DNS until manual repair. Upstream
kubeadm ships CoreDNS as a 2-replica Deployment with pod
anti-affinity, dedicated SA + ClusterRoleBinding, and an addon
manifest gated by feature flag.

Kube-proxy is missing from `bootstrap-cluster.yaml` entirely —
it's a compose service in `compose.yml:170-188`, not an
in-cluster DaemonSet. Consequences: `kubectl get pods -n
kube-system` doesn't show it; it can't be upgraded by changing
a DaemonSet image; a new node added to the cluster does NOT
automatically get kube-proxy (the operator must edit compose).

Landing this means a `manifests/` directory written by `init`,
reconciled by an addon controller (upstream's
`cluster/addons/addon-manager/` pattern).

### 7. CSR signer

`CertificateSigningRequestController` approves CSRs but does
not sign them (`certificate_signing_request.rs:244-283`,
literal `certificate: None, // External signer will add the
certificate`). For Missing #3 to work end-to-end, the
controller needs a `sign_csr` step that loads the cluster CA
key, parses `spec.request`, validates the requested CN / O /
SANs against signer policy:

- `kubernetes.io/kube-apiserver-client-kubelet` —
  `CN=system:node:<name>, O=system:nodes`, no DNS / IP SANs.
- `kubernetes.io/kubelet-serving` — same subject, DNS / IP
  SANs taken only from the node's `Status.Addresses`.
- `kubernetes.io/kube-apiserver-client` — RBAC-gated to the
  requester.
- `kubernetes.io/legacy-unknown` — disabled by default since
  1.18.

It must issue X.509 certs with the right EKU
(`clientAuth` vs `serverAuth`), write the PEM into
`status.certificate`, and honour `spec.expirationSeconds`
(KEP-2057, GA 1.24). Without a signer, the approval is half a
feature: clients waiting on `status.certificate` block
indefinitely.

### 8. Multi-node join

The closest available "add a worker" action is editing
`compose.yml` to add a third kubelet entry and `podman compose
up -d`. Implications: shared cert volume, manual non-colliding
`--node-name`, full cluster restart, no discovery, no
CA-hash-pinned bootstrap.

Upstream `kubeadm join` solves this with two flows:

- Worker join — discovery via `cluster-info` ConfigMap in
  `kube-public` (hash-pinned), bootstrap-token auth, TLS
  bootstrap (Missing #3). One command per node:
  `kubeadm join 10.0.0.5:6443 --token abcdef.0123456789abcdef
  --discovery-token-ca-cert-hash sha256:...`.
- Control-plane join — adds an etcd member, pulls the
  encrypted `kubeadm-certs` Secret to seed local PKI, joins as
  worker plus marks itself control-plane.

Until these land, Rusternetes' HA / multi-node posture is
static-topology only.

### 9. Reset / wipe / cleanup

`kubeadm reset` reverses every side effect of `init` / `join`:
unmounts pod volumes, removes static-pod manifests, removes
`/etc/kubernetes`, clears iptables (`-F`, `-t nat -F`,
`-t mangle -F`, `ipvsadm -C` if relevant), drops kubelet
config, optionally cleans CNI interfaces.

Rusternetes has `scripts/unbreak-host-dns.sh` for the specific
kube-proxy MASQUERADE-rule leak that the user has historically
tripped (referenced in `MEMORY.md`), but no general reset. The
operator removes `.rusternetes/`, the etcd/SQLite volumes, and
the `rusternetes-network` bridge by hand.

### 10. Upgrades

`kubeadm upgrade plan` reads `kubeadm-config` (Missing #4),
checks etcd / control-plane versions, warns about deprecated
APIs, outlines the upgrade path. `kubeadm upgrade apply
v1.X.Y` rotates control-plane manifests one at a time,
upgrades etcd, re-renders kubelet config. `kubeadm upgrade
node` does the per-node kubelet config / cert rotation.

Rusternetes has none of this. The implicit flow is "rebuild
the container image, `compose down`, `compose up`". Storage
compatibility across versions is best-effort (etcd / rhino
just store JSON; old data usually decodes).

### 11. ClusterTrustBundle distribution (KEP-3257)

Upstream introduces `ClusterTrustBundle` resources distributed
to nodes via a kubelet projected volume, replacing ad-hoc CA
cert injection. Rusternetes injects the api-server cert into
the CoreDNS pod out-of-band by `cp`-ing it into a host
directory (`generate-certs.sh:122-129`). No
`ClusterTrustBundle` resource, no kubelet projected-volume
implementation.

### 12. Encryption-at-rest for Secrets

Outside kubeadm but adjacent: `EncryptionConfiguration`
(`--encryption-provider-config` on the api-server) lets etcd
data be encrypted at rest via aescbc / aesgcm / secretbox /
kms-v2 transformers. Rusternetes has neither the api-server
flag nor the value-transformer plumbing — etcd / rhino store
plaintext JSON. Also called out in `docs/missing-features/storage.md`
"Missing #4"; listed here because kubeadm wires
`EncryptionConfiguration` up as part of `init phase certs` for
clusters that opt in.

## Partial / stubbed

- **Bootstrap tokens — middleware-only path.** Auth middleware
  parses the token format and emits `system:bootstrappers`
  (`middleware.rs:75-77`), but no Secret-backed populator
  exists, so the manager is permanently empty in practice.
- **CSR approval — no RBAC check.** Auto-approval is keyed on
  `signer_name` alone (`certificate_signing_request.rs:216-242`).
  Upstream `csrapproving` demands the requester has
  `create certificatesigningrequests/<signer-suffix>` RBAC
  permission. Without this, any client that can POST a CSR
  gets a kubelet cert (once Missing #7 lands).
- **Leader election — controller-manager only.** Scheduler has
  no `--leader-elect` flag. `compose.ha.yml` runs two
  schedulers but both reconcile every Pending Pod (best-effort
  idempotent on the storage layer; not a correctness
  guarantee).
- **PriorityClasses bootstrapped — but only two.** Upstream
  also creates `system-node-critical-no-preempt` and reserves
  the `system-` prefix at the admission layer; Rusternetes'
  admission does not reject user-created `system-*`
  PriorityClasses.
- **`kube-public` namespace.** Not created by
  `bootstrap-cluster.yaml`. The `cluster-info` ConfigMap that
  underpins `kubeadm join` discovery has nowhere to live.

## Known in-code TODOs

- `crates/controller-manager/src/controllers/certificate_signing_request.rs:19-21`
  — "Actual certificate signing is typically handled by
  external signers like cert-manager or cloud provider
  certificate managers in production." (Marker that the signer
  is intentionally absent.)
- `crates/controller-manager/src/controllers/certificate_signing_request.rs:270`
  — `certificate: None, // External signer will add the
  certificate`.
- `scripts/generate-default-serviceaccounts.sh:41-42`
  — "In production Kubernetes, ServiceAccounts and tokens are
  created automatically by the ServiceAccount controller and
  TokenController."
- `crates/controller-manager/src/lib.rs:41` — "No leader
  election in all-in-one mode — single instance" (intentional
  for the embedded binary; needs revisiting for
  HA-on-compose).
- `crates/rusternetes/src/main.rs:201` — fixed 1-second sleep
  to let the api-server bind before clients start; replace
  with a readiness probe.

## References

Source files cited above:

- `crates/rusternetes/src/main.rs`
- `crates/api-server/src/handlers/certificates.rs`,
  `crates/api-server/src/middleware.rs`
- `crates/common/src/auth.rs`,
  `crates/common/src/resources/certificates.rs`,
  `crates/common/src/resources/coordination.rs`
- `crates/controller-manager/src/lib.rs`,
  `crates/controller-manager/src/main.rs`,
  `crates/controller-manager/src/controllers/certificate_signing_request.rs`
- `crates/kubectl/src/commands/certificate.rs`,
  `crates/kubectl/src/commands/config.rs`
- `scripts/bootstrap-cluster.sh`,
  `scripts/generate-certs.sh`,
  `scripts/generate-default-serviceaccounts.sh`,
  `scripts/generate-sa-signing-key.sh`,
  `scripts/unbreak-host-dns.sh`
- `bootstrap-cluster.yaml`
- `compose.yml`, `compose.sqlite.yml`, `compose.redis.yml`,
  `compose.all-in-one.yml`, `compose.all-in-one-redis.yml`,
  `compose.ha.yml`
- `docs/BOOTSTRAP.md`, `docs/HIGH_AVAILABILITY.md`,
  `docs/TLS_GUIDE.md`, `docs/AUTHENTICATION.md`

Upstream:

- `kubernetes/kubernetes` —
  `cmd/kubeadm/app/cmd/{init,join,reset,upgrade,token,certs,config}.go`
- `kubernetes/kubernetes` —
  `cmd/kubeadm/app/phases/{certs,kubeconfig,kubelet,controlplane,bootstraptoken,addons,upgrade}/`
- `kubernetes/kubernetes` —
  `pkg/controller/bootstrap/` (token cleaner + populator)
- `kubernetes/kubernetes` —
  `pkg/controller/certificates/{approver,signer}/`
- `kubernetes/kubernetes` — `cluster/addons/` (legacy
  addon-manager manifests)
- KEP-1003 (Bootstrap Token), KEP-2057
  (`CertificateSigningRequest.spec.expirationSeconds`),
  KEP-2400 (Node-level swap), KEP-2453
  (Kubelet TLS bootstrap & rotation), KEP-3257
  (ClusterTrustBundles)
- Reference:
  `https://kubernetes.io/docs/reference/setup-tools/kubeadm/`,
  `https://kubernetes.io/docs/reference/access-authn-authz/bootstrap-tokens/`,
  `https://kubernetes.io/docs/reference/access-authn-authz/kubelet-tls-bootstrapping/`
