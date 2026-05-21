# kubeadm

Source-of-truth: <https://github.com/kubernetes/kubernetes> — `master`
branch, fetched 2026-05-21. Files under
`cmd/kubeadm/app/...`. Cross-referenced with
`k8s.io/cluster-bootstrap/token/api` and
`k8s.io/cluster-bootstrap/token/util`.

## What this tool does

`kubeadm init` writes static-pod manifests, waits for the kube-apiserver
to be reachable, then makes a long sequence of REST calls to upload its
own configuration, mark the node as control-plane, lay down
bootstrap-token Secrets + auto-approval RBAC for joining workers, and
finally installs CoreDNS + kube-proxy. `kubeadm join` reads
`cluster-info` anonymously from `kube-public`, fetches a TLS bootstrap
token, submits a CertificateSigningRequest, and waits for the local
kubelet to be approved. Both flows assume strategic-merge PATCH
semantics and standard CRUD on every core + RBAC resource.

## Phase order

`kubeadm init` (`cmd/kubeadm/app/cmd/init.go` L161-L174):

1. `preflight` — local checks, no API calls.
2. `certs` — local key/cert generation.
3. `kubeconfig` — writes `/etc/kubernetes/{admin,kubelet,controller-manager,scheduler}.conf`.
4. `etcd` — local static pod manifests.
5. `control-plane` — writes static pod manifests for kube-apiserver / kube-controller-manager / kube-scheduler.
6. `kubelet-start` — starts the local kubelet, which begins running the static pods.
7. `wait-control-plane` — first API contact (see "Health gate" below).
8. `upload-config` — first POST.
9. `upload-certs` — control-plane cert sharing.
10. `mark-control-plane` — node PATCH.
11. `bootstrap-token` — Secret + RBAC for joiners.
12. `kubelet-finalize` — local cert rotation switch.
13. `addon` — CoreDNS + kube-proxy installation.
14. `show-join-command` — prints the join string.

`kubeadm join` (`cmd/kubeadm/app/cmd/join.go` L240-L247): `preflight` →
`control-plane-prepare` → `check-etcd` → `kubelet-start` →
`etcd-join` → `kubelet-wait-bootstrap` → `control-plane-join` →
`wait-control-plane`.

## Bootstrap / preflight endpoints (health gate)

Source: `cmd/kubeadm/app/util/apiclient/wait.go`.

- Endpoint constants (L42-L45):
  - `endpointHealthz = "healthz"`
  - `endpointLivez   = "livez"`
- `WaitForControlPlaneComponents` (L254-L313):
  - **kube-apiserver**: `GET /livez` via
    `client.Discovery().RESTClient().Get().AbsPath(comp.endpoint)`
    (L291-L298, L339-L344). Uses the kubeadm client (not raw
    `http.Client`) so anonymous-auth-disabled clusters still pass.
  - **kube-controller-manager**: `GET https://<addr>:<port>/healthz`
    (L60, L345-L353) using a raw `http.Client` that skips TLS verify.
  - **kube-scheduler**: `GET https://<addr>:<port>/livez` (L67).
  - Polls via `wait.PollUntilContextTimeout` until each returns 200.
- `WaitForKubelet` (L343-L387): `GET http://<addr>:<port>/healthz` on
  the local kubelet (default port `10248`). Skipped when port = 0.
- `WaitForPodsWithLabel` (L315-L341): `LIST /api/v1/namespaces/kube-system/pods?labelSelector=...`
  blocking until count > 0 and every pod is `Running`. Used after
  CoreDNS install.
- `WaitForStaticPodHashChange` (L470-L490): `GET /api/v1/namespaces/kube-system/pods/<name>`
  to read the `kubernetes.io/config.hash` annotation during upgrades.

`cmd/kubeadm/app/phases/upgrade/health.go` adds extra preflight gates
during `kubeadm upgrade` (not init): a probe Job
(`POST /apis/batch/v1/namespaces/kube-system/jobs` L75-L152 then poll
`.status.succeeded == 1`) plus
`LIST /api/v1/nodes?labelSelector=node-role.kubernetes.io/control-plane`
asserting `Ready=True` (L154-L170).

## JSON payloads written during `kubeadm init`

Below: every write operation the upload/mark/bootstrap/addon phases
issue, in execution order. Verbs are HTTP verbs (the kubeadm helpers
`CreateOrUpdate` / `CreateOrMutate` map to `POST`-then-`PUT`-on-conflict;
`PatchNode` uses `PATCH` with `application/strategic-merge-patch+json`).
See `cmd/kubeadm/app/util/apiclient/idempotency.go` L58-L218.

### 1. upload-config — `cmd/kubeadm/app/phases/uploadconfig/uploadconfig.go`

`POST /api/v1/namespaces/kube-system/configmaps` (L62-L73):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kubeadm-config       # constants.go L453
  namespace: kube-system
data:
  ClusterConfiguration: |    # constants.go L456 — key name verbatim
    apiVersion: kubeadm.k8s.io/v1beta4
    kind: ClusterConfiguration
    kubernetesVersion: v1.35.0
    # ...full YAML-marshalled ClusterConfiguration...
```

Followed by Role + RoleBinding in `kube-system` granting `get` on this
ConfigMap to groups `system:bootstrappers:kubeadm:default-node-token`
and `system:nodes`:

- `POST /apis/rbac.authorization.k8s.io/v1/namespaces/kube-system/roles`
- `POST /apis/rbac.authorization.k8s.io/v1/namespaces/kube-system/rolebindings`

`uploadconfig.go` also writes the kubelet config ConfigMap via
`cmd/kubeadm/app/phases/kubelet/config.go` L156-L218:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kubelet-config       # constants.go L465
  namespace: kube-system
data:
  kubelet: |                 # KubeletBaseConfigurationConfigMapKey
    apiVersion: kubelet.config.k8s.io/v1beta1
    kind: KubeletConfiguration
    # ...
```

Same Role / RoleBinding pair, also in `kube-system`, subjects =
`system:nodes` + the bootstrap-token group.

### 2. upload-certs — `cmd/kubeadm/app/phases/copycerts/copycerts.go`

`POST /api/v1/namespaces/kube-system/secrets` (L114-L123):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kubeadm-certs        # KubeadmCertsSecret
  namespace: kube-system
  ownerReferences:           # owned by a "kubeadm-certs" expiry Secret
    - apiVersion: v1
      kind: Secret
      name: kubeadm-certs
      uid: <auto>
# Secret has no explicit type; defaults to Opaque
data:
  ca.crt:               <base64 cryptoutil.EncryptBytes>
  ca.key:               <base64>
  front-proxy-ca.crt:   <base64>
  front-proxy-ca.key:   <base64>
  sa.pub:               <base64>
  sa.key:               <base64>
  etcd-ca.crt:          <base64>
  etcd-ca.key:          <base64>
```

Each value is AES-encrypted with a user-supplied hex key before being
base64'd (L145-L147, L194-L196). Joining control-plane nodes decrypt
on `kubeadm join --control-plane`.

### 3. mark-control-plane — `cmd/kubeadm/app/phases/markcontrolplane/markcontrolplane.go`

`PATCH /api/v1/nodes/<hostname>` with
`Content-Type: application/strategic-merge-patch+json` (helper:
`cmd/kubeadm/app/util/apiclient/idempotency.go` L197, L207-L218):

```json
{
  "metadata": {
    "labels": {
      "node-role.kubernetes.io/control-plane": "",
      "node.kubernetes.io/exclude-from-external-load-balancers": ""
    }
  },
  "spec": {
    "taints": [
      {
        "key": "node-role.kubernetes.io/control-plane",
        "effect": "NoSchedule"
      }
    ]
  }
}
```

`markcontrolplane.go` L56-L61 merges the new taint with existing
`node.Spec.Taints` before encoding, so the patch only ever adds.

### 4. bootstrap-token — `cmd/kubeadm/app/phases/bootstraptoken/node/`

#### 4a. Per-token Secret (`token.go` L38-L70)

`POST /api/v1/namespaces/kube-system/secrets`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: bootstrap-token-<id>       # BootstrapTokenSecretPrefix + tokenID
  namespace: kube-system
type: bootstrap.kubernetes.io/token   # SecretTypeBootstrapToken
stringData:
  token-id:                         "<id>"
  token-secret:                     "<secret>"
  expiration:                       "2026-05-22T00:00:00Z"   # optional, RFC3339
  description:                      "kubeadm-generated bootstrap token"
  auth-extra-groups:                "system:bootstrappers:kubeadm:default-node-token"
  usage-bootstrap-authentication:   "true"
  usage-bootstrap-signing:          "true"
```

Constants from `k8s.io/cluster-bootstrap/token/api/types.go`:
`BootstrapTokenSecretPrefix = "bootstrap-token-"`,
`SecretTypeBootstrapToken = "bootstrap.kubernetes.io/token"`.

#### 4b. TLS bootstrap RBAC (`tlsbootstrap.go`)

Five `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings`:

- `kubeadm:kubelet-bootstrap` → `system:node-bootstrapper`, subject
  `system:bootstrappers:kubeadm:default-node-token` (L33-L45). Lets
  bootstrap tokens POST CSRs.
- `kubeadm:get-nodes` → eponymous ClusterRole, same subject (L60-L72).
- `kubeadm:node-autoapprove-bootstrap` →
  `system:certificates.k8s.io:certificatesigningrequests:nodeclient`,
  same subject (L82-L94). Auto-approves first-time node CSRs.
- `kubeadm:node-autoapprove-certificate-rotation` →
  `system:certificates.k8s.io:certificatesigningrequests:selfnodeclient`,
  subject `system:nodes` (L104-L116). Auto-approves rotation CSRs.
- `kubeadm:kubelet-api-admin` → `system:kubelet-api-admin`, subject the
  apiserver-kubelet-client cert CN (L126-L138). Lets apiserver hit
  kubelet logs/exec/portforward.

One `POST /apis/rbac.authorization.k8s.io/v1/clusterroles`:

```json
{
  "apiVersion": "rbac.authorization.k8s.io/v1",
  "kind": "ClusterRole",
  "metadata": {"name": "kubeadm:get-nodes"},
  "rules": [
    {"verbs": ["get"], "apiGroups": [""], "resources": ["nodes"]}
  ]
}
```

#### 4c. cluster-info (`clusterinfo/clusterinfo.go`)

`POST /api/v1/namespaces/kube-public/configmaps` (L48-L57):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-info               # bootstrapapi.ConfigMapClusterInfo
  namespace: kube-public
data:
  kubeconfig: |                    # bootstrapapi.KubeConfigKey
    apiVersion: v1
    kind: Config
    clusters:
    - name: ""
      cluster:
        certificate-authority-data: <base64>
        server: https://<endpoint>:6443
    # no users, no contexts — anonymous-readable bootstrap config
```

Plus a Role + RoleBinding in `kube-public` (L65-L98) granting `get` on
`configmaps/cluster-info` to subject `Kind=User Name=system:anonymous`,
so a joining node with no credentials can read the kubeconfig + CA.

### 5. addon — CoreDNS (`addons/dns/dns.go`)

All resources in `kube-system`. Verbs are `POST`-or-`PUT` via
`CreateOrUpdate`.

- `POST .../serviceaccounts` — `coredns` (L137).
- `POST .../configmaps` — `coredns` with templated `Corefile` (L113).
- `POST /apis/rbac.../clusterroles` — `system:coredns` (L125):
  `list/watch` on endpoints, services, pods, namespaces, and
  `discovery.k8s.io/v1/endpointslices`.
- `POST /apis/rbac.../clusterrolebindings` — `system:coredns` (L132).
- `POST /apis/apps/v1/namespaces/kube-system/deployments` — `coredns`,
  `spec.replicas: 2`, control-plane toleration (L154).
- `POST .../services` — `kube-dns` with hardcoded
  `spec.clusterIP: 10.96.0.10` (L158).

kubeadm then calls `WaitForPodsWithLabel(k8s-app=kube-dns)` to confirm
pods reach `Running`.

### 6. addon — kube-proxy (`addons/proxy/proxy.go`)

- `POST .../serviceaccounts` — `kube-proxy` (L115).
- `POST /apis/rbac.../clusterrolebindings` — `kubeadm:node-proxier` →
  `system:node-proxier`, subject the `kube-proxy` SA (L131).
- `POST /apis/rbac.../namespaces/kube-system/roles` — `kube-proxy`,
  `get` on `configmaps/kube-proxy` to bootstrap-token group (L136).
- `POST /apis/rbac.../namespaces/kube-system/rolebindings` —
  `kube-proxy` (L141).
- `POST .../configmaps` — `kube-proxy` with `config.conf`
  (KubeProxyConfiguration YAML) + `kubeconfig.conf` data keys (L201).
- `POST /apis/apps/v1/namespaces/kube-system/daemonsets` — `kube-proxy`
  DaemonSet (L227). `hostNetwork: true`, tolerates all taints, mounts
  the `kube-proxy` ConfigMap.

## kubeadm join — additional API calls

- `GET /api/v1/namespaces/kube-public/configmaps/cluster-info` —
  anonymous read of the kubeconfig + CA (cluster-info).
- `GET /api/v1/namespaces/kube-system/configmaps/kubeadm-config` —
  authenticated read of `ClusterConfiguration` once a bootstrap token
  is in hand.
- `GET /api/v1/namespaces/kube-system/configmaps/kubelet-config` —
  reads `kubelet` data key.
- `POST /apis/certificates.k8s.io/v1/certificatesigningrequests` —
  kubelet TLS bootstrap CSR. Issued by the kubelet itself (not kubeadm
  directly), via the bootstrap-token kubeconfig.
- `GET /apis/certificates.k8s.io/v1/certificatesigningrequests/<name>`
  — polls until `status.certificate` populated by the autoapprover.
- `GET /api/v1/nodes/<hostname>` — kubelet self-check.
- For control-plane joins only:
  `GET /api/v1/namespaces/kube-system/secrets/kubeadm-certs` then
  decrypt + write to disk.

## Expected responses / assertions

- Writes assume HTTP 201 on first POST or 409 (then PUT on the retry
  path inside `CreateOrUpdate`).
- `PatchNode` uses strategic-merge: apiserver must merge
  `metadata.labels` (additive) and replace `spec.taints` wholesale
  (kubeadm pre-merges client-side at L56-L61).
- `WaitForPodsWithLabel` requires list-or-watch; pod `.status.phase`
  must transition to `Running`.
- `WaitForControlPlaneComponents` requires `/livez` on apiserver and
  `/healthz` on controller-manager to return 200. Apiserver path goes
  through Discovery REST, so failing `Get`s must propagate non-nil
  errors (not be silently retried).
- Auto-approve ClusterRoleBindings imply a CSR-approver controller
  (kube-controller-manager `csrapproving`). Without it, joins hang.

## Rusternetes-compat checklist

Local file:
`/home/jones/PhpstormProjects/rusternetes/.claude/worktrees/agent-a7e922e7ff475c41d/crates/api-server/src/router.rs`
unless noted.

| kubeadm-required endpoint | rusternetes status | Evidence |
| --- | --- | --- |
| `GET /livez`, `/healthz`, `/readyz` | present | router.rs L668-L671 |
| `GET /metrics` | present | router.rs L672 |
| Core API discovery (`/api`, `/api/v1`) | present | router.rs L684-L687 |
| Aggregated API discovery (`/apis`, `/apis/rbac.authorization.k8s.io/v1`, `/apis/certificates.k8s.io/v1`, `/apis/coordination.k8s.io/v1`) | present | router.rs L688-L797 (incl. L704, L724, L732) |
| `POST/PUT/PATCH /api/v1/namespaces/:namespace/configmaps[/:name]` | present | router.rs L1023-L1032 |
| `LIST /api/v1/configmaps` (all namespaces) | present | router.rs L1037 |
| `POST/PUT/PATCH /api/v1/namespaces/:namespace/secrets[/:name]` | present | router.rs L1045-L1056 |
| `LIST /api/v1/secrets` (all namespaces) | present | router.rs L1059 |
| `POST/PUT/PATCH /api/v1/nodes[/:name]` + `/status` | present | router.rs L1069-L1083 |
| Node strategic-merge PATCH content-type | **VERIFY** | grep `strategic-merge` in handlers — see crates/api-server/src/handlers/node.rs; needed for mark-control-plane |
| `POST /api/v1/namespaces/:namespace/serviceaccounts` | present | router.rs L1290-L1300 |
| `POST /api/v1/namespaces/:namespace/serviceaccounts/:sa/token` (TokenRequest) | present | router.rs L2207 — required by addons that mount projected SA tokens |
| `POST /apis/authentication.k8s.io/v1/tokenreviews` | present | router.rs L2199 |
| `POST/PUT/PATCH /apis/rbac.authorization.k8s.io/v1/(namespaces/:ns/)?roles[/:name]` | present | router.rs L1314-L1326 |
| `POST/PUT/PATCH /apis/rbac.authorization.k8s.io/v1/(namespaces/:ns/)?rolebindings[/:name]` | present | router.rs L1331-L1344 |
| `POST/PUT/PATCH /apis/rbac.authorization.k8s.io/v1/clusterroles[/:name]` | present | router.rs L1348-L1356 |
| `POST/PUT/PATCH /apis/rbac.authorization.k8s.io/v1/clusterrolebindings[/:name]` | present | router.rs L1360-L1368 |
| Watch RBAC (`/apis/rbac.../watch/...`) | present | router.rs L2319-L2331 |
| `POST/PUT/PATCH /apis/apps/v1/namespaces/:ns/deployments[/:name]` + `/status` + `/scale` | present | router.rs L1100-L1120 |
| `POST/PUT/PATCH /apis/apps/v1/namespaces/:ns/daemonsets[/:name]` + `/status` + `/scale` | present | router.rs L1202-L1221 |
| `POST /apis/certificates.k8s.io/v1/certificatesigningrequests` + `/status` + `/approval` | present | router.rs L1731-L1753 |
| Watch CSRs (`/apis/certificates.k8s.io/v1/watch/...`) | present | router.rs L2347-L2348 |
| `/apis/coordination.k8s.io/v1/.../leases` | present | router.rs L1674-L1688 |
| `POST /apis/batch/v1/namespaces/:ns/jobs` (used by `kubeadm upgrade health.go`) | **VERIFY** | grep `/apis/batch/v1` in router.rs — needed for upgrades, optional for plain init |
| `GET /openid/v1/jwks` + service-account-issuer discovery (`/.well-known/openid-configuration`) | partial — `/openid/v1/jwks` at router.rs L678; verify `/.well-known/openid-configuration` is wired in router.rs L674 |
| CSR auto-approval controller running in controller-manager | **VERIFY** | look under `crates/controller-manager/src/controllers/` for a `csrapproving` controller; without it `kubeadm join` hangs |
| `bootstrap.kubernetes.io/token` Secret type recognized by token-controller | **VERIFY** | required so the bootsigner refreshes the cluster-info signature — see `crates/controller-manager/src/controllers/` |

### Items the catalog flags as likely-missing in rusternetes (no router hit)

- `POST /apis/batch/v1/...` — `grep -n batch/v1 crates/api-server/src/router.rs` returned 0 hits. Only needed for `kubeadm upgrade`, but listed because it surfaces during the upgrade conformance lane.
- Cluster-info Role/RoleBinding with `system:anonymous` subject — the
  routes exist, but the **auth layer** must permit anonymous access to
  `GET /api/v1/namespaces/kube-public/configmaps/cluster-info`.
  Verify in `crates/api-server/src/state.rs` and the auth middleware
  whether `system:anonymous` is honoured for kube-public.
- Strategic-merge-patch tolerant Node handler — kubeadm sends
  `application/strategic-merge-patch+json`; if rusternetes' patch
  handler only accepts JSON-Merge or JSON-Patch, mark-control-plane
  fails silently. Check `crates/api-server/src/handlers/node.rs` and
  the dispatch in the patch handler middleware.
- BootstrapSigner / TokenCleaner controllers — needed to refresh the
  signed `cluster-info` ConfigMap so joining nodes can verify it. Grep
  `crates/controller-manager/src/controllers/` for `bootstrapsigner`
  and `tokencleaner` — likely absent.

### Confirmed compatible

router.rs already routes every CRUD endpoint kubeadm POSTs during init
(ConfigMap, Secret, Node, ServiceAccount, all four RBAC kinds,
Deployment, DaemonSet, CSR, Lease) plus discovery + health gates. The
remaining risk is at the **semantics** layer (PATCH content-type,
anonymous auth, controller presence), not the routing layer.
