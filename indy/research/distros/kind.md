# kind

Source-of-truth: https://github.com/kubernetes-sigs/kind @ commit
`71f3111ef05b8a608d90b6913325416863866a37` (main, 2026-05-13).

## What this tool does

kind ("Kubernetes IN Docker") boots a cluster by spawning one or more
Docker/Podman containers that emulate Kubernetes nodes, then runs
`kubeadm init` on the control-plane container and `kubeadm join` on
workers. After kubeadm is done it pipes a CNI manifest (`kindnet` by
default) and a host-path `StorageClass` through `kubectl create -f -`
on the control-plane node, then polls the API server until every
control-plane node reports `status.conditions[Ready].status == "True"`.
Action ordering lives in
[`pkg/cluster/internal/create/create.go`](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/create.go):
loadbalancer -> config -> kubeadminit -> installcni -> installstorage
-> kubeadmjoin -> waitforready.

## Bootstrap / preflight endpoints

All API traffic during cluster creation is issued by `kubectl` running
inside the control-plane container against
`https://<api>:6443` with kubeconfig `/etc/kubernetes/admin.conf`. The
ordered list, with upstream citations:

- `POST /api/v1/namespaces/kube-system/configmaps` and friends -
  written internally by `kubeadm init --config=/kind/kubeadm.conf`
  ([`kubeadminit/init.go` L70-L80](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/actions/kubeadminit/init.go)).
  kubeadm itself creates the `kubeadm-config`, `kubelet-config`,
  `cluster-info`, `kube-proxy`, and `coredns` ConfigMaps plus the
  bootstrap RBAC.
- `PATCH /api/v1/nodes/<name>` (strategic-merge) - issued by
  `kubectl taint nodes --all node-role.kubernetes.io/control-plane-`
  in `kubeadminit/init.go` after `kubeadm init` returns, only for
  single-node clusters
  ([`init.go`, taintArgs block](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/actions/kubeadminit/init.go)).
- `PATCH /api/v1/nodes/<name>` - `kubectl label nodes --all
  node.kubernetes.io/exclude-from-external-load-balancers-` (same file).
- `POST /apis/apps/v1/namespaces/kube-system/daemonsets` and
  `POST /apis/rbac.authorization.k8s.io/v1/clusterroles[,bindings]` -
  the kindnet manifest is read from
  `/kind/manifests/default-cni.yaml` inside the node and piped via
  `kubectl create --kubeconfig=/etc/kubernetes/admin.conf -f -`
  ([`installcni/cni.go`](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/actions/installcni/cni.go)).
- `POST /apis/storage.k8s.io/v1/storageclasses` - host-path
  `StorageClass` named `standard`, applied via
  `kubectl --kubeconfig=/etc/kubernetes/admin.conf apply -f -`
  ([`installstorage/storage.go`](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/actions/installstorage/storage.go)).
- `GET /api/v1/nodes?labelSelector=node-role.kubernetes.io/control-plane`
  - the ready poll
  ([`waitforready.go` L102-L113](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/actions/waitforready/waitforready.go);
  jsonpath `{.items..status.conditions[-1:].status}`).

## JSON payloads

kind generally hands a YAML/JSON document to `kubectl` and lets the
client encode it. The shapes the api-server must accept are:

**`StorageClass` (installstorage)** - applied with `kubectl apply`,
so the api-server sees both a `POST` and follow-up `PATCH` (server-side
apply or strategic-merge depending on `kubectl` version):

```json
{
  "apiVersion": "storage.k8s.io/v1",
  "kind": "StorageClass",
  "metadata": {
    "namespace": "kube-system",
    "name": "standard",
    "annotations": {"storageclass.kubernetes.io/is-default-class": "true"}
  },
  "provisioner": "kubernetes.io/host-path"
}
```

**kindnet `DaemonSet` (installcni)** - a single
`kubectl create -f -` of the rendered manifest. The pod template uses
`hostNetwork: true`, `securityContext.privileged: true`, mounts
`/etc/cni/net.d` and `/run/xtables.lock`, and exposes
`CONTROL_PLANE_ENDPOINT` and `POD_SUBNET` env vars patched in by
`installcni/cni.go`.

**Node taint patch (kubeadminit)** - `kubectl taint` issues a strategic
merge against `/api/v1/nodes/<name>`:

```json
{"spec": {"taints": null}}
```

(The `-` suffix on the taint key removes the taint; kubectl converts
that into a strategic-merge patch deleting the offending entry.)

**Node label patch (kubeadminit)** - `kubectl label … -` patches:

```json
{"metadata": {"labels": {"node.kubernetes.io/exclude-from-external-load-balancers": null}}}
```

**CI extras (`hack/ci/e2e-k8s.sh`)** - the test driver also runs a
JSON-patch against the kube-proxy DaemonSet:

```
kubectl patch -n kube-system daemonset/kube-proxy --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/command/-","value":"--v=4"}]'
```

i.e. the api-server must accept `application/json-patch+json` against
`/apis/apps/v1/namespaces/kube-system/daemonsets/kube-proxy` ([`hack/ci/e2e-k8s.sh`](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/hack/ci/e2e-k8s.sh)).

kind never issues raw `curl`/`http.Client` calls during bootstrap -
everything is funnelled through `kubectl` exec'd inside the node.

## Expected responses / assertions

"Cluster is up" is defined in
[`waitforready.go`](https://raw.githubusercontent.com/kubernetes-sigs/kind/main/pkg/cluster/internal/create/actions/waitforready/waitforready.go):

```go
// L102-L131
func waitForReady(node nodes.Node, until time.Time, selectorLabel string) bool {
    return tryUntil(until, func() bool {
        cmd := node.Command(
            "kubectl",
            "--kubeconfig=/etc/kubernetes/admin.conf",
            "get", "nodes",
            "--selector="+selectorLabel,
            "-o=jsonpath='{.items..status.conditions[-1:].status}'",
        )
        lines, err := exec.OutputLines(cmd)
        if err != nil { return false }
        status := strings.Fields(lines[0])
        for _, s := range status {
            if !strings.Contains(s, "True") { return false }
        }
        return true
    })
}
```

Translated: kind requires the api-server to return a node list filtered
by `node-role.kubernetes.io/control-plane` (or `…/master` pre-1.24) where
the **last** entry of `status.conditions` (jsonpath `[-1:]`) has
`status == "True"`. Any timeout returns a warning, not a hard failure
(L88-L92), but the cluster is considered unhealthy.

## Rusternetes-compat checklist

Grep done against `crates/api-server/src/router.rs` and
`scripts/bootstrap-cluster.sh` in the local worktree.

| kind expectation | Status | Rusternetes evidence |
|---|---|---|
| `GET /api/v1/nodes?labelSelector=…` | covered | `router.rs:1069-1070` lists nodes; handler honours `labelSelector` (`crates/api-server/src/handlers/node.rs:195`) |
| Node `status.conditions[Ready].status == "True"` shape | covered | controller writes `Ready` condition in `crates/controller-manager/src/controllers/node.rs:398-406`; kubelet eviction tweaks via `crates/kubelet/src/eviction.rs:310-365` |
| jsonpath `status.conditions[-1:].status` (relies on Ready being last, or any condition being True - kind's check is sloppy) | partial | rusternetes appends Ready last on creation but does not guarantee ordering across updates; verify the `Ready` condition is the last element returned, or kind's `-1:` slice may pick e.g. `MemoryPressure=False` |
| `PATCH /api/v1/nodes/:name` (strategic-merge taint removal) | covered | `router.rs:1073-1077` exposes PATCH; strategic-merge regression tests added in `crates/api-server/tests/patch_strategic_merge_semantics_test.rs` |
| `PATCH /api/v1/nodes/:name` (label removal via `null`) | covered | same route; tombstone-via-null semantics tracked in the same strategic-merge test |
| `POST /apis/storage.k8s.io/v1/storageclasses` (host-path StorageClass) | covered | `router.rs:1427` registers create/list; `provisioner: kubernetes.io/host-path` is not in-tree but storing the object is sufficient for kind's smoke test |
| `POST /apis/apps/v1/namespaces/:namespace/daemonsets` (kindnet) | covered | `router.rs:1202` |
| `kubectl patch --type=json` against the kube-proxy DaemonSet | covered | `router.rs:1206` (PATCH on `/apis/apps/v1/namespaces/:namespace/daemonsets/:name`); JSON Patch acceptance must round-trip — confirm via existing patch tests |
| `POST /apis/rbac.authorization.k8s.io/v1/{clusterroles,clusterrolebindings}` (kindnet RBAC) | covered | `router.rs:1348-1364` |
| `POST /api/v1/namespaces/kube-system/configmaps` (kubeadm-config, kubelet-config, coredns) | covered | `router.rs:1025` |
| `kubectl apply -f -` (server-side apply for StorageClass / CNI) | partial | rusternetes accepts strategic-merge PATCH; full server-side-apply field-manager semantics still incomplete - kind uses `apply` only for StorageClass, so this is a likely failure path if rusternetes ever sees a re-apply |
| Bootstrap parity with kind (CoreDNS, CNI, StorageClass, RBAC) | partial | `scripts/bootstrap-cluster.sh` boots CoreDNS but does not install a default CNI DaemonSet or a `standard` StorageClass; kind-style consumers would have to layer those manually before running e2e |
| `kubeadm init` cert SANs `[localhost, <APIServerAddress>]` | n/a | rusternetes does not run kubeadm; certs come from `scripts/generate-certs.sh` and must include the bridge IPs noted in `CLAUDE.md` |

Net gap: rusternetes' router serves every endpoint kind touches during
bootstrap, but two soft risks remain - (a) the Ready condition is not
guaranteed to be the last entry of `status.conditions`, which kind's
sloppy `[-1:]` jsonpath relies on, and (b) `scripts/bootstrap-cluster.sh`
does not pre-install kindnet or a `standard` StorageClass, so a kind-shaped
smoke test would need an extra `kubectl apply` step before claiming
parity.
