# k0s

Source-of-truth: <https://github.com/k0sproject/k0s> @ `adae350819bb08cf82baa32a5ccf6a21fa611aa1` (main, 2026-05-21). Latest tagged release: `v1.35.4+k0s.0` (2026-05-13).

## What this tool does

k0s is a single-binary Kubernetes distribution. Each controller node embeds the upstream `kube-apiserver`, `kube-scheduler`, `kube-controller-manager`, and a configurable datastore (etcd, kine over SQLite, MySQL, or PostgreSQL). The controller process supervises each component as a `manager.Component` with `Init()`/`Start()`/`Ready()` lifecycle hooks. A component is considered "ready" only when its `Ready()` returns `nil`; the api-server's hook polls `GET /readyz?verbose` over mTLS using `admin.crt`/`admin.key`/`ca.crt`, the kine hook write-reads a sentinel key on the embedded socket, and the endpoint reconciler then keeps the `default/kubernetes` `Endpoints` object pointing at the live controller IPs. Integration tests under `inttest/` exercise the same surface: they wait for `kc.ServerVersion()` plus `GET /readyz` to succeed, then proceed to list/watch real workloads.

## Bootstrap / preflight endpoints

- `GET /readyz?verbose` — mTLS-authenticated api-server probe used by the controller's `APIServer.Ready()` component hook. `pkg/component/controller/apiserver.go:245-280` (function `Ready()`, request at ~line 272). Client cert: `admin.crt`/`admin.key`; trust anchor: `ca.crt` from `K0sVars.CertRootDir`.
- `GET /readyz` — un-`verbose` variant used by the inttest harness `BootlooseSuite.WaitForKubeAPI` after a successful `kc.ServerVersion()` call. `inttest/common/bootloosesuite.go:1217-1245`.
- `GET /version` — implicit; reached through `kc.ServerVersion()` (client-go) inside the same `WaitForKubeAPI`. Must respond before `/readyz` is polled. `inttest/common/bootloosesuite.go:1227-1232`.
- `GET /api/v1/namespaces/default/endpoints/kubernetes` — the API-endpoint reconciler boots up and reads this object before publishing controller IPs. `pkg/component/controller/apiendpointreconciler.go:88-143` (function `reconcileEndpoints`, read at lines 105-106).
- `PUT /api/v1/namespaces/default/endpoints/kubernetes` — issued when the existing object's `Subsets` differ from the resolved controller IPs. Same file, line 131. If the object does not exist, falls through to `POST /api/v1/namespaces/default/endpoints` via `createEndpoint()` at lines 135-143.
- `GET /api/v1/namespaces/kube-system/pods` — basic smoke (`AssertSomeKubeSystemPods`). `inttest/common/util.go` and `inttest/basic/basic_test.go:~65`.
- `GET /api/v1/nodes/{name}` — `WaitForNodeReady` + `GetNodeLabels`. `inttest/common/bootloosesuite.go:1149-1165`.
- `GET /apis/coordination.k8s.io/v1/namespaces/kube-system/leases/kube-scheduler` and `.../kube-controller-manager` — single-node mode asserts they return `IsNotFound`. `inttest/singlenode/singlenode_test.go:57-63`.
- `WATCH /apis/certificates.k8s.io/v1/certificatesigningrequests` (field selector `spec.signerName=kubernetes.io/kubelet-serving`, username prefix `system:node:worker`) — basic suite asserts an approver fires. `inttest/basic/basic_test.go:~109-117`.
- `GET /apis/apps/v1/namespaces/kube-system/daemonsets/kube-router` — DaemonSet readiness via `WaitForKubeRouterReady`. `inttest/common/util.go` (function `WaitForKubeRouterReady`).
- `GET /apis/discovery.k8s.io/v1/namespaces/kube-system/endpointslices` — CoreDNS readiness probe via `WaitForCoreDNSReady`. `inttest/common/util.go:110-137`.

## JSON payloads

The reconciler PATCH/PUTs the `default/kubernetes` Endpoints with this body (struct literal at `pkg/component/controller/apiendpointreconciler.go:118-128`, marshalled to JSON by client-go):

```json
{
  "kind": "Endpoints",
  "apiVersion": "v1",
  "metadata": { "name": "kubernetes", "namespace": "default" },
  "subsets": [
    {
      "addresses": [ { "ip": "<controller-1-ip>" }, { "ip": "<controller-2-ip>" } ],
      "ports": [ { "name": "https", "protocol": "TCP", "port": 6443 } ]
    }
  ]
}
```

Bootstrap RBAC is applied verbatim from the embedded `pkg/component/controller/systemrbac.yaml` — three `ClusterRoleBinding`s (`kubelet-bootstrap`, `node-autoapprove-bootstrap`, `node-autoapprove-certificate-rotation`) wired to `system:node-bootstrapper`, `system:certificates.k8s.io:certificatesigningrequests:nodeclient`, and `system:certificates.k8s.io:certificatesigningrequests:selfnodeclient` for groups `system:bootstrappers` / `system:nodes`. Applied via the standard apply path → `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` (or `PUT` if extant).

The kine readiness write is etcd-protocol (gRPC over UNIX socket), not REST: key `/k0s-health-check`, value `"value"`, 64s TTL. `pkg/component/controller/kine.go:128-152`.

## Expected responses / assertions

- `Ready()` (apiserver): `resp.StatusCode == http.StatusOK` — anything else returns `fmt.Errorf("expected 200 for api server ready check, got %d", resp.StatusCode)` after logging the body at debug level. `pkg/component/controller/apiserver.go:268-279`.
- `WaitForKubeAPI` (inttest): `kc.ServerVersion()` must succeed AND `RequestURI("/readyz").Do(ctx)` must yield HTTP 200. 5 s per-attempt context timeout, ~5 minute outer `Poll`. `inttest/common/bootloosesuite.go:1217-1245`.
- `reconcileEndpoints`: `Get` of `default/kubernetes` must succeed (or `IsNotFound`); on mismatch with the resolved controller IPs it issues `Update`. `pkg/component/controller/apiendpointreconciler.go:105-131`.
- Singlenode mode: `Get` of `kube-scheduler` / `kube-controller-manager` Lease MUST return `apierrors.IsNotFound` — the test fails otherwise. `inttest/singlenode/singlenode_test.go:57-63`.
- CSR approver: condition `Reason == "Autoapproved by K0s CSRApprover"` on the approval. `inttest/basic/basic_test.go:~115`.
- Log markers from the api-server component: "api server readyz output: ..." (debug) on non-200; success path is silent.

## Rusternetes-compat checklist

- `GET /readyz`, `/readyz?verbose`: PRESENT. `crates/api-server/src/router.rs:672` (`/readyz` → `handlers::health::readyz`). Verbose variant served at `/healthz/verbose` (`router.rs:670`); `/readyz?verbose` query param is NOT branched in the handler — verify k0s' substring match still succeeds. `crates/api-server/src/handlers/health.rs:77` (`healthz_verbose`).
- `GET /healthz`, `GET /livez`: PRESENT. `router.rs:669-671` (both routed to `handlers::health::healthz`).
- `GET /version`: PRESENT. `router.rs:793` → `handlers::discovery::get_version`.
- `GET/PUT/POST /api/v1/namespaces/default/endpoints/kubernetes`: route is generic but PRESENT. `router.rs:999-1007`. No dedicated controller writes `default/kubernetes` — `crates/controller-manager/src/controllers/service.rs:29-30` only reserves `10.96.0.1` for the Service ClusterIP; nothing populates the Endpoints object. **GAP**: needs a reconciler analogous to k0s' `apiendpointreconciler.go` (the Go upstream uses `kube-apiserver --service-cluster-ip-range` and the built-in master service reconciler to do this; rusternetes runs the API server in-tree so it must own this).
- `GET /apis/discovery.k8s.io/v1/namespaces/.../endpointslices`: PRESENT. `router.rs:1770-1786`. EndpointSlice controller exists at `crates/controller-manager/src/controllers/endpointslice.rs`.
- `GET /apis/coordination.k8s.io/v1/namespaces/kube-system/leases/{name}`: PRESENT (Lease resource is wired in router). Singlenode-mode `IsNotFound` assertion is satisfied by default — no controller installs those leases unless scheduler/CM run.
- CSR auto-approver for `kubernetes.io/kubelet-serving`: PRESENT. `crates/controller-manager/src/controllers/certificate_signing_request.rs:232` matches the signer name. **GAP**: rusternetes' approval `Reason` string is not `"Autoapproved by K0s CSRApprover"` (that string is k0s-specific) — k0s' own inttest would fail against rusternetes, but Kubernetes conformance only checks status `Approved=True`, so this is informational, not a conformance gap. Verify in `certificate_signing_request.rs:244-268` (function `approve_csr`).
- Bootstrap ClusterRoleBindings (`kubelet-bootstrap`, `node-autoapprove-bootstrap`, `node-autoapprove-certificate-rotation`): **MISSING** in tree. No matches for `kubelet-bootstrap`, `node-autoapprove`, or `system:bootstrappers` in `scripts/bootstrap-cluster.sh` or anywhere under `crates/`. The group `system:bootstrappers` is recognised in `crates/common/src/auth.rs:401` but no controller seeds the bindings. If a conformance run uses `kubeadm`-style bootstrap tokens, these bindings must be created on first boot.
- `--enable-bootstrap-token-auth`, `--enable-admission-plugins=NodeRestriction`, `--authorization-mode=Node,RBAC`: rusternetes has its own admission/auth stack; not a CLI-flag concept. NodeRestriction admission status unverified — grep `crates/api-server/src/` for `NodeRestriction` if pursuing token-bootstrap conformance.
- Kine-style write-read sentinel on startup: N/A — rusternetes uses `StorageBackend` abstraction; the equivalent is `Storage::list` succeeding before serving traffic. Not modeled as an explicit `Ready()` gate.
