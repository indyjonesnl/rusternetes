# k3s

Source-of-truth: [k3s-io/k3s @ `26e2d49`](https://github.com/k3s-io/k3s/tree/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3) (default branch `main`, snapshotted 2026-05-21).

## What this tool does

k3s is a single-binary Kubernetes distribution that boots an embedded
api-server, scheduler, controller-manager, and (on agent nodes) kubelet +
kube-proxy. Boot order: kine/etcd → kube-apiserver → controllers → deploy
manifests (CoreDNS, local-path-provisioner, metrics-server, optional
Traefik). "Ready" is a layered check: the server logs `etcd is now running`
and `kube-apiserver is now running`, an HTTP `GET /readyz?verbose` against
the embedded apiserver succeeds, and the default deployments under
`kube-system` reach `ReadyReplicas == Replicas`. Agents and the in-tree
e2e suite poll the same `/readyz` endpoint plus the k3s-private supervisor
endpoints under `/v1-k3s/...`.

## Bootstrap / preflight endpoints

- `GET /readyz?verbose=` — official upstream readiness gate. Polled by
  `util.WaitForAPIServerReady` until `200 OK`.
  [`pkg/util/api.go` `WaitForAPIServerReady`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/pkg/util/api.go)
  — `restClient.Get().AbsPath("/readyz").Param("verbose", "").DoRaw(ctx)`.
- `GET /ping` — k3s supervisor liveness, returns 200 + body `pong`.
  [`pkg/server/handlers/router.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/pkg/server/handlers/router.go).
- `GET /cacerts` — fetches server CA bundle for cert bootstrap.
  Same router file.
- `GET /v1-k3s/readyz` — supervisor proxy of apiserver readiness. Agents
  call this before they consider the control plane usable.
  [`pkg/agent/config/config.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/pkg/agent/config/config.go).
- `GET /v1-k3s/config` — pulls agent config blob during agent bootstrap.
- `GET /v1-k3s/apiservers` — returns list of reachable apiservers.
- `GET /v1-k3s/serving-kubelet.crt`, `GET /v1-k3s/client-kubelet.crt`,
  `GET /v1-k3s/client-kube-proxy.crt`, `GET /v1-k3s/client-k3s-controller.crt`,
  `GET /v1-k3s/server-ca.crt`, `GET /v1-k3s/client-ca.crt` — node-cert
  issuance handlers.
- `GET /v1-k3s/encrypt/status`, `GET /v1-k3s/encrypt/config`,
  `GET /v1-k3s/cert/cacerts`, `GET /v1-k3s/server-bootstrap`,
  `GET /v1-k3s/token` — server-only management endpoints.
- `CONNECT /v1-k3s/connect`, `CONNECT /` — websocket tunnel for
  apiserver-to-node traffic.

Kubernetes API endpoints exercised by the in-tree e2e/integration tests
that act as "is the apiserver actually working" smoke checks
([`tests/client.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/tests/client.go),
[`tests/e2e/startup/startup_test.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/tests/e2e/startup/startup_test.go),
[`tests/e2e/validatecluster/validatecluster_test.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/tests/e2e/validatecluster/validatecluster_test.go)):

- `GET /api/v1/nodes` — `NodesReady` / `ParseNodes`, verifies every
  expected node has `NodeReady=True`.
- `GET /api/v1/namespaces/kube-system/pods` — `AllPodsUp`, expects every
  pod in `Running` or `Succeeded`.
- `GET /apis/apps/v1/namespaces/kube-system/deployments` —
  `CheckDeployments`, expects `ReadyReplicas == Replicas` for
  `coredns`, `local-path-provisioner`, `metrics-server`, `traefik`.
- `POST /api/v1/namespaces/kube-system/pods` (via `kubectl apply -f
  testdata/dummy.yaml`) — integration smoke test that posts a single Pod
  and waits for a `Container started` event.
- `GET /api/v1/namespaces/kube-system/events?fieldSelector=involvedObject.name=dummy`
  — verifies the apiserver returns event objects.
- `GET /api/v1/nodes/{node}/proxy/configz` — startup_test asserts kubelet
  config is reachable through the node-proxy subresource and contains the
  expected `shutdownGracePeriod` values.
- `POST /api/v1/namespaces/default/pods/busybox` (via
  `kubectl run busybox --restart=Never --image=…`) — exercises pod create
  + log retrieval.

## JSON payloads

`POST .../pods` body from `tests/integration/startup/testdata/dummy.yaml`
([source](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/tests/integration/startup/testdata/dummy.yaml)),
which `kubectl apply` serializes to JSON:

```json
{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": { "name": "dummy", "namespace": "kube-system" },
  "spec": {
    "containers": [{
      "name": "dummy",
      "image": "rancher/mirrored-library-nginx:1.29.1-alpine",
      "imagePullPolicy": "IfNotPresent"
    }]
  }
}
```

`POST .../configmaps` body from
[`validatecluster_test.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/tests/e2e/validatecluster/validatecluster_test.go)
("migration-test" cm exercised via `kubectl create configmap`):

```json
{
  "apiVersion": "v1",
  "kind": "ConfigMap",
  "metadata": { "name": "migration-test" },
  "data": { "test": "before-migration" }
}
```

`GET .../pods` response shape is consumed via `corev1.PodList` decoding —
k3s test code only reads `.items[*].status.phase`. No custom payload is
posted beyond standard core/v1 resources; k3s never PATCHes during boot
smoke tests, it only POSTs and GETs.

## Expected responses / assertions

- `GET /readyz` must return `200 OK`. `WaitForAPIServerReady` polls until
  it does, default timeout 15m.
- `GET /ping` must return `200 OK` + literal `pong`
  (`text/plain; charset=utf-8`). `pkg/server/handlers/handlers.go`:
  `data := []byte("pong"); resp.WriteHeader(http.StatusOK)`.
- `GET /v1-k3s/readyz` returns `200 OK` + literal `ok` when
  `control.Runtime.Core != nil`, else `503` via `util.SendError`.
- Log gates considered fully-ready in [`pkg/cli/server/server.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/pkg/cli/server/server.go):
  1. `ETCD server is now running`
  2. `Kube API server is now running`
  3. `k3s is up and running`
  4. systemd `READY=1\n` notification.
- Agent log gate: `k3s agent is up and running` once the kubelet starts
  and the tunnel is established
  ([`pkg/agent/run.go`](https://github.com/k3s-io/k3s/blob/26e2d49800b639fd84a9a3b1ed40ef6b628a27d3/pkg/agent/run.go)).
- e2e gates from `tests/client.go`: `node.condition.Type==NodeReady &&
  Status==ConditionTrue`; pod phase `Running` or `Succeeded`; deployment
  `ReadyReplicas == Replicas` for the four default deployments.

## Rusternetes-compat checklist

Verified against this worktree.

- `GET /readyz` — **covered**.
  `crates/api-server/src/router.rs:671` maps `/readyz` to
  `handlers::health::readyz`; handler at
  `crates/api-server/src/handlers/health.rs:29` returns a JSON
  `HealthStatus` body with `200 OK` (or `503` if storage check fails).
  Caveat vs. k3s: k3s's apiserver `/readyz` accepts the `?verbose=` query
  param and returns a plain-text per-component listing — rusternetes
  returns JSON instead. `WaitForAPIServerReady` only checks the HTTP
  status code, so this passes, but `kubectl get --raw /readyz?verbose`
  produces a different body shape than upstream.
- `GET /healthz`, `GET /livez` — **covered**.
  `router.rs:668-670`, both wired to `handlers::health::healthz` (always
  `200`).
- `GET /version` — **covered**. `router.rs:801`,
  `handlers::discovery::get_version`.
- `GET /ping` (supervisor `pong`) — **missing**. No `/ping` route in
  `router.rs`. k3s-specific; not needed for Sonobuoy conformance, but a
  smoke-test runner that polls `/ping` (e.g. external rancher tooling)
  would fail closed.
- `/v1-k3s/*` supervisor endpoints (`config`, `apiservers`, `*.crt`,
  `connect`, `server-bootstrap`, `token`, `encrypt/*`) — **missing &
  out-of-scope**. These are k3s-specific cluster join APIs; rusternetes
  uses raw kubeconfig + cert files generated by `scripts/generate-certs.sh`
  and does not need to emulate them.
- `GET /api/v1/nodes` (list nodes + status) — **covered**.
  `router.rs` registers list/get/status routes for nodes; the bootstrap
  script `scripts/bootstrap-cluster.sh` (line 149) calls
  `kubectl get pod coredns -n kube-system -o jsonpath='{.status.phase}'`,
  exercising the same list shape.
- `GET /api/v1/namespaces/kube-system/pods` — **covered**.
  `router.rs` registers per-namespace pod list. Bootstrap polls pod phase.
- `GET /apis/apps/v1/namespaces/kube-system/deployments` — **covered**.
  Apps/v1 routes registered around `router.rs:1100+`. k3s expects
  `ReadyReplicas == Replicas` for `coredns`, `local-path-provisioner`,
  `metrics-server`, `traefik`. Rusternetes ships only CoreDNS by default
  (`scripts/bootstrap-cluster.sh:131-158`); the other three are k3s-only
  components and not required for conformance.
- `POST /api/v1/namespaces/{ns}/pods` (apply dummy.yaml) — **covered**.
  Pod create handler is registered.
- `POST /api/v1/namespaces/{ns}/configmaps` (migration-test) —
  **covered**. ConfigMap create handler is registered.
- `GET /api/v1/namespaces/{ns}/events?fieldSelector=…` — **covered**.
  `router.rs:1529-1547` register namespaced events list + cluster-wide
  `/api/v1/events` + watch. Field-selector filtering needs to honor
  `involvedObject.name` for the k3s integration smoke test to match
  upstream exactly — worth confirming in handler code.
- `GET /api/v1/nodes/{name}/proxy/configz` (kubelet config via node
  proxy) — **partial**. `router.rs:1086` registers
  `/api/v1/nodes/:name/proxy/*path` → `handlers::proxy::proxy_node`. The
  route exists, but k3s's startup_test specifically asserts
  `kubectl get --raw /api/v1/nodes/{node}/proxy/configz` returns a
  kubelet config JSON containing
  `shutdownGracePeriod` / `shutdownGracePeriodCriticalPods`. Confirm
  rusternetes kubelet serves `/configz` and that the node-proxy forwards
  it intact — this is a likely conformance-failure surface.
- systemd `READY=1\n` notification — **n/a**. Rusternetes does not run
  under systemd in compose; conformance test runner does not require it.

Net: the API endpoints k3s actually polls during boot smoke
(`/readyz`, `/healthz`, `/version`, core/v1 + apps/v1 list/get/create,
events list, node proxy) are all wired. Two concrete gaps worth probing
when triaging conformance failures: (1) `/readyz?verbose` body shape
parity, (2) node `/proxy/configz` end-to-end behaviour through the
kubelet.
