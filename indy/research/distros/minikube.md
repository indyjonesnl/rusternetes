# minikube

Source-of-truth: [kubernetes/minikube @ master `bcf6373`](https://github.com/kubernetes/minikube/tree/bcf637352a2989a5368e910f0e37e7403e02cb9d) (latest tag `v1.38.1`).

## What this tool does

`minikube start` provisions a single-node (or small multi-node) cluster by
delegating control-plane setup to **kubeadm**. After `kubeadm init` finishes,
minikube's own verification layer (`pkg/minikube/bootstrapper/bsutil/kverify/`)
runs a series of gates that decide whether the cluster is "ready". The gates
are orchestrated by `kubeadm.Bootstrapper.WaitForNode` in
`pkg/minikube/bootstrapper/kubeadm/kubeadm.go:688-789` and keyed by
`cfg.VerifyComponents` (see `kverify.AllComponentsList`). The default set
(`DefaultComponents`, `kverify.go:43`) only requires `apiserver` +
`system_pods`, but `minikube start` opts into the full list. "Ready" means: the
api-server process is up, `/healthz` returns 200, `ServerVersion()` matches the
expected version, every kube-system pod is Running, the `default` ServiceAccount
exists, requested apps are Running, the node reaches `NodeReady=True`, and the
kubelet systemd unit is `active`.

## Bootstrap / preflight endpoints

- `GET https://<apiserver>/healthz` — raw HTTP probe used by
  `apiServerHealthzNow` to flip the api-server status to `state.Running`.
  TLS dialer uses the cluster CA from `localpath.CACert()`, 5 s per-request
  timeout, retried via `retry.Local` for 15 s total. Source:
  [`pkg/minikube/bootstrapper/bsutil/kverify/api_server.go`](https://raw.githubusercontent.com/kubernetes/minikube/master/pkg/minikube/bootstrapper/bsutil/kverify/api_server.go)
  lines 220-265 (URL built at line 237:
  `fmt.Sprintf("https://%s/healthz", net.JoinHostPort(hostname, fmt.Sprint(port)))`).
- `GET /version` — invoked indirectly via client-go's `client.ServerVersion()`
  inside `APIServerVersionMatch` (`api_server.go:109-118`). client-go translates
  this into `GET /version` and decodes the JSON into `version.Info`.
- `GET /api/v1/namespaces/default/serviceaccounts` — `WaitForDefaultSA`
  ([`default_sa.go:38`](https://raw.githubusercontent.com/kubernetes/minikube/master/pkg/minikube/bootstrapper/bsutil/kverify/default_sa.go))
  lists SAs in `default` and asserts one is named `default`.
- `GET /api/v1/namespaces/kube-system/pods` — `WaitForSystemPods`
  ([`system_pods.go:48`](https://raw.githubusercontent.com/kubernetes/minikube/master/pkg/minikube/bootstrapper/bsutil/kverify/system_pods.go))
  unfiltered list, then matches labels `component=` / `k8s-app=` in-memory.
- `GET /api/v1/nodes/<name>` — `WaitNodeCondition`
  ([`node_ready.go:69`](https://raw.githubusercontent.com/kubernetes/minikube/master/pkg/minikube/bootstrapper/bsutil/kverify/node_ready.go))
  polls a single node by name until `status.conditions[type=Ready].status==True`.
- `GET /api/v1/namespaces/<ns>/pods?labelSelector=<label>` — `WaitForAppsRunning`
  ([`pod_ready.go:56`](https://raw.githubusercontent.com/kubernetes/minikube/master/pkg/minikube/bootstrapper/bsutil/kverify/pod_ready.go))
  iterates caller-supplied labels (typically `k8s-app=kube-dns`,
  `k8s-app=kube-proxy`, `component=etcd`, `component=kube-apiserver`,
  `component=kube-controller-manager`, `component=kube-scheduler`).
- Local exec only (no HTTP): `WaitForAPIServerProcess` (`api_server.go:44-64`)
  polls the host pidfile every 500 ms before any HTTP probe fires, and
  `WaitForService` (in `system_svc.go`) shells out to `systemctl is-active kubelet`.

## JSON payloads

minikube itself **does not send JSON request bodies** to the cluster as part of
the smoke test — every readiness gate is a `GET`. The control-plane resources
(Pods, Deployments, ConfigMaps, RBAC) are created by `kubeadm init` upstream of
minikube, so the JSON shapes that matter for cluster bring-up are kubeadm's, not
minikube's. minikube only **adds** the following client-side JSON consumers on
top:

- `kubectl version --client --output=json` (with `--short` fallback) parsed by
  `kubectlVersion()` in
  [`cmd/minikube/cmd/start.go:1049-1076`](https://raw.githubusercontent.com/kubernetes/minikube/master/cmd/minikube/cmd/start.go).
  The struct minikube unmarshals into expects upstream's
  `clientVersion.gitVersion` shape (`{"clientVersion":{"gitVersion":"v1.x.y", ...}}`).
- `client.ServerVersion()` JSON from `GET /version` — minikube decodes the
  standard `k8s.io/apimachinery/pkg/version.Info` payload (`{"major","minor",
  "gitVersion","gitCommit","gitTreeState","buildDate","goVersion","compiler",
  "platform"}`) and calls `.String()`, which produces `gitVersion`.
- Built-image metadata: `cat /version.json` over SSH (`start.go:879-900`)
  reads `{"minikubeVersion":"..."}` written into the VM image at build time.

## Expected responses / assertions

- `/healthz` MUST return HTTP **200** with any body. HTTP **401** triggers a
  distinct unauthorized error; any other status is treated as down
  (`api_server.go:253-259`).
- `/version` MUST return a parseable `version.Info` and the resulting string
  MUST satisfy
  `version.CompareKubeAwareVersionStrings(serverVer, expectedVer) == 0`
  (`api_server.go:115`). The comparator is upstream's semver-ish helper that
  ignores trailing `+build` tags but enforces `major`/`minor`/`patch` equality.
- `kubectl version --output=json` is checked for `clientVersion.major ==
  cluster.major` and minor skew ≤ 1; mismatches log a warning, not a hard fail
  (`start.go:975-1048`).
- ServiceAccount list MUST contain an item with `metadata.name == "default"`
  (`default_sa.go:38-43`).
- `kube-system` pod list MUST include label-matched entries for each component
  in the expected list (`system_pods.go:72-76`), and each must reach
  `status.conditions[type=Ready].status==True`.
- Node `Get` MUST return `status.conditions[type=Ready].status==True` within
  the user-supplied timeout (`node_ready.go:33-52`).
- All polling uses `kconst.APICallRetryInterval` between attempts; problem
  banners fire after `minLogCheckTime = 60s` (`kverify.go:24`).

Gate keys (`kverify.go:27-39`): `apiserver`, `system_pods`, `default_sa`,
`apps_running`, `node_ready`, `kubelet`, `extra`. `DefaultComponents`
(`kverify.go:43`) is `{apiserver: true, system_pods: true}`.

## Rusternetes-compat checklist

Grepped on this worktree (`research/distros-minikube` @ rusternetes
`0c821edb`):

- `/healthz` — present, `crates/api-server/src/router.rs:669` -> `handlers::health::healthz` returning literal `"ok"` (`crates/api-server/src/handlers/health.rs:23-25`). Matches upstream's `pkg/server/healthz/healthz.go` body of `"ok"`, satisfies minikube's `StatusCode == 200` assertion.
- `/livez`, `/readyz` — present, `crates/api-server/src/router.rs:671-672`. minikube does not call these, but other distros do.
- `/version` — present, `crates/api-server/src/router.rs:793` -> `handlers::discovery::get_version` (`crates/api-server/src/handlers/discovery.rs:1466-1482`). Returns the canonical `version.Info` JSON. `gitVersion = "v1.35.0"` (hardcoded at line 1472), so minikube's `APIServerVersionMatch` will pass only when its expected version is also `v1.35.0`.
- `GET /api` / `GET /apis` — present, `crates/api-server/src/router.rs:685` and `:689`. Required by client-go discovery before `ServerVersion()` decode.
- `GET /api/v1/namespaces/:namespace/serviceaccounts` — present, `crates/api-server/src/router.rs:1287-1288`. `WaitForDefaultSA` will succeed once the `default` SA is materialised; bootstrap script does this via `bash scripts/bootstrap-cluster.sh`.
- `GET /api/v1/namespaces/:namespace/pods` — present, `crates/api-server/src/router.rs:853`. Backs minikube's `kube-system` pod list.
- `GET /api/v1/nodes/:name` — present, `crates/api-server/src/router.rs:1068`. Used by `WaitNodeCondition`.
- TLS / CA wiring — minikube reads `localpath.CACert()`; rusternetes ships certs in `.rusternetes/certs/` per `CLAUDE.md`, but the **SAN list** is documented as `172.18.0.2-5, 10.89.0.x` only. minikube uses `hostname` from the kubeconfig server URL — if a user points minikube at rusternetes with a hostname outside the SAN list, the TLS handshake fails before `/healthz` is hit. Worth flagging in compat notes; no code change needed unless we want to support minikube-as-client.
- `kubectl version --client --output=json` — local `kubectl` binary, not a rusternetes endpoint. No grep target.
- Label selector on `kube-system` pods (`k8s-app`, `component`) — labels are applied per-pod by whichever component creates them. `scripts/bootstrap-cluster.sh` creates CoreDNS; confirm via `grep -n 'k8s-app: kube-dns' bootstrap-cluster.yaml` (file is at repo root, `bootstrap-cluster.yaml:1`).

**Net:** every HTTP path minikube touches already exists in rusternetes. The
two non-obvious failure modes are (a) the hardcoded `v1.35.0` in
`get_version()` will mismatch any minikube invocation that doesn't expect that
exact tag, and (b) cert SAN coverage if minikube is ever pointed at a
non-bridge IP.
