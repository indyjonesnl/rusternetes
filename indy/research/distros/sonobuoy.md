# Sonobuoy

Source-of-truth: https://github.com/vmware-tanzu/sonobuoy @ tag `v0.57.3`
(default branch `main`, HEAD `09b10f4e` at time of writing).

## What this tool does

`sonobuoy run` is the canonical wrapper around the upstream Kubernetes e2e
binary. It (1) runs preflight checks against the live cluster, (2) generates
a manifest (Namespace, ServiceAccount, ClusterRole(Binding) or
Role(Binding), ConfigMaps, optional Secrets, a Service, and the
`sonobuoy` aggregator Pod) and applies it via the dynamic client, then
(3) the aggregator Pod launches the configured plugins (commonly `e2e`
as a Job and `systemd-logs` as a DaemonSet). Worker containers next to
each plugin PUT their tarballs back to the aggregator's HTTPS sidecar
on the in-cluster Service (`sonobuoy-aggregator:8080`). When all
expected results land, the aggregator writes the run status to an
annotation on its own pod, the CLI polls that annotation, then
`sonobuoy retrieve` streams the tarball out via `pods/exec` (`tar`).

## Bootstrap / preflight endpoints

All three preflight checks live in `pkg/client/preflight.go`
(registered in `validPreflightChecks`, lines 36-40).

- `GET /version` — `preflightVersionCheck`,
  `pkg/client/preflight.go:126-131`. Compares the cluster's
  `kubeVersion` against sonobuoy's min/max supported range; warns when
  above the max but does not abort.
- `GET /api/v1/namespaces/{name}` — `preflightExistingNamespace`,
  `pkg/client/preflight.go:161-166`. Refuses to start if the target
  namespace (`sonobuoy` by default) already exists.
- `GET /api/v1/namespaces/{namespace}/pods?labelSelector=...` —
  `preflightDNSCheck`, `pkg/client/preflight.go:89-94`. Lists DNS pods
  in `kube-system` (or configured namespace) matching the supplied
  label selector and errors if none are found.

Implicit discovery surface used by every step that follows: `GET /api`,
`GET /apis`, `GET /apis/{group}/{version}` via the client-go discovery
client when building the dynamic client (`pkg/client/run.go:166-182`,
`RunManifest` at `pkg/client/run.go:54-109`).

## JSON payloads

Created by the CLI itself before any plugin runs
(`pkg/client/gen.go`, line numbers approximate):

- `POST /api/v1/namespaces` — Namespace `sonobuoy` (skipped when
  `AggregatorPermissions == cluster-admin` and an existing namespace is
  reused), labelled with pod-security-admission enforcement
  (`gen.go:~1020`).
- `POST /api/v1/namespaces/sonobuoy/serviceaccounts` —
  `sonobuoy-serviceaccount` (skipped if `ExistingServiceAccount` is
  set, `gen.go:~1010`).
- `POST /apis/rbac.authorization.k8s.io/v1/clusterroles` and
  `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` —
  cluster-admin or cluster-read modes (`gen.go:~1000`).
- `POST /apis/rbac.authorization.k8s.io/v1/namespaces/sonobuoy/roles`
  and `POST /apis/.../namespaces/sonobuoy/rolebindings` — for the
  namespace-admin mode.
- `POST /api/v1/namespaces/sonobuoy/configmaps`:
  - `sonobuoy-config-cm` — JSON-marshalled run config (`gen.go:~980`).
  - `sonobuoy-plugins-cm` — serialised plugin manifests (`gen.go:~852`).
  - `plugin-<name>-cm` per plugin that has inline ConfigMap data
    (`gen.go:~826`).
- `POST /api/v1/namespaces/sonobuoy/secrets` — optional
  `kubernetes.io/dockerconfigjson` (`E2EDockerConfigFile`,
  `gen.go:~750`) and optional Opaque SSH-key secret (`gen.go:~770`).
- `POST /api/v1/namespaces/sonobuoy/services` —
  `sonobuoy-aggregator` ClusterIP on port 8080 (`gen.go:~950`).
- `POST /api/v1/namespaces/sonobuoy/pods` — the aggregator Pod
  `sonobuoy` (`gen.go:~860`), which then creates the plugin Job /
  DaemonSet via the same dynamic client at runtime.
- `PATCH /api/v1/namespaces/sonobuoy/pods/sonobuoy` (content-type
  `application/merge-patch+json`) — the aggregator writes the
  aggregation status as an annotation on its own pod
  (`pkg/discovery/discovery.go:88,194,307` via `setPodStatusAnnotation`
  / `client.CoreV1().Pods(ns).Patch(...)`).

The aggregator container additionally exposes an HTTPS server inside
the cluster (`pkg/plugin/aggregation/run.go:114-133`,
`auth.MakeServerConfig` for TLS, `ListenAndServeTLS` in a goroutine)
with four routes registered in `pkg/plugin/aggregation/handler.go`
(`NewHandler`, lines 91-109):

- `PUT  /api/v1/results/by-node/{node}/{plugin}` (`handler.go:39,54,103`)
- `PUT  /api/v1/results/global/{plugin}`        (`handler.go:43,57,104`)
- `POST /api/v1/progress/by-node/{node}/{plugin}` (`handler.go:47,60,106`)
- `POST /api/v1/progress/global/{plugin}`       (`handler.go:51,63,107`)

Worker side (the sidecar that ships with every plugin pod) builds the
request in `pkg/worker/request.go`: `http.MethodPut` (`request.go:79`)
with `Content-Type: <mime>` (line 80) and either
`Content-Disposition: attachment;filename=<name>` (line 84) or just
`attachment` (line 85). The aggregator parses the filename in
`handler.go:223-231`. Progress relay from the plugin container into
the worker happens on `localProgressURLPath = "/progress"`
(`pkg/worker/worker.go:24,84,89`) which the worker then POSTs to the
aggregator's progress endpoint.

## Expected responses / assertions

- Preflight must succeed before any manifest is applied
  (`cmd/sonobuoy/app/run.go:68`); each failure short-circuits the run.
- `/version` must return a JSON `version.Info`-shaped object with a
  parseable `gitVersion` so sonobuoy's semver comparison can run.
- `GET /api/v1/namespaces/sonobuoy` must return `404 NotFound` (NOT 200)
  for the run to proceed — sonobuoy treats a present namespace as a
  prior, unfinished run.
- DNS pod list must return at least one pod matching the configured
  label selector (default `k8s-app=kube-dns` in `kube-system`).
- Discovery (`/api`, `/apis`, `/apis/{group}/{version}`) must return
  the resource lists the dynamic client needs for every Kind in the
  generated manifest — otherwise `RunManifest` errors before any
  resource is created.
- Aggregator status pull: `GetStatus`
  (`pkg/client/status.go:24-50`, delegating to `aggregation.GetStatus`
  at `status.go:49`) reads the `sonobuoy.hept.io/status` annotation on
  the aggregator pod, which means **PATCH on
  `/api/v1/namespaces/sonobuoy/pods/sonobuoy` must persist
  `metadata.annotations` and a subsequent GET must return them**.
- Retrieve: `POST /api/v1/namespaces/sonobuoy/pods/sonobuoy/exec` with
  command `["/sonobuoy", "splat", <path>]` and SPDY upgrade
  (`pkg/client/retrieve.go:74-104`). The exec stream must deliver the
  tarball bytes back to stdout for `UntarAll`
  (`pkg/client/retrieve.go:134-211`) to extract.

## Rusternetes-compat checklist

Grepped against `crates/api-server/src/router.rs` and
`scripts/run-conformance.sh` at HEAD `6be308f9` (fork/main).

- `GET /version` — covered, `crates/api-server/src/router.rs:793`.
- `GET /api/v1/namespaces/:name` — covered,
  `crates/api-server/src/router.rs:821`.
- `POST /api/v1/namespaces` — covered,
  `crates/api-server/src/router.rs:817`.
- `GET /api/v1/namespaces/:namespace/pods` (with `labelSelector`) —
  covered as part of the pods routes (label selector handling is in
  the list handler, not the router); needs verification that the DNS
  selector form `k8s-app=kube-dns` evaluates correctly.
- `POST /api/v1/namespaces/:namespace/serviceaccounts` — covered,
  `crates/api-server/src/router.rs:1287`.
- `POST /api/v1/namespaces/:namespace/configmaps` — covered,
  `crates/api-server/src/router.rs:1020`.
- `POST /api/v1/namespaces/:namespace/secrets` — covered,
  `crates/api-server/src/router.rs:1042`.
- `POST /api/v1/namespaces/:namespace/services` — covered,
  `crates/api-server/src/router.rs:947`.
- `POST /apis/rbac.authorization.k8s.io/v1/clusterroles` — covered,
  `crates/api-server/src/router.rs:1343`.
- `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` —
  covered, `crates/api-server/src/router.rs:1355`.
- `POST /api/v1/namespaces/:namespace/pods` — covered (via the pods
  collection route just before `crates/api-server/src/router.rs:857`).
- `PATCH /api/v1/namespaces/:namespace/pods/:name` (merge-patch
  semantics, must persist `metadata.annotations`) — route present at
  `crates/api-server/src/router.rs:857-860` (`.patch(handlers::pod::patch)`);
  annotations-persistence is the load-bearing behaviour for `sonobuoy
  status`, worth a targeted test.
- `POST /api/v1/namespaces/:namespace/pods/:name/exec` (SPDY upgrade,
  carries `["/sonobuoy", "splat", <path>]`) — route present at
  `crates/api-server/src/router.rs:874-877` via
  `handlers::pod_subresources::exec`. SPDY/WebSocket streaming and
  tar-over-stdout fidelity is the likely gap; `sonobuoy retrieve` will
  fail loudly if the stream truncates or upgrade negotiation breaks.
- `POST /apis/apps/v1/namespaces/:namespace/daemonsets` (for the
  `systemd-logs` plugin) — covered,
  `crates/api-server/src/router.rs:1197`.
- `POST /apis/batch/v1/namespaces/:namespace/jobs` (for the `e2e`
  plugin Job) — covered, `crates/api-server/src/router.rs:1231`.
- `GET /api`, `GET /apis`, `GET /apis/:group/:version` — covered,
  `crates/api-server/src/router.rs:685-714`.
- `scripts/run-conformance.sh` — drives the lifecycle as
  `sonobuoy run --mode=...` (line 78) → `sonobuoy retrieve` (line 93)
  → `sonobuoy results` (lines 98, 101); pre-cleans the `sonobuoy`
  namespace + resources (lines 22-30) and pokes
  `https://localhost:6443/api/v1/namespaces/kube-system/pods/coredns`
  (line 44) to bounce CoreDNS before each run. No additional sonobuoy
  endpoints are touched outside the CLI.

No route gaps spotted; the remaining risk surface is **behavioural**:
merge-patch annotation persistence on `pods/{name}` and SPDY tar
streaming on `pods/{name}/exec` — both of which `sonobuoy retrieve` /
`sonobuoy status` exercise on every conformance run.
