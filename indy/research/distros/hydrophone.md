# Hydrophone

Source-of-truth: https://github.com/kubernetes-sigs/hydrophone @ `af7045acf202b57d763d0450b32b577dd2fc3e05`
(Merge PR #304 — "Bump Kubernetes on (05/18/2026)", current `main` HEAD at time of
research).

Verified upstream files (all `WebFetch`-resolved):

- `cmd/root.go`
- `pkg/conformance/runner.go`
- `pkg/conformance/deploy.go`
- `pkg/conformance/cleanup.go`
- `pkg/conformance/const.go`
- `pkg/conformance/client/client.go`
- `pkg/conformance/client/logs.go`
- `pkg/conformance/client/download.go`
- `pkg/conformance/client/exitcode.go`
- `pkg/common/pod.go`

## What this tool does

Hydrophone is the SIG-testing-replacement for Sonobuoy: a small Go CLI that runs the
`registry.k8s.io/conformance:<version>` image as a **single Pod** (not as a Sonobuoy
worker DaemonSet + aggregator). It does *no* in-cluster preflight DaemonSet, ships
no plugin model, and assumes the api-server, scheduler, and a node that can pull
the conformance image are already healthy. The full lifecycle is: discover server
version → derive image tag → create Namespace + ServiceAccount + ClusterRole +
ClusterRoleBinding (+ optional ConfigMap) → create one Pod with a `conformance` and
an `output-sidecar` container → stream Pod logs + watch for terminated state →
`exec cat /tmp/results/{e2e.log,junit_01.xml}` to download results → delete the
RBAC and Namespace. Because there is no aggregator pod and no DaemonSet, the
api-server surface Hydrophone exercises is a strict subset of Sonobuoy's.

## Bootstrap / preflight endpoints

- `GET /version` — server version discovery, used both to log cluster info and to
  derive the default conformance image tag (`registry.k8s.io/conformance:<v>`).
  `cmd/root.go:91-94` (`clientset.ServerVersion()`), `cmd/root.go:101-103`
  (image string built), `cmd/root.go:177-186` (`normalizeVersion` returns e.g.
  `v1.35.0`).
- `POST /api/v1/namespaces` — creates the conformance namespace if missing
  (`pkg/conformance/deploy.go:186`).
- `POST /api/v1/namespaces/{ns}/serviceaccounts` — `e2e-conformance-test-sa`
  (`pkg/conformance/deploy.go:200`).
- `POST /apis/rbac.authorization.k8s.io/v1/clusterroles` — namespaced-name
  ClusterRole granting `*` on `*` plus `get` on `/metrics`, `/logs`, `/logs/*`
  (`pkg/conformance/deploy.go:213`).
- `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` — binds the SA to
  the ClusterRole (`pkg/conformance/deploy.go:226`).
- `POST /api/v1/namespaces/{ns}/configmaps` — only when `--test-repo-list` is set;
  creates `repo-list-config` (`pkg/conformance/deploy.go:265`).
- `POST /api/v1/namespaces/{ns}/pods` — `e2e-conformance-test`
  (`pkg/conformance/deploy.go:289`, via `common.CreatePod`).
- `GET /api/v1/namespaces/{ns}/pods?watch=true&fieldSelector=metadata.name=e2e-conformance-test`
  — watch polling loop, success on first non-`Pending` phase
  (`pkg/common/pod.go:34`; success condition `pod.Status.Phase != PodPending`).
- `GET /api/v1/namespaces/{ns}/pods/{name}` — single status read used by
  `IsPodRunning` (`pkg/conformance/client/logs.go:47-48`).
- `GET /api/v1/namespaces/{ns}/pods/{name}/log?follow=true&container=conformance-container`
  — streamed via `Follow: true` (`pkg/conformance/client/logs.go:89`).
- `POST /api/v1/namespaces/{ns}/pods/{name}/exec?container=conformance-container&command=cat&command=/tmp/results/e2e.log`
  — exec subresource over WebSocket first then SPDY fallback (KEP-4006)
  (`pkg/conformance/client/logs.go:116-124`, `pkg/conformance/client/download.go:62-73`).
- `GET /api/v1/namespaces/{ns}/pods/{name}/log?tailLines=30` — final tail read
  for "did the suite finish" assertion (`pkg/conformance/client/logs.go:152`).
- `DELETE /apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{name}`
  (`pkg/conformance/cleanup.go:41-46`).
- `DELETE /apis/rbac.authorization.k8s.io/v1/clusterroles/{name}`
  (`pkg/conformance/cleanup.go:48-53`).
- `DELETE /api/v1/namespaces/{name}` plus a Watch for completion
  (`pkg/conformance/cleanup.go:57-75`).

## JSON payloads

Names are stable (`pkg/conformance/const.go`): `PodName=e2e-conformance-test`,
`ServiceAccountName=conformance-serviceaccount`,
`ClusterRoleName=conformance-serviceaccount`,
`ClusterRoleBindingName=conformance-serviceaccount-role`,
`ConformanceContainer=conformance-container`,
`OutputContainer=output-container`. Each is suffixed with the namespace via
`TestRunner.namespacedName` (`pkg/conformance/runner.go`) for the cluster-scoped
RBAC names so multiple parallel runs do not collide.

```yaml
# Namespace
apiVersion: v1
kind: Namespace
metadata:
  name: <config.Namespace>            # default: conformance
```

```yaml
# ServiceAccount
apiVersion: v1
kind: ServiceAccount
metadata:
  name: conformance-serviceaccount
  namespace: <config.Namespace>
  labels: {component: conformance}
```

```yaml
# ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: <ns>-conformance-serviceaccount
  labels: {component: conformance}
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["*"]
  - nonResourceURLs: ["/metrics", "/logs", "/logs/*"]
    verbs: ["get"]
```

```yaml
# ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: <ns>-conformance-serviceaccount-role
  labels: {component: conformance}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: <ns>-conformance-serviceaccount}
subjects:
  - {kind: ServiceAccount, name: conformance-serviceaccount, namespace: <ns>}
```

```yaml
# Pod (abbreviated; full env list in deploy.go:289)
apiVersion: v1
kind: Pod
metadata: {name: e2e-conformance-test, namespace: <ns>}
spec:
  serviceAccountName: conformance-serviceaccount
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  volumes:
    - {name: output-volume, emptyDir: {}}
  containers:
    - name: conformance-container
      image: <config.ConformanceImage>          # registry.k8s.io/conformance:v1.35.0
      imagePullPolicy: IfNotPresent
      env:
        - {name: E2E_FOCUS,        value: "<--focus>"}
        - {name: E2E_SKIP,         value: "<--skip>"}
        - {name: E2E_PROVIDER,     value: skeleton}
        - {name: E2E_VERBOSITY,    value: "<--verbosity>"}
        - {name: E2E_USE_GO_RUNNER, value: "true"}
        - {name: E2E_EXTRA_ARGS,        value: "<space-joined>"}
        - {name: E2E_EXTRA_GINKGO_ARGS, value: "<space-joined>"}
      volumeMounts: [{name: output-volume, mountPath: /tmp/results}]
      securityContext:
        allowPrivilegeEscalation: false
        runAsNonRoot: true
        runAsUser: 65534
        seccompProfile: {type: RuntimeDefault}
        capabilities: {drop: [ALL]}
    - name: output-container
      image: <config.BusyboxImage>
      command: ["/bin/sh", "-c", "sleep infinity"]
      volumeMounts: [{name: output-volume, mountPath: /tmp/results}]
      securityContext: { ... same hardening ... }
```

## Expected responses / assertions

- `GET /version` must return a JSON `VersionInfo` whose `gitVersion` parses through
  `normalizeVersion` (`cmd/root.go:177-186`); a leading `v` plus three dotted
  ints is required to derive the image tag.
- `POST` of each RBAC object must return 201 with the SA token controller having
  populated `secrets[]` (Pod will fail to start otherwise — Hydrophone does not
  poll the SA itself; it relies on Pod-start retries).
- Pod-create watch loop succeeds the moment `pod.Status.Phase != "Pending"`
  (`pkg/common/pod.go`); failure conditions are `containerStatus.State.Waiting.Reason`
  or `Terminated.Reason` in `{ErrImagePull, ImagePullBackOff, Error, CrashLoopBackOff}`,
  surfaced by `CheckFailedPod()`.
- Log streaming requires the standard `GET …/log?follow=true` chunked
  `text/plain` stream.
- Exec uses `SubResource("exec")` with `command=cat&command=/tmp/results/e2e.log`,
  `container=conformance-container`, `stdout=true`, `stderr=true`,
  `tty=false`. WebSocket transport is tried first (KEP-4006 `v5.channel.k8s.io`),
  then falls back to SPDY.
- Exit-code is read by **watching** Pod events and reading
  `containerStatus.State.Terminated.ExitCode`
  (`pkg/conformance/client/exitcode.go:34-56`) — Hydrophone never polls
  `GET pod` for this.
- Cleanup uses `DELETE` returning 200/202; the namespace deletion is awaited via
  a Watch with the `metadata.name` field selector until the DELETE event arrives.

## Rusternetes-compat checklist

Grepped against this worktree (`/home/jones/PhpstormProjects/rusternetes`):

- `GET /version` — wired
  (`crates/api-server/src/router.rs:793` → `handlers::discovery::get_version` at
  `crates/api-server/src/handlers/discovery.rs:1468`, returns `major:"1"`,
  `minor:"35"`, `gitVersion:"v1.35.0"`). **Compatible** with Hydrophone's
  `normalizeVersion` regex.
- `POST /api/v1/namespaces` — wired
  (`crates/api-server/src/router.rs:817`, `handlers::namespace::list`/`create`).
- `POST /api/v1/namespaces/{ns}/serviceaccounts` — wired
  (`crates/api-server/src/router.rs:1287`).
- `POST /apis/rbac.authorization.k8s.io/v1/clusterroles` — wired
  (`crates/api-server/src/router.rs:1343`).
- `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` — wired
  (`crates/api-server/src/router.rs:1355`).
- `POST /api/v1/namespaces/{ns}/configmaps` — wired
  (`crates/api-server/src/router.rs:1020`).
- `POST /api/v1/namespaces/{ns}/pods` — wired
  (`crates/api-server/src/router.rs:853`).
- `GET /api/v1/watch/namespaces/{ns}/pods` — wired
  (`crates/api-server/src/router.rs:937`); Hydrophone uses a field-selector
  watch — confirm `fieldSelector=metadata.name=…` is honored on this path.
- `GET /api/v1/namespaces/{ns}/pods/{name}/log` — wired
  (`crates/api-server/src/router.rs:870`,
  `crates/api-server/src/handlers/pod_subresources.rs:127` `generate_pod_logs`);
  must honor `follow=true`, `tailLines=30`, `container=`.
- `POST /api/v1/namespaces/{ns}/pods/{name}/exec` — wired
  (`crates/api-server/src/router.rs:874`,
  `crates/api-server/src/handlers/pod_subresources.rs:419`); both SPDY and
  WebSocket transports documented at
  `crates/api-server/src/handlers/pod_subresources.rs:5`. Hydrophone prefers
  WebSocket first → SPDY fallback, so both must actually wire stdout/stderr.
- `DELETE /apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{name}` —
  wired (`crates/api-server/src/router.rs:1359`).
- `DELETE /apis/rbac.authorization.k8s.io/v1/clusterroles/{name}` — wired
  (`crates/api-server/src/router.rs:1347`).
- `DELETE /api/v1/namespaces/{name}` — wired
  (`crates/api-server/src/router.rs:821`); cleanup also watches the collection
  for the DELETE event via
  `GET /api/v1/watch/namespaces` (`crates/api-server/src/router.rs:839`).

No `scripts/run-conformance.sh` Hydrophone integration exists today
(`scripts/run-conformance.sh` calls `sonobuoy`, line 16). Adding a
`scripts/run-hydrophone.sh` is out of scope for this catalog.
