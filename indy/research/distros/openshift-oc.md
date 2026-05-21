# OpenShift oc

Source-of-truth: https://github.com/openshift/oc @ `a052bd4018333201e48f7370b2263db2dc6a5c99`
(master HEAD at time of capture, 2026-05-21).

## What this tool does

`oc` vendors `k8s.io/kubectl` and adds OpenShift verbs under `oc adm ...`.
For smoke-testing a fresh control plane the closest "exhaustive client
probe" is `oc adm inspect` — a discovery-first sweep that walks every
group/version, followed by `oc adm must-gather`, which schedules a
privileged pod to collect host-level data. `oc version` is the cheapest
preflight. Unlike vanilla `kubectl`, `oc` exercises **aggregated-discovery
v2** (`AcceptV2`) by default and sets `QPS=Burst=999999` on its REST
config so the api-server gets hammered with parallel resource walks.

## Bootstrap / preflight endpoints

### `oc version`
- `GET /version` — discovery `ServerVersion()`, parsed into `version.Info`.
  `pkg/cli/version/version.go:95-97` via `discoveryClient.ServerVersion()`.
- `GET /apis/config.openshift.io/v1/clusterversions/version` — OpenShift
  ClusterVersion lookup; reads `.status.history[?(@.state=="Completed")]`
  for the last completed update. `pkg/cli/version/version.go:99-117`.
  Only the kube `/version` call is in-scope for rusternetes; the OpenShift
  group is out-of-scope (see Rusternetes-compat checklist).

### `oc adm inspect`
Boots the discovery client and overrides QPS/Burst for parallelism:
`pkg/cli/admin/inspect/inspect.go:87-88` sets `RESTConfig.QPS = 999999`
and `RESTConfig.Burst = 999999`. Then in order:

- `GET /api` with header `Accept: <discovery.AcceptV2>` — written verbatim
  to `<DestDir>/aggregated-discovery-api.yaml`.
  `pkg/cli/admin/inspect/discovery.go:~14` (RESTClient `.AbsPath("/api")`).
- `GET /apis` with the same `Accept: <discovery.AcceptV2>` —
  written to `<DestDir>/aggregated-discovery-apis.yaml`.
  `pkg/cli/admin/inspect/discovery.go:~11`.
  Both calls go through `discoveryClient.RESTClient().Get().AbsPath(url)
  .SetHeader("Accept", discovery.AcceptV2).Do(ctx).ContentType(...)
  .StatusCode(...).Raw()`.
- `discoveryClient.ServerGroupsAndResources()` — populates the per-group
  resource catalogue used to dispatch sub-walks.
  `pkg/cli/admin/inspect/inspect.go:245-246`.

Cluster-scoped fan-out (`pkg/cli/admin/inspect/inspect.go:36-99`):
- `GET /apis/config.openshift.io/v1/clusteroperators` plus per-name GETs
  through `gatherRelatedObjects()`.
- `GET /api/v1/namespaces` for the namespaces flagged by `.status.relatedObjects`.
- `GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions`.
- `GET /apis/admissionregistration.k8s.io/v1/{mutating,validating}webhookconfigurations`.

Per-namespace fan-out from `namespaceResourcesToCollect()`
(`pkg/cli/admin/inspect/namespace.go:19-37`):
- `GET /api/v1/namespaces/{ns}` (the namespace itself)
- LIST against the pseudo-group `all` plus the explicit GVRs:
  `configmaps`, `events`, `endpoints`, `endpointslices`,
  `networkpolicies`, `persistentvolumeclaims`, `poddisruptionbudgets`,
  `secrets`, `networking.k8s.io/ingresses` and the OpenShift-only
  `egressfirewalls`, `egressqoses`, `servicemonitors`,
  `userdefinednetworks`.
- For each pod found: two log subresource reads per container —
  `GET /api/v1/namespaces/{ns}/pods/{pod}/log?container={c}&timestamps=true&previous=false`
  and the `previous=true` variant.
  `pkg/cli/admin/inspect/pod.go:288-322`. `SinceTime` / `SinceSeconds`
  are appended when `--since-time` / `--since` are set.
  Retries set `insecureSkipTLSVerifyBackend=true`
  (`pod.go:304`, `pod.go:330`).

### `oc adm must-gather`
- `POST /api/v1/namespaces` — creates a temporary namespace with labels
  `openshift.io/run-level=0`, `pod-security.kubernetes.io/enforce=privileged`,
  `security.openshift.io/scc.podSecurityLabelSync=false`.
  `pkg/cli/admin/mustgather/mustgather.go:1043` and `1061-1069`.
- `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` — binds
  `system:serviceaccount:{ns}:default` to ClusterRole `cluster-admin`.
  `mustgather.go:1048`, body shape at `mustgather.go:1272-1295`.
- `POST /api/v1/namespaces/{ns}/pods` — the gather pod (one per plugin
  image). `mustgather.go:921`. Pod shape below.
- `GET /api/v1/namespaces/{ns}/pods/{name}` — polled every 10 s for
  `Status.ContainerStatuses[0].State.Running` then `.Terminated.ExitCode`.
  `mustgather.go:949` `waitForGatherToComplete()`, `983`
  `waitForGatherContainerRunning()`, `963` exit-code check.
- `GET /api/v1/namespaces/{ns}/pods/{name}/log?container=gather` —
  streamed via `Pods(...).GetLogs(...).Stream(ctx)`.
  `mustgather.go:1206`.
- `POST /api/v1/namespaces/{ns}/pods/{name}/exec` — invoked indirectly
  through `rsync.RsyncOptions.RunRsync()` to pull `/must-gather/` out of
  the pod. `mustgather.go:1222`.

### `oc adm upgrade recommend`
- `GET /apis/config.openshift.io/v1/clusterversions/version` —
  the entire decision tree reads `Status.AvailableUpdates` /
  `Status.ConditionalUpdates` from this one object.
  `pkg/cli/admin/upgrade/recommend/recommend.go:171`.

## JSON payloads

`oc adm inspect` is almost entirely GET; the only writes happen via
`metav1.GetOptions{}` / `ListOptions{}` parameters wrapped by
client-go. The notable wire-level traits to mimic:

- **`Accept: <discovery.AcceptV2>`** on `/api` and `/apis`. The full
  Accept value is
  `application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,
  application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,
  application/json`. Discovery must content-negotiate or the response is
  treated as malformed.
- **`QPS=Burst=999999`** — the inspect command bursts hundreds of
  concurrent GETs against `/apis/<group>/<version>/<plural>`. Any
  api-server-side per-client throttling needs to be high enough to not
  hit `429 Too Many Requests` for a legitimate inspect.

`oc adm must-gather` does post bodies. The gather pod shape
(`mustgather.go` `newPod()` builder):

```json
{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "generateName": "must-gather-",
    "labels": {"app": "must-gather"}
  },
  "spec": {
    "priorityClassName": "system-cluster-critical",
    "restartPolicy": "Never",
    "hostNetwork": false,
    "containers": [{
      "name": "gather",
      "image": "<plugin-image>",
      "imagePullPolicy": "IfNotPresent",
      "command": ["/bin/bash", "-c", "<volumeChecker & gatherCommand>"],
      "securityContext": {"capabilities": {"add": ["CAP_NET_RAW"]}},
      "volumeMounts": [
        {"name": "must-gather-output", "mountPath": "/must-gather/"}
      ]
    }],
    "volumes": [{"name": "must-gather-output", "emptyDir": {}}],
    "tolerations": [{"operator": "Exists"}]
  }
}
```

ClusterRoleBinding body (`mustgather.go:1272-1295`):

```json
{
  "apiVersion": "rbac.authorization.k8s.io/v1",
  "kind": "ClusterRoleBinding",
  "metadata": {"name": "must-gather-<rand>"},
  "subjects": [{
    "kind": "ServiceAccount",
    "name": "default",
    "namespace": "<temp-ns>"
  }],
  "roleRef": {
    "apiGroup": "rbac.authorization.k8s.io",
    "kind": "ClusterRole",
    "name": "cluster-admin"
  }
}
```

## Expected responses / assertions

- `/api` and `/apis` MUST return aggregated-discovery v2 when
  `Accept: application/json;g=apidiscovery.k8s.io;v=v2;...` is offered.
  Falling back to the v1 `APIGroupList` shape works for vanilla kubectl
  but `oc adm inspect` writes the raw body to disk and validates the
  content type — see `discovery.go` `ContentType(&responseContentType)`.
- `GET /apis/<group>/<version>/<plural>` must accept arbitrary CRD
  groups and return either a normal `List` or `404` for unknown groups —
  inspect tolerates the 404 (`apierrors.IsNotFound` at `inspect.go:236-241`)
  but does NOT tolerate a `500`.
- `GET /api/v1/namespaces/{ns}/pods/{pod}/log` must accept the
  `container=`, `previous=`, `timestamps=`, `sinceTime=`, `sinceSeconds=`,
  and `insecureSkipTLSVerifyBackend=` query params.
- `POST /api/v1/namespaces/{ns}/pods` must accept
  `securityContext.capabilities.add=["CAP_NET_RAW"]`,
  `priorityClassName: system-cluster-critical`, and `tolerations:
  [{operator: Exists}]` without rejecting on admission.
- Polled pod GETs must populate `status.containerStatuses[0].state`
  through `Waiting -> Running -> Terminated` with `.terminated.exitCode`
  set, otherwise `waitForGatherToComplete` loops forever.
- `GET /version` must return a `version.Info` JSON document with
  `major`, `minor`, `gitVersion` fields.

## Rusternetes-compat checklist

- `GET /version` — present. `crates/api-server/src/router.rs:793`.
- `GET /api`, `/api/v1`, `/apis` — present. `router.rs:685-691`.
- Aggregated-discovery v2 content negotiation — implemented.
  `crates/api-server/src/handlers/discovery.rs:154,341,3736-3787`;
  conformance at
  `crates/api-server/tests/conformance_apimachinery_aggregation_discovery.rs:363-401`.
- `GET /openapi/v2` and `/openapi/v3{/*path}` — present.
  `router.rs:795-798`. (`inspect` doesn't fetch these, but the discovery
  cache hits them on first contact.)
- `GET /healthz`, `/healthz/verbose`, `/livez`, `/readyz` — present.
  `router.rs:669-672`.
- `POST /api/v1/namespaces`, `GET /api/v1/namespaces/{name}` — covered
  by the generic namespace handlers in `router.rs`.
- `POST /apis/rbac.authorization.k8s.io/v1/clusterrolebindings` —
  present. `router.rs:1355-1359`.
- `POST /api/v1/namespaces/{ns}/pods`, `GET .../pods/{pod}/log`,
  `POST .../pods/{pod}/exec` — present. `router.rs:870,874`.
- `GET /apis/admissionregistration.k8s.io/v1/{mutating,validating}webhookconfigurations`
  — present. `router.rs:1659-1679`.
- `GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions` —
  present. `router.rs:1622-1634`.
- `LIST /api/v1/events`, `/api/v1/namespaces/{ns}/events` — present.
  `router.rs:1524-1536`.
- TokenRequest POST — present.
  `crates/api-server/src/handlers/authentication.rs:108-236`,
  `crates/api-server/src/handlers/service_account.rs:455`.
- `POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews` —
  present. `crates/api-server/src/handlers/authorization.rs:92-118`.

OpenShift-only groups (out of scope, but the discovery contract still
matters):

- `route.openshift.io`, `image.openshift.io`, `config.openshift.io`,
  `operator.openshift.io`, `security.openshift.io`, `apps.openshift.io`
  — rusternetes does not implement these and must **not** advertise
  them in `/apis`. `oc version` then skips the ClusterVersion branch
  (`pkg/cli/version/version.go:99-100` returns NotFound, tolerated).
  `oc adm inspect clusteroperator/...` short-circuits with "no matches"
  — acceptable.
- Critical rule: `oc adm inspect` issues the AcceptV2 GET against `/api`
  and `/apis` *before* walking user-requested resources. If rusternetes
  ever returns a non-v2 body to a v2-only Accept header, every `oc adm`
  subcommand goes red. Keep
  `application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList`
  wired through any future discovery refactor.
- `QPS=Burst=999999`: rusternetes must not impose a low per-client rate
  limit. Current router has no `tower::limit` / `RateLimit` middleware
  in `crates/api-server/src/`, so this is safe today — track if we ever
  add one.
