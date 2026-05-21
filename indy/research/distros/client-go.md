# client-go / kubectl

Source-of-truth (fetched 2026-05-21, `master` branch):

- `kubernetes/client-go` discovery:
  <https://github.com/kubernetes/client-go/blob/master/discovery/discovery_client.go>
  (raw: <https://raw.githubusercontent.com/kubernetes/client-go/master/discovery/discovery_client.go>)
- `kubernetes/client-go` aggregated discovery merger:
  <https://github.com/kubernetes/client-go/blob/master/discovery/aggregated_discovery.go>
- `kubernetes/client-go` OpenAPI v3 root walker:
  <https://github.com/kubernetes/client-go/blob/master/openapi3/root.go>
- `kubernetes/client-go` REST config / User-Agent:
  <https://github.com/kubernetes/client-go/blob/master/rest/config.go>
- `kubectl version`:
  <https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/kubectl/pkg/cmd/version/version.go>
- `kubectl cluster-info`:
  <https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/kubectl/pkg/cmd/clusterinfo/clusterinfo.go>
- `kubectl get`:
  <https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/kubectl/pkg/cmd/get/get.go>
- `apimachinery` version payload shape:
  <https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/version/types.go>

## What this surface is

`client-go` is the canonical Kubernetes Go SDK. Every other distro tool
— kubectl, kubeadm, helm, controller-runtime, kustomize, sonobuoy,
hydrophone, the e2e harness, every controller — calls into the same
`discovery.DiscoveryClient` to learn which group/versions and resources
the api-server exposes, and into `rest.RESTClient` to issue typed
requests. If a server is broken from `client-go`'s point of view, it is
broken for everything downstream. The endpoints below are therefore the
absolute minimum smoke surface a rusternetes deployment has to satisfy
before any conformance suite will progress past startup.

## Bootstrap / preflight endpoints

All requests are HTTPS `GET` unless otherwise stated.

- `GET /api` — `DiscoveryClient.ServerGroups()` →
  `downloadLegacy()` builds the request at
  `discovery_client.go:277-282`. Returns `APIVersions` (legacy) or
  `APIGroupDiscoveryList` (aggregated v2) depending on `Accept`.
- `GET /apis` — `downloadAPIs()` at
  `discovery_client.go:315-320`. Returns `APIGroupList` or
  `APIGroupDiscoveryList`.
- `GET /apis/{group}/{version}` — per-group resource list,
  `ServerResourcesForGroupVersion()` at `discovery_client.go:391`.
  Returns `APIResourceList` (kind+verbs+namespaced flag per resource).
  Called once per group/version after `/apis` to populate the RESTMapper.
- `GET /version` — `DiscoveryClient.ServerVersion()` at
  `discovery_client.go:580`. Returns `apimachinery/pkg/version.Info`
  (see JSON shape below). `kubectl version` calls this from
  `version.go:167` via `o.discoveryClient.ServerVersion()`.
- `GET /openapi/v2` — `DiscoveryClient.OpenAPISchema()` at
  `discovery_client.go:588`. Used by kubectl validation and dry-run.
- `GET /openapi/v3` — `openapi3.Root.GroupVersions()` issues
  `AbsPath("/openapi/v3")` (openapi3/root.go:48) and walks the
  returned `paths` map. Each `paths[<key>].serverRelativeURL` is then
  fetched individually (e.g. `/openapi/v3/apis/apps/v1?hash=...`).
- `GET /api/v1/namespaces/{ns}/{resource}[?...]` — what
  `kubectl get pods` ultimately issues. `get.go:391-450` chains a
  Builder → RESTMapper → typed request; `RequestChunksOf(chunkSize)`
  (line 398) adds `?limit=&continue=` for paginated lists.
  `r.Watch(rv)` (line 351-370) adds `?watch=true`.
- `kubectl cluster-info` — lists Services in `kube-system` with
  `kubernetes.io/cluster-service=true`
  (`clusterinfo.go:63-64`) and constructs proxy URLs
  `/api/{version}/namespaces/{ns}/services/{name}/proxy` (line 82)
  or `/apis/{group}/{version}/namespaces/{ns}/services/{name}/proxy`
  (line 84). Also prints `o.Client.Host` (line 75) as the
  "Kubernetes control plane" URL — meaning the address has to be
  resolvable from the kubeconfig context, no extra request needed.

## JSON payloads

Discovery is `GET`-only; client-go negotiates response shape entirely
through `Accept`. The server MUST recognise these media types
(`discovery_client.go:55,59-67`):

```text
AcceptV1        = application/json
AcceptV2        = application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList
AcceptV2NoPeer  = application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=nopeer
openAPIV2mimePb = application/com.github.proto-openapi.spec.v2@v1.0+protobuf
```

`selectDiscoveryAcceptHeader(useLegacy, nopeer)` (lines 330-338) sends
either `AcceptV1` alone, or `AcceptV2NoPeer + "," + acceptDiscoveryFormats`,
or `acceptDiscoveryFormats = AcceptV2 + "," + AcceptV1` (line 67), so
the server has to honour quality-style fallback when both are listed.

`kubectl get` adds two more headers for table / partial-object views
(handled by the `transformRequests` machinery in `get.go:318-329`):

```text
Accept: application/json;as=Table;v=v1;g=meta.k8s.io,application/json
Accept: application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io,application/json
```

Default `User-Agent` is constructed at `rest/config.go:414-431`:

```text
{command}/{semver} ({os}/{arch}) kubernetes/{commit7}
# e.g. kubectl/v1.35.0 (linux/amd64) kubernetes/abc1234
```

`ContentConfig.AcceptContentTypes` is empty by default
(`config.go:318-333`); when unset, client-go falls back to
`Content-Type` (`application/json`) for the `Accept` header as well.

## Expected responses / assertions

- `/api` legacy → `APIVersions{kind, apiVersion:"v1", versions:["v1"], serverAddressByClientCIDRs:[…]}`.
- `/apis` legacy → `APIGroupList{kind, apiVersion:"v1", groups:[APIGroup{name, versions, preferredVersion}…]}`.
- `/apis` aggregated v2 → `APIGroupDiscoveryList{kind:"APIGroupDiscoveryList", apiVersion:"apidiscovery.k8s.io/v2", items:[…]}` where each item carries `versions[].resources[]` with verbs inline. `aggregated_discovery.go:45` then splits this into legacy `*metav1.APIGroupList` + `map[GV]*metav1.APIResourceList` for callers that only know v1.
- `/apis/{group}/{version}` → `APIResourceList{groupVersion, resources:[APIResource{name, singularName, namespaced, kind, verbs, shortNames?, categories?, storageVersionHash?}]}`.
- `/version` → `apimachinery/pkg/version.Info` (types.go:22-40):
  ```json
  {
    "major": "1", "minor": "35",
    "gitVersion": "v1.35.0", "gitCommit": "…", "gitTreeState": "clean",
    "buildDate": "…", "goVersion": "…", "compiler": "…", "platform": "linux/amd64",
    "emulationMajor": "…", "emulationMinor": "…",
    "minCompatibilityMajor": "…", "minCompatibilityMinor": "…"
  }
  ```
  The four `emulation*` / `minCompatibility*` fields are `omitempty`.
- `/openapi/v2` JSON → standard Swagger 2.0 doc. Protobuf accept →
  serialised `openapi_v2.Document` with content-type
  `application/com.github.proto-openapi.spec.v2@v1.0+protobuf` (note `@`).
- `/openapi/v3` → `{ "paths": { "<key>": { "serverRelativeURL": "/openapi/v3/<key>?hash=…" } } }`
  where keys are `api/v1`, `apis/apps/v1`, … (openapi3/root.go:56-59 walks the map).

## Rusternetes-compat checklist

Local worktree: `/home/jones/PhpstormProjects/rusternetes/.claude/worktrees/agent-a9a4812fa542fa2bd`.

- `GET /api` — present, `crates/api-server/src/router.rs:685-686`,
  handler `crates/api-server/src/handlers/discovery.rs:129` (`get_core_api`).
  Aggregated-discovery negotiation at lines 78-120. OK.
- `GET /apis` — present, `router.rs:689-690`, handler
  `discovery.rs:186` (`get_api_groups`). Honours both legacy `APIGroupList`
  and aggregated `APIGroupDiscoveryList`. OK.
- `GET /apis/{group}/` — present, `router.rs:691`, handler
  `discovery.rs:1402` (`get_api_group`). Returns `APIGroup` (single).
  **Gap**: clients calling `/apis/{group}` *without* trailing slash will
  miss this route (axum treats `:group` and `:group/` as distinct);
  worth adding the no-slash alias.
- `GET /apis/{group}/{version}` — **NOT a wildcard route**. Each GV is
  hardcoded at `router.rs:692-787` (20+ explicit `.route()` calls,
  each pointing at a `get_<group>_v<n>_resources` handler in
  `discovery.rs:1958-3218`). CRDs are merged into `/apis` aggregated
  discovery but **no fallback** exists for arbitrary CRD GVs; an
  unrecognised group/version returns 404. K8s upstream handles this
  generically via the CRD handler chain — see
  `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_discovery_controller.go`.
- `GET /version` — present, `router.rs:793`, handler
  `discovery.rs:1468` (`get_version`). JSON tags match `version.Info`
  (`discovery.rs:11-26`) except the four optional `emulation*` /
  `minCompatibility*` fields are not represented at all — harmless
  while we report `omitempty`-equivalent values, but worth noting for
  v1.35-strict clients.
- `GET /openapi/v2` — present, `router.rs:795` (plus alias
  `/swagger.json` at line 801), handler
  `crates/api-server/src/handlers/openapi.rs:330` (`get_swagger_spec`).
  Protobuf branch at lines 382-419. **Gap (matches PR #682
  divergence note)**: rusternetes emits
  `application/com.github.proto-openapi.spec.v2.v1.0+protobuf`
  (dotted form, lines 399 & 416) while client-go's constant uses
  `@v1.0` (`discovery_client.go:55`). Strict accept matching on the
  client will reject the dotted form; protobuf clients fall back to
  JSON. Either rewrite the content-type to use `@` (preferred — matches
  upstream) or document the rusternetes-only divergence.
- `GET /openapi/v3` — present, `router.rs:796`, handler
  `openapi.rs:36` (`get_openapi_spec`). Returns the root paths map
  (hardcoded list at lines 43-67 + dynamic CRD paths at lines 70-96).
  **Mismatch**: the JSON key is `serverRelativeURL` (line 41) which is
  the field name client-go expects (openapi3/root.go example at line
  56), but rusternetes omits the `?hash=…` query-string that upstream
  uses for cache busting. Clients still work; informer caches just
  refetch every poll.
- `GET /openapi/v3/*path` — present, `router.rs:797-798` →
  `openapi.rs:111` (`get_openapi_spec_path`). OK.
- `GET /api/v1/namespaces/{ns}/{resource}` — covered per-resource;
  namespaces list lives at `router.rs:817`. The list/get/watch
  contract is exercised by every conformance test; `?watch=true`,
  `?limit=`, `?continue=` semantics depend on individual handlers,
  not on this discovery surface.
- `kubectl cluster-info` proxy paths
  (`/api/v1/namespaces/kube-system/services/{name}/proxy`) — depend on
  the service-proxy handler (out of scope for this catalog; see
  `crates/api-server/src/handlers/service.rs`).
- `Accept: */*` and quality-value fallback — handled in
  `aggregated_discovery_version()` at `discovery.rs:78-120` (q-value
  parsing at lines 90-110). Matches client-go expectations.
- Default `User-Agent` parsing — rusternetes does not branch on
  User-Agent; harmless. No checklist item.
