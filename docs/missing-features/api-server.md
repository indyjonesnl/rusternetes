# Missing Features — api-server

Comparison of Rusternetes `crates/api-server` against upstream
`cmd/kube-apiserver` + `staging/src/k8s.io/apiserver` at
`github.com/kubernetes/kubernetes`.

## Scope

`kube-apiserver` is the Kubernetes control plane's stateful HTTP front
end. It terminates client TLS, authenticates requests (x509 client
certs, bearer tokens, service account JWTs, OIDC, webhook token
review), authorizes them (RBAC + ABAC + Node + webhook authorizers),
runs admission chains (mutating webhooks + built-in admission plugins
+ validating webhooks + ValidatingAdmissionPolicy CEL), defaults +
converts + validates objects against registered schemes, persists them
to etcd through an envelope-encrypted KV layer with per-resource
versioning, serves watch/list/get/patch/apply/delete (including dry
run, server-side apply, foreground/background/orphan cascade), exposes
discovery + OpenAPI v2/v3, aggregates third-party `APIService`
backends, runs API priority & fairness (APF) for per-flow concurrency
shaping, ships audit events through staged log/webhook backends, and
gracefully drains in-flight requests on shutdown. Implemented across
~17 top-level subdirectories in `pkg/`: admission, apis, audit,
authentication, authorization, cel, endpoints, features, quota,
reconcilers, registry, server, sharding, storage, storageversion,
validation, warning.

## Current Rusternetes state

`crates/api-server` is an Axum REST API wired through ~80 per-resource
handler files in `crates/api-server/src/handlers/` and a 2.4 KLoC
route table at `crates/api-server/src/router.rs:1`. Authentication is
in `crates/api-server/src/middleware.rs:66` (service-account JWT then
anonymous fallback). TLS termination uses `axum-server` rustls
(`crates/api-server/src/main.rs:29`). Admission webhooks (mutating +
validating, FailurePolicy, namespaceSelector, objectSelector,
matchConditions CEL, reinvocationPolicy=IfNeeded, SideEffects
dry-run gating, structural pruning of CR after mutation) live in
`crates/api-server/src/admission_webhook.rs:1`. Built-in admission
(ResourceQuota / LimitRange / ServiceAccount injection) is at
`crates/api-server/src/admission.rs:1`. ValidatingAdmissionPolicy with
CEL is in `crates/api-server/src/handlers/validating_admission_policy.rs:1`
and `crates/api-server/src/handlers/cel_validation.rs:1`. APF types
are wired (`crates/api-server/src/router.rs:1722`) and a per-priority
semaphore exists at `crates/api-server/src/flow_control.rs:18`, but
the FlowSchema → request matching path is not enforced on the request
middleware. Watch cache multiplexer is
`crates/api-server/src/watch_cache.rs:1` with bookmark events in
`crates/api-server/src/handlers/watch.rs:289`. OpenAPI v2 (with
gnostic protobuf for v2) and v3 group/version index live at
`crates/api-server/src/openapi.rs:1`, `crates/api-server/src/gnostic.rs:1`,
`crates/api-server/src/handlers/openapi.rs:31`. Aggregated discovery
v2 / v2beta1 negotiation is at
`crates/api-server/src/handlers/discovery.rs:78`. SPDY exec / attach /
port-forward upgrades are in `crates/api-server/src/spdy.rs:1`,
`crates/api-server/src/spdy_handlers.rs:1`,
`crates/api-server/src/spdy_upgrade.rs:1`. Server-side apply is in
`crates/api-server/src/handlers/apply.rs:1`; patch (Strategic / JSON
/ Merge) is in `crates/api-server/src/patch.rs:1`. Finalizers
(foreground / background / orphan) and dry-run are in
`crates/api-server/src/handlers/finalizers.rs:1` and
`crates/api-server/src/handlers/dryrun.rs:1`. Conversion webhooks for
CRDs are wired in `crates/api-server/src/conversion.rs:1`. APIService
aggregator forwarder is at `crates/api-server/src/router.rs:46` and
`crates/api-server/src/handlers/generic.rs`. List chunking
(`continue=` / `limit=`) is wired in resource handlers, e.g.
`crates/api-server/src/handlers/podtemplate.rs:247`. An
`EncryptionConfig` type exists at `crates/common/src/encryption.rs:34`
but is **not referenced** by the api-server or storage crates. There
is no audit pipeline, no feature gate registry, no impersonation
middleware, no warning header subsystem, no graceful-shutdown drain,
and no APF request gating.

## Parity matrix

| Feature | Upstream | Rusternetes | Status | Notes |
|---|---|---|---|---|
| TLS server (rustls) | `pkg/server/secure_serving.go` | `crates/api-server/src/main.rs:29` | full | rustls via axum-server |
| mTLS / client cert auth | `pkg/authentication/request/x509` | partial | partial | `--tls-client-ca-file` flag exists (`main.rs:102`); user extraction from cert not wired into `middleware.rs` |
| Service-account JWT auth | `pkg/authentication/serviceaccount` | `crates/api-server/src/middleware.rs:66` | full | SA token validation + anonymous fallback |
| OIDC token auth | `pkg/authentication/token/oidc` | none | missing | no OIDC provider |
| Webhook token review | `pkg/authentication/token/webhook` | none | missing | local TokenReview handler only |
| Bootstrap token auth | `pkg/authentication/token/bootstrap` | none | missing | |
| Anonymous auth toggle | `pkg/server/options/authentication.go` | always-on | partial | hard-coded fallback, no `--anonymous-auth=false` |
| Impersonation (`Impersonate-User`) | `pkg/endpoints/filters/impersonation.go` | none | missing | required for `kubectl --as`, `--as-group` |
| RBAC authorizer | `plugin/pkg/auth/authorizer/rbac` | `crates/api-server/src/middleware.rs`, `crates/api-server/src/handlers/rbac.rs:1` | full | |
| Node authorizer | `plugin/pkg/auth/authorizer/node` | none | missing | kubelet uses RBAC instead |
| ABAC authorizer | `pkg/auth/authorizer/abac` | none | missing | (rarely used upstream) |
| Webhook authorizer | `staging/src/k8s.io/apiserver/plugin/pkg/authorizer/webhook` | none | missing | |
| Admission: mutating webhooks | `pkg/admission/plugin/webhook/mutating` | `crates/api-server/src/admission_webhook.rs:1020` | full | incl. reinvocation, matchConditions, structural pruning |
| Admission: validating webhooks | `pkg/admission/plugin/webhook/validating` | `crates/api-server/src/admission_webhook.rs:595` | full | |
| ValidatingAdmissionPolicy (CEL) | `pkg/admission/plugin/policy/validating` | `crates/api-server/src/handlers/validating_admission_policy.rs:1` | partial | CEL eval present; ParamRef binding, audit-annotations, message-expression budgets need verification |
| MutatingAdmissionPolicy (CEL, beta) | KEP-3962 | none | missing | new in 1.32, gates `MutatingAdmissionPolicy` |
| Built-in admission: ResourceQuota | `plugin/pkg/admission/resourcequota` | `crates/api-server/src/admission.rs:158` | full | pod scope matching at `:53` |
| Built-in admission: LimitRange | `plugin/pkg/admission/limitranger` | `crates/api-server/src/admission.rs:496` | full | |
| Built-in admission: ServiceAccount | `plugin/pkg/admission/serviceaccount` | `crates/api-server/src/admission.rs` | partial | SA token mount, no projected SA enforcement gate |
| Built-in admission: NamespaceLifecycle | `plugin/pkg/admission/namespace/lifecycle` | partial | partial | `Terminating` phase set at `crates/api-server/src/handlers/namespace.rs:360`; per-request rejection on terminating ns not enforced globally |
| Built-in admission: PodSecurity | `staging/src/k8s.io/pod-security-admission` | none | missing | PSA enforce/audit/warn labels |
| Built-in admission: DefaultStorageClass | `plugin/pkg/admission/storage/storageclass/setdefault` | none | missing | |
| Built-in admission: DefaultTolerationSeconds | `plugin/pkg/admission/defaulttolerationseconds` | none | missing | |
| Built-in admission: EventRateLimit | `plugin/pkg/admission/eventratelimit` | none | missing | |
| `--admission-control` / plugin ordering | `pkg/admission/config` | none | missing | order is hard-coded in Rust call sites |
| API Priority & Fairness types | `pkg/apis/flowcontrol` | `crates/api-server/src/router.rs:1722` | full | CRUD only |
| API Priority & Fairness gating | `pkg/util/flowcontrol` | `crates/api-server/src/flow_control.rs:18` | stubbed | per-priority `Semaphore` exists; no FlowSchema match in middleware, no PF response headers |
| Audit logging | `pkg/audit` | none | **missing** | no AuditEvent, no AuditPolicy, no log/webhook sinks |
| Encryption at rest (KMS v1/v2) | `pkg/storage/value/encrypt` | `crates/common/src/encryption.rs:34` (unused) | stubbed | type exists, not wired into storage |
| Feature gates | `component-base/featuregate` | none | missing | no central gate registry |
| Discovery v1 (`/api`, `/apis`) | `pkg/endpoints/discovery` | `crates/api-server/src/handlers/discovery.rs` | full | |
| Aggregated discovery v2 | `apidiscovery.k8s.io/v2` | `crates/api-server/src/handlers/discovery.rs:78` | full | content-type negotiation works |
| OpenAPI v2 swagger | `pkg/endpoints/openapi` | `crates/api-server/src/router.rs:795` | partial | JSON served; gnostic v2 protobuf at `crates/api-server/src/gnostic.rs:15` |
| OpenAPI v3 group index | `pkg/endpoints/openapi/v3` | `crates/api-server/src/handlers/openapi.rs:31` | full | |
| CRD OpenAPI v3 publishing | `apiextensions-apiserver/pkg/apiserver` | partial | partial | gap is the largest single conformance miss (9 failures) |
| APIService aggregation | `pkg/aggregator` | `crates/api-server/src/router.rs:46` | partial | HTTP forward only; no `availableCondition` controller, no TLS dial to backend service IP |
| Conversion webhooks (CRD) | `apiextensions-apiserver/pkg/apiserver/conversion` | `crates/api-server/src/conversion.rs:91` | full | |
| Structural schema pruning | `apiextensions-apiserver/pkg/apiserver/schema/pruning` | `crates/api-server/src/admission_webhook.rs:1582` + handlers/custom_resource | full | |
| Server-side apply | `pkg/util/managedfields` | `crates/api-server/src/handlers/apply.rs:106` | partial | apply works; managedFields ownership conflict resolution and `force=true` semantics need audit |
| Patch: Strategic / JSON / Merge | `pkg/api/patch` | `crates/api-server/src/patch.rs:1` | full | |
| Dry-run | `pkg/endpoints/handlers` | `crates/api-server/src/handlers/dryrun.rs:1` | full | |
| Finalizers + cascade modes | `pkg/registry/generic` | `crates/api-server/src/handlers/finalizers.rs:1` | full | foreground/background/orphan |
| Watch + bookmarks | `pkg/storage/cacher` | `crates/api-server/src/watch_cache.rs:1`, `crates/api-server/src/handlers/watch.rs:160` | full | 500-event ring replay |
| WatchList (KEP-3157) consistent reads | `pkg/storage/cacher` | partial | partial | `sendInitialEvents` query parsed (`crates/api-server/src/handlers/daemonset.rs:237`) but `resourceVersionMatch=NotOlderThan` semantics not fully implemented |
| Streaming chunked list pagination | `pkg/storage/etcd3` | e.g. `crates/api-server/src/handlers/podtemplate.rs:247` | full | continueToken+limit |
| Table response (kubectl get) | `pkg/registry/.../table` | `crates/api-server/src/handlers/table.rs:1` | partial | generic table at `:196`; per-resource printer columns are minimal |
| Protobuf wire format | `pkg/runtime/serializer/protobuf` | `crates/api-server/src/protobuf.rs:1` | partial | decoder for known types; not all groups have schemas |
| Warning headers (`Warning: 299 ...`) | `pkg/warning` | none | missing | deprecation announcement vehicle |
| Request deadline / timeout | `pkg/endpoints/filters/timeout.go` | none | missing | no per-request timeout enforcement |
| Max request body size | `pkg/server/config.go` | none | missing | no explicit cap |
| `--max-requests-inflight` / mutating | `pkg/server/filters/maxinflight.go` | none | missing | unbounded |
| Request ID + trace correlation | `pkg/endpoints/handlers/responsewriters` | partial | partial | tracing emits spans but no `Audit-ID` UUID header |
| OpenTelemetry tracing export | `pkg/server/options/tracing.go` | none | missing | `tracing` crate is local-only; no OTLP/Jaeger exporter |
| Graceful shutdown drain | `pkg/server/genericapiserver.go` | none | missing | no `--shutdown-delay-duration`, no readyz=false during drain |
| Leader election (HA) | `tools/leaderelection` | none | missing | no multi-replica api-server coordination via Lease |
| `SelfSubjectReview` | `pkg/registry/authentication/selfsubjectreview` | `crates/api-server/src/handlers/authentication.rs` | full | |
| `TokenRequest` (bound SA tokens) | `pkg/registry/authentication/tokenrequest` | `crates/api-server/src/handlers/authentication.rs:115` | partial | request decoded; audience binding + expiration enforcement need audit |
| SubjectAccessReview, SelfSubjectAccessReview | `pkg/registry/authorization` | `crates/api-server/src/handlers/authorization.rs` | full | |
| CertificateSigningRequests + approval | `pkg/registry/certificates` | `crates/api-server/src/handlers/certificates.rs` | full | |
| `--audit-policy-file` / `--audit-log-path` flags | `cmd/kube-apiserver/app/options` | none | missing | matches audit pipeline gap above |
| SNI multi-cert serving | `pkg/server/dynamiccertificates` | none | missing | single cert pair only |
| Dual-stack IPv4/IPv6 services | `pkg/apis/core/validation` | `crates/api-server/src/handlers/service.rs:132` | partial | defaults to `IPv4`+`SingleStack`; dual-stack code paths exist but the controllers have to back this end-to-end |
| Egress selector (Konnectivity) | `pkg/server/egressselector` | none | missing | needed for private control planes |

## Missing features

### Audit logging pipeline (audit.k8s.io)

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/audit/`, KEP-1191 (dynamic
  audit, deprecated) and the stable static config via `--audit-policy-file`
  + `--audit-log-path` / `--audit-webhook-config-file`
- **Why it matters**: required by CIS Benchmark, PCI-DSS, FedRAMP, and most
  conformance audits. Without it, security incidents cannot be replayed and
  the cluster fails most compliance scans. Several CNCF projects (Falco,
  kubescape) ingest audit events as their primary signal.
- **Effort hint**: L — needs request-level interception at four stages
  (RequestReceived, ResponseStarted, ResponseComplete, Panic), an
  AuditEvent type, AuditPolicy matcher (level + stage + resources +
  users), and at least the log+webhook backends.

### Encryption at rest wiring

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/storage/value/encrypt`,
  KEP-3299 (KMS v2)
- **Why it matters**: `crates/common/src/encryption.rs` defines a config
  type but no call site exists in `crates/api-server` or `crates/storage`.
  Today Secrets land in etcd / SQLite / Redis as plaintext. That breaks
  CIS 1.2.34 and means any backup of the data dir leaks every Secret
  cluster-wide.
- **Effort hint**: M — wire the existing `EncryptionConfig` into the
  `Storage` trait at the value-codec layer. KMS v1+v2 envelope drivers are
  the extra work beyond aescbc/aesgcm.

### API Priority & Fairness request gating

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/util/flowcontrol`,
  KEP-1040
- **Why it matters**: FlowSchema/PriorityLevelConfiguration CRUD works
  (`crates/api-server/src/router.rs:1722`) and a semaphore exists at
  `crates/api-server/src/flow_control.rs:18`, but nothing in the request
  middleware looks up the matching FlowSchema for each request, charges
  seats, or queues. A misbehaving client can saturate the api-server.
- **Effort hint**: L — adds a middleware layer, fair-queuing scheduler,
  seat estimation (lists cost N seats), and the
  `X-Kubernetes-PF-FlowSchema-UID` / `PF-PriorityLevel-UID` response
  headers.

### Impersonation

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/endpoints/filters/impersonation.go`
- **Why it matters**: `kubectl --as`, `--as-group`, `--as-uid` is how
  operators sudo through their own credentials. Without it, dashboard-style
  tooling and `kubectl auth can-i --as` are broken. RBAC has a verb
  (`impersonate`) that today nothing checks against because the header
  pipeline doesn't honor `Impersonate-User`/`Impersonate-Group`/`Impersonate-Extra-*`.
- **Effort hint**: S — single middleware that swaps `UserInfo` after
  authorizing the `impersonate` verb on the target user/group/uid.

### Feature gates registry

- **Upstream**: `staging/src/k8s.io/component-base/featuregate/`
- **Why it matters**: every alpha/beta feature in upstream is gated.
  Today Rusternetes ships beta features (CEL admission policies,
  WatchList, MutatingAdmissionPolicy semantics) with no way to disable
  them per-cluster. Operators cannot opt out, and conformance runners
  cannot align their feature matrix to ours.
- **Effort hint**: S — central enum + `--feature-gates=Foo=true,Bar=false`
  parsing + a global `Gates` handle queried at feature decision points.

### Warning headers (`Warning: 299 - "..."`)

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/warning/`
- **Why it matters**: every deprecation of an API field in upstream uses
  the RFC-7234 `Warning` response header so `kubectl` can print
  `Warning:` lines. We have no mechanism to do this, so deprecation
  visibility is invisible to clients (e.g. CronJob `spec.timeZone`
  deprecations, removed-in-future-version fields).
- **Effort hint**: S — add a thread-local/request-scoped warning sink
  that the response writer drains into `Warning:` headers before flushing.

### Request timeout / deadline propagation

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/endpoints/filters/timeout.go`
- **Why it matters**: upstream caps every request at 60s by default (or
  the client's `?timeoutSeconds=`) and returns a synthetic `504` with a
  `Timeout` Status. Today a slow webhook can wedge an axum worker
  indefinitely. Watch handler does parse `timeoutSeconds`
  (`crates/api-server/src/handlers/watch.rs:137`) but non-watch verbs do
  not.
- **Effort hint**: S — tower timeout layer + `tokio::select!` that
  returns `metav1.Status` on cancellation.

### `--max-requests-inflight` / `--max-mutating-requests-inflight`

- **Upstream**: `pkg/server/filters/maxinflight.go`
- **Why it matters**: hard cap on concurrent requests before APF kicks
  in. Without it, the only protection against a thundering herd is
  whatever the OS scheduler gives us.
- **Effort hint**: S — two semaphores keyed off the HTTP verb (read vs
  mutating).

### Graceful shutdown drain

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go`
  (`ShutdownDelayDuration`, `ShutdownSendRetryAfter`)
- **Why it matters**: in a rolling restart, the old api-server must
  start failing `/readyz` while still serving in-flight requests, so the
  load balancer drains it before the listener stops. Today we just bind
  and serve until SIGTERM with no drain phase, so kubectl errors during
  upgrades.
- **Effort hint**: M — needs `axum_server` with a graceful shutdown
  channel, plus a readiness probe that switches state on SIGTERM, plus
  optional 503 + `Retry-After` after the delay.

### Audit-ID + structured request tracing

- **Upstream**: `pkg/endpoints/handlers/responsewriters/writers.go`
  sets `Audit-ID` UUID per request.
- **Why it matters**: every conformance log and bug report keys off
  `Audit-ID`; without one, debugging cross-component issues is much
  harder, and audit events (when added) cannot be correlated to the
  request response a client saw.
- **Effort hint**: S — generate a UUID in middleware, expose it in the
  `Audit-ID` response header, and stash it in the tracing span.

### MutatingAdmissionPolicy (CEL)

- **Upstream**: KEP-3962 (beta in 1.32, gates `MutatingAdmissionPolicy`)
- **Why it matters**: the in-tree alternative to mutating webhooks, no
  network hop. Communities are moving policy logic into CEL because
  it's safer and faster than a webhook fleet. We already have
  ValidatingAdmissionPolicy CEL, so adding the mutating variant is the
  next step.
- **Effort hint**: M — share the CEL evaluator with the validating
  side; difference is producing a patched object instead of a boolean.

### OIDC / webhook authentication

- **Upstream**: `pkg/authentication/token/oidc`,
  `pkg/authentication/token/webhook`
- **Why it matters**: every managed Kubernetes (EKS/GKE/AKS) and most
  on-prem clusters auth users through OIDC. Rusternetes today only
  accepts SA tokens + anonymous, so it cannot be the API for a real
  multi-tenant cluster.
- **Effort hint**: M — JWKS fetcher, issuer trust list, claim mapping
  for username/groups; webhook variant is a POST-to-URL with
  TokenReview body.

### Aggregator availability controller

- **Upstream**: `staging/src/k8s.io/kube-aggregator/pkg/controllers/status`
- **Why it matters**: today, requests to a registered `APIService` are
  forwarded blindly (`crates/api-server/src/router.rs:46`). Upstream
  maintains an `APIService.status.conditions[Available]` condition by
  health-probing the backend Service; without it, kubectl spins instead
  of seeing `APIService not available`, and discovery lists stale groups.
- **Effort hint**: M — small controller that probes each APIService's
  Service ClusterIP + `/healthz` and patches `.status.conditions`.

### Egress selector / Konnectivity

- **Upstream**: `staging/src/k8s.io/apiserver/pkg/server/egressselector`
- **Why it matters**: needed when api-server lives on the public
  internet but kubelets/webhooks are private. Production GKE/EKS
  control planes use this. Without it, webhooks have to be publicly
  reachable.
- **Effort hint**: XL — separate Konnectivity dialer + TCP tunnel
  protocol implementation.

### SNI multi-certificate serving

- **Upstream**: `pkg/server/dynamiccertificates`
- **Why it matters**: lets one api-server present `kubernetes.default.svc`
  to in-cluster clients and a public hostname to external ones with
  different CAs. Today we have one rustls cert (`main.rs:343`).
- **Effort hint**: M — `rustls::server::ResolvesServerCertUsingSni`
  and dynamic cert hot-reload.

## Partial / stubbed

These features exist but are incomplete:

- **APF gating** — `crates/api-server/src/flow_control.rs:18` allocates
  a `Semaphore` per PriorityLevelConfiguration but the request-side
  match-and-acquire is never invoked from middleware. CRUD only.
- **Encryption at rest** — `crates/common/src/encryption.rs:34` defines
  the config but no caller in `crates/api-server` or `crates/storage`
  references it; secrets land plaintext.
- **mTLS client cert auth** — `crates/api-server/src/main.rs:102`
  parses `--tls-client-ca-file` but the verified subject is not surfaced
  into `UserInfo` in `crates/api-server/src/middleware.rs:66`.
- **WatchList / KEP-3157** — `sendInitialEvents` is parsed in several
  handlers (`crates/api-server/src/handlers/daemonset.rs:237`,
  `crates/api-server/src/handlers/cronjob.rs:207`,
  `crates/api-server/src/handlers/ipaddress.rs:189`) but
  `resourceVersionMatch=NotOlderThan` consistency semantics in the watch
  cache are not verified end-to-end.
- **CRD OpenAPI v3 publishing** — biggest single conformance gap (9
  failures, see `docs/CONFORMANCE.md`). Group index works at
  `crates/api-server/src/handlers/openapi.rs:41` but per-CRD schema
  conversion under `/openapi/v3/apis/<group>/<version>` is incomplete.
- **Table responses** — generic table at
  `crates/api-server/src/handlers/table.rs:196` only ships minimal
  columns; upstream defines per-resource `TableConvertor` printers.
- **Server-side apply** — `crates/api-server/src/handlers/apply.rs:106`
  handles the patch type but `managedFields` ownership conflict
  resolution and `force=true` re-acquisition semantics need an audit
  against upstream's `fieldpath` package.
- **APIService aggregation** — request forwarder works
  (`crates/api-server/src/router.rs:46`) but there is no
  `availableCondition` controller and the backend dial does not
  re-verify the APIService's `caBundle`.
- **NamespaceLifecycle admission** — terminating phase is set
  (`crates/api-server/src/handlers/namespace.rs:360`) but no global
  admission gate rejects writes to terminating namespaces; controllers
  rely on per-handler logic.
- **TokenRequest** — `crates/api-server/src/handlers/authentication.rs:115`
  decodes the request but bound-token features (audience binding, pod
  binding, expirationSeconds enforcement) need audit.
- **Protobuf wire format** — `crates/api-server/src/protobuf.rs:1`
  decodes known message types; not all wired API groups have schemas.

## Known in-code TODOs

- `crates/api-server/src/handlers/pod_subresources.rs:1111` — pod
  log/exec selectors do not yet support `matchExpressions` (only
  `matchLabels`).

## References

Upstream source:

- `https://github.com/kubernetes/kubernetes/tree/master/cmd/kube-apiserver`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver/pkg/audit`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver/pkg/storage/value/encrypt`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver/pkg/util/flowcontrol`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver/pkg/endpoints/filters`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/kube-aggregator`
- `https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiextensions-apiserver`

KEPs:

- KEP-1040 — API Priority and Fairness
- KEP-1191 — Dynamic Audit (deprecated, but the static audit pipeline
  it grew out of is still the surface)
- KEP-3157 — WatchList streaming consistent reads
- KEP-3299 — KMS v2 encryption providers
- KEP-3488 — CEL ValidatingAdmissionPolicy (already implemented here)
- KEP-3962 — MutatingAdmissionPolicy (CEL)
- KEP-4222 — Bound service account token improvements

Concepts docs:

- `https://kubernetes.io/docs/concepts/cluster-administration/flow-control/`
- `https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/`
- `https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/`
- `https://kubernetes.io/docs/reference/using-api/api-concepts/#streaming-lists`
- `https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/`

Internal:

- `docs/CONFORMANCE.md` — current conformance results showing the
  CRD-OpenAPI / session-affinity / webhook-edge-case clusters that map
  to the partials above.
- `docs/ADVANCED_API_FEATURES.md`
- `docs/WEBHOOK_INTEGRATION.md`
- `docs/api-gap-analysis.md`
