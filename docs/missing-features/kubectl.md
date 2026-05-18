# Missing Features — kubectl

## Scope

Compares `crates/kubectl` (the Rusternetes kubectl reimplementation) against the
upstream Kubernetes CLI in `cmd/kubectl` + `staging/src/k8s.io/kubectl/pkg`.
Every one of the 48 upstream top-level subcommands is wired through `clap` in
`crates/kubectl/src/main.rs`, so parity gaps are at the **flag / behavior /
output-format / wire-protocol** level — not at the "command exists" level. The
goal of this document is to enumerate those finer-grained gaps so they can be
prioritized.

Authentication-provider plumbing (exec credential plugins / cloud
auth-providers), output formatting (`go-template`, `custom-columns`,
`jsonpath-as-json`), apply-time pruning, and the SPDY fallback transport for
streaming endpoints are the largest cross-cutting gaps.

## Current Rusternetes state

Workspace crate: `crates/kubectl` — `~23,791` total source lines across
`src/commands/` (excl. tests). Entry point `crates/kubectl/src/main.rs:1` defines
the `clap` `Cli` struct and dispatches to one module per subcommand.

Subcommands present (cite path : module):

- `crates/kubectl/src/commands/annotate.rs:1` annotate
- `crates/kubectl/src/commands/api_resources.rs:1` api-resources
- `crates/kubectl/src/commands/api_versions.rs:1` api-versions
- `crates/kubectl/src/commands/apply.rs:1` apply (+ view/set/edit-last-applied)
- `crates/kubectl/src/commands/attach.rs:1` attach (WebSocket only)
- `crates/kubectl/src/commands/auth.rs:1` auth (can-i, whoami, reconcile)
- `crates/kubectl/src/commands/autoscale.rs:1` autoscale
- `crates/kubectl/src/commands/certificate.rs:1` certificate approve/deny
- `crates/kubectl/src/commands/cluster_info.rs:1` cluster-info
- `crates/kubectl/src/commands/completion.rs:1` completion (static script)
- `crates/kubectl/src/commands/config.rs:1` config (kubeconfig mgmt)
- `crates/kubectl/src/commands/cp.rs:1` cp (WebSocket-driven exec tar)
- `crates/kubectl/src/commands/create.rs:1` create + 17 sub-generators
- `crates/kubectl/src/commands/debug.rs:1` debug (pod ephemeralContainers, node)
- `crates/kubectl/src/commands/delete.rs:1` delete
- `crates/kubectl/src/commands/describe.rs:1` describe
- `crates/kubectl/src/commands/diff.rs:1` diff
- `crates/kubectl/src/commands/drain.rs:1` drain / cordon / uncordon
- `crates/kubectl/src/commands/edit.rs:1` edit
- `crates/kubectl/src/commands/events.rs:1` events
- `crates/kubectl/src/commands/exec.rs:1` exec (WebSocket only)
- `crates/kubectl/src/commands/explain.rs:1` explain
- `crates/kubectl/src/commands/expose.rs:1` expose
- `crates/kubectl/src/commands/get.rs:1` get
- `crates/kubectl/src/commands/help.rs:1` help
- `crates/kubectl/src/commands/kuberc.rs:1` kuberc (alpha config wrapper)
- `crates/kubectl/src/commands/kustomize.rs:1` kustomize (standalone only)
- `crates/kubectl/src/commands/label.rs:1` label
- `crates/kubectl/src/commands/logs.rs:1` logs
- `crates/kubectl/src/commands/options.rs:1` options
- `crates/kubectl/src/commands/patch.rs:1` patch (json / merge / strategic)
- `crates/kubectl/src/commands/plugin.rs:1` plugin list
- `crates/kubectl/src/commands/port_forward.rs:1` port-forward (WebSocket only)
- `crates/kubectl/src/commands/proxy.rs:1` proxy
- `crates/kubectl/src/commands/replace.rs:1` replace
- `crates/kubectl/src/commands/rollout.rs:1` rollout (status/history/undo/restart/pause/resume)
- `crates/kubectl/src/commands/run.rs:1` run
- `crates/kubectl/src/commands/scale.rs:1` scale
- `crates/kubectl/src/commands/set.rs:1` set (image / env / resources / selector / serviceaccount / subject)
- `crates/kubectl/src/commands/taint.rs:1` taint
- `crates/kubectl/src/commands/top.rs:1` top (node, pod)
- `crates/kubectl/src/commands/version.rs:1` version
- `crates/kubectl/src/commands/wait.rs:1` wait

No subcommand is `todo!()` / `unimplemented!()` — every dispatch reaches an
implementation. Six call sites in `annotate.rs:207`, `autoscale.rs:335`,
`label.rs:183`, `patch.rs:82/93/104/167` are defensive `panic!("unexpected"|"invalid")`
guards on already-narrowed enums (clippy-style "unreachable" assertions, not
stubs).

Kubeconfig parsing is in `crates/kubectl/src/kubeconfig.rs:1` — fields for
`auth_provider` (line 80) and `exec` (line 82) **are deserialized but never
invoked**: the resulting `User` is read only for `client_certificate_data`,
`client_key_data`, and `token` in `client.rs:1`, and the HTTP client is built
with `reqwest::Client::new()` / `danger_accept_invalid_certs(true)` —
**no mTLS identity, no exec-plugin call-out, no auth-provider token refresh**.

## Parity matrix

| Capability | Upstream `kubectl` | Rusternetes `kubectl` | Notes |
|---|---|---|---|
| `get` table output | yes (server-side `Table` via discovery) | partial; per-kind hard-coded printers in `get.rs:1252+` | No server-side `Table` accept header; renderer is per-resource Rust code. |
| `get -o wide` | yes | partial — table only for known kinds | Falls back to pretty-printed JSON for the long tail (`get.rs:80`). |
| `get -o json/yaml/name` | yes | yes | `OutputFormat` enum at `get.rs:22`. |
| `get -o jsonpath=` | yes (full Go template) | partial — hand-rolled (`get.rs:90`) | No filter expressions `[?(@.x=="y")]`, no range, no string concat across `{}{}{}`; filter returns empty string by design (`get.rs:135-138`). |
| `get -o jsonpath-as-json=` | yes | missing | Not in `OutputFormat::from_str`. |
| `get -o go-template=` / `go-template-file=` | yes | missing | No Go-template engine; would need a Rust template impl. |
| `get -o custom-columns=` / `custom-columns-file=` | yes | missing | Not parsed. |
| `get -o template=` (alias) | yes | missing | — |
| `get --watch` | yes | yes — chunked-stream watch (`get.rs:259`) | Per-kind code path. |
| `get --watch-only` | yes | missing flag | No initial-list-skip option. |
| `get --ignore-not-found` | yes | missing flag | Not a flag; `not found` always errors. |
| `get --chunk-size` | yes (default 500) | missing flag | No `?limit=&continue=` pagination on list calls. |
| `get --show-kind` | yes | missing flag | — |
| `get --show-managed-fields` | yes | missing flag | Managed fields are emitted unconditionally in JSON/YAML. |
| `get --show-labels` | yes | yes (`get.rs:84`) | Only honoured by known-kind printers. |
| `get --field-selector` | yes | yes (CLI flag at `main.rs:75`) | Forwarded as `?fieldSelector=`. |
| `get --label-selector` / `-l` | yes | yes (`main.rs:71`) | — |
| `get --all-namespaces` / `-A` | yes | yes (`main.rs:59`) | — |
| `get --sort-by=` | yes | partial (`get.rs:220`) | Simple JSONPath; no filter or array indexing. |
| `get --raw` | yes (any path, no decode) | missing | Not exposed. |
| `apply --server-side` | yes | partial — flag exists (`main.rs:174`) | Sends `?fieldManager=` + `Content-Type: application/apply-patch+yaml`; **does not** retry on conflict semantics or surface managed-field merge errors with hints. |
| `apply --field-manager` | yes | yes (default `kubectl-client-side-apply`) | — |
| `apply --force-conflicts` | yes | partial; flag named `--force` (`main.rs:178`) | Forwards as `?force=true`. |
| `apply --prune` / `--prune-allowlist` / `--prune-whitelist` | yes | missing | No prune logic anywhere in `apply.rs:1`. Grep for `prune` / `allowlist` returns 0 hits. |
| `apply --openapi-patch` | yes | missing | Strategic-merge fallback uses local knowledge only. |
| `apply --dry-run=client|server|none` | yes | partial | `--dry-run` accepted as `String`, threaded into `?dryRun=All` for server; no client-side dry-run rendering. |
| `apply --validate=true\|false\|strict\|warn\|ignore` | yes | partial (`main.rs:194`) | Flag accepted; no client-side schema validation. |
| `apply --recursive` / `-R` | yes | yes | Uses `walkdir`. |
| `apply` last-applied annotation | yes | yes (`apply.rs:300+`, 1340+) | Including `view/set/edit-last-applied`. |
| `apply -k` (kustomize) | yes | missing | `Apply` does not accept `-k`; standalone `kubectl kustomize` exists but isn't wired through `apply` / `delete`. |
| `delete -k` | yes | missing | Same. |
| `delete --cascade={background,foreground,orphan}` | yes | yes (`main.rs:143`) | — |
| `delete --grace-period`, `--force`, `--wait`, `--dry-run` | yes | yes | — |
| `delete --field-selector` | yes | missing flag | Only `-l` selector. |
| `delete -A` / all namespaces | yes | missing flag | — |
| `diff --server-side` | yes | missing | Renderer in `diff.rs:1` is local-yaml-vs-server-yaml line diff. |
| `diff` honours `KUBECTL_EXTERNAL_DIFF` | yes | missing | No env-var lookup in `diff.rs:1`. |
| `debug` pod ephemeralContainers | yes | yes (`debug.rs:25+`) | — |
| `debug` node (privileged pod) | yes | yes (`debug.rs:39+`) | — |
| `debug --profile` (general, baseline, restricted, netadmin, sysadmin) | yes | missing | No `--profile` flag. |
| `debug --image-profile` | yes | missing | — |
| `debug --copy-to`, `--share-processes`, `--set-image`, `--keep-*` | yes | missing | No copy-target workflow. |
| `exec` / `attach` WebSocket (KEP-4006) | yes (beta 1.31) | yes — only this (`exec.rs:1`, `attach.rs:1`, `websocket.rs:1`) | — |
| `exec` / `attach` SPDY fallback | yes (default before 1.31) | missing | No SPDY/4 client; if API server falls back to SPDY (older clusters / proxies that strip WS upgrade) the command fails. |
| `port-forward` WebSocket | yes | yes (`port_forward.rs:29`) | — |
| `port-forward` SPDY fallback | yes | missing | Same as above. |
| `port-forward --address` / multi-port | yes | partial | `address` honoured (`main.rs:352`); multiple ports passed as `Vec<String>` but binding loop is single-stream per port. |
| `cp` to/from container | yes (uses exec+tar) | yes (`cp.rs:476+`) | WebSocket exec only. |
| `cp` `--no-preserve` | yes | missing flag | — |
| `cp` `--retries`, `--keep-tail` | yes | missing | — |
| `logs --since`, `--since-time`, `--tail`, `--previous`, `-f`, `--timestamps` | yes | yes (`main.rs:294-312`) | — |
| `logs --all-containers` | yes | missing flag | — |
| `logs --prefix`, `--max-log-requests` | yes | missing flag | — |
| `logs --selector` (multi-pod) | yes | missing | `pod_name` is a required positional. |
| `wait --for=condition=` | yes | yes | `wait.rs:40+`. |
| `wait --for=delete` | yes | yes (`wait.rs:28+`) | — |
| `wait --for=jsonpath=` | yes | missing | Only `condition` / `delete` (`wait.rs:54`). |
| `wait --for=create` | yes | missing | — |
| `wait --all` | yes | missing flag | — |
| `auth can-i` | yes | yes (`auth.rs:50`) | — |
| `auth can-i --list` | yes | missing | Reviews `SelfSubjectAccessReview` only, not `SelfSubjectRulesReview`. |
| `auth can-i --subresource` | yes | missing flag | — |
| `auth whoami` | yes | yes | — |
| `auth reconcile` | yes | yes (`auth.rs`, types.rs:546) | — |
| `rollout status --watch --timeout` | yes | partial | `rollout.rs:1` has no `--watch` / `--timeout` flag on status. |
| `rollout history --revision` | yes | yes (`rollout.rs:28`) | — |
| `rollout undo --to-revision` | yes | yes (`rollout.rs:34`) | — |
| `rollout restart`, `pause`, `resume` | yes | yes | — |
| `set env --from`, `--keys`, `--prefix`, `--containers` | yes | missing flags | `SetCommands::Env` (`types.rs:574-590`) only has `--container` (singular) and `--list`. |
| `set image`, `set resources`, `set selector`, `set serviceaccount`, `set subject` | yes | yes | All wired. |
| `version --output=json/yaml` | yes | yes (`version.rs:42-83`) | — |
| `version --client` | yes | yes | — |
| `version --short` | yes | missing flag | — |
| `events --for kind/name`, `--types`, `--watch`, `--no-headers` | yes | yes (`main.rs:776-794`) | — |
| `events --since` | yes | missing flag | — |
| `kustomize` (standalone) | yes (embeds kustomize) | partial | `kustomize.rs` is a thin wrapper — see "Partial" below. |
| Plugin discovery (`kubectl-*` on PATH) | yes (cobra) | yes (`plugin.rs:12`) | Lists, dedups, warns on overshadow + non-exec. |
| Plugin **invocation** (`kubectl foo` → exec `kubectl-foo`) | yes | **missing** | No `args[0]` fallback to PATH lookup when subcommand is unknown; clap rejects unknown subcommands immediately. |
| Plugin completion (`__complete` shim) | yes (cobra) | missing | — |
| krew integration | yes (`kubectl krew`) | missing | — |
| Resource shortname expansion (`po`→`pods`, `deploy`→`deployments`) | yes (via discovery) | partial — hard-coded in each handler | No live discovery-driven map; each command parses its own subset (e.g. `debug.rs:25`). |
| Generic CRD `get`/`describe`/`edit` via discovery | yes (`cli-runtime` builder + visitor) | partial | `get.rs` handles a fixed set; CRDs go through the `CustomResourceDefinition` branch only — not `kubectl get widgets.example.com`. |
| `--raw` for arbitrary path | yes | missing | — |
| Server-side completion (`__complete`, `__completeNoDesc`) | yes | missing | `completion.rs:25` is a static hand-written bash script. |
| Shell completion: bash / zsh / fish / powershell | yes (cobra-generated) | yes — but static | `completion.rs:1`. Static template, not introspected. |
| `options` global flag enumeration | yes | partial (`options.rs:1`) | Hand-maintained list. |
| `alpha` command group | yes | missing | No `alpha` subcommand tree. |
| `--cache-dir`, `--cluster`, `--user`, `--as`, `--as-group`, `--as-uid` | yes (global flags) | **all missing** | `main.rs:19-42` only exposes `--kubeconfig`, `--context`, `--server`, `--insecure-skip-tls-verify`, `--token`. |
| `--request-timeout` | yes | missing | — |
| `--profile cpu/mem` (pprof) | yes | missing | — |
| `--warnings-as-errors` | yes | missing | — |
| `--tls-server-name` | yes | missing | — |
| Auth providers: gcp / azure / oidc / aws-iam | yes (exec or in-tree) | **none** | `AuthProvider` deserialized in `kubeconfig.rs:86` but never consulted. |
| Exec credential plugins (`client.authentication.k8s.io/v1`, `v1beta1`) | yes | **none** | `ExecConfig` deserialized in `kubeconfig.rs:93` but never invoked. |
| Client mTLS via `client-certificate-data` / `client-key-data` | yes | **none** | `client.rs:46` builds a `reqwest::Client` with no `Identity`. The data is read into a getter (`kubeconfig.rs:206`) but never installed into the HTTP client. |
| Cluster CA via `certificate-authority-data` | yes | **none** | Not loaded into the `reqwest::ClientBuilder`. |
| `KUBECONFIG` multi-path merge | yes | partial (`kubeconfig.rs:118`) | Honours the env var as a single path; does **not** split on `:` and merge. |
| `KUBECTL_EXTERNAL_DIFF` env var | yes | missing | — |
| `KUBECTL_COMMAND_HEADERS` (telemetry) | yes (HTTP header) | missing | — |
| `KUBECTL_REMOTE_COMMAND_WEBSOCKETS` env toggle | yes (gate for KEP-4006) | n/a — WS is the only path | Inverse problem: there's no way to opt out to SPDY because SPDY isn't implemented. |
| kuberc (alpha config wrapper, KEP-3104) | yes | partial (`kuberc.rs:1`, 80 lines) | Stub set/view only — no aliases / default-flags resolution at command dispatch. |

## Missing features

### 1. Exec credential plugins and auth providers are dead code

`crates/kubectl/src/kubeconfig.rs:80-83` deserializes both `auth_provider` and
`exec` blocks from kubeconfig, but neither is ever consulted when building the
HTTP client. `crates/kubectl/src/client.rs:40-60` constructs a `reqwest::Client`
with at most `danger_accept_invalid_certs(true)` and a static bearer token. The
practical consequences:

- A kubeconfig produced by `aws eks update-kubeconfig`, `gcloud container
  clusters get-credentials`, or `az aks get-credentials` cannot authenticate at
  all — those write `users[].user.exec` blocks.
- Token rotation (every cloud auth provider) does not work; even if the user
  hand-runs the exec plugin and pastes the token, the refresh window is
  measured in minutes.
- OIDC refresh-token flows in the `auth-provider: { name: oidc }` legacy block
  are also ignored.

The minimum viable implementation is: spawn the configured `command` + `args`
with `env: { KUBERNETES_EXEC_INFO: <json> }`, parse stdout as
`client.authentication.k8s.io/v1.ExecCredential`, install
`status.clientCertificateData` / `status.clientKeyData` as a `reqwest`
`Identity`, or use `status.token` as the bearer. Cache by user-name +
expiration.

### 2. Client mTLS / cluster CA are silently dropped

`User::client_certificate_data` and `User::client_key_data` are read in
`kubeconfig.rs:206/214` via `#[allow(dead_code)]` getters that no caller
invokes; same for `Cluster::certificate_authority_data` (`kubeconfig.rs:51`).
`client.rs:40` never calls `ClientBuilder::identity(...)` or
`add_root_certificate(...)`. As a result, any cluster whose user is
authenticated by client-cert (the default for kubeadm-style clusters, kind,
minikube, and CI fixtures) is unreachable except by `--insecure-skip-tls-verify
--token <override>`.

### 3. `apply` cannot prune

`grep -n 'prune\|allowlist' crates/kubectl/src/commands/apply.rs` returns no
matches. Upstream `kubectl apply --prune` walks the cluster looking for objects
with a matching `last-applied-configuration` annotation that were *not* in the
input set and deletes them — a critical workflow for GitOps-style apply (with
or without `--prune-allowlist` to restrict the cleanup scope). Without this
flag, removing a manifest from a directory leaves a zombie object behind.

### 4. SPDY transport is not implemented for exec/attach/port-forward/cp

`crates/kubectl/src/websocket.rs:1` is the only streaming transport. The KEP-4006
"WebSockets for remotecommand" beta-gate was added in 1.31; clusters older than
1.30 (or any cluster where an intermediate proxy strips the `Upgrade:
websocket` header) require the SPDY/3.1 transport. Upstream `kubectl` probes
WebSocket first then falls back; Rusternetes has no fallback path. A SPDY
implementation needs HTTP/2-like framing over the existing TCP+TLS connection
plus channel multiplexing (channels 0–4 for stdin/stdout/stderr/error/resize)
— roughly mirroring `websocket.rs:11-32` but on top of `k8s.io/apimachinery`
SPDY-3.1.

### 5. `--raw` and arbitrary discovery-driven `get` are missing

There is no `get --raw /metrics`, `get --raw /readyz`, etc. — useful for
debugging the apiserver and conformance triage. Each `get` resource type is
also hand-enumerated in `get.rs:585+`; arbitrary CRDs cannot be listed by
their short or long name (e.g. `kubectl get widgets.example.com`) because the
match arm only switches on a closed set of known kinds. A `cli-runtime`-style
builder + visitor over `/apis/.../resources` discovery is needed to make the
CLI work against any installed CRD without recompiling.

### 6. Output-format coverage is narrow

`get.rs:22-30` only models `Table | Json | Yaml | Wide | Name | JsonPath`. The
following upstream `--output` modes are absent:

- `go-template=<tmpl>` / `go-template-file=<path>`
- `custom-columns=NAME:.spec.foo,KIND:.kind` / `custom-columns-file=<path>`
- `jsonpath-as-json=<expr>` (emit as JSON value, not stringified)
- `template=<tmpl>` (alias for go-template)
- Server-side `Table` (uses the API server's `accept: application/json;as=Table;…`
  content negotiation, so any resource — including CRDs with `additionalPrinterColumns`
  — gets a sensible table without client-side knowledge).

The `jsonpath` engine itself in `get.rs:90-160` is hand-rolled and lacks filter
expressions (`get.rs:135-138` returns `""` for `[?(@.x=="y")]`), range loops,
string-literal concatenation across `{}{}{}`, and the `..` recursive descent
operator.

### 7. Plugin invocation (cobra's `kubectl <plugin>` fallthrough)

`crates/kubectl/src/commands/plugin.rs:12` correctly *lists* `kubectl-*`
binaries on PATH, but the main dispatcher in `main.rs:1066-1695` uses `clap`'s
strict subcommand match — there is no fallback that says "if `Commands::parse`
fails with `UnknownSubcommand`, look up `kubectl-<name>` on PATH and exec it
with the remaining argv". Plugins (krew packages, custom enterprise tooling)
are therefore listable but not runnable. Implementation note: `clap`'s
`ignore_errors(true)` + manual residue handling, or pre-clap argv inspection.

### 8. Server-side shell completion is missing

`completion.rs:1` emits a static template. Upstream emits a cobra-generated
script that calls back into `kubectl __complete <args>` for dynamic completion
(pod names, namespaces, contexts, JSONPath fields). Rusternetes has neither
the `__complete` / `__completeNoDesc` hidden commands nor the dynamic
namespace/resource enumeration. As a result, tab completion only works on
static subcommand and resource-type lists.

### 9. `kustomize` integration is partial

`kustomize.rs` (74 lines) exists as a standalone subcommand, but `apply`,
`delete`, `diff`, and `get` do not accept `-k <kustomization-dir>`. This is the
day-to-day way Kustomize is used today (`kubectl apply -k overlays/prod`).
Wiring requires resolving `-k` to a built `Resources` blob before the existing
`-f` plumbing kicks in.

### 10. `debug` lacks profiles and copy workflows

`debug.rs:1` implements only the two simplest debug shapes: add an ephemeral
container to a pod, and spawn a privileged debug pod onto a node. Missing:
`--profile={general,baseline,restricted,netadmin,sysadmin}` (controls
capabilities/securityContext for the ephemeral container), `--image-profile`,
`--copy-to <new-pod>` (clone the target pod with modifications),
`--share-processes`, `--set-image`, `--keep-labels`, `--keep-annotations`,
`--keep-init-containers`, `--keep-liveness`, `--keep-readiness`,
`--keep-startup`. These are critical for production troubleshooting workflows.

### 11. `wait` lacks `--for=jsonpath=` and `--for=create`

`wait.rs:54` only branches on `for_condition` and `for_delete`. Upstream
supports `--for=jsonpath='{.status.phase}'=Running` (poll an arbitrary
JSONPath until it equals a value) and `--for=create` (wait until the object
*exists*, useful right after `kubectl apply`). Also missing: `--all` to wait
on every object of a kind that matches a selector.

### 12. `--field-manager`, list pagination, and managed-fields display

Three small but visible apiserver-touch gaps:

- `get --chunk-size=<N>` is unsupported — large lists are fetched in a single
  request (no `?limit=&continue=` handshake), so a cluster with 10k pods is a
  10k-row response. Rusternetes' own api-server now supports chunking (see
  `crates/api-server/src/handlers/`), so this is a pure client-side gap.
- `get --show-managed-fields` toggle is missing; managed fields ride along in
  `-o yaml` / `-o json` whether you want them or not. Upstream strips them by
  default.
- `apply --field-manager` is honored (`main.rs:185`) but `--server-side` does
  not surface the `Conflict` response with the upstream's "use
  --force-conflicts to overwrite" hint — the user gets a raw status.

### 13. Global flags `--as`, `--as-group`, `--as-uid` (impersonation)

`main.rs:19-42` exposes only `--kubeconfig`, `--context`, `--server`,
`--insecure-skip-tls-verify`, `--token`. Upstream's impersonation triple
(`--as`, `--as-group=` repeated, `--as-uid`) maps to the `Impersonate-User`,
`Impersonate-Group`, `Impersonate-Uid` headers and is the standard way an
admin tests "what can user X do?". Currently the only workaround is to
manually craft a token for that user.

### 14. `KUBECONFIG` colon-separated merge and `--cluster`/`--user` overrides

`kubeconfig.rs:118` reads `$KUBECONFIG` as a single file path. The upstream
loader splits on `:` (`;` on Windows), parses every entry, and merges them
(later wins) — which is how `aws eks update-kubeconfig --kubeconfig` users
combine personal + team configs. Per-invocation `--cluster=`, `--user=`,
`--namespace=` overrides (separate from `--context`) are also missing.

### 15. `alpha` subcommand group and KUBECTL_*` env vars

`kubectl alpha` is a stable umbrella for not-yet-promoted commands (events
historically lived there, debug lived there). Rusternetes has no `alpha` group
— so any future experimental command has to be added at the top level.
Adjacent missing env-var integrations: `KUBECTL_EXTERNAL_DIFF` (custom diff
program), `KUBECTL_COMMAND_HEADERS` (telemetry suppression),
`KUBECTL_REMOTE_COMMAND_WEBSOCKETS` (the toggle that lets ops force SPDY when
WS is broken on the wire path).

## Partial / stubbed

- `kustomize.rs` (74 lines) — the standalone `kubectl kustomize <dir>` shells
  out / re-implements only the simplest cases; no `bases`, `components`,
  `patches`, `replicas`, `images`, `configMapGenerator/secretGenerator`,
  `vars`, `replacements`, or remote-URL resolution comparable to
  `sigs.k8s.io/kustomize/api`.
- `kuberc.rs` (80 lines) — alpha config wrapper exists as `set`/`view`
  scaffolding but is never read at command dispatch time. The whole point of
  KEP-3104 kuberc (default flags + command aliases) is to mutate the effective
  argv before clap parses it; Rusternetes parses argv first and never consults
  kuberc.
- `completion.rs` — static hand-written bash/zsh/fish/powershell scripts with
  a manually-maintained subcommand and resource list (`completion.rs:35`,
  `:43`). Will drift any time a new subcommand/resource is added.
- `diff.rs` — line-by-line YAML diff against the live object only. No
  `--server-side` (server-render the merge result), no
  `KUBECTL_EXTERNAL_DIFF` (`diff.rs:1` makes no env-var lookup), no `--recursive`.
- `auth can-i` — only `SelfSubjectAccessReview` (`auth.rs:67-86`). Missing
  `--list` (which uses `SelfSubjectRulesReview` to enumerate everything the
  user can do in a namespace) and `--subresource=<sub>` for things like
  `pods/exec`, `pods/log`.
- `options` — static text (`options.rs:6-8`) listing only the flags the
  Rusternetes CLI itself defines. Will diverge from the actual `--global`
  flag surface as soon as one is added.
- `top` — depends on Metrics Server endpoints; partial because filtering /
  `--containers` / sort order is incomplete in `top.rs`.

## Known in-code TODOs

`grep -rn 'TODO\|FIXME\|todo!\|unimplemented' crates/kubectl/src/` returns
zero matches — there are no explicit TODO comments. The implicit gaps are
encoded as missing CLI flags and missing match arms, enumerated above.

Defensive panics (not stubs, but worth noting because they indicate
not-fully-exhaustive matches that future flag additions would have to revisit):

- `crates/kubectl/src/commands/annotate.rs:207` — `panic!("unexpected")`
- `crates/kubectl/src/commands/autoscale.rs:335` — `panic!("unexpected")`
- `crates/kubectl/src/commands/label.rs:183` — `panic!("unexpected")`
- `crates/kubectl/src/commands/patch.rs:82, 93, 104, 167` — `panic!("invalid"|"unexpected")`

JSONPath filter expressions silently return empty strings (`crates/kubectl/src/commands/get.rs:135-138`)
— a correctness gap masquerading as a missing flag.

## References

- `crates/kubectl/src/main.rs:1` — `clap` `Cli`/`Commands` definitions for all
  48 subcommands.
- `crates/kubectl/src/types.rs:1` — subcommand `enum`s
  (`CreateCommands`, `RolloutCommands`, `AuthCommands`, `SetCommands`,
  `ApplyCommands`, `ConfigCommands`, etc.).
- `crates/kubectl/src/kubeconfig.rs:1` — kubeconfig parser; auth-provider /
  exec / client-cert getters that no caller invokes.
- `crates/kubectl/src/client.rs:1` — `reqwest`-based HTTP client; no mTLS, no
  exec-plugin call-out.
- `crates/kubectl/src/websocket.rs:1` — WebSocket-only streaming
  implementation.
- `crates/kubectl/src/commands/get.rs:22` — `OutputFormat` enum (narrow).
- `crates/kubectl/src/commands/apply.rs:79` — apply pipeline (no prune, no
  client validation).
- `crates/kubectl/src/commands/diff.rs:8` — yaml-line diff (no external diff,
  no server-side).
- `crates/kubectl/src/commands/plugin.rs:12` — plugin *discovery* only; no
  invocation fall-through.
- `crates/kubectl/src/commands/completion.rs:22` — static completion scripts.
- Upstream: `kubernetes/kubernetes` tree under
  `staging/src/k8s.io/kubectl/pkg/cmd/` — one Go package per subcommand.
- Upstream: `staging/src/k8s.io/cli-runtime/pkg/` — resource builder and
  visitor used by every upstream subcommand for arbitrary-resource handling
  (the architectural piece Rusternetes' per-kind handlers replace, and which
  blocks generic-CRD support).
- KEP-4006 — "WebSockets for kubectl exec/attach/port-forward" (beta in 1.31).
- KEP-3104 — `kuberc` configuration file (alpha).
- KEP-2585 — `kubectl debug` ephemeral containers (GA in 1.25).
