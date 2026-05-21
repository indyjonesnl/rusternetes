# k3d

Source-of-truth: <https://github.com/k3d-io/k3d> @ `be2d9693c3954c176308b7a1dcf39bf0e15a856a` (main, 2026-04-02)

## What this tool does

k3d wraps **k3s** (Rancher's lightweight Kubernetes) inside Docker containers so a
multi-node cluster can be brought up on a developer laptop. Each `k3d cluster
create` builds one or more containers per role (`server`, `agent`, optional
`serverlb` load-balancer, optional `registry`) and exposes the k3s api-server
through the load-balancer. k3d **never speaks HTTP to the api-server** during
bootstrap — its definition of "ready" is purely log-marker matching on the
container's stdout/stderr stream (and a follow-up CoreDNS configmap log
sentinel). The kubeconfig produced at the end is then used by the *user* with
kubectl; k3d itself does not hit `/healthz`, `/readyz`, or `/livez`.

## Bootstrap / preflight endpoints

k3d performs **zero HTTP calls** against the api-server. All readiness checks
are log-substring matches via `NodeWaitForLogMessage`:

- *(no HTTP probe)* — `pkg/client/node.go:484` calls
  `NodeWaitForLogMessage(ctx, runtime, node, nodeStartOpts.ReadyLogMessage, startTime)`
  to block until the per-role marker shows up in container logs.
- *(no HTTP probe)* — `pkg/client/loadbalancer.go:73` waits for the serverlb
  (nginx) "start worker processes" marker, and `:76` watches for the failure
  marker `"host not found in upstream"` on a parallel context.
- *(no HTTP probe)* — `pkg/client/cluster.go:1139` waits for the marker
  `"Cluster dns configmap"` on the first server so the CoreDNS rewrite hook
  can fire safely.
- *(no HTTP probe)* — `pkg/client/node.go:635-640` scans for `level=fatal` in
  the previous log line to fast-fail on crashloops.

There is one indirect "wait-for-server" toggle that only chains more log waits;
it never opens a socket to the api-server:
`cmd/cluster/clusterCreate.go:152-153` sets `WaitForServer = true` when
`--kubeconfig-update-default` is on, which is consumed at
`cmd/cluster/clusterCreate.go:99`.

## JSON payloads

**None.** k3d's readiness layer is log-driven, not API-driven. The transport
is Docker's `ContainerLogs` stream (see `NodeWaitForLogMessage` at
`pkg/client/node.go:790-860`). No JSON is POSTed, no resource is GET'd against
the api-server during the create/start path.

The closest k3d gets to "JSON" is `actions.RewriteFileAction` in
`pkg/client/cluster.go` rewriting the in-container CoreDNS YAML manifest
through the docker exec/cp surface — still no api-server traffic.

## Expected responses / assertions

k3d looks for these exact case-insensitive substrings in container logs
(`pkg/types/k3slogs.go:34-50`):

| Role                                 | Intent              | Substring                       |
| ------------------------------------ | ------------------- | ------------------------------- |
| `InternalRoleInitServer` (etcd init) | `IntentClusterCreate` | `Containerd is now running`     |
| `InternalRoleInitServer`             | `IntentClusterStart`  | `Running kube-apiserver`        |
| `InternalRoleInitServer`             | `IntentAny`           | `Running kube-apiserver`        |
| `ServerRole`                         | `IntentAny`           | `k3s is up and running`         |
| `AgentRole`                          | `IntentAny`           | `Successfully registered node`  |
| `LoadBalancerRole`                   | `IntentAny`           | `start worker processes`        |
| `RegistryRole`                       | `IntentAny`           | `listening on`                  |

Additional post-start markers in `pkg/client/`:

- `Cluster dns configmap` — server log proving CoreDNS configmap is loaded
  (`cluster.go:1139`).
- `host not found in upstream` — explicit *failure* signal scraped from the
  serverlb (nginx) log to short-circuit a wedged start
  (`loadbalancer.go:76-77`).
- `level=fatal` — generic crash detector
  (`node.go:635-640`).

The match is implemented at `pkg/client/node.go:605` as
`strings.Contains(strings.ToLower(scanner.Text()), message)`, so all markers
are matched case-insensitively against each log line. There is no JSON
schema, no HTTP status code check, no condition matcher beyond the substring.

## Rusternetes-compat checklist

k3d does not target rusternetes (it boots k3s), so a strict compat answer is
"k3d will not work as-is." That said, the question is whether the markers
k3d watches for would show up if you swapped the k3s image for a rusternetes
api-server. Grepped against this worktree:

- `Running kube-apiserver` — **missing.** No emit in `crates/api-server/`.
  Rusternetes logs `Starting Rusternetes API Server` instead at
  `crates/api-server/src/lib.rs:83` and `crates/api-server/src/main.rs:123`.
- `k3s is up and running` — **missing** (rusternetes is not k3s; cannot and
  should not fake this marker).
- `Containerd is now running` — **missing.** Rusternetes uses bollard against
  Docker; no containerd surface exists in `crates/kubelet/`.
- `Successfully registered node` — **missing.** The Node-registration path in
  `crates/api-server/src/handlers/node.rs` does not log this string.
- `start worker processes` — **n/a** (this is nginx-in-serverlb, not a
  rusternetes component).
- `listening on` — **partial match.** `crates/api-server/src/lib.rs:259,270`
  and `crates/api-server/src/main.rs:345,379` emit
  `HTTPS server listening on {addr}` / `API Server listening on {addr}`.
  k3d's RegistryRole regex would substring-match these, but rusternetes is not
  registered as a `RegistryRole` node in k3d's typing.
- `/healthz`, `/livez`, `/readyz` — **present but unused by k3d.** Routes
  exist at `crates/api-server/src/router.rs:669-672` (handlers in
  `crates/api-server/src/handlers/health.rs:23,31,77`); k3d simply never
  calls them.
- `Cluster dns configmap` — **missing.** Rusternetes' CoreDNS bootstrap is
  driven by `scripts/bootstrap-cluster.sh`, which the tree at HEAD ships
  (verified via `git ls-files scripts/bootstrap-cluster.sh`); the api-server
  itself does not log this string.

**Conclusion:** k3d's readiness contract is a closed set of k3s-specific log
strings. Closing rusternetes' 26 conformance gaps does not require any
k3d-facing surface change, because k3d would never spin up a rusternetes
container in the first place (`k3d cluster create` hard-codes the k3s image
tree at `pkg/types/defaults.go`). If we ever want a "k3d-style" launcher for
rusternetes, the cheapest copy-paste is to emit `Running kube-apiserver` at
the existing `info!("Starting Rusternetes API Server")` site in
`crates/api-server/src/lib.rs:83` so a forked k3d with a swapped image could
detect us via its existing `InitServer` / `ServerRole` matcher.
