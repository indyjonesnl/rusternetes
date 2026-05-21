# microk8s

Source-of-truth: https://github.com/canonical/microk8s tree `master`
(content captured 2026-05-21; pin to whichever commit `master` resolves to
on inspection — Canonical does not tag a stable SHA per branch tip).

Files referenced below (all live under the same repo root):

- `microk8s-resources/actions/common/utils.sh`
- `microk8s-resources/default-args/kube-apiserver`
- `microk8s-resources/default-args/cluster-agent`
- `microk8s-resources/wrappers/microk8s-status.wrapper`
- `microk8s-resources/wrappers/microk8s-start.wrapper`
- `microk8s-resources/wrappers/run-cluster-agent-with-args`
- `scripts/wrappers/status.py`
- `scripts/wrappers/common/utils.py`
- `tests/smoke-test.sh`
- `tests/test-simple.py`
- `tests/test-cluster.py`
- `tests/test-cluster-agent.py`

## What this tool does

MicroK8s is the Canonical snap-packaged Kubernetes distribution. The whole
cluster boots from a single snap (`microk8s`), and a wrapper CLI
(`microk8s status`, `microk8s kubectl`, ...) gates user commands behind
permission/lock-file checks. "Ready" is a layered concept: the snap's
systemd-style units (managed via `snapctl services`) must be `active`,
the api-server's TLS endpoint must respond `ok` on `/readyz`, the
`cluster-agent` HTTP service on port `25000` must answer `/health`, and
finally `kubectl get all --all-namespaces` must contain
`service/kubernetes` while `kubectl get nodes` shows a ` Ready ` line.
Smoke tests in `tests/` orchestrate these checks plus a dataplane probe
(deploy nginx, hit the cluster-IP, expect 200).

## Bootstrap / preflight endpoints

- `GET https://127.0.0.1:16443/readyz` — `is_apiserver_ready` in
  `microk8s-resources/actions/common/utils.sh:1060-1067`. Uses mTLS
  (`--cert server.crt --key server.key --cacert ca.crt`) and asserts the
  body contains the literal `ok`.
- `GET https://127.0.0.1:25000/health` — cluster-agent health probe in
  `tests/test-cluster-agent.py:13-15`. Verified with the snap CA bundle
  (`/var/snap/microk8s/current/certs/ca.crt`). JSON body checked for
  `status == "OK"`.
- `GET /api/v1/services?allNamespaces=true` via `kubectl get all
  --all-namespaces` — `is_cluster_ready` in
  `scripts/wrappers/common/utils.py:127-132`. Output must contain
  `service/kubernetes`.
- `GET /api/v1/nodes` via `kubectl get nodes` — same
  `is_cluster_ready`. Output must contain ` Ready ` (leading and
  trailing space matters).
- `GET /apis/apps/v1/namespaces/kube-system/deployments/calico-kube-controllers`
  via `kubectl -n kube-system rollout status
  deployment.apps/calico-kube-controllers` — `tests/smoke-test.sh:21`.
- `GET /api/v1/namespaces/kube-system/pods` via `kubectl get pods -n
  kube-system` — `tests/test-simple.py:19-31`. Asserts every calico pod
  reports `Running`.
- `GET /apis/apps/v1/namespaces/<ns>/deployments/<name>` (rollout
  status) — `tests/test-simple.py:33-56` after applying
  `simple-deploy.yaml`.
- `GET /api/v1/namespaces/<ns>/services/nginx-service` — same test,
  uses `kubectl get svc ... -o jsonpath={.spec.clusterIP}` then issues
  `requests.get(f"http://{clusterIP}:80")`.
- `snapctl services microk8s.daemon-<svc>` (not HTTP) — `wait_for_service`
  in `microk8s-resources/actions/common/utils.sh:590-607`. Polls
  systemd-via-snapd for `active` 30 times at 1 s intervals.

## JSON payloads

MicroK8s drives the api-server almost exclusively through
`microk8s.kubectl` (`scripts/wrappers/common/utils.py:KUBECTL =
"$SNAP/microk8s-kubectl.wrapper"`), so the wire payloads are whatever
`kubectl get/apply/rollout` emit. Notable shapes the tests rely on:

- `kubectl get all --all-namespaces` — returns concatenated
  `service/kubernetes`, `pod/...`, `deployment.apps/...` lines.
  `is_cluster_ready` greps for the literal `service/kubernetes`, so the
  api-server must surface the bootstrap `kubernetes` Service in
  `default`.
- `kubectl get nodes` — must produce a row containing ` Ready ` (space
  before, space after). The space-padding is how `is_cluster_ready`
  rejects `NotReady`.
- `kubectl get pods -n kube-system -o wide` — `tests/test-simple.py`
  splits on whitespace and asserts column 3 (`STATUS`) is `Running`.
- `kubectl get svc <name> -o jsonpath={.spec.clusterIP}` — must return
  exactly the IP, no quoting, so `.spec.clusterIP` cannot be
  `None`/`null` for a ClusterIP service.
- `kubectl -n kube-system rollout status deployment.apps/<name>` —
  drives `GET /apis/apps/v1/.../deployments/<name>` and watches
  `.status.observedGeneration` and `.status.readyReplicas`.
- Cluster-agent `/health` — direct HTTP GET; no JSON sent, JSON
  returned (`{"status":"OK"}`).
- Cluster-agent join/sign-cert routes (`/cluster/api/v1.0/*`,
  `/cluster/api/v2.0/join`) live in the closed-source `cluster-agent`
  binary started by `run-cluster-agent-with-args`. They are NOT
  exercised by `tests/test-cluster-agent.py` (only `/health` is). The
  binary listens on `--bind 0.0.0.0:25000` per
  `microk8s-resources/default-args/cluster-agent`.

## Expected responses / assertions

- `is_apiserver_ready` (`utils.sh:1060-1067`):
  ```bash
  if (${SNAP}/usr/bin/curl -L --cert ${SNAP_DATA}/certs/server.crt \
      --key ${SNAP_DATA}/certs/server.key --cacert ${SNAP_DATA}/certs/ca.crt \
      https://127.0.0.1:16443/readyz | $SNAP/bin/grep -z "ok") &> /dev/null
  then return 0; else return 1; fi
  ```
- `is_cluster_ready` (`scripts/wrappers/common/utils.py:127-132`):
  ```python
  return "service/kubernetes" in kubectl_get("all") and (
      not with_ready_node or " Ready " in kubectl_get("nodes")
  )
  ```
- `wait_for_ready` (same file, 134-143): polls `is_cluster_ready` every
  2 s until `timeout` elapses.
- `tests/smoke-test.sh:5-12`: 10 retries × 20 s waiting for
  `service/kubernetes` to appear.
- `tests/smoke-test.sh:14-19`: 3 retries × 20 s waiting for any `Ready`
  node.
- `tests/test-simple.py:7-17`: asserts `parts[1] == "Ready"` for every
  node returned by `kubectl get nodes`.
- `tests/test-cluster-agent.py:13-15`: `requests.get(...,
  verify=ca_path)` then `assert response.json()["status"] == "OK"`.
- `microk8s-resources/default-args/kube-apiserver`:
  `--secure-port=16443`, `--authorization-mode=AlwaysAllow`,
  `--allow-privileged=true`, `--profiling=false`, `--event-ttl=5m`.
  Aggregation layer + EventRateLimit admission are enabled by default.

## Rusternetes-compat checklist

- `GET /readyz` — present at `crates/api-server/src/router.rs:672`,
  handler `handlers::health::readyz`
  (`crates/api-server/src/handlers/health.rs:31`). Must respond with
  body containing `ok` for microk8s-style `curl | grep -z "ok"` to
  succeed — verify body shape matches upstream (`pkg/server/healthz`
  returns the literal `ok`; current handler returns
  `Json<HealthStatus>`, which is JSON, not `ok` — likely fails the
  microk8s grep). FIX CANDIDATE.
- `GET /healthz` and `/livez` — present at
  `crates/api-server/src/router.rs:669-671`, both route to
  `handlers::health::healthz` which (per the comment at
  `handlers/health.rs:20`) emits `200 ok` as upstream does. Good.
- `GET /api/v1/services` (cluster-wide list) — present at
  `crates/api-server/src/router.rs:994-995`
  (`handlers::service::list_all_services`). Needed so `kubectl get all
  --all-namespaces` surfaces `service/kubernetes`.
- Bootstrap `kubernetes` Service in `default` namespace — created at
  `crates/api-server/src/lib.rs:135-141` and
  `crates/api-server/src/main.rs:221` (servicecidrs + name
  `"kubernetes"`). Required for the `"service/kubernetes" in
  kubectl_get("all")` assertion.
- `GET /api/v1/nodes` — present at
  `crates/api-server/src/router.rs:1064`. Required for `kubectl get
  nodes` to emit ` Ready ` rows.
- `GET /apis/apps/v1/namespaces/:namespace/deployments/:name/status`
  — present at `crates/api-server/src/router.rs:1106`. Required for
  `kubectl rollout status deployment.apps/...`.
- `GET /api/v1/namespaces/:namespace/services/:name/proxy[/*path]` —
  present at `crates/api-server/src/router.rs:964-980`. Not strictly
  required for microk8s smoke tests (they hit ClusterIP directly), but
  cluster-IP routing through kube-proxy is — verify kube-proxy
  programs iptables for the deployed `nginx-service`.
- `GET /cluster/api/v1.0/*` and `GET /cluster/api/v2.0/join` — missing
  by design; these are the proprietary cluster-agent endpoints
  (`microk8s-resources/default-args/cluster-agent` binds
  `0.0.0.0:25000`). Rusternetes has no equivalent; not blocking for
  conformance.
- `GET https://127.0.0.1:25000/health` — missing; same reasoning.
- `--authorization-mode=AlwaysAllow` parity — rusternetes defaults
  differ (RBAC is enforced); the microk8s smoke path runs with auth
  effectively disabled, so any 401/403 differences are likely
  authorization-mode mismatches, not endpoint gaps.
