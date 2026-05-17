# Kubelet Node Conformance

Rusternetes runs the official Kubernetes v1.35 `e2e_node.test` suite focused on `[NodeConformance]` against a single kubelet via `scripts/run-node-conformance.sh`.

This is **complementary** to the full Sonobuoy run tracked in `docs/CONFORMANCE.md`. Node conformance is faster (minutes) and isolates kubelet bugs from scheduler/controller-manager/kube-proxy noise.

## How to run

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
bash scripts/run-node-conformance.sh
```

The script:

1. Brings up `compose.node-conformance.yml` (etcd + api-server + one kubelet)
2. Fetches `kubernetes-test-linux-amd64.tar.gz` for v1.35 if not cached, extracts `e2e_node.test` + `ginkgo` to `.bin/`
3. Runs `ginkgo --focus='[NodeConformance]'` against `localhost:10250`
4. Writes the full log to `/tmp/node-conformance/ginkgo.log` and prints PASS / FAIL / SKIP counts

## Results

| Round | Date | Pass | Fail | Skip | Pass Rate | Notes |
|-------|------|------|------|------|-----------|-------|
| 1 | 2026-05-17 | — | — | — | — | Initial scaffold; many endpoints not yet implemented |

## Currently unimplemented kubelet endpoints

The following are expected by `e2e_node.test` and are not yet served by Rusternetes' kubelet. PR2 of this initiative implements them.

- `GET /pods` — pods bound to this node
- `GET /runningpods/` — running subset
- `GET /healthz` — sync-loop liveness probe
- `GET /stats/summary` — minimal cAdvisor shape
- `GET /logs/:pod/:ns/:container` — log proxy
- `POST /run/:pod/:ns/:container` — exec alias

## Related

- `docs/CONFORMANCE.md` — full Sonobuoy suite
- `docs/superpowers/specs/2026-05-17-node-conformance-design.md` — design rationale
- [Upstream node conformance docs](https://kubernetes.io/docs/setup/best-practices/node-conformance/)
