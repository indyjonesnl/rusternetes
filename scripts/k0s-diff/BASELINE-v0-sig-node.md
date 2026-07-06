# v0 baseline — sig-node

Reference result the later variant swaps (v1–v6) diff against. Captured by
`run-variant.sh v0 sig-node` against the Task-1 v0 stack (stock all-Go k0s
control plane + kubelet, containerd-rs CRI → crun, kuberouter CNI, kube-proxy
enabled, kine→SQLite datastore).

| field | value |
|-------|-------|
| conformance image | `registry.k8s.io/conformance:v1.35.5` |
| server version | `v1.35.5+k0s` (kubectl client v1.35.6) |
| ginkgo focus | `\[sig-node\].*\[Conformance\]` (no `--skip`) |
| specs selected | 105 of 7355 |
| **passed** | **105** |
| **failed** | **0** |
| skipped | 7250 |
| wall time | 2761 s (~46 min), serial (p=1) |

**Failing tests:** none. Full green sig-node [Conformance] baseline.

No cap was hit — the run completed within the ~45 min window.

Raw output (git-ignored, machine-local): `scripts/k0s-diff/results/v0/sig-node/`
(`e2e.log`, `junit_01.xml`); summary at `scripts/k0s-diff/results/v0/sig-node.json`.
