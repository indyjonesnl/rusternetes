# Vanilla-cluster single-module swap testing

Run a stock upstream Kubernetes cluster (via [kind](https://kind.sigs.k8s.io/))
with **exactly one** rusternetes component swapped in, then run a test subset
scoped to that component. Every other participant is unmodified upstream, so any
failure is attributable to the single swapped module — no debugging interactions
between multiple half-working rusternetes parts.

## Why

The all-rusternetes stack couples five components. When conformance fails there,
the cause could be any of them or their interaction. This harness isolates one
module at a time against a known-good vanilla cluster, giving a clean per-module
conformance signal (e.g. "does our kubelet work in a real Kubernetes cluster?").

## Modules and how each is swapped

| Module | Swap mechanism | Scoped subset |
|--------|----------------|----------------|
| `kubelet` | joins as an extra worker node (own containerd + bridge CNI) via `kubeadm join` | `sig-node` `[NodeConformance]` |
| `kube-proxy` | patch the `kube-system` kube-proxy DaemonSet image | `sig-network` Services |
| `api-server` | swap the `kube-apiserver` static-pod image (own storage backend) | `sig-api-machinery` |
| `scheduler` | swap the `kube-scheduler` static-pod image | `sig-scheduling` |
| `controller-manager` | swap the `kube-controller-manager` static-pod image | `sig-apps` |

The swapped module interoperates over standard interfaces only — the Kubernetes
API for control-plane components, CRI/CNI (and CSI where relevant) for the kubelet.

## Run it locally

```bash
# Prereqs (fail fast, not auto-installed): docker, kind, kubectl, hydrophone, jq
scripts/vanilla-swap-run.sh --module kubelet
```

Flags: `--module <name>` (required, exactly one), `--env local|ci|cloud`,
`--keep` (skip teardown), `--k8s-version vX.Y` (default `v1.35`).

Pin the rusternetes image tag with `RUSTERNETES_IMAGE_TAG` (default `main`).

## Outcomes and exit codes

| exit | outcome | meaning |
|------|---------|---------|
| 0 | `test-passed` | module came up and the scoped subset passed |
| 1 | `test-failed` | module came up but failed scoped tests (a real gap) |
| 2 | — | usage / missing tool / invalid registry |
| 3 | `guard-rejected` | more than one rusternetes component present |
| 4 | `version-skew-unsupported` | baseline version incompatible with the build |
| 5 | `module-did-not-come-up` | module never became healthy (integration bug) |

A machine-readable `run-result.json` is written to the run's work dir.

## In CI

**One workflow per module — never a matrix.** Each module has its own
`.github/workflows/vanilla-swap-<module>.yml` on the self-hosted DinD runners,
so a module's run has its own status, its own badge, its own concurrency group,
and its own dispatch button. A matrix would collapse five independent isolation
tests into one run status, where a queued-but-unstarted module is
indistinguishable from a passing one, and "re-run just the kubelet" would mean
re-running all five.

| Workflow | Module | Nightly (UTC) |
|----------|--------|---------------|
| `vanilla-swap-kubelet.yml` | `kubelet` | 04:00 |
| `vanilla-swap-api-server.yml` | `api-server` | 06:00 |
| `vanilla-swap-scheduler.yml` | `scheduler` | 08:00 |
| `vanilla-swap-controller-manager.yml` | `controller-manager` | 10:00 |
| `vanilla-swap-kube-proxy.yml` | `kube-proxy` | 12:00 |

Run one on demand:

```bash
gh workflow run vanilla-swap-kubelet.yml                      # latest main image
gh workflow run vanilla-swap-kubelet.yml -f image-tag=pr-1234 # a specific build
```

Those caller files are **generated** — edit `ci/vanilla-swap/targets.json` and
re-run `scripts/gen-vanilla-swap-workflows.sh`;
`scripts/tests/test-vanilla-swap-workflows-sync.sh` guards the drift (and fails
if a matrix ever reappears). All the real logic lives in the reusable engine
`.github/workflows/vanilla-swap-module.yml`, which is also directly
`workflow_dispatch`-able with any `module`.

### Badges

Each module publishes a README badge from its nightly run. A run with spec
counts shows the pass rate (`87% (27/31)`); a run that produced no counts —
`module-did-not-come-up`, `guard-rejected` — publishes that outcome in red
instead, so a broken module can't leave a stale green pass rate behind. See
`scripts/update-badge.sh`.

## Files

- `scripts/vanilla-swap-run.sh` — driver (see the CLI contract in `specs/003-vanilla-module-swap/contracts/harness-cli.md`)
- `scripts/vanilla-swap-common.sh` — shared helpers (guard, swap recipes, readiness, result)
- `scripts/gen-vanilla-swap-workflows.sh` — emits the per-module workflow callers
- `ci/vanilla-swap/targets.json` — the isolation-target registry (one entry per module)
- `ci/vanilla-swap/kind/` — the vanilla base cluster config + per-module swap recipes

## Design docs

Full spec, plan, and task breakdown live under `specs/003-vanilla-module-swap/`
(spec.md, plan.md, research.md, data-model.md, contracts/, quickstart.md, tasks.md).
