# Upstream conformance timing baseline

Per-spec conformance timings for **upstream Kubernetes v1.35.0**, measured so
Rusternetes timings can be read as a *ratio* rather than an absolute. A spec
that is slow on both is slow by design; a spec that is slow only here is ours
to fix.

Measured once, 2026-09-07. Upstream v1.35.0 does not change, so this does not
need re-running unless the conformance image pin moves.

## Method

Same harness on both sides — only the implementation under test differs:

| | value |
|---|---|
| baseline cluster | `kind` v0.33.0, `kindest/node:v1.35.0` |
| topology | 1 control-plane + 1 worker (matches the compose stacks) |
| runtime | containerd 2.2.0 |
| conformance image | `registry.k8s.io/conformance:v1.35.0` |
| runner | `scripts/conformance-tags-run.sh --parallel 4` (two-phase split) |
| box | otherwise idle — a competing run inverts the conclusion |

`--skip-preflight` is required against kind: the gate inspects Rusternetes
containers by name.

## Upstream results

| phase | specs | wall | passed |
|---|---|---|---|
| 1 (parallel, 4 procs) | 406 | **22.7 min** | 405 |
| 2 (`[Serial]`/`[Slow]`, 1 proc) | 35 | **36.7 min** | 34 |

Phase 1 spec time totals **81.0 min** across the 406 specs (wall is lower
because of parallelism).

Two failures, both environmental rather than upstream defects:

- `[sig-architecture] … should have at least two untainted nodes` — kind taints
  its control-plane, leaving one schedulable node. An artifact of the 2-node
  topology chosen for parity.
- `[sig-apps] Daemon set [Serial] should rollback without unnecessary restarts`

## What the ratio showed

Against a Rusternetes phase-1 run on the same 406 specs:

| | upstream | Rusternetes | ratio |
|---|---|---|---|
| phase 1 wall | 22.7 min | 95 min | **4.2x** |
| phase 1 spec time | 81.0 min | 371.1 min | **4.6x** |
| phase 1 passed | 405/406 | 383/406 | |
| phase 1 -> phase 2 cleanup | **39 s** | never completed (2 attempts) | |

**80% of the excess sits in 88 specs — 21% of them.** Excess by sig:
api-machinery 96.6 min, node 67.5, network 49.3, apps 32.1, storage 30.0,
cli 9.7, auth 3.1, scheduling 1.2, instrumentation 0.6.

Phase 2 is roughly at **parity** (upstream 36.7 min; a previous Rusternetes
phase-2 measurement was 38m01s). Its slowest specs wait by design — API
chunking 598 s, SchedulerPredicates 304 s, suspended CronJob 300 s — so the
serial phase is not where Rusternetes loses time. Phase 1 is.

## Findings worth keeping

Two plausible-sounding conclusions the data refuted:

1. **"The AdmissionWebhook specs are slow by design."** They are not: `should
   mutate configmap` is 3.4 s upstream and 366 s here, ~108x. Six webhook specs
   lose ~2000 s between them — the largest cluster after the single outlier.
2. **"There is a fixed latency floor."** Specs upstream finishes in 0.1 s do
   take 20-68 s here, but that cohort is only 25 specs and 10.3 min — **3%** of
   total. Real, and not the prize.

The largest single item is `[sig-node] Pods Extended (pod generation) … issue
500 podspec updates`, at 2754 s here against 262 s upstream. Note upstream is
also slow on it — the spec is inherently expensive — but the 2493 s of excess
is still the biggest recoverable block in the suite.

## Reproducing

```bash
kind create cluster --name conformance-baseline \
  --config <2-node config> --image kindest/node:v1.35.0 \
  --kubeconfig /tmp/kind.kubeconfig

bash scripts/conformance-tags-run.sh \
  --kubeconfig /tmp/kind.kubeconfig --parallel 4 --skip-preflight \
  --output-dir <dir>
```

Pin the node image explicitly. A version-skewed baseline selects a different
spec set and silently invalidates every ratio computed from it.
