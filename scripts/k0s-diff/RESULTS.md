# k0s Differential Conformance Harness — Results

This harness answers one question per Rusternetes component: **swap ONE
piece of a real, otherwise-stock k0s cluster (v1.35.5) for its Rusternetes
equivalent, keep everything else Go, and see whether Kubernetes Conformance
still passes.** Doing this component-by-component (rather than running the
all-Rust stack directly) isolates each component's gaps from the others'.

Baseline (`v0`, unmodified k0s) establishes the reference count per sig.
Each variant `v1`..`v6` swaps exactly one component. `sig-node` and
`sig-network` were run first per the harness's fixed sig order
(cheapest/highest-signal feedback first — see `lib.sh`); the remaining sigs
were not yet run against any variant.

## How to read/reproduce this

```bash
bash scripts/k0s-diff/results-diff.sh summarize results/<vN>/<sig>   # one cell
bash scripts/k0s-diff/results-diff.sh grid                           # full table
bash scripts/k0s-diff/results-diff.sh <vN> v0 <sig>                  # regressions vs baseline
bash scripts/k0s-diff/run-matrix.sh                                  # full sweep (HOURS — see its header)
```

## Variant x component x result

| Variant | Swapped component      | Convergence            | sig-node    | sig-network       | Notes |
|---------|-------------------------|-------------------------|-------------|--------------------|-------|
| v0      | (none — baseline)       | N/A (stock k0s)         | 105/105 ✅  | 47/47 ✅           | Reference counts for every other row. |
| v1      | api-server              | ❌ node never Ready     | not run     | not run             | rusternetes' etcd-v3 client cannot drive k0s's `kine` datastore — every list/get/put fails immediately after a lazily-successful connect. |
| v2      | kubelet                 | ❌ CrashLoopBackOff     | not run     | not run             | `ApiClient` has no client-cert/mTLS auth path; k0s api-server requires it for the kubelet's identity. Cross-cutting — the same wall blocks v3/v4. |
| v3      | scheduler               | ❌ expected 401         | not run     | not run             | Same `ApiClient` mTLS gap as v2 (shim + staging + wiring verified correct; the 401 is the documented, not re-diagnosed, expected outcome). |
| v4      | controller-manager      | ❌ expected 401         | not run     | not run             | Same `ApiClient` mTLS gap as v2/v3. |
| v5      | kube-proxy (workload)   | ❌ crashes past auth    | not run     | not run (smoke-fail)| Image built/pushed/pulled fine (in-cluster SA-token auth for a *pod* workload is a different path than v2-v4's static-pod client-cert path, and it worked far enough to get the container running). Crashes creating an iptables chain — no working nft/legacy backend found on the node's Alpine base. Stock Go kube-proxy runs fine on the same node, so this is a real portability gap, not an environment issue. |
| v6      | dns (workload, CoreDNS) | ✅ **CONVERGED**        | not run     | **45/47** ✅❌❌    | First Rusternetes component to run inside a live k8s cluster and pass real conformance tests. 2 failures, both ExternalName Service tests — see below. |

Grid form (from `results-diff.sh grid`, `results/GRID.tsv`; `-` = not run):

```
variant  sig-node        sig-api-machinery  sig-apps  sig-storage  sig-network        sig-auth  sig-scheduling  sig-cli  sig-instrumentation
v0       PASS 105/FAIL 0  -                  -         -            PASS 47/FAIL 0      -         -               -        -
v1       -                -                  -         -            -                   -         -               -        -
v2       -                -                  -         -            -                   -         -               -        -
v3       -                -                  -         -            -                   -         -               -        -
v4       -                -                  -         -            -                   -         -               -        -
v5       -                -                  -         -            -                   -         -               -        -
v6       -                -                  -         -            PASS 45/FAIL 2      -         -               -        -
```

## v0 baselines

- `sig-node`: **105/105 pass, 0 fail** against `registry.k8s.io/conformance:v1.35.5`.
- `sig-network`: **47/47 pass, 0 fail** against the same image.

Both are the reference every other variant's same-sig run is diffed against.

## v6 (rusternetes-dns): the harness's headline result

v6 replaces k0s's kube-system CoreDNS Deployment with `rusternetes-dns`,
same name/namespace/selector so the `kube-dns` Service (pinned at
`10.96.0.10`) keeps routing to it. It converged and ran real conformance:
**45 of 47 `sig-network` Conformance tests pass**, with genuine in-cluster
behavior (DNS answering real queries over its own SA-token-authenticated API
watches — not a stub). Its `ApiClient` bearer-token in-cluster auth is
accepted by k0s's api-server under Node/RBAC, which is notable because that
same client-cert/mTLS wall blocks v1-v4 entirely (see above) — the pod-SA-
token auth path rusternetes-dns uses is a different, and evidently working,
code path from the static-pod client-cert path the other components need.

The 2 failures are both `[sig-network] Services` Conformance tests, absent
from the v0 baseline (which passes all 47):

```
REGRESSED in v6: [It] [sig-network] Services should be able to change the type from ClusterIP to ExternalName [Conformance]
REGRESSED in v6: [It] [sig-network] Services should be able to change the type from NodePort to ExternalName [Conformance]
```

Both are ExternalName-Service resolution tests — i.e. cases where a Service's
`spec.type` becomes `ExternalName` and DNS is expected to resolve the
Service name to the external CNAME target rather than a ClusterIP. This
points at a gap in `rusternetes-dns`'s handling of `ExternalName` Services
specifically (filed as issue #1580 — see below).

## Filed issues

Every capability gap this harness surfaced was filed as a GitHub issue on
`indyjonesnl/rusternetes` (component-scoped) or, where the gap was in the
adjacent CRI runtime rather than Rusternetes itself, on `containerd-rs`:

| Issue | Component | Gap |
|-------|-----------|-----|
| #1573 | api-server / storage | etcd-v3 client cannot drive k0s's `kine` datastore (every op fails after a lazily-successful connect) — the v1 blocker. |
| #1574 | api-server / storage | No unix-socket etcd client transport (kine's kine-over-etcd-v3 endpoint is a unix socket). |
| #1575 | api-server | No external ServiceAccount-token validation path. |
| #1576 | api-server | No RBAC bootstrap / `system:masters` equivalent. |
| #1577 | api-server | No front-proxy / admission / egress-selector support. |
| #1578 | kubelet / scheduler / controller-manager | `ApiClient` has no client-cert/mTLS auth — blocks v2, v3, and v4 identically. |
| #1579 | kubelet | `KubeletConfiguration` `Duration` fields don't parse k0s's config format. |
| #1580 | dns | `rusternetes-dns` fails 2 `sig-network` Conformance tests: ExternalName Service type-change resolution (ClusterIP→ExternalName, NodePort→ExternalName). |
| #1581 | kube-proxy | `rusternetes-kube-proxy` crashes creating an iptables chain on the node's Alpine base (no nft/legacy backend found); stock Go kube-proxy works fine on the same node. |
| containerd-rs#39 | containerd-rs (CRI runtime) | No local-image ingest path (no `ctr`/`crictl`-style load, predates the insecure-registry env feature, TLS-only pull defaults) — blocked v5/v6 workload swaps until a newer musl build was baked in as a harness-scoped workaround. |

## Harness mechanics worth knowing when re-running this

- **Baked binary swaps (v1-v4):** k0s re-extracts `/var/lib/k0s/bin/<name>`
  from its embedded assets unless the on-disk file's mtime matches the k0s
  executable's mtime AND its size matches the embedded original exactly
  (`pkg/assets/stage.go`, mtime+size check — NOT a content hash). The harness
  exploits this: it stages a same-size shim, `touch -r`s it to the k0s
  executable's mtime, and k0s's own staging pass then reuses it verbatim on
  every supervisor restart.
- **Workload swaps (v5 kube-proxy / v6 dns):** these replace an in-cluster
  DaemonSet/Deployment via `kubectl` instead, and need a way to get a
  locally-built Rusternetes pod image into the node's containerd-rs image
  store — see the containerd-rs#39 note above for why that needed a
  harness-local workaround (a throwaway HTTP registry + a newer
  containerd-rs baked in just for those two variants).
- **Clean-checkout prerequisites:** v1-v4 are self-contained
  (`run-variant.sh` invokes `build-swap-binaries.sh` to cross-build the musl
  binary + extract the genuine k0s binary). v5/v6 additionally require the
  `kube-proxy`/`rusternetes-dns` musl binaries pre-built in the shared cargo
  target dir AND an adjacent `../containerd-rs` checkout built static-musl
  with insecure-registry support. Both are gated with explicit error
  messages, but v5/v6 do NOT run from a rusternetes-only clone.
- **Sequential only:** every variant publishes the same host port (26444 for
  the k0s admin API), so exactly one variant's compose stack can be up at a
  time. `run-matrix.sh` tears down each variant (`compose down -v`) before
  bringing up the next.
- **Smoke gate:** `run-variant.sh` runs a smoke check before spending time on
  a full conformance sig; on smoke failure it records a `{"smoke":"fail"}`
  marker and skips conformance for that variant/sig pair rather than burning
  a full Hydrophone run against a cluster that's already known-broken.
