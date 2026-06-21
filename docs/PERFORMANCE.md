# Rusternetes footprint & performance

The project thesis — *"k3s without melting your laptop"* — rests on footprint
numbers. This document is the reproducible methodology and the recorded results.
Numbers are produced by [`scripts/footprint-benchmark.sh`](../scripts/footprint-benchmark.sh)
(#1038) so anyone can re-run them head-to-head against k3s/k0s/microk8s on their
own hardware.

> **Status:** the harness is in place and records the all-in-one binary's own
> footprint. The head-to-head columns against k3s are filled in by running the
> same harness against a k3s binary on identical hardware (see
> [Comparing against k3s](#comparing-against-k3s)) — do not quote a competitive
> claim until both halves were measured on the *same* host in the *same* run.

## What is measured

Four numbers, all for the **all-in-one** control plane (api-server + scheduler +
controller-manager + kubelet + kube-proxy + dns in one process, default SQLite
backend) on a single node with default CNI + DNS:

1. **Binary size** — the release artifact, raw and `strip`ped. Also build the
   musl static variant for the distribution number (see below).
2. **Time-to-cluster** — wall-clock from process start to the API server's
   `/readyz` returning `200`.
3. **Idle RSS** — steady-state `VmRSS` of the idle control plane (min/avg/max
   over the sample window).
4. **Idle CPU %** — steady-state whole-process CPU over the sample window
   (all tokio worker threads), from `/proc/<pid>/stat`.

## Competitive bar

Idle control-plane RSS of the incumbents (single node, defaults), for context:

| Distro | Idle RSS |
| --- | --- |
| k3s | ~535–750 MB (1–1.2 GB without `GOMEMLIMIT`) |
| k0s | ~658 MB |
| microk8s (HA off) | ~526 MB |
| OS baseline | ~167 MB |

**Target to claim the niche:** sub-400 MB idle (stretch 250–300 MB).

> **Risk (measure before claiming):** the etcd client, smoltcp, and the full
> tokio stack are compiled into the all-in-one unconditionally; the current
> footprint may not yet beat k3s. The whole point of this harness is to replace
> assumption with a number.

## How to run

```bash
# 1. build the release all-in-one (glibc)
cargo build --release -p rusternetes

# 2. run the harness (boots a throwaway cluster, samples, tears down)
scripts/footprint-benchmark.sh

# longer windows for a steadier idle reading
scripts/footprint-benchmark.sh --settle 20 --seconds 60
```

The harness honors `CARGO_TARGET_DIR` (shared-target-dir checkouts) for the
default binary path. It boots into a `mktemp` data dir and removes it on exit
(`--keep-data` to retain). The all-in-one serves plain HTTP on `:6443` by
default (TLS is opt-in via `--tls`), so readiness is probed over HTTP; override
with `--ready-url` for a TLS or non-default bind.

### musl static variant (distribution size)

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release -p rusternetes --target x86_64-unknown-linux-musl --features mimalloc
scripts/footprint-benchmark.sh \
  --binary target/x86_64-unknown-linux-musl/release/rusternetes
```

mimalloc is linked via the `mimalloc` feature (#1041): musl's default allocator
is ~10× slower under multi-threaded lock contention, so the musl footprint
number should always be taken with mimalloc on.

### Comparing against k3s

Point the same harness at a k3s binary on the same host so the numbers are
directly comparable:

```bash
scripts/footprint-benchmark.sh --label "k3s" \
  --binary "$(command -v k3s)" \
  --args "server --disable=traefik --disable=servicelb" \
  --ready-url "http://127.0.0.1:6443/readyz" \
  --settle 30 --seconds 60
```

(k3s serves `/readyz` on its supervisor/apiserver port; adjust `--ready-url` to
your kubeconfig's server address. k3s needs root for its networking; run it the
way you normally would and pass `--ready-url` accordingly.)

## Results

> Re-generate this section with `scripts/footprint-benchmark.sh` and paste its
> Markdown table here, recording the host and date. The harness prints a
> ready-to-paste `## Footprint — <label>` block.

_No head-to-head run recorded yet — populate from a single-host run against k3s._
