#!/usr/bin/env bash
# scripts/k0s-diff/run-variant.sh <vN> <sig>
#
# Bring up one variant's compose stack, obtain its admin kubeconfig, run the
# smoke gate, and — only if smoke passes — run ONE conformance sig via
# Hydrophone. Writes:
#   results/<vN>/<sig>/          raw Hydrophone output (e2e.log, junit_*.xml)
#   results/<vN>/<sig>.json      one-line summary {pass,fail,image,...}
# and prints "PASS n / FAIL m" to stdout.
#
# On smoke failure: writes {"smoke":"fail"} to results/<vN>/<sig>.json and
# exits 0 WITHOUT running conformance (the gate).
#
# NOTE: pass/fail counting is delegated to parse-results.py, the same shared
# parser results-diff.sh (Task 7) uses for its grid + regression report — kept
# in one file so this per-run summary and the later cross-variant report never
# diverge.
#
# Env knobs:
#   K0S_DIFF_FOCUS   override the ginkgo --focus regex (e.g. a single test, to
#                    de-risk the wiring before a full sig run).
#   K0S_DIFF_SKIP    ginkgo --skip regex (default empty = run everything).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
source "$here/lib.sh"
require_docker

v="${1:?usage: run-variant.sh <vN> <sig>}"
sig="${2:?usage: run-variant.sh <vN> <sig>}"

# Validate the variant is known.
known=0
for x in "${VARIANTS[@]}"; do [ "$x" = "$v" ] && known=1; done
[ "$known" = 1 ] || { echo "unknown variant '$v' (known: ${VARIANTS[*]})" >&2; exit 1; }

f="$here/compose.k0s-${v}.yml"
[ -f "$f" ] || { echo "no compose file for $v: $f — run gen-compose.sh first" >&2; exit 1; }

# Resolve the swapped component for this variant (SWAP is index-aligned with
# VARIANTS in lib.sh). Baked swaps (v1..v4) need their .build/ inputs produced
# from source before the image build so a clean checkout works end-to-end.
swap=""
for i in "${!VARIANTS[@]}"; do [ "${VARIANTS[$i]}" = "$v" ] && swap="${SWAP[$i]}"; done

resdir="$here/results/${v}"
mkdir -p "$resdir"

# Bring the variant up. Build the image ONLY when it is missing (or when
# K0S_DIFF_REBUILD=1): an unconditional `up --build` re-exports a fresh image
# digest every run, which forces a container recreate and churns the kube-system
# pods (racing the smoke gate). Building-when-absent keeps an already-healthy
# variant reused in place — no recreate.
img="$(awk '/^[[:space:]]*image:/{print $2; exit}' "$f")"
if [ "${K0S_DIFF_REBUILD:-0}" = 1 ] || ! docker image inspect "$img" >/dev/null 2>&1; then
  # Baked component swaps (v1..v4) bake .build/rusternetes-<component> +
  # .build/<k0s-binary>.real; produce them from source first (idempotent).
  # v5 (kube-proxy) / v6 (dns) are workload swaps — same base image, no bake.
  case "$swap" in
    api-server|kubelet|scheduler|controller-manager)
      log "producing swap binaries for '$swap' (build-swap-binaries.sh)"
      bash "$here/build-swap-binaries.sh" "$swap"
      ;;
  esac
  log "building image $img"
  docker compose -f "$f" build
fi

# --- v5/v6 workload-swap pre-up: local registry + pushed pod image ------------
# containerd-rs has NO local-image load path (containerd-rs.toml) — it can only
# PULL. So for the workload swaps we stand up a throwaway HTTP registry on the
# k0s-diff-net gateway (the host address the in-container containerd-rs reaches),
# push the rusternetes-<component> image there, and tell containerd-rs to pull it
# over HTTP via CONTAINERD_RS_INSECURE_REGISTRIES (set BEFORE `up` so the daemon
# starts trusting it). apply-workload-swap.sh later references the image by this
# same registry host:port.
REGISTRY_HOSTPORT=""
case "$swap" in
  kube-proxy|dns)
    # The pinned v0.1.3 runtime baked in the node image cannot pull a local
    # registry (no load path; predates CONTAINERD_RS_INSECURE_REGISTRIES; oci
    # defaults = HTTPS+verify). Bake a newer static-musl containerd-rs that
    # carries the env feature so containerd-rs pulls the pushed image over HTTP.
    # Path overridable via K0S_DIFF_CONTAINERD_RS_MUSL.
    crs_musl="${K0S_DIFF_CONTAINERD_RS_MUSL:-$HOME/.cache/containerd-rs-target/x86_64-unknown-linux-musl/release/containerd-rs}"
    if [ ! -f "$crs_musl" ]; then
      echo "workload swaps need a newer containerd-rs (env-feature, static-musl) at:" >&2
      echo "  $crs_musl" >&2
      echo "build it: (cd ../../../containerd-rs && CARGO_TARGET_DIR=\$HOME/.cache/containerd-rs-target cargo build --release --target x86_64-unknown-linux-musl -p containerd-rs)" >&2
      exit 1
    fi
    # NB: grep -c (not -q) — a -q closes the pipe on first match, `strings` then
    # dies with SIGPIPE, and `set -o pipefail` turns that into a false negative.
    crs_feat="$(strings "$crs_musl" 2>/dev/null | grep -c CONTAINERD_RS_INSECURE_REGISTRIES || true)"
    if [ "${crs_feat:-0}" -eq 0 ]; then
      echo "containerd-rs at $crs_musl lacks CONTAINERD_RS_INSECURE_REGISTRIES support — rebuild from a source that has it" >&2
      exit 1
    fi
    install -m0755 "$crs_musl" "$here/.build/containerd-rs"
    log "baking newer containerd-rs (env-feature) into the $v node image"
    docker compose -f "$f" build --build-arg CONTAINERD_RS_BINARY=.build/containerd-rs

    # Local throwaway HTTP registry, host-published on :5000 (independent of the
    # k0s-diff-net). Push via localhost:5000 — docker auto-trusts localhost as an
    # insecure registry, so no daemon config is needed.
    if ! docker ps --format '{{.Names}}' | grep -qx k0s-diff-registry; then
      docker rm -f k0s-diff-registry >/dev/null 2>&1 || true
      log "starting local registry k0s-diff-registry on :5000"
      docker run -d --name k0s-diff-registry --restart unless-stopped -p 5000:5000 registry:2 >/dev/null
      for _ in $(seq 1 30); do curl -fsS "http://localhost:5000/v2/" >/dev/null 2>&1 && break; sleep 1; done
    fi
    bash "$here/build-swap-images.sh" "$swap" >/dev/null
    tag="${K8S_VERSION#v}"
    push_img="localhost:5000/rusternetes-${swap}:${tag}"
    docker tag "k0s-diff-rusternetes-${swap}:${tag}" "$push_img"
    log "pushing $push_img"
    docker push "$push_img" >/dev/null

    # The k0s-diff-net gateway is PINNED (lib.sh / compose template), so we know
    # the host address the in-container containerd-rs reaches the host-published
    # registry through BEFORE `up` — no recreate needed. Export the insecure-
    # registry env so the single `up` below starts containerd-rs already trusting
    # the registry over HTTP (same repo, pulled by the gateway ref).
    REGISTRY_HOSTPORT="${K0S_DIFF_NET_GATEWAY}:5000"
    export CONTAINERD_RS_INSECURE_REGISTRIES="$REGISTRY_HOSTPORT"
    log "insecure registry for containerd-rs: $REGISTRY_HOSTPORT (image ref ${REGISTRY_HOSTPORT}/rusternetes-${swap}:${tag})"
    ;;
esac

log "bringing up $v from $(basename "$f")"
docker compose -f "$f" up -d

# --- kubeconfig from the container, server rewritten to the published port ----
cname="k0s-diff-${v}"
kc="/tmp/k0s-diff-${v}.kubeconfig"
log "waiting for k0s admin kubeconfig from $cname"
for _ in $(seq 1 60); do
  if docker exec "$cname" k0s kubeconfig admin > "$kc" 2>/dev/null && [ -s "$kc" ]; then
    break
  fi
  sleep 2
done
[ -s "$kc" ] || { echo "could not obtain kubeconfig from $cname" >&2; exit 1; }
sed -i 's#server: https://.*:6443#server: https://127.0.0.1:26444#' "$kc"
export KUBECONFIG="$kc"

# --- readiness pre-gate: wait for the node to register AND kube-system pods to
# be created before invoking smoke. `k0s kubeconfig admin` succeeds as soon as
# the api-server answers — which is BEFORE the worker registers and the addon
# controllers create the kube-system pods. smoke.sh's `kubectl wait --all` errors
# ("no matching resources found") on an empty set, so gate on non-empty first.
log "waiting for node + kube-system pods to appear"
for _ in $(seq 1 90); do
  n="$(kubectl get nodes --no-headers 2>/dev/null | wc -l)"
  p="$(kubectl -n kube-system get pods --no-headers 2>/dev/null | wc -l)"
  [ "${n:-0}" -ge 1 ] && [ "${p:-0}" -ge 1 ] && break
  sleep 5
done

# --- smoke gate ---------------------------------------------------------------
if ! bash "$here/smoke.sh"; then
  log "SMOKE-FAIL $v — writing marker, skipping conformance"
  printf '{"variant":"%s","sig":"%s","smoke":"fail"}\n' "$v" "$sig" > "$resdir/${sig}.json"
  exit 0
fi

# --- workload swaps (v5 kube-proxy / v6 dns): apply AFTER smoke -------------
# Unlike the baked binary swaps (v1..v4), v5/v6 replace an in-cluster workload
# (DaemonSet/Deployment) with a Rusternetes pod image. The all-Go stack must be
# healthy (smoke passed) first; only then do we swap the component and re-verify.
# NB: the swap itself runs here, AFTER smoke. What must precede the compose
# `up` above is the registry/env staging (CONTAINERD_RS_INSECURE_REGISTRIES
# pointing at the local registry) so containerd-rs can pull the swap image —
# that is staged in a pre-up hook (see the swap!=baked branch near `up -d`).
case "$swap" in
  kube-proxy|dns)
    if ! bash "$here/apply-workload-swap.sh" "$v" "$swap" "$REGISTRY_HOSTPORT"; then
      log "WORKLOAD-SWAP-FAIL $v ($swap) — writing marker, skipping conformance"
      printf '{"variant":"%s","sig":"%s","swap":"%s","workloadSwap":"fail"}\n' \
        "$v" "$sig" "$swap" > "$resdir/${sig}.json"
      exit 0
    fi
    ;;
esac

# --- conformance image MUST match the running server version exactly ----------
gitv="$(kubectl get --raw /version | python3 -c 'import json,sys;print(json.load(sys.stdin)["gitVersion"])')"
img_tag="${gitv%%+*}"                       # v1.35.5+k0s -> v1.35.5
conf_image="registry.k8s.io/conformance:${img_tag}"
log "server $gitv -> conformance image $conf_image"

# --- one sig's Conformance tests ----------------------------------------------
sig_short="${sig#sig-}"
focus="${K0S_DIFF_FOCUS:-\\[sig-${sig_short}\\].*\\[Conformance\\]}"
skip="${K0S_DIFF_SKIP:-}"
outdir="$resdir/${sig}"
mkdir -p "$outdir"

# NB: hydrophone treats --conformance and --focus as mutually exclusive, so we
# drive the run with --focus only; the regex carries \[Conformance\] itself.
hydro_args=(--conformance-image "$conf_image"
  --focus "$focus"
  --kubeconfig "$KUBECONFIG"
  --output-dir "$outdir")
[ -n "$skip" ] && hydro_args+=(--skip "$skip")

log "hydrophone --focus '$focus' ${skip:+--skip '$skip'}"
hydrophone "${hydro_args[@]}" || true

# --- pass/fail count (shared parser — see parse-results.py) -------------------
read -r pass fail < <(python3 "$here/parse-results.py" summarize "$outdir")
pass="${pass:-0}"; fail="${fail:-0}"

printf '{"variant":"%s","sig":"%s","image":"%s","serverVersion":"%s","pass":%s,"fail":%s}\n' \
  "$v" "$sig" "$conf_image" "$gitv" "$pass" "$fail" > "$resdir/${sig}.json"

echo "PASS $pass / FAIL $fail"
