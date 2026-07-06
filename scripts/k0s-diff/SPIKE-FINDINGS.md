# k0s Differential Conformance Harness — Task 1 SPIKE findings

**Status: v0 baseline is LIVE and passes the smoke gate.**
Node `Ready`, all 5 kube-system pods `Ready`, `smoke.sh` exits 0.
CRI reported by the kubelet: `containerd-rs://0.1.3`. OCI runtime: `crun 1.28`.
k8s: `v1.35.5+k0s` (all-Go control plane).

```
NAMESPACE     NAME                             READY   STATUS    RESTARTS
kube-system   coredns-5f6cf6fb46-h9pbx         1/1     Running   0
kube-system   konnectivity-agent-rgzzp         1/1     Running   0
kube-system   kube-proxy-gtmw9                 1/1     Running   0
kube-system   kube-router-9j58l                1/1     Running   0
kube-system   metrics-server-df68c566c-jrqfp   1/1     Running   0
```

---

## v0 topology (what actually runs)

ONE privileged `k0s` container (kind-style), image `k0s-diff-v0-node:v1.35.5`,
built from `Dockerfile.k0s-node` = stock `k0sproject/k0s:v1.35.5-k0s.0`
+ baked-in `containerd-rs` (Rust CRI, GHCR `v0.1.3`, static musl), `crun 1.28`,
`iproute2`, and the standard CNI plugins in `/opt/cni/bin`.

The compose entrypoint starts `containerd-rs` in the background, waits for its
socket, then `exec k0s controller --config=/etc/k0s/k0s.yaml --enable-worker
--no-taints --cri-socket remote:unix:///run/containerd-rs.sock`. k0s cedes the
CRI to containerd-rs and launches **no containerd of its own**.

- **Datastore:** embedded etcd (all-Go). apiserver `--etcd-servers=https://127.0.0.1:2379`.
- **CNI:** k0s built-in `kuberouter` provider (upstream **Go** kube-router, CNI-only). Pod IPs land in `10.244.0.0/16`.
- **Service proxy:** upstream **Go** `kube-proxy`, ENABLED (kube-router runs CNI only; kube-proxy is a swap target in a later variant, so the baseline must run it).
- **CRI socket:** `/run/containerd-rs.sock` (containerd-rs config `/etc/containerd-rs.toml`).

### Deviations from the brief (justified)

1. **Single baked container, NOT the brief's two-service (k0s + containerd-rs)
   compose.** The CRI runtime and the kubelet must share a filesystem and
   mount/pid namespaces — the kubelet references pod bundles, netns and mounts
   that containerd-rs/crun create; a separate compose service cannot see them.
   Every prior working k0s+containerd-rs stack in this monorepo
   (`rustified-kubernetes-stack/`, `rusternetes-m1/`) bakes the runtime into the
   node image for this reason. This is the "simplest wiring that works" the task
   ambiguity note allows.
2. **`kuberouter` CNI, NOT `flannel/vxlan` from the repo `k0s-config.yaml`.**
   k0s v1.35 rejects `flannel`: `provider: Unsupported value: "flannel":
   supported values: "kuberouter", "calico", "custom"`. The repo
   `k0s-config.yaml` is invalid for this k0s version. `kuberouter` is k0s's
   default all-Go CNI and (unlike the `kube-router-rs` prior art) keeps
   kube-proxy enabled — required so kube-proxy stays a swappable component.

---

## CRUX 1 — binary staging: does k0s re-extract `/var/lib/k0s/bin`? YES, unconditionally.

**Verdict: k0s re-stages its supervised binaries from embedded assets before
EVERY launch — on k0s-process start AND on supervisor restart of an individual
component. The trigger is content-based (not just size); a running binary is
additionally mmap-locked. Neither runtime bind-mount nor pre-staged file
replacement survives.**

The k0s-supervised binaries live in `/var/lib/k0s/bin/` (no stamp/marker files
in that dir): `etcd`, `konnectivity-server`, `kube-apiserver`,
`kube-controller-manager`, `kube-scheduler`, `kubelet` (+ iptables symlinks).

### Evidence

**Exp A — overwrite while running:** `echo BROKEN > kube-scheduler` →
`sh: can't create ...: Text file busy`. The live binary is mmap-locked; you
cannot overwrite it in place (so a runtime bind-mount over a running file is
impossible too).

**Exp B — unlink+replace (6-byte garbage), then `docker compose restart k0s`:**
before restart the on-disk file was 6 bytes `BROKEN`; after restart it was the
original **47 751 352-byte ELF**, `sha256 0d6789cf…b108a145`, timestamp reverted
to the embed time (`Jun 16 10:20`). → **re-extracted on k0s start.**

**Exp C — kill just the scheduler process, replace binary while dead, let the
supervisor restart it:** replaced with 10-byte `BROKENSHIM`; ~15 s later the
supervisor had restored the exact ELF (`sha256 0d6789cf…`) and the scheduler was
running again (`Successfully acquired lease "kube-system/kube-scheduler"`). →
**re-extracted on supervisor component-restart, not only on k0s start.**

**Exp D — same-SIZE decoy (size-only loophole test):** replaced with a
`47 751 352`-byte all-zero file (byte-identical size to the original), killed the
process. After supervisor restart the first bytes were `7f 45 4c 46` (ELF), i.e.
restored. → **re-staging is content-based; matching the size does NOT prevent
it.** No size-padding loophole.

There is **no CLI flag or env var to disable asset extraction** (`k0s controller
--help` exposes only `--data-dir` to relocate the whole tree).

### Chosen override mechanism for later variants (v1–v6)

The swap point differs by component class — this is the single most important
output for the downstream shim tasks:

| Variant | Component | Where it runs in v0 | Swappable by |
|--------|-----------|---------------------|--------------|
| v1 | kube-apiserver | `/var/lib/k0s/bin/kube-apiserver` (staged, re-extracted) | **baked custom image** — see below |
| v2 | kubelet | `/var/lib/k0s/bin/kubelet` (staged, re-extracted) | **baked custom image** |
| v3 | kube-scheduler | `/var/lib/k0s/bin/kube-scheduler` (staged, re-extracted) | **baked custom image** |
| v4 | kube-controller-manager | `/var/lib/k0s/bin/kube-controller-manager` (staged, re-extracted) | **baked custom image** |
| v5 | kube-proxy | `/usr/local/bin/kube-proxy` **inside a DaemonSet pod** (NOT k0s-staged) | **workload swap** — replace the DaemonSet image/manifest via kubectl |
| v6 | dns (coredns) | coredns **Deployment** (NOT k0s-staged) | **workload swap** — replace the Deployment image/manifest via kubectl |

**Recommendation — baked custom image per variant (v1–v4).** Because k0s
re-extracts staged binaries from its *embedded* assets before every launch, the
only reliable place to inject a Rusternetes replacement is to defeat that
extraction, and the robust way is a per-variant node image. Two candidate
implementations for the swap task to pick between (both need one confirmatory
experiment, tracked below):

- **(preferred) Interpose at the exec, not the file.** Wrap so k0s launches the
  Rusternetes binary even though it re-extracts the Go one — e.g. bake the
  Rusternetes component elsewhere and make `k0s`'s supervised path resolve to
  it. The Rusternetes components run in "API mode" via `--kubeconfig` against
  the k0s api-server (kubeconfigs enumerated below), so the shim needs the
  component's kubeconfig + the config file k0s generated (paths below), not the
  raw Go argv verbatim in all cases.
- **(fallback) External-process topology.** Run the Rusternetes component as a
  separate process/container against the k0s api-server and neutralize k0s's own
  copy. k0s has no per-component disable flag, so this only cleanly applies to
  api-server (v1: run Rusternetes api-server externally, point k0s workers at it,
  the way the prior stacks point k0s at external rhino/etcd).

**Open question for the swap task (must confirm before writing v1–v4):** whether
k0s re-extracts on the SAME PATH it then execs (it does) and whether an exec
interposition (e.g. wrapper on PATH, or replacing the embedded asset in a
rebuilt k0s) is accepted by the supervisor. Runtime bind-mount and pre-staging
are already ruled OUT by Exp A–D. containerd-rs itself sits OUTSIDE
`/var/lib/k0s/bin` (external CRI k0s never manages), so baking it — as v0 does —
is stable and unaffected.

---

## CRUX 2 — verbatim launch argv (captured from `/proc/*/cmdline`)

Raw, unedited (single space between args; trailing space as emitted).

### kube-apiserver
```
/var/lib/k0s/bin/kube-apiserver --service-account-signing-key-file=/var/lib/k0s/pki/sa.key --allow-privileged=true --requestheader-username-headers=X-Remote-User --kubelet-client-key=/var/lib/k0s/pki/apiserver-kubelet-client.key --service-account-key-file=/var/lib/k0s/pki/sa.pub --kubelet-certificate-authority=/var/lib/k0s/pki/ca.crt --egress-selector-config-file=/var/lib/k0s/konnectivity.conf --requestheader-extra-headers-prefix=X-Remote-Extra- --service-account-jwks-uri=https://kubernetes.default.svc/openid/v1/jwks --advertise-address=192.168.80.2 --feature-gates= --service-account-issuer=https://kubernetes.default.svc --kubelet-client-certificate=/var/lib/k0s/pki/apiserver-kubelet-client.crt --proxy-client-cert-file=/var/lib/k0s/pki/front-proxy-client.crt --anonymous-auth=false --profiling=false --tls-cert-file=/var/lib/k0s/pki/server.crt --v=1 --tls-min-version=VersionTLS12 --authorization-mode=Node,RBAC --secure-port=6443 --requestheader-client-ca-file=/var/lib/k0s/pki/front-proxy-ca.crt --requestheader-group-headers=X-Remote-Group --api-audiences=https://kubernetes.default.svc,system:konnectivity-server --enable-bootstrap-token-auth=true --kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname --requestheader-allowed-names=front-proxy-client --service-cluster-ip-range=10.96.0.0/12 --tls-private-key-file=/var/lib/k0s/pki/server.key --tls-cipher-suites=TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 --enable-admission-plugins=NodeRestriction --client-ca-file=/var/lib/k0s/pki/ca.crt --proxy-client-key-file=/var/lib/k0s/pki/front-proxy-client.key --etcd-servers=https://127.0.0.1:2379 --etcd-cafile=/var/lib/k0s/pki/etcd/ca.crt --etcd-certfile=/var/lib/k0s/pki/apiserver-etcd-client.crt --etcd-keyfile=/var/lib/k0s/pki/apiserver-etcd-client.key
```

### kubelet
```
/var/lib/k0s/bin/kubelet --node-labels=node.k0sproject.io/role=control-plane --runtime-cgroups=/system.slice/containerd.service --hostname-override=k0s-diff-v0 --root-dir=/var/lib/k0s/kubelet --config=/run/k0s/kubelet/config.yaml --kubeconfig=/run/k0s/kubelet-direct.conf --v=1 --cert-dir=/var/lib/k0s/kubelet/pki
```
Note: most kubelet tuning is in the generated `--config` file
`/run/k0s/kubelet/config.yaml` (KubeletConfiguration), not on the argv. The v2
kubelet shim will need that file's contents too. `--runtime-cgroups` names
`containerd.service` even though the runtime is containerd-rs (cosmetic; k0s
default string).

### kube-scheduler
```
/var/lib/k0s/bin/kube-scheduler --profiling=false --authentication-kubeconfig=/var/lib/k0s/pki/scheduler.conf --authorization-kubeconfig=/var/lib/k0s/pki/scheduler.conf --kubeconfig=/var/lib/k0s/pki/scheduler.conf --v=1 --feature-gates= --bind-address=127.0.0.1 --leader-elect=true
```

### kube-controller-manager
```
/var/lib/k0s/bin/kube-controller-manager --feature-gates= --profiling=false --terminated-pod-gc-threshold=12500 --authorization-kubeconfig=/var/lib/k0s/pki/ccm.conf --kubeconfig=/var/lib/k0s/pki/ccm.conf --cluster-signing-key-file=/var/lib/k0s/pki/ca.key --node-cidr-mask-size=24 --bind-address=127.0.0.1 --service-account-private-key-file=/var/lib/k0s/pki/sa.key --service-cluster-ip-range=10.96.0.0/12 --use-service-account-credentials=true --allocate-node-cidrs=true --cluster-name=k0s --controllers=*,bootstrapsigner,tokencleaner --leader-elect=true --client-ca-file=/var/lib/k0s/pki/ca.crt --cluster-signing-cert-file=/var/lib/k0s/pki/ca.crt --cluster-cidr=10.244.0.0/16 --v=1 --authentication-kubeconfig=/var/lib/k0s/pki/ccm.conf --requestheader-client-ca-file=/var/lib/k0s/pki/front-proxy-ca.crt --root-ca-file=/var/lib/k0s/pki/ca.crt
```

### kube-proxy
```
/usr/local/bin/kube-proxy --config=/var/lib/kube-proxy/config.conf --hostname-override=k0s-diff-v0
```
kube-proxy runs as a **DaemonSet pod** (binary from the kube-proxy pod image at
`/usr/local/bin/kube-proxy`), NOT a k0s-staged binary. Its tuning is in the
`KubeProxyConfiguration` at `/var/lib/kube-proxy/config.conf` (inside the pod).
The v5 swap replaces the DaemonSet, not a `/var/lib/k0s/bin` binary.

---

## CRUX 3 — admin kubeconfig

Inside the container: **`/var/lib/k0s/pki/admin.conf`** (`-rw------- root:root`,
also exported as `ENV KUBECONFIG` in the stock k0s image). `k0s kubeconfig admin`
prints an equivalent kubeconfig on stdout.

Extract to the host (server rewritten to the published port `26443`):
```
docker exec k0s-diff-v0 k0s kubeconfig admin > /tmp/k0s-diff.kubeconfig
sed -i 's#server: https://.*:6443#server: https://127.0.0.1:26443#' /tmp/k0s-diff.kubeconfig
```

Per-component kubeconfigs the API-mode shims (v1–v4) pass as `--kubeconfig`
(inside the container):
- scheduler: `/var/lib/k0s/pki/scheduler.conf`
- controller-manager: `/var/lib/k0s/pki/ccm.conf`
- kubelet: `/run/k0s/kubelet-direct.conf` (+ config `/run/k0s/kubelet/config.yaml`)
- api-server: serves on `:6443`, talks to etcd on `127.0.0.1:2379` (no kubeconfig; it IS the API)

---

## Reproduce v0 from scratch

```bash
cd scripts/k0s-diff
export CONTAINER_RUNTIME=docker
docker compose -f compose.k0s.template.yml up -d --build     # ~1 min after image cached
# wait ~100s for control plane + CNI
docker exec k0s-diff-v0 k0s kubeconfig admin > /tmp/k0s-diff.kubeconfig
sed -i 's#server: https://.*:6443#server: https://127.0.0.1:26443#' /tmp/k0s-diff.kubeconfig
KUBECONFIG=/tmp/k0s-diff.kubeconfig bash smoke.sh            # exit 0 = healthy
docker compose -f compose.k0s.template.yml down -v           # teardown
```

`smoke.sh` applies an **idempotent coredns Corefile repair** first: stock k0s
CoreDNS forwards to `/etc/resolv.conf`, which loops on this nftables dev host
(the `loop` plugin FATALs → CrashLoop). The repair rewrites the forward to
`8.8.8.8 1.1.1.1`. Deterministic on this box; a no-op where already fixed. The
patch persists across restarts (k0s does not revert it).

---

## Follow-ups for the swap tasks (do not lose these)

1. **Confirm the v1–v4 injection mechanism** (baked image: exec-interposition vs
   rebuilt-embedded-asset vs external-process). Runtime bind-mount and
   pre-staging are ruled out (Crux 1 Exp A–D). This is the gating unknown for v1.
2. **Capture the kubelet `KubeletConfiguration`** (`/run/k0s/kubelet/config.yaml`)
   and **kube-proxy `KubeProxyConfiguration`** (`/var/lib/kube-proxy/config.conf`)
   verbatim — the argv alone under-specifies v2/v5.
3. **coredns repair should move into a bring-up/bootstrap step** shared by all
   variants (currently inlined in smoke.sh); ensure the v6 dns swap accounts for
   it.
