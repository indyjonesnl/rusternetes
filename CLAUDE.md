# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Upstream-first — port, do NOT reinvent (MANDATORY)

Rusternetes reimplements Kubernetes. **Almost every behavior you need already
exists in a battle-tested upstream implementation.** Before designing ANY
non-trivial logic — an authorizer rule, a bootstrap step, a controller loop, a
validation, a CRI/CNI/CSI call sequence, a default, an error string, a status
transition — you MUST first read how upstream does it and port that, rather than
deriving it yourself.

**This is not optional and it is not "when in doubt." It is the default first
step for every behavioral change.** Deriving logic that already exists upstream
wastes time and tokens and produces subtly-wrong divergences (wrong mechanism,
wrong defaults, broken invariants) that only surface as failing tests later.

### The gate (MUST — this is a precondition, not advice)

**You MUST NOT write new behavioural logic in Rusternetes without first
locating and citing the upstream implementation of that behaviour.** If you
cannot name the upstream file and symbol, you have not finished looking — go
look. "I could not find it" is a search result to report, not a licence to
invent.

This applies to writing code AND to deciding what code should do. It is not
satisfied by knowing how Kubernetes works, by reasoning from a conformance
test, or by inferring the shape from an error message.

**Do not derive what you can read.** `../kubernetes` is a full checkout of the
`release-1.35` branch on this machine — 12,294 Go files, the real
implementation of nearly everything this project reimplements. A `grep` into it
costs seconds. Deriving the same behaviour from first principles costs an
implementation, a review, a CI run, and — when the derivation is subtly wrong —
a revert and a re-measurement. That trade is never worth taking, and it is the
single most expensive habit in this repo.

Reverse the usual order of investigation:

| Instead of | Do this |
|---|---|
| Reading our logs to work out what *should* happen | Read upstream for what should happen; use logs only to confirm what *did* |
| Inferring semantics from a failing conformance spec | Read the spec source for the assertion, then read the upstream code that satisfies it |
| Designing a mechanism and checking it against upstream after | Read upstream first, then write the Rust |
| Experimenting to find the right default/threshold/ordering | Look it up — upstream has a named constant for it |

The work is a **faithful, idiomatic Go→Rust rewrite**, not an independent
re-derivation that happens to agree. Where Rust idiom differs (ownership,
`Result`, async), change the *expression*, never the mechanism or its bounds.

### The rule
1. **Look first.** Grep the reference checkouts for the exact concept BEFORE
   writing code. This is usually a 2-second `grep`/`rg`, not an investigation.
   `../kubernetes` is the `release-1.35` branch (the version this project
   targets); `git -C ../kubernetes fetch` if you need a newer 1.35.x patch. If
   a grep for the obvious name misses, grep for the *constant*, the error
   string, or the flag name — upstream almost always has one.
2. **Port the mechanism, not just the outcome.** If upstream achieves an effect
   via a seeded object, a reconciler, or a post-start hook, replicate *that
   mechanism* — do not fake the effect with a shortcut (e.g. an authorizer
   hard-code). Shortcuts break the invariants the real mechanism preserves.
3. **Port the WHOLE mechanism, not the one function you came for.** Read the
   caller and every gate around it before you copy anything. Upstream functions
   sit inside systems that make them safe; lifting one out ships an accelerator
   with no brake. Ask: what does upstream check *before* calling this? What does
   it do with the error? What bounds it?
4. **Read upstream's TESTS, not just its code.** They encode the edge cases you
   will not derive — ordering constraints, partial-failure semantics, what must
   happen on recreate. When a test exists for something that looks obvious,
   that is usually because it is not.
5. **A server-side change usually has a client-side counterpart.** If you add a
   rejection, a condition, or a new error, grep for who upstream expects to
   consume it. Shipping one half of a pair is worse than shipping neither: the
   new behaviour becomes reachable with nothing prepared for it.
6. **Check whether this repo already implements it** before writing your own.
   `grep` the other controllers first — a sibling has often solved the same
   problem, and a second copy is a future divergence.
7. **Cite it.** In the code comment and the commit/PR, name the upstream file
   (and ideally the symbol/line) you ported from. Reviewers verify against it.
8. **Only invent when upstream genuinely has no equivalent** (a Rust-specific
   concern, a Rusternetes-only feature). Say so explicitly when you do — and if
   you deviate deliberately, say that too, with the reason.

### Worked example — a value looked up, a topology not

The clearest way this rule gets missed is a *partial* lookup: reading upstream
for the number and not for the structure the number lives in.

`#1856` added client-side rate limiting. Upstream's controller-manager default
was looked up correctly — `ClientConnection.QPS = 20`
(`pkg/controller/apis/config/v1alpha1/defaults.go:59`) — and applied to the one
`ApiClient` all 31 of our controllers share.

Upstream does not share one client. It clones the config per controller:

```text
// ClientConfig is a skeleton config to clone and use as the basis for each controller client
clientConfig := *b.ClientConfig
```

(`staging/src/k8s.io/controller-manager/pkg/clientbuilder/client_builder.go:40-47`)
and `RESTClientFor` builds a **fresh limiter for every client** that does not
carry one (`staging/src/k8s.io/client-go/rest/config.go:370-381`). So "20 QPS"
means 20 QPS *each*, not 20 QPS shared — the applied value was a ~31x
under-provision.

Cost: a conformance leg fell from 92/3 to 87/8, teardown ran 3.5 hours until the
job's 300-minute timeout, and the change was reverted (#1862). One additional
file read — the one that constructs the client — would have prevented all of it.

The lesson is rule 3 stated precisely: **a constant is not a mechanism.** When
you look up a value, also look up who constructs the thing that holds it, and
how many of them exist.

### Debugging: read upstream before reading logs

When behaviour is wrong and upstream implements that behaviour, the answer is
usually in upstream's source, not in our logs. Log-diving tells you *what*
happened; upstream tells you what *should* happen, which is the thing you
actually need. Reach for `../kubernetes` first and use logs to confirm, not to
derive.

### Worked example — why 3-5 above are their own rules

The ReplicationController could not create 100 pods inside a conformance spec's
120s budget. `slowStartBatch`
(`pkg/controller/replicaset/replica_set.go:820-844`) was ported on its own in
PR #1853, and it made things **worse**: the leg went 91/94 -> 90/94, the
conformance suite's own client rate limiter starved, and a namespace teardown
that had been taking 81s took 3h23m.

Reading `manageReplicas` and `syncReplicaSet` properly turned up three parts
that had been left behind, each of which exists precisely to make batched
creation safe:

- `ControllerExpectations` (`:619`, `:728`) — the brake. Without it a
  watch-driven controller wakes on its own create events, lists a backend that
  has not caught up, and issues the batch again.
- `burstReplicas = 500` (`:72`, `:611`, `:653`) — the cap. Batches double
  without limit, so nothing else bounds one object's burst.
- `NamespaceTerminatingCause` handling (`:643-651`) — the client half of a pair
  whose server half we had already shipped in #1849, leaving every namespace
  teardown in a create-retry loop.

Two of those came out of upstream's *tests*: `TestRSSyncExpectations` is the
reason expectations must be read BEFORE listing pods (check after, and a pod
arriving in between makes the record look fulfilled while your list still lacks
it — so you duplicate it), and `TestSlowStartBatch` pins what a partially
failing batch must report. Neither is inferable from the implementation alone.

### Reference checkouts (read these — they are on this machine)
- **`../kubernetes`** — upstream Kubernetes (Go). The primary source of truth
  for api-server, kubelet, scheduler, controller-manager, kube-proxy, RBAC,
  validation, bootstrap policy, conformance expectations. Pinned to the target
  version.
- **`../containerd`**, **`../containerd-rs`** — CRI runtime + a Rust CRI client
  to mirror for kubelet↔runtime work.
- **k3s / k0s / rke2 / kind** — distributions that *embed or ship the upstream
  kube-apiserver/kubelet/etc.* Consult them (via `gh`/web or a clone) for how a
  real distro wires bootstrap, addons (CNI, kube-proxy, CoreDNS), node
  registration, and cluster bring-up. They rarely reimplement core behavior —
  which is itself the signal that you shouldn't either.

### CRI / CNI / CSI
The interface contracts are a hard constraint, not a suggestion — match the
upstream call sequence and the published specs exactly. See CLAUDE.local.md for
the full statement; the upstream-first rule above is how you satisfy it.

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build (fast iteration)
cargo build --release          # Release build

# Test
cargo test                     # All workspace tests
cargo test -p rusternetes-api-server  # Single crate
cargo test test_name           # Single test by name
cargo test test_name -- --nocapture  # With stdout

# Lint & Format
cargo fmt --all                # Format (REQUIRED before every commit — see below)
cargo fmt --all -- --check     # Verify formatting without writing (CI uses this)
cargo clippy --all-targets --all-features -- -D warnings  # Lint
make pre-commit                # Format + clippy + test (run before commits)

# Cluster (Podman or Docker)
# One-shot bring-up (teardown → certs → build → up → bootstrap). Pre-creates
# the bind-mount host dirs so a fresh checkout works with no manual chown:
bash scripts/cluster-up.sh                       # SQLite (default); --backend etcd|redis

# Manual bring-up. IMPORTANT: pre-create .rusternetes/manifests (and volumes)
# BEFORE `compose up` — otherwise the daemon creates the bind-mount source as
# root and bootstrap-cluster.sh fails to template static-pod YAML into it (#1152).
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
mkdir -p .rusternetes/manifests .rusternetes/certs "$KUBELET_VOLUMES_PATH"
podman compose build           # Build images (~1 hour first build, faster with cache)
podman compose up -d           # Start cluster (etcd, default)
podman compose down            # Stop cluster
bash scripts/bootstrap-cluster.sh  # Create CoreDNS, services, SA tokens

# Alternative storage backends (multi-container)
podman compose -f compose.sqlite.yml up -d     # SQLite via rhino server
podman compose -f compose.redis.yml up -d      # Redis via rhino server

# All-in-one binary (single container + storage)
podman compose -f compose.all-in-one.yml up -d        # Embedded SQLite
podman compose -f compose.all-in-one-redis.yml up -d  # Redis (requires adjacent rhino repo)

# Conformance testing
bash scripts/run-conformance.sh       # Full conformance lifecycle
bash scripts/conformance-progress.sh  # Monitor pass/fail progress
# e2e output is in /tmp/sonobuoy/results/e2e.log inside the e2e container
```

KUBECONFIG: `~/.kube/rusternetes-config`

### Local build performance

**Always build via `make` — never `cargo build --release` directly.** The
`[profile.release]` profile is `lto = "thin"` + `codegen-units = 1` (tuned for
shipped artifacts), which serialises codegen on one unit and takes *minutes per
crate* on the big crates (api-server, ~9400-line router). For local iteration,
building a test/container image, or any functional (non-shipped) binary, use:

```bash
make build-fast ARGS="-p rusternetes-api-server --bin api-server --features sqlite"
```

`build-fast` uses the `release-fast` profile (`codegen-units = 16`, `lto = off`)
— same speed class as CI's test build, output at `target/release-fast/`. Reserve
`make build` (full `release`) for the shipped `indyjonesnl/rusternetes` release
artifacts only. `make build-dev` is the debug build. Rule of thumb: if the
binary just needs to *run* (a swap test, a canary image, local debugging), it
does NOT need LTO — use `make build-fast`.

The slow part of local iteration is **codegen on every edit**, not linking.
Two settings dominate; both are machine-local (shell + `~/.cargo/config.toml`),
not checked in:

- **Incremental compilation, no local sccache.** Local sccache forces
  `CARGO_INCREMENTAL=0`, and on a single-developer machine cargo's own
  fingerprinting already skips unchanged crates, so local sccache hit-rate is
  ~0% — you pay the no-incremental cost for no cache benefit. Locally, **unset
  `RUSTC_WRAPPER` and set `CARGO_INCREMENTAL=1`**. Measured: a trivial edit →
  rebuild of one crate drops from ~35s to ~5s. CI runs its own cluster-side
  sccache→MinIO (see `indy/arc-runner/sccache-minio`), independent of local —
  leave that alone.
- **mold linker.** Uncomment the `[target.x86_64-unknown-linux-gnu]` block in
  `~/.cargo/config.toml` (`linker = "clang"`, `-fuse-ld=mold`; needs
  `apt-get install mold clang`). 3–5× faster links. The checked-in
  `.cargo/config.toml` keeps this commented so CI links with system `ld` and
  doesn't paper over mold-specific release issues. **Do not** add a
  `[build] rustflags = ["-Z", "threads=24"]` line: `-Z` is nightly-only (the
  pinned toolchain is stable) and cargo silently drops `[build].rustflags`
  whenever a `[target.*].rustflags` exists — it's dead config that breaks
  stable builds the moment the mold block is removed.

**Shared target dir across worktrees.** Set
`CARGO_TARGET_DIR=$HOME/.cache/rusternetes-target` and symlink each checkout's
`./target` to it (`ln -s "$CARGO_TARGET_DIR" target`). All worktrees then share
one compiled-dependency cache (~40–120G once, not per-worktree) and reuse each
other's artifacts. Trade-off: cargo's target-lock serializes concurrent builds
across worktrees (one `cargo build` at a time), which also keeps the disk from
filling. Scripts that reference `target/debug/<bin>` keep working through the
symlink.

> Shell env vars set in `~/.bashrc` only reach **new interactive** shells
> (`~/.bashrc` returns early for non-interactive shells). After editing them,
> open a fresh terminal — already-running processes keep the old environment.

## Architecture

Rust reimplementation of Kubernetes. Workspace with 10 crates:

- **`common`** - Shared resource types (Pod, Deployment, Service, etc.), error types, utilities. All resource structs live in `src/resources/`. Error type in `src/error.rs` maps to Kubernetes StatusReason and implements Axum's `IntoResponse`.
- **`api-server`** - Axum-based REST API. Routes in `src/router.rs` (~9400 lines). One handler file per resource type in `src/handlers/`. State in `src/state.rs` holds storage, auth, IP allocator, webhook manager, watch cache.
- **`storage`** - `Storage` trait in `src/lib.rs` with etcd backend (`src/etcd.rs`), SQLite backend via rhino (`src/rhino.rs`, behind `sqlite` feature), Redis backend via rhino (`src/rhino.rs`, behind `redis` feature), and memory backend (`src/memory.rs`). `StorageBackend` enum dispatches to the selected backend. Keys follow `/registry/{resource_type}/{namespace}/{name}`. Resource versions map to backend revision numbers. The rhino dependency uses an in-tree submodule path (`../../rhino` from `crates/storage`, i.e. the `./rhino` submodule).
- **`controller-manager`** - 31 controllers in `src/controllers/`. Each has a struct with `storage: Arc<S>` + `interval: Duration`, an infinite `run()` loop, and `reconcile_one()` per resource.
- **`kubelet`** - Container runtime via CRI (CRI v1 gRPC to containerd, which runs containers with Youki). Manages pod lifecycle, volumes, probes, networking. The CRI endpoint comes from `CONTAINER_RUNTIME_ENDPOINT` (default `unix:///run/containerd/containerd.sock`).
- **`kube-proxy`** - iptables-based service routing. Runs in host network mode. Reads both Endpoints and EndpointSlices.
- **`scheduler`** - Pod scheduling with affinity, taints/tolerations, priority/preemption. Plugins in `src/plugins/`.
- **`kubectl`** - CLI tool. Commands in `src/commands/`.
- **`cloud-providers`** - AWS/GCP/Azure integrations.
- **`rusternetes`** - All-in-one binary. Spawns api-server, scheduler, controller-manager, kubelet, and kube-proxy as concurrent tokio tasks sharing one `StorageBackend`. Defaults to embedded SQLite. Supports `--storage-backend redis --redis-url redis://host:6379` for Redis via rhino.

## Key Conventions

### Serialization (critical for K8s API compatibility)
- All resource structs use `#[serde(rename_all = "camelCase")]`
- Optional fields use `#[serde(skip_serializing_if = "Option::is_none")]`
- TypeMeta is flattened: `#[serde(flatten)] pub type_meta: TypeMeta`
- camelCase abbreviations follow K8s style: `podIP` not `podIp`, `hostIP` not `hostIp`, `containerID` not `containerId`

### Adding a New Resource
1. Define struct in `crates/common/src/resources/{type}.rs`
2. Add handlers in `crates/api-server/src/handlers/{type}.rs`
3. Register route in `crates/api-server/src/router.rs`
4. Add controller in `crates/controller-manager/src/controllers/` if needed

### Controller Pattern
```rust
pub struct FooController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}
impl<S: Storage> FooController<S> {
    pub async fn run(&self) -> Result<()> {
        loop {
            self.reconcile_all().await?;
            tokio::time::sleep(self.interval).await;
        }
    }
}
```

### Testing
- Async tests: `#[tokio::test]`
- Use `MemoryStorage` (not etcd) for unit tests
- Serial tests when needed: `#[serial_test::serial]`

### Commit Messages
Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.

Keep the subject line ≤72 characters. The GitHub PR-creation page truncates longer titles, and CI / commitlint flags them. If you exceed 72, amend before pushing.

### Pre-push checklist (REQUIRED)
The `.github/workflows/fmt.yml` action runs `cargo fmt --all -- --check` on every PR. If formatting drift exists, the PR is red and unmergeable. **Before pushing any branch, run:**

```bash
cargo fmt --all
```

This applies the canonical style in-place. Then verify with:

```bash
cargo fmt --all -- --check    # must exit 0
```

If you forget and push a branch with formatting drift, the `Check formatting` job goes red. Recovery: run `cargo fmt --all` locally, `git add -A`, `git commit --amend --no-edit`, `git push --force-with-lease`. Prefer to catch it before pushing — `make pre-commit` runs fmt + clippy + test in one shot.

When working across multiple branches (rebases, cherry-picks, parallel work), re-run `cargo fmt --all` on **every** branch before pushing, not just the one you happen to be on. Multi-line `if let` bindings and method chains are the most common drift cases.

## Cluster Details

### Multi-container (compose.yml / compose.redis.yml / compose.sqlite.yml)
Services: etcd/rhino/redis, api-server (port 6443 with TLS), scheduler, controller-manager, 2x kubelet (node-1, node-2), kube-proxy (host network).

### All-in-one (compose.all-in-one.yml / compose.all-in-one-redis.yml)
Single `rusternetes` binary container + optional Redis container. All components run as tokio tasks in one process. No kube-proxy (disabled, no iptables in container).

### Common
- TLS certs in `.rusternetes/certs/`, generated by `scripts/generate-certs.sh`
- Cert SANs must include bridge IPs (172.18.0.2-5, 10.89.0.x)
- kube-proxy needs `CAP_NET_ADMIN` for iptables; runs host network mode
- `KUBERNETES_SERVICE_HOST_OVERRIDE` env var sets the API server address for pods
- CoreDNS ClusterIP pinned to 10.96.0.10
- Rhino is an in-tree git submodule at `./rhino` (pinned to `indyjonesnl/rhino`) for SQLite/Redis embedded builds. Clone with `git clone --recurse-submodules`, or run `git submodule update --init` in an existing checkout.
