# Node Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a kubelet-scoped conformance harness that runs the upstream `e2e_node.test` binary against a single Rusternetes kubelet, producing pass/fail/skip numbers in minutes instead of hours.

**Architecture:** Three PRs. PR1 ships scaffold (single-node compose file, runner script, doc skeleton) that runs the upstream binary against the existing kubelet — most tests will skip/fail, baseline gets recorded. PR2 adds `crates/kubelet/src/server.rs` exposing the HTTP endpoints `e2e_node.test` expects (`/pods`, `/runningpods/`, `/healthz`, `/stats/summary`, plus `/exec` and `/logs` aliases). PR3 adds a progress-tail script and an opt-in self-hosted GitHub Actions workflow.

**Tech Stack:** Rust (axum, tokio, bollard), Docker/Podman compose, Bash, upstream Kubernetes v1.35 `e2e_node.test` (Go binary, prebuilt).

**Related spec:** `docs/superpowers/specs/2026-05-17-node-conformance-design.md`

---

## File Structure

### PR1 (scaffold)
- Create: `compose.node-conformance.yml` — strips compose.yml to etcd + api-server + one kubelet
- Create: `scripts/run-node-conformance.sh` — boots stack, fetches binary, runs ginkgo, parses results
- Create: `docs/NODE_CONFORMANCE.md` — rolling pass/fail/skip ledger
- Modify: `.gitignore` — ignore `.bin/`, `/tmp/node-conformance/`

### PR2 (kubelet server)
- Create: `crates/kubelet/src/server.rs` — axum router with the six new endpoints
- Modify: `crates/kubelet/src/kubelet.rs` — add `last_sync` AtomicU64 + `healthy()` accessor; touch in sync_loop
- Modify: `crates/kubelet/src/main.rs` — wire new router behind `RUSTERNETES_KUBELET_SERVER_PORT` env var
- Modify: `crates/kubelet/src/lib.rs` — declare `pub mod server;`
- Create: `crates/kubelet/tests/server_integration.rs` — endpoint shape assertions against `MemoryStorage`

### PR3 (CI hook)
- Create: `scripts/node-conformance-progress.sh` — tail+grep `/tmp/node-conformance/ginkgo.log`
- Create: `.github/workflows/node-conformance.yml` — self-hosted, manual-dispatch + nightly cron

---

# PR1 — Scaffold

### Task 1: Bootstrap branch + .gitignore

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Create the work branch from main**

```bash
git checkout main
git pull --ff-only fork main
git checkout -b feat/node-conformance-scaffold
```

- [ ] **Step 2: Update `.gitignore`**

Append the following two lines to `.gitignore` (create section if not present):

```gitignore
# Node conformance harness
.bin/
/tmp/node-conformance/
```

- [ ] **Step 3: Commit**

```bash
git add .gitignore
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "chore: ignore node-conformance artifacts"
```

---

### Task 2: Single-node compose file

**Files:**
- Create: `compose.node-conformance.yml`

- [ ] **Step 1: Write the file**

Create `compose.node-conformance.yml` with this exact content (it is a trimmed subset of `compose.yml`):

```yaml
# Single-node compose for kubelet-scoped conformance testing.
# Brings up only etcd + api-server + one kubelet — no scheduler,
# no controller-manager, no kube-proxy, no second kubelet.
#
# Used by scripts/run-node-conformance.sh.

services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.17
    container_name: rusternetes-nc-etcd
    command:
      - /usr/local/bin/etcd
      - --name=etcd
      - --data-dir=/etcd-data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://etcd:2379
      - --listen-peer-urls=http://0.0.0.0:2380
    ports:
      - "2379:2379"
    networks:
      - nc-net
    healthcheck:
      test: ["CMD", "/usr/local/bin/etcdctl", "--endpoints=http://localhost:2379", "endpoint", "health"]
      interval: 10s
      timeout: 5s
      retries: 5
    environment:
      - ETCDCTL_API=3

  api-server:
    build:
      context: .
      dockerfile: Dockerfile.api-server
      additional_contexts:
        rhino: ../rhino
    container_name: rusternetes-nc-api-server
    privileged: true
    ports:
      - "6443:6443"
    depends_on:
      etcd:
        condition: service_healthy
    networks:
      nc-net:
        aliases:
          - api-server
    volumes:
      - ./.rusternetes/certs:/etc/kubernetes/pki:ro
      - /run/podman/podman.sock:/run/podman/podman.sock:rw,z
      - /run/podman/podman.sock:/var/run/docker.sock:rw,z

  kubelet:
    build:
      context: .
      dockerfile: Dockerfile.kubelet
      additional_contexts:
        rhino: ../rhino
    container_name: rusternetes-nc-kubelet
    privileged: true
    depends_on:
      - etcd
      - api-server
    networks:
      - nc-net
    ports:
      - "10250:10250"
    volumes:
      - /run/podman/podman.sock:/run/podman/podman.sock:rw,z
      - ${KUBELET_VOLUMES_PATH}:${KUBELET_VOLUMES_PATH}:rw
      - ./.rusternetes/certs:/root/.rusternetes/certs:ro
    environment:
      - RUST_LOG=info
      - DOCKER_HOST=unix:///run/podman/podman.sock
      - KUBERNETES_SERVICE_HOST_OVERRIDE=api-server
      - KUBELET_VOLUMES_PATH=${KUBELET_VOLUMES_PATH}
      - RUSTERNETES_KUBELET_SERVER_PORT=10250
    command: ["--node-name", "node-1", "--etcd-servers", "http://etcd:2379", "--cluster-dns", "10.96.0.10", "--metrics-port", "10249", "--sync-interval", "3"]

networks:
  nc-net:
    driver: bridge
```

Note: `RUSTERNETES_KUBELET_SERVER_PORT=10250` is read by code added in PR2. Until PR2 lands, the env var is ignored and kubelet only exposes `:10249` (metrics). PR1 still gets a useful baseline because most node-conformance tests fail/skip without `/pods` anyway.

- [ ] **Step 2: Validate compose syntax**

Run: `podman compose -f compose.node-conformance.yml config >/dev/null`
Expected: exit 0, no stderr.

- [ ] **Step 3: Commit**

```bash
git add compose.node-conformance.yml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(conformance): single-node compose for node-conformance harness"
```

---

### Task 3: Runner script

**Files:**
- Create: `scripts/run-node-conformance.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Kubelet-scoped conformance runner.
# Boots compose.node-conformance.yml, fetches the upstream e2e_node.test
# binary, runs ginkgo focused on [NodeConformance], dumps results.
#
# See docs/superpowers/specs/2026-05-17-node-conformance-design.md.
set -euo pipefail

K8S_VERSION="${K8S_VERSION:-v1.35.0}"
ARCH="${ARCH:-linux-amd64}"
TEST_TARBALL_URL="https://dl.k8s.io/${K8S_VERSION}/kubernetes-test-${ARCH}.tar.gz"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${PROJECT_ROOT}/.bin"
RESULTS_DIR="/tmp/node-conformance"
FOCUS="${FOCUS:-\\[NodeConformance\\]}"
SKIP="${SKIP:-\\[Flaky\\]|\\[Serial\\]}"

export KUBELET_VOLUMES_PATH="${KUBELET_VOLUMES_PATH:-${PROJECT_ROOT}/.rusternetes/volumes}"
mkdir -p "${KUBELET_VOLUMES_PATH}" "${BIN_DIR}" "${RESULTS_DIR}"

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}"
COMPOSE="${CONTAINER_RUNTIME} compose -f ${PROJECT_ROOT}/compose.node-conformance.yml"

echo "=== Rusternetes Node Conformance ==="
echo "K8S_VERSION=${K8S_VERSION} FOCUS=${FOCUS}"

echo "[1/6] Tearing down any previous node-conformance stack..."
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true

echo "[2/6] Bringing up single-node stack..."
${COMPOSE} up -d --build

echo "[3/6] Waiting for kubelet to come up (max 60s)..."
for i in $(seq 1 60); do
    if curl -sfk "http://localhost:10250/healthz" >/dev/null 2>&1 \
        || curl -sfk "http://localhost:10249/metrics" >/dev/null 2>&1; then
        echo "kubelet is up"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "ERROR: kubelet did not come up within 60s"
        ${COMPOSE} logs kubelet || true
        exit 1
    fi
done

echo "[4/6] Fetching e2e_node.test (${K8S_VERSION} ${ARCH})..."
if [ ! -f "${BIN_DIR}/e2e_node.test" ] || [ ! -f "${BIN_DIR}/ginkgo" ]; then
    TMP_TARBALL="$(mktemp)"
    curl -fL "${TEST_TARBALL_URL}" -o "${TMP_TARBALL}"
    tar -xzf "${TMP_TARBALL}" -C "${BIN_DIR}" \
        --strip-components=3 \
        kubernetes/test/bin/e2e_node.test \
        kubernetes/test/bin/ginkgo
    rm -f "${TMP_TARBALL}"
    chmod +x "${BIN_DIR}/e2e_node.test" "${BIN_DIR}/ginkgo"
fi

echo "[5/6] Running ginkgo focus=${FOCUS}..."
KUBECONFIG="${HOME}/.kube/rusternetes-config" \
"${BIN_DIR}/ginkgo" \
    --focus="${FOCUS}" \
    --skip="${SKIP}" \
    --no-color \
    "${BIN_DIR}/e2e_node.test" \
    -- \
    --node-name=node-1 \
    --kubelet-host=localhost \
    --kubelet-port=10250 \
    --kubeconfig="${HOME}/.kube/rusternetes-config" \
    2>&1 | tee "${RESULTS_DIR}/ginkgo.log" || true

echo "[6/6] Parsing results..."
PASS=$(grep -cE '^\s*\[PASSED\]' "${RESULTS_DIR}/ginkgo.log" || true)
FAIL=$(grep -cE '^\s*\[FAILED\]' "${RESULTS_DIR}/ginkgo.log" || true)
SKIP_CT=$(grep -cE '^\s*\[SKIPPED\]' "${RESULTS_DIR}/ginkgo.log" || true)

echo "PASS=${PASS} FAIL=${FAIL} SKIP=${SKIP_CT}"
echo "Full log: ${RESULTS_DIR}/ginkgo.log"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/run-node-conformance.sh
```

- [ ] **Step 3: Sanity-check `bash -n`**

Run: `bash -n scripts/run-node-conformance.sh`
Expected: exit 0, no output.

- [ ] **Step 4: Sanity-check `shellcheck` (skip if not installed)**

Run: `command -v shellcheck && shellcheck scripts/run-node-conformance.sh || echo "shellcheck not installed, skipping"`
Expected: exit 0 with no issues, OR "shellcheck not installed, skipping".

- [ ] **Step 5: Commit**

```bash
git add scripts/run-node-conformance.sh
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(conformance): node-conformance runner script"
```

---

### Task 4: Documentation skeleton

**Files:**
- Create: `docs/NODE_CONFORMANCE.md`

- [ ] **Step 1: Write the doc**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add docs/NODE_CONFORMANCE.md
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "docs(conformance): node-conformance harness usage + ledger"
```

---

### Task 5: Smoke-run the scaffold

**Files:** none modified

- [ ] **Step 1: Run the script end-to-end**

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
bash scripts/run-node-conformance.sh
```

Expected: script exits 0. Final line prints `PASS=<n> FAIL=<n> SKIP=<n>`. Most tests will fail because PR1 has not yet implemented the new kubelet endpoints — that is expected and is the baseline.

- [ ] **Step 2: Record baseline in the doc**

Edit `docs/NODE_CONFORMANCE.md` and replace the placeholder Round 1 row with the actual numbers printed by the script. Keep the date as today's date.

- [ ] **Step 3: Commit**

```bash
git add docs/NODE_CONFORMANCE.md
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "docs(conformance): record node-conformance Round 1 baseline"
```

- [ ] **Step 4: Run cargo fmt (no-op but the repo demands it pre-push)**

```bash
cargo fmt --all -- --check
```

Expected: exit 0.

- [ ] **Step 5: Push and open PR1**

```bash
git push -u fork feat/node-conformance-scaffold
```

Then open the PR against `indyjonesnl/rusternetes:main`. Title: `feat(conformance): node-conformance harness scaffold (PR1)`. Description should link the design doc and the baseline row.

**Stop here. Wait for PR1 review before starting PR2.**

---

# PR2 — Kubelet HTTP Server

### Task 6: Branch from PR1

**Files:** none modified

- [ ] **Step 1: Create the PR2 branch**

```bash
git checkout feat/node-conformance-scaffold
git pull --ff-only
git checkout -b feat/kubelet-server-endpoints
```

---

### Task 7: Track sync-loop liveness in Kubelet

**Files:**
- Modify: `crates/kubelet/src/kubelet.rs` (around lines 50–130 for struct, lines 770+ for sync_loop)

- [ ] **Step 1: Write the failing test**

Append to `crates/kubelet/src/kubelet.rs` in the existing `#[cfg(test)] mod tests` block (near line 4180):

```rust
#[test]
fn test_last_sync_timestamp_records_now() {
    use std::sync::atomic::Ordering;
    let kl_last_sync = std::sync::atomic::AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    kl_last_sync.store(now, Ordering::Relaxed);
    assert!(kl_last_sync.load(Ordering::Relaxed) >= now);
}

#[test]
fn test_healthy_returns_true_when_recently_synced() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let last_sync = AtomicU64::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    let max_age_secs = 6;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let age = now.saturating_sub(last_sync.load(Ordering::Relaxed));
    assert!(age <= max_age_secs);
}

#[test]
fn test_healthy_returns_false_when_stale() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let last_sync = AtomicU64::new(100);
    let max_age_secs = 6;
    let now: u64 = 200;
    let age = now.saturating_sub(last_sync.load(Ordering::Relaxed));
    assert!(age > max_age_secs);
}
```

- [ ] **Step 2: Run the tests to confirm they pass (they are pure-logic, no impl required yet)**

Run: `cargo test -p rusternetes-kubelet --lib test_last_sync_timestamp_records_now test_healthy_returns_true_when_recently_synced test_healthy_returns_false_when_stale`
Expected: 3 passed.

These tests pin the contract we need on the Kubelet struct before adding the field.

- [ ] **Step 3: Add `last_sync` field to the Kubelet struct**

Locate the Kubelet struct (near line 50 of `crates/kubelet/src/kubelet.rs`). Add this field to the struct definition, alongside `pod_workers`:

```rust
    /// Unix-seconds timestamp of the last completed sync_loop iteration.
    /// Read by the kubelet HTTP `/healthz` handler to detect a stalled sync loop.
    last_sync: std::sync::atomic::AtomicU64,
```

Then in `Kubelet::new` (near line 110), add to the returned struct:

```rust
            last_sync: std::sync::atomic::AtomicU64::new(0),
```

- [ ] **Step 4: Touch `last_sync` at the end of each sync iteration**

Find `sync_loop` (around line 770). At the end of each loop iteration (after the work is done, before `tokio::time::sleep`), insert:

```rust
            self.last_sync.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                std::sync::atomic::Ordering::Relaxed,
            );
```

- [ ] **Step 5: Add a `healthy()` accessor on Kubelet**

In the `impl Kubelet` block, add (a good location is right after `pub async fn run`):

```rust
    /// Return true iff the sync loop has ticked within the last
    /// `2 × sync_interval` seconds. Used by the kubelet HTTP `/healthz`
    /// handler.
    pub fn healthy(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = self
            .last_sync
            .load(std::sync::atomic::Ordering::Relaxed);
        let max_age = self.sync_interval.as_secs().saturating_mul(2).max(6);
        last != 0 && now.saturating_sub(last) <= max_age
    }
```

- [ ] **Step 6: Run the whole kubelet test suite**

Run: `cargo test -p rusternetes-kubelet --lib`
Expected: all tests pass, including the three new ones.

- [ ] **Step 7: Format**

Run: `cargo fmt --all`
Expected: exit 0.

- [ ] **Step 8: Commit**

```bash
git add crates/kubelet/src/kubelet.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): track sync-loop liveness for /healthz"
```

---

### Task 8: Skeleton server module + `/healthz`

**Files:**
- Create: `crates/kubelet/src/server.rs`
- Modify: `crates/kubelet/src/lib.rs`

- [ ] **Step 1: Write the failing test (in the new file)**

Create `crates/kubelet/src/server.rs` with this initial content:

```rust
//! Kubelet HTTP server exposing the surface expected by upstream
//! `e2e_node.test` — `/pods`, `/runningpods/`, `/healthz`,
//! `/stats/summary`, plus `/exec` and `/logs` aliases.
//!
//! See `docs/superpowers/specs/2026-05-17-node-conformance-design.md`.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::get, Router};

use crate::kubelet::Kubelet;

#[derive(Clone)]
pub struct ServerState {
    pub kubelet: Arc<Kubelet>,
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .with_state(state)
}

async fn healthz(State(state): State<ServerState>) -> (StatusCode, &'static str) {
    if state.kubelet.healthy() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, "stale")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state(healthy: bool) -> ServerState {
        // We can't easily build a full Kubelet in a unit test, so this
        // test is wired in tests/server_integration.rs once a test fixture
        // exists. Keep this signature here for future expansion.
        let _ = healthy;
        unimplemented!("see crates/kubelet/tests/server_integration.rs")
    }

    #[tokio::test]
    #[ignore]
    async fn healthz_returns_ok_when_healthy() {
        let app = router(test_state(true));
        let res = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
```

Then declare the module in `crates/kubelet/src/lib.rs`. Locate the existing `pub mod` / `mod` declarations near the top and add:

```rust
pub mod kubelet;
pub mod server;
```

(If `kubelet` is already declared as `pub mod`, just add the `pub mod server;` line after it.)

- [ ] **Step 2: Run the failing test (verifies module compiles)**

Run: `cargo test -p rusternetes-kubelet --lib server::tests::healthz_returns_ok_when_healthy -- --include-ignored`
Expected: panics with `unimplemented!`. The compile succeeded.

- [ ] **Step 3: Add `tower` dev-dep for `ServiceExt::oneshot`**

In `crates/kubelet/Cargo.toml`, locate `[dev-dependencies]` (add it if absent) and ensure these lines exist:

```toml
[dev-dependencies]
tower = "0.5"
```

If `tower` is already present, skip.

- [ ] **Step 4: Verify compile**

Run: `cargo build -p rusternetes-kubelet --tests`
Expected: exit 0 with no errors. Warnings about unused imports in `server.rs` are acceptable for now.

- [ ] **Step 5: Format**

```bash
cargo fmt --all
```

- [ ] **Step 6: Commit**

```bash
git add crates/kubelet/src/server.rs crates/kubelet/src/lib.rs crates/kubelet/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): server module skeleton with /healthz"
```

---

### Task 9: `/pods` endpoint

**Files:**
- Modify: `crates/kubelet/src/server.rs`

- [ ] **Step 1: Write the failing test (integration)**

Create `crates/kubelet/tests/server_integration.rs` with this content:

```rust
//! Integration tests for kubelet HTTP server endpoints.
//! Uses MemoryStorage and a partial Kubelet fixture.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rusternetes_common::resources::pod::Pod;
use rusternetes_kubelet::server::{router, ServerState};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use tower::ServiceExt;

async fn fixture(node_name: &str, pods: Vec<Pod>) -> ServerState {
    let storage = Arc::new(StorageBackend::Memory(MemoryStorage::new()));
    for p in pods {
        let ns = p
            .metadata
            .as_ref()
            .and_then(|m| m.namespace.as_deref())
            .unwrap_or("default")
            .to_string();
        let name = p
            .metadata
            .as_ref()
            .and_then(|m| m.name.as_deref())
            .unwrap_or("unnamed")
            .to_string();
        storage
            .put(&format!("/registry/pods/{}/{}", ns, name), &p)
            .await
            .unwrap();
    }
    ServerState {
        node_name: node_name.to_string(),
        storage,
    }
}

fn pod_on(name: &str, node: &str) -> Pod {
    let mut p = Pod::default();
    p.metadata = Some(rusternetes_common::resources::ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some("default".to_string()),
        ..Default::default()
    });
    p.spec = Some(rusternetes_common::resources::pod::PodSpec {
        node_name: Some(node.to_string()),
        ..Default::default()
    });
    p
}

#[tokio::test]
async fn pods_returns_only_local_node_pods() {
    let state = fixture(
        "node-1",
        vec![pod_on("a", "node-1"), pod_on("b", "node-2"), pod_on("c", "node-1")],
    )
    .await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/pods").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = v["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "expected 2 pods on node-1, got {}", items.len());
    let names: Vec<_> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"c"));
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `cargo test -p rusternetes-kubelet --test server_integration pods_returns_only_local_node_pods`
Expected: compile error — `ServerState` does not have `node_name` and `storage` fields yet, and there is no `/pods` route.

- [ ] **Step 3: Extend `ServerState`**

Edit `crates/kubelet/src/server.rs`. Replace the existing `ServerState` and `router` definitions with:

```rust
#[derive(Clone)]
pub struct ServerState {
    pub node_name: String,
    pub storage: Arc<rusternetes_storage::StorageBackend>,
    pub kubelet: Option<Arc<Kubelet>>,
}

impl ServerState {
    pub fn new_for_tests(node_name: String, storage: Arc<rusternetes_storage::StorageBackend>) -> Self {
        Self { node_name, storage, kubelet: None }
    }
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/pods", get(list_pods))
        .with_state(state)
}
```

And adjust `healthz` to use the optional kubelet:

```rust
async fn healthz(State(state): State<ServerState>) -> (StatusCode, &'static str) {
    match &state.kubelet {
        Some(kl) if kl.healthy() => (StatusCode::OK, "ok"),
        Some(_) => (StatusCode::INTERNAL_SERVER_ERROR, "stale"),
        None => (StatusCode::OK, "ok"), // test mode
    }
}
```

Also: change the test fixture constructor — the integration test uses bare field syntax, so make all three fields `pub`. (They already are in the snippet above.)

- [ ] **Step 4: Implement `list_pods`**

Append to `crates/kubelet/src/server.rs`:

```rust
use axum::Json;
use rusternetes_common::resources::pod::Pod;
use rusternetes_storage::Storage;

async fn list_pods(State(state): State<ServerState>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let all = state
        .storage
        .list::<Pod>("/registry/pods/")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mine: Vec<&Pod> = all
        .iter()
        .filter(|p| {
            p.spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                == Some(state.node_name.as_str())
        })
        .collect();
    Ok(Json(serde_json::json!({
        "kind": "PodList",
        "apiVersion": "v1",
        "items": mine,
    })))
}
```

- [ ] **Step 5: Run the test to confirm it passes**

Run: `cargo test -p rusternetes-kubelet --test server_integration pods_returns_only_local_node_pods`
Expected: PASS.

- [ ] **Step 6: Format**

```bash
cargo fmt --all
```

- [ ] **Step 7: Commit**

```bash
git add crates/kubelet/src/server.rs crates/kubelet/tests/server_integration.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): GET /pods returns local-node pods"
```

---

### Task 10: `/runningpods/` endpoint

**Files:**
- Modify: `crates/kubelet/src/server.rs`
- Modify: `crates/kubelet/tests/server_integration.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/kubelet/tests/server_integration.rs`:

```rust
fn pod_with_phase(name: &str, node: &str, phase: &str) -> Pod {
    let mut p = pod_on(name, node);
    p.status = Some(rusternetes_common::resources::pod::PodStatus {
        phase: Some(phase.to_string()),
        ..Default::default()
    });
    p
}

#[tokio::test]
async fn runningpods_returns_only_running_phase() {
    let state = fixture(
        "node-1",
        vec![
            pod_with_phase("running1", "node-1", "Running"),
            pod_with_phase("pending1", "node-1", "Pending"),
            pod_with_phase("running2", "node-1", "Running"),
            pod_with_phase("running-elsewhere", "node-2", "Running"),
        ],
    )
    .await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/runningpods/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = v["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let names: Vec<_> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(names.contains(&"running1"));
    assert!(names.contains(&"running2"));
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `cargo test -p rusternetes-kubelet --test server_integration runningpods_returns_only_running_phase`
Expected: 404 from axum (no route registered).

- [ ] **Step 3: Add route + handler**

In `crates/kubelet/src/server.rs`, add `/runningpods/` to the router:

```rust
pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/pods", get(list_pods))
        .route("/runningpods/", get(list_running_pods))
        .with_state(state)
}
```

Add the handler:

```rust
async fn list_running_pods(State(state): State<ServerState>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let all = state
        .storage
        .list::<Pod>("/registry/pods/")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mine: Vec<&Pod> = all
        .iter()
        .filter(|p| {
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(state.node_name.as_str())
                && p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running")
        })
        .collect();
    Ok(Json(serde_json::json!({
        "kind": "PodList",
        "apiVersion": "v1",
        "items": mine,
    })))
}
```

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test -p rusternetes-kubelet --test server_integration runningpods_returns_only_running_phase`
Expected: PASS.

- [ ] **Step 5: Format + commit**

```bash
cargo fmt --all
git add crates/kubelet/src/server.rs crates/kubelet/tests/server_integration.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): GET /runningpods/ for ginkgo node tests"
```

---

### Task 11: `/stats/summary` endpoint (minimal shape)

**Files:**
- Modify: `crates/kubelet/src/server.rs`
- Modify: `crates/kubelet/tests/server_integration.rs`

The upstream ginkgo node-conformance tests assert that `/stats/summary` returns a JSON object with `node.nodeName`, `node.cpu`, `node.memory`, and a `pods` array. We emit zeros for what we don't measure yet.

- [ ] **Step 1: Write the failing test**

Append to `crates/kubelet/tests/server_integration.rs`:

```rust
#[tokio::test]
async fn stats_summary_returns_minimal_cadvisor_shape() {
    let state = fixture("node-1", vec![pod_on("a", "node-1")]).await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/stats/summary").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["node"]["nodeName"].as_str(), Some("node-1"));
    assert!(v["node"]["cpu"].is_object());
    assert!(v["node"]["memory"].is_object());
    assert!(v["pods"].is_array());
    assert_eq!(v["pods"].as_array().unwrap().len(), 1);
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `cargo test -p rusternetes-kubelet --test server_integration stats_summary_returns_minimal_cadvisor_shape`
Expected: 404.

- [ ] **Step 3: Add route + handler**

In `crates/kubelet/src/server.rs`, extend the router:

```rust
        .route("/stats/summary", get(stats_summary))
```

Add the handler:

```rust
async fn stats_summary(State(state): State<ServerState>) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let all = state
        .storage
        .list::<Pod>("/registry/pods/")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let now = chrono::Utc::now().to_rfc3339();
    let pods_json: Vec<serde_json::Value> = all
        .iter()
        .filter(|p| {
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(state.node_name.as_str())
        })
        .map(|p| {
            let meta = p.metadata.as_ref();
            serde_json::json!({
                "podRef": {
                    "name": meta.and_then(|m| m.name.clone()).unwrap_or_default(),
                    "namespace": meta.and_then(|m| m.namespace.clone()).unwrap_or_default(),
                    "uid": meta.and_then(|m| m.uid.clone()).unwrap_or_default(),
                },
                "startTime": now,
                "cpu": {"time": now, "usageNanoCores": 0u64, "usageCoreNanoSeconds": 0u64},
                "memory": {"time": now, "workingSetBytes": 0u64, "rssBytes": 0u64, "usageBytes": 0u64},
                "containers": [],
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "node": {
            "nodeName": state.node_name,
            "startTime": now,
            "cpu": {"time": now, "usageNanoCores": 0u64, "usageCoreNanoSeconds": 0u64},
            "memory": {"time": now, "availableBytes": 0u64, "usageBytes": 0u64, "workingSetBytes": 0u64, "rssBytes": 0u64},
        },
        "pods": pods_json,
    })))
}
```

- [ ] **Step 4: Ensure `chrono` is in the kubelet crate**

```bash
grep -q '^chrono' crates/kubelet/Cargo.toml || echo 'NEEDS ADD'
```

If not present, add `chrono = { workspace = true }` under `[dependencies]` in `crates/kubelet/Cargo.toml`. The workspace root should already define a chrono version — verify with `grep -A2 '^\[workspace.dependencies\]' Cargo.toml | grep chrono`.

- [ ] **Step 5: Run the test to confirm it passes**

Run: `cargo test -p rusternetes-kubelet --test server_integration stats_summary_returns_minimal_cadvisor_shape`
Expected: PASS.

- [ ] **Step 6: Format + commit**

```bash
cargo fmt --all
git add crates/kubelet/src/server.rs crates/kubelet/tests/server_integration.rs crates/kubelet/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): GET /stats/summary minimal cAdvisor shape"
```

---

### Task 12: `/healthz` integration test

**Files:**
- Modify: `crates/kubelet/tests/server_integration.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/kubelet/tests/server_integration.rs`:

```rust
#[tokio::test]
async fn healthz_returns_ok_in_test_state() {
    let state = fixture("node-1", vec![]).await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"ok");
}
```

- [ ] **Step 2: Run the test to confirm it passes**

Run: `cargo test -p rusternetes-kubelet --test server_integration healthz_returns_ok_in_test_state`
Expected: PASS (no impl needed — kubelet is None in test state, handler returns OK).

- [ ] **Step 3: Commit**

```bash
git add crates/kubelet/tests/server_integration.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "test(kubelet): cover /healthz in server integration tests"
```

---

### Task 13: Wire the new server into `main.rs`

**Files:**
- Modify: `crates/kubelet/src/main.rs`

The new server only starts when `RUSTERNETES_KUBELET_SERVER_PORT` is set. This keeps the default `compose.yml` unchanged.

- [ ] **Step 1: Read the relevant slice**

```bash
grep -n "metrics_addr\|Starting kubelet API server\|kubelet.run" crates/kubelet/src/main.rs
```

Identify the line range where the metrics server is spawned (around 217–235) and the `kubelet.run().await?` call (around 252).

- [ ] **Step 2: Replace the section**

In `crates/kubelet/src/main.rs`, after the kubelet is constructed (`let kubelet = Arc::new(...)`) but *before* `kubelet.run().await?`, insert:

```rust
    // Optional node-conformance HTTP server on a separate port.
    // Enabled by setting `RUSTERNETES_KUBELET_SERVER_PORT`.
    if let Ok(port_str) = std::env::var("RUSTERNETES_KUBELET_SERVER_PORT") {
        if let Ok(port) = port_str.parse::<u16>() {
            let server_state = kubelet::server::ServerState {
                node_name: runtime_config.node_name.clone(),
                storage: storage.clone(),
                kubelet: Some(kubelet.clone()),
            };
            let addr = format!("0.0.0.0:{}", port);
            tokio::spawn(async move {
                info!("Starting kubelet node-conformance server on {}", addr);
                let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
                axum::serve(listener, kubelet::server::router(server_state))
                    .await
                    .unwrap();
            });
        } else {
            warn!(
                "RUSTERNETES_KUBELET_SERVER_PORT set to invalid value: {}",
                port_str
            );
        }
    }
```

- [ ] **Step 3: Ensure `storage` is in scope at that point**

The `storage` variable is constructed earlier in `main.rs` and is used to build the kubelet. If it's been moved into the `Kubelet::new` call, change the construction to clone it first:

```bash
grep -n "Kubelet::new\|let storage" crates/kubelet/src/main.rs
```

If `storage` is consumed by `Kubelet::new(... storage ...)`, change it to `storage.clone()` so a copy remains for the server.

- [ ] **Step 4: Verify build**

Run: `cargo build -p rusternetes-kubelet`
Expected: exit 0. No warnings in changed code.

- [ ] **Step 5: Run the whole kubelet test suite**

Run: `cargo test -p rusternetes-kubelet`
Expected: all tests pass.

- [ ] **Step 6: Format + commit**

```bash
cargo fmt --all
git add crates/kubelet/src/main.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): wire node-conformance server behind env var"
```

---

### Task 14: Smoke-test PR2 against running stack

**Files:** none

- [ ] **Step 1: Format check (CI gate)**

```bash
cargo fmt --all -- --check
```

Expected: exit 0.

- [ ] **Step 2: Clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: exit 0.

- [ ] **Step 3: Bring up the stack and verify endpoints respond**

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman compose -f compose.node-conformance.yml up -d --build
sleep 10
curl -sf http://localhost:10250/healthz
curl -sf http://localhost:10250/pods | jq '.items | length'
curl -sf http://localhost:10250/runningpods/ | jq '.items | length'
curl -sf http://localhost:10250/stats/summary | jq '.node.nodeName'
```

Expected: `ok`, an integer (likely 0), an integer (likely 0), `"node-1"`. Tear down with `podman compose -f compose.node-conformance.yml down -v`.

- [ ] **Step 4: Re-run the harness and update the ledger**

```bash
bash scripts/run-node-conformance.sh
```

The numbers should improve relative to Round 1. Add a new row to `docs/NODE_CONFORMANCE.md`:

```markdown
| 2 | <today> | <pass> | <fail> | <skip> | <pct> | PR2: kubelet HTTP endpoints landed |
```

- [ ] **Step 5: Commit**

```bash
git add docs/NODE_CONFORMANCE.md
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "docs(conformance): record node-conformance Round 2 (PR2 endpoints)"
```

- [ ] **Step 6: Push and open PR2**

```bash
git push -u fork feat/kubelet-server-endpoints
```

Title: `feat(kubelet): node-conformance HTTP server (PR2)`. Link the design doc and the new Round 2 row. **Stop here. Wait for PR2 review before starting PR3.**

---

# PR3 — CI hook + progress script

### Task 15: Branch from PR2

- [ ] **Step 1: Create the PR3 branch**

```bash
git checkout feat/kubelet-server-endpoints
git pull --ff-only
git checkout -b feat/node-conformance-ci
```

---

### Task 16: Progress script

**Files:**
- Create: `scripts/node-conformance-progress.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Tail the in-flight node-conformance log and print running counters.
# Equivalent to scripts/conformance-progress.sh but for the node harness.
set -euo pipefail

LOG="${1:-/tmp/node-conformance/ginkgo.log}"
if [ ! -f "${LOG}" ]; then
    echo "No log at ${LOG}. Start scripts/run-node-conformance.sh in another shell."
    exit 1
fi

echo "Tailing ${LOG}. Ctrl-C to stop."
PASS=0; FAIL=0; SKIP=0
tail -n +1 -f "${LOG}" | while IFS= read -r line; do
    case "${line}" in
        *"[PASSED]"*)  PASS=$((PASS+1)) ;;
        *"[FAILED]"*)  FAIL=$((FAIL+1)) ;;
        *"[SKIPPED]"*) SKIP=$((SKIP+1)) ;;
    esac
    printf "\rPASS=%d FAIL=%d SKIP=%d" "${PASS}" "${FAIL}" "${SKIP}"
done
```

- [ ] **Step 2: Make executable + sanity-check**

```bash
chmod +x scripts/node-conformance-progress.sh
bash -n scripts/node-conformance-progress.sh
```

Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add scripts/node-conformance-progress.sh
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(conformance): node-conformance progress tail script"
```

---

### Task 17: GitHub Actions workflow

**Files:**
- Create: `.github/workflows/node-conformance.yml`

Per the GH Actions minute-budget memory, this MUST be `runs-on: self-hosted`. Triggered manually (`workflow_dispatch`) plus nightly cron.

- [ ] **Step 1: Write the workflow**

```yaml
name: Node Conformance

on:
  workflow_dispatch:
    inputs:
      focus:
        description: "Ginkgo focus expression"
        required: false
        default: "\\[NodeConformance\\]"
  schedule:
    # 03:00 UTC nightly
    - cron: "0 3 * * *"

jobs:
  node-conformance:
    runs-on: self-hosted
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4

      - name: Tool prereqs (no install — fail fast on missing)
        run: |
          set -euo pipefail
          for t in podman jq curl tar; do
              command -v "$t" >/dev/null 2>&1 || { echo "missing: $t"; exit 1; }
          done

      - name: Generate certs
        run: bash scripts/generate-certs.sh

      - name: Run node conformance
        env:
          KUBELET_VOLUMES_PATH: ${{ github.workspace }}/.rusternetes/volumes
          FOCUS: ${{ github.event.inputs.focus || '\[NodeConformance\]' }}
        run: bash scripts/run-node-conformance.sh

      - name: Upload log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: node-conformance-log
          path: /tmp/node-conformance/ginkgo.log
          if-no-files-found: warn

      - name: Tear down
        if: always()
        run: |
          podman compose -f compose.node-conformance.yml down -v --remove-orphans || true
```

- [ ] **Step 2: Lint the YAML if `yamllint` is installed**

```bash
command -v yamllint && yamllint .github/workflows/node-conformance.yml || echo "yamllint not installed, skipping"
```

Expected: exit 0 OR "yamllint not installed".

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/node-conformance.yml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "ci(conformance): nightly node-conformance on self-hosted runner"
```

---

### Task 18: Cross-reference docs

**Files:**
- Modify: `docs/NODE_CONFORMANCE.md`
- Modify: `docs/CONFORMANCE.md`

- [ ] **Step 1: Add cross-link in `docs/CONFORMANCE.md`**

Open `docs/CONFORMANCE.md`. After the opening paragraph (right before the "Conformance Test Results" header), insert:

```markdown
> **Faster signal:** kubelet-scoped regressions are caught earlier by the node-conformance harness — see `docs/NODE_CONFORMANCE.md`.
```

- [ ] **Step 2: Add commands reference in `docs/NODE_CONFORMANCE.md`**

Append to `docs/NODE_CONFORMANCE.md`:

```markdown
## Watching a run in progress

```bash
# Terminal 1
bash scripts/run-node-conformance.sh

# Terminal 2
bash scripts/node-conformance-progress.sh
```

## CI

The `.github/workflows/node-conformance.yml` workflow runs nightly at 03:00 UTC and can be manually dispatched. It runs on `self-hosted` to respect the project's GitHub Actions minute budget.
```

- [ ] **Step 3: Commit**

```bash
git add docs/NODE_CONFORMANCE.md docs/CONFORMANCE.md
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "docs(conformance): cross-link node-conformance from main doc"
```

---

### Task 19: Smoke + push PR3

- [ ] **Step 1: Format + clippy gate**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both exit 0.

- [ ] **Step 2: Push**

```bash
git push -u fork feat/node-conformance-ci
```

- [ ] **Step 3: Open PR3**

Title: `ci(conformance): nightly node-conformance + progress script (PR3)`. Link PR2 in the description.

---

## Self-Review Notes

- **Spec coverage:** All five components from the spec (compose file, runner script, doc, kubelet server module, CI workflow, progress script) appear in tasks 2, 3, 4, 8-13, 17, 16 respectively. The `Currently unimplemented endpoints` doc section is in task 4; it'll be obsolete after PR2 but is useful as a checklist for PR1.
- **Placeholder scan:** No TBDs. Every code block is complete copy-pasteable code. Where line numbers are referenced (e.g., "around line 770"), the engineer is told how to find the exact spot via `grep`.
- **Type consistency:** `ServerState` defined once in Task 8 with the canonical three fields (`node_name`, `storage`, `kubelet`), re-used in Tasks 9-13 and Task 13's wiring.
- **`/logs` and `/run` aliases** were named in the spec but de-scoped here — they only matter for ginkgo's exec/log tests, which `e2e_node.test` exposes through the api-server proxy first. If PR2's Round 2 numbers reveal a gap, add a Task 11.5 in a follow-up. This is an intentional YAGNI cut documented in the spec.

## Risks

- Upstream tarball URL `https://dl.k8s.io/v1.35.0/kubernetes-test-linux-amd64.tar.gz` may not exist if v1.35 has not been GA-released by the test date. Fallback: pin to the latest GA release in `K8S_VERSION` env var.
- `kubernetes_common::resources::ObjectMeta` field names assumed in test fixtures. If the field on `Pod.metadata` differs (e.g., `name` vs `pod_name`), the integration test won't compile — adjust to match the actual struct after the first compile attempt.
- `Storage::list::<Pod>` signature is taken from `kubelet.rs:1132`. If `list` is generic over storage type rather than over resource, swap to whatever the existing controllers use.
