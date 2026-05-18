# Missing Features — storage

## Scope

This document compares the Rusternetes `crates/storage` module (the
`Storage` trait, the etcd / rhino / memory backends, the work queue, and
the small concurrency helper) against upstream Kubernetes
`staging/src/k8s.io/apiserver/pkg/storage` — primarily
`interfaces.go`, the `etcd3/` package, the `value/` transformer chain
(encryption at rest), and the `cacher/` watch-cache layer.

The api-server's own `WatchCache` (`crates/api-server/src/watch_cache.rs`)
sits one layer above the storage trait; it is in scope here only where
upstream pushes equivalent functionality down into the storage interface
itself (consistent reads, bookmarks, progress notify, list-from-cache).

Out of scope: the higher-level `genericregistry` / `RESTStorage` strategy
glue (that belongs with api-server), and the storage-version-migration
controller (`storage-version-migrator/`).

## Current Rusternetes state

The `Storage` trait is intentionally small (11 methods, all defined
once in `crates/storage/src/lib.rs:28-141`) and is implemented by:

- `EtcdStorage` — `crates/storage/src/etcd.rs:14-586`, raw etcd v3 gRPC
  via the `etcd-client` crate. Uses a single shared `Client` (cheap to
  clone because tonic multiplexes over one HTTP/2 connection),
  `Compare::version`-guarded create transactions
  (`etcd.rs:90-105`), `Compare::mod_revision`-guarded update
  transactions (`etcd.rs:170-212`), and prefix-paginated list with a
  500-key page size (`etcd.rs:313-396`).
- `RhinoStorage<B>` — `crates/storage/src/rhino.rs:28-403`, generic
  over a `rhino::backend::Backend` (currently `SqliteBackend` or
  `RedisBackend`, both behind `sqlite` / `redis` Cargo features —
  `Cargo.toml`). Rhino exposes a near-etcd-shaped backend API
  (`create`, `get`, `update` with CAS, `delete`, `list`, `watch`,
  `current_revision`) so the implementation mirrors `EtcdStorage`
  closely.
- `MemoryStorage` — `crates/storage/src/memory.rs:25-268`,
  `HashMap<String, String>` behind a `RwLock` plus a single
  `tokio::sync::broadcast` channel for watch events. Used exclusively
  for unit / integration tests; offers test hooks
  `inject_conflicts(n)` and `compact_to(rv)`
  (`memory.rs:41-65`).
- `StorageBackend` — `crates/storage/src/lib.rs:306-499`, runtime
  dispatch enum so the rest of the codebase can remain generic over
  `S: Storage` while the concrete backend is selected once at startup.

Keys follow `/registry/{resource_type}/{namespace}/{name}` for
namespaced resources and `/registry/{resource_type}/{name}` for
cluster-scoped resources (`lib.rs:534-547`). `resourceVersion` is the
etcd / rhino `mod_revision` rendered as a decimal string and injected
into `metadata.resourceVersion` on every read
(`etcd.rs:48-60`, `rhino.rs:95-106`,
`concurrency.rs:36-44`). The continue token format is `c1:<rv>:<key>`
(`lib.rs:174-199`) and pagination returns `Error::Gone` when the token
refers to a compacted revision (`lib.rs:96-105`).

Watch events are a three-variant enum — `Added(key, value)`,
`Modified(key, value)`, `Deleted(key, prev_value)`
(`lib.rs:202-207`). `Storage::watch_from_revision` is implemented for
etcd (`etcd.rs:402-486`) and rhino (`rhino.rs:343-386`); for
`MemoryStorage` it transparently delegates to `watch` with no history
replay (`memory.rs:229-232`).

Auxiliary modules: `concurrency.rs` (54 lines, just RV ↔ mod_revision
helpers) and `workqueue.rs` (515 lines, a client-go-style rate-limited
work queue with backoff, dedup, and `last_dequeued` self-write-loop
protection — `workqueue.rs:78-94`).

## Parity matrix

| Feature | Upstream | Rusternetes | Notes |
| --- | --- | --- | --- |
| CRUD primitives | `Create`, `Get`, `Delete`, `GetList` | `create`, `get`, `delete`, `list` | `lib.rs:30-53` |
| Recursive list | `ListOptions.Recursive` | always prefix-scan | etcd backend pages at 500 keys (`etcd.rs:313-396`) |
| Paginated list (chunking) | `ListOptions.Limit` + `Continue` token | `list_paginated` w/ `c1:<rv>:<key>` token | `lib.rs:73-128` |
| Resume token compaction guard | etcd returns `ErrCompacted` → 410 Gone | `is_revision_compacted` check → `Error::Gone` | `lib.rs:96-105`, `etcd.rs:567-585` |
| Optimistic concurrency | `Preconditions{UID, ResourceVersion}` + `GuaranteedUpdate` | `Compare::mod_revision` txn | `etcd.rs:170-212`, `rhino.rs:185-208` |
| `GuaranteedUpdate` retry loop | yes, in-storage | NO — caller-side retry only | gap; see Missing #1 |
| Preconditions: UID | yes | NO | gap; api-server handler must check separately |
| `Versioner` abstraction | yes (object/list RV setters) | string-injection helper | `concurrency.rs:5-44` |
| Watch (current rev) | `Watch` (RV=0 or "") | `watch` | `lib.rs:131`, `etcd.rs:488-555` |
| Watch from revision | `Watch` with explicit RV | `watch_from_revision` | `lib.rs:134`, `etcd.rs:402-486` |
| `prev_kv` on DELETE | yes (etcd `WithPrevKV`) | yes (etcd + rhino) | `etcd.rs:492`, `rhino.rs:363-371` |
| Bookmark events | yes (`Bookmark` watch.Event type) | NO | gap; see Missing #2 |
| Progress notify | `RequestWatchProgress` | NO | gap; see Missing #2 |
| `SendInitialEvents` (KEP-3157) | yes | NO | gap; see Missing #3 |
| Consistent reads (RV match `Exact` / `NotOlderThan`) | yes | NO | always reads at latest, no `ResourceVersionMatch` plumbing |
| List-from-cache (watch cache fan-out) | `cacher.Cacher` in front of every store | partial — `WatchCache` in api-server only multiplexes *watches*, lists still hit storage | `crates/api-server/src/watch_cache.rs:1-100` |
| Encryption at rest | `value.Transformer` chain (identity / aescbc / aesgcm / secretbox / kms v1 / kms v2) | NO — plaintext JSON on disk | gap; see Missing #4 |
| KMS v2 plugin protocol | gRPC w/ `key_id` annotations, automatic key rotation | NO | gap; see Missing #4 |
| TTL / lease (`Create` with TTL) | `lease_manager.go` | NO — TTL parameter not on trait | gap; see Missing #5 |
| Periodic compaction trigger | `compact.go` (5-minute loop) | etcd: external responsibility; rhino: `compact_interval: 300s` (`rhino.rs:41-43`) | partial |
| Defragmentation | not in apiserver (etcdctl / etcd) | not exposed | n/a |
| Readiness / health check | `ReadinessCheck()` on the Interface | NO | gap; api-server `/readyz` only checks that storage is constructed |
| Latency / blocked-op metrics | `latency_tracker.go`, `block_logger.go`, `metrics/` | NO Prometheus counters | gap; see Missing #6 |
| Corrupt-object handling | `corrupt_obj_deleter.go` + `ExpectTransformOrDecodeError` | NO — failed deserialize is silently skipped on list (`etcd.rs:372-376`) | gap; see Missing #7 |
| Index hints (`MatchValue`) | `IndexerFunc` + `MatchValue` (used by trigger-by-namespace optimisation) | NO | gap |
| Transaction API surface (`clientv3.Txn`) | exposed indirectly through `GuaranteedUpdate` | NO — etcd Txn is used internally but not callable by handlers | gap; see Missing #8 |
| Decoder caching | `decoder.go` reuses object pool | every read does `serde_json::from_str` afresh | minor perf gap |
| `Stats()` (DB size, key count) | yes | NO | gap |
| `GetCurrentResourceVersion()` | yes | yes (`current_revision`) | `lib.rs:137` |
| `CompactRevision()` (last-observed compacted RV) | yes | NO (only `is_revision_compacted(rv)`) | minor gap |
| `EnableResourceSizeEstimation` (KeysFunc async) | yes | NO | minor gap |
| Storage version migration (`storage-version-migrator`) | yes, separate controller | NO | out of scope (controller-manager) |
| Multi-tenant key prefix (`--etcd-prefix`) | yes | hard-coded `/registry/` (`lib.rs:534-547`) | gap |

## Missing features

### 1. `GuaranteedUpdate` in-storage retry loop with `UpdateFunc`

**Upstream.** `Interface.GuaranteedUpdate(ctx, key, ptr, ignoreNotFound,
preconditions, tryUpdate UpdateFunc, cachedExistingObject)` lives in
`staging/src/k8s.io/apiserver/pkg/storage/interfaces.go` and is the
canonical mutating primitive. Its contract: on `ErrConflict` the
storage layer re-reads the object, re-invokes `tryUpdate`, and retries
the CAS — invisibly to the caller. `etcd3/store.go` implements this
with an inner `for` loop, optional `cachedExistingObject` to skip the
first read, and `Preconditions{UID, ResourceVersion}` enforced before
`tryUpdate` is called.

**Rusternetes today.** `Storage::update` (`lib.rs:40-42`) is a single
CAS attempt. On `Error::Conflict` the caller — every handler, every
controller — is responsible for re-reading, re-mutating, and
retrying. `RhinoStorage::update` does a one-shot retry only on the
*no-RV-provided* branch (`rhino.rs:230-253`); the with-RV branch
returns `Conflict` immediately (`rhino.rs:204-208`). `Preconditions`
do not exist at all; UID checks are open-coded in some handlers and
missing in others.

**Why it matters.** Every controller hand-rolls its own retry loop.
The grep target `Error::Conflict` shows the same `for _ in 0..5 { ... }`
pattern duplicated across reconcilers. Each duplicate is a candidate
for subtle bugs (forgetting to clear stale state between retries,
losing the `cachedExistingObject` optimisation, racing with deletion).

**Effort.** Medium. Add `Storage::guaranteed_update<T, F: FnMut(T) ->
Result<T>>` with default `Preconditions`. Implement on `EtcdStorage`
and `RhinoStorage` using a bounded retry loop (default 8). Migrate
hottest reconcilers (deployment, statefulset, replicaset) first;
leave the trait-level retry alongside the existing single-shot
`update` so the migration is gradual.

### 2. Bookmark events and `RequestWatchProgress`

**Upstream.** `metav1.WatchEventBookmark` is a synthetic event emitted
by the watch cache every ~1 minute (or on demand via
`RequestWatchProgress`) carrying only `resourceVersion`. It lets
clients persist a checkpoint without buffering every change, and lets
the server prove the watch is alive when no real events flow.
KEP-1904 and KEP-3157.

**Rusternetes today.** `WatchEvent` has only `Added` / `Modified` /
`Deleted` (`lib.rs:202-207`). Clients receive nothing between real
events; long-lived watches over quiet prefixes look indistinguishable
from a dead TCP connection. `allow_watch_bookmarks` is parsed in many
handlers (e.g. `crates/api-server/src/handlers/event.rs:41`) but
discarded because the storage layer cannot emit them.

**Why it matters.** Clients that use `metadata.resourceVersion` of the
last bookmark to resume after a disconnect cannot do so without
bookmarks. The watch cache must replay from the earliest in-memory
revision, which is bounded by `HISTORY_CAPACITY` =
500 events (`crates/api-server/src/watch_cache.rs:17`).

**Effort.** Medium. Add a `WatchEvent::Bookmark(rv: i64)` variant.
Emit one every ~60s from `EtcdStorage::watch_from_revision` using
etcd's `--experimental-watch-progress-notify-interval` plus
`RequestWatchProgress` (etcd-client exposes `Client::watch_progress`).
Rhino does not support progress notify yet — needs an upstream change
or a wall-clock timer fallback.

### 3. `SendInitialEvents` / consistent list (KEP-3157, WatchList)

**Upstream.** With `WatchListClient` feature gate, a client requests
`?sendInitialEvents=true&resourceVersionMatch=NotOlderThan`. The
server synthesises an `ADDED` event for each existing object at a
consistent revision, followed by a `Bookmark` marking
`initial-events-end`. Clients build their cache directly off the
watch stream — no separate `LIST` round-trip — saving one full
serialisation pass and giving a strict snapshot at a single RV.

**Rusternetes today.** Not implemented at any layer. `watch_from_revision`
streams events strictly after the supplied revision; the api-server
falls back to `list` + `watch_from_revision(current_rv + 1)`, which
has a known race: an object created between the `LIST` snapshot and
the watch start RV is observed twice (once in the list, once as
`Added`). No `ResourceVersionMatch` parameter is plumbed through the
`Storage` trait at all.

**Why it matters.** This is *the* path forward for scalable controller
caches in modern Kubernetes; client-go is migrating away from
list-then-watch. Conformance suites are starting to assert it.

**Effort.** Large. Requires (a) `ListOptions` parameter struct with
`resource_version: Option<i64>` and
`resource_version_match: Option<RVMatch>`, (b) a
`Storage::list_at_revision` method that uses etcd's
`GetOptions::with_revision`, (c) `WatchEvent::Bookmark` from #2.

### 4. Encryption at rest (transformer chain, KMS v1/v2)

**Upstream.** `value/transformer.go` defines a `Transformer` with
`TransformFromStorage(ctx, data, dataCtx) -> (plaintext, stale, err)`
and `TransformToStorage`. The api-server config
(`--encryption-provider-config`) builds a per-resource
`PrefixTransformer` chain: each provider writes a fixed byte prefix
(`k8s:enc:aescbc:v1:<keyname>:`), and reads try transformers in order
until one's prefix matches. Provider implementations live in
`value/encrypt/`: `aes/`, `aescbc/`, `secretbox/`, and
`envelope/` (KMS v1 gRPC) / `envelope/kmsv2/` (KMS v2, KEP-3299,
gRPC w/ `key_id` rotation and `encryptedKEKs`).

**Rusternetes today.** Every value written to etcd / sqlite / redis is
the raw JSON string (`etcd.rs:39-41`, `rhino.rs:91-93`). `secrets/`,
`configmaps/`, service-account tokens, and webhook-config certs are
all stored in plaintext. Anyone with `etcdctl get --prefix /registry`
can read every Secret in the cluster.

**Why it matters.** This is the single largest security gap in
Rusternetes. Conformance suite `[sig-storage] etcd encryption` will
fail; CIS-Kubernetes benchmark requires it; production deployments
will reject the cluster outright.

**Effort.** Large. Phasing suggestion:
1. Add `trait Transformer { fn transform_from_storage(&self, data: &[u8],
   ctx: &TransformContext) -> Result<(Vec<u8>, bool)>; fn
   transform_to_storage(&self, data: &[u8], ctx: &TransformContext) ->
   Result<Vec<u8>>; }`.
2. Implement `IdentityTransformer` (no-op, current behaviour) and
   `PrefixTransformer` (chain dispatch).
3. Implement `AesGcmTransformer` and `AesCbcTransformer` using
   `aes-gcm` / `aes` crates.
4. Wire `--encryption-provider-config` parsing into
   `StorageBackend::new` and apply the transformer per `resource_type`
   in `EtcdStorage`'s read / write paths.
5. (Later) KMS v2 gRPC client.

### 5. TTL / lease support on `Create`

**Upstream.** `Interface.Create(ctx, key, obj, out, ttl uint64)` accepts
a TTL in seconds. `etcd3/lease_manager.go` batches lease grants
(default 60s grant, refcounted across keys with the same TTL) to
avoid one `LeaseGrant` RPC per object. `Events` is the canonical
caller — every event expires after `ttl-events-after` (default 1h).

**Rusternetes today.** `Storage::create` has no `ttl` parameter
(`lib.rs:30-32`). Events accumulate in etcd forever; the only cleanup
is whatever the event controller does, and there is no
`ttlSecondsAfterFinished` enforcement at the storage layer for
batch/Job either.

**Why it matters.** Events table grows unbounded; etcd performance
degrades; a 30-day-old cluster has tens of thousands of stale events.

**Effort.** Medium. Add `Storage::create_with_ttl(&self, key, value,
ttl_seconds: u64)`. Etcd: implement a tiny `LeaseManager` (HashMap
keyed by TTL bucket, refcount, `LeaseRevoke` on last drop). Rhino:
needs upstream support — rhino's `Backend::create` already accepts a
TTL arg (the `0` passed at `rhino.rs:134`), check whether it honours
it, otherwise add a soft-expiry sweeper.

### 6. Storage-layer metrics (latency, blocked ops, DB size, watch fan-out)

**Upstream.** `etcd3/metrics/` exports per-operation latency histograms
(`etcd_request_duration_seconds`), the `block_logger.go` flags
operations slower than its budget, `stats.go` reports DB size. The
cacher exports `apiserver_watch_cache_events_dispatched_total`,
`apiserver_watch_cache_capacity`, and the high-watermark queue depth.

**Rusternetes today.** Zero storage-layer Prometheus counters.
`tracing::debug!` / `info!` lines exist (`etcd.rs:107`,
`rhino.rs:143`) but there is no metrics registry plumbed through
`StorageBackend`. The api-server's `/metrics` endpoint reports
nothing about storage.

**Why it matters.** Operating Rusternetes in production without
visibility into etcd latency, lock contention, or the rhino
sqlite-checkpoint cost is blind flying. Conformance includes a
metrics smoke test.

**Effort.** Small to medium. Add a `prometheus` registry to
`StorageBackend`; wrap each method in a histogram observer; export
via the existing api-server metrics route.

### 7. Corrupt-object handling and `unsafe` deletion path

**Upstream.** `etcd3/corrupt_obj_deleter.go` plus `ExpectTransformOrDecodeError`
on `DeleteOptions` let an operator remove a key whose value cannot
be deserialised (typically after a schema change or a corrupted KMS
key). The list path counts decode errors as a separate metric and
fails the list rather than silently skipping.

**Rusternetes today.** A failed deserialise on the list path is
swallowed with `error!("Failed to deserialize value at {}: {}", ...,
e); continue;` (`etcd.rs:372-376` and `rhino.rs:320-324`). The
caller never knows the response is incomplete, so e.g. a corrupted
Pod simply disappears from `kubectl get pods`.

**Why it matters.** Silent data loss after a schema mistake is a
debugging nightmare. Upstream chose to fail loudly precisely because
hiding the failure is worse than the symptom.

**Effort.** Small. Bubble decode errors up as `Error::Storage` with a
new `Storage::list_lenient` opt-in if any caller needs the current
behaviour.

### 8. Transaction API surface (multi-key atomic writes)

**Upstream.** While `Interface` does not expose `Txn` directly,
`GuaranteedUpdate` and `Create` already use multi-op transactions
internally, and the API server's quota / namespace / endpoint
controllers depend on cross-key invariants (e.g. "increment quota
status iff Pod create succeeds"). Etcd3 store leans on
`clientv3.Txn` for these.

**Rusternetes today.** `EtcdStorage` uses `etcd_client::Txn`
internally (`etcd.rs:90-105`, `etcd.rs:174-184`) but nothing is
exposed on the `Storage` trait. Multi-key invariants therefore have
to be implemented as best-effort sequential writes with no rollback.

**Why it matters.** ResourceQuota updates and PodDisruptionBudget
counter updates are racy without atomic multi-key writes; quota
overshoot is observable in chaos tests today.

**Effort.** Large. Designing a portable transaction API across etcd +
rhino + memory is non-trivial; rhino does support multi-op txns
(`Backend::txn`) so it's feasible, but the trait shape needs careful
design.

### 9. `--etcd-prefix` / multi-tenant prefix isolation

**Upstream.** The api-server flag `--etcd-prefix` (default
`/registry`) is passed into every storage call as a prefix. This
lets multiple logical clusters share one etcd, and is also how the
admission-webhook test infrastructure isolates state.

**Rusternetes today.** The prefix `/registry/` is hard-coded in
`build_key` / `build_prefix` (`lib.rs:534-547`) and the `AuthzStorage`
helpers (`etcd.rs:596-655`, `rhino.rs:408-475`, `memory.rs:280-330`).
There is no way to run two Rusternetes clusters against one etcd
without key collisions.

**Why it matters.** Test harness flexibility, multi-tenant POCs.

**Effort.** Small. Thread a `prefix: &str` through `StorageConfig`
and have the helpers prepend it.

### 10. `Stats()` / DB-size reporting

**Upstream.** `Interface.Stats()` returns `StorageStats{ObjectCount,
EstimatedAverageObjectSizeBytes}`. The api-server uses this to power
the `apiserver_storage_objects` metric and the
`storage_db_total_size_in_bytes` admission check.

**Rusternetes today.** Not implemented; closest is
`MemoryStorage::len()` (`memory.rs:51-54`) which is test-only.

**Effort.** Small.

## Partial / stubbed

- **Watch fan-out.** `crates/api-server/src/watch_cache.rs` multiplexes
  one storage watch into a `tokio::sync::broadcast` per resource
  prefix with a 500-event history ring buffer
  (`watch_cache.rs:14-48`). This is roughly equivalent to upstream
  `cacher.Cacher` for the *watch* side, but **the list side still
  bypasses the cache** and hits etcd directly. Upstream serves
  `LIST` from the cacher when the client passes `resourceVersion=0`
  (or `NotOlderThan`), which is a major perf win.

- **Compaction.** Rhino has a 5-minute internal compaction loop
  (`rhino.rs:41-43`, `rhino.rs:67-69`). Etcd compaction is the
  operator's responsibility (no `--etcd-compaction-interval` plumbing
  in Rusternetes). Upstream's `compact.go` periodically calls
  `Compact()` from inside the api-server so a fresh cluster
  self-maintains.

- **Pagination on rhino / memory.** `Storage::list_paginated` has a
  default implementation that lists everything and slices in memory
  (`lib.rs:73-128`). Only etcd's native `RangeRequest.limit` could
  override it for efficiency; the override is not yet implemented.

- **`watch_from_revision` on `MemoryStorage`.** Silently ignores the
  revision argument and delegates to `watch` (`memory.rs:229-232`).
  Tests that exercise replay semantics must use the etcd or rhino
  backend.

- **Optimistic concurrency.** The CAS itself is correct
  (`etcd.rs:170-212`, `rhino.rs:185-208`) but no retry, no UID
  precondition, no `cachedExistingObject` shortcut.

## Known in-code TODOs

`rg --no-heading -n 'TODO|FIXME|XXX|HACK' crates/storage/src/` returns
**zero hits** — the crate has no explicit TODOs, which means the gaps
above are not tracked as work items in the source itself. They live
only in this document and (partially) in `docs/api-gap-analysis.md`.

Implicit ones worth flagging:

- `etcd.rs:54-58` — `serde_json::to_string(&v).unwrap_or_else(|_|
  json.to_string())` swallows a serialisation failure. The comment
  claims it is "infallible in practice"; it should be a hard error.
- `etcd.rs:372-376` and `rhino.rs:320-324` — list silently skips
  un-deserialisable values (see Missing #7).
- `etcd.rs:597-619` and `rhino.rs:412-437` — the `AuthzStorage`
  implementation uses `std::any::type_name::<T>()` string matching to
  route to RBAC prefixes. This is genuinely fragile (a rename of
  `ClusterRoleBinding` would silently misroute) and is a refactor
  target for a typed registry.

## References

Upstream code (read 2026-05-18, master @ HEAD):

- `staging/src/k8s.io/apiserver/pkg/storage/interfaces.go` — the
  `Interface` contract, `GuaranteedUpdate`, `UpdateFunc`,
  `Preconditions`, `GetOptions`, `ListOptions`.
- `staging/src/k8s.io/apiserver/pkg/storage/etcd3/{store,watcher,
  lease_manager,compact,healthcheck,latency_tracker,
  corrupt_obj_deleter,event,stats}.go` — etcd3 backend.
- `staging/src/k8s.io/apiserver/pkg/storage/value/{transformer.go,
  encrypt/aes,encrypt/aescbc,encrypt/aesgcm,encrypt/secretbox,
  encrypt/envelope,encrypt/envelope/kmsv2}` — encryption at rest.
- `staging/src/k8s.io/apiserver/pkg/storage/cacher/{cacher,watch_cache,
  cache_watcher,ready,delegator,compactor,lister_watcher}.go` —
  watch cache + list-from-cache.

KEPs:

- KEP-1904 `WatchBookmark` (GA in 1.16) — bookmark events.
- KEP-3157 `WatchList` / consistent reads from the watch cache (Beta).
- KEP-3299 KMS v2 (GA in 1.29) — key rotation, `key_id`.
- KEP-365 storage version migration (controller-manager, out of scope).

Rusternetes code referenced (paths are absolute from repo root):

- `crates/storage/src/lib.rs` — `Storage` trait, `StorageBackend`,
  continue-token codec, key helpers.
- `crates/storage/src/etcd.rs` — etcd backend.
- `crates/storage/src/rhino.rs` — sqlite / redis backend via the
  in-process rhino crate.
- `crates/storage/src/memory.rs` — test backend.
- `crates/storage/src/concurrency.rs` — resourceVersion ↔ mod_revision
  helpers.
- `crates/storage/src/workqueue.rs` — controller work queue (adjacent
  to storage but not part of the storage interface itself).
- `crates/api-server/src/watch_cache.rs` — the layer-above-storage
  watch multiplexer; relevant for items #2 and #3.
