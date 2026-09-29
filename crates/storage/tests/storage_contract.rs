//! Every `Storage` backend must satisfy the same contract. See
//! `contract/mod.rs` for the provenance of these tests.

mod contract;

// MemoryStorage now keeps a monotonic write counter and stamps
// `metadata.resourceVersion` from it (#1942), so it satisfies the revision
// rows. It still keeps no per-revision history, so it cannot serve a paged
// list as a snapshot at a past revision -- that gap is real and tracked, not
// asserted away.
contract_suite!(memory, async { crate::contract::fixtures::memory() }, revisions: true, snapshot_paging: false);
contract_suite!(etcd, crate::contract::fixtures::etcd(), revisions: true, snapshot_paging: true);
contract_suite!(kine, crate::contract::fixtures::kine(), revisions: true, snapshot_paging: true);

// The rhino/SQLite backend the cluster actually runs. Feature-gated, so a
// default `cargo test -p rusternetes-storage` compiles it out; the
// storage-contract workflow builds with `--features sqlite`.
#[cfg(feature = "sqlite")]
contract_suite!(rhino_sqlite, crate::contract::fixtures::rhino_sqlite(), revisions: true, snapshot_paging: true);
