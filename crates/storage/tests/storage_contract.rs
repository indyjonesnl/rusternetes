//! Every `Storage` backend must satisfy the same contract. See
//! `contract/mod.rs` for the provenance of these tests.

mod contract;

// MemoryStorage keeps a monotonic write counter and stamps
// `metadata.resourceVersion` from it (#1942), and an undo log of each retained
// write's previous value, so it serves a list as a snapshot at a past revision
// (#2684).
contract_suite!(memory, async { crate::contract::fixtures::memory() }, revisions: true, snapshot_paging: true);
contract_suite!(etcd, crate::contract::fixtures::etcd(), revisions: true, snapshot_paging: true);
contract_suite!(kine, crate::contract::fixtures::kine(), revisions: true, snapshot_paging: true);

// The rhino/SQLite backend the cluster actually runs. Feature-gated, so a
// default `cargo test -p rusternetes-storage` compiles it out; CI's nextest
// job builds with all features and runs it.
#[cfg(feature = "sqlite")]
contract_suite!(rhino_sqlite, crate::contract::fixtures::rhino_sqlite(), revisions: true, snapshot_paging: true);
