//! Every `Storage` backend must satisfy the same contract. See
//! `contract/mod.rs` for the provenance of these tests.

mod contract;

// MemoryStorage has no revision concept: no resourceVersion on create, and
// current_revision() is a wall-clock timestamp. It therefore cannot serve a
// paged list as a snapshot at a past revision either. Both gaps are real and
// tracked, not asserted away.
contract_suite!(memory, async { crate::contract::fixtures::memory() }, revisions: false, snapshot_paging: false);
contract_suite!(etcd, crate::contract::fixtures::etcd(), revisions: true, snapshot_paging: true);
contract_suite!(kine, crate::contract::fixtures::kine(), revisions: true, snapshot_paging: true);
