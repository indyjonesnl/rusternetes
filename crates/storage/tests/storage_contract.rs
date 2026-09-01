//! Every `Storage` backend must satisfy the same contract. See
//! `contract/mod.rs` for the provenance of these tests.

mod contract;

// MemoryStorage has no revision concept: no resourceVersion on create, and
// current_revision() is a wall-clock timestamp. Tracked as a real gap.
contract_suite!(memory, async { crate::contract::fixtures::memory() }, revisions: false);
contract_suite!(etcd, crate::contract::fixtures::etcd(), revisions: true);
contract_suite!(kine, crate::contract::fixtures::kine(), revisions: true);
