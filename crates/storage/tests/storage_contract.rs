//! Every `Storage` backend must satisfy the same contract. See
//! `contract/mod.rs` for the provenance of these tests.

mod contract;

contract_suite!(memory, async { crate::contract::fixtures::memory() });
contract_suite!(etcd, crate::contract::fixtures::etcd());
contract_suite!(kine, crate::contract::fixtures::kine());
