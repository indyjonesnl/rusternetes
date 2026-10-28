//! Controllers shared between the kube-apiserver and the controller-manager.
//!
//! Upstream runs the aggregator's APIService availability controller and the
//! ClusterRole aggregation controller inside kube-apiserver (post-start hooks),
//! and the controller-manager hosts the rest. They live in this small crate so
//! the api-server does not have to depend on (and recompile with) the whole
//! controller-manager. `rusternetes-controller-manager` re-exports these
//! modules at their original `controllers::*` paths.

pub mod apiservice;
pub mod clusterrole_aggregation;
pub mod worker_pool;
