//! Unit tests moved out of `src/` so the lib is not compiled a second time as a test target.
#![allow(non_snake_case)]
#[cfg(feature = "api-client")]
mod api_storage;
mod busy;
mod concurrency;
mod etcd;
mod etcd_endpoint;
mod event_bus;
mod event_recorder;
mod latency;
mod lease_lock;
mod lib;
mod memory;
mod metadata;
mod preconditions;
#[cfg(any(feature = "sqlite", feature = "redis"))]
mod rhino;
mod size_estimator;
mod workqueue;
