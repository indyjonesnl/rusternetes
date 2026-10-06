//! The Service registry — ports of `pkg/registry/core/service` (strategy),
//! its `storage` (the Store hooks), `allocator` and `portallocator`.

pub mod alloc;
pub mod allocator;
pub mod ipallocator;
pub mod ipranges;
pub mod portallocator;
pub mod storage;
pub mod strategy;
