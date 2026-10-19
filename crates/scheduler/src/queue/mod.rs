//! Scheduling queue with QueueingHints — port of upstream
//! `pkg/scheduler/backend/queue/` plus the event/hint types from
//! `pkg/scheduler/framework/types.go` and `interface.go`.
//!
//! - [`events`]: `ActionType`, `EventResource`, `ClusterEvent`,
//!   `QueueingHint`, `QueueingHintFn`, `ClusterEventWithHint`,
//!   `EnqueueExtensions`, the `PodSchedulingPropertiesChange` /
//!   `NodeSchedulingPropertiesChange` extractors.
//! - [`heap`]: the keyed heap behind activeQ/backoffQ.
//! - [`scheduling_queue`]: `PriorityQueue` (activeQ + backoffQ +
//!   unschedulablePods, in-flight events, `isPodWorthRequeuing`).
//! - [`hints`]: per-plugin `EventsToRegister` and `isSchedulableAfter*`
//!   functions, and `buildQueueingHintMap`.

pub mod events;
pub mod heap;
pub mod hints;
pub mod scheduling_queue;
pub mod volume_hints;

pub use events::*;
pub use scheduling_queue::*;
