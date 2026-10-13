//! Rusternetes API client (client-go-lite): HTTP ops, kubeconfig and
//! in-cluster auth, typed watch, reflector/informer.

pub mod api_status;
pub mod config;
pub mod events;
pub mod http;
pub mod kubeconfig;
pub mod ratelimit;
pub mod reflector;
pub mod watch;
