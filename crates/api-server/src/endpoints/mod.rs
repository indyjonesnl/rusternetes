//! Port of `staging/src/k8s.io/apiserver/pkg/endpoints/`: the HTTP layer in
//! front of the registry.
//!
//! Upstream serves every built-in resource with the same handful of handlers
//! (`endpoints/handlers/{create,update,patch,delete}.go`), each parameterised
//! by a `RequestScope` and a `rest.Storage` — in practice a
//! `genericregistry.Store`. [`handlers`] is that set, driving
//! [`crate::registry::generic::Store`]. A resource moved onto the Store
//! serves its writes through these instead of hand-rolled handler code
//! (#1990).

pub mod handlers;
