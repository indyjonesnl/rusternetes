//! Secure serving for the controller-manager and scheduler: TLS, delegated
//! authentication and authorization, and the handler chain that guards
//! `/metrics`, `/statusz` and `/flagz` (#3437).
//!
//! Port of `k8s.io/apiserver/pkg/server/options` (serving, authentication,
//! authorization) plus `server.BuildHandlerChain` as called from
//! `cmd/kube-controller-manager/app/controllermanager.go:236` and
//! `cmd/kube-scheduler/app/server.go:250-254`.

pub mod authn;
pub mod authz;
pub mod filters;
pub mod options;
pub mod serve;
pub mod webhook;
