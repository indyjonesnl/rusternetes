//! Function declarations and overload resolution of the CEL environment a DRA
//! device selector is compiled in (`cel_dra`).
//!
//! The implementation moved to the `k3rs-cel-checker` crate (type
//! model, `checker/types.go` unification, `resolveOverload`, and the standard
//! plus library overload table; tracking issue #3440). This module re-exports
//! it so `cel_dra` is unchanged.

pub use k3rs_cel_checker::{
    format_signature, is_assignable, is_macro, lookup, resolve, table, DType, Overload, Resolution,
};
