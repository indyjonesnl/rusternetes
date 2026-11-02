//! The cel-go overload ids the cost estimator switches on.
//!
//! cel-go declares every overload under a hand-written id
//! (`common/overloads/overloads.go`) and `checker/cost.go` `functionCost`
//! keys its O(n) / O(nm) cost rules on a handful of them. This crate
//! synthesizes `<function>(<arg types>)` ids for the table (see
//! [`crate::Overload::id`]), so the ids `functionCost` reads are mapped here,
//! from the function name, call style and argument types of the declaration.
//!
//! Only the ids `functionCost` and `CostEstimator.EstimateCallCost` use are
//! mapped (the Kubernetes estimator switches on function names, not ids);
//! every other overload keeps its synthetic id, which `functionCost` prices
//! as O(1) exactly as it prices a cel-go overload it has no rule for.

use crate::types::DType;

/// `overloads.Conditional`.
pub const CONDITIONAL: &str = "conditional";
/// `overloads.LogicalAnd`.
pub const LOGICAL_AND: &str = "logical_and";
/// `overloads.LogicalOr`.
pub const LOGICAL_OR: &str = "logical_or";
/// `overloads.Equals`.
pub const EQUALS: &str = "equals";
/// `overloads.NotEquals`.
pub const NOT_EQUALS: &str = "not_equals";
pub const LESS_STRING: &str = "less_string";
pub const LESS_BYTES: &str = "less_bytes";
pub const LESS_EQUALS_STRING: &str = "less_equals_string";
pub const LESS_EQUALS_BYTES: &str = "less_equals_bytes";
pub const GREATER_STRING: &str = "greater_string";
pub const GREATER_BYTES: &str = "greater_bytes";
pub const GREATER_EQUALS_STRING: &str = "greater_equals_string";
pub const GREATER_EQUALS_BYTES: &str = "greater_equals_bytes";
pub const ADD_STRING: &str = "add_string";
pub const ADD_BYTES: &str = "add_bytes";
pub const ADD_LIST: &str = "add_list";
pub const INDEX_LIST: &str = "index_list";
pub const INDEX_MAP: &str = "index_map";
pub const IN_LIST: &str = "in_list";
pub const IN_MAP: &str = "in_map";
/// `overloads.ExtQuoteString`.
pub const EXT_QUOTE_STRING: &str = "strings_quote";
pub const CONTAINS_STRING: &str = "contains_string";
pub const ENDS_WITH_STRING: &str = "ends_with_string";
pub const MATCHES_STRING: &str = "matches_string";
pub const STARTS_WITH_STRING: &str = "starts_with_string";
/// `overloads.ExtFormatString`.
pub const EXT_FORMAT_STRING: &str = "string_format";
pub const STRING_TO_BYTES: &str = "string_to_bytes";
pub const BYTES_TO_STRING: &str = "bytes_to_string";
/// `overloads.TimestampToYear`: the one overload id the `checker/cost_test.go`
/// estimator prices.
pub const TIMESTAMP_TO_YEAR: &str = "timestamp_to_year";

/// The cel-go id of the overload of `function` taking `args` (the receiver
/// first for a member overload), when `functionCost` or the index-path
/// tracking of `costCall` has a rule for it.
pub fn cel_go_overload_id(function: &str, member: bool, args: &[DType]) -> Option<&'static str> {
    let first = args.first();
    let is = |t: &DType| first == Some(t);
    let is_list = matches!(first, Some(DType::List(_)));
    let is_map = matches!(first, Some(DType::Map(..)));
    // The ordering operators have string and bytes overloads.
    let ordering = |s: &'static str, b: &'static str| match first {
        Some(DType::Str) => Some(s),
        Some(DType::Bytes) => Some(b),
        _ => None,
    };
    match function {
        "_?_:_" => Some(CONDITIONAL),
        "_&&_" => Some(LOGICAL_AND),
        "_||_" => Some(LOGICAL_OR),
        "_==_" => Some(EQUALS),
        "_!=_" => Some(NOT_EQUALS),
        "_<_" => ordering(LESS_STRING, LESS_BYTES),
        "_<=_" => ordering(LESS_EQUALS_STRING, LESS_EQUALS_BYTES),
        "_>_" => ordering(GREATER_STRING, GREATER_BYTES),
        "_>=_" => ordering(GREATER_EQUALS_STRING, GREATER_EQUALS_BYTES),
        "_+_" => match first {
            Some(DType::Str) => Some(ADD_STRING),
            Some(DType::Bytes) => Some(ADD_BYTES),
            _ if is_list => Some(ADD_LIST),
            _ => None,
        },
        "_[_]" if is_list => Some(INDEX_LIST),
        "_[_]" if is_map => Some(INDEX_MAP),
        "@in" => match args.get(1) {
            Some(DType::List(_)) => Some(IN_LIST),
            Some(DType::Map(..)) => Some(IN_MAP),
            _ => None,
        },
        // The global `matches` overload keeps the id `matches`, which has no
        // cost rule.
        "matches" if member && is(&DType::Str) => Some(MATCHES_STRING),
        "contains" if member && is(&DType::Str) => Some(CONTAINS_STRING),
        "endsWith" if member && is(&DType::Str) => Some(ENDS_WITH_STRING),
        "startsWith" if member && is(&DType::Str) => Some(STARTS_WITH_STRING),
        "bytes" if args.len() == 1 && is(&DType::Str) => Some(STRING_TO_BYTES),
        "string" if args.len() == 1 && is(&DType::Bytes) => Some(BYTES_TO_STRING),
        "format" if member && is(&DType::Str) => Some(EXT_FORMAT_STRING),
        "strings.quote" => Some(EXT_QUOTE_STRING),
        "getFullYear" if member && args == [DType::Timestamp] => Some(TIMESTAMP_TO_YEAR),
        _ => None,
    }
}
