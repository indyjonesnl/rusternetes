//! The `OverloadCostEstimate`s of cel-go's `ext` libraries that the
//! Kubernetes environments include (`ext/sets.go` `setsLib.CompileOptions`,
//! `ext/lists.go` `listsLib.CompileOptions` at version 3): the overloads whose
//! cost is O(n), O(n*m) or O(n^2) in the list sizes. They override the
//! [`CostEstimator`] for their overload ids (`checker.OverloadCostEstimate`).

use crate::cost::{
    AstNode, CallEstimate, CostEstimate, CostEstimator, FunctionEstimator, OverloadEstimators,
    SizeEstimate, LIST_CREATE_BASE_COST, STRING_TRAVERSAL_COST_FACTOR,
};
use crate::types::DType;
use cel::common::ast::{Expr, LiteralValue};

/// `callCostEstimate`.
fn call_cost_estimate() -> CostEstimate {
    CostEstimate::fixed(1)
}

/// `estimateSize` (`ext/sets.go`).
fn estimate_size(estimator: &dyn CostEstimator, node: &AstNode<'_>) -> SizeEstimate {
    if let Some(l) = node.computed_size {
        return l;
    }
    if let Some(l) = estimator.estimate_size(node) {
        return l;
    }
    SizeEstimate::UNKNOWN
}

/// `estimateSetsCost`.
fn estimate_sets_cost(cost_factor: f64) -> FunctionEstimator {
    Box::new(move |estimator, _target, args| {
        if args.len() != 2 {
            return None;
        }
        let a0 = estimate_size(estimator, &args[0]);
        let a1 = estimate_size(estimator, &args[1]);
        let cost = a0
            .multiply(a1)
            .multiply_by_cost_factor(cost_factor)
            .add(call_cost_estimate());
        Some(CallEstimate {
            cost,
            result_size: None,
        })
    })
}

/// `nodeAsIntValue`: the value of an integer literal node (0 when negative),
/// else `default`.
fn node_as_int_value(node: &AstNode<'_>, default: u64) -> u64 {
    match node.expr.map(|e| &e.expr) {
        Some(Expr::Literal(LiteralValue::Int(i))) => (*i.inner()).max(0) as u64,
        _ => default,
    }
}

/// `estimateListCall`.
fn estimate_list_call(cost_factor: f64, list_size: SizeEstimate, allocates: bool) -> CallEstimate {
    let mut cost = list_size
        .multiply_by_cost_factor(cost_factor)
        .add(call_cost_estimate());
    if allocates {
        cost = cost.add(CostEstimate::fixed(LIST_CREATE_BASE_COST));
    }
    CallEstimate {
        cost,
        result_size: Some(list_size),
    }
}

/// `estimateAllocatingListCall`.
fn estimate_allocating_list_call(cost_factor: f64, list_size: SizeEstimate) -> CallEstimate {
    estimate_list_call(cost_factor, list_size, true)
}

/// `estimateListSlice`.
fn estimate_list_slice(
    estimator: &dyn CostEstimator,
    target: Option<&AstNode<'_>>,
    args: &[AstNode<'_>],
) -> Option<CallEstimate> {
    let target = target?;
    if args.len() != 2 {
        return None;
    }
    let sz = estimate_size(estimator, target);
    let start = node_as_int_value(&args[0], 0);
    let end = node_as_int_value(&args[1], sz.max);
    Some(estimate_allocating_list_call(
        1.0,
        SizeEstimate::fixed(end.wrapping_sub(start)),
    ))
}

/// `estimateListsRange`.
fn estimate_lists_range(
    _: &dyn CostEstimator,
    target: Option<&AstNode<'_>>,
    args: &[AstNode<'_>],
) -> Option<CallEstimate> {
    if target.is_some() || args.len() != 1 {
        return None;
    }
    Some(estimate_allocating_list_call(
        1.0,
        SizeEstimate::fixed(node_as_int_value(&args[0], u64::MAX)),
    ))
}

/// `estimateListReverse`.
fn estimate_list_reverse(
    estimator: &dyn CostEstimator,
    target: Option<&AstNode<'_>>,
    args: &[AstNode<'_>],
) -> Option<CallEstimate> {
    let target = target?;
    if !args.is_empty() {
        return None;
    }
    Some(estimate_allocating_list_call(
        1.0,
        estimate_size(estimator, target),
    ))
}

/// `estimateListFlatten`: an O(n) flatten with a cost factor proportional to
/// the flatten depth.
fn estimate_list_flatten(
    estimator: &dyn CostEstimator,
    target: Option<&AstNode<'_>>,
    args: &[AstNode<'_>],
) -> Option<CallEstimate> {
    let target = target?;
    if args.len() > 1 {
        return None;
    }
    let mut depth = 1u64;
    if args.len() == 1 {
        depth = node_as_int_value(&args[0], u64::MAX);
    }
    Some(estimate_allocating_list_call(
        depth as f64,
        estimate_size(estimator, target),
    ))
}

/// `estimateListDistinct`: an O(n^2) with a cost factor of 2, equivalent to
/// `sets.contains` with a result list which can vary in size from 1 element to
/// the original list size.
fn estimate_list_distinct(
    estimator: &dyn CostEstimator,
    target: Option<&AstNode<'_>>,
    args: &[AstNode<'_>],
) -> Option<CallEstimate> {
    let target = target?;
    if !args.is_empty() {
        return None;
    }
    let sz = estimate_size(estimator, target);
    Some(estimate_allocating_list_call(2.0, sz.multiply(sz)))
}

/// `estimateListSortCost`: an O(n^2) sort with a cost factor of 2 for the
/// equality operations which occur during the sort.
fn estimate_list_sort_cost(
    estimator: &dyn CostEstimator,
    node: &AstNode<'_>,
    elem_is_string_or_bytes: bool,
) -> CallEstimate {
    let sz = estimate_size(estimator, node);
    let mut cost_factor = 2.0;
    if elem_is_string_or_bytes {
        cost_factor += STRING_TRAVERSAL_COST_FACTOR;
    }
    estimate_allocating_list_call(cost_factor, sz.multiply(sz))
}

/// `estimateListSort(t)`.
fn estimate_list_sort(elem_is_string_or_bytes: bool) -> FunctionEstimator {
    Box::new(move |estimator, target, args| {
        let target = target?;
        if !args.is_empty() {
            return None;
        }
        Some(estimate_list_sort_cost(
            estimator,
            target,
            elem_is_string_or_bytes,
        ))
    })
}

/// `estimateListSortBy(u)`: an O(n^2) sort with a cost factor of 2 for the
/// equality operations against the sort index list.
fn estimate_list_sort_by(elem_is_string_or_bytes: bool) -> FunctionEstimator {
    Box::new(move |estimator, target, args| {
        target?;
        if args.len() != 1 {
            return None;
        }
        // The size of the list used as the sort index.
        Some(estimate_list_sort_cost(
            estimator,
            &args[0],
            elem_is_string_or_bytes,
        ))
    })
}

/// `ext.Sets()` and `ext.Lists(ext.ListsVersion(3))`'s
/// `cel.CostEstimatorOptions`.
pub fn ext_overload_estimators() -> OverloadEstimators {
    let mut m: OverloadEstimators = OverloadEstimators::new();
    m.insert("list_sets_contains_list".into(), estimate_sets_cost(1.0));
    m.insert("list_sets_intersects_list".into(), estimate_sets_cost(1.0));
    // Equivalence requires potentially two m*n comparisons to ensure each
    // list is contained by the other.
    m.insert("list_sets_equivalent_list".into(), estimate_sets_cost(2.0));
    m.insert("list_slice".into(), Box::new(estimate_list_slice));
    m.insert("list_flatten".into(), Box::new(estimate_list_flatten));
    m.insert("list_flatten_int".into(), Box::new(estimate_list_flatten));
    m.insert("lists_range".into(), Box::new(estimate_lists_range));
    m.insert("list_reverse".into(), Box::new(estimate_list_reverse));
    m.insert("list_distinct".into(), Box::new(estimate_list_distinct));
    // `comparableTypes`; the type names are `types.Type.TypeName()`.
    for t in [
        DType::Int,
        DType::Uint,
        DType::Double,
        DType::Bool,
        DType::Duration,
        DType::Timestamp,
        DType::Str,
        DType::Bytes,
    ] {
        let name = comparable_type_name(&t);
        let by_traversal = matches!(t, DType::Str | DType::Bytes);
        m.insert(
            format!("list_{name}_sort"),
            estimate_list_sort(by_traversal),
        );
        m.insert(
            format!("list_{name}_sortByAssociatedKeys"),
            estimate_list_sort_by(by_traversal),
        );
    }
    m
}

/// `types.Type.TypeName()` of a comparable type, as the overload ids embed it.
pub fn comparable_type_name(t: &DType) -> &'static str {
    match t {
        DType::Int => "int",
        DType::Uint => "uint",
        DType::Double => "double",
        DType::Bool => "bool",
        DType::Duration => "google.protobuf.Duration",
        DType::Timestamp => "google.protobuf.Timestamp",
        DType::Str => "string",
        DType::Bytes => "bytes",
        _ => "",
    }
}
