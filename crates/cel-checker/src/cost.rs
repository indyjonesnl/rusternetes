//! `checker/cost.go`: the static cost estimator of a type-checked expression.
//!
//! A port of cel-go's `coster` (vendor/github.com/google/cel-go/checker/cost.go,
//! the version Kubernetes release-1.35 vendors), including the overflow-safe
//! [`SizeEstimate`] / [`CostEstimate`] arithmetic, the field-path, size and
//! entry-size tracking that lets an estimator bound the size of a variable
//! length value, and `functionCost`.
//!
//! cel-go warns that "any changes to cost calculations in this file require a
//! corresponding change in `interpreter/runtimecost.go`". Runtime cost tracking
//! is not part of this crate; only the estimate is.
//!
//! Not ported: `OverloadCostEstimate` (per-call overrides of the estimator;
//! Kubernetes does not use it) and `ast.Expr` nil handling, which the Rust AST
//! cannot express.

use crate::checker::CheckedAst;
use crate::overload_ids as ids;
use crate::types::DType;
use cel::common::ast::{EntryExpr, Expr, IdedExpr, LiteralValue};
use std::collections::HashMap;

/// `common.SelectAndIdentCost`.
pub const SELECT_AND_IDENT_COST: u64 = 1;
/// `common.ConstCost`.
pub const CONST_COST: u64 = 0;
/// `common.ListCreateBaseCost`.
pub const LIST_CREATE_BASE_COST: u64 = 10;
/// `common.MapCreateBaseCost`.
pub const MAP_CREATE_BASE_COST: u64 = 30;
/// `common.StructCreateBaseCost`.
pub const STRUCT_CREATE_BASE_COST: u64 = 40;
/// `common.StringTraversalCostFactor`.
pub const STRING_TRAVERSAL_COST_FACTOR: f64 = 0.1;
/// `common.RegexStringLengthCostFactor`.
pub const REGEX_STRING_LENGTH_COST_FACTOR: f64 = 0.25;

/// `parser.AccumulatorName` and `parser.HiddenAccumulatorName`.
const ACCUMULATOR_NAME: &str = "__result__";
const HIDDEN_ACCUMULATOR_NAME: &str = "@result";

/// `doubleTwoTo64`.
const DOUBLE_TWO_TO_64: f64 = 18446744073709551616.0;

fn add_u64_no_overflow(x: u64, y: u64) -> u64 {
    x.saturating_add(y)
}

fn multiply_u64_no_overflow(x: u64, y: u64) -> u64 {
    x.saturating_mul(y)
}

/// `multiplyByCostFactor`: the nearest integer, rounded up.
fn multiply_by_cost_factor(x: u64, y: f64) -> u64 {
    let x_float = x as f64;
    if x_float > 0.0 && y > 0.0 && x_float > u64::MAX as f64 / y {
        return u64::MAX;
    }
    let ceil = (x_float * y).ceil();
    if ceil >= DOUBLE_TWO_TO_64 {
        return u64::MAX;
    }
    ceil as u64
}

/// `checker.SizeEstimate`: an estimated size of a variable length string,
/// bytes, map or list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SizeEstimate {
    pub min: u64,
    pub max: u64,
}

impl SizeEstimate {
    /// `UnknownSizeEstimate`: between 0 and the max `uint64`.
    pub const UNKNOWN: SizeEstimate = SizeEstimate {
        min: 0,
        max: u64::MAX,
    };

    /// `FixedSizeEstimate`.
    pub fn fixed(size: u64) -> Self {
        Self {
            min: size,
            max: size,
        }
    }

    /// `SizeEstimate.Add`.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, o: SizeEstimate) -> SizeEstimate {
        SizeEstimate {
            min: add_u64_no_overflow(self.min, o.min),
            max: add_u64_no_overflow(self.max, o.max),
        }
    }

    /// `SizeEstimate.Multiply`.
    pub fn multiply(self, o: SizeEstimate) -> SizeEstimate {
        SizeEstimate {
            min: multiply_u64_no_overflow(self.min, o.min),
            max: multiply_u64_no_overflow(self.max, o.max),
        }
    }

    /// `SizeEstimate.MultiplyByCostFactor`.
    pub fn multiply_by_cost_factor(self, cost_per_unit: f64) -> CostEstimate {
        CostEstimate {
            min: multiply_by_cost_factor(self.min, cost_per_unit),
            max: multiply_by_cost_factor(self.max, cost_per_unit),
        }
    }

    /// `SizeEstimate.MultiplyByCost`.
    pub fn multiply_by_cost(self, cost: CostEstimate) -> CostEstimate {
        CostEstimate {
            min: multiply_u64_no_overflow(self.min, cost.min),
            max: multiply_u64_no_overflow(self.max, cost.max),
        }
    }

    /// `SizeEstimate.Union`: encompasses both.
    pub fn union(self, o: SizeEstimate) -> SizeEstimate {
        SizeEstimate {
            min: self.min.min(o.min),
            max: self.max.max(o.max),
        }
    }
}

/// `checker.CostEstimate`: an estimated cost range with add and multiply
/// operations that do not overflow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CostEstimate {
    pub min: u64,
    pub max: u64,
}

impl CostEstimate {
    /// `UnknownCostEstimate`.
    pub const UNKNOWN: CostEstimate = CostEstimate {
        min: 0,
        max: u64::MAX,
    };

    /// `FixedCostEstimate`.
    pub fn fixed(cost: u64) -> Self {
        Self {
            min: cost,
            max: cost,
        }
    }

    /// `CostEstimate.Add`.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, o: CostEstimate) -> CostEstimate {
        CostEstimate {
            min: add_u64_no_overflow(self.min, o.min),
            max: add_u64_no_overflow(self.max, o.max),
        }
    }

    /// `CostEstimate.Multiply`.
    pub fn multiply(self, o: CostEstimate) -> CostEstimate {
        CostEstimate {
            min: multiply_u64_no_overflow(self.min, o.min),
            max: multiply_u64_no_overflow(self.max, o.max),
        }
    }

    /// `CostEstimate.MultiplyByCostFactor`.
    pub fn multiply_by_cost_factor(self, cost_per_unit: f64) -> CostEstimate {
        CostEstimate {
            min: multiply_by_cost_factor(self.min, cost_per_unit),
            max: multiply_by_cost_factor(self.max, cost_per_unit),
        }
    }

    /// `CostEstimate.Union`.
    pub fn union(self, o: CostEstimate) -> CostEstimate {
        CostEstimate {
            min: self.min.min(o.min),
            max: self.max.max(o.max),
        }
    }
}

/// `checker.CallEstimate`: the cost of a call and, for a call that results in
/// a map, list, string or bytes, an optional estimate of the result size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CallEstimate {
    pub cost: CostEstimate,
    pub result_size: Option<SizeEstimate>,
}

impl CallEstimate {
    fn of(cost: CostEstimate) -> Self {
        Self {
            cost,
            result_size: None,
        }
    }
}

/// `checker.AstNode`: an expression as the estimator sees it.
#[derive(Clone, Debug)]
pub struct AstNode<'a> {
    /// `Path()`: a field path through the declared types to the node's type.
    /// The first element is a variable, the rest are field names, `@items`,
    /// `@keys` or `@values`. Empty when the node is not reachable from the
    /// declarations.
    pub path: Vec<String>,
    /// `Type()`.
    pub ty: DType,
    /// `Expr()`.
    pub expr: &'a IdedExpr,
    /// `ComputedSize()`: derived from the expression itself (a literal, an
    /// inline list or map, a concatenation), when it can be.
    pub computed_size: Option<SizeEstimate>,
}

/// `checker.CostEstimator`: estimates the sizes of variable length input data
/// and the costs of functions.
pub trait CostEstimator {
    /// `EstimateSize`: the size (the result of CEL's `size()`) of a node whose
    /// size CEL does not know, or `None` when there is no estimate.
    fn estimate_size(&self, node: &AstNode<'_>) -> Option<SizeEstimate>;

    /// `EstimateCallCost`: the cost of an invocation, or `None` when the
    /// estimator has no estimate.
    fn estimate_call_cost(
        &self,
        function: &str,
        overload_id: &str,
        target: Option<&AstNode<'_>>,
        args: &[AstNode<'_>],
    ) -> Option<CallEstimate>;
}

/// An estimator with no opinion: every size is computed from the expression
/// and every call is priced by `functionCost`.
pub struct NoEstimator;

impl CostEstimator for NoEstimator {
    fn estimate_size(&self, _: &AstNode<'_>) -> Option<SizeEstimate> {
        None
    }

    fn estimate_call_cost(
        &self,
        _: &str,
        _: &str,
        _: Option<&AstNode<'_>>,
        _: &[AstNode<'_>],
    ) -> Option<CallEstimate> {
        None
    }
}

/// `checker.CostOption`s.
#[derive(Clone, Copy, Debug)]
pub struct CostOptions {
    /// `PresenceTestHasCost`: whether a presence test (`has()`) costs one or
    /// zero. cel-go defaults to one; the Kubernetes environments set it to
    /// false (`apiserver/pkg/cel/environment/base.go:77-79`).
    pub presence_test_has_cost: bool,
}

impl Default for CostOptions {
    fn default() -> Self {
        Self {
            presence_test_has_cost: true,
        }
    }
}

/// `checker.Cost`: estimates the cost of the type-checked expression `root`.
pub fn cost(
    checked: &CheckedAst,
    root: &IdedExpr,
    estimator: &dyn CostEstimator,
    opts: CostOptions,
) -> CostEstimate {
    let mut c = Coster {
        checked,
        estimator,
        expr_paths: HashMap::new(),
        local_vars: HashMap::new(),
        computed_sizes: HashMap::new(),
        computed_entry_sizes: HashMap::new(),
        presence_test_cost: if opts.presence_test_has_cost {
            CostEstimate::fixed(SELECT_AND_IDENT_COST)
        } else {
            CostEstimate::fixed(0)
        },
    };
    c.cost(root)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContainerKind {
    List,
    Map,
}

/// `entrySizeEstimate`: the container kind and the key / index and value
/// sizes of a list or map. Only exists when both are known.
#[derive(Clone, Copy, Debug)]
struct EntrySizeEstimate {
    container: ContainerKind,
    key: SizeEstimate,
    val: SizeEstimate,
}

/// `entrySizeEstimate.union`: `None` when either side is.
fn union_entry(
    s: Option<EntrySizeEstimate>,
    other: Option<EntrySizeEstimate>,
) -> Option<EntrySizeEstimate> {
    let (s, o) = (s?, other?);
    Some(EntrySizeEstimate {
        container: s.container,
        key: s.key.union(o.key),
        val: s.val.union(o.val),
    })
}

/// `localVar`: the size and entry size of a local or iteration variable.
#[derive(Clone, Debug)]
struct LocalVar {
    path: Vec<String>,
    size: Option<SizeEstimate>,
    entry_size: Option<EntrySizeEstimate>,
}

struct Coster<'a> {
    checked: &'a CheckedAst,
    estimator: &'a dyn CostEstimator,
    /// `exprPaths`: expression id to field path.
    expr_paths: HashMap<u64, Vec<String>>,
    /// `localVars`: the stack of variables per name (`cel.bind` and
    /// comprehension scopes).
    local_vars: HashMap<String, Vec<LocalVar>>,
    /// `computedSizes`: the sizes of call results.
    computed_sizes: HashMap<u64, SizeEstimate>,
    /// `computedEntrySizes`: the sizes of list and map entries.
    computed_entry_sizes: HashMap<u64, EntrySizeEstimate>,
    presence_test_cost: CostEstimate,
}

fn append(mut path: Vec<String>, seg: &str) -> Vec<String> {
    path.push(seg.to_string());
    path
}

impl<'a> Coster<'a> {
    fn push_var(&mut self, name: &str, v: LocalVar) {
        self.local_vars.entry(name.to_string()).or_default().push(v);
    }

    fn pop_var(&mut self, name: &str) {
        if let Some(s) = self.local_vars.get_mut(name) {
            s.pop();
        }
    }

    fn peek_var(&self, name: &str) -> Option<&LocalVar> {
        self.local_vars.get(name).and_then(|s| s.last())
    }

    /// The container kind of `range`: its entry size's, else its type's.
    fn container_of(
        &mut self,
        range: &IdedExpr,
        entry: Option<EntrySizeEstimate>,
    ) -> Option<ContainerKind> {
        if let Some(e) = entry {
            return Some(e.container);
        }
        match self.get_type(range) {
            DType::List(_) => Some(ContainerKind::List),
            DType::Map(..) => Some(ContainerKind::Map),
            _ => None,
        }
    }

    fn push_iter_key(&mut self, var: &str, range: &IdedExpr) {
        let entry = self.compute_entry_size(range);
        let size = entry.map(|e| e.key);
        let path = self.get_path(range);
        let sub = match self.container_of(range, entry) {
            Some(ContainerKind::List) => "@indices",
            _ => "@keys",
        };
        self.push_var(
            var,
            LocalVar {
                path: append(path, sub),
                size,
                entry_size: None,
            },
        );
    }

    fn push_iter_value(&mut self, var: &str, range: &IdedExpr) {
        let entry = self.compute_entry_size(range);
        let size = entry.map(|e| e.val);
        let path = self.get_path(range);
        let sub = match self.container_of(range, entry) {
            Some(ContainerKind::List) => "@items",
            _ => "@values",
        };
        self.push_var(
            var,
            LocalVar {
                path: append(path, sub),
                size,
                entry_size: None,
            },
        );
    }

    fn push_iter_single(&mut self, var: &str, range: &IdedExpr) {
        let entry = self.compute_entry_size(range);
        let mut size = entry.map(|e| e.key);
        let mut sub = "@keys";
        if self.container_of(range, entry) == Some(ContainerKind::List) {
            size = entry.map(|e| e.val);
            sub = "@items";
        }
        let path = self.get_path(range);
        self.push_var(
            var,
            LocalVar {
                path: append(path, sub),
                size,
                entry_size: None,
            },
        );
    }

    fn push_local_var(&mut self, var: &str, e: &IdedExpr) {
        let path = self.get_path(e);
        // The entry size of the binding expression is propagated: it could be
        // a list or map.
        let entry_size = self.compute_entry_size(e);
        let size = self.compute_size(e);
        self.push_var(
            var,
            LocalVar {
                path,
                size,
                entry_size,
            },
        );
    }

    fn cost(&mut self, e: &'a IdedExpr) -> CostEstimate {
        match &e.expr {
            Expr::Literal(_) => CostEstimate::fixed(CONST_COST),
            Expr::Ident(name) => self.cost_ident(e, name),
            Expr::Select(_) => self.cost_select(e),
            Expr::Call(_) => self.cost_call(e),
            Expr::List(_) => self.cost_create_list(e),
            Expr::Map(_) => self.cost_create_map(e),
            Expr::Struct(_) => self.cost_create_struct(e),
            Expr::Comprehension(_) => {
                if self.is_bind(e) {
                    self.cost_bind(e)
                } else {
                    self.cost_comprehension(e)
                }
            }
            Expr::Unspecified => CostEstimate::default(),
        }
    }

    fn cost_ident(&mut self, e: &IdedExpr, name: &str) -> CostEstimate {
        // Build and track the field path.
        let path = match self.peek_var(name) {
            Some(v) => v.path.clone(),
            None => vec![name.to_string()],
        };
        self.expr_paths.insert(e.id, path);
        CostEstimate::fixed(SELECT_AND_IDENT_COST)
    }

    fn cost_select(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::Select(sel) = &e.expr else {
            unreachable!()
        };
        let mut sum = CostEstimate::default();
        if sel.test {
            // Recurse, but add no cost for the qualifier: this is how
            // `evalTestOnly` increments the runtime cost counter.
            sum = sum.add(self.presence_test_cost);
            sum = sum.add(self.cost(&sel.operand));
            return sum;
        }
        sum = sum.add(self.cost(&sel.operand));
        if matches!(
            self.get_type(&sel.operand),
            DType::Map(..) | DType::Struct(_) | DType::Param(_)
        ) {
            sum = sum.add(CostEstimate::fixed(SELECT_AND_IDENT_COST));
        }
        // Build and track the field path.
        let path = append(self.get_path(&sel.operand), &sel.field);
        self.expr_paths.insert(e.id, path);
        sum
    }

    fn cost_call(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::Call(call) = &e.expr else {
            unreachable!()
        };
        // `dyn` is just a way to disable type-checking: the cost of 1 plus
        // the cost of the argument.
        if call.func_name == "dyn" && !call.args.is_empty() {
            let arg = &call.args[0];
            let arg_cost = self.cost(arg);
            self.copy_size_estimates(e, arg);
            return CostEstimate::fixed(1).add(arg_cost);
        }

        let mut sum = CostEstimate::default();
        let mut arg_nodes = Vec::with_capacity(call.args.len());
        let mut arg_costs = Vec::with_capacity(call.args.len());
        for arg in &call.args {
            arg_costs.push(self.cost(arg));
            arg_nodes.push(self.new_ast_node(arg));
        }

        let overload_ids = self
            .checked
            .ref_map
            .get(&e.id)
            .map(|r| r.cost_overload_ids.clone())
            .unwrap_or_default();
        if overload_ids.is_empty() {
            return CostEstimate::default();
        }
        let mut target_node = None;
        if let Some(t) = &call.target {
            sum = sum.add(self.cost(t));
            target_node = Some(self.new_ast_node(t));
        }
        // Pick a cost estimate range that covers all the overload cost
        // estimation ranges.
        let mut fn_cost = CostEstimate {
            min: u64::MAX,
            max: 0,
        };
        let mut result_size: Option<SizeEstimate> = None;
        for overload in &overload_ids {
            let oc = self.function_cost(
                e,
                &call.func_name,
                overload,
                target_node.as_ref(),
                &arg_nodes,
                &arg_costs,
            );
            fn_cost = fn_cost.union(oc.cost);
            if let Some(rs) = oc.result_size {
                result_size = Some(match result_size {
                    None => rs,
                    Some(prev) => prev.union(rs),
                });
            }
            // Build and track the field path for index operations.
            match overload.as_str() {
                ids::INDEX_LIST => {
                    if let Some(a0) = call.args.first() {
                        result_size = self.compute_entry_size(a0).map(|s| s.val);
                        let path = append(self.get_path(a0), "@items");
                        self.expr_paths.insert(e.id, path);
                    }
                }
                ids::INDEX_MAP => {
                    if let Some(a0) = call.args.first() {
                        result_size = self.compute_entry_size(a0).map(|s| s.val);
                        let path = append(self.get_path(a0), "@values");
                        self.expr_paths.insert(e.id, path);
                    }
                }
                _ => {}
            }
            if result_size.is_none() {
                result_size = self.compute_size(e);
            }
        }
        self.set_size(e, result_size);
        sum.add(fn_cost)
    }

    fn cost_create_list(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::List(list) = &e.expr else {
            unreachable!()
        };
        let mut sum = CostEstimate::default();
        let mut item_size = SizeEstimate {
            min: u64::MAX,
            max: 0,
        };
        if list.elements.is_empty() {
            item_size.min = 0;
        }
        for el in &list.elements {
            sum = sum.add(self.cost(el));
            let is = self.size_or_unknown(el);
            item_size = item_size.union(is);
        }
        self.set_entry_size(
            e,
            Some(EntrySizeEstimate {
                container: ContainerKind::List,
                key: SizeEstimate::fixed(1),
                val: item_size,
            }),
        );
        sum.add(CostEstimate::fixed(LIST_CREATE_BASE_COST))
    }

    fn cost_create_map(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::Map(map) = &e.expr else {
            unreachable!()
        };
        let mut sum = CostEstimate::default();
        let mut key_size = SizeEstimate {
            min: u64::MAX,
            max: 0,
        };
        let mut val_size = key_size;
        if map.entries.is_empty() {
            val_size.min = 0;
            key_size.min = 0;
        }
        for ent in &map.entries {
            let EntryExpr::MapEntry(entry) = &ent.expr else {
                continue;
            };
            sum = sum.add(self.cost(&entry.key));
            sum = sum.add(self.cost(&entry.value));
            let ks = self.size_or_unknown(&entry.key);
            key_size = key_size.union(ks);
            let vs = self.size_or_unknown(&entry.value);
            val_size = val_size.union(vs);
        }
        self.set_entry_size(
            e,
            Some(EntrySizeEstimate {
                container: ContainerKind::Map,
                key: key_size,
                val: val_size,
            }),
        );
        sum.add(CostEstimate::fixed(MAP_CREATE_BASE_COST))
    }

    fn cost_create_struct(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::Struct(st) = &e.expr else {
            unreachable!()
        };
        let mut sum = CostEstimate::default();
        for ent in &st.entries {
            if let EntryExpr::StructField(f) = &ent.expr {
                sum = sum.add(self.cost(&f.value));
            }
        }
        sum.add(CostEstimate::fixed(STRUCT_CREATE_BASE_COST))
    }

    fn cost_comprehension(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::Comprehension(comp) = &e.expr else {
            unreachable!()
        };
        let mut sum = CostEstimate::default();
        sum = sum.add(self.cost(&comp.iter_range));
        sum = sum.add(self.cost(&comp.accu_init));
        self.push_local_var(&comp.accu_var, &comp.accu_init);

        // Track the iterRange of each IterVar and AccuVar for field path
        // construction.
        if let Some(var2) = &comp.iter_var2 {
            self.push_iter_key(&comp.iter_var, &comp.iter_range);
            self.push_iter_value(var2, &comp.iter_range);
        } else {
            self.push_iter_single(&comp.iter_var, &comp.iter_range);
        }

        // The cost for each element in the loop.
        let loop_cost = self.cost(&comp.loop_cond);
        let step_cost = self.cost(&comp.loop_step);

        // Clear the intermediate variable tracking.
        self.pop_var(&comp.iter_var);
        if let Some(var2) = &comp.iter_var2 {
            self.pop_var(var2);
        }

        // The result cost.
        sum = sum.add(self.cost(&comp.result));
        self.pop_var(&comp.accu_var);

        // The cost of the loop.
        let range_cnt = self.size_or_unknown(&comp.iter_range);
        let range_cost = range_cnt.multiply_by_cost(step_cost.add(loop_cost));
        sum = sum.add(range_cost);

        match &comp.accu_init.expr {
            Expr::Literal(_) => {
                let sz = self.compute_size(&comp.accu_init);
                self.set_size(e, sz);
            }
            Expr::List(_) | Expr::Map(_) => {
                self.set_size(e, Some(range_cnt));
                // A step which produces a container value has an entry size
                // associated with its expression id.
                if let Some(step) = self.compute_entry_size(&comp.loop_step) {
                    self.set_entry_size(e, Some(step));
                }
            }
            _ => {}
        }
        sum
    }

    /// `isBind`: a `cel.bind` comprehension, which has an empty iteration
    /// range, a `false` loop condition and a non-accumulator variable.
    fn is_bind(&self, e: &IdedExpr) -> bool {
        let Expr::Comprehension(comp) = &e.expr else {
            return false;
        };
        matches!(&comp.iter_range.expr, Expr::List(l) if l.elements.is_empty())
            && matches!(
                &comp.loop_cond.expr,
                Expr::Literal(LiteralValue::Boolean(b)) if !*b.inner()
            )
            && comp.accu_var != ACCUMULATOR_NAME
            && comp.accu_var != HIDDEN_ACCUMULATOR_NAME
    }

    fn cost_bind(&mut self, e: &'a IdedExpr) -> CostEstimate {
        let Expr::Comprehension(comp) = &e.expr else {
            unreachable!()
        };
        let mut sum = CostEstimate::default();
        // Binds are lazily initialized, so the cost of an empty iteration
        // range is retained.
        sum = sum.add(self.cost(&comp.iter_range));
        sum = sum.add(self.cost(&comp.accu_init));

        self.push_local_var(&comp.accu_var, &comp.accu_init);
        sum = sum.add(self.cost(&comp.result));
        self.pop_var(&comp.accu_var);

        // Associate the bind output size with the result size.
        self.copy_size_estimates(e, &comp.result);
        sum
    }

    /// `coster.functionCost`.
    fn function_cost(
        &mut self,
        e: &IdedExpr,
        function: &str,
        overload_id: &str,
        target: Option<&AstNode<'a>>,
        args: &[AstNode<'a>],
        arg_costs: &[CostEstimate],
    ) -> CallEstimate {
        let arg_cost_sum = arg_costs
            .iter()
            .fold(CostEstimate::default(), |s, c| s.add(*c));
        if let Some(est) = self
            .estimator
            .estimate_call_cost(function, overload_id, target, args)
        {
            return CallEstimate {
                cost: est.cost.add(arg_cost_sum),
                result_size: est.result_size,
            };
        }
        let unknown = |n: &AstNode<'_>| n.computed_size.unwrap_or(SizeEstimate::UNKNOWN);
        let traversal = STRING_TRAVERSAL_COST_FACTOR;
        match overload_id {
            // O(n) functions.
            ids::EXT_FORMAT_STRING => {
                if let Some(t) = target {
                    // ResultSize not calculated because the max size cannot
                    // be bounded.
                    return CallEstimate::of(
                        unknown(t)
                            .multiply_by_cost_factor(traversal)
                            .add(arg_cost_sum),
                    );
                }
            }
            ids::STRING_TO_BYTES => {
                if args.len() == 1 {
                    let sz = unknown(&args[0]);
                    // ResultSize max is when each char converts to 4 bytes.
                    return CallEstimate {
                        cost: sz.multiply_by_cost_factor(traversal).add(arg_cost_sum),
                        result_size: Some(SizeEstimate {
                            min: sz.min,
                            max: sz.max.wrapping_mul(4),
                        }),
                    };
                }
            }
            ids::BYTES_TO_STRING => {
                if args.len() == 1 {
                    let sz = unknown(&args[0]);
                    // ResultSize min is when 4 bytes convert to 1 char.
                    return CallEstimate {
                        cost: sz.multiply_by_cost_factor(traversal).add(arg_cost_sum),
                        result_size: Some(SizeEstimate {
                            min: sz.min / 4,
                            max: sz.max,
                        }),
                    };
                }
            }
            ids::EXT_QUOTE_STRING => {
                if args.len() == 1 {
                    let sz = unknown(&args[0]);
                    // ResultSize max is when each char is escaped. 2 quote
                    // chars are always added.
                    return CallEstimate {
                        cost: sz.multiply_by_cost_factor(traversal).add(arg_cost_sum),
                        result_size: Some(SizeEstimate {
                            min: sz.min.wrapping_add(2),
                            max: sz.max.wrapping_mul(2).wrapping_add(2),
                        }),
                    };
                }
            }
            ids::STARTS_WITH_STRING | ids::ENDS_WITH_STRING => {
                if args.len() == 1 {
                    return CallEstimate::of(
                        unknown(&args[0])
                            .multiply_by_cost_factor(traversal)
                            .add(arg_cost_sum),
                    );
                }
            }
            ids::IN_LIST => {
                // If a list is composed entirely of constant values this is
                // O(1), but all list containment checks are assumed O(n).
                if args.len() == 2 {
                    return CallEstimate::of(
                        unknown(&args[1])
                            .multiply_by_cost_factor(1.0)
                            .add(arg_cost_sum),
                    );
                }
            }
            // O(nm) functions.
            ids::MATCHES_STRING => {
                // https://swtch.com/~rsc/regexp/regexp1.html applies to the
                // RE2 implementation supported by CEL.
                if let (Some(t), 1) = (target, args.len()) {
                    // Add one to the string length so the product of string
                    // and regex is not 0 for an empty string with an
                    // expensive regex.
                    let str_cost = unknown(t)
                        .add(SizeEstimate { min: 1, max: 1 })
                        .multiply_by_cost_factor(traversal);
                    // Each expression in a regex is guessed to be at least 4
                    // chars long.
                    let regex_cost =
                        unknown(&args[0]).multiply_by_cost_factor(REGEX_STRING_LENGTH_COST_FACTOR);
                    return CallEstimate::of(str_cost.multiply(regex_cost).add(arg_cost_sum));
                }
            }
            ids::CONTAINS_STRING => {
                if let (Some(t), 1) = (target, args.len()) {
                    let str_cost = unknown(t).multiply_by_cost_factor(traversal);
                    let substr_cost = unknown(&args[0]).multiply_by_cost_factor(traversal);
                    return CallEstimate::of(str_cost.multiply(substr_cost).add(arg_cost_sum));
                }
            }
            ids::LOGICAL_OR | ids::LOGICAL_AND => {
                if arg_costs.len() >= 2 {
                    let (lhs, rhs) = (arg_costs[0], arg_costs[1]);
                    // The min cost is the min of the LHS for a short
                    // circuited && or ||.
                    return CallEstimate::of(CostEstimate {
                        min: lhs.min,
                        max: lhs.add(rhs).max,
                    });
                }
            }
            ids::CONDITIONAL => {
                if args.len() == 3 {
                    let size = unknown(&args[1]).union(unknown(&args[2]));
                    let result_entry = union_entry(
                        self.compute_entry_size(args[1].expr),
                        self.compute_entry_size(args[2].expr),
                    );
                    self.set_entry_size(e, result_entry);
                    let arg_cost = arg_costs[0].add(arg_costs[1].union(arg_costs[2]));
                    return CallEstimate {
                        cost: arg_cost,
                        result_size: Some(size),
                    };
                }
            }
            ids::ADD_STRING | ids::ADD_BYTES | ids::ADD_LIST => {
                if args.len() == 2 {
                    let lhs_size = unknown(&args[0]);
                    let rhs_size = unknown(&args[1]);
                    let result_size = lhs_size.add(rhs_size);
                    let rhs_entry = self.compute_entry_size(args[0].expr);
                    let lhs_entry = self.compute_entry_size(args[1].expr);
                    if let Some(re) = union_entry(rhs_entry, lhs_entry) {
                        self.set_entry_size(e, Some(re));
                    }
                    return if overload_id == ids::ADD_LIST {
                        // List concatenation is O(1), but handled here to
                        // track the size.
                        CallEstimate {
                            cost: CostEstimate::fixed(1).add(arg_cost_sum),
                            result_size: Some(result_size),
                        }
                    } else {
                        CallEstimate {
                            cost: result_size
                                .multiply_by_cost_factor(traversal)
                                .add(arg_cost_sum),
                            result_size: Some(result_size),
                        }
                    };
                }
            }
            ids::LESS_STRING
            | ids::GREATER_STRING
            | ids::LESS_EQUALS_STRING
            | ids::GREATER_EQUALS_STRING
            | ids::LESS_BYTES
            | ids::GREATER_BYTES
            | ids::LESS_EQUALS_BYTES
            | ids::GREATER_EQUALS_BYTES
            | ids::EQUALS
            | ids::NOT_EQUALS
                if args.len() >= 2 =>
            {
                let lhs = unknown(&args[0]);
                let rhs = unknown(&args[1]);
                let smallest_max = lhs.max.min(rhs.max);
                let min = u64::from(smallest_max > 0);
                // Equality of 2 scalar values results in a cost of 1.
                return CallEstimate::of(
                    CostEstimate {
                        min,
                        max: smallest_max,
                    }
                    .multiply_by_cost_factor(traversal)
                    .add(arg_cost_sum),
                );
            }
            _ => {}
        }
        // O(1) functions. Benchmarks suggest most other operations take +/-
        // 50% of a base cost unit.
        CallEstimate::of(CostEstimate::fixed(1).add(arg_cost_sum))
    }

    fn get_type(&self, e: &IdedExpr) -> DType {
        self.checked
            .type_map
            .get(&e.id)
            .cloned()
            .unwrap_or(DType::Error)
    }

    fn get_path(&self, e: &IdedExpr) -> Vec<String> {
        if let Expr::Ident(name) = &e.expr {
            if let Some(v) = self.peek_var(name) {
                return v.path.clone();
            }
        }
        self.expr_paths.get(&e.id).cloned().unwrap_or_default()
    }

    fn new_ast_node(&mut self, e: &'a IdedExpr) -> AstNode<'a> {
        let mut path = self.get_path(e);
        if path
            .first()
            .is_some_and(|p| p == ACCUMULATOR_NAME || p == HIDDEN_ACCUMULATOR_NAME)
        {
            // Only paths to root variables are provided: accumulator
            // variables are omitted.
            path = Vec::new();
        }
        AstNode {
            path,
            ty: self.get_type(e),
            expr: e,
            computed_size: self.compute_size(e),
        }
    }

    fn set_size(&mut self, e: &IdedExpr, size: Option<SizeEstimate>) {
        if let Some(s) = size {
            self.computed_sizes.insert(e.id, s);
        }
    }

    fn size_or_unknown(&mut self, e: &IdedExpr) -> SizeEstimate {
        self.compute_size(e).unwrap_or(SizeEstimate::UNKNOWN)
    }

    fn copy_size_estimates(&mut self, dst: &IdedExpr, src: &IdedExpr) {
        let s = self.compute_size(src);
        self.set_size(dst, s);
        let es = self.compute_entry_size(src);
        self.set_entry_size(dst, es);
    }

    /// `coster.computeSize`.
    fn compute_size(&mut self, e: &IdedExpr) -> Option<SizeEstimate> {
        if let Some(s) = self.computed_sizes.get(&e.id) {
            return Some(*s);
        }
        if let Some(s) = compute_expr_size(e) {
            return Some(s);
        }
        // Size estimates are computed first as users may choose to override
        // the costs CEL would otherwise ascribe to the type.
        let ty = self.get_type(e);
        let node = AstNode {
            path: self.get_path(e),
            ty: ty.clone(),
            expr: e,
            computed_size: None,
        };
        if let Some(s) = self.estimator.estimate_size(&node) {
            // Storing the computed size reduces calls to EstimateSize().
            self.computed_sizes.insert(e.id, s);
            return Some(s);
        }
        if let Some(s) = compute_type_size(&ty) {
            return Some(s);
        }
        if let Expr::Ident(name) = &e.expr {
            if let Some(v) = self.peek_var(name) {
                if let Some(s) = v.size {
                    return Some(s);
                }
            }
        }
        None
    }

    fn set_entry_size(&mut self, e: &IdedExpr, size: Option<EntrySizeEstimate>) {
        if let Some(s) = size {
            self.computed_entry_sizes.insert(e.id, s);
        }
    }

    fn compute_entry_size(&mut self, e: &IdedExpr) -> Option<EntrySizeEstimate> {
        if let Some(s) = self.computed_entry_sizes.get(&e.id) {
            return Some(*s);
        }
        if let Expr::Ident(name) = &e.expr {
            if let Some(v) = self.peek_var(name) {
                if let Some(s) = v.entry_size {
                    return Some(s);
                }
            }
        }
        None
    }
}

/// `computeExprSize`: the size of a literal, an inline list or an inline map.
fn compute_expr_size(e: &IdedExpr) -> Option<SizeEstimate> {
    let v = match &e.expr {
        Expr::Literal(l) => match l {
            // The size is the number of runes, as `size()` computes at
            // runtime.
            LiteralValue::String(s) => s.inner().chars().count() as u64,
            LiteralValue::Bytes(b) => b.inner().len() as u64,
            LiteralValue::Boolean(_)
            | LiteralValue::Double(_)
            | LiteralValue::Int(_)
            | LiteralValue::UInt(_)
            | LiteralValue::Null => 1,
        },
        Expr::List(l) => l.elements.len() as u64,
        Expr::Map(m) => m.entries.len() as u64,
        _ => return None,
    };
    Some(SizeEstimate::fixed(v))
}

/// `computeTypeSize`.
fn compute_type_size(t: &DType) -> Option<SizeEstimate> {
    is_scalar(t).then(|| SizeEstimate::fixed(1))
}

/// `isScalar`: whether the type is known to be of a constant size at compile
/// time. Strings are variable-width and `Any` / `Value` are not knowable.
fn is_scalar(t: &DType) -> bool {
    match t {
        DType::Bool
        | DType::Double
        | DType::Duration
        | DType::Int
        | DType::Timestamp
        | DType::Uint => true,
        DType::Opaque(n, ps) if n == "optional_type" => ps.first().is_some_and(is_scalar),
        _ => false,
    }
}
