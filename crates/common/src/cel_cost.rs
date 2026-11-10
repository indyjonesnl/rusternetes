//! CEL runtime cost tracking (#3323): the budgets and constants of
//! `staging/src/k8s.io/apiserver/pkg/apis/cel/config.go`, the per-node cost
//! model of cel-go `interpreter/runtimecost.go`, the per-function costs of
//! `staging/src/k8s.io/apiserver/pkg/cel/library/cost.go`
//! (`CostEstimator.CallCost`), and the budget accounting of
//! `staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/activation.go`
//! (`evaluationActivation.Evaluate`).
//!
//! The `cel` crate (0.13 and 0.15) has no cost-tracking or evaluation-observer hook
//! (`Program::execute` is `Value::resolve` with no callbacks), so cel-go's
//! `ObservableInterpretable` cannot be ported as an observer. The same costs are
//! produced by [`eval_with_cost`], which walks the expression itself and lets the
//! crate evaluate only what is left of each node once its operands are
//! evaluated (operands are bound to hidden variables), mirroring the evaluation
//! order of `objects.rs` `Value::resolve_val` (short-circuit `&&`/`||`/`?:`,
//! comprehension loop).
//!
//! Known gaps against cel-go: the walk follows the `cel` crate's own
//! macro expansion and evaluation order (loop condition before the iteration
//! variable is bound, no two-variable comprehensions); the Kubernetes extension
//! library types (quantity, IP, CIDR, URL, semver, format, authorizer) are not
//! values in this runtime, so only their name-keyed costs are ported.

use cel::common::ast::{
    operators, CallExpr, ComprehensionExpr, EntryExpr, Expr, IdedEntryExpr, IdedExpr, ListExpr,
    LiteralValue, MapEntryExpr, MapExpr, SelectExpr, StructExpr, StructFieldExpr,
};
use cel::objects::Key;
use cel::{Context, ExecutionError, Value};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// `PerCallLimit`: the cost limit per CEL evaluation (`config.go:21`).
pub const PER_CALL_LIMIT: u64 = 1_000_000;
/// `RuntimeCELCostBudget`: per ValidatingAdmissionPolicyBinding or custom
/// resource (`config.go:25`).
pub const RUNTIME_CEL_COST_BUDGET: i64 = 10_000_000;
/// `RuntimeCELCostBudgetMatchConditions`: per object with matchConditions
/// (`config.go:31`).
pub const RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS: i64 = 2_500_000;
/// `CheckFrequency` (`config.go:36`).
pub const CHECK_FREQUENCY: u64 = 100;
/// `MaxRequestSizeBytes` (`config.go:41`).
pub const MAX_REQUEST_SIZE_BYTES: i64 = 3 * 1024 * 1024;
/// `MaxEvaluatedMessageExpressionSizeBytes` (`config.go:44`).
pub const MAX_EVALUATED_MESSAGE_EXPRESSION_SIZE_BYTES: i64 = 5 * 1024;

/// `common.StringTraversalCostFactor` (cel-go `common/cost.go`).
pub const STRING_TRAVERSAL_COST_FACTOR: f64 = 0.1;
/// `common.RegexStringLengthCostFactor`.
pub const REGEX_STRING_LENGTH_COST_FACTOR: f64 = 0.25;
/// `common.ListCreateBaseCost`.
pub const LIST_CREATE_BASE_COST: u64 = 10;
/// `common.MapCreateBaseCost`.
pub const MAP_CREATE_BASE_COST: u64 = 30;
/// `common.StructCreateBaseCost`.
pub const STRUCT_CREATE_BASE_COST: u64 = 40;
/// `common.SelectAndIdentCost`.
pub const SELECT_AND_IDENT_COST: u64 = 1;

/// The message of cel-go's `EvalCancelledError{Cause: CostLimitExceeded}`
/// (`runtimecost.go` `Observe`).
pub const COST_LIMIT_EXCEEDED_MESSAGE: &str = "operation cancelled: actual cost limit exceeded";

/// `activation.go:150`: the detail of the out-of-budget `cel.Error`.
pub const OUT_OF_BUDGET_DETAIL: &str =
    "validation failed due to running out of cost budget, no further validation rules will be run";

/// Why a costed evaluation stopped.
#[derive(Debug)]
pub enum CostEvalError {
    /// An ordinary CEL evaluation error.
    Exec(ExecutionError),
    /// `EvalCancelledError{Cause: CostLimitExceeded}`.
    LimitExceeded,
}

impl fmt::Display for CostEvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CostEvalError::Exec(e) => write!(f, "{e}"),
            CostEvalError::LimitExceeded => f.write_str(COST_LIMIT_EXCEEDED_MESSAGE),
        }
    }
}

impl From<ExecutionError> for CostEvalError {
    fn from(e: ExecutionError) -> Self {
        CostEvalError::Exec(e)
    }
}

/// Options of the `CostTracker` (`CostTrackerLimit`, `PresenceTestHasCost`).
#[derive(Debug, Clone, Copy)]
pub struct CostOptions {
    /// `CostTracker.Limit`.
    pub limit: Option<u64>,
    /// `presenceTestHasCost`; the apiserver environment sets false
    /// (`environment/base.go:87`).
    pub presence_test_has_cost: bool,
}

impl Default for CostOptions {
    /// `environment/base.go:83-87`: `cel.CostLimit(PerCallLimit)` and
    /// `PresenceTestHasCost(false)`.
    fn default() -> Self {
        CostOptions {
            limit: Some(PER_CALL_LIMIT),
            presence_test_has_cost: false,
        }
    }
}

type R = Result<Value, CostEvalError>;

/// Evaluate `expr` and return the result with the actual cost
/// (`EvalDetails.ActualCost()`), stopping with [`CostEvalError::LimitExceeded`]
/// as soon as the cost passes the limit.
pub fn eval_with_cost(
    expr: &IdedExpr,
    ctx: &Context,
    opts: CostOptions,
) -> (Result<Value, CostEvalError>, u64) {
    eval_with_cost_lazy(expr, ctx, opts, None)
}

/// [`eval_with_cost`] with the `variables` of the activation resolved lazily
/// through `lazy` (see [`LazyVariables`]).
pub fn eval_with_cost_lazy(
    expr: &IdedExpr,
    ctx: &Context,
    opts: CostOptions,
    lazy: Option<&LazyVariables>,
) -> (Result<Value, CostEvalError>, u64) {
    let visible = lazy.map_or(0, |l| l.defs.len());
    eval_visible(expr, ctx, opts, lazy, visible)
}

fn eval_visible(
    expr: &IdedExpr,
    ctx: &Context,
    opts: CostOptions,
    lazy: Option<&LazyVariables>,
    visible: usize,
) -> (Result<Value, CostEvalError>, u64) {
    let mut tracker = Tracker {
        cost: 0,
        opts,
        lazy,
        visible,
    };
    // The `cel` interpreter panics on a few inputs (`Expr::Struct`); never
    // propagate that out of an admission request.
    let r = catch_unwind(AssertUnwindSafe(|| tracker.walk(expr, ctx))).unwrap_or_else(|_| {
        Err(CostEvalError::Exec(ExecutionError::function_error(
            "cel",
            "CEL evaluation failed unexpectedly",
        )))
    });
    (r, tracker.cost)
}

/// `CostTracker`: `cost` is `CostTracker.cost`, `opts` its `Limit` and
/// `presenceTestHasCost`.
struct Tracker<'a> {
    cost: u64,
    opts: CostOptions,
    /// The lazily composited `variables` of the activation, if any.
    lazy: Option<&'a LazyVariables>,
    /// How many of `lazy`'s variables this expression can see: a variable
    /// is compiled against the ones declared before it
    /// (`CompileAndStoreVariable` adds each field to `MapType` after
    /// compiling it), an ordinary expression against all of them.
    visible: usize,
}

/// The lazily evaluated, memoized `variables` map of an activation.
///
/// Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/
/// composition.go` (`compositionContext.Variables`, `variableAccessor.Callback`)
/// over `staging/src/k8s.io/apiserver/pkg/cel/lazy/lazy.go` (`MapValue`):
///
/// * a variable is evaluated the first time an expression reads it
///   (`MapValue.resolveField`) and the result, error included, is kept for
///   the rest of the activation (`values`);
/// * its actual cost is reported to the composition context (`reportCost`)
///   and charged to the budget by the caller (`GetAndResetCost`, see
///   [`LazyVariables::get_and_reset_cost`] and [`charge_budget`]);
/// * a failure is the value `composited variable %q fails to evaluate: %v` /
///   `... fails to compile: %v`, an error only for the expression that reads it.
///
/// One `LazyVariables` is one activation (`CreateActivation`): the
/// validations of a policy share one, the `messageExpression`s and the
/// `auditAnnotations` (separate `ForInput` calls) each get their own.
///
/// The `cel` crate resolves identifiers to a `Value`, which cannot be lazy,
/// so [`Tracker::walk`] intercepts `variables.<name>` / `variables["<name>"]`
/// and evaluates `<name>` first.
pub struct LazyVariables {
    /// `(name, expression)` in declaration order.
    defs: Vec<(String, String)>,
    /// The activation's other variables (`object`, `request`, ...).
    base: std::collections::HashMap<String, Value>,
    state: std::sync::Mutex<LazyState>,
}

#[derive(Default)]
struct LazyState {
    values: std::collections::HashMap<String, Result<Value, String>>,
    accumulated_cost: i64,
}

impl LazyVariables {
    /// `defs` are `spec.variables` as `(name, expression)`; `base` the
    /// activation's variables other than `variables`.
    pub fn new(
        defs: Vec<(String, String)>,
        base: std::collections::HashMap<String, Value>,
    ) -> Self {
        LazyVariables {
            defs,
            base,
            state: std::sync::Mutex::new(LazyState::default()),
        }
    }

    /// `compositionContext.GetAndResetCost`.
    pub fn get_and_reset_cost(&self) -> i64 {
        let mut st = self.state.lock().unwrap();
        std::mem::take(&mut st.accumulated_cost)
    }

    /// How many variables have been evaluated so far.
    pub fn evaluated_count(&self) -> usize {
        self.state.lock().unwrap().values.len()
    }

    /// `MapValue.resolveField` for `name`, among the first `visible`
    /// variables: evaluate it once and report its cost.
    fn ensure(&self, name: &str, visible: usize, opts: CostOptions) {
        let Some(idx) = self.defs[..visible].iter().position(|(n, _)| n == name) else {
            return;
        };
        if self.state.lock().unwrap().values.contains_key(name) {
            return;
        }
        let (_, expression) = &self.defs[idx];
        // `variableAccessor.Callback`
        let (result, cost) = match crate::cel_struct::compile(expression) {
            Err(e) => (
                Err(format!(
                    "composited variable {name:?} fails to compile: {e}"
                )),
                0,
            ),
            Ok(prog) => {
                let mut ctx = crate::cel_struct::new_context_for(&prog);
                for (k, v) in &self.base {
                    let _ = ctx.add_variable(k.clone(), v.clone());
                }
                let (r, cost) = eval_visible(prog.expression(), &ctx, opts, Some(self), idx);
                (
                    r.map_err(|e| format!("composited variable {name:?} fails to evaluate: {e}")),
                    cost,
                )
            }
        };
        let mut st = self.state.lock().unwrap();
        // `reportCost`; `cost > MaxInt64` is clamped
        st.accumulated_cost = st
            .accumulated_cost
            .saturating_add(i64::try_from(cost).unwrap_or(i64::MAX));
        st.values.insert(name.to_string(), result);
    }

    fn lookup(&self, name: &str) -> Option<Result<Value, String>> {
        self.state.lock().unwrap().values.get(name).cloned()
    }

    /// The map of the evaluated variables a scope binds `variables` to.
    fn bound_map(&self) -> Value {
        let st = self.state.lock().unwrap();
        let map = st
            .values
            .iter()
            .filter_map(|(k, v)| Some((Key::from(k.as_str()), v.as_ref().ok()?.clone())))
            .collect();
        Value::Map(cel::objects::Map {
            map: std::sync::Arc::new(map),
        })
    }
}

/// Hidden identifier a [`LazyVariables`] map is bound to for one access.
const LAZY_VARIABLES_IDENT: &str = "@variables";

/// The name read by `variables.<name>` / `variables["<name>"]` (also the
/// presence test), when `e` is one.
fn lazy_access(e: &IdedExpr) -> Option<String> {
    let is_vars = |x: &IdedExpr| matches!(&x.expr, Expr::Ident(n) if n == "variables");
    match &e.expr {
        Expr::Select(s) if is_vars(&s.operand) => Some(s.field.clone()),
        Expr::Call(c) if is_index(c) && is_vars(&c.args[0]) => match &c.args[1].expr {
            Expr::Literal(LiteralValue::String(k)) => Some(k.to_string()),
            _ => None,
        },
        _ => None,
    }
}

/// `e` with the `variables` operand renamed to the hidden identifier.
fn rename_variables_operand(e: &IdedExpr) -> IdedExpr {
    let mut out = e.clone();
    match &mut out.expr {
        Expr::Select(s) => *s.operand = ident(LAZY_VARIABLES_IDENT.to_string()),
        Expr::Call(c) => c.args[0] = ident(LAZY_VARIABLES_IDENT.to_string()),
        _ => {}
    }
    out
}

fn ident(name: String) -> IdedExpr {
    IdedExpr {
        id: 0,
        expr: Expr::Ident(name),
    }
}

fn alias(i: usize) -> IdedExpr {
    ident(format!("@cost{i}"))
}

fn resolve(ctx: &Context, e: &IdedExpr) -> R {
    Value::resolve(e, ctx).map_err(CostEvalError::Exec)
}

/// Evaluate `e` with its operands replaced by hidden variables bound to the
/// already-evaluated operand values.
fn resolve_aliased(ctx: &Context, vals: &[Value], e: &IdedExpr) -> R {
    let mut inner = ctx.new_inner_scope();
    for (i, v) in vals.iter().enumerate() {
        inner.add_variable_from_value(format!("@cost{i}"), v.clone());
    }
    resolve(&inner, e)
}

fn expect_bool(v: R) -> Result<bool, CostEvalError> {
    match v? {
        Value::Bool(b) => Ok(b),
        _ => Err(CostEvalError::Exec(ExecutionError::no_such_overload(
            "cost",
            Vec::new(),
        ))),
    }
}

fn is_index(c: &CallExpr) -> bool {
    (c.func_name == operators::INDEX || c.func_name == operators::OPT_INDEX)
        && c.args.len() == 2
        && c.target.is_none()
}

/// An expression the cel-go planner turns into an attribute with qualifiers
/// (identifier, select, index) as opposed to a value computed by a call.
fn is_attr_like(e: &IdedExpr) -> bool {
    match &e.expr {
        Expr::Ident(_) | Expr::Select(_) => true,
        Expr::Call(c) => is_index(c),
        _ => false,
    }
}

/// `overloads.IsTypeConversionFunction`.
fn is_type_conversion(f: &str) -> bool {
    matches!(
        f,
        "bool"
            | "bytes"
            | "double"
            | "duration"
            | "dyn"
            | "int"
            | "string"
            | "timestamp"
            | "type"
            | "uint"
    )
}

/// An `InterpretableConst` after the planner's `decOptimize`
/// (`interpreter/decorators.go:107-124`): a literal, a list or map whose
/// elements are all constants (`maybeBuildListLiteral`, `maybeBuildMapLiteral`)
/// and a type conversion of a constant (`maybeOptimizeConstUnary`). A struct
/// (`Object{}`) is never folded.
fn is_const(e: &IdedExpr) -> bool {
    match &e.expr {
        Expr::Literal(_) => true,
        Expr::List(l) => l.elements.iter().all(is_const),
        Expr::Map(m) => m.entries.iter().all(|en| match &en.expr {
            EntryExpr::MapEntry(me) => is_const(&me.key) && is_const(&me.value),
            EntryExpr::StructField(_) => false,
        }),
        Expr::Call(c) => {
            c.target.is_none()
                && c.args.len() == 1
                && is_type_conversion(&c.func_name)
                && is_const(&c.args[0])
        }
        _ => false,
    }
}

/// `maybeOptimizeSetMembership` (`decorators.go:222-291`): `x in <constant
/// list of primitives>` becomes an `evalSetMembership`, which is not an
/// `InterpretableCall`, so `Observe` charges it nothing.
fn is_set_membership(list: &IdedExpr) -> bool {
    let Expr::List(l) = &list.expr else {
        return false;
    };
    is_const(list)
        && l.elements.iter().all(|el| {
            matches!(
                &el.expr,
                Expr::Literal(
                    LiteralValue::Boolean(_)
                        | LiteralValue::Int(_)
                        | LiteralValue::UInt(_)
                        | LiteralValue::Double(_)
                        | LiteralValue::String(_)
                )
            )
        })
}

fn key_value(k: &Key) -> Value {
    match k {
        Key::Int(i) => Value::Int(*i),
        Key::Uint(u) => Value::UInt(*u),
        Key::Bool(b) => Value::Bool(*b),
        Key::String(s) => Value::String(s.clone()),
    }
}

impl Tracker<'_> {
    /// The cost increment of `Observe` followed by its limit check
    /// (`runtimecost.go` `if tracker.Limit != nil && tracker.cost > *tracker.Limit`).
    fn add(&mut self, n: u64) -> Result<(), CostEvalError> {
        self.cost = self.cost.saturating_add(n);
        match self.opts.limit {
            Some(limit) if self.cost > limit => Err(CostEvalError::LimitExceeded),
            _ => Ok(()),
        }
    }

    /// Evaluate `e`, accumulating what cel-go's `Observe` would have for every
    /// node the evaluation reaches.
    fn walk(&mut self, e: &IdedExpr, ctx: &Context) -> R {
        if let Some(lazy) = self.lazy {
            if let Some(name) = lazy_access(e) {
                // `MapValue.Find`: evaluate the field on first access; an
                // unknown name is `no such key` from the lookup.
                lazy.ensure(&name, self.visible, self.opts);
                let mut scope = ctx.new_inner_scope();
                scope.add_variable_from_value(LAZY_VARIABLES_IDENT, lazy.bound_map());
                if let Some(Err(msg)) = lazy.lookup(&name) {
                    // The attribute and its qualifier are observed; the
                    // qualifier yields the error value.
                    self.add(SELECT_AND_IDENT_COST)?;
                    self.add(SELECT_AND_IDENT_COST)?;
                    return Err(CostEvalError::Exec(ExecutionError::function_error(
                        "variables",
                        msg,
                    )));
                }
                return self.walk_node(&rename_variables_operand(e), &scope);
            }
            if matches!(&e.expr, Expr::Ident(n) if n == "variables") {
                // The whole map (`Iterator.Next` evaluates every field).
                for (name, _) in &lazy.defs[..self.visible] {
                    lazy.ensure(name, self.visible, self.opts);
                }
                let mut scope = ctx.new_inner_scope();
                scope.add_variable_from_value("variables", lazy.bound_map());
                self.add(SELECT_AND_IDENT_COST)?;
                for (name, _) in &lazy.defs[..self.visible] {
                    if let Some(Err(msg)) = lazy.lookup(name) {
                        return Err(CostEvalError::Exec(ExecutionError::function_error(
                            "variables",
                            msg,
                        )));
                    }
                }
                return resolve(&scope, e);
            }
        }
        self.walk_node(e, ctx)
    }

    fn walk_node(&mut self, e: &IdedExpr, ctx: &Context) -> R {
        if is_const(e) {
            // `InterpretableConst`: zero cost.
            return resolve(ctx, e);
        }
        match &e.expr {
            // `InterpretableAttribute`: `SelectAndIdentCost` per attribute.
            Expr::Ident(_) => {
                self.add(SELECT_AND_IDENT_COST)?;
                resolve(ctx, e)
            }
            Expr::Select(s) => self.walk_select(e, s, ctx),
            Expr::Call(c) => self.walk_call(e, c, ctx),
            Expr::List(l) => self.walk_list(e, l, ctx),
            Expr::Map(m) => self.walk_map(e, m, ctx),
            Expr::Comprehension(c) => self.walk_comprehension(c, ctx),
            Expr::Struct(st) => self.walk_struct(e, st, ctx),
            Expr::Literal(_) | Expr::Unspecified => resolve(ctx, e),
        }
    }

    /// One qualifier of an attribute (`Qualifier`: `tracker.cost++`). The
    /// attribute itself is charged once, by its root (`evalWatchAttr`: the
    /// identifier arm of [`Self::walk`], or +1 for an attribute rooted in a call).
    /// A presence test charges `-SelectAndIdentCost` on the attribute unless
    /// `presenceTestHasCost` (`!tracker.presenceTestHasCost` -> `cost -= 1`).
    fn walk_select(&mut self, e: &IdedExpr, s: &SelectExpr, ctx: &Context) -> R {
        let operand = self.walk(&s.operand, ctx)?;
        if !is_attr_like(&s.operand) {
            // a relative attribute over a computed operand is itself observed
            self.add(SELECT_AND_IDENT_COST)?;
        }
        if !s.test || self.opts.presence_test_has_cost {
            self.add(SELECT_AND_IDENT_COST)?;
        }
        let mut out = e.clone();
        if let Expr::Select(sel) = &mut out.expr {
            *sel.operand = alias(0);
        }
        resolve_aliased(ctx, &[operand], &out)
    }

    fn walk_call(&mut self, e: &IdedExpr, c: &CallExpr, ctx: &Context) -> R {
        let f = c.func_name.as_str();
        // `evalConditional`: no direct cost, only the condition and the branch.
        if c.args.len() == 3 && f == operators::CONDITIONAL {
            let cond = expect_bool(self.walk(&c.args[0], ctx))?;
            return self.walk(&c.args[if cond { 1 } else { 2 }], ctx);
        }
        if c.args.len() == 2 && (f == operators::LOGICAL_OR || f == operators::LOGICAL_AND) {
            return self.walk_logical(f == operators::LOGICAL_OR, c, ctx);
        }
        if is_index(c) {
            return self.walk_index(e, c, ctx);
        }

        let mut operands: Vec<&IdedExpr> = Vec::new();
        if let Some(t) = &c.target {
            operands.push(t);
        }
        operands.extend(c.args.iter());
        // Every operand is evaluated before an error is returned
        // (`evalBinary.Eval`, `evalVarArgs.Eval`).
        let mut vals = Vec::with_capacity(operands.len());
        let mut first_err = None;
        for op in operands {
            match self.walk(op, ctx) {
                Ok(v) => vals.push(v),
                Err(CostEvalError::LimitExceeded) => return Err(CostEvalError::LimitExceeded),
                Err(err) => {
                    first_err.get_or_insert(err);
                    vals.push(Value::Null);
                }
            }
        }
        let charged = !f.starts_with("__")
            && !(f == operators::IN && c.args.len() == 2 && is_set_membership(&c.args[1]));
        let result = match first_err {
            Some(err) => Err(err),
            None => {
                let mut call = c.clone();
                let mut i = 0;
                if call.target.is_some() {
                    call.target = Some(Box::new(alias(0)));
                    i = 1;
                }
                for (n, a) in call.args.iter_mut().enumerate() {
                    *a = alias(i + n);
                }
                let mut out = e.clone();
                out.expr = Expr::Call(call);
                resolve_aliased(ctx, &vals, &out)
            }
        };
        if charged {
            self.add(call_cost(f, &vals, result.as_ref().ok()))?;
        }
        result
    }

    /// `evalOr` / `evalAnd` (no direct cost) with the short-circuit and error
    /// absorption of `objects.rs` `Value::resolve_val`.
    fn walk_logical(&mut self, or: bool, c: &CallExpr, ctx: &Context) -> R {
        let left = self.walk(&c.args[0], ctx);
        if let Err(CostEvalError::LimitExceeded) = left {
            return left;
        }
        let left = expect_bool(left);
        if let Ok(b) = left {
            if b == or {
                return Ok(Value::Bool(or));
            }
        }
        let right = match self.walk(&c.args[1], ctx)? {
            Value::Bool(b) => Some(b),
            _ => None,
        };
        match (&left, right) {
            (Ok(_), Some(r)) => Ok(Value::Bool(r)),
            (Err(_), Some(r)) if r == or => Ok(Value::Bool(or)),
            _ => Err(left
                .err()
                .unwrap_or(CostEvalError::Exec(ExecutionError::no_such_overload(
                    "cost",
                    Vec::new(),
                )))),
        }
    }

    /// `a[k]` is a qualifier of the attribute `a` (`ConstantQualifier` for a
    /// constant key: `cost++`; `evalWatchAttrQual` for an attribute key: the
    /// key's own attribute observation is the qualifier observation).
    fn walk_index(&mut self, e: &IdedExpr, c: &CallExpr, ctx: &Context) -> R {
        let base = self.walk(&c.args[0], ctx)?;
        if !is_attr_like(&c.args[0]) {
            self.add(SELECT_AND_IDENT_COST)?;
        }
        let key = self.walk(&c.args[1], ctx)?;
        if is_const(&c.args[1]) || !is_attr_like(&c.args[1]) {
            self.add(SELECT_AND_IDENT_COST)?;
        }
        let mut call = c.clone();
        call.args = vec![alias(0), alias(1)];
        let mut out = e.clone();
        out.expr = Expr::Call(call);
        resolve_aliased(ctx, &[base, key], &out)
    }

    /// `InterpretableConstructor`: `ListCreateBaseCost`.
    fn walk_list(&mut self, e: &IdedExpr, l: &ListExpr, ctx: &Context) -> R {
        let mut vals = Vec::with_capacity(l.elements.len());
        for el in &l.elements {
            vals.push(self.walk(el, ctx)?);
        }
        self.add(LIST_CREATE_BASE_COST)?;
        let mut list = l.clone();
        list.elements = (0..vals.len()).map(alias).collect();
        let mut out = e.clone();
        out.expr = Expr::List(list);
        resolve_aliased(ctx, &vals, &out)
    }

    /// `InterpretableConstructor`: `MapCreateBaseCost`.
    fn walk_map(&mut self, e: &IdedExpr, m: &MapExpr, ctx: &Context) -> R {
        let mut vals = Vec::new();
        let mut entries = Vec::with_capacity(m.entries.len());
        for en in &m.entries {
            let EntryExpr::MapEntry(me) = &en.expr else {
                return resolve(ctx, e);
            };
            let k = vals.len();
            vals.push(self.walk(&me.key, ctx)?);
            vals.push(self.walk(&me.value, ctx)?);
            entries.push(IdedEntryExpr {
                id: en.id,
                expr: EntryExpr::MapEntry(MapEntryExpr {
                    key: alias(k),
                    value: alias(k + 1),
                    optional: me.optional,
                }),
            });
        }
        self.add(MAP_CREATE_BASE_COST)?;
        let mut out = e.clone();
        out.expr = Expr::Map(MapExpr { entries });
        resolve_aliased(ctx, &vals, &out)
    }

    /// `InterpretableConstructor`: `StructCreateBaseCost` (`Object{}` /
    /// `JSONPatch{}`). The field values are walked here so the stock
    /// interpreter does not evaluate them unobserved.
    fn walk_struct(&mut self, e: &IdedExpr, s: &StructExpr, ctx: &Context) -> R {
        let mut vals = Vec::new();
        let mut entries = Vec::with_capacity(s.entries.len());
        for en in &s.entries {
            let EntryExpr::StructField(f) = &en.expr else {
                return resolve(ctx, e);
            };
            let k = vals.len();
            vals.push(self.walk(&f.value, ctx)?);
            entries.push(IdedEntryExpr {
                id: en.id,
                expr: EntryExpr::StructField(StructFieldExpr {
                    field: f.field.clone(),
                    value: alias(k),
                    optional: f.optional,
                }),
            });
        }
        self.add(STRUCT_CREATE_BASE_COST)?;
        let mut out = e.clone();
        out.expr = Expr::Struct(StructExpr {
            type_name: s.type_name.clone(),
            entries,
        });
        resolve_aliased(ctx, &vals, &out)
    }

    /// `evalFold`: no direct cost; the range, the initial value and every node
    /// of every iteration are observed. The loop is that of `objects.rs`
    /// `Expr::Comprehension` (condition first, then bind, then step).
    fn walk_comprehension(&mut self, c: &ComprehensionExpr, ctx: &Context) -> R {
        let accu_init = self.walk(&c.accu_init, ctx)?;
        let range = self.walk(&c.iter_range, ctx)?;
        let items: Vec<Value> = match &range {
            Value::List(l) => l.iter().cloned().collect(),
            Value::Map(m) => m.map.keys().map(key_value).collect(),
            _ => {
                return Err(CostEvalError::Exec(ExecutionError::no_such_overload(
                    "cost",
                    Vec::new(),
                )))
            }
        };
        let mut inner = ctx.new_inner_scope();
        inner.add_variable_from_value(c.accu_var.clone(), accu_init);
        for item in items {
            if !expect_bool(self.walk(&c.loop_cond, &inner))? {
                break;
            }
            inner.add_variable_from_value(c.iter_var.clone(), item);
            let accu = self.walk(&c.loop_step, &inner)?;
            inner.add_variable_from_value(c.accu_var.clone(), accu);
        }
        self.walk(&c.result, &inner)
    }
}

/// `actualSize` (cost.go:544): the size of a `traits.Sizer`, else 1.
fn actual_size(v: &Value) -> u64 {
    match v {
        Value::String(s) => s.chars().count() as u64,
        Value::Bytes(b) => b.len() as u64,
        Value::List(l) => l.len() as u64,
        Value::Map(m) => m.map.len() as u64,
        _ => 1,
    }
}

/// `multiplyByCostFactor` (checker/cost.go:236): `ceil(x * factor)`.
fn ceil_factor(x: u64, factor: f64) -> u64 {
    (x as f64 * factor).ceil() as u64
}

/// `traversalCost` (cost.go:616).
fn traversal_cost(v: &Value) -> u64 {
    match v {
        Value::String(s) => (s.len() as f64 * STRING_TRAVERSAL_COST_FACTOR) as u64,
        Value::Bytes(b) => (b.len() as f64 * STRING_TRAVERSAL_COST_FACTOR) as u64,
        Value::List(l) => l.iter().map(traversal_cost).sum(),
        Value::Map(m) => m
            .map
            .iter()
            .map(|(k, v)| traversal_cost(&key_value(k)) + traversal_cost(v))
            .sum(),
        _ => 1,
    }
}

/// `selectorCostEstimate` (cost.go:75-82), `.Max`.
fn selector_cost(selector_length: u64) -> u64 {
    let parse = ceil_factor(selector_length, STRING_TRAVERSAL_COST_FACTOR);
    let requirement_count = ceil_factor(selector_length, 0.5);
    let requirement = ceil_factor(
        requirement_count,
        (LIST_CREATE_BASE_COST + STRUCT_CREATE_BASE_COST) as f64,
    );
    LIST_CREATE_BASE_COST + parse + requirement
}

/// The regex cost shared by `matches`, `find` and `findAll`.
fn regex_cost(text_size: u64, regex_size: u64) -> u64 {
    ceil_factor(1 + text_size, STRING_TRAVERSAL_COST_FACTOR)
        * ceil_factor(regex_size, REGEX_STRING_LENGTH_COST_FACTOR)
}

fn is_string(v: &Value) -> bool {
    matches!(v, Value::String(_))
}

fn is_bytes(v: &Value) -> bool {
    matches!(v, Value::Bytes(_))
}

/// `CostEstimator.CallCost` (`library/cost.go:99-233`); `None` is a nil
/// estimate, which falls through to the standard library costs.
fn library_call_cost(function: &str, args: &[Value], result: Option<&Value>) -> Option<u64> {
    match function {
        // An authorization check allows only two checks per expression.
        "check" => Some(350_000),
        "serviceAccount" | "path" | "group" | "resource" | "subresource" | "namespace" | "name"
        | "allowed" | "reason" | "error" | "errored" => Some(1),
        "fieldSelector" | "labelSelector" if args.len() >= 2 => {
            Some(selector_cost(actual_size(&args[1])))
        }
        "isSorted" | "sum" | "max" | "min" | "indexOf" | "lastIndexOf" => {
            Some(args.first().map(traversal_cost).unwrap_or(0))
        }
        "url" | "lowerAscii" | "upperAscii" | "substring" | "trim" | "jsonpatch.escapeKey"
            if !args.is_empty() =>
        {
            Some(ceil_factor(
                actual_size(&args[0]),
                STRING_TRAVERSAL_COST_FACTOR,
            ))
        }
        "replace" | "split" if !args.is_empty() => Some(ceil_factor(
            actual_size(&args[0]) * 2,
            STRING_TRAVERSAL_COST_FACTOR,
        )),
        "join" if !args.is_empty() => Some(ceil_factor(
            result.map(actual_size).unwrap_or(1) * 2,
            STRING_TRAVERSAL_COST_FACTOR,
        )),
        "find" | "findAll" if args.len() >= 2 => {
            Some(regex_cost(actual_size(&args[0]), actual_size(&args[1])))
        }
        "cidr" | "isIP" | "isCIDR" if !args.is_empty() => Some(ceil_factor(
            actual_size(&args[0]),
            STRING_TRAVERSAL_COST_FACTOR,
        )),
        // overload `cidr_ip`: the IP member of a CIDR object (not a string).
        "ip" if !args.is_empty() => Some(if is_string(&args[0]) {
            ceil_factor(actual_size(&args[0]), STRING_TRAVERSAL_COST_FACTOR)
        } else {
            1
        }),
        "ip.isCanonical" if !args.is_empty() => Some(ceil_factor(
            actual_size(&args[0]) * 2,
            STRING_TRAVERSAL_COST_FACTOR,
        )),
        "masked"
        | "prefixLength"
        | "family"
        | "isUnspecified"
        | "isLoopback"
        | "isLinkLocalMulticast"
        | "isLinkLocalUnicast"
        | "isGlobalUnicast" => Some(1),
        // overload `cidr_contains_ip_string`.
        "containsIP" if args.len() >= 2 => {
            let cidr = actual_size(&args[0]);
            let mut cost = ceil_factor(cidr + cidr, STRING_TRAVERSAL_COST_FACTOR);
            if is_string(&args[1]) {
                cost += ceil_factor(actual_size(&args[1]), STRING_TRAVERSAL_COST_FACTOR);
            }
            Some(cost)
        }
        // overload `cidr_contains_cidr_string`.
        "containsCIDR" if args.len() >= 2 => {
            let cidr = actual_size(&args[0]);
            let mut cost = ceil_factor(cidr + cidr, STRING_TRAVERSAL_COST_FACTOR);
            cost += ceil_factor(cidr, STRING_TRAVERSAL_COST_FACTOR) + 1;
            if is_string(&args[1]) {
                cost += ceil_factor(actual_size(&args[1]), STRING_TRAVERSAL_COST_FACTOR);
            }
            Some(cost)
        }
        "quantity" | "isQuantity" | "semver" | "isSemver" if !args.is_empty() => Some(ceil_factor(
            actual_size(&args[0]),
            STRING_TRAVERSAL_COST_FACTOR,
        )),
        "format.named" => Some(1),
        "sign" | "asInteger" | "isInteger" | "asApproximateFloat" | "isGreaterThan"
        | "isLessThan" | "compareTo" | "add" | "sub" | "major" | "minor" | "patch" => Some(1),
        "getScheme" | "getHostname" | "getHost" | "getPort" | "getEscapedPath" | "getQuery" => {
            Some(1)
        }
        // The Kubernetes types (quantity, IP, CIDR, format, URL, semver,
        // authorizer values) compare at unit cost.
        operators::EQUALS if args.len() == 2 && matches!(args[0], Value::Opaque(_)) => Some(1),
        _ => None,
    }
}

/// `CostEstimator.CallCost`, then the standard library switch of
/// `CostTracker.costCall` (`runtimecost.go`), keyed by the overload that the
/// operand types select.
pub fn call_cost(function: &str, args: &[Value], result: Option<&Value>) -> u64 {
    if let Some(cost) = library_call_cost(function, args, result) {
        return cost;
    }
    let size = |i: usize| args.get(i).map(actual_size).unwrap_or(1);
    let both_strings = args.len() >= 2 && is_string(&args[0]) && is_string(&args[1]);
    let both_bytes = args.len() >= 2 && is_bytes(&args[0]) && is_bytes(&args[1]);
    match function {
        // O(n): starts_with_string, ends_with_string, string_to_bytes,
        // bytes_to_string, ext quote/format string.
        "startsWith" | "endsWith" | "format" | "strings.quote" | "quote" | "bytes"
            if args.first().is_some_and(is_string) =>
        {
            ceil_factor(size(0), STRING_TRAVERSAL_COST_FACTOR)
        }
        "string" if args.first().is_some_and(is_bytes) => {
            ceil_factor(size(0), STRING_TRAVERSAL_COST_FACTOR)
        }
        // in_list: O(n); "If a list is composed entirely of constant values
        // this is O(1), but we don't account for that here."
        operators::IN if args.len() == 2 && matches!(args[1], Value::List(_)) => size(1),
        // O(min(m, n)): ordering of strings/bytes, and (in)equality of anything.
        operators::LESS
        | operators::GREATER
        | operators::LESS_EQUALS
        | operators::GREATER_EQUALS
            if both_strings || both_bytes =>
        {
            ceil_factor(size(0).min(size(1)), STRING_TRAVERSAL_COST_FACTOR)
        }
        operators::EQUALS | operators::NOT_EQUALS if args.len() >= 2 => {
            ceil_factor(size(0).min(size(1)), STRING_TRAVERSAL_COST_FACTOR)
        }
        // O(m+n): add_string, add_bytes.
        operators::ADD if both_strings || both_bytes => {
            ceil_factor(size(0) + size(1), STRING_TRAVERSAL_COST_FACTOR)
        }
        // O(nm): matches_string.
        "matches" if args.len() >= 2 && is_string(&args[0]) => regex_cost(size(0), size(1)),
        // O(nm): contains_string.
        "contains" if both_strings => {
            ceil_factor(size(0), STRING_TRAVERSAL_COST_FACTOR)
                * ceil_factor(size(1), STRING_TRAVERSAL_COST_FACTOR)
        }
        // O(1): everything else (`default: cost++`).
        _ => 1,
    }
}

/// The `cel.Error` of `activation.go`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetError {
    pub detail: String,
    /// `Cause == ErrOutOfBudget`.
    pub out_of_budget: bool,
}

fn out_of_budget() -> BudgetError {
    BudgetError {
        detail: OUT_OF_BUDGET_DETAIL.to_string(),
        out_of_budget: true,
    }
}

/// The budget half of `evaluationActivation.Evaluate` (`activation.go:
/// 143-174`): charge the composition (variable) cost, then the actual cost,
/// against `remaining_budget`; the remaining budget, or the out-of-budget
/// error that stops all further evaluation.
pub fn charge_budget(
    composition_cost: Option<i64>,
    rt_cost: u64,
    remaining_budget: i64,
) -> Result<i64, BudgetError> {
    let mut remaining = remaining_budget;
    // budget may be spent due to lazy evaluation of composited variables
    if let Some(composition) = composition_cost {
        if composition > remaining {
            return Err(out_of_budget());
        }
        remaining -= composition;
    }
    // `evalDetails == nil` / `ActualCost() == nil` cannot occur here: every
    // evaluation reports its cost.
    if rt_cost > i64::MAX as u64 || rt_cost as i64 > remaining {
        return Err(out_of_budget());
    }
    Ok(remaining - rt_cost as i64)
}

/// `evaluationActivation.Evaluate` from the evaluation on (`activation.go:
/// 132-189`): [`charge_budget`] for `evaluate`'s cost. Returns the evaluation
/// result (an `Err` is `evaluation.Error`'s detail) and the remaining budget;
/// the `BudgetError` is the error returned instead of an `EvaluationResult`.
pub fn evaluate_with_budget(
    expression: &str,
    evaluate: impl FnOnce() -> (Result<Value, CostEvalError>, u64),
    composition_cost: Option<i64>,
    remaining_budget: i64,
) -> Result<(Result<Value, String>, i64), BudgetError> {
    let (result, rt_cost) = evaluate();
    let remaining = charge_budget(composition_cost, rt_cost, remaining_budget)?;
    match result {
        Err(e) => Ok((
            Err(format!("expression '{expression}' resulted in error: {e}")),
            remaining,
        )),
        Ok(v) => Ok((Ok(v), remaining)),
    }
}
