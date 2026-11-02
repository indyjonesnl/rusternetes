//! Shadow run of the cel-go checker port (`rusternetes-cel-checker`) next to
//! the existing compile-time probe, [`crate::cel_versions::compile_failure`].
//!
//! Controlled by `RUSTERNETES_CEL_CHECKER`:
//!
//! * unset / `off` (default): nothing runs.
//! * `shadow`: every probe result is compared with the checker's verdict for
//!   the same expression and environment, and a disagreement is logged
//!   (`tracing::warn!`, target `cel_checker_shadow`). The probe result is
//!   never changed: the checker cannot reject anything in this mode.
//!
//! There is deliberately no enforcing mode yet. Variables are declared `dyn`
//! (schema-typed environments come with a later slice), so the comparison
//! measures the library / overload layer only.
//!
//! The environment follows upstream's `EnvSet` (`environment/base.go`
//! `MustBaseEnvSet`, `environment.go` `Env`): the same compatibility version
//! and `NewExpressions` / `StoredExpressions` choice the probe was given, with
//! `AuthorizeWithSelectors` read from the feature gate as `baseOpts` does.

use crate::cel_versions::{EnvSet, EnvType};
use crate::feature_gates::{self, Feature};
use cel::parser::Parser;
use rusternetes_cel_checker as checker;
use rusternetes_cel_checker::decl::{DeclType, DeclTypeProvider};
use rusternetes_cel_checker::DType;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Environment variable selecting the mode.
pub const FLAG: &str = "RUSTERNETES_CEL_CHECKER";

/// What `RUSTERNETES_CEL_CHECKER` selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off,
    Shadow,
}

impl Mode {
    /// Parses the flag value; anything but `shadow` is `off`.
    pub fn parse(value: Option<&str>) -> Mode {
        match value.map(str::trim) {
            Some(v) if v.eq_ignore_ascii_case("shadow") => Mode::Shadow,
            _ => Mode::Off,
        }
    }

    /// The mode of this process, read once.
    pub fn current() -> Mode {
        static MODE: std::sync::OnceLock<Mode> = std::sync::OnceLock::new();
        *MODE.get_or_init(|| Mode::parse(std::env::var(FLAG).ok().as_deref()))
    }
}

/// The variables the Kubernetes CEL environments declare, as `dyn`.
const DYN_VARIABLES: &[&str] = &[
    "object",
    "oldObject",
    "request",
    "params",
    "variables",
    "namespaceObject",
    "authorizer",
    "self",
    "oldSelf",
    "device",
];

/// The checker's `EnvSet` for the probe's `EnvSet` and environment type: the
/// same compatibility version, `AuthorizeWithSelectors` from the feature gate
/// as `base.go` `baseOpts` reads it.
pub(crate) fn checker_env_set(
    set: &EnvSet,
    env_type: EnvType,
) -> (checker::EnvSet, checker::EnvType) {
    let version = set.compatibility_version();
    let features = checker::Features {
        authorize_with_selectors: feature_gates::enabled(Feature::AuthorizeWithSelectors),
    };
    let env_set = checker::EnvSet::must_base_env_set(
        checker::Version::major_minor(version.major, version.minor),
        &features,
    );
    let ct = match env_type {
        EnvType::StoredExpressions => checker::EnvType::StoredExpressions,
        EnvType::NewExpressions => checker::EnvType::NewExpressions,
    };
    (env_set, ct)
}

/// The scope of a CRD rule: the type `self` (and `oldSelf`) has.
#[derive(Clone, Debug)]
pub struct SelfScope {
    pub decl: DeclType,
    /// `optionalOldSelf`: `oldSelf` is `optional_type(T)`.
    pub optional_old_self: bool,
}

/// `generateUniqueSelfTypeName` (`compilation.go`).
fn unique_self_type_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("selfType{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// The checker's verdict for `expression`: its error messages (empty when the
/// expression type-checks), or `None` when the expression does not parse
/// (syntax errors are not this comparison's concern). With `scope` the
/// environment is the CRD rule environment (`self` / `oldSelf` typed by the
/// schema, no other variables; `compilation.go` `prepareEnvSet`), otherwise
/// the variables of [`DYN_VARIABLES`] are `dyn`.
pub fn check_expression(
    expression: &str,
    set: &EnvSet,
    env_type: EnvType,
    scope: Option<&SelfScope>,
) -> Option<Vec<String>> {
    run_checker(expression, set, env_type, scope).map(|c| c.errors)
}

/// A parsed and checked expression.
struct Checked {
    expr: cel::common::ast::IdedExpr,
    ast: checker::CheckedAst,
    errors: Vec<String>,
    /// The type `self` has in a scoped check, with its unique name.
    scoped: Option<DeclType>,
}

fn run_checker(
    expression: &str,
    set: &EnvSet,
    env_type: EnvType,
    scope: Option<&SelfScope>,
) -> Option<Checked> {
    let source = expression.to_string();
    // The antlr4rust parser panics on some invalid expressions.
    let parsed =
        std::panic::catch_unwind(|| Parser::new().enable_optional_syntax(true).parse(&source))
            .ok()?
            .ok()?;
    let version = set.compatibility_version();
    let (env_set, ct) = checker_env_set(set, env_type);
    let stored = env_type == EnvType::StoredExpressions;
    let (provider, scoped): (Arc<dyn checker::Provider>, Option<DeclType>) = match scope {
        Some(scope) => {
            let scoped = scope.decl.maybe_assign_type_name(&unique_self_type_name());
            // `environment.go:268-272`: keywords as field names from 1.31.
            let keywords = stored || (version.major, version.minor) >= (1, 31);
            let provider = DeclTypeProvider::new(
                std::slice::from_ref(&scoped),
                Arc::new(checker::MapProvider::new()),
                keywords,
            );
            (Arc::new(provider), Some(scoped))
        }
        None => (Arc::new(checker::MapProvider::new()), None),
    };
    let mut env = env_set.env(ct, checker::Container::default(), provider);
    let scoped_type = scoped.clone();
    match (scope, scoped) {
        (Some(scope), Some(scoped)) => {
            let t = scoped.dtype();
            env.add_ident(checker::VariableDecl::new("self", t.clone()))
                .ok();
            // `oldSelf` is introduced at 1.24 (`compilation.go:prepareEnvSet`).
            if stored || (version.major, version.minor) >= (1, 24) {
                let old = if scope.optional_old_self {
                    DType::optional(t)
                } else {
                    t
                };
                env.add_ident(checker::VariableDecl::new("oldSelf", old))
                    .ok();
            }
        }
        _ => {
            for name in DYN_VARIABLES {
                env.add_ident(checker::VariableDecl::new(*name, DType::Dyn))
                    .ok();
            }
        }
    }
    let (ast, errors) = checker::check(&parsed, &mut env, &checker::NoLocations);
    Some(Checked {
        expr: parsed,
        ast,
        errors: errors.errors().iter().map(|e| e.message.clone()).collect(),
        scoped: scoped_type,
    })
}

/// A disagreement between the probe and the checker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Divergence {
    /// The probe's failure detail, `None` when it accepted the expression.
    pub probe: Option<String>,
    /// The checker's error messages, empty when it accepted the expression.
    pub checker: Vec<String>,
}

/// Compares the probe's result with the checker's verdict. `None` when they
/// agree on accept / reject (the wording of a rejection is not compared) or
/// when the expression does not parse.
pub fn divergence(
    expression: &str,
    set: &EnvSet,
    env_type: EnvType,
    probe: &Option<String>,
    scope: Option<&SelfScope>,
) -> Option<Divergence> {
    let checker = check_expression(expression, set, env_type, scope)?;
    (probe.is_some() != !checker.is_empty()).then(|| Divergence {
        probe: probe.clone(),
        checker,
    })
}

/// The hook called by the probe: in `shadow` mode logs a divergence. Never
/// affects the probe's result.
pub fn observe(expression: &str, set: &EnvSet, env_type: EnvType, probe: &Option<String>) {
    observe_scoped(expression, set, env_type, probe, None);
}

/// [`observe`] for a CRD rule, with `self` / `oldSelf` typed by the schema.
pub fn observe_scoped(
    expression: &str,
    set: &EnvSet,
    env_type: EnvType,
    probe: &Option<String>,
    scope: Option<&SelfScope>,
) {
    if Mode::current() != Mode::Shadow {
        return;
    }
    if let Some(d) = divergence(expression, set, env_type, probe, scope) {
        tracing::warn!(
            target: "cel_checker_shadow",
            expression,
            env_type = ?env_type,
            typed = scope.is_some(),
            version = ?set.compatibility_version(),
            probe = ?d.probe,
            checker = ?d.checker,
            "cel checker disagrees with the compile probe"
        );
    }
}

/// The static cost estimate of a CRD rule: `ruleEnv.EstimateCost(ast,
/// estimator)` of `compilation.go` `compileRule`, with the estimator of
/// `newCostEstimator(declType)` (the library estimator over the schema size
/// estimator). `None` when the expression does not parse or does not
/// type-check.
pub fn estimate_rule_cost(
    expression: &str,
    set: &EnvSet,
    env_type: EnvType,
    scope: &SelfScope,
) -> Option<checker::cost::CostEstimate> {
    let c = run_checker(expression, set, env_type, Some(scope))?;
    if !c.errors.is_empty() {
        return None;
    }
    let sizes = crate::cel_crd_cost::SchemaSizeEstimator::new(c.scoped?);
    Some(checker::library_cost::kubernetes_cost(
        &c.ast,
        &c.expr,
        Some(&sizes),
    ))
}

/// The hook of the cost check of a CRD rule: in `shadow` mode compares the
/// static estimate with the heuristic the validation decides with, and logs
/// when they disagree on whether the rule is over `limit`. Never affects the
/// heuristic's result.
pub fn observe_cost(
    expression: &str,
    set: &EnvSet,
    env_type: EnvType,
    scope: &SelfScope,
    heuristic_cost: u64,
    limit: u64,
) {
    if Mode::current() != Mode::Shadow {
        return;
    }
    let Some(est) = estimate_rule_cost(expression, set, env_type, scope) else {
        return;
    };
    // `validation.go:1280-1287` compares the estimate's `Max` to the limit.
    if (est.max > limit) != (heuristic_cost > limit) {
        tracing::warn!(
            target: "cel_checker_shadow",
            expression,
            estimated_min = est.min,
            estimated_max = est.max,
            heuristic = heuristic_cost,
            limit,
            "cel cost estimate disagrees with the heuristic on exceeding the limit"
        );
    }
}
