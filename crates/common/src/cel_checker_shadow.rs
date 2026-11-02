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

/// The checker's verdict for `expression`: its error messages (empty when the
/// expression type-checks), or `None` when the expression does not parse
/// (syntax errors are not this comparison's concern).
pub fn check_expression(expression: &str, set: &EnvSet, env_type: EnvType) -> Option<Vec<String>> {
    let source = expression.to_string();
    // The antlr4rust parser panics on some invalid expressions.
    let parsed =
        std::panic::catch_unwind(|| Parser::new().enable_optional_syntax(true).parse(&source))
            .ok()?
            .ok()?;
    let version = set.compatibility_version();
    let features = checker::Features {
        authorize_with_selectors: feature_gates::enabled(Feature::AuthorizeWithSelectors),
    };
    let env_set = checker::EnvSet::must_base_env_set(
        checker::Version::major_minor(version.major, version.minor),
        &features,
    );
    let ct = match env_type {
        EnvType::NewExpressions => checker::EnvType::NewExpressions,
        EnvType::StoredExpressions => checker::EnvType::StoredExpressions,
    };
    let mut env = env_set.env(
        ct,
        checker::Container::default(),
        Arc::new(checker::MapProvider::new()),
    );
    for name in DYN_VARIABLES {
        env.add_ident(checker::VariableDecl::new(*name, checker::DType::Dyn))
            .ok();
    }
    let (_, errors) = checker::check(&parsed, &mut env, &checker::NoLocations);
    Some(errors.errors().iter().map(|e| e.message.clone()).collect())
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
) -> Option<Divergence> {
    let checker = check_expression(expression, set, env_type)?;
    (probe.is_some() != !checker.is_empty()).then(|| Divergence {
        probe: probe.clone(),
        checker,
    })
}

/// The hook called by the probe: in `shadow` mode logs a divergence. Never
/// affects the probe's result.
pub fn observe(expression: &str, set: &EnvSet, env_type: EnvType, probe: &Option<String>) {
    if Mode::current() != Mode::Shadow {
        return;
    }
    if let Some(d) = divergence(expression, set, env_type, probe) {
        tracing::warn!(
            target: "cel_checker_shadow",
            expression,
            env_type = ?env_type,
            version = ?set.compatibility_version(),
            probe = ?d.probe,
            checker = ?d.checker,
            "cel checker disagrees with the compile probe"
        );
    }
}
