//! Version-gated CEL function libraries: the `EnvSet` of
//! `staging/src/k8s.io/apiserver/pkg/cel/environment` (release-1.35).
//!
//! Upstream builds two cel-go environments per compatibility version
//! (`environment.go:30-77`): `NewExpressions`, holding only the libraries
//! introduced at or before the version (`filterAndBuildOpts`), and
//! `StoredExpressions`, holding every library (`Extend` passes `MaxUint` as the
//! version and ignores feature gates). `base.go` is the `baseOpts` table of
//! which library arrives (and is replaced) at which Kubernetes version, and
//! `DefaultCompatibilityVersion` is `EffectiveVersion.MinCompatibilityVersion()`.
//!
//! The `cel` crate has no checker and no per-environment function
//! declarations (it resolves functions at execution), so the declaration half
//! of an environment is modelled as the set of function names each library
//! declares, and [`compile_failure`] reports the checker's
//! `undeclared reference to 'f' (in container '')` for a call to a function
//! whose libraries are all outside the environment. Calls are matched by name
//! (the checker's overload resolution is not modelled), the same reduction
//! `cel_env` makes.

use crate::feature_gates::{self, Feature, Version};
use cel::common::ast::{EntryExpr, Expr, IdedExpr};

/// `environment.VersionedOptions`, reduced to the declarations that decide
/// whether a call resolves.
#[derive(Debug, Clone, Copy)]
pub struct VersionedOptions {
    /// `Library.LibraryName()` of the cel-go library the options install.
    pub library: &'static str,
    /// `IntroducedVersion`.
    pub introduced: Version,
    /// `RemovedVersion`.
    pub removed: Option<Version>,
    /// `FeatureEnabled`.
    pub feature_enabled: Option<fn() -> bool>,
    /// Names of the functions the library declares (`cel.Function(name, ...)`,
    /// namespaced as `ns.name`).
    pub functions: &'static [&'static str],
    /// The library adds the two-variable comprehension macros
    /// (`ext.TwoVarComprehensions`).
    pub two_var_comprehensions: bool,
}

const fn v(minor: u32) -> Version {
    Version::major_minor(1, minor)
}

const fn lib(
    library: &'static str,
    introduced: Version,
    removed: Option<Version>,
    functions: &'static [&'static str],
) -> VersionedOptions {
    VersionedOptions {
        library,
        introduced,
        removed,
        feature_enabled: None,
        functions,
        two_var_comprehensions: false,
    }
}

/// `utilfeature.DefaultFeatureGate.Enabled(genericfeatures.AuthorizeWithSelectors)`
/// (`base.go:131-141`).
fn authorize_with_selectors() -> bool {
    feature_gates::enabled(Feature::AuthorizeWithSelectors)
}

/// `format.<name>()` for every `ConstantFormats` key
/// (`library/format.go:143-247`, registered at `:125-136`), `format.named`
/// and `validate` (`:249-255`).
const FORMAT_FUNCTIONS: &[&str] = &[
    "validate",
    "format.named",
    "format.dns1123Label",
    "format.dns1123Subdomain",
    "format.dns1035Label",
    "format.qualifiedName",
    "format.dns1123LabelPrefix",
    "format.dns1123SubdomainPrefix",
    "format.dns1035LabelPrefix",
    "format.labelValue",
    "format.uri",
    "format.uuid",
    "format.byte",
    "format.date",
    "format.datetime",
];

const STRINGS_V0: &[&str] = &[
    "charAt",
    "indexOf",
    "lastIndexOf",
    "lowerAscii",
    "replace",
    "split",
    "substring",
    "trim",
    "upperAscii",
];

const STRINGS_V2: &[&str] = &[
    "charAt",
    "indexOf",
    "lastIndexOf",
    "lowerAscii",
    "replace",
    "split",
    "substring",
    "trim",
    "upperAscii",
    "format",
    "strings.quote",
    "join",
];

/// `baseOpts` (`base.go:63-169`). Options that install no function and no
/// macro (`HomogeneousAggregateLiterals`, the AST validators, cost options,
/// `CrossTypeNumericComparisons`) are not modelled: they declare nothing a
/// call can resolve against. The function names are the `cel.Function` names
/// of each library: `library/{urls,regex,lists,authz,quantity,ip,cidr,format,
/// semverlib}.go`, cel-go `ext/{strings,sets,lists}.go` (vendored at the
/// release-1.35 pin). `string` (a standard-library conversion that `ip` and
/// `cidr` also overload) is left out.
static BASE_OPTS: &[VersionedOptions] = &[
    lib(
        "kubernetes.urls",
        v(0),
        None,
        &[
            "url",
            "isURL",
            "getScheme",
            "getHost",
            "getHostname",
            "getPort",
            "getEscapedPath",
            "getQuery",
        ],
    ),
    lib("kubernetes.regex", v(0), None, &["find", "findAll"]),
    lib(
        "kubernetes.lists",
        v(0),
        None,
        &["isSorted", "sum", "min", "max", "indexOf", "lastIndexOf"],
    ),
    lib(
        "kubernetes.authz",
        v(27),
        None,
        &[
            "path",
            "group",
            "serviceAccount",
            "resource",
            "subresource",
            "namespace",
            "name",
            "check",
            "errored",
            "error",
            "allowed",
            "reason",
        ],
    ),
    lib(
        "cel.lib.optional",
        v(28),
        None,
        &[
            "optional.of",
            "optional.none",
            "optional.ofNonZeroValue",
            "hasValue",
            "orValue",
            "optMap",
            "optFlatMap",
        ],
    ),
    lib(
        "kubernetes.quantity",
        v(28),
        None,
        &[
            "quantity",
            "isQuantity",
            "sign",
            "isGreaterThan",
            "isLessThan",
            "compareTo",
            "asApproximateFloat",
            "asInteger",
            "isInteger",
            "add",
            "sub",
        ],
    ),
    // `ext.Strings(ext.StringsVersion(0))` until 1.29, then
    // `ext.StringsVersion(2)` (cel-go ext/strings.go:373-515).
    lib("cel.lib.ext.strings", v(0), Some(v(29)), STRINGS_V0),
    lib("cel.lib.ext.strings", v(29), None, STRINGS_V2),
    // `ext.Sets()` (ext/sets.go:112-118).
    lib(
        "cel.lib.ext.sets",
        v(29),
        None,
        &["sets.contains", "sets.equivalent", "sets.intersects"],
    ),
    // `library.IP`, `library.CIDR` (ip.go:147-185, cidr.go:124-156).
    lib(
        "kubernetes.net.ip",
        v(30),
        None,
        &[
            "ip",
            "family",
            "ip.isCanonical",
            "isUnspecified",
            "isLoopback",
            "isLinkLocalMulticast",
            "isLinkLocalUnicast",
            "isGlobalUnicast",
            "isIP",
        ],
    ),
    lib(
        "kubernetes.net.cidr",
        v(30),
        None,
        &[
            "cidr",
            "containsIP",
            "containsCIDR",
            "ip",
            "prefixLength",
            "masked",
            "isCIDR",
        ],
    ),
    lib("kubernetes.format", v(31), None, FORMAT_FUNCTIONS),
    // Authz selectors, gated by AuthorizeWithSelectors (base.go:131-141).
    VersionedOptions {
        library: "kubernetes.authzSelectors",
        introduced: v(31),
        removed: None,
        feature_enabled: Some(authorize_with_selectors),
        functions: &["fieldSelector", "labelSelector"],
        two_var_comprehensions: false,
    },
    // `ext.TwoVarComprehensions()` (ext/comprehensions.go).
    VersionedOptions {
        library: "cel.lib.ext.comprehensions",
        introduced: v(32),
        removed: None,
        feature_enabled: None,
        functions: &["transformList", "transformMap", "transformMapEntry"],
        two_var_comprehensions: true,
    },
    // `library.SemverLib(library.SemverVersion(1))` (semverlib.go:135-157).
    lib(
        "kubernetes.Semver",
        v(33),
        None,
        &[
            "semver",
            "isSemver",
            "isGreaterThan",
            "isLessThan",
            "compareTo",
            "major",
            "minor",
            "patch",
        ],
    ),
    // `ext.Lists(ext.ListsVersion(3))` (ext/lists.go:197-349).
    lib(
        "cel.lib.ext.lists",
        v(34),
        None,
        &[
            "slice",
            "flatten",
            "sort",
            "sortBy",
            "lists.range",
            "reverse",
            "distinct",
        ],
    ),
];

pub fn base_opts() -> &'static [VersionedOptions] {
    BASE_OPTS
}

/// `environment.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvType {
    NewExpressions,
    StoredExpressions,
}

/// `environment.EnvSet`: the libraries enabled for each environment type.
#[derive(Debug, Clone)]
pub struct EnvSet {
    compatibility_version: Version,
    new_expressions: Vec<&'static VersionedOptions>,
    stored_expressions: Vec<&'static VersionedOptions>,
}

/// `filterAndBuildOpts` (`environment.go:238-264`).
fn filter_opts(
    compat: Version,
    honor_feature_gate_enablement: bool,
) -> Vec<&'static VersionedOptions> {
    let mut out = Vec::new();
    for opt in base_opts() {
        let mut allowed_by_feature_gate = false;
        if let (Some(enabled), true) = (opt.feature_enabled, honor_feature_gate_enablement) {
            // Feature-gate-enabled libraries must follow compatible default
            // feature enablement (environment.go:245-247).
            allowed_by_feature_gate = enabled();
            if !allowed_by_feature_gate {
                continue;
            }
        }
        let allowed_by_version =
            compat >= opt.introduced && opt.removed.is_none_or(|removed| compat < removed);
        if allowed_by_feature_gate || allowed_by_version {
            out.push(opt);
        }
    }
    out
}

impl EnvSet {
    /// `MustBaseEnvSet` (`base.go:216-241`) via `newEnvSet` + `Extend`
    /// (`environment.go:79-93, 118-142`): `NewExpressions` is built for
    /// `version` honouring feature gates, `StoredExpressions` for
    /// `MaxUint.MaxUint` without.
    pub fn must_base_env_set(version: Version) -> Self {
        Self {
            compatibility_version: version,
            new_expressions: filter_opts(version, true),
            stored_expressions: filter_opts(Version::major_minor(u32::MAX, u32::MAX), false),
        }
    }

    pub fn compatibility_version(&self) -> Version {
        self.compatibility_version
    }

    fn libs(&self, env: EnvType) -> &[&'static VersionedOptions] {
        match env {
            EnvType::NewExpressions => &self.new_expressions,
            EnvType::StoredExpressions => &self.stored_expressions,
        }
    }

    /// Whether `function` resolves in the environment of type `env`. A name no
    /// library declares is not this set's concern and resolves (the checker's
    /// standard declarations and the request's variables decide it).
    pub fn declares(&self, env: EnvType, function: &str) -> bool {
        let known = base_opts().iter().any(|o| o.functions.contains(&function));
        !known
            || self
                .libs(env)
                .iter()
                .any(|o| o.functions.contains(&function))
    }

    /// Whether the two-variable comprehension macros are enabled.
    pub fn two_var_comprehensions(&self, env: EnvType) -> bool {
        self.libs(env).iter().any(|o| o.two_var_comprehensions)
    }
}

/// `DefaultCompatibilityVersion` (`base.go:52-58`):
/// `EffectiveVersion.MinCompatibilityVersion()`.
pub fn default_compatibility_version() -> Version {
    feature_gates::min_compatibility_version()
}

/// The checker's `undeclared reference` for the first call in `expression`
/// that resolves in no library of `env`, as the `Detail` of the compilation
/// error, or `None`. Syntax errors are not reported here.
pub fn compile_failure(expression: &str, set: &EnvSet, env: EnvType) -> Option<String> {
    let source = expression.to_string();
    let program = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cel::Program::compile(&source)
    }))
    .ok()?
    .ok()?;
    let name = first_undeclared(program.expression(), set, env)?;
    Some(format!(
        "compilation failed: ERROR: <input>: undeclared reference to '{name}' (in container '')"
    ))
}

fn first_undeclared(e: &IdedExpr, set: &EnvSet, env: EnvType) -> Option<String> {
    match &e.expr {
        Expr::Unspecified | Expr::Literal(_) | Expr::Ident(_) => None,
        Expr::Select(s) => first_undeclared(&s.operand, set, env),
        Expr::Call(c) => {
            if let Some(t) = &c.target {
                if let Some(n) = first_undeclared(t, set, env) {
                    return Some(n);
                }
            }
            if let Some(n) = c.args.iter().find_map(|a| first_undeclared(a, set, env)) {
                return Some(n);
            }
            // The `cel` crate's parser has no two-variable macros, so
            // `all`/`exists`/`existsOne` with an iteration and a value
            // variable (comprehensions.go:56-75, three arguments) stay plain
            // calls; they are the `TwoVarComprehensions` library's.
            if c.target.is_some()
                && c.args.len() == 3
                && matches!(
                    c.func_name.as_str(),
                    "all" | "exists" | "existsOne" | "exists_one"
                )
                && !set.two_var_comprehensions(env)
            {
                return Some(c.func_name.clone());
            }
            // `ns.name()` resolves as the qualified function when a library
            // declares it (the checker's qualified-name resolution).
            if let Some(Expr::Ident(ns)) = c.target.as_deref().map(|t| &t.expr) {
                let q = format!("{ns}.{}", c.func_name);
                if base_opts()
                    .iter()
                    .any(|o| o.functions.contains(&q.as_str()))
                {
                    return (!set.declares(env, &q)).then_some(q);
                }
            }
            (!set.declares(env, &c.func_name)).then(|| c.func_name.clone())
        }
        Expr::List(l) => l
            .elements
            .iter()
            .find_map(|a| first_undeclared(a, set, env)),
        Expr::Map(m) => m
            .entries
            .iter()
            .find_map(|en| entry_undeclared(&en.expr, set, env)),
        Expr::Struct(s) => s
            .entries
            .iter()
            .find_map(|en| entry_undeclared(&en.expr, set, env)),
        Expr::Comprehension(c) => {
            // The two-variable forms of `all`/`exists`/`existsOne` come from
            // the same library as `transformList` (comprehensions.go:36-41).
            if c.iter_var2.is_some() && !set.two_var_comprehensions(env) {
                return Some("all".to_string());
            }
            first_undeclared(&c.iter_range, set, env)
                .or_else(|| first_undeclared(&c.accu_init, set, env))
                .or_else(|| first_undeclared(&c.loop_cond, set, env))
                .or_else(|| first_undeclared(&c.loop_step, set, env))
                .or_else(|| first_undeclared(&c.result, set, env))
        }
    }
}

fn entry_undeclared(en: &EntryExpr, set: &EnvSet, env: EnvType) -> Option<String> {
    match en {
        EntryExpr::StructField(f) => first_undeclared(&f.value, set, env),
        EntryExpr::MapEntry(m) => {
            first_undeclared(&m.key, set, env).or_else(|| first_undeclared(&m.value, set, env))
        }
    }
}
