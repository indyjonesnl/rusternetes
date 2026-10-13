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

use crate::feature_gates::{Feature, Version};

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

/// `baseOpts`.
pub fn base_opts() -> &'static [VersionedOptions] {
    &[]
}

/// `environment.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvType {
    NewExpressions,
    StoredExpressions,
}

/// `environment.EnvSet`.
#[derive(Debug, Clone)]
pub struct EnvSet {
    compatibility_version: Version,
}

impl EnvSet {
    /// `MustBaseEnvSet`.
    pub fn must_base_env_set(version: Version) -> Self {
        Self {
            compatibility_version: version,
        }
    }

    pub fn compatibility_version(&self) -> Version {
        self.compatibility_version
    }

    /// Whether `function` resolves in the environment of type `env`.
    pub fn declares(&self, _env: EnvType, _function: &str) -> bool {
        true
    }

    /// Whether the two-variable comprehension macros are enabled.
    pub fn two_var_comprehensions(&self, _env: EnvType) -> bool {
        true
    }
}

/// `DefaultCompatibilityVersion`.
pub fn default_compatibility_version() -> Version {
    crate::feature_gates::min_compatibility_version()
}

/// The checker's `undeclared reference` for the first call in `expression`
/// that resolves in no library of `env`, as the `Detail` of the compilation
/// error, or `None`. Syntax errors are not reported here.
pub fn compile_failure(_expression: &str, _set: &EnvSet, _env: EnvType) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feature_gates::{with_feature, Version};
    use serial_test::serial;

    fn v(minor: u32) -> Version {
        Version::major_minor(1, minor)
    }

    fn fails(expr: &str, ver: Version, env: EnvType) -> String {
        compile_failure(expr, &EnvSet::must_base_env_set(ver), env).unwrap_or_default()
    }

    /// `TestLibraryCoverage` (base_test.go:49-107): no gaps or overlap when a
    /// library is version-bumped, `Removed > Introduced`, nothing removed
    /// without a replacement.
    #[test]
    fn library_coverage() {
        let opts = base_opts();
        assert!(!opts.is_empty());
        let mut sorted: Vec<_> = opts.iter().collect();
        sorted.sort_by_key(|o| o.introduced);
        // library -> (added, removed)
        let mut tracked: std::collections::BTreeMap<&str, (Option<Version>, Option<Version>)> =
            Default::default();
        for o in sorted {
            if let Some(r) = o.removed {
                assert!(o.introduced < r, "{}: Introduced must be < Removed", o.library);
            }
            let t = tracked.entry(o.library).or_default();
            assert!(
                t.0.is_none(),
                "{} added again at {} while already added at {:?}",
                o.library,
                o.introduced,
                t.0
            );
            t.0 = Some(o.introduced);
            if let Some(removed) = t.1 {
                assert!(
                    removed == o.introduced,
                    "{}: gap/overlap, removed {removed} re-added {}",
                    o.library,
                    o.introduced
                );
                t.1 = None;
            }
            if let Some(r) = o.removed {
                t.0 = None;
                t.1 = Some(r);
            }
        }
        for (name, (_, removed)) in tracked {
            assert!(removed.is_none(), "{name} removed without replacement");
        }
    }

    /// `base.go:96-104`: `ext.Sets()` and the first `ext.Strings` version.
    #[test]
    fn a_library_is_declared_from_its_introduced_version() {
        let e = |ver| EnvSet::must_base_env_set(v(ver));
        // format library: IntroducedVersion 1.31 (base.go:123-129).
        assert!(!e(30).declares(EnvType::NewExpressions, "format.named"));
        assert!(e(31).declares(EnvType::NewExpressions, "format.named"));
        // semver: 1.33 (base.go:149-155).
        assert!(!e(32).declares(EnvType::NewExpressions, "semver"));
        assert!(e(33).declares(EnvType::NewExpressions, "semver"));
        // ext.Lists: 1.34 (base.go:156-162).
        assert!(!e(33).declares(EnvType::NewExpressions, "lists.range"));
        assert!(e(34).declares(EnvType::NewExpressions, "lists.range"));
        // ip/cidr: 1.30 (base.go:116-122).
        assert!(!e(29).declares(EnvType::NewExpressions, "isCIDR"));
        assert!(e(30).declares(EnvType::NewExpressions, "isCIDR"));
    }

    /// `Extend` passes `MaxUint` and ignores feature gates for
    /// `storedExpressions` (environment.go:222-228).
    #[test]
    fn stored_expressions_declare_every_library() {
        let e = EnvSet::must_base_env_set(v(31));
        for f in ["semver", "lists.range", "format.named", "isCIDR", "quantity"] {
            assert!(e.declares(EnvType::StoredExpressions, f), "{f}");
        }
        assert!(e.two_var_comprehensions(EnvType::StoredExpressions));
        assert!(!e.two_var_comprehensions(EnvType::NewExpressions));
    }

    /// Functions present from 1.0 are never gated, and a name shared by two
    /// libraries resolves while either is enabled (quantity 1.28 / semver 1.33
    /// both declare `isGreaterThan`, `compareTo`).
    #[test]
    fn always_present_and_shared_names() {
        let e = EnvSet::must_base_env_set(v(31));
        for f in ["isSorted", "sum", "indexOf", "find", "isURL", "charAt", "trim"] {
            assert!(e.declares(EnvType::NewExpressions, f), "{f}");
        }
        for f in ["isGreaterThan", "compareTo", "isLessThan"] {
            assert!(e.declares(EnvType::NewExpressions, f), "{f}");
        }
        assert!(!e.declares(EnvType::NewExpressions, "isSemver"));
    }

    /// `FeatureEnabled` is honoured for new expressions only
    /// (environment.go:243-251): the AuthzSelectors library at 1.31.
    #[test]
    #[serial]
    fn feature_gate_overrides_the_version_for_new_expressions() {
        let e = EnvSet::must_base_env_set(v(31));
        let _g = with_feature(Feature::AuthorizeWithSelectors, false);
        assert!(!e.declares(EnvType::NewExpressions, "fieldSelector"));
        assert!(e.declares(EnvType::StoredExpressions, "fieldSelector"));
        drop(_g);
        let _g = with_feature(Feature::AuthorizeWithSelectors, true);
        // Enabled by the gate even below the introduced version.
        let old = EnvSet::must_base_env_set(v(30));
        assert!(old.declares(EnvType::NewExpressions, "fieldSelector"));
        assert!(e.declares(EnvType::NewExpressions, "fieldSelector"));
    }

    #[test]
    fn a_call_to_an_undeclared_function_is_reported() {
        let m = fails("semver('1.2.3').major() > 0", v(32), EnvType::NewExpressions);
        assert_eq!(
            m,
            "compilation failed: ERROR: <input>: undeclared reference to 'semver' (in container '')"
        );
        assert_eq!(fails("semver('1.2.3').major() > 0", v(33), EnvType::NewExpressions), "");
        assert_eq!(fails("semver('1.2.3').major() > 0", v(32), EnvType::StoredExpressions), "");
        // Member calls and nested calls are checked.
        let m = fails("[1].all(x, x.isSemver())", v(32), EnvType::NewExpressions);
        assert!(m.contains("undeclared reference to 'isSemver'"), "{m}");
        // Namespaced functions are matched on `ns.name`.
        let m = fails("sets.contains([1], [1])", v(28), EnvType::NewExpressions);
        assert!(m.contains("undeclared reference to 'sets.contains'"), "{m}");
        assert_eq!(fails("sets.contains([1], [1])", v(31), EnvType::NewExpressions), "");
        // Unknown names are not this module's concern.
        assert_eq!(fails("size(self) > 0", v(31), EnvType::NewExpressions), "");
    }

    /// `ext.TwoVarComprehensions` at 1.32 (base.go:141-147).
    #[test]
    fn two_variable_comprehensions_arrive_at_1_32() {
        let expr = "self.transformList(i, x, x)";
        let m = fails(expr, v(31), EnvType::NewExpressions);
        assert!(m.contains("undeclared reference to 'transformList'"), "{m}");
        assert_eq!(fails(expr, v(32), EnvType::NewExpressions), "");
        let two = "self.all(i, x, x > 0)";
        assert!(!fails(two, v(31), EnvType::NewExpressions).is_empty());
        assert_eq!(fails(two, v(32), EnvType::NewExpressions), "");
    }

    #[test]
    #[serial]
    fn default_compatibility_version_is_the_min_compatibility_version() {
        assert_eq!(
            default_compatibility_version(),
            crate::feature_gates::min_compatibility_version()
        );
    }
}
