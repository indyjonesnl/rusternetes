//! Version-gated environments: `staging/src/k8s.io/apiserver/pkg/cel/environment`
//! (release-1.35).
//!
//! Upstream builds two cel-go environments per compatibility version
//! (`environment.go:79-93`, `Extend`): `NewExpressions`, holding only the
//! options introduced at or before the version and not yet removed, with
//! feature-gated ones honouring their gate (`filterAndBuildOpts`,
//! `environment.go:238-264`), and `StoredExpressions`, built for the newest
//! version ignoring feature gates, so that an expression stored by a newer
//! server still compiles. `base.go:63-169` (`baseOpts`) is the table of which
//! library arrives, and is replaced, at which Kubernetes version.
//!
//! This is the typed counterpart of the name-only model in
//! `k3rs_common::cel_versions`, which a later slice replaces with it.
//! Options that declare nothing the checker looks at (`EagerlyValidateDeclarations`,
//! `DefaultUTCTimeZone`, the cost options, the AST validators other than
//! homogeneous aggregate literals) are not modelled.

use crate::env::{Container, Env, Options, Provider};
use crate::table::{self, Table};
use std::sync::Arc;

/// `version.Version` reduced to `major.minor`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
}

impl Version {
    pub const fn major_minor(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// `version.MajorMinor(math.MaxUint, math.MaxUint)`, the version
    /// `StoredExpressions` is built for.
    pub const MAX: Version = Version::major_minor(u32::MAX, u32::MAX);
}

const fn v(minor: u32) -> Version {
    Version::major_minor(1, minor)
}

/// A library's declarations: what `cel.Lib(...)` adds to the environment.
type Library = fn(&mut Table);

/// `environment.VersionedOptions`, reduced to what the checker needs.
pub struct VersionedOptions {
    /// `IntroducedVersion`.
    pub introduced: Version,
    /// `RemovedVersion`.
    pub removed: Option<Version>,
    /// `FeatureEnabled`, resolved against [`Features`].
    pub feature_enabled: Option<fn(&Features) -> bool>,
    /// `cel.HomogeneousAggregateLiterals()`.
    pub homogeneous_aggregate_literals: bool,
    /// `ext.Strings(StringsVersion(1+))` adds the `format` validator, which
    /// exempts the literals under `format` from the homogeneity check.
    pub string_format_validator: bool,
    /// `cel.CrossTypeNumericComparisons(true)`.
    pub cross_type_numeric_comparisons: bool,
    /// `cel.OptionalTypes()`: the optional library and `optional_type`.
    pub optional_types: bool,
    /// `ext.TwoVarComprehensions()` (macros only, declares no function).
    pub two_var_comprehensions: bool,
    /// `ext.Lists(..)`'s `sortBy` macro.
    pub sort_by_macro: bool,
    /// The function libraries the options install.
    pub libraries: &'static [Library],
}

/// The feature gates `baseOpts` reads (`base.go:131-141`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Features {
    /// `AuthorizeWithSelectors`.
    pub authorize_with_selectors: bool,
}

const NONE: VersionedOptions = VersionedOptions {
    introduced: v(0),
    removed: None,
    feature_enabled: None,
    homogeneous_aggregate_literals: false,
    string_format_validator: false,
    cross_type_numeric_comparisons: false,
    optional_types: false,
    two_var_comprehensions: false,
    sort_by_macro: false,
    libraries: &[],
};

/// `baseOpts` (`base.go:63-169`). The libraries are `library.URLs`,
/// `Regex`, `Lists`, `Authz`, `Quantity`, `IP`, `CIDR`, `Format`,
/// `AuthzSelectors`, `SemverLib` and cel-go `ext.Strings`, `Sets`, `Lists`.
pub static BASE_OPTS: &[VersionedOptions] = &[
    VersionedOptions {
        // CEL epoch was actually 1.23, but is set to 1.0 so these options are
        // always present.
        homogeneous_aggregate_literals: true,
        libraries: &[table::urls, table::regex, table::k8s_lists],
        ..NONE
    },
    VersionedOptions {
        introduced: v(27),
        libraries: &[table::authz],
        ..NONE
    },
    VersionedOptions {
        introduced: v(28),
        cross_type_numeric_comparisons: true,
        optional_types: true,
        libraries: &[table::quantity],
        ..NONE
    },
    // `ext.Strings(ext.StringsVersion(0))` until 1.29, then version 2.
    VersionedOptions {
        removed: Some(v(29)),
        libraries: &[table::strings_v0],
        ..NONE
    },
    VersionedOptions {
        introduced: v(29),
        string_format_validator: true,
        libraries: &[table::strings_v0, table::strings_v2_additions],
        ..NONE
    },
    VersionedOptions {
        introduced: v(29),
        libraries: &[table::sets],
        ..NONE
    },
    VersionedOptions {
        introduced: v(30),
        libraries: &[table::ip, table::cidr],
        ..NONE
    },
    VersionedOptions {
        introduced: v(31),
        libraries: &[table::format],
        ..NONE
    },
    VersionedOptions {
        introduced: v(31),
        feature_enabled: Some(|f| f.authorize_with_selectors),
        libraries: &[table::authz_selectors],
        ..NONE
    },
    VersionedOptions {
        introduced: v(32),
        two_var_comprehensions: true,
        libraries: &[table::two_var_comprehensions],
        ..NONE
    },
    VersionedOptions {
        introduced: v(33),
        libraries: &[table::semver],
        ..NONE
    },
    VersionedOptions {
        introduced: v(34),
        sort_by_macro: true,
        libraries: &[table::ext_lists],
        ..NONE
    },
];

/// `environment.Type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvType {
    NewExpressions,
    StoredExpressions,
}

/// `environment.EnvSet`: the options enabled for each environment type.
pub struct EnvSet {
    compatibility_version: Version,
    new_expressions: Vec<&'static VersionedOptions>,
    stored_expressions: Vec<&'static VersionedOptions>,
}

/// `filterAndBuildOpts` (`environment.go:238-264`). A feature-gated option
/// whose gate is on is allowed whatever its version (`allowedByFeatureGate`).
fn filter_opts(
    compat: Version,
    honor_feature_gates: bool,
    features: &Features,
) -> Vec<&'static VersionedOptions> {
    let mut out = Vec::new();
    for opt in BASE_OPTS {
        let mut allowed_by_gate = false;
        if let (Some(enabled), true) = (opt.feature_enabled, honor_feature_gates) {
            allowed_by_gate = enabled(features);
            if !allowed_by_gate {
                continue;
            }
        }
        let allowed_by_version =
            compat >= opt.introduced && opt.removed.is_none_or(|removed| compat < removed);
        if allowed_by_gate || allowed_by_version {
            out.push(opt);
        }
    }
    out
}

impl EnvSet {
    /// `MustBaseEnvSet` (`base.go:216-241`): `NewExpressions` for `version`
    /// honouring `features`, `StoredExpressions` for the newest version
    /// without feature gates.
    pub fn must_base_env_set(version: Version, features: &Features) -> Self {
        Self {
            compatibility_version: version,
            new_expressions: filter_opts(version, true, features),
            stored_expressions: filter_opts(Version::MAX, false, features),
        }
    }

    pub fn compatibility_version(&self) -> Version {
        self.compatibility_version
    }

    fn opts(&self, t: EnvType) -> &[&'static VersionedOptions] {
        match t {
            EnvType::NewExpressions => &self.new_expressions,
            EnvType::StoredExpressions => &self.stored_expressions,
        }
    }

    /// `EnvSet.Env`: a checker environment with the standard library and the
    /// libraries enabled for `t`. The environment is mutable (lookups import
    /// struct types, comprehensions open scopes), so each check builds its own.
    pub fn env(&self, t: EnvType, container: Container, provider: Arc<dyn Provider>) -> Env {
        let opts = self.opts(t);
        let mut env = Env::new(
            container,
            provider,
            Options {
                homogeneous_aggregate_literals: false,
                validate_homogeneous_literals: opts
                    .iter()
                    .any(|o| o.homogeneous_aggregate_literals),
                homogeneous_exempt_functions: if opts.iter().any(|o| o.string_format_validator) {
                    vec!["format".to_string()]
                } else {
                    vec![]
                },
                cross_type_numeric_comparisons: opts
                    .iter()
                    .any(|o| o.cross_type_numeric_comparisons),
            },
        );
        env.add_std_types();
        env.add_library(table::stdlib);
        for o in opts {
            if o.optional_types {
                env.add_optional_type_ident();
                env.add_library(table::optional);
            }
            for lib in o.libraries {
                env.add_library(*lib);
            }
        }
        env
    }

    /// Whether the two-variable comprehension macros are enabled
    /// (`ext.TwoVarComprehensions`).
    pub fn two_var_comprehensions(&self, t: EnvType) -> bool {
        self.opts(t).iter().any(|o| o.two_var_comprehensions)
    }

    /// Whether the `sortBy` macro is enabled (`ext.Lists`).
    pub fn sort_by_macro(&self, t: EnvType) -> bool {
        self.opts(t).iter().any(|o| o.sort_by_macro)
    }
}
