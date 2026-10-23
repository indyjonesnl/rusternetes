//! Deprecated-API registry: port of
//! `staging/src/k8s.io/apiserver/pkg/endpoints/deprecation/deprecation.go`
//! (`MajorMinor`, `IsDeprecated`, `RemovedRelease`, `WarningMessage`) over the
//! `APILifecycleDeprecated` / `APILifecycleRemoved` / `APILifecycleReplacement`
//! data upstream generates into `zz_generated.prerelease-lifecycle.go`
//! (mechanically extracted into [`table`] by `scripts/gen_deprecation_table.py`).
//!
//! Upstream decides per installed route (`endpoints/installer.go:781-797`):
//! the route's registered versioned kind is stamped with its GVK and, if
//! `deprecation.IsDeprecated(obj, currentMajor, currentMinor)`, the route is
//! `deprecated` with `removedRelease = deprecation.RemovedRelease(obj)`.
//! Here a request carries a resource rather than a route, so [`lookup`]
//! resolves `(group, version, resource, subresource)` to the registered kind
//! with `meta.UnsafeGuessKindToResource` (apimachinery
//! `pkg/api/meta/restmapper.go`), the rule the generated REST mapping uses.
//!
//! The endpoint-level `Warning:` header (`installer.go:814`
//! `utilwarning.AddWarningsHandler`) is in [`warnings`].

mod table;
pub mod warnings;

/// The kubernetes version this server reports (`versioninfo.Get()` is
/// `v1.35.0`, see `openapi.rs`), as `(major, minor)`.
pub const CURRENT_MAJOR_MINOR: (u32, u32) = (1, 35);

/// The lifecycle methods of one generated type
/// (`APILifecycleDeprecated`, `APILifecycleRemoved`,
/// `APILifecycleReplacement`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifecycle {
    pub group: &'static str,
    pub version: &'static str,
    pub kind: &'static str,
    pub deprecated: (u32, u32),
    pub removed: Option<(u32, u32)>,
    /// `(group, version, kind)`; all-empty is the zero GVK.
    pub replacement: Option<(&'static str, &'static str, &'static str)>,
}

/// `MajorMinor` (deprecation.go:46-56): "The minor version drops all
/// characters after the first non-digit character". `Atoi` on the major
/// accepts an optional sign and leading zeros; the minor goes through the
/// `^(\d+)` match first, so "A" yields "" which `Atoi` rejects.
#[allow(dead_code)] // bin target: only tests reach it until the version info is parsed
pub fn major_minor(major: &str, minor: &str) -> Result<(i64, i64), String> {
    let major = major
        .parse::<i64>()
        .map_err(|e| format!("strconv.Atoi: parsing {major:?}: {e}"))?;
    let digits: String = minor.chars().take_while(|c| c.is_ascii_digit()).collect();
    let minor = digits
        .parse::<i64>()
        .map_err(|e| format!("strconv.Atoi: parsing {digits:?}: {e}"))?;
    Ok((major, minor))
}

/// `IsDeprecated` (deprecation.go:59-78): `None` models "does not implement
/// APILifecycleDeprecated()".
pub fn is_deprecated(obj: Option<&Lifecycle>, current_major: i64, current_minor: i64) -> bool {
    let Some(obj) = obj else { return false };
    let (dep_major, dep_minor) = (obj.deprecated.0 as i64, obj.deprecated.1 as i64);
    // no deprecation version expressed
    if dep_major == 0 && dep_minor == 0 {
        return false;
    }
    // no current version info available
    if current_major == 0 && current_minor == 0 {
        return true;
    }
    // compare deprecation version to current version
    if dep_major > current_major {
        return false;
    }
    if dep_major == current_major && dep_minor > current_minor {
        return false;
    }
    true
}

/// `RemovedRelease` (deprecation.go:81-89): "<major>.<minor>" when the object
/// reports a non-zero removal version, else "".
pub fn removed_release(obj: Option<&Lifecycle>) -> String {
    match obj.and_then(|o| o.removed) {
        Some((major, minor)) if major != 0 || minor != 0 => format!("{major}.{minor}"),
        _ => String::new(),
    }
}

/// `WarningMessage` (deprecation.go:93-123).
#[allow(dead_code)] // bin target: awaits the per-request warning recorder
pub fn warning_message(obj: Option<&Lifecycle>) -> String {
    let Some(obj) = obj else { return String::new() };
    let (dep_major, dep_minor) = obj.deprecated;
    if dep_major == 0 && dep_minor == 0 {
        return String::new();
    }
    // `gvk.Empty()`
    if obj.group.is_empty() && obj.version.is_empty() && obj.kind.is_empty() {
        return String::new();
    }
    let mut warning = format!(
        "{} {} is deprecated in v{dep_major}.{dep_minor}+",
        group_version_string(obj.group, obj.version),
        obj.kind
    );
    if let Some((major, minor)) = obj.removed {
        if major != 0 || minor != 0 {
            warning.push_str(&format!(", unavailable in v{major}.{minor}+"));
        }
    }
    if let Some((g, v, k)) = obj.replacement {
        if !(g.is_empty() && v.is_empty() && k.is_empty()) {
            warning.push_str(&format!("; use {} {k}", group_version_string(g, v)));
        }
    }
    warning
}

/// `GroupVersion.String()`.
#[allow(dead_code)]
fn group_version_string(group: &str, version: &str) -> String {
    match (group.is_empty(), version.is_empty()) {
        (true, _) => version.to_string(),
        (false, true) => group.to_string(),
        _ => format!("{group}/{version}"),
    }
}

/// `meta.UnsafeGuessKindToResource` (apimachinery restmapper.go): lower-case
/// the kind and pluralize ("s" -> "ses", "y" -> "ies", else "s").
fn guess_kind_to_resource(kind: &str) -> String {
    let single = kind.to_lowercase();
    if single.ends_with("endpoints") {
        return single;
    }
    if single.ends_with('s') {
        format!("{single}es")
    } else if let Some(stem) = single.strip_suffix('y') {
        format!("{stem}ies")
    } else {
        format!("{single}s")
    }
}

/// The kind a subresource route serves when it is not its parent's kind
/// (`pkg/registry/*/storage`: `deployments/scale` is `Scale`,
/// `deployments/rollback` is `DeploymentRollback`, `pods/eviction` is
/// `Eviction`). Every other subresource (`status`, `approval`, ...) serves
/// the parent kind.
fn subresource_kind(subresource: &str) -> Option<&'static str> {
    match subresource {
        "scale" => Some("Scale"),
        "rollback" => Some("DeploymentRollback"),
        "eviction" => Some("Eviction"),
        _ => None,
    }
}

/// The lifecycle of the kind served at `group/version` `resource[/subresource]`,
/// or `None` when the route is not a generated deprecated type.
pub fn lookup(
    group: &str,
    version: &str,
    resource: &str,
    subresource: &str,
) -> Option<&'static Lifecycle> {
    let in_gv = |l: &&'static Lifecycle| l.group == group && l.version == version;
    if let Some(kind) = subresource_kind(subresource) {
        // The subresource kind is only deprecated along with a deprecated parent.
        table::TABLE
            .iter()
            .filter(in_gv)
            .any(|l| guess_kind_to_resource(l.kind) == resource)
            .then(|| table::TABLE.iter().filter(in_gv).find(|l| l.kind == kind))
            .flatten()
    } else {
        table::TABLE
            .iter()
            .filter(in_gv)
            .find(|l| !l.kind.ends_with("List") && guess_kind_to_resource(l.kind) == resource)
    }
}

/// `resourceExpirationEvaluator`
/// (`staging/src/k8s.io/apiserver/pkg/server/deleted_kinds.go:41-58`): decides
/// whether a kind whose `APILifecycleRemoved` is `(major, minor)` is still
/// served by this binary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceExpirationEvaluator {
    pub current_version: (u32, u32),
    pub is_alpha: bool,
    pub is_alpha_zero: bool,
    pub strict_removed_handling_in_alpha: bool,
    pub serve_removed_apis_one_more_release: bool,
}

impl ResourceExpirationEvaluator {
    /// `NewResourceExpirationEvaluatorFromOptions` (`deleted_kinds.go:91-123`)
    /// with the two env switches passed in (`None` = unset) so tests need not
    /// touch the process environment. `prerelease` is the version's
    /// pre-release part (e.g. `alpha.1.62+a5d22854a2ac21`; empty for GA).
    pub fn new(
        current_version: (u32, u32),
        prerelease: &str,
        strict_removed_handling_in_alpha: Option<&str>,
        serve_removed_apis_one_more_release: Option<&str>,
    ) -> Result<Self, String> {
        // Go `strconv.ParseBool` accepts exactly these spellings.
        fn parse_bool(v: &str) -> Result<bool, String> {
            match v {
                "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
                "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
                _ => Err(format!("strconv.ParseBool: parsing {v:?}: invalid syntax")),
            }
        }
        let mut ret = Self {
            current_version,
            is_alpha: prerelease.contains("alpha"),
            is_alpha_zero: prerelease.contains("alpha.0"),
            ..Default::default()
        };
        if let Some(v) = strict_removed_handling_in_alpha {
            ret.strict_removed_handling_in_alpha = parse_bool(v)?;
        }
        if let Some(v) = serve_removed_apis_one_more_release {
            ret.serve_removed_apis_one_more_release = parse_bool(v)?;
        }
        Ok(ret)
    }

    /// `NewResourceExpirationEvaluator` plus its env lookups
    /// (`KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA`,
    /// `KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE`).
    pub fn from_env(current_version: (u32, u32), prerelease: &str) -> Result<Self, String> {
        let strict = std::env::var("KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA").ok();
        let one_more = std::env::var("KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE").ok();
        Self::new(
            current_version,
            prerelease,
            strict.as_deref(),
            one_more.as_deref(),
        )
    }

    /// `ShouldServeForVersion` (`deleted_kinds.go:150-172`).
    pub fn should_serve_for_version(&self, removed: (u32, u32)) -> bool {
        // MajorMinor ordering is (major, minor) lexicographic.
        if removed > self.current_version {
            return true;
        }
        if removed < self.current_version {
            return false;
        }
        // Major and minor are equal: removed when the current release GAs.
        // Alphas don't remove by default (but strict handling removes from
        // alpha.1+), and an admin may serve removed APIs one more release.
        if self.is_alpha && !self.is_alpha_zero && self.strict_removed_handling_in_alpha {
            return false;
        }
        if self.is_alpha {
            return true;
        }
        self.serve_removed_apis_one_more_release
    }
}

/// The process-wide evaluator: this binary is a GA build of
/// [`CURRENT_MAJOR_MINOR`] (no pre-release), honouring the upstream env
/// switches. Upstream fails server construction on an unparsable env value;
/// here an invalid value is logged and ignored (no startup hook owns it yet).
fn evaluator() -> &'static ResourceExpirationEvaluator {
    static E: std::sync::OnceLock<ResourceExpirationEvaluator> = std::sync::OnceLock::new();
    E.get_or_init(|| {
        ResourceExpirationEvaluator::from_env(CURRENT_MAJOR_MINOR, "").unwrap_or_else(|e| {
            tracing::error!("invalid removed-API env switch, ignoring: {e}");
            ResourceExpirationEvaluator {
                current_version: CURRENT_MAJOR_MINOR,
                ..Default::default()
            }
        })
    })
}

/// `ShouldServeForVersion` for this binary (see [`evaluator`]): a kind whose
/// `APILifecycleRemoved` is at or before the current release is not served
/// (`removeDeletedKinds`, `:182-184`, drops its storage and so its route).
pub fn should_serve_for_version(removed: (u32, u32)) -> bool {
    evaluator().should_serve_for_version(removed)
}

/// `(deprecated, removedRelease)` for a request, as computed once per route in
/// `installer.go:781-797` against [`CURRENT_MAJOR_MINOR`]. A kind that is no
/// longer served has no installed route, hence no deprecation (#3755).
pub fn deprecation_for(
    group: &str,
    version: &str,
    resource: &str,
    subresource: &str,
) -> (bool, String) {
    let obj = lookup(group, version, resource, subresource);
    let (major, minor) = CURRENT_MAJOR_MINOR;
    // `isNotRemoved` (deleted_kinds.go:127-148): a type without
    // `APILifecycleRemoved` is always served.
    if let Some(removed) = obj.and_then(|o| o.removed) {
        if !should_serve_for_version(removed) {
            return (false, String::new());
        }
    }
    if is_deprecated(obj, major as i64, minor as i64) {
        (true, removed_release(obj))
    } else {
        (false, String::new())
    }
}

#[cfg(test)]
mod tests;
