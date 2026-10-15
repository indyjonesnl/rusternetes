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
//! `utilwarning.AddWarningsHandler`) is not wired: [`warning_message`] is
//! ported and tested but the per-request warning recorder it feeds does not
//! exist yet.

mod table;

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

/// `(deprecated, removedRelease)` for a request, as computed once per route in
/// `installer.go:781-797` against [`CURRENT_MAJOR_MINOR`].
pub fn deprecation_for(
    group: &str,
    version: &str,
    resource: &str,
    subresource: &str,
) -> (bool, String) {
    let obj = lookup(group, version, resource, subresource);
    let (major, minor) = CURRENT_MAJOR_MINOR;
    if is_deprecated(obj, major as i64, minor as i64) {
        (true, removed_release(obj))
    } else {
        (false, String::new())
    }
}

#[cfg(test)]
mod tests;
