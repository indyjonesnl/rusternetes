//! `meta.RESTMapper` resource/kind lookup over the served discovery documents.
//!
//! Ported from Kubernetes release-1.35:
//! - `staging/src/k8s.io/apimachinery/pkg/api/meta/restmapper.go`:
//!   `DefaultRESTMapper` (`AddSpecific` :104-113, `UnsafeGuessKindToResource`
//!   :126-148, `ResourceSingularizer` :152-180, `ResourcesFor` :192-277,
//!   `ResourceFor` :279-289, `KindsFor` :291-356, `KindFor` :358-368, and the
//!   two `*ByPreferredGroupVersion` sorts :370-444).
//! - `.../meta/multirestmapper.go`: `MultiRESTMapper` (`ResourceSingularizer`
//!   :47-55, `ResourcesFor` :57-90, `KindsFor` :92-125, `ResourceFor`
//!   :127-137, `KindFor` :139-149).
//! - `.../meta/priority.go`: `PriorityRESTMapper` (`ResourceFor` :60-93,
//!   `KindFor` :96-129, `resourceMatches` / `kindMatches` :131-157).
//! - `.../meta/errors.go`: `NoResourceMatchError`, `AmbiguousResourceError`.
//! - `staging/src/k8s.io/client-go/restmapper/discovery.go`:
//!   `NewDiscoveryRESTMapper` :43-137.
//!
//! The `RESTMapping` / `RESTMappings` halves of upstream's interface are not
//! ported: no caller of this module needs them (`DiscoveryRestMapper` in the
//! api-server admission code covers kind -> resource separately).
//!
//! Rust-only: the input is built from the discovery documents this crate
//! serves ([`served_group_resources`]) instead of a discovery client, and the
//! per-mapper maps are ordered (`BTreeMap`) where Go iterates a map in random
//! order (every result is sorted afterwards, so the outcome is unchanged).

use std::collections::BTreeMap;
use std::fmt;

/// `meta.AnyGroup` / `meta.AnyVersion` / `meta.AnyResource` / `meta.AnyKind`
/// (interfaces.go, priority.go:28-30).
pub const ANY: &str = "*";

/// `schema.GroupVersionResource`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Gvr {
    pub group: String,
    pub version: String,
    pub resource: String,
}

impl Gvr {
    pub fn new(group: &str, version: &str, resource: &str) -> Self {
        Self {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
        }
    }

    fn group_version(&self) -> (&str, &str) {
        (&self.group, &self.version)
    }
}

/// `GroupVersionResource.String()` (group_version.go:114-116).
impl fmt::Display for Gvr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}, Resource={}",
            self.group, self.version, self.resource
        )
    }
}

/// `schema.GroupVersionKind`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Gvk {
    pub group: String,
    pub version: String,
    pub kind: String,
}

impl Gvk {
    pub fn new(group: &str, version: &str, kind: &str) -> Self {
        Self {
            group: group.to_string(),
            version: version.to_string(),
            kind: kind.to_string(),
        }
    }

    fn group_version(&self) -> (&str, &str) {
        (&self.group, &self.version)
    }
}

/// `GroupVersionKind.String()` (group_version.go).
impl fmt::Display for Gvk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}, Kind={}", self.group, self.version, self.kind)
    }
}

/// The error half of `RESTMapper.ResourcesFor` / `KindsFor` (errors.go).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapperError {
    /// `NoResourceMatchError`.
    NoResourceMatch { partial: Gvr },
    /// `AmbiguousResourceError`.
    AmbiguousResource {
        partial: Gvr,
        resources: Vec<Gvr>,
        kinds: Vec<Gvk>,
    },
    /// A plain `fmt.Errorf`.
    Other(String),
}

fn join<T: fmt::Display>(items: &[T]) -> String {
    format!(
        "[{}]",
        items
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ")
    )
}

impl fmt::Display for MapperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // errors.go:89-91
            MapperError::NoResourceMatch { partial } => write!(f, "no matches for {partial}"),
            // errors.go:35-45
            MapperError::AmbiguousResource {
                partial,
                resources,
                kinds,
            } => match (kinds.is_empty(), resources.is_empty()) {
                (false, false) => write!(
                    f,
                    "{partial} matches multiple resources {} and kinds {}",
                    join(resources),
                    join(kinds)
                ),
                (false, true) => write!(f, "{partial} matches multiple kinds {}", join(kinds)),
                (true, false) => {
                    write!(
                        f,
                        "{partial} matches multiple resources {}",
                        join(resources)
                    )
                }
                (true, true) => write!(f, "{partial} matches multiple resources or kinds"),
            },
            MapperError::Other(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for MapperError {}

impl MapperError {
    /// `meta.IsNoMatchError`, restricted to the errors this port produces.
    pub fn is_no_match(&self) -> bool {
        matches!(self, MapperError::NoResourceMatch { .. })
    }
}

/// `UnsafeGuessKindToResource` (restmapper.go:126-148): `(plural, singular)`.
pub fn unsafe_guess_kind_to_resource(kind: &Gvk) -> (Gvr, Gvr) {
    let kind_name = &kind.kind;
    if kind_name.is_empty() {
        return (Gvr::default(), Gvr::default());
    }
    let singular_name = kind_name.to_lowercase();
    let singular = Gvr::new(&kind.group, &kind.version, &singular_name);

    // `unpluralizedSuffixes` (restmapper.go:119-121).
    if singular_name.ends_with("endpoints") {
        return (singular.clone(), singular);
    }
    let plural_name = if singular_name.ends_with('s') {
        format!("{singular_name}es")
    } else if let Some(stem) = singular_name.strip_suffix('y') {
        format!("{stem}ies")
    } else {
        format!("{singular_name}s")
    };
    (Gvr::new(&kind.group, &kind.version, &plural_name), singular)
}

/// `coerceResourceForMatching` (restmapper.go:183-190); the internal-version
/// rewrite has no Rust counterpart.
fn coerce_resource_for_matching(resource: &Gvr) -> Gvr {
    let mut r = resource.clone();
    r.resource = r.resource.to_lowercase();
    r
}

/// Go's `sort.Sort` runs `insertionSort` below 12 elements; the `Less`
/// functions below are not a strict weak order when both group-versions are
/// unlisted, so a comparator-based sort could panic. Lists are tiny.
fn insertion_sort<T>(list: &mut [T], less: impl Fn(&T, &T) -> bool) {
    for i in 1..list.len() {
        let mut j = i;
        while j > 0 && less(&list[j], &list[j - 1]) {
            list.swap(j, j - 1);
            j -= 1;
        }
    }
}

/// `resourceByPreferredGroupVersion.Less` / `kindByPreferredGroupVersion.Less`
/// (restmapper.go:377-406, 415-444). `lhs_name`/`rhs_name` are the Resource or
/// Kind.
fn preferred_less(
    lhs_gv: (&str, &str),
    lhs_name: &str,
    rhs_gv: (&str, &str),
    rhs_name: &str,
    order: &[(String, String)],
) -> bool {
    if lhs_gv == rhs_gv {
        return lhs_name < rhs_name;
    }
    let index = |gv: (&str, &str)| {
        order
            .iter()
            .rposition(|(g, v)| g == gv.0 && v == gv.1)
            .map(|i| i as isize)
            .unwrap_or(-1)
    };
    let rhs_index = index(rhs_gv);
    if rhs_index == -1 {
        return true;
    }
    index(lhs_gv) < rhs_index
}

/// `meta.DefaultRESTMapper` (without the kind -> scope / RESTMapping side).
#[derive(Debug, Clone)]
pub struct DefaultRestMapper {
    default_group_versions: Vec<(String, String)>,
    resource_to_kind: BTreeMap<Gvr, Gvk>,
    plural_to_singular: BTreeMap<Gvr, Gvr>,
}

impl DefaultRestMapper {
    /// `NewDefaultRESTMapper` (restmapper.go:81-97).
    pub fn new(default_group_versions: &[(&str, &str)]) -> Self {
        Self {
            default_group_versions: default_group_versions
                .iter()
                .map(|(g, v)| (g.to_string(), v.to_string()))
                .collect(),
            resource_to_kind: BTreeMap::new(),
            plural_to_singular: BTreeMap::new(),
        }
    }

    /// `Add` (restmapper.go:99-102).
    pub fn add(&mut self, kind: Gvk) {
        let (plural, singular) = unsafe_guess_kind_to_resource(&kind);
        self.add_specific(kind, plural, singular);
    }

    /// `AddSpecific` (restmapper.go:104-113).
    pub fn add_specific(&mut self, kind: Gvk, plural: Gvr, singular: Gvr) {
        self.plural_to_singular
            .insert(plural.clone(), singular.clone());
        self.resource_to_kind.insert(singular, kind.clone());
        self.resource_to_kind.insert(plural, kind);
    }

    /// `ResourceSingularizer` (restmapper.go:152-180).
    pub fn resource_singularizer(&self, resource_type: &str) -> Result<String, MapperError> {
        let partial = Gvr::new("", "", resource_type);
        let resources = self.resources_for(&partial)?;

        let mut singular = Gvr::default();
        for curr in &resources {
            let Some(curr_singular) = self.plural_to_singular.get(curr) else {
                continue;
            };
            if singular == Gvr::default() {
                singular = curr_singular.clone();
                continue;
            }
            if curr_singular.resource != singular.resource {
                return Err(MapperError::Other(format!(
                    "multiple possible singular resources ({}) found for {resource_type}",
                    join(&resources)
                )));
            }
        }
        if singular == Gvr::default() {
            return Err(MapperError::Other(format!(
                "no singular of resource {resource_type} has been defined"
            )));
        }
        Ok(singular.resource)
    }

    /// `ResourcesFor` (restmapper.go:192-277).
    pub fn resources_for(&self, input: &Gvr) -> Result<Vec<Gvr>, MapperError> {
        let resource = coerce_resource_for_matching(input);
        let has_resource = !resource.resource.is_empty();
        let has_group = !resource.group.is_empty();
        let has_version = !resource.version.is_empty();
        if !has_resource {
            return Err(MapperError::Other(format!(
                "a resource must be present, got: {resource}"
            )));
        }

        let mut ret: Vec<Gvr> = Vec::new();
        if has_group && has_version {
            // fully qualified.  Find the exact match
            for (plural, singular) in &self.plural_to_singular {
                if *singular == resource || *plural == resource {
                    ret.push(plural.clone());
                    break;
                }
            }
        } else if has_group {
            // given a group, prefer an exact match.  If you don't find one,
            // resort to a prefix match on group
            let mut found_exact = false;
            for (plural, singular) in &self.plural_to_singular {
                if singular.group == resource.group && singular.resource == resource.resource {
                    found_exact = true;
                    ret.push(plural.clone());
                }
                if plural.group == resource.group && plural.resource == resource.resource {
                    found_exact = true;
                    ret.push(plural.clone());
                }
            }
            // storageclass.storage matches storageclass.storage.k8s.io
            if !found_exact {
                for (plural, singular) in &self.plural_to_singular {
                    if !plural.group.starts_with(&resource.group) {
                        continue;
                    }
                    if singular.resource == resource.resource {
                        ret.push(plural.clone());
                    }
                    if plural.resource == resource.resource {
                        ret.push(plural.clone());
                    }
                }
            }
        } else if has_version {
            for (plural, singular) in &self.plural_to_singular {
                if singular.version == resource.version && singular.resource == resource.resource {
                    ret.push(plural.clone());
                }
                if plural.version == resource.version && plural.resource == resource.resource {
                    ret.push(plural.clone());
                }
            }
        } else {
            for (plural, singular) in &self.plural_to_singular {
                if singular.resource == resource.resource {
                    ret.push(plural.clone());
                }
                if plural.resource == resource.resource {
                    ret.push(plural.clone());
                }
            }
        }

        if ret.is_empty() {
            return Err(MapperError::NoResourceMatch { partial: resource });
        }
        let order = &self.default_group_versions;
        insertion_sort(&mut ret, |l, r| {
            l != r
                && preferred_less(
                    l.group_version(),
                    &l.resource,
                    r.group_version(),
                    &r.resource,
                    order,
                )
        });
        Ok(ret)
    }

    /// `ResourceFor` (restmapper.go:279-289).
    pub fn resource_for(&self, resource: &Gvr) -> Result<Gvr, MapperError> {
        let mut resources = self.resources_for(resource)?;
        if resources.len() == 1 {
            return Ok(resources.remove(0));
        }
        Err(MapperError::AmbiguousResource {
            partial: resource.clone(),
            resources,
            kinds: Vec::new(),
        })
    }

    /// `KindsFor` (restmapper.go:291-356).
    pub fn kinds_for(&self, input: &Gvr) -> Result<Vec<Gvk>, MapperError> {
        let resource = coerce_resource_for_matching(input);
        let has_resource = !resource.resource.is_empty();
        let has_group = !resource.group.is_empty();
        let has_version = !resource.version.is_empty();
        if !has_resource {
            return Err(MapperError::Other(format!(
                "a resource must be present, got: {resource}"
            )));
        }

        let mut ret: Vec<Gvk> = Vec::new();
        if has_group && has_version {
            if let Some(kind) = self.resource_to_kind.get(&resource) {
                ret.push(kind.clone());
            }
        } else if has_group {
            let mut found_exact = false;
            for (curr, kind) in &self.resource_to_kind {
                if curr.group == resource.group && curr.resource == resource.resource {
                    found_exact = true;
                    ret.push(kind.clone());
                }
            }
            if !found_exact {
                for (curr, kind) in &self.resource_to_kind {
                    if !curr.group.starts_with(&resource.group) {
                        continue;
                    }
                    if curr.resource == resource.resource {
                        ret.push(kind.clone());
                    }
                }
            }
        } else if has_version {
            for (curr, kind) in &self.resource_to_kind {
                if curr.version == resource.version && curr.resource == resource.resource {
                    ret.push(kind.clone());
                }
            }
        } else {
            for (curr, kind) in &self.resource_to_kind {
                if curr.resource == resource.resource {
                    ret.push(kind.clone());
                }
            }
        }

        if ret.is_empty() {
            // restmapper.go:351: the error carries the un-coerced input.
            return Err(MapperError::NoResourceMatch {
                partial: input.clone(),
            });
        }
        let order = &self.default_group_versions;
        insertion_sort(&mut ret, |l, r| {
            l != r
                && preferred_less(
                    l.group_version(),
                    &l.kind,
                    r.group_version(),
                    &r.kind,
                    order,
                )
        });
        Ok(ret)
    }

    /// `KindFor` (restmapper.go:358-368).
    pub fn kind_for(&self, resource: &Gvr) -> Result<Gvk, MapperError> {
        let mut kinds = self.kinds_for(resource)?;
        if kinds.len() == 1 {
            return Ok(kinds.remove(0));
        }
        Err(MapperError::AmbiguousResource {
            partial: resource.clone(),
            resources: Vec::new(),
            kinds,
        })
    }
}

/// `meta.MultiRESTMapper` over `DefaultRESTMapper`s.
#[derive(Debug, Clone, Default)]
pub struct MultiRestMapper(pub Vec<DefaultRestMapper>);

impl MultiRestMapper {
    /// multirestmapper.go:47-55: the first mapper that answers; the last
    /// attempt's result when none does.
    pub fn resource_singularizer(&self, resource: &str) -> Result<String, MapperError> {
        let mut last = Ok(String::new());
        for t in &self.0 {
            last = t.resource_singularizer(resource);
            if last.is_ok() {
                return last;
            }
        }
        last
    }

    /// multirestmapper.go:57-90.
    pub fn resources_for(&self, resource: &Gvr) -> Result<Vec<Gvr>, MapperError> {
        let mut all: Vec<Gvr> = Vec::new();
        for t in &self.0 {
            match t.resources_for(resource) {
                // ignore "no match" errors, but any other error percolates up
                Err(e) if e.is_no_match() => continue,
                Err(e) => return Err(e),
                Ok(gvrs) => {
                    for curr in gvrs {
                        if !all.contains(&curr) {
                            all.push(curr);
                        }
                    }
                }
            }
        }
        if all.is_empty() {
            return Err(MapperError::NoResourceMatch {
                partial: resource.clone(),
            });
        }
        Ok(all)
    }

    /// multirestmapper.go:92-125.
    pub fn kinds_for(&self, resource: &Gvr) -> Result<Vec<Gvk>, MapperError> {
        let mut all: Vec<Gvk> = Vec::new();
        for t in &self.0 {
            match t.kinds_for(resource) {
                Err(e) if e.is_no_match() => continue,
                Err(e) => return Err(e),
                Ok(gvks) => {
                    for curr in gvks {
                        if !all.contains(&curr) {
                            all.push(curr);
                        }
                    }
                }
            }
        }
        if all.is_empty() {
            return Err(MapperError::NoResourceMatch {
                partial: resource.clone(),
            });
        }
        Ok(all)
    }
}

fn resource_matches(pattern: &Gvr, resource: &Gvr) -> bool {
    (pattern.group == ANY || pattern.group == resource.group)
        && (pattern.version == ANY || pattern.version == resource.version)
        && (pattern.resource == ANY || pattern.resource == resource.resource)
}

fn kind_matches(pattern: &Gvk, kind: &Gvk) -> bool {
    (pattern.group == ANY || pattern.group == kind.group)
        && (pattern.version == ANY || pattern.version == kind.version)
        && (pattern.kind == ANY || pattern.kind == kind.kind)
}

/// `meta.PriorityRESTMapper`: picks one Resource or Kind when several match.
///
/// Upstream's `ResourceFor`/`KindFor` also forward an error that accompanied a
/// non-empty result (priority.go:61-67); `MultiRestMapper` never returns both,
/// so that arm has no counterpart here.
#[derive(Debug, Clone, Default)]
pub struct PriorityRestMapper {
    pub delegate: MultiRestMapper,
    pub resource_priority: Vec<Gvr>,
    pub kind_priority: Vec<Gvk>,
}

impl PriorityRestMapper {
    /// priority.go:60-93.
    pub fn resource_for(&self, partial: &Gvr) -> Result<Gvr, MapperError> {
        let original = self.delegate.resources_for(partial)?;
        if original.len() == 1 {
            return Ok(original[0].clone());
        }
        let mut remaining = original.clone();
        for pattern in &self.resource_priority {
            let matched: Vec<Gvr> = remaining
                .iter()
                .filter(|g| resource_matches(pattern, g))
                .cloned()
                .collect();
            match matched.len() {
                // nothing matched this pattern, move to the next
                0 => continue,
                1 => return Ok(matched[0].clone()),
                // use the matched hits as the list for the next pattern
                _ => remaining = matched,
            }
        }
        Err(MapperError::AmbiguousResource {
            partial: partial.clone(),
            resources: original,
            kinds: Vec::new(),
        })
    }

    /// priority.go:96-129.
    pub fn kind_for(&self, partial: &Gvr) -> Result<Gvk, MapperError> {
        let original = self.delegate.kinds_for(partial)?;
        if original.len() == 1 {
            return Ok(original[0].clone());
        }
        let mut remaining = original.clone();
        for pattern in &self.kind_priority {
            let matched: Vec<Gvk> = remaining
                .iter()
                .filter(|g| kind_matches(pattern, g))
                .cloned()
                .collect();
            match matched.len() {
                0 => continue,
                1 => return Ok(matched[0].clone()),
                _ => remaining = matched,
            }
        }
        Err(MapperError::AmbiguousResource {
            partial: partial.clone(),
            resources: Vec::new(),
            kinds: original,
        })
    }

    pub fn resource_singularizer(&self, resource: &str) -> Result<String, MapperError> {
        self.delegate.resource_singularizer(resource)
    }

    pub fn resources_for(&self, partial: &Gvr) -> Result<Vec<Gvr>, MapperError> {
        self.delegate.resources_for(partial)
    }

    pub fn kinds_for(&self, partial: &Gvr) -> Result<Vec<Gvk>, MapperError> {
        self.delegate.kinds_for(partial)
    }
}

/// `metav1.APIResource`, the fields `NewDiscoveryRESTMapper` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiResource {
    pub name: String,
    pub singular_name: String,
    pub namespaced: bool,
    pub kind: String,
}

/// `restmapper.APIGroupResources` (discovery.go:34-39): the group's name, its
/// versions in discovery order, the preferred version and the resources per
/// version.
#[derive(Debug, Clone, Default)]
pub struct ApiGroupResources {
    pub name: String,
    pub versions: Vec<String>,
    pub preferred_version: String,
    pub versioned_resources: BTreeMap<String, Vec<ApiResource>>,
}

/// `restmapper.NewDiscoveryRESTMapper` (discovery.go:43-137).
pub fn new_discovery_rest_mapper(group_resources: &[ApiGroupResources]) -> PriorityRestMapper {
    let mut union = MultiRestMapper::default();

    let mut group_priority: Vec<&str> = Vec::new();
    // /v1 is special.  It should always come first
    let mut resource_priority = vec![Gvr::new("", "v1", ANY)];
    let mut kind_priority = vec![Gvk::new("", "v1", ANY)];

    for group in group_resources {
        group_priority.push(&group.name);

        // Make sure the preferred version comes first
        if !group.preferred_version.is_empty()
            && group
                .versioned_resources
                .contains_key(&group.preferred_version)
        {
            resource_priority.push(Gvr::new(&group.name, &group.preferred_version, ANY));
            kind_priority.push(Gvk::new(&group.name, &group.preferred_version, ANY));
        }

        for version in &group.versions {
            let Some(resources) = group.versioned_resources.get(version) else {
                continue;
            };

            // Add non-preferred versions after the preferred version, in case
            // there are resources that only exist in those versions
            if *version != group.preferred_version {
                resource_priority.push(Gvr::new(&group.name, version, ANY));
                kind_priority.push(Gvk::new(&group.name, version, ANY));
            }

            let mut version_mapper = DefaultRestMapper::new(&[(&group.name, version)]);
            for resource in resources {
                // a slash is a subresource; no mappings for those
                if resource.name.contains('/') {
                    continue;
                }
                let plural = Gvr::new(&group.name, version, &resource.name);
                let mut singular = Gvr::new(&group.name, version, &resource.singular_name);
                // legacy resources and servers which don't list singular forms
                if resource.singular_name.is_empty() {
                    singular = unsafe_guess_kind_to_resource(&Gvk::new(
                        &group.name,
                        version,
                        &resource.kind,
                    ))
                    .1;
                }
                version_mapper.add_specific(
                    Gvk::new(&group.name, version, &resource.kind.to_lowercase()),
                    plural.clone(),
                    singular.clone(),
                );
                version_mapper.add_specific(
                    Gvk::new(&group.name, version, &resource.kind),
                    plural,
                    singular,
                );
                // upstream TODO: unsafe guesses that match previous behavior
                version_mapper.add(Gvk::new(
                    &group.name,
                    version,
                    &format!("{}List", resource.kind),
                ));
            }
            // TODO why is this type not in discovery (at least for "v1")
            version_mapper.add(Gvk::new(&group.name, version, "List"));
            union.0.push(version_mapper);
        }
    }

    for group in group_priority {
        resource_priority.push(Gvr::new(group, ANY, ANY));
        kind_priority.push(Gvk::new(group, ANY, ANY));
    }

    PriorityRestMapper {
        delegate: union,
        resource_priority,
        kind_priority,
    }
}

fn resources_of(group: &str, version: &str) -> Vec<ApiResource> {
    let text = |v: &serde_json::Value, k: &str| v[k].as_str().unwrap_or("").to_string();
    super::get_aggregated_resources_for_group_uncategorized(group, version)
        .iter()
        .map(|r| ApiResource {
            name: text(r, "resource"),
            singular_name: text(r, "singularResource"),
            namespaced: r["scope"].as_str() == Some("Namespaced"),
            kind: text(&r["responseKind"], "kind"),
        })
        .collect()
}

/// What `restmapper.GetAPIGroupResources` gathers from `ServerGroupsAndResources`
/// (discovery.go:140-): the groups this server serves, core first, then the
/// built-in groups in the order `/apis` lists them, then the groups defined by
/// the established `crds` (CustomResourceDefinition objects).
pub fn served_group_resources(crds: &[serde_json::Value]) -> Vec<ApiGroupResources> {
    let mut out = Vec::new();
    let group_of = |name: &str, versions: &[&str]| {
        let mut g = ApiGroupResources {
            name: name.to_string(),
            versions: versions.iter().map(|v| v.to_string()).collect(),
            preferred_version: versions[0].to_string(),
            versioned_resources: BTreeMap::new(),
        };
        for v in versions {
            g.versioned_resources
                .insert(v.to_string(), resources_of(name, v));
        }
        g
    };

    out.push(group_of("", &["v1"]));
    for (name, version) in super::get_api_group_names() {
        // The same version sets `get_api_groups` lists.
        let versions: Vec<&str> = if super::group_v1beta1_served(name) {
            vec![version, "v1beta1"]
        } else if name == "autoscaling" {
            vec!["v2", "v1"]
        } else {
            vec![version]
        };
        out.push(group_of(name, &versions));
    }

    // Established CRDs, one APIResource per served version.
    for crd in crds.iter().filter(|c| super::crd_established(c)) {
        let (Some(group), Some(names), Some(versions)) = (
            crd.pointer("/spec/group").and_then(|v| v.as_str()),
            crd.pointer("/spec/names"),
            crd.pointer("/spec/versions").and_then(|v| v.as_array()),
        ) else {
            continue;
        };
        let text = |k: &str| names.get(k).and_then(|v| v.as_str()).unwrap_or("");
        let (plural, kind) = (text("plural"), text("kind"));
        // apiextensions defaults a missing singular to the lowercased kind.
        let singular = match text("singular") {
            "" => kind.to_lowercase(),
            s => s.to_string(),
        };
        let namespaced = crd.pointer("/spec/scope").and_then(|v| v.as_str()) != Some("Cluster");
        let idx = match out.iter().position(|g| g.name == group) {
            Some(i) => i,
            None => {
                out.push(ApiGroupResources {
                    name: group.to_string(),
                    ..Default::default()
                });
                out.len() - 1
            }
        };
        for ver in versions {
            if !ver.get("served").and_then(|v| v.as_bool()).unwrap_or(false) {
                continue;
            }
            let Some(ver_name) = ver.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let g = &mut out[idx];
            if !g.versions.iter().any(|v| v == ver_name) {
                g.versions.push(ver_name.to_string());
            }
            if g.preferred_version.is_empty() {
                g.preferred_version = ver_name.to_string();
            }
            g.versioned_resources
                .entry(ver_name.to_string())
                .or_default()
                .push(ApiResource {
                    name: plural.to_string(),
                    singular_name: singular.clone(),
                    namespaced,
                    kind: kind.to_string(),
                });
        }
    }
    out
}

/// Whether `group` is one of the groups this server serves natively (not a
/// CRD-defined group); a custom resource is stored under a different key.
pub fn is_builtin_group(group: &str) -> bool {
    group.is_empty()
        || super::get_api_group_names()
            .iter()
            .any(|(g, _)| *g == group)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(name: &str, kind: &str) -> ApiResource {
        ApiResource {
            name: name.into(),
            singular_name: String::new(),
            namespaced: true,
            kind: kind.into(),
        }
    }

    fn group(
        name: &str,
        versions: &[&str],
        preferred: &str,
        resources: &[(&str, Vec<ApiResource>)],
    ) -> ApiGroupResources {
        ApiGroupResources {
            name: name.into(),
            versions: versions.iter().map(|v| v.to_string()).collect(),
            preferred_version: preferred.into(),
            versioned_resources: resources
                .iter()
                .map(|(v, r)| (v.to_string(), r.clone()))
                .collect(),
        }
    }

    /// client-go/restmapper/discovery_test.go:38-243 `TestRESTMapper`.
    #[test]
    fn discovery_rest_mapper_prefers_core_then_preferred_versions() {
        let resources = vec![
            group(
                "extensions",
                &["v1beta"],
                "v1beta",
                &[("v1beta", vec![res("jobs", "Job"), res("pods", "Pod")])],
            ),
            group(
                "",
                &["v1", "v2"],
                "v1",
                &[
                    ("v1", vec![res("pods", "Pod")]),
                    ("v2", vec![res("pods", "Pod")]),
                ],
            ),
            group(
                "unpreferred",
                &["v1", "v2beta1", "v2alpha1"],
                "v1",
                &[
                    ("v1", vec![res("broccoli", "Broccoli")]),
                    (
                        "v2beta1",
                        vec![res("broccoli", "Broccoli"), res("peas", "Pea")],
                    ),
                    (
                        "v2alpha1",
                        vec![res("broccoli", "Broccoli"), res("peas", "Pea")],
                    ),
                ],
            ),
        ];
        let m = new_discovery_rest_mapper(&resources);

        let kinds = [
            (Gvr::new("", "", "pods"), Gvk::new("", "v1", "Pod")),
            (Gvr::new("", "v1", "pods"), Gvk::new("", "v1", "Pod")),
            (Gvr::new("", "v2", "pods"), Gvk::new("", "v2", "Pod")),
            (
                Gvr::new("", "", "jobs"),
                Gvk::new("extensions", "v1beta", "Job"),
            ),
            (
                Gvr::new("", "", "peas"),
                Gvk::new("unpreferred", "v2beta1", "Pea"),
            ),
        ];
        for (input, want) in kinds {
            assert_eq!(m.kind_for(&input), Ok(want), "KindFor({input})");
        }
        let resources = [
            (Gvr::new("", "", "pods"), Gvr::new("", "v1", "pods")),
            (Gvr::new("", "v1", "pods"), Gvr::new("", "v1", "pods")),
            (Gvr::new("", "v2", "pods"), Gvr::new("", "v2", "pods")),
            (
                Gvr::new("", "", "jobs"),
                Gvr::new("extensions", "v1beta", "jobs"),
            ),
        ];
        for (input, want) in resources {
            assert_eq!(m.resource_for(&input), Ok(want), "ResourceFor({input})");
        }
    }

    /// priority_test.go:28-145 `TestPriorityRESTMapperResourceForErrorHandling`.
    /// Upstream uses a fixed delegate; this one is a real mapper in which every
    /// `(group, version)` serves the resource `dup`.
    #[test]
    fn priority_mapper_narrows_by_patterns_then_reports_ambiguity() {
        let gvs = [("one", "a"), ("two", "b"), ("one", "c")];
        let delegate = MultiRestMapper(
            gvs.iter()
                .map(|(g, v)| {
                    let mut d = DefaultRestMapper::new(&[(g, v)]);
                    d.add(Gvk::new(g, v, "dup"), Scope::Namespace);
                    d
                })
                .collect(),
        );
        let ask = Gvr::new("", "", "dup");
        let with = |patterns: Vec<Gvr>| PriorityRestMapper {
            delegate: delegate.clone(),
            resource_priority: patterns,
            ..Default::default()
        };
        // "error": nothing named "x"; the delegate error passes through.
        assert!(matches!(
            with(vec![]).resource_for(&Gvr::new("", "", "x")),
            Err(MapperError::NoResourceMatch { .. })
        ));
        // "ambiguous match"
        let err = with(vec![]).resource_for(&ask).unwrap_err();
        assert!(
            err.to_string().contains("matches multiple resources"),
            "{err}"
        );
        // "group selection" narrows to two, which stays ambiguous.
        assert!(with(vec![Gvr::new("one", ANY, ANY)])
            .resource_for(&ask)
            .is_err());
        // "empty match continues" + "group followed by version selection"
        assert_eq!(
            with(vec![
                Gvr::new("fail", ANY, ANY),
                Gvr::new("one", ANY, ANY),
                Gvr::new(ANY, "a", ANY),
            ])
            .resource_for(&ask),
            Ok(Gvr::new("one", "a", "dups"))
        );
        // "group selection" to a single hit
        assert_eq!(
            with(vec![Gvr::new("two", ANY, ANY)]).resource_for(&ask),
            Ok(Gvr::new("two", "b", "dups"))
        );
    }

    /// restmapper_test.go:262-438 `TestRESTMapperResourcesFor` (group-prefix
    /// and ambiguity cases).
    #[test]
    fn resources_for_prefers_exact_group_then_prefix() {
        let order = [
            ("second-group", "first-version"),
            ("first-group", "first-version"),
        ];
        let mut m = DefaultRestMapper::new(&order);
        m.add(
            Gvk::new("first-group", "first-version", "my-kind"),
            Scope::Namespace,
        );
        m.add(
            Gvk::new("second-group", "first-version", "my-kind"),
            Scope::Namespace,
        );
        for name in ["my-kinds", "my-kind"] {
            // group prefix works
            let got = m.resources_for(&Gvr::new("first", "", name)).unwrap();
            assert_eq!(
                got,
                vec![Gvr::new("first-group", "first-version", "my-kinds")]
            );
            // ambiguous, ordered by the preferred group-versions
            let got = m.resources_for(&Gvr::new("", "", name)).unwrap();
            assert_eq!(
                got,
                vec![
                    Gvr::new("second-group", "first-version", "my-kinds"),
                    Gvr::new("first-group", "first-version", "my-kinds"),
                ]
            );
            let err = m.resource_for(&Gvr::new("", "", name)).unwrap_err();
            assert!(
                err.to_string().contains(" matches multiple resources "),
                "{err}"
            );
            // explicit group match
            assert_eq!(
                m.resource_for(&Gvr::new("first-group", "", name)),
                Ok(Gvr::new("first-group", "first-version", "my-kinds"))
            );
        }
    }

    /// restmapper_test.go:440-464 `TestKindToResource`.
    #[test]
    fn unsafe_guess_kind_to_resource_pluralizes() {
        for (kind, plural, singular) in [
            ("Pod", "pods", "pod"),
            (
                "ReplicationController",
                "replicationcontrollers",
                "replicationcontroller",
            ),
            ("ImageRepository", "imagerepositories", "imagerepository"),
            ("miss", "misses", "miss"),
            ("lowercase", "lowercases", "lowercase"),
        ] {
            let (p, s) = unsafe_guess_kind_to_resource(&Gvk::new("", "", kind));
            assert_eq!(
                (p.resource.as_str(), s.resource.as_str()),
                (plural, singular)
            );
        }
    }

    /// restmapper_test.go:466-497 `TestRESTMapperResourceSingularizer`.
    #[test]
    fn resource_singularizer_returns_the_singular() {
        for (kind, plural, singular) in [
            ("Pod", "pods", "pod"),
            ("ImageRepository", "imagerepositories", "imagerepository"),
            ("Status", "statuses", "status"),
            ("lowercases", "lowercaseses", "lowercases"),
        ] {
            let mut m = DefaultRestMapper::new(&[("tgroup", "test")]);
            m.add(Gvk::new("tgroup", "test", kind), Scope::Namespace);
            assert_eq!(m.resource_singularizer(plural).as_deref(), Ok(singular));
        }
    }

    #[test]
    fn error_messages_match_upstream() {
        // errors.go:89-91 with GroupVersionResource.String().
        let e = MapperError::NoResourceMatch {
            partial: Gvr::new("", "", "foos"),
        };
        assert_eq!(e.to_string(), "no matches for /, Resource=foos");
    }

    #[test]
    fn served_discovery_resolves_builtin_forms() {
        let m = new_discovery_rest_mapper(&served_group_resources(&[]));
        // core group first, singular accepted
        assert_eq!(
            m.resource_for(&Gvr::new("", "", "pod")),
            Ok(Gvr::new("", "v1", "pods"))
        );
        assert_eq!(
            m.kind_for(&Gvr::new("", "", "pods")),
            Ok(Gvk::new("", "v1", "Pod"))
        );
        // `resource.group`
        assert_eq!(
            m.kind_for(&Gvr::new("apps", "", "deployment")),
            Ok(Gvk::new("apps", "v1", "Deployment"))
        );
        // "events" is served by core and events.k8s.io; core wins (priority).
        assert_eq!(
            m.kind_for(&Gvr::new("", "", "events")),
            Ok(Gvk::new("", "v1", "Event"))
        );
        // autoscaling prefers v2 over v1
        assert_eq!(
            m.resource_for(&Gvr::new("autoscaling", "", "horizontalpodautoscalers")),
            Ok(Gvr::new("autoscaling", "v2", "horizontalpodautoscalers"))
        );
        assert!(m.resource_for(&Gvr::new("", "", "foos")).is_err());
    }

    #[test]
    fn served_discovery_includes_established_crds() {
        let crd = serde_json::json!({
            "spec": {
                "group": "example.com", "scope": "Cluster",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "versions": [{"name": "v1", "served": true}]
            },
            "status": {"conditions": [{"type": "Established", "status": "True"}]}
        });
        let m = new_discovery_rest_mapper(&served_group_resources(&[crd]));
        assert_eq!(
            m.kind_for(&Gvr::new("example.com", "", "widget")),
            Ok(Gvk::new("example.com", "v1", "Widget"))
        );
    }

    fn gk(group: &str, kind: &str) -> Gk {
        Gk::new(group, kind)
    }

    fn mapper_with(gvs: &[(&str, &str)], kinds: &[(&str, &str, &str)]) -> DefaultRestMapper {
        let mut m = DefaultRestMapper::new(gvs);
        for (g, v, k) in kinds {
            m.add(Gvk::new(g, v, k), Scope::Namespace);
        }
        m
    }

    /// restmapper_test.go:499-560 `TestRESTMapperRESTMapping`.
    #[test]
    fn default_mapper_rest_mapping_table() {
        let tg = "tgroup";
        // no default versions: Unknown / InternalObject both fail
        let mut m = DefaultRestMapper::new(&[]);
        m.add(Gvk::new(tg, "test", "InternalObject"), Scope::Namespace);
        assert!(m.rest_mapping(&gk(tg, "Unknown"), &[]).is_err());
        assert!(m.rest_mapping(&gk(tg, "InternalObject"), &[]).is_err());

        let m = mapper_with(&[(tg, "test")], &[(tg, "test", "InternalObject")]);
        assert!(m.rest_mapping(&gk(tg, "Unknown"), &[]).is_err());
        let got = m
            .rest_mapping(&gk(tg, "InternalObject"), &["test"])
            .unwrap();
        assert_eq!(got.resource, Gvr::new(tg, "test", "internalobjects"));
        assert_eq!(got.kind, Gvk::new(tg, "test", "InternalObject"));
        assert_eq!(got.scope, Scope::Namespace);
        // no versions asked for: the default group-versions are searched
        let got = m.rest_mapping(&gk(tg, "InternalObject"), &[]).unwrap();
        assert_eq!(got.resource, Gvr::new(tg, "test", "internalobjects"));
    }

    /// restmapper_test.go:562-619 `TestRESTMapperRESTMappingSelectsVersion`.
    #[test]
    fn default_mapper_rest_mapping_selects_version() {
        let g = "tgroup";
        let mut m = DefaultRestMapper::new(&[(g, "test1"), (g, "test2")]);
        m.add(Gvk::new(g, "test1", "InternalObject"), Scope::Namespace);
        m.add(Gvk::new(g, "test2", "OtherObject"), Scope::Namespace);
        let internal = gk(g, "InternalObject");
        let other = gk(g, "OtherObject");

        // pick default matching object kind based on search order
        let got = m.rest_mapping(&other, &[]).unwrap();
        assert_eq!(got.resource, Gvr::new(g, "test2", "otherobjects"));
        assert_eq!(got.kind, Gvk::new(g, "test2", "OtherObject"));
        let got = m.rest_mapping(&internal, &[]).unwrap();
        assert_eq!(got.resource, Gvr::new(g, "test1", "internalobjects"));

        // mismatch of version
        assert!(m.rest_mapping(&internal, &["test2"]).is_err());
        assert!(m.rest_mapping(&other, &["test1"]).is_err());
        // not in the search versions
        assert!(m.rest_mapping(&other, &["test3"]).is_err());
        // explicit search order
        assert!(m.rest_mapping(&other, &["test3", "test1"]).is_err());
        let got = m.rest_mapping(&other, &["test3", "test2"]).unwrap();
        assert_eq!(got.resource, Gvr::new(g, "test2", "otherobjects"));
        assert_eq!(got.kind, Gvk::new(g, "test2", "OtherObject"));
    }

    /// restmapper_test.go:719-734 `TestRESTMapperReportsErrorOnBadVersion`
    /// plus the error strings of errors.go:106-120.
    #[test]
    fn rest_mapping_no_match_error_names_the_searched_versions() {
        let g = "tgroup";
        let m = mapper_with(
            &[(g, "test1"), (g, "test2")],
            &[(g, "test1", "InternalObject")],
        );
        let err = m
            .rest_mapping(&gk(g, "InternalObject"), &["test3"])
            .unwrap_err();
        assert!(err.is_no_match());
        assert_eq!(
            err.to_string(),
            r#"no matches for kind "InternalObject" in version "tgroup/test3""#
        );
        let none = MapperError::NoKindMatch {
            gk: gk(g, "Foo"),
            searched_versions: vec![],
        };
        assert_eq!(
            none.to_string(),
            r#"no matches for kind "Foo" in group "tgroup""#
        );
        let many = MapperError::NoKindMatch {
            gk: gk("", "Foo"),
            searched_versions: vec!["v2".into(), "v1beta".into(), "v2".into()],
        };
        // `sets.String.List()` sorts and de-duplicates; `%q` of a []string.
        assert_eq!(
            many.to_string(),
            r#"no matches for kind "Foo" in versions ["v1beta" "v2"]"#
        );
    }

    /// restmapper_test.go:621-717 `TestRESTMapperRESTMappings`.
    #[test]
    fn default_mapper_rest_mappings_table() {
        let tg = "tgroup";
        assert!(DefaultRestMapper::new(&[])
            .rest_mappings(&gk(tg, "Unknown"), &[])
            .is_err());
        let m = mapper_with(&[(tg, "v1")], &[]);
        assert!(m.rest_mappings(&gk(tg, "Unknown"), &[]).is_err());
        // ask for specific version - not available - thus error
        assert!(m.rest_mappings(&gk(tg, "InternalObject"), &["v2"]).is_err());

        // ask for specific version - available
        let m = mapper_with(&[(tg, "v1")], &[(tg, "v2", "InternalObject")]);
        let got = m.rest_mappings(&gk(tg, "InternalObject"), &["v2"]).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].resource, Gvr::new(tg, "v2", "internalobjects"));
        assert_eq!(got[0].kind, Gvk::new(tg, "v2", "InternalObject"));

        // ask for specific versions - only one available
        let got = m
            .rest_mappings(&gk(tg, "InternalObject"), &["v3", "v2"])
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, Gvk::new(tg, "v2", "InternalObject"));

        // do not ask for specific version - search through default versions
        let m = mapper_with(
            &[(tg, "v1"), (tg, "v2")],
            &[(tg, "v1", "InternalObject"), (tg, "v2", "InternalObject")],
        );
        let got = m.rest_mappings(&gk(tg, "InternalObject"), &[]).unwrap();
        assert_eq!(
            got.iter().map(|r| r.kind.clone()).collect::<Vec<_>>(),
            vec![
                Gvk::new(tg, "v1", "InternalObject"),
                Gvk::new(tg, "v2", "InternalObject")
            ]
        );
        // `runtime.APIVersionInternal` is skipped, so only the default
        // versions are searched (restmapper.go:473).
        let got = m
            .rest_mappings(&gk(tg, "InternalObject"), &["__internal"])
            .unwrap();
        assert_eq!(got.len(), 2);
    }

    /// multirestmapper_test.go:259-345 `TestMultiRESTMapperRESTMappings`
    /// (the cases a `DefaultRestMapper` can produce: upstream's
    /// `fixedRESTMapper{err: ...}` cases have no counterpart here).
    #[test]
    fn multi_mapper_rest_mappings() {
        let foo = gk("", "Foo");
        // "empty with no versions" / "one version" / "multi(two) versions"
        for versions in [vec![], vec!["v1beta"], vec!["v1beta", "v2"]] {
            let err = MultiRestMapper::default()
                .rest_mappings(&foo, &versions)
                .unwrap_err();
            assert_eq!(
                err,
                MapperError::NoKindMatch {
                    gk: foo.clone(),
                    searched_versions: versions.iter().map(|v| v.to_string()).collect(),
                }
            );
        }
        // "ignore not found": the delegate's no-match is dropped and replaced
        // by the multi mapper's own error carrying the asked-for versions.
        let multi = MultiRestMapper(vec![mapper_with(&[("", "v1")], &[("", "v1", "Bar")])]);
        let err = multi.rest_mappings(&foo, &["v1beta"]).unwrap_err();
        assert_eq!(
            err,
            MapperError::NoKindMatch {
                gk: foo.clone(),
                searched_versions: vec!["v1beta".into()],
            }
        );
        // "return both"
        let multi = MultiRestMapper(vec![
            mapper_with(&[("", "v1")], &[("", "v1", "Foo")]),
            mapper_with(&[("", "v2")], &[("", "v2", "Foo")]),
        ]);
        let got = multi.rest_mappings(&foo, &[]).unwrap();
        assert_eq!(
            got.iter().map(|m| m.kind.clone()).collect::<Vec<_>>(),
            vec![Gvk::new("", "v1", "Foo"), Gvk::new("", "v2", "Foo")]
        );
    }

    /// multirestmapper.go:154-187: one mapping wins, several are ambiguous.
    #[test]
    fn multi_mapper_rest_mapping_single_or_ambiguous() {
        let foo = gk("", "Foo");
        let one = MultiRestMapper(vec![
            mapper_with(&[("", "v1")], &[("", "v1", "Foo")]),
            mapper_with(&[("", "v2")], &[("", "v2", "Bar")]),
        ]);
        assert_eq!(
            one.rest_mapping(&foo, &[]).unwrap().kind,
            Gvk::new("", "v1", "Foo")
        );
        let two = MultiRestMapper(vec![
            mapper_with(&[("", "v1")], &[("", "v1", "Foo")]),
            mapper_with(&[("", "v2")], &[("", "v2", "Foo")]),
        ]);
        let err = two.rest_mapping(&foo, &[]).unwrap_err();
        assert_eq!(
            err,
            MapperError::AmbiguousKind {
                partial: Gvk::new("", "", "Foo"),
                resources: vec![],
                kinds: vec![Gvk::new("", "v1", "Foo"), Gvk::new("", "v2", "Foo")],
            }
        );
        assert_eq!(
            err.to_string(),
            "/, Kind=Foo matches multiple kinds [/v1, Kind=Foo /v2, Kind=Foo]"
        );
        let none = MultiRestMapper::default()
            .rest_mapping(&foo, &[])
            .unwrap_err();
        assert!(none.is_no_match());
    }

    fn priority_fixture(kind_priority: Vec<Gvk>) -> PriorityRestMapper {
        // priority_test.go:266-286 `mapping1..3` behind three mappers.
        PriorityRestMapper {
            delegate: MultiRestMapper(vec![
                mapper_with(&[("", "v1alpha1")], &[("", "v1alpha1", "Foo")]),
                mapper_with(&[("", "v1")], &[("", "v1", "Foo")]),
                mapper_with(&[("other", "v1")], &[("other", "v1", "Foo")]),
            ]),
            resource_priority: vec![],
            kind_priority,
        }
    }

    /// priority_test.go:266-372 `TestPriorityRESTMapperRESTMapping`
    /// (without the `fixedRESTMapper` error / "result + error" cases).
    #[test]
    fn priority_mapper_rest_mapping() {
        let foo = gk("", "Foo");
        // "empty"
        let err = PriorityRestMapper::default()
            .rest_mapping(&foo, &[])
            .unwrap_err();
        assert_eq!(
            err,
            MapperError::NoKindMatch {
                gk: foo.clone(),
                searched_versions: vec![]
            }
        );
        // "return error for ambiguous"
        let err = priority_fixture(vec![])
            .rest_mapping(&foo, &[])
            .unwrap_err();
        assert_eq!(
            err,
            MapperError::AmbiguousKind {
                partial: Gvk::new("", "", "Foo"),
                resources: vec![],
                kinds: vec![
                    Gvk::new("", "v1alpha1", "Foo"),
                    Gvk::new("", "v1", "Foo"),
                    Gvk::new("other", "v1", "Foo"),
                ],
            }
        );
        // "accept only item"
        let only = PriorityRestMapper {
            delegate: MultiRestMapper(vec![mapper_with(&[("", "v1")], &[("", "v1", "Foo")])]),
            ..Default::default()
        };
        assert_eq!(
            only.rest_mapping(&foo, &[]).unwrap().kind,
            Gvk::new("", "v1", "Foo")
        );
        // "return single priority"
        let got = priority_fixture(vec![Gvk::new("", "v1", ANY), Gvk::new("", "v1alpha1", ANY)])
            .rest_mapping(&foo, &[])
            .unwrap();
        assert_eq!(got.kind, Gvk::new("", "v1", "Foo"));
        // "return out of group match"
        let got = priority_fixture(vec![Gvk::new(ANY, "v1", ANY), Gvk::new("other", ANY, ANY)])
            .rest_mapping(&foo, &[])
            .unwrap();
        assert_eq!(got.kind, Gvk::new("other", "v1", "Foo"));
        // RESTMappings is the delegate's.
        assert_eq!(
            priority_fixture(vec![])
                .rest_mappings(&foo, &[])
                .unwrap()
                .len(),
            3
        );
    }

    /// priority_test.go:374-407 `TestPriorityRESTMapperRESTMappingHonorsUserVersion`.
    #[test]
    fn priority_mapper_rest_mapping_honors_user_version() {
        let mapper = PriorityRestMapper {
            delegate: MultiRestMapper(vec![
                mapper_with(&[("Bar", "v2alpha1")], &[("Bar", "v2alpha1", "Foo")]),
                mapper_with(&[("Bar", "v1")], &[("Bar", "v1", "Foo")]),
            ]),
            resource_priority: vec![],
            kind_priority: vec![Gvk::new("Bar", "v2alpha1", ANY), Gvk::new("Bar", ANY, ANY)],
        };
        let foo = gk("Bar", "Foo");
        assert_eq!(
            mapper.rest_mapping(&foo, &["v1"]).unwrap().kind,
            Gvk::new("Bar", "v1", "Foo")
        );
        assert_eq!(
            mapper.rest_mapping(&foo, &["v2alpha1"]).unwrap().kind,
            Gvk::new("Bar", "v2alpha1", "Foo")
        );
        // Without a user version the priority list picks v2alpha1.
        assert_eq!(
            mapper.rest_mapping(&foo, &[]).unwrap().kind,
            Gvk::new("Bar", "v2alpha1", "Foo")
        );
    }

    /// discovery.go:96-121: namespaced vs cluster scope, and `List` is Root.
    #[test]
    fn discovery_mapper_rest_mapping_carries_scope() {
        let m = new_discovery_rest_mapper(&served_group_resources(&[]));
        let pod = m.rest_mapping(&gk("", "Pod"), &[]).unwrap();
        assert_eq!(pod.resource, Gvr::new("", "v1", "pods"));
        assert_eq!(pod.scope, Scope::Namespace);
        let node = m.rest_mapping(&gk("", "Node"), &[]).unwrap();
        assert_eq!(node.scope, Scope::Root);
        let list = m.rest_mapping(&gk("", "List"), &[]).unwrap();
        assert_eq!(list.scope, Scope::Root);
        // autoscaling prefers v2 over v1; asking for v1 honours the user.
        let hpa = m
            .rest_mapping(&gk("autoscaling", "HorizontalPodAutoscaler"), &[])
            .unwrap();
        assert_eq!(hpa.kind.version, "v2");
        let hpa = m
            .rest_mapping(&gk("autoscaling", "HorizontalPodAutoscaler"), &["v1"])
            .unwrap();
        assert_eq!(hpa.kind.version, "v1");
    }
}
