use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

pub use crate::serde_helpers::{
    k8s_float, k8s_micro_time, k8s_micro_time_required, k8s_time, k8s_time_map, k8s_time_required,
    k8s_time_str, sorted_opt_map,
};

/// ObjectMeta is metadata that all persisted resources must have
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct ObjectMeta {
    /// Name must be unique within a namespace
    #[serde(
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub name: String,

    /// GenerateName is an optional prefix used to generate a unique name when Name is empty.
    /// The server will append a random suffix to this prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generate_name: Option<String>,

    /// Namespace defines the space within which the resource name must be unique
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// UID is a unique identifier for the resource. The decoder leaves it
    /// empty: upstream mints it in the registry, in `FillObjectMetaSystemFields`
    /// (`staging/src/k8s.io/apiserver/pkg/registry/rest/create.go`), which
    /// `registry::generic::store` ports as `fill_object_meta_system_fields`.
    /// A virtual object that never reaches a store — a SubjectAccessReview, a
    /// TokenReview — therefore keeps an empty uid, which is what lets
    /// `validate_subject_access_review`'s `must be empty` rule work.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,

    // NOTE: field order mirrors Go's `metav1.ObjectMeta` declaration order
    // (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go`), because
    // a marshalled bad value renders struct fields in that order (#2190).
    /// ResourceVersion is an opaque value for concurrency control
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,

    /// Generation is a sequence number representing a specific generation of the desired state.
    /// Go: plain `int64` `json:"generation,omitempty"` (meta/v1/types.go), so
    /// zero is absent (#2955).
    #[serde(
        default,
        deserialize_with = "crate::serde_helpers::zero_as_none",
        skip_serializing_if = "crate::serde_helpers::is_none_or_zero"
    )]
    pub generation: Option<i64>,

    /// CreationTimestamp is the creation time
    // Go 1.35: `CreationTimestamp Time `json:"creationTimestamp,omitempty,omitzero"``
    // (meta/v1/types.go:188). `omitzero` + `(*Time).IsZero` (time.go:60)
    // drops an unset timestamp, so it is NOT emitted as `null` (it was before
    // `omitzero` was added). Keep skipping `None`.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize",
        default
    )]
    pub creation_timestamp: Option<DateTime<Utc>>,

    /// DeletionTimestamp is the time when the resource will be deleted
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize",
        default
    )]
    pub deletion_timestamp: Option<DateTime<Utc>>,

    /// DeletionGracePeriodSeconds is the number of seconds before the object should be deleted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_grace_period_seconds: Option<i64>,

    /// Labels are key-value pairs for categorization
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "sorted_opt_map::serialize"
    )]
    pub labels: Option<HashMap<String, String>>,

    /// Annotations are key-value pairs for arbitrary metadata
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "sorted_opt_map::serialize"
    )]
    pub annotations: Option<HashMap<String, String>>,

    /// OwnerReferences are references to objects that own this object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_references: Option<Vec<OwnerReference>>,

    /// Finalizers are pre-deletion hooks that must complete before deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalizers: Option<Vec<String>>,

    /// ManagedFields maps workflow-id and version to the set of fields that are managed by that workflow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_fields: Option<Vec<ManagedFieldsEntry>>,
}

impl ObjectMeta {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            generate_name: None,
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            generation: None,
            managed_fields: None,
            resource_version: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            labels: None,
            annotations: None,
            finalizers: None,
            owner_references: None,
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = Some(labels);
        self
    }

    pub fn with_annotations(mut self, annotations: HashMap<String, String>) -> Self {
        self.annotations = Some(annotations);
        self
    }

    pub fn with_owner_reference(mut self, owner: OwnerReference) -> Self {
        self.owner_references = Some(vec![owner]);
        self
    }

    pub fn with_finalizers(mut self, finalizers: Vec<String>) -> Self {
        self.finalizers = Some(finalizers);
        self
    }

    /// Resolve `generateName` into a concrete name. If `name` is empty and
    /// `generateName` is a non-empty prefix, append a random 5-character suffix.
    ///
    /// When neither is set the name is left empty so downstream validation can
    /// reject the object — this must NOT fabricate an `auto-<id>` name, which is
    /// non-conformant (a create with no name and no generateName is invalid).
    /// Server-side name generation is normally handled centrally by
    /// `generate_name_middleware` before any handler runs; this remains for
    /// in-process callers that build objects directly.
    pub fn ensure_name(&mut self) {
        if self.name.is_empty() {
            if let Some(prefix) = self.generate_name.as_deref().filter(|p| !p.is_empty()) {
                let suffix = uuid::Uuid::new_v4().simple().to_string()[..5].to_string();
                self.name = format!("{prefix}{suffix}");
            }
        }
    }

    /// Ensure uid is populated (generate if empty). Also resolves generateName if name is empty.
    pub fn ensure_uid(&mut self) {
        self.ensure_name();
        if self.uid.is_empty() {
            self.uid = uuid::Uuid::new_v4().to_string();
        }
    }

    /// Ensure creation timestamp is set
    pub fn ensure_creation_timestamp(&mut self) {
        if self.creation_timestamp.is_none() {
            self.creation_timestamp = Some(Utc::now());
        }
    }

    /// Ensure generation is set (defaults to 1 for new resources)
    pub fn ensure_generation(&mut self) {
        if self.generation.is_none() {
            self.generation = Some(1);
        }
    }

    /// Check if the object is being deleted (has deletion timestamp)
    pub fn is_being_deleted(&self) -> bool {
        self.deletion_timestamp.is_some()
    }

    /// Add a finalizer to the object
    pub fn add_finalizer(&mut self, finalizer: String) {
        let finalizers = self.finalizers.get_or_insert_with(Vec::new);
        if !finalizers.contains(&finalizer) {
            finalizers.push(finalizer);
        }
    }

    /// Remove a finalizer from the object
    pub fn remove_finalizer(&mut self, finalizer: &str) {
        if let Some(finalizers) = &mut self.finalizers {
            finalizers.retain(|f| f != finalizer);
        }
    }

    /// Check if the object has any finalizers
    pub fn has_finalizers(&self) -> bool {
        self.finalizers.as_ref().is_some_and(|f| !f.is_empty())
    }
}

/// TypeMeta describes the type of the object
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypeMeta {
    /// Kind is the object's type
    #[serde(default)]
    pub kind: String,

    /// APIVersion defines the versioned schema of the object
    #[serde(default)]
    pub api_version: String,
}

/// Resource status phase
///
/// `Unknown` carries `#[serde(alias = "")]` because kubectl / client-go send
/// `status: { phase: "" }` on CREATE for resources where the server assigns
/// the phase (namespace, pod, etc.). Upstream Kubernetes accepts the empty
/// string and overwrites it; without this alias `Option<Phase>` rejects the
/// empty inner string and the api-server returns 422 before any handler
/// runs, breaking every kubectl-driven CREATE including the one hydrophone
/// + sonobuoy issue to bootstrap their test namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Phase {
    Pending,
    Running,
    Succeeded,
    Failed,
    #[serde(alias = "")]
    Unknown,
    Active,
    Terminating,
}

/// Label selector for matching resources
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "sorted_opt_map::serialize"
    )]
    pub match_labels: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<LabelSelectorRequirement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    /// Both `key` and `operator` are plain (non-pointer) Go fields upstream, so
    /// an absent one decodes to `""` and is answered by validation:
    /// `ValidateLabelSelectorRequirement`
    /// (`apimachinery/pkg/apis/meta/v1/validation/validation.go`) sends `key`
    /// through `ValidateLabelName` and rejects an unknown `operator` with "not
    /// a valid selector operator".
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub operator: String, // In, NotIn, Exists, DoesNotExist
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// The three answers of `LabelSelectorAsSelector`
/// (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/helpers.go:36-72`).
///
/// ```go
/// func LabelSelectorAsSelector(ps *LabelSelector) (labels.Selector, error) {
///     if ps == nil {
///         return labels.Nothing(), nil
///     }
///     if len(ps.MatchLabels)+len(ps.MatchExpressions) == 0 {
///         return labels.Everything(), nil
///     }
///     ...
/// }
/// ```
///
/// The distinction between the first two is load-bearing and easy to collapse:
/// an **absent** selector matches nothing, an **empty** (`{}`) one matches
/// everything. `policy/v1` says so in the API itself — "A null selector will
/// match no pods, while an empty ({}) selector will select all pods within the
/// namespace" (`staging/src/k8s.io/api/policy/v1/types.go:36-42`).
#[derive(Debug, Clone, PartialEq)]
pub enum Selector {
    /// `labels.Nothing()` — the selector was absent.
    Nothing,
    /// `labels.Everything()` — the selector was present and empty.
    Everything,
    /// One requirement per `matchLabels` entry and per `matchExpressions`
    /// entry, AND-combined. Every operator here is one of the four upstream
    /// knows; an unknown one is an error from `label_selector_as_selector`, not
    /// a requirement that never matches.
    Requirements(LabelSelector),
}

impl LabelSelector {
    /// `metav1.LabelSelectorAsSelector(ps).String()`
    /// (apimachinery/pkg/apis/meta/v1/helpers.go:36-74 and
    /// labels/selector.go): each `matchLabels` entry becomes `k=v`, each
    /// expression `k in (a,b)` / `k notin (a,b)` / `k` / `!k` with its values
    /// sorted, and the requirements are sorted by key (`Selector.Add`) and
    /// joined with `,`. An empty selector is `Everything()`, whose string is
    /// empty. The error is `NewRequirement`'s aggregate for a requirement it
    /// refuses.
    pub fn as_selector_string(&self) -> std::result::Result<String, String> {
        self.requirements(true)
    }

    /// The `NewRequirement` pass of `LabelSelectorAsSelector`
    /// (helpers.go:62-79) with no `String()` rendering: the same errors as
    /// [`Self::as_selector_string`], without building the text that
    /// `label_selector_as_selector` throws away (#2923).
    pub fn validate_requirements(&self) -> std::result::Result<(), String> {
        self.requirements(false).map(drop)
    }

    fn requirements(&self, render: bool) -> std::result::Result<String, String> {
        use crate::validation::field::{Error, Path};
        use crate::validation::metav1::{is_qualified_name, is_valid_label_value};

        let mut requirements: Vec<(String, String)> = Vec::new();
        let mut errs = Vec::new();
        // `NewRequirement` (labels/selector.go:185-225).
        let mut require = |key: &str, op: &str, values: &[String]| {
            let key_errs = is_qualified_name(key);
            if !key_errs.is_empty() {
                errs.push(Error::invalid(&Path::new("key"), key, key_errs.join("; ")));
            }
            let values_path = Path::new("values");
            let count_error = match op {
                "in" | "notin" if values.is_empty() => {
                    Some("for 'in', 'notin' operators, values set can't be empty")
                }
                "=" if values.len() != 1 => {
                    Some("exact-match compatibility requires one single value")
                }
                "" | "!" if !values.is_empty() => {
                    Some("values set must be empty for exists and does not exist")
                }
                _ => None,
            };
            if let Some(msg) = count_error {
                errs.push(Error::invalid(&values_path, values.to_vec(), msg));
            }
            for value in values {
                let value_errs = is_valid_label_value(value);
                if !value_errs.is_empty() {
                    errs.push(Error::invalid(
                        &values_path.key(key),
                        value.as_str(),
                        value_errs.join("; "),
                    ));
                }
            }
            if !render {
                return;
            }
            // `Requirement.String` (labels/selector.go:344-395).
            let mut sorted = values.to_vec();
            sorted.sort();
            let text = match op {
                "=" => format!("{key}={}", sorted.join(",")),
                "in" | "notin" => format!("{key} {op} ({})", sorted.join(",")),
                "!" => format!("!{key}"),
                _ => key.to_string(),
            };
            requirements.push((key.to_string(), text));
        };

        if let Some(labels) = &self.match_labels {
            for (k, v) in labels {
                require(k, "=", std::slice::from_ref(v));
            }
        }
        for expr in self.match_expressions.iter().flatten() {
            let op = match expr.operator.as_str() {
                "In" => "in",
                "NotIn" => "notin",
                "Exists" => "",
                "DoesNotExist" => "!",
                other => {
                    return Err(format!(
                        "\"{other}\" is not a valid label selector operator"
                    ))
                }
            };
            require(&expr.key, op, expr.values.as_deref().unwrap_or_default());
        }
        match errs.len() {
            0 => {}
            // `ErrorList.ToAggregate().Error()`: one error bare, several in
            // brackets.
            1 => return Err(errs[0].to_string()),
            _ => {
                let all: Vec<String> = errs.iter().map(ToString::to_string).collect();
                return Err(format!("[{}]", all.join(", ")));
            }
        }
        // `sort.Sort(ByKey(...))` is not stable, but requirements with equal
        // keys only arise from a selector repeating a key.
        requirements.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(requirements
            .into_iter()
            .map(|(_, text)| text)
            .collect::<Vec<_>>()
            .join(","))
    }
}

impl Selector {
    /// `labels.Selector.Matches`.
    pub fn matches(&self, labels: Option<&HashMap<String, String>>) -> bool {
        let selector = match self {
            Selector::Nothing => return false,
            Selector::Everything => return true,
            Selector::Requirements(selector) => selector,
        };

        if let Some(match_labels) = &selector.match_labels {
            for (k, v) in match_labels {
                if labels.and_then(|l| l.get(k)) != Some(v) {
                    return false;
                }
            }
        }

        for req in selector.match_expressions.iter().flatten() {
            let value = labels.and_then(|l| l.get(&req.key));
            let in_values =
                value.is_some_and(|v| req.values.as_ref().is_some_and(|values| values.contains(v)));
            let matched = match req.operator.as_str() {
                "In" => in_values,
                "NotIn" => !in_values,
                "Exists" => value.is_some(),
                "DoesNotExist" => value.is_none(),
                // Unreachable: `label_selector_as_selector` rejects it.
                _ => false,
            };
            if !matched {
                return false;
            }
        }

        true
    }

    /// Whether this is `labels.Everything()` — the `{}` selector.
    pub fn is_everything(&self) -> bool {
        matches!(self, Selector::Everything)
    }
}

/// `LabelSelectorAsSelector` (`helpers.go:36-72`).
///
/// The error is upstream's `"%q is not a valid label selector operator"`. What
/// a caller does with it is upstream's business and differs per call site:
/// eviction skips the object ("This object has an invalid selector, it does not
/// match the pod", `pkg/registry/core/pod/storage/eviction.go:498-502`), the
/// ClusterRole aggregation controller fails the sync
/// (`pkg/controller/clusterroleaggregation/clusterroleaggregation_controller.go:104-107`),
/// and the workload validators skip the template-matches-selector check
/// (`if err == nil && !selector.Matches(...)`).
pub fn label_selector_as_selector(selector: Option<&LabelSelector>) -> Result<Selector, String> {
    let Some(selector) = selector else {
        return Ok(Selector::Nothing);
    };
    let match_labels = selector.match_labels.as_ref().map_or(0, HashMap::len);
    let match_expressions = selector.match_expressions.as_ref().map_or(0, Vec::len);
    if match_labels + match_expressions == 0 {
        return Ok(Selector::Everything);
    }
    // Every requirement goes through `labels.NewRequirement`
    // (`helpers.go:62-79`, `labels/selector.go:185-225`): an unknown operator,
    // an invalid key or value, and a wrong value count are all errors.
    // `validate_requirements` is that same port (shared with
    // `as_selector_string`), and checks the operator with upstream's message.
    selector.validate_requirements()?;
    Ok(Selector::Requirements(selector.clone()))
}

impl LabelSelector {
    /// Whether `labels` satisfies this selector, for a caller that treats an
    /// invalid operator as "does not match" — which is what upstream's eviction
    /// path does. Equivalent to
    /// `LabelSelectorAsSelector(&self)` followed by `Matches`.
    ///
    /// An **empty** selector matches everything. It used to match nothing here,
    /// justified by a "`selector.Empty()` guard" in eviction and in the
    /// ClusterRole aggregation controller that release-1.35 does not have
    /// (#2012): both read the selector through `LabelSelectorAsSelector` and
    /// neither tests it for emptiness.
    pub fn matches_labels(&self, labels: &HashMap<String, String>) -> bool {
        label_selector_as_selector(Some(self))
            .map(|selector| selector.matches(Some(labels)))
            .unwrap_or(false)
    }
}

/// Tolerant deserializer for `HashMap<String, String>` maps whose
/// values represent Kubernetes `resource.Quantity` wire-form values.
///
/// Upstream `Quantity` round-trips as a JSON string (`"100m"`, `"1Gi"`),
/// but the Go marshaller routes values through `inf.Dec` and several
/// code paths emit a bare JSON number rather than a quoted string —
/// notably zero-valued quantities defaulted from `int64` fields, and
/// some legacy clients. K8s' Go deserialiser tolerates all three forms
/// (`UnmarshalJSON` in `pkg/api/resource/quantity.go`).
///
/// Our previous plain `HashMap<String, String>` deserialiser rejected
/// every pod-update body that carried a numeric `0` in
/// `containers[*].resources.{requests,limits}` (column 942 of the
/// canonical service-account-injected pod body). This function accepts
/// `String`, integer, and float JSON values and renders each into the
/// canonical string form so existing string-typed maps round-trip
/// without forcing every caller to switch to a newtype.
pub fn deserialize_quantity_map<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{MapAccess, Visitor};
    use std::fmt;

    struct OuterVisitor;
    impl<'de> Visitor<'de> for OuterVisitor {
        type Value = Option<HashMap<String, String>>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("null or a map<string, Quantity>")
        }
        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D2>(self, d: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            d.deserialize_map(InnerVisitor).map(Some)
        }
        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            InnerVisitor.visit_map(map).map(Some)
        }
    }

    struct InnerVisitor;
    impl<'de> Visitor<'de> for InnerVisitor {
        type Value = HashMap<String, String>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a map<string, Quantity>")
        }
        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut out = HashMap::new();
            while let Some(key) = map.next_key::<String>()? {
                let v: serde_json::Value = map.next_value()?;
                let s = match v {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Null => continue,
                    other => {
                        return Err(serde::de::Error::custom(format!(
                            "Quantity value must be a string or number, got {}",
                            other
                        )));
                    }
                };
                crate::quantity::Quantity::parse(&s).map_err(|e| {
                    serde::de::Error::custom(format!("invalid Quantity value for {key:?}: {e}"))
                })?;
                out.insert(key, s);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_option(OuterVisitor)
}

/// Resource requirements for compute resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "deserialize_quantity_map"
    )]
    pub limits: Option<HashMap<String, String>>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "deserialize_quantity_map"
    )]
    pub requests: Option<HashMap<String, String>>,

    /// Claims lists the names of resources, defined in spec.resourceClaims, that are used by this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claims: Option<Vec<ResourceClaim>>,
}

/// ResourceClaim references one entry in PodSpec.resourceClaims
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaim {
    /// Name must match the name of one entry in pod.spec.resourceClaims of the Pod
    pub name: String,

    /// Request is the name chosen for a request in the referenced claim
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,
}

/// OwnerReference contains information about an object that owns another object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    /// API version of the referent
    pub api_version: String,

    /// Kind of the referent
    pub kind: String,

    /// Name of the referent
    pub name: String,

    /// UID of the referent
    pub uid: String,

    /// If true, AND if the owner has the "foregroundDeletion" finalizer, then
    /// the owner cannot be deleted from the key-value store until this
    /// reference is removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_owner_deletion: Option<bool>,

    /// If true, this reference points to the managing controller
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller: Option<bool>,
}

impl OwnerReference {
    /// Create a new owner reference
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            name: name.into(),
            uid: uid.into(),
            block_owner_deletion: None,
            controller: None,
        }
    }

    /// Mark this reference as the managing controller
    pub fn with_controller(mut self, is_controller: bool) -> Self {
        self.controller = Some(is_controller);
        self
    }

    /// Set whether this reference blocks owner deletion
    pub fn with_block_owner_deletion(mut self, block: bool) -> Self {
        self.block_owner_deletion = Some(block);
        self
    }
}

/// Deletion propagation policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum DeletionPropagation {
    /// Orphan dependents (default for some resources)
    Orphan,
    /// Foreground deletion - delete dependents first
    Foreground,
    /// Background deletion - delete owner first, let GC clean up dependents
    Background,
}

/// ListMeta describes metadata for list objects
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListMeta {
    /// resourceVersion is a version string for the list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,

    /// continue token for pagination
    #[serde(skip_serializing_if = "Option::is_none", rename = "continue")]
    pub continue_token: Option<String>,

    /// remainingItemCount is the number of subsequent items in the list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_item_count: Option<i64>,
}

impl Default for ListMeta {
    fn default() -> Self {
        Self {
            // Use "0" as default. Handlers should call with_resource_version()
            // to set the actual value from storage. DO NOT use timestamps here
            // as they create revision space mismatches with etcd mod_revisions.
            resource_version: Some("0".to_string()),
            continue_token: None,
            remaining_item_count: None,
        }
    }
}

/// List is a generic wrapper for Kubernetes list responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct List<T> {
    /// APIVersion defines the versioned schema of this representation
    pub api_version: String,

    /// Kind is a string value representing the REST resource
    pub kind: String,

    /// Standard list metadata
    pub metadata: ListMeta,

    /// List of objects
    pub items: Vec<T>,
}

impl<T> List<T> {
    /// Create a new List with the specified kind and API version.
    /// Automatically sets resourceVersion from the highest item's resourceVersion.
    pub fn new(kind: impl Into<String>, api_version: impl Into<String>, items: Vec<T>) -> Self
    where
        T: serde::Serialize,
    {
        let mut list = Self {
            kind: kind.into(),
            api_version: api_version.into(),
            metadata: ListMeta::default(),
            items,
        };
        list.set_resource_version_from_items();
        list
    }

    /// Set the list's resourceVersion from the highest item resourceVersion.
    /// This ensures clients get a valid revision for subsequent watches.
    pub fn set_resource_version_from_items(&mut self)
    where
        T: serde::Serialize,
    {
        let mut max_rv: i64 = 0;
        for item in &self.items {
            if let Ok(v) = serde_json::to_value(item) {
                if let Some(rv_str) = v
                    .get("metadata")
                    .and_then(|m| m.get("resourceVersion"))
                    .and_then(|r| r.as_str())
                {
                    if let Ok(rv) = rv_str.parse::<i64>() {
                        if rv > max_rv {
                            max_rv = rv;
                        }
                    }
                }
            }
        }
        // Always set a resource version — "0" or empty causes client-go to fail
        // with "initial RV '0' is not supported". Use max item RV or "1" as fallback.
        self.metadata.resource_version = Some(if max_rv > 0 {
            max_rv.to_string()
        } else {
            "1".to_string()
        });
    }

    /// Create a new List with resource version
    pub fn with_resource_version(mut self, resource_version: impl Into<String>) -> Self {
        self.metadata.resource_version = Some(resource_version.into());
        self
    }
}

/// Condition contains details for one aspect of the current state of an API Resource
/// This is the standard Kubernetes condition type (metav1.Condition)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition in CamelCase
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition: True, False, or Unknown
    pub status: String,

    /// ObservedGeneration represents the .metadata.generation that the condition was set based upon
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// LastTransitionTime is the last time the condition transitioned from one status to another
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub last_transition_time: Option<DateTime<Utc>>,

    /// Reason contains a programmatic identifier indicating the reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Message is a human readable message indicating details about the transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Status is a return value for calls that don't return other objects (metav1.Status).
/// This is the standard Kubernetes error/status response type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    /// Kind is always "Status"
    #[serde(default = "default_status_kind")]
    pub kind: String,

    /// APIVersion is always "v1"
    #[serde(default = "default_status_api_version")]
    pub api_version: String,

    /// Standard list metadata (usually empty for Status)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ListMeta>,

    /// Status of the operation: "Success" or "Failure"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    /// A human-readable description of the status of this operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// A machine-readable description of why this operation is in the "Failure" status.
    /// E.g., "NotFound", "AlreadyExists", "Conflict", "Invalid", "Forbidden", "Unauthorized"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Extended data associated with the reason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<StatusDetails>,

    /// Suggested HTTP return code for this status (0 if not set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
}

fn default_status_kind() -> String {
    "Status".to_string()
}

fn default_status_api_version() -> String {
    "v1".to_string()
}

impl Status {
    /// Create a failure Status response
    pub fn failure(message: impl Into<String>, reason: impl Into<String>, code: u16) -> Self {
        Self {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            metadata: None,
            status: Some("Failure".to_string()),
            message: Some(message.into()),
            reason: Some(reason.into()),
            details: None,
            code: Some(code),
        }
    }

    /// Create a failure Status with details
    pub fn failure_with_details(
        message: impl Into<String>,
        reason: impl Into<String>,
        code: u16,
        details: StatusDetails,
    ) -> Self {
        Self {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            metadata: None,
            status: Some("Failure".to_string()),
            message: Some(message.into()),
            reason: Some(reason.into()),
            details: Some(details),
            code: Some(code),
        }
    }

    /// Create a success Status response
    pub fn success() -> Self {
        Self {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            metadata: None,
            status: Some("Success".to_string()),
            message: None,
            reason: None,
            details: None,
            code: Some(200),
        }
    }
}

/// StatusDetails is a set of additional properties that MAY be set by the server
/// to provide additional information about a response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusDetails {
    /// The name attribute of the resource associated with the status StatusReason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The group attribute of the resource associated with the status StatusReason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,

    /// The kind attribute of the resource associated with the status StatusReason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,

    /// UID of the resource (when there is a single resource which can be described)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// The Causes array includes more details associated with the StatusReason failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causes: Option<Vec<StatusCause>>,

    /// If specified, the time in seconds before the operation should be retried
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<i32>,
}

/// StatusCause provides more information about an api.Status failure, including
/// cases when multiple errors are encountered.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusCause {
    /// A machine-readable description of the cause of the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// A human-readable description of the cause of the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// The field of the resource that has caused this error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// ManagedFieldsEntry is a workflow-id, a FieldSet and the group version of the resource
/// that the fieldset applies to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedFieldsEntry {
    /// Manager is an identifier of the workflow managing these fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manager: Option<String>,

    /// Operation is the type of operation which lead to this ManagedFieldsEntry (Apply or Update)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,

    /// APIVersion defines the version of the resource that this field set applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,

    /// Time is the timestamp of when the ManagedFields entry was added
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub time: Option<DateTime<Utc>>,

    /// FieldsType is the discriminator for the different fields format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields_type: Option<String>,

    /// FieldsV1 stores a JSON representation of the fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields_v1: Option<serde_json::Value>,

    /// Subresource is the name of the subresource used to update that object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresource: Option<String>,
}
