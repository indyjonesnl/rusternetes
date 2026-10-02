//! Custom resources — port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresource/`
//! (`strategy.go`, `status_strategy.go`, `validator.go`, `etcd.go`) over
//! [`crate::registry::generic::Store`].
//!
//! Upstream builds one `customResourceStrategy` per served version of a CRD
//! (`customresource_handler.go: getOrCreateServingInfoFor`) and keeps the
//! objects as `unstructured.Unstructured`. Here the object is
//! [`CustomResource`], whose `extra` map carries every top-level field beyond
//! `spec` and `status`, and the strategy is built per request from the CRD the
//! handler just read — the per-request construction `handlers/pod.rs` also
//! uses.
//!
//! Where the validation lives. `validator.go` / `strategy.go` call the
//! structural-schema, list-type and CEL validators of
//! `pkg/apiserver/{validation,schema/...}`. Rusternetes' equivalents are
//! `handlers::custom_resource::validate_custom_resource_with_old` (schema +
//! `x-kubernetes-validations` with KEP-4008 ratcheting) and
//! `handlers::cel_validation`; they report a rendered message rather than a
//! field error list, so [`legacy_errors`] turns the message back into one.
//!
//! Not ported here, and still served by the bespoke handlers:
//! - `/scale` (`etcd.go: ScaleREST`), tracked in #2134;
//! - list and watch, tracked in #2135;
//! - `GetResetFields` (managed-fields reset sets) and table conversion;
//! - `x-kubernetes-list-type` map/set uniqueness (`listtype.ValidateListSetsAndMaps`)
//!   and the `schemaobjectmeta.Validate` embedded-resource check, which
//!   Rusternetes' schema validator does not implement.
//!
//! Deviation from upstream, deliberate: upstream's request decoder
//! (`schemaCoercingDecoder`, customresource_handler.go:1321-1346) prunes and
//! defaults the object as it is decoded, so admission webhooks see the coerced
//! object and `fieldValidation=Strict` learns the unknown fields from the
//! pruning. Rusternetes' endpoint handlers decode with a plain `fn` hook that
//! cannot carry a CRD, so [`Strategy::prepare_for_create`] /
//! [`Strategy::prepare_for_update`] default and prune instead — after the
//! mutating webhooks, which is where the re-decode of a webhook's patch
//! (patch.go / mutating dispatcher) puts them upstream too — and
//! [`CustomResourceRest`] runs the strict-field checks before that pruning.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::resources::{
    prune_custom_resource, CustomResource, CustomResourceDefinition,
    CustomResourceSubresourceScale, ResourceScope,
};
use rusternetes_common::schema_validation::SchemaValidator;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, ErrorType, Path};
use rusternetes_common::validation::metav1::is_qualified_name;
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use crate::handlers::custom_resource::{
    apply_schema_defaults, validate_custom_resource_with_old, validate_field_selector_paths,
};
use crate::registry::generic::store::{BeginUpdate, Finish};
use crate::registry::generic::{CreateOptions, Deleted, Store, UpdateOptions};
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject, ValidateObjectUpdate,
};

/// The first segment of a custom resource's storage key
/// (`/registry/{this}/{ns}/{name}`): the group with `.` as `_`, then the
/// plural. Upstream keys on `/<group>/<plural>`; this is the layout the
/// bespoke handlers already wrote, so stored objects stay readable.
pub fn storage_prefix(group: &str, plural: &str) -> String {
    format!("{}_{}", group.replace('.', "_"), plural)
}

/// `validateKubeFinalizerName` (validator.go:81-98) as a list of warnings.
fn validate_kube_finalizer_name(value: &str, fld_path: &Path) -> Vec<String> {
    let mut warnings: Vec<String> = is_qualified_name(value)
        .into_iter()
        .map(|msg| format!("{fld_path}: {value:?}: {msg}"))
        .collect();
    if value.split('/').count() == 1
        && !["orphan", "foregroundDeletion", "kubernetes"].contains(&value)
    {
        if value.contains('.') {
            warnings.push(format!(
                "{fld_path}: {value:?}: prefer a domain-qualified finalizer name including a path (/) to avoid accidental conflicts with other finalizer writers"
            ));
        } else {
            warnings.push(format!(
                "{fld_path}: {value:?}: prefer a domain-qualified finalizer name to avoid accidental conflicts with other finalizer writers"
            ));
        }
    }
    warnings
}

/// `generateWarningsFromObj` (strategy.go:204-228): a warning for each
/// finalizer the write adds whose name is not domain-qualified.
fn generate_warnings(obj: &CustomResource, old: Option<&CustomResource>) -> Vec<String> {
    let fld_path = Path::new("metadata").child("finalizers");
    let old_finalizers: Vec<&String> = old
        .and_then(|o| o.metadata.finalizers.as_ref())
        .map(|f| f.iter().collect())
        .unwrap_or_default();
    let mut added: Vec<&String> = obj
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.iter().filter(|n| !old_finalizers.contains(n)).collect())
        .unwrap_or_default();
    added.sort();
    added.dedup();
    added
        .into_iter()
        .flat_map(|f| validate_kube_finalizer_name(f, &fld_path))
        .collect()
}

/// A validation message in the rendered form the bespoke validators produce
/// (`spec.bars[0].name: Required value`, `spec.size: Invalid value: ...`) as
/// the field errors they stand for, so the Store reports them like any other
/// strategy's. The rendered text round-trips exactly: the field is the part
/// before the first `": "`, the error type the label that follows it.
pub fn legacy_errors(err: Error) -> ErrorList {
    let msg = match err {
        Error::InvalidResource(msg) | Error::BadRequest(msg) => msg,
        other => other.to_string(),
    };
    vec![parse_rendered(&msg)]
}

fn parse_rendered(msg: &str) -> FieldError {
    const LABELS: [(&str, ErrorType); 8] = [
        ("Required value", ErrorType::Required),
        ("Invalid value", ErrorType::Invalid),
        ("Unsupported value", ErrorType::NotSupported),
        ("Duplicate value", ErrorType::Duplicate),
        ("Too long", ErrorType::TooLong),
        ("Too many", ErrorType::TooMany),
        ("Forbidden", ErrorType::Forbidden),
        ("Not found", ErrorType::NotFound),
    ];
    let (field, body) = match msg.split_once(": ") {
        Some((head, rest)) if !head.is_empty() && !head.contains(char::is_whitespace) => {
            (head, rest)
        }
        _ => ("", msg),
    };
    for (label, error_type) in LABELS {
        let detail = if body == label {
            Some("")
        } else {
            body.strip_prefix(label).and_then(|r| r.strip_prefix(": "))
        };
        if let Some(detail) = detail {
            return FieldError {
                error_type,
                field: field.to_string(),
                bad_value: rusternetes_common::validation::field::BadValue::Omit,
                detail: detail.to_string(),
                origin: String::new(),
            };
        }
    }
    FieldError {
        error_type: ErrorType::Invalid,
        field: field.to_string(),
        bad_value: rusternetes_common::validation::field::BadValue::Omit,
        detail: body.to_string(),
        origin: String::new(),
    }
}

/// `ValidateScaleSpec` / `ValidateScaleStatus` (validator.go:134-180): the
/// replica counts at the scale paths must be non-negative integers that fit
/// an int32.
fn validate_scale(cr: &CustomResource, scale: &CustomResourceSubresourceScale) -> ErrorList {
    let mut errs = Vec::new();
    let value = serde_json::to_value(cr).unwrap_or_default();
    let mut check = |path: &str| {
        let mut cur = &value;
        for part in path.trim_start_matches('.').split('.') {
            match cur.get(part) {
                Some(next) => cur = next,
                None => return,
            }
        }
        let Some(n) = cur.as_i64() else {
            if !cur.is_null() {
                errs.push(FieldError::invalid(
                    &Path::new(path),
                    0,
                    "unexpected type: expected an integer",
                ));
            }
            return;
        };
        if n < 0 {
            errs.push(FieldError::invalid(
                &Path::new(path),
                n,
                "should be a non-negative integer",
            ));
        } else if n > i32::MAX as i64 {
            errs.push(FieldError::invalid(
                &Path::new(path),
                n,
                format!("should be less than or equal to {}", i32::MAX),
            ));
        }
    };
    check(&scale.spec_replicas_path);
    check(&scale.status_replicas_path);
    errs
}

/// The `fieldValidation=Strict` checks of the bespoke handlers, run against
/// the object before it is pruned. `Patch` reports an unknown top-level field
/// the way the patch path always has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrictMode {
    Off,
    Write,
    Patch,
}

/// Fields `ObjectMeta` declares; anything else under a `metadata` key, at the
/// root or in an embedded object, is unknown
/// (`pkg/apiserver/schema/objectmeta/validation.go`).
const KNOWN_META: &[&str] = &[
    "name",
    "generateName",
    "namespace",
    "selfLink",
    "uid",
    "resourceVersion",
    "generation",
    "creationTimestamp",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "labels",
    "annotations",
    "ownerReferences",
    "finalizers",
    "managedFields",
    "clusterName",
];

fn check_embedded_meta(value: &serde_json::Value, path: &str) -> Option<String> {
    if let Some(obj) = value.as_object() {
        if let Some(meta) = obj.get("metadata").and_then(|m| m.as_object()) {
            let meta_path = if path.is_empty() {
                ".metadata".to_string()
            } else {
                format!("{path}.metadata")
            };
            for key in meta.keys() {
                if !KNOWN_META.contains(&key.as_str()) {
                    return Some(format!("{meta_path}.{key}: field not declared in schema"));
                }
            }
        }
        for (key, val) in obj {
            if key == "metadata" {
                continue;
            }
            let child = if path.is_empty() {
                format!(".{key}")
            } else {
                format!("{path}.{key}")
            };
            if let Some(err) = check_embedded_meta(val, &child) {
                return Some(err);
            }
        }
    } else if let Some(arr) = value.as_array() {
        for item in arr {
            if let Some(err) = check_embedded_meta(item, path) {
                return Some(err);
            }
        }
    }
    None
}

/// `customResourceStrategy` (strategy.go:50-60) for one served version.
pub struct Strategy {
    crd: Arc<CustomResourceDefinition>,
    version: String,
}

impl Strategy {
    pub fn new(crd: Arc<CustomResourceDefinition>, version: &str) -> Self {
        Self {
            crd,
            version: version.to_string(),
        }
    }

    fn api_version(&self) -> String {
        format!("{}/{}", self.crd.spec.group, self.version)
    }

    fn crd_version(
        &self,
    ) -> Option<&rusternetes_common::resources::CustomResourceDefinitionVersion> {
        self.crd
            .spec
            .versions
            .iter()
            .find(|v| v.name == self.version)
    }

    fn subresources(&self) -> Option<&rusternetes_common::resources::CustomResourceSubresources> {
        self.crd_version().and_then(|v| v.subresources.as_ref())
    }

    fn status_enabled(&self) -> bool {
        self.subresources().is_some_and(|s| s.status.is_some())
    }

    fn scale(&self) -> Option<&CustomResourceSubresourceScale> {
        self.subresources().and_then(|s| s.scale.as_ref())
    }

    /// What the request decoder does to a CR that the strategies rely on:
    /// type meta, schema defaults and structural pruning
    /// (customresource_handler.go:1195-1250, 1406-1470).
    fn coerce(&self, cr: &mut CustomResource) {
        cr.api_version = self.api_version();
        if cr.kind.is_empty() {
            cr.kind = self.crd.spec.names.kind.clone();
        }
        apply_schema_defaults(&self.crd, &self.version, cr);
        prune_custom_resource(&self.crd, &self.version, cr);
    }

    /// The object's schema and CEL errors, as field errors.
    fn schema_errors(&self, cr: &CustomResource, old: Option<&CustomResource>) -> ErrorList {
        let mut errs = Vec::new();
        if let Err(e) = validate_custom_resource_with_old(&self.crd, &self.version, cr, old) {
            errs.extend(legacy_errors(e));
        }
        if let Some(scale) = self.scale() {
            errs.extend(validate_scale(cr, scale));
        }
        errs
    }

    /// `ValidateTypeMeta` (validator.go:120-132).
    pub fn type_meta_errors(&self, cr: &CustomResource) -> ErrorList {
        if !cr.kind.is_empty() && cr.kind != self.crd.spec.names.kind {
            return vec![FieldError::invalid(
                &Path::new("kind"),
                cr.kind.clone(),
                format!("must be {}", self.crd.spec.names.kind),
            )];
        }
        Vec::new()
    }
}

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:105-107.
    fn namespace_scoped(&self) -> bool {
        !matches!(self.crd.spec.scope, ResourceScope::Cluster)
    }
}

impl RestCreateStrategy<CustomResource> for Strategy {
    /// `PrepareForCreate` (strategy.go:120-135): with a `/status`
    /// subresource a create cannot set status; the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut CustomResource) {
        if self.status_enabled() {
            obj.status = None;
        }
        obj.metadata.generation = Some(1);
        self.coerce(obj);
    }

    /// `Validate` (strategy.go:208-230, validator.go:44-58).
    fn validate(&self, _ctx: &RequestContext, obj: &CustomResource) -> ErrorList {
        let mut errs = self.type_meta_errors(obj);
        if !errs.is_empty() {
            return errs;
        }
        errs.extend(validate_object_meta(
            &obj.metadata,
            self.namespace_scoped(),
            name_is_dns_subdomain,
            &Path::new("metadata"),
        ));
        errs.extend(self.schema_errors(obj, None));
        errs
    }

    /// `WarningsOnCreate` (strategy.go:233-235).
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &CustomResource) -> Vec<String> {
        generate_warnings(obj, None)
    }
}

/// The content that counts towards `metadata.generation`: everything but
/// `metadata` (strategy.go:176-184 `copyNonMetadata`) — and the `apiVersion`,
/// which only records which served version the write came through.
fn non_metadata(cr: &CustomResource) -> serde_json::Value {
    let mut value = serde_json::to_value(cr).unwrap_or_default();
    if let Some(obj) = value.as_object_mut() {
        obj.remove("metadata");
        obj.remove("apiVersion");
    }
    value
}

impl RestUpdateStrategy<CustomResource> for Strategy {
    /// strategy.go:276-279: a POST is needed to create a custom resource.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:138-174): with a `/status`
    /// subresource an update cannot set status, and any change other than to
    /// `metadata` bumps the generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CustomResource,
        old: &CustomResource,
    ) {
        if self.status_enabled() {
            obj.status = old.status.clone();
        }
        self.coerce(obj);
        if non_metadata(obj) != non_metadata(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateUpdate` (strategy.go:290-345, validator.go:60-71). The
    /// `ValidateObjectMetaUpdate` half is `BeforeUpdate`'s.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResource,
        old: &CustomResource,
    ) -> ErrorList {
        let errs = self.type_meta_errors(obj);
        if !errs.is_empty() {
            return errs;
        }
        self.schema_errors(obj, Some(old))
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResource,
        old: &CustomResource,
    ) -> Vec<String> {
        generate_warnings(obj, Some(old))
    }

    /// strategy.go:282-285.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// Custom resources use the default delete strategy.
impl RestDeleteStrategy<CustomResource> for Strategy {}

/// `statusStrategy` (status_strategy.go:38-45): the update strategy of
/// `/status`.
pub struct StatusStrategy(Arc<Strategy>);

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        self.0.namespace_scoped()
    }
}

impl RestUpdateStrategy<CustomResource> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (status_strategy.go:62-86): the old object, with
    /// the new object's status — and its managed fields — on it.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CustomResource,
        old: &CustomResource,
    ) {
        let status = obj.status.take();
        let managed_fields = obj.metadata.managed_fields.take();
        *obj = old.clone();
        obj.status = status;
        obj.metadata.managed_fields = managed_fields;
        obj.api_version = self.0.api_version();
    }

    /// `ValidateUpdate` (status_strategy.go:88-135). The bespoke validator
    /// checks every property against its schema, which on a status write the
    /// spec — copied from the old object above — passes through ratcheting.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResource,
        old: &CustomResource,
    ) -> ErrorList {
        self.0.schema_errors(obj, Some(old))
    }

    /// `WarningsOnUpdate` (status_strategy.go:140-143).
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        _obj: &CustomResource,
        _old: &CustomResource,
    ) -> Vec<String> {
        Vec::new()
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

struct Noop;

#[async_trait]
impl Finish for Noop {
    async fn finish(self: Box<Self>, _success: bool) {}
}

/// Upstream decodes what it reads from storage through
/// `crdConversionRESTOptionsGetter` (customresource_handler.go:1275-1310):
/// the schema defaults and pruning of every read, and — for objects written
/// before ObjectMeta was validated — a repaired generation
/// (`repairGeneration`, :1465-1467). Objects the bespoke handlers stored also
/// lack `metadata.namespace`, which the request path supplies.
struct DecodeRepair {
    strategy: Arc<Strategy>,
}

impl DecodeRepair {
    fn repair(&self, ctx: &RequestContext, cr: &mut CustomResource) {
        if self.strategy.namespace_scoped()
            && cr.metadata.namespace.as_deref().unwrap_or("").is_empty()
        {
            cr.metadata.namespace = ctx.namespace.clone();
        }
        if cr.metadata.generation.unwrap_or(0) == 0 {
            cr.metadata.generation = Some(1);
        }
    }
}

#[async_trait]
impl BeginUpdate<CustomResource> for DecodeRepair {
    /// The update strategy compares the new object with the stored one as
    /// decoded: repaired, defaulted and pruned like the new one.
    async fn begin_update(
        &self,
        ctx: &RequestContext,
        _obj: &mut CustomResource,
        old: &mut CustomResource,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        self.repair(ctx, old);
        let api_version = old.api_version.clone();
        self.strategy.coerce(old);
        // The stored version stays what it was; only the comparison and the
        // conversion care.
        old.api_version = api_version;
        Ok(Box::new(Noop))
    }
}

/// `NewStorage`'s store (etcd.go:52-85) for one served version.
pub fn new_store(
    storage: Arc<StorageBackend>,
    crd: Arc<CustomResourceDefinition>,
    version: &str,
    status: bool,
) -> Store<CustomResource, StorageBackend> {
    let strategy = Arc::new(Strategy::new(crd.clone(), version));
    let mut store = Store::new(
        storage,
        GroupResource::new(&crd.spec.group, &crd.spec.names.plural),
        strategy.clone(),
    );
    store.storage_prefix = storage_prefix(&crd.spec.group, &crd.spec.names.plural);
    store.begin_update = Some(Arc::new(DecodeRepair {
        strategy: strategy.clone(),
    }));
    if status {
        // `statusStore := *store; statusStore.UpdateStrategy = statusStrategy`
        // (etcd.go:75-80).
        store = store.with_update_strategy(Arc::new(StatusStrategy(strategy)));
    }
    store
}

/// The REST storage behind a custom resource's endpoints (`REST` and
/// `StatusREST`, etcd.go:87-215): the Store, with what the request path owns
/// that the Store does not — conversion to the requested version, the
/// schema defaults of a read, and the strict-field checks.
pub struct CustomResourceRest {
    store: Store<CustomResource, StorageBackend>,
    strategy: Arc<Strategy>,
    strict: StrictMode,
    repair: DecodeRepair,
}

impl CustomResourceRest {
    pub fn new(
        storage: Arc<StorageBackend>,
        crd: Arc<CustomResourceDefinition>,
        version: &str,
        status: bool,
        strict: StrictMode,
    ) -> Self {
        let strategy = Arc::new(Strategy::new(crd.clone(), version));
        Self {
            store: new_store(storage, crd, version, status),
            repair: DecodeRepair {
                strategy: strategy.clone(),
            },
            strategy,
            strict,
        }
    }

    fn crd(&self) -> &CustomResourceDefinition {
        &self.strategy.crd
    }

    /// The strict-decoding errors of the bespoke handlers: unknown top-level
    /// fields, unknown fields of `metadata` (also in embedded objects), and
    /// unknown nested fields of `spec` (customresource_handler.go:1406-1470).
    fn check_strict(&self, cr: &CustomResource) -> Result<()> {
        if self.strict == StrictMode::Off {
            return Ok(());
        }
        let crd = self.crd();
        let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
        let schema = self.strategy.crd_version().and_then(|v| v.schema.as_ref());
        let schema_preserves = schema
            .map(|s| s.open_apiv3_schema.x_kubernetes_preserve_unknown_fields == Some(true))
            .unwrap_or(false);
        let preserves = crd_preserves || schema_preserves;

        if !cr.extra.is_empty() && !preserves {
            let unknown: Vec<&String> = cr.extra.keys().collect();
            return Err(Error::InvalidResource(match self.strict {
                StrictMode::Patch => format!(".{}: field not declared in schema", unknown[0]),
                _ => format!("strict decoding error: unknown field \"{}\"", unknown[0]),
            }));
        }
        if let Ok(value) = serde_json::to_value(cr) {
            if let Some(err) = check_embedded_meta(&value, "") {
                return Err(Error::InvalidResource(err));
            }
        }
        if !preserves {
            if let Some(spec_schema) = schema
                .and_then(|s| s.open_apiv3_schema.properties.as_ref())
                .and_then(|p| p.get("spec"))
            {
                if let Some(spec) = &cr.spec {
                    SchemaValidator::validate_strict(spec_schema, spec, "spec")?;
                }
            }
        }
        Ok(())
    }

    fn check_write(&self, cr: &CustomResource) -> Result<()> {
        let errs = self.strategy.type_meta_errors(cr);
        if !errs.is_empty() {
            return Err(Error::Invalid(errs));
        }
        self.check_strict(cr)
    }

    /// A stored object as the requested version serves it: repaired,
    /// defaulted on read, then converted (get.go's `transformResponseObject`
    /// over the CRD's `schemaCoercingConverter`).
    async fn serve(&self, ctx: &RequestContext, mut cr: CustomResource) -> Result<CustomResource> {
        self.repair.repair(ctx, &mut cr);
        apply_schema_defaults(self.crd(), &self.strategy.version, &mut cr);
        crate::conversion::convert_custom_resource(
            self.crd(),
            cr,
            &self.strategy.version,
            &self.store.storage,
        )
        .await
    }
}

/// An [`UpdatedObjectInfo`] whose object — a PUT's body, or a patch applied
/// to the stored object, after mutating admission — goes through the strict
/// checks before the Store sees it.
struct CheckedUpdate<'a> {
    inner: &'a dyn UpdatedObjectInfo<CustomResource>,
    rest: &'a CustomResourceRest,
}

#[async_trait]
impl UpdatedObjectInfo<CustomResource> for CheckedUpdate<'_> {
    fn preconditions(&self) -> Option<Preconditions> {
        self.inner.preconditions()
    }

    async fn updated_object(
        &self,
        ctx: &RequestContext,
        old: Option<&CustomResource>,
    ) -> Result<CustomResource> {
        let obj = self.inner.updated_object(ctx, old).await?;
        self.rest.check_write(&obj)?;
        Ok(obj)
    }
}

#[async_trait]
impl RestStorage<CustomResource> for CustomResourceRest {
    fn qualified_resource(&self) -> &GroupResource {
        &self.store.qualified_resource
    }

    fn namespace_scoped(&self) -> bool {
        self.strategy.namespace_scoped()
    }

    async fn get(&self, ctx: &RequestContext, name: &str) -> Result<CustomResource> {
        let cr = self.store.get(ctx, name).await?;
        self.serve(ctx, cr).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: CustomResource,
        create_validation: Option<&dyn ValidateObject<CustomResource>>,
        options: &CreateOptions,
    ) -> Result<CustomResource> {
        self.check_write(&obj)?;
        self.store
            .create(ctx, obj, create_validation, options)
            .await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<CustomResource>,
        create_validation: Option<&dyn ValidateObject<CustomResource>>,
        update_validation: Option<&dyn ValidateObjectUpdate<CustomResource>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(CustomResource, bool)> {
        let checked = CheckedUpdate {
            inner: obj_info,
            rest: self,
        };
        self.store
            .update(
                ctx,
                name,
                &checked,
                create_validation,
                update_validation,
                force_allow_create,
                options,
            )
            .await
    }

    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<CustomResource>>,
        options: DeleteOptions,
    ) -> Result<(Deleted<CustomResource>, bool)> {
        self.store
            .delete(ctx, name, delete_validation, options)
            .await
    }

    /// `Store.DeleteCollection` over the requested version's view of the
    /// collection: a field selector on a non-storage version selects against
    /// that version's layout, so the selectable paths are checked, then every
    /// object is defaulted and converted before the selectors apply — the
    /// pipeline of LIST.
    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<CustomResource>>,
        options: &DeleteOptions,
        list_options: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<CustomResource>> {
        if let Some(fs) = list_options.get("fieldSelector").filter(|s| !s.is_empty()) {
            validate_field_selector_paths(self.crd(), &self.strategy.version, fs)?;
        }
        let prefix =
            rusternetes_storage::build_prefix(&self.store.storage_prefix, ctx.namespace.as_deref());
        let mut items: Vec<CustomResource> =
            rusternetes_storage::Storage::list(&*self.store.storage, &prefix).await?;
        for item in &mut items {
            self.repair.repair(ctx, item);
            apply_schema_defaults(self.crd(), &self.strategy.version, item);
        }
        let mut items = crate::conversion::convert_custom_resources(
            self.crd(),
            items,
            &self.strategy.version,
            &self.store.storage,
        )
        .await?;
        crate::handlers::filtering::apply_selectors(&mut items, list_options)?;
        self.store
            .delete_collection(ctx, items, delete_validation, options)
            .await
    }
}
