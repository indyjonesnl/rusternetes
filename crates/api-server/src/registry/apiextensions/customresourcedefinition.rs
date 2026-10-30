//! CustomResourceDefinition strategies and storage — port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition/strategy.go`
//! and `.../etcd.go`, with the validation of
//! `pkg/apis/apiextensions/validation/validation.go` (in
//! `rusternetes_common::validation::crd`) and the controllers of
//! [`super::controllers`].
//!
//! Not modelled:
//! - The deprecated top-level `spec.validation` schema that
//!   `getUnrecognizedFormatsInCRD` (strategy.go:203-206) also checks:
//!   `CustomResourceDefinitionSpec` is v1-only and has no such field.
//! - `dropDisabledFields` (strategy.go:322-341): every gate it consults is
//!   GA or default-on except `CRDObservedGenerationTracking`, whose fields
//!   [`CustomResourceDefinitionStatus`] does not have.
//! - The structural-schema half of `validateCustomResourceDefinitionSpec`
//!   and CEL cost estimation: `handlers::cel_validation` keeps its own,
//!   weaker, rule checks, reported here as field errors.
//! - `ShortNames` and
//!   `Categories` of `REST` (discovery), table conversion.
//! - `StatusREST.Update`'s `forceAllowCreate=false` is the status strategy's
//!   `AllowCreateOnUpdate`.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::resources::{
    CustomResourceDefinition, JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
    JSONSchemaPropsOrStringArray,
};
use rusternetes_common::validation::crd::{
    is_crd_condition_true, validate_custom_resource_definition,
    validate_custom_resource_definition_update, validate_update_custom_resource_definition_status,
    CUSTOM_RESOURCE_CLEANUP_FINALIZER,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_prefix, Storage, StorageBackend};
use tracing::warn;

use super::controllers::{
    self, condition, crd_has_finalizer, crd_remove_finalizer, set_crd_condition, ESTABLISHED,
    TERMINATING,
};
use super::finalizer_queue;
use crate::registry::generic::{CreateOptions, Deleted, Store, UpdateOptions};
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject, ValidateObjectUpdate,
};
use crate::ssa::ResetFields;

/// `SetDefaults_CustomResourceDefinition` (apiextensions/v1/defaults.go:30-40),
/// run by the codec on every decode.
pub fn convert_to_internal(crd: &mut CustomResourceDefinition) {
    rusternetes_common::validation::crd::set_defaults_custom_resource_definition(crd);
}

/// `IsStoredVersion` (helpers.go:145-152).
fn is_stored_version(crd: &CustomResourceDefinition, version: &str) -> bool {
    crd.status
        .as_ref()
        .and_then(|s| s.stored_versions.as_ref())
        .is_some_and(|v| v.iter().any(|s| s == version))
}

/// The storage-version step both `PrepareForCreate` (strategy.go:77-84) and
/// `PrepareForUpdate` (strategy.go:105-112) run: the storage version is
/// recorded in `status.storedVersions`.
fn record_storage_version(crd: &mut CustomResourceDefinition) {
    let Some(version) = crd.spec.versions.iter().find(|v| v.storage) else {
        return;
    };
    let name = version.name.clone();
    if !is_stored_version(crd, &name) {
        crd.status
            .get_or_insert_with(Default::default)
            .stored_versions
            .get_or_insert_with(Vec::new)
            .push(name);
    }
}

/// The CEL rule checks of `handlers::cel_validation`, as field errors under
/// the schema they were found in.
fn schema_rule_errors(
    crd: &CustomResourceDefinition,
    old: Option<&CustomResourceDefinition>,
) -> ErrorList {
    schema_rule_errors_in(
        crd,
        old,
        &rusternetes_common::cel_versions::EnvSet::must_base_env_set(
            rusternetes_common::cel_versions::default_compatibility_version(),
        ),
    )
}

/// [`schema_rule_errors`] against an explicit environment set.
pub fn schema_rule_errors_in(
    crd: &CustomResourceDefinition,
    old: Option<&CustomResourceDefinition>,
    env_set: &rusternetes_common::cel_versions::EnvSet,
) -> ErrorList {
    // `findPreexistingExpressions(&oldObj.Spec)` (validation.go:241); empty
    // on create (validation.go:153).
    let preexisting = old
        .map(|o| crate::handlers::cel_validation::find_preexisting_expressions(&o.spec))
        .unwrap_or_default();
    // `findVersionsWithUnchangedSchemas(obj, oldObj)` (validation.go:242);
    // empty on create.
    let unchanged = old
        .map(|o| versions_with_unchanged_schemas(crd, o))
        .unwrap_or_default();
    let mut errs = Vec::new();
    for (i, version) in crd.spec.versions.iter().enumerate() {
        let Some(schema) = version
            .schema
            .as_ref()
            .and_then(|s| s.open_apiv3_schema.as_ref())
        else {
            continue;
        };
        errs.extend(crate::handlers::cel_validation::validate_crd_schema_rules(
            schema,
            &Path::new("spec")
                .child("versions")
                .index(i)
                .child("schema")
                .child("openAPIV3Schema"),
            env_set,
            &preexisting,
            // `suppressExpressionCostForUnchangedSchema` (validation.go:219-226).
            unchanged.contains(&version.name),
        ));
    }
    errs
}

/// `findVersionsWithUnchangedSchemas` (apiextensions validation.go:197-217):
/// each version of `obj` whose schema equals the one the old object has for
/// the same version name. (`GetSchemaForVersion`'s fallback to the
/// deprecated top-level `spec.validation` is v1-only and not modelled.)
fn versions_with_unchanged_schemas(
    obj: &CustomResourceDefinition,
    old: &CustomResourceDefinition,
) -> std::collections::HashSet<String> {
    let schema_of = |crd: &CustomResourceDefinition, name: &str| {
        crd.spec
            .versions
            .iter()
            .find(|v| v.name == name)
            .map(|v| serde_json::to_value(&v.schema).ok())
    };
    obj.spec
        .versions
        .iter()
        .filter(|v| {
            matches!((schema_of(obj, &v.name), schema_of(old, &v.name)),
                (Some(a), Some(b)) if a == b)
        })
        .map(|v| v.name.clone())
        .collect()
}

/// `supportedVersionedFormats` (apiserver/validation/formats.go:32-71) as
/// recognised at `DefaultCompatibilityVersion()`.
///
/// That version is `EffectiveVersion.MinCompatibilityVersion()`
/// (apiserver/pkg/cel/environment/base.go:52-58), i.e. one minor below the
/// 1.35 target: 1.34, which includes both the 1.0 set and the 1.34 additions
/// (`k8s-short-name`, `k8s-long-name`). Names are stored normalised
/// (`-` removed, formats.go:85, 141-147).
const RECOGNIZED_FORMATS: &[&str] = &[
    "bsonobjectid",
    "uri",
    "email",
    "hostname",
    "ipv4",
    "ipv6",
    "cidr",
    "mac",
    "uuid",
    "uuid3",
    "uuid4",
    "uuid5",
    "isbn",
    "isbn10",
    "isbn13",
    "creditcard",
    "ssn",
    "hexcolor",
    "rgbcolor",
    "byte",
    "password",
    "date",
    "duration",
    "datetime",
    "k8sshortname",
    "k8slongname",
];

/// `GetUnrecognizedFormats` (formats.go:104-119) for one schema node: only a
/// `type: string` schema with a non-empty format outside the recognised set
/// is reported.
fn unrecognized_format(s: &JSONSchemaProps) -> Option<&str> {
    let format = s.format.as_deref().filter(|f| !f.is_empty())?;
    if s.type_.as_deref() != Some("string") {
        return None;
    }
    let normalized = format.replace('-', "");
    (!RECOGNIZED_FORMATS.contains(&normalized.as_str())).then_some(format)
}

/// The traversal of `SchemaHas` (validation.go:1659-1728) with the predicate
/// of `getUnrecognizedFormatsInSchema` (strategy.go:219-235), which never
/// stops the walk: collects every unrecognized format below `s`.
fn collect_unrecognized_formats<'a>(s: &'a JSONSchemaProps, out: &mut Vec<&'a str>) {
    if let Some(f) = unrecognized_format(s) {
        out.push(f);
    }
    match s.items.as_deref() {
        Some(JSONSchemaPropsOrArray::Schema(i)) => collect_unrecognized_formats(i, out),
        Some(JSONSchemaPropsOrArray::Schemas(is)) => {
            is.iter().for_each(|i| collect_unrecognized_formats(i, out))
        }
        None => {}
    }
    for list in [&s.all_of, &s.any_of, &s.one_of].into_iter().flatten() {
        list.iter()
            .for_each(|i| collect_unrecognized_formats(i, out));
    }
    if let Some(n) = &s.not {
        collect_unrecognized_formats(n, out);
    }
    for map in [&s.properties, &s.pattern_properties, &s.definitions]
        .into_iter()
        .flatten()
    {
        map.values()
            .for_each(|i| collect_unrecognized_formats(i, out));
    }
    for or_bool in [&s.additional_properties, &s.additional_items]
        .into_iter()
        .flatten()
    {
        if let JSONSchemaPropsOrBool::Schema(i) = or_bool.as_ref() {
            collect_unrecognized_formats(i, out);
        }
    }
    for d in s.dependencies.iter().flat_map(|m| m.values()) {
        if let JSONSchemaPropsOrStringArray::Schema(i) = d {
            collect_unrecognized_formats(i, out);
        }
    }
}

/// `getUnrecognizedFormatsInCRD` (strategy.go:199-216), per-version schemas.
fn unrecognized_formats_in_crd(crd: &CustomResourceDefinition) -> Vec<String> {
    let mut out = Vec::new();
    for v in &crd.spec.versions {
        if let Some(schema) = v.schema.as_ref().and_then(|s| s.open_apiv3_schema.as_ref()) {
            collect_unrecognized_formats(schema, &mut out);
        }
    }
    out.into_iter().map(str::to_string).collect()
}

fn unrecognized_format_warning(format: &str) -> String {
    format!("unrecognized format {format:?}")
}

/// `strategy` (strategy.go:42-48).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:54-56.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<CustomResourceDefinition> for Strategy {
    /// `PrepareForCreate` (strategy.go:74-89): status is cleared, the
    /// generation starts at 1 and the storage version is recorded.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut CustomResourceDefinition) {
        obj.status = Some(Default::default());
        obj.metadata.generation = Some(1);
        record_storage_version(obj);
    }

    /// `Validate` (strategy.go:120-122).
    fn validate(&self, _ctx: &RequestContext, obj: &CustomResourceDefinition) -> ErrorList {
        let mut errs = validate_custom_resource_definition(obj);
        errs.extend(schema_rule_errors(obj, None));
        errs
    }

    /// `WarningsOnCreate` (strategy.go:125-142).
    fn warnings_on_create(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResourceDefinition,
    ) -> Vec<String> {
        unrecognized_formats_in_crd(obj)
            .iter()
            .map(|f| unrecognized_format_warning(f))
            .collect()
    }
}

impl RestUpdateStrategy<CustomResourceDefinition> for Strategy {
    /// strategy.go:146-148.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:91-118): the status is the stored
    /// one, any change to the spec bumps the generation, and the storage
    /// version is recorded.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CustomResourceDefinition,
        old: &CustomResourceDefinition,
    ) {
        obj.status = old.status.clone();
        if serde_json::to_value(&obj.spec).ok() != serde_json::to_value(&old.spec).ok() {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
        record_storage_version(obj);
    }

    /// `ValidateUpdate` (strategy.go:160-162).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResourceDefinition,
        old: &CustomResourceDefinition,
    ) -> ErrorList {
        let mut errs = validate_custom_resource_definition_update(obj, old);
        errs.extend(schema_rule_errors(obj, Some(old)));
        errs
    }

    /// `WarningsOnUpdate` (strategy.go:165-197): only formats the old object
    /// did not already carry warn (ratcheting).
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResourceDefinition,
        old: &CustomResourceDefinition,
    ) -> Vec<String> {
        let old_formats: std::collections::HashSet<String> =
            unrecognized_formats_in_crd(old).into_iter().collect();
        unrecognized_formats_in_crd(obj)
            .iter()
            .filter(|f| !old_formats.contains(*f))
            .map(|f| unrecognized_format_warning(f))
            .collect()
    }

    /// strategy.go:151-153.
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (customresourcedefinition/strategy.go:60-71): `status`
    /// for v1 and v1beta1.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new()
            .with("apiextensions.k8s.io/v1", &[&["status"]])
            .with("apiextensions.k8s.io/v1beta1", &[&["status"]])
    }
}

/// CustomResourceDefinitions use the default delete strategy.
impl RestDeleteStrategy<CustomResourceDefinition> for Strategy {}

/// `statusStrategy` (strategy.go:237-295): the update strategy of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<CustomResourceDefinition> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:267-274): only the status may change.
    ///
    /// Upstream follows the spec reset with
    /// `ResetObjectMetaForStatus(&newObj.ObjectMeta, &newObj.ObjectMeta)` —
    /// the new metadata against itself, a no-op — so a status write keeps the
    /// metadata the client sent. The CRD finalizer relies on it: it drops the
    /// cleanup finalizer through `UpdateStatus` (crd_finalizer.go:176).
    /// Ported as written.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CustomResourceDefinition,
        old: &CustomResourceDefinition,
    ) {
        obj.spec = old.spec.clone();
    }

    /// `ValidateUpdate` (strategy.go:287-289).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CustomResourceDefinition,
        old: &CustomResourceDefinition,
    ) -> ErrorList {
        validate_update_custom_resource_definition_status(obj, old)
    }

    /// strategy.go:280-282.
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (customresourcedefinition/strategy.go:252-265):
    /// `metadata` and `spec` for v1 and v1beta1.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new()
            .with("apiextensions.k8s.io/v1", &[&["metadata"], &["spec"]])
            .with("apiextensions.k8s.io/v1beta1", &[&["metadata"], &["spec"]])
    }
}

/// `NewREST`'s store (etcd.go:41-64).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<CustomResourceDefinition, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apiextensions.k8s.io", "customresourcedefinitions"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store (etcd.go:179-187).
pub fn new_status_store(
    storage: Arc<StorageBackend>,
) -> Store<CustomResourceDefinition, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// `StatusREST` (etcd.go:179-187) with the informer's update event: a write
/// through `/status` queues the CRD for the controllers
/// (`UpdateFunc` -> `queue.Add(key)`, naming_controller.go:359-363,
/// establishing_controller.go:71-73), as one through [`CrdRest`] does.
pub struct CrdStatusRest {
    store: Store<CustomResourceDefinition, StorageBackend>,
    controllers: Arc<super::controller_queue::CrdControllers>,
}

impl CrdStatusRest {
    pub fn with_controllers(
        storage: Arc<StorageBackend>,
        controllers: Arc<super::controller_queue::CrdControllers>,
    ) -> Self {
        Self {
            store: new_status_store(storage),
            controllers,
        }
    }
}

/// The informer's event handlers of the CRD finalizer: `AddFunc` for a
/// create (`old` is `None`), `UpdateFunc` otherwise
/// (crd_finalizer.go:332-366); a CRD they accept is queued on the
/// finalizer's workqueue, whose workers are started on first use.
fn finalizer_event(
    controllers: &Arc<super::controller_queue::CrdControllers>,
    storage: &Arc<StorageBackend>,
    old: Option<&CustomResourceDefinition>,
    new: &CustomResourceDefinition,
) {
    // A write that changed nothing makes no new revision, so no event (the
    // informer only sees revisions); the same revision is the resync, which
    // `resync_one` delivers itself.
    if old.is_some_and(|o| o.metadata.resource_version == new.metadata.resource_version) {
        return;
    }
    let enqueue = match old {
        None => finalizer_queue::add_event_enqueues(new),
        Some(old) => finalizer_queue::update_event_enqueues(old, new),
    };
    if enqueue {
        controllers.ensure_started(storage.clone());
        controllers.finalizer.add(&new.metadata.name);
    }
}

/// An update whose new object is computed from the stored one — a controller
/// writing through `UpdateStatus`.
struct ControllerUpdate<'a> {
    compute: &'a (dyn Fn(&CustomResourceDefinition) -> CustomResourceDefinition + Send + Sync),
    /// The stored object the last attempt was computed from: the informer's
    /// `oldObj` for the write's event.
    old: std::sync::Mutex<Option<CustomResourceDefinition>>,
}

#[async_trait]
impl UpdatedObjectInfo<CustomResourceDefinition> for ControllerUpdate<'_> {
    fn preconditions(&self) -> Option<Preconditions> {
        None
    }

    async fn updated_object(
        &self,
        _ctx: &RequestContext,
        old: Option<&CustomResourceDefinition>,
    ) -> Result<CustomResourceDefinition> {
        let old = old.ok_or_else(|| Error::NotFound("customresourcedefinition".to_string()))?;
        *self.old.lock().unwrap_or_else(|e| e.into_inner()) = Some(old.clone());
        Ok((self.compute)(old))
    }
}

/// An update that remembers the stored object it was applied to: the
/// informer's `oldObj` for the event the write causes.
struct RecordOld<'a> {
    inner: &'a dyn UpdatedObjectInfo<CustomResourceDefinition>,
    old: std::sync::Mutex<Option<CustomResourceDefinition>>,
}

impl<'a> RecordOld<'a> {
    fn new(inner: &'a dyn UpdatedObjectInfo<CustomResourceDefinition>) -> Self {
        Self {
            inner,
            old: std::sync::Mutex::new(None),
        }
    }

    fn take(&self) -> Option<CustomResourceDefinition> {
        self.old.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

#[async_trait]
impl UpdatedObjectInfo<CustomResourceDefinition> for RecordOld<'_> {
    fn preconditions(&self) -> Option<Preconditions> {
        self.inner.preconditions()
    }

    async fn updated_object(
        &self,
        ctx: &RequestContext,
        old: Option<&CustomResourceDefinition>,
    ) -> Result<CustomResourceDefinition> {
        *self.old.lock().unwrap_or_else(|e| e.into_inner()) = old.cloned();
        self.inner.updated_object(ctx, old).await
    }
}

/// `REST` (etcd.go:36-38, 84-176): the Store, with a `Delete` that starts the
/// CRD's termination, and the controllers that act on every write.
pub struct CrdRest {
    store: Store<CustomResourceDefinition, StorageBackend>,
    status_store: Store<CustomResourceDefinition, StorageBackend>,
    /// The controllers' workqueue and the condition controllers' memos.
    controllers: Arc<super::controller_queue::CrdControllers>,
}

impl CrdRest {
    /// The Store over `storage`, with the controllers it feeds.
    pub fn with_controllers(
        storage: Arc<StorageBackend>,
        controllers: Arc<super::controller_queue::CrdControllers>,
    ) -> Self {
        Self {
            store: new_store(storage.clone()),
            status_store: new_status_store(storage),
            controllers,
        }
    }

    /// The informer's event handler: queue the CRD for the naming and
    /// establishing controllers (naming_controller.go:343-363), starting their worker on first use.
    fn enqueue(&self, name: &str) {
        self.controllers.ensure_started(self.store.storage.clone());
        self.controllers.add(name);
    }

    /// The nonstructuralschema and apiapproval controllers' `AddFunc` /
    /// `UpdateFunc`: `queue.Add(key)` on each of their own queues.
    fn enqueue_conditions(&self, name: &str) {
        self.controllers.ensure_started(self.store.storage.clone());
        self.controllers.add_conditions(name);
    }

    fn finalizer_event(
        &self,
        old: Option<&CustomResourceDefinition>,
        new: &CustomResourceDefinition,
    ) {
        finalizer_event(&self.controllers, &self.store.storage, old, new);
    }

    /// `sync` (crd_finalizer.go:112-179) for one key off the finalizer's
    /// queue. A CRD it removed is a `DeleteFunc` for the other controllers
    /// (naming_controller.go:365-379): its names are free again.
    pub(super) async fn process_finalizer_key(&self, name: &str) -> Result<()> {
        self.finalize(name).await?;
        if self.get_crd(name).await?.is_none() {
            self.controllers.conditions.forget(name);
            self.enqueue(name);
        }
        Ok(())
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    /// `UpdateStatus` as a controller issues it: the status store, with
    /// `compute` applied to whatever is stored at the time of each attempt.
    /// A CRD that is gone or changed in the meantime is the controller's
    /// "we'll get called again" (naming_controller.go:278,
    /// establishing_controller.go:163).
    async fn update_status(
        &self,
        name: &str,
        compute: &(dyn Fn(&CustomResourceDefinition) -> CustomResourceDefinition + Send + Sync),
    ) -> Result<Option<CustomResourceDefinition>> {
        let update = ControllerUpdate {
            compute,
            old: std::sync::Mutex::new(None),
        };
        let result = self
            .status_store
            .update(
                &Self::ctx(),
                name,
                &update,
                None,
                None,
                false,
                &UpdateOptions::default(),
            )
            .await;
        match result {
            Ok((out, _)) => {
                // The write is an informer event for the finalizer too (its
                // own `UpdateStatus` calls included).
                let old = update.old.lock().unwrap_or_else(|e| e.into_inner()).take();
                self.finalizer_event(old.as_ref(), &out);
                Ok(Some(out))
            }
            Err(Error::NotFound(_)) | Err(Error::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn group_crds(&self, group: &str) -> Result<Vec<CustomResourceDefinition>> {
        let prefix = build_prefix(&self.store.storage_prefix, None);
        Ok(self
            .store
            .storage
            .list::<CustomResourceDefinition>(&prefix)
            .await?
            .into_iter()
            .map(|mut c| {
                convert_to_internal(&mut c);
                c
            })
            .filter(|c| c.spec.group == group)
            .collect())
    }

    pub async fn get_crd(&self, name: &str) -> Result<Option<CustomResourceDefinition>> {
        match self
            .store
            .get(
                &Self::ctx(),
                name,
                &crate::registry::generic::GetOptions::default(),
            )
            .await
        {
            Ok(crd) => Ok(Some(crd)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// The naming controller's `sync` then the establishing controller's, for
    /// one CRD.
    async fn sync_one(&self, name: &str) -> Result<bool> {
        let Some(crd) = self.get_crd(name).await? else {
            return Ok(false);
        };
        let group = self.group_crds(&crd.spec.group).await?;
        let names_changed = self
            .update_status(name, &|old| {
                controllers::sync_names(old, &group).unwrap_or_else(|| old.clone())
            })
            .await?
            .is_some_and(|out| out != crd);
        self.update_status(name, &|old| {
            controllers::sync_establishing(old).unwrap_or_else(|| old.clone())
        })
        .await?;
        Ok(names_changed)
    }

    /// The nonstructuralschema controller's `sync`
    /// (nonstructuralschema_controller.go:135-188) for one key off its queue.
    pub(super) async fn sync_non_structural(&self, name: &str) -> Result<()> {
        let Some(crd) = self.get_crd(name).await? else {
            return Ok(());
        };
        let conditions = &self.controllers.conditions;
        if conditions.sync_non_structural(&crd).is_none() {
            return Ok(());
        }
        let written = self
            .update_status(name, &|old| {
                conditions
                    .sync_non_structural(old)
                    .unwrap_or_else(|| old.clone())
            })
            .await?;
        if let Some(out) = written {
            conditions.record_non_structural(&out);
        }
        Ok(())
    }

    /// The apiapproval controller's `sync` (apiapproval_controller.go:130-173)
    /// for one key off its queue.
    pub(super) async fn sync_api_approval(&self, name: &str) -> Result<()> {
        let Some(crd) = self.get_crd(name).await? else {
            return Ok(());
        };
        let conditions = &self.controllers.conditions;
        if conditions.sync_api_approval(&crd).is_none() {
            return Ok(());
        }
        let written = self
            .update_status(name, &|old| {
                conditions
                    .sync_api_approval(old)
                    .unwrap_or_else(|| old.clone())
            })
            .await?;
        if let Some(out) = written {
            conditions.record_api_approval(&out);
        }
        Ok(())
    }

    /// `requeueAllOtherGroupCRDs` (naming_controller.go:383-401): a CRD that
    /// changed or freed its names may unblock the others of its group.
    async fn sync_group_except(&self, group: &str, except: &str) -> Result<()> {
        for other in self.group_crds(group).await? {
            if other.metadata.name != except {
                self.enqueue(&other.metadata.name);
            }
        }
        Ok(())
    }

    /// One key off the controllers' queue: the naming controller's `sync`,
    /// the establishing controller's and the condition controllers'. A CRD
    /// that is gone has freed its names, so its group is reconsidered
    /// (naming_controller.go:241-249).
    pub(super) async fn process_one(&self, name: &str) -> Result<()> {
        let Some(crd) = self.get_crd(name).await? else {
            if let Some((_, group)) = name.split_once('.') {
                self.sync_group_except(group, name).await?;
            }
            return Ok(());
        };
        if self.sync_one(name).await? {
            self.sync_group_except(&crd.spec.group, name).await?;
        }
        Ok(())
    }

    /// One CRD as the informer-driven controllers see it on a resync: the
    /// naming and establishing controllers' `sync` (their `UpdateFunc`
    /// enqueues every update, resyncs included - naming_controller.go:359-363),
    /// then the finalizer's event, which queues a CRD that is being deleted
    /// and still holds the cleanup finalizer (crd_finalizer.go:341-352).
    pub async fn resync_one(&self, name: &str) -> Result<()> {
        let Some(crd) = self.get_crd(name).await? else {
            return Ok(());
        };
        if self.sync_one(name).await? {
            self.sync_group_except(&crd.spec.group, name).await?;
        }
        // Their `UpdateFunc` enqueues a resync on the nonstructuralschema
        // and apiapproval controllers' queues too.
        self.enqueue_conditions(name);
        // The finalizer's `UpdateFunc` for a resync, `oldObj` being `newObj`
        // (crd_finalizer.go:348-352): "always requeue resyncs just in case".
        if let Some(crd) = self.get_crd(name).await? {
            if finalizer_queue::update_event_enqueues(&crd, &crd) {
                self.controllers.ensure_started(self.store.storage.clone());
                self.controllers.finalizer.add(name);
            }
        }
        Ok(())
    }

    /// The names of every CRD, as the informer's initial list and its
    /// periodic resync deliver them.
    async fn all_names(&self) -> Result<Vec<String>> {
        let prefix = build_prefix(&self.store.storage_prefix, None);
        Ok(self
            .store
            .storage
            .list::<CustomResourceDefinition>(&prefix)
            .await?
            .into_iter()
            .map(|c| c.metadata.name)
            .collect())
    }

    /// A resync of every CRD (`NewSharedInformerFactory(crdClient,
    /// 5*time.Minute)`, apiserver.go:170): the names whose sync failed, which
    /// the caller requeues with backoff (crd_finalizer.go:296-308).
    pub async fn resync(&self) -> Vec<String> {
        let names = match self.all_names().await {
            Ok(names) => names,
            Err(e) => {
                warn!("customresourcedefinitions: could not list for resync: {e}");
                return Vec::new();
            }
        };
        run_workers(names, CRD_FINALIZER_WORKERS, |name| async move {
            match self.resync_one(&name).await {
                Ok(()) => None,
                Err(e) => {
                    warn!("customresourcedefinition {name}: resync failed: {e}");
                    Some(name)
                }
            }
        })
        .await
        .into_iter()
        .flatten()
        .collect()
    }

    /// The stored instances of the CRD as `(namespace, name)`: upstream's
    /// `crClient.List(ctx, nil)` (crd_finalizer.go:195, :255). A stored
    /// instance carries its `metadata.namespace`; one without is listed with
    /// none, as upstream's `GetNamespace()` is empty.
    async fn list_instances(
        &self,
        crd: &CustomResourceDefinition,
    ) -> Result<Vec<(Option<String>, String)>> {
        let resource_type = instance_resource_type(crd);
        let items: Vec<serde_json::Value> = self
            .store
            .storage
            .list(&build_prefix(&resource_type, None))
            .await?;
        Ok(items
            .iter()
            .filter_map(|item| {
                let name = item.pointer("/metadata/name")?.as_str()?.to_string();
                // `metadata.GetNamespace()`; empty means no namespace.
                let namespace = item
                    .pointer("/metadata/namespace")
                    .and_then(|v| v.as_str())
                    .filter(|n| !n.is_empty())
                    .map(str::to_string);
                Some((namespace, name))
            })
            .collect())
    }

    /// `deleteInstances` (crd_finalizer.go:181-273): the stored instances of
    /// the CRD are deleted through its instance store, one `DeleteCollection`
    /// per namespace (:204-229; an instance with a finalizer is only marked
    /// for deletion); then wait until a list of them comes back empty
    /// (:253-272). The `Err` carries the
    /// `Terminating` condition upstream returns with its error.
    async fn delete_instances(
        &self,
        crd: &CustomResourceDefinition,
    ) -> std::result::Result<
        (),
        Box<(
            rusternetes_common::resources::CustomResourceDefinitionCondition,
            Error,
        )>,
    > {
        let failed = |reason: &str, message: String, e: Error| {
            Box::new((condition(TERMINATING, "True", reason, &message), e))
        };
        let items = self.list_instances(crd).await.map_err(|e| {
            failed(
                "InstanceDeletionFailed",
                format!("could not list instances: {e}"),
                e,
            )
        })?;
        // `GetCustomResourceListerCollectionDeleter(crd)` (crd_finalizer.go:
        // 183): the instance store of the CRD, so a delete honours the
        // instance's finalizers and graceful deletion like a request would.
        let storage_version = crd
            .spec
            .versions
            .iter()
            .find(|v| v.storage)
            .map(|v| v.name.clone())
            .unwrap_or_default();
        let instances = super::customresource::new_store(
            self.store.storage.clone(),
            Arc::new(crd.clone()),
            &storage_version,
            false,
        );
        // `DeleteCollection(nsCtx, ValidateAllObjectFunc, nil, nil)`: nil
        // options, not `DeleteOptions::default()` (which is the API's 30s
        // grace and background propagation).
        let no_options = DeleteOptions {
            propagation_policy: None,
            grace_period_seconds: None,
            preconditions: None,
            orphan_dependents: None,
            dry_run: None,
            ignore_store_read_error_with_cluster_breaking_potential: None,
        };
        issue_delete_collections(&items, |ctx| {
            let instances = &instances;
            let no_options = &no_options;
            async move {
                // `DeleteCollection(nsCtx, rest.ValidateAllObjectFunc, nil, nil)`.
                RestStorage::delete_collection(
                    instances,
                    &ctx,
                    None,
                    no_options,
                    &Default::default(),
                )
                .await
                .map(|_| ())
            }
        })
        .await
        .map_err(|msg| {
            failed(
                "InstanceDeletionFailed",
                format!("could not issue all deletes: {msg}"),
                Error::Internal(msg),
            )
        })?;
        poll_until_gone(INSTANCE_DRAIN_INTERVAL, INSTANCE_DRAIN_TIMEOUT, || async {
            Ok(self.list_instances(crd).await?.len())
        })
        .await
        .map_err(|msg| {
            failed(
                "InstanceDeletionCheck",
                format!("could not confirm zero CustomResources remaining: {msg}"),
                Error::Internal(msg),
            )
        })
    }

    /// `CRDFinalizer.sync` (crd_finalizer.go:112-179).
    async fn finalize(&self, name: &str) -> Result<()> {
        let Some(crd) = self.get_crd(name).await? else {
            return Ok(());
        };
        // No work to do.
        if crd.metadata.deletion_timestamp.is_none()
            || !crd_has_finalizer(&crd, CUSTOM_RESOURCE_CLEANUP_FINALIZER)
        {
            return Ok(());
        }

        // Update the status condition. This cleanup could take a while.
        let Some(crd) = self
            .update_status(name, &|old| {
                let mut c = old.clone();
                set_crd_condition(
                    &mut c,
                    condition(
                        TERMINATING,
                        "True",
                        "InstanceDeletionInProgress",
                        "CustomResource deletion is in progress",
                    ),
                );
                c
            })
            .await?
        else {
            return Ok(());
        };

        // Now we can start deleting items. No need to delete if not
        // established.
        // "Skip deletion, explain why, and proceed to remove the finalizer and
        // delete the CRD" (crd_finalizer.go:140-146).
        let terminating = if finalizer_queue::overlapping_built_in_resource(
            &crd.spec.group,
            &crd.spec.names.plural,
        ) {
            condition(
                TERMINATING,
                "False",
                "OverlappingBuiltInResource",
                "instances overlap with built-in resources in storage",
            )
        } else if is_crd_condition_true(&crd, ESTABLISHED) {
            match self.delete_instances(&crd).await {
                Ok(()) => condition(
                    TERMINATING,
                    "False",
                    "InstanceDeletionCompleted",
                    "removed all instances",
                ),
                Err(boxed) => {
                    let (failed, e) = *boxed;
                    self.update_status(name, &|old| {
                        let mut c = old.clone();
                        set_crd_condition(&mut c, failed.clone());
                        c
                    })
                    .await?;
                    return Err(e);
                }
            }
        } else {
            condition(
                TERMINATING,
                "False",
                "NeverEstablished",
                "resource was never established",
            )
        };

        self.update_status(name, &|old| {
            let mut c = old.clone();
            set_crd_condition(&mut c, terminating.clone());
            crd_remove_finalizer(&mut c, CUSTOM_RESOURCE_CLEANUP_FINALIZER);
            c
        })
        .await?;
        Ok(())
    }
}

/// The `DeleteCollection` loop of `deleteInstances` (crd_finalizer.go:
/// 204-229): one call per namespace found among the listed instances
/// ("don't retry deleting the same namespace", :216-219), a failure noted and
/// the loop continued (:222-225), the failures returned as
/// `utilerrors.NewAggregate(deleteErrors)` (:227), whose `Error()`
/// (apimachinery/pkg/util/errors/errors.go:70-96) is a lone message bare and
/// several as `[a, b]`, a repeated message once.
pub async fn issue_delete_collections<F, Fut>(
    items: &[(Option<String>, String)],
    mut delete_collection: F,
) -> std::result::Result<(), String>
where
    F: FnMut(RequestContext) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut deleted_namespaces = std::collections::HashSet::new();
    let mut messages: Vec<String> = Vec::new();
    for (namespace, _) in items {
        if !deleted_namespaces.insert(namespace.clone()) {
            continue;
        }
        if let Err(e) = delete_collection(RequestContext::new(namespace.as_deref())).await {
            messages.push(e.to_string());
        }
    }
    let mut seen = std::collections::HashSet::new();
    let unique: Vec<&String> = messages.iter().filter(|m| seen.insert(*m)).collect();
    match unique.len() {
        0 => Ok(()),
        1 => Err(unique[0].clone()),
        _ => Err(format!(
            "[{}]",
            unique
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// The storage resource type of a CRD's instances.
fn instance_resource_type(crd: &CustomResourceDefinition) -> String {
    format!(
        "{}_{}",
        crd.spec.group.replace('.', "_"),
        crd.spec.names.plural
    )
}

/// `wait.PollUntilContextTimeout(ctx, 5*time.Second, 1*time.Minute, true, ..)`
/// (crd_finalizer.go:253).
pub const INSTANCE_DRAIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
pub const INSTANCE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The finalizer's wait "until all the resources are deleted"
/// (crd_finalizer.go:253-267): `remaining` is tried at once (`immediate`
/// true), then every `interval`, until it reports zero; a list error ends the
/// wait with that error, and `timeout` ends it with the
/// `context deadline exceeded` of `PollUntilContextTimeout`.
pub async fn poll_until_gone<F, Fut>(
    interval: std::time::Duration,
    timeout: std::time::Duration,
    mut remaining: F,
) -> std::result::Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<usize>>,
{
    let poll = async {
        loop {
            match remaining().await {
                Err(e) => return Err(e.to_string()),
                Ok(0) => return Ok(()),
                Ok(n) => tracing::debug!("waiting for {n} items to be removed"),
            }
            tokio::time::sleep(interval).await;
        }
    };
    match tokio::time::timeout(timeout, poll).await {
        Ok(r) => r,
        Err(_) => Err("context deadline exceeded".to_string()),
    }
}

/// The first-delete mutation of `REST.Delete` (etcd.go:127-148): the
/// deletion is stamped, the cleanup finalizer added and the CRD marked
/// `Terminating`.
pub fn start_deletion(crd: &mut CustomResourceDefinition) {
    if crd.metadata.deletion_timestamp.is_none() {
        crd.metadata.deletion_timestamp = Some(chrono::Utc::now());
    }
    if !crd_has_finalizer(crd, CUSTOM_RESOURCE_CLEANUP_FINALIZER) {
        crd.metadata
            .finalizers
            .get_or_insert_with(Vec::new)
            .push(CUSTOM_RESOURCE_CLEANUP_FINALIZER.to_string());
    }
    set_crd_condition(
        crd,
        condition(
            TERMINATING,
            "True",
            "InstanceDeletionPending",
            "CustomResourceDefinition marked for deletion; CustomResource deletion will begin soon",
        ),
    );
}

#[async_trait]
impl RestStorage<CustomResourceDefinition> for CrdRest {
    fn qualified_resource(&self) -> &GroupResource {
        self.store.qualified_resource()
    }

    fn namespace_scoped(&self) -> bool {
        RestStorage::namespace_scoped(&self.store)
    }

    /// The REST embeds `*genericregistry.Store`, so `GetResetFields` is promoted
    /// from it (store.go:1698-1703).
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        RestStorage::get_reset_fields(&self.store)
    }

    async fn get(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: &crate::registry::generic::GetOptions,
    ) -> Result<CustomResourceDefinition> {
        RestStorage::get(&self.store, ctx, name, options).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: CustomResourceDefinition,
        create_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        options: &CreateOptions,
    ) -> Result<CustomResourceDefinition> {
        let created =
            RestStorage::create(&self.store, ctx, obj, create_validation, options).await?;
        if options.dry_run {
            return Ok(created);
        }
        // The create event wakes the controllers; the response is the object
        // as written, without their conditions.
        self.enqueue(&created.metadata.name);
        self.enqueue_conditions(&created.metadata.name);
        self.finalizer_event(None, &created);
        Ok(created)
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<CustomResourceDefinition>,
        create_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        update_validation: Option<&dyn ValidateObjectUpdate<CustomResourceDefinition>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(CustomResourceDefinition, bool)> {
        let recorded = RecordOld::new(obj_info);
        let (updated, created) = RestStorage::update(
            &self.store,
            ctx,
            name,
            &recorded,
            create_validation,
            update_validation,
            force_allow_create,
            options,
        )
        .await?;
        if options.dry_run {
            return Ok((updated, created));
        }
        self.enqueue(name);
        self.enqueue_conditions(name);
        self.finalizer_event(recorded.take().as_ref(), &updated);
        Ok((updated, created))
    }

    /// `REST.Delete` (etcd.go:84-176).
    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        mut options: DeleteOptions,
    ) -> Result<(Deleted<CustomResourceDefinition>, bool)> {
        let crd = RestStorage::get(
            &self.store,
            ctx,
            name,
            &crate::registry::generic::GetOptions::default(),
        )
        .await?;

        // Ensure we have a UID precondition (:92-114).
        let preconditions = options
            .preconditions
            .get_or_insert_with(Preconditions::default);
        match &preconditions.uid {
            None => preconditions.uid = Some(crd.metadata.uid.clone()),
            Some(uid) if *uid != crd.metadata.uid => {
                return Err(self.store.conflict(
                    name,
                    format!(
                        "Precondition failed: UID in precondition: {uid}, UID in object meta: {}",
                        crd.metadata.uid
                    ),
                ));
            }
            Some(_) => {}
        }
        if let Some(rv) = &preconditions.resource_version {
            let stored = crd.metadata.resource_version.clone().unwrap_or_default();
            if *rv != stored {
                return Err(self.store.conflict(
                    name,
                    format!(
                        "Precondition failed: ResourceVersion in precondition: {rv}, ResourceVersion in object meta: {stored}"
                    ),
                ));
            }
        }
        let dry_run = options.dry_run.as_ref().is_some_and(|d| !d.is_empty());

        // Upon first request to delete, add our finalizer and then delegate
        // (:116-174).
        if crd.metadata.deletion_timestamp.is_none() {
            let preconditions = options.preconditions.clone();
            let out = self
                .store
                .guaranteed_update_for_delete(
                    ctx,
                    name,
                    preconditions.as_ref(),
                    dry_run,
                    delete_validation,
                    &start_deletion,
                )
                .await?;
            if !dry_run {
                // The update that stamped the deletion is the informer event
                // that wakes the CRD finalizer (crd_finalizer.go:341-366); its
                // workers run off their own queue, so this returns now.
                self.finalizer_event(Some(&crd), &out);
            }
            return Ok((Deleted::Object(out), false));
        }

        let out = RestStorage::delete(&self.store, ctx, name, delete_validation, options).await?;
        // `DeleteFunc` -> `queue.Add(key)` (naming_controller.go:365-379).
        if !dry_run {
            self.enqueue(name);
        }
        Ok(out)
    }

    /// `Store.DeleteCollection` deletes through the embedded Store, not
    /// `REST.Delete` (store.go:1237-1384), so the finalizer is not added.
    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        options: &DeleteOptions,
        list_options: &std::collections::HashMap<String, String>,
    ) -> Result<crate::registry::rest::DeletedCollection<CustomResourceDefinition>> {
        let deleted = RestStorage::delete_collection(
            &self.store,
            ctx,
            delete_validation,
            options,
            list_options,
        )
        .await?;
        let dry_run = options.dry_run.as_ref().is_some_and(|d| !d.is_empty());
        if !dry_run {
            for crd in &deleted.items {
                self.controllers.conditions.forget(&crd.metadata.name);
                let _ = self
                    .sync_group_except(&crd.spec.group, &crd.metadata.name)
                    .await;
                self.enqueue(&crd.metadata.name);
            }
        }
        Ok(deleted)
    }
}

#[async_trait]
impl RestStorage<CustomResourceDefinition> for CrdStatusRest {
    fn qualified_resource(&self) -> &GroupResource {
        self.store.qualified_resource()
    }

    fn namespace_scoped(&self) -> bool {
        RestStorage::namespace_scoped(&self.store)
    }

    /// The REST embeds `*genericregistry.Store`, so `GetResetFields` is promoted
    /// from it (store.go:1698-1703).
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        RestStorage::get_reset_fields(&self.store)
    }

    async fn get(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: &crate::registry::generic::GetOptions,
    ) -> Result<CustomResourceDefinition> {
        RestStorage::get(&self.store, ctx, name, options).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: CustomResourceDefinition,
        create_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        options: &CreateOptions,
    ) -> Result<CustomResourceDefinition> {
        RestStorage::create(&self.store, ctx, obj, create_validation, options).await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<CustomResourceDefinition>,
        create_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        update_validation: Option<&dyn ValidateObjectUpdate<CustomResourceDefinition>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(CustomResourceDefinition, bool)> {
        let recorded = RecordOld::new(obj_info);
        let out = RestStorage::update(
            &self.store,
            ctx,
            name,
            &recorded,
            create_validation,
            update_validation,
            force_allow_create,
            options,
        )
        .await?;
        if !options.dry_run {
            self.controllers.ensure_started(self.store.storage.clone());
            self.controllers.add(name);
            self.controllers.add_conditions(name);
            let old = recorded.take();
            let new = out.0.clone();
            finalizer_event(&self.controllers, &self.store.storage, old.as_ref(), &new);
        }
        Ok(out)
    }

    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        options: DeleteOptions,
    ) -> Result<(Deleted<CustomResourceDefinition>, bool)> {
        RestStorage::delete(&self.store, ctx, name, delete_validation, options).await
    }

    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        options: &DeleteOptions,
        list_options: &std::collections::HashMap<String, String>,
    ) -> Result<crate::registry::rest::DeletedCollection<CustomResourceDefinition>> {
        RestStorage::delete_collection(&self.store, ctx, delete_validation, options, list_options)
            .await
    }
}

/// `NewSharedInformerFactory(crdClient, 5*time.Minute)` (apiserver.go:170).
/// `go finalizingController.Run(5, hookContext.Done())` (apiserver.go:249):
/// `RunWithContext` starts `workers` goroutines on one queue
/// (crd_finalizer.go:280-282).
pub const CRD_FINALIZER_WORKERS: usize = 5;

/// Run `f` over `items` with at most `workers` in flight, as the finalizer's
/// worker goroutines drain its queue.
pub async fn run_workers<T, R, F, Fut>(items: Vec<T>, workers: usize, f: F) -> Vec<R>
where
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = R>,
{
    use futures::StreamExt;
    futures::stream::iter(items.into_iter().map(f))
        .buffer_unordered(workers)
        .collect()
        .await
}

pub const CRD_RESYNC_PERIOD: std::time::Duration = std::time::Duration::from_secs(300);

/// `workqueue.TypedItemExponentialFailureRateLimiter`
/// (client-go/util/workqueue/default_rate_limiters.go:100-141): a per-item
/// `base * 2^failures` backoff capped at `max`; `When` counts the failure,
/// `Forget` clears it.
pub struct ItemExponentialFailureRateLimiter {
    failures: std::collections::HashMap<String, u32>,
    base: std::time::Duration,
    max: std::time::Duration,
}

impl ItemExponentialFailureRateLimiter {
    pub fn new(base: std::time::Duration, max: std::time::Duration) -> Self {
        Self {
            failures: std::collections::HashMap::new(),
            base,
            max,
        }
    }

    /// `When` (:116-135). Overflow of the shift or the multiply returns `max`.
    pub fn when(&mut self, item: &str) -> std::time::Duration {
        let exp = self.failures.entry(item.to_string()).or_insert(0);
        let failures = *exp;
        *exp = exp.saturating_add(1);
        1u32.checked_shl(failures)
            .and_then(|m| self.base.checked_mul(m))
            .map_or(self.max, |d| d.min(self.max))
    }

    /// `NumRequeues` (:137-141).
    pub fn num_requeues(&self, item: &str) -> usize {
        self.failures.get(item).map_or(0, |n| *n as usize)
    }

    /// `Forget` (:143-148).
    pub fn forget(&mut self, item: &str) {
        self.failures.remove(item);
    }
}

/// `workqueue.TypedBucketRateLimiter` over `rate.NewLimiter(qps, burst)`
/// (default_rate_limiters.go:61-80; golang.org/x/time/rate `Reserve().Delay()`):
/// every call reserves one token, so tokens go negative once the burst is
/// spent and the delay is the time to refill the debt.
pub struct BucketRateLimiter {
    qps: f64,
    burst: f64,
    tokens: f64,
    last: Option<tokio::time::Instant>,
}

impl BucketRateLimiter {
    pub fn new(qps: f64, burst: u32) -> Self {
        Self {
            qps,
            burst: f64::from(burst),
            tokens: f64::from(burst),
            last: None,
        }
    }

    pub fn when_at(&mut self, now: tokio::time::Instant) -> std::time::Duration {
        if let Some(last) = self.last {
            let elapsed = now.saturating_duration_since(last).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.qps).min(self.burst);
        }
        self.last = Some(now);
        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_secs_f64(-self.tokens / self.qps)
        }
    }
}

/// `workqueue.DefaultTypedControllerRateLimiter` (default_rate_limiters.go:50-56):
/// `NewTypedMaxOfRateLimiter` of the per-item exponential limiter
/// (5ms, 1000s) and the overall bucket limiter (10 qps, burst 100). Both are
/// consulted on every call, and the longer delay wins (`TypedMaxOfRateLimiter.When`).
pub struct ControllerRateLimiter {
    item: ItemExponentialFailureRateLimiter,
    bucket: BucketRateLimiter,
}

impl Default for ControllerRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerRateLimiter {
    pub fn new() -> Self {
        Self {
            item: ItemExponentialFailureRateLimiter::new(
                std::time::Duration::from_millis(5),
                std::time::Duration::from_secs(1000),
            ),
            bucket: BucketRateLimiter::new(10.0, 100),
        }
    }

    pub fn when_at(&mut self, item: &str, now: tokio::time::Instant) -> std::time::Duration {
        let a = self.item.when(item);
        let b = self.bucket.when_at(now);
        a.max(b)
    }

    pub fn num_requeues(&self, item: &str) -> usize {
        self.item.num_requeues(item)
    }

    pub fn forget(&mut self, item: &str) {
        self.item.forget(item);
    }
}

/// The post-start hook that starts the CRD controllers
/// (apiserver.go:244-252), for the part that runs on a timer: a sweep of every
/// CRD at startup (the informer's initial list) and every
/// [`CRD_RESYNC_PERIOD`], and a rate-limited retry of each CRD whose sync
/// failed (`AddRateLimited`, crd_finalizer.go:307; polled every second like
/// `wait.UntilWithContext(ctx, c.runWorker, time.Second)`, :280-282). A CRD
/// left Terminating by a restart or a failed `delete_instances` is retried.
///
/// Upstream's controllers react to each informer event: [`run_crd_informer`]
/// (a storage watch) feeds them here, and a CRD write through the request
/// path queues the CRD directly as well (the queues dedupe). The workers
/// (naming/establishing on `controller_queue.rs`, the finalizer's five on
/// `finalizer_queue.rs`) are started by this hook, as `go <controller>.Run`
/// in apiserver.go:244-249; this task is the resync-driven half.
///
/// Registered as the `start-apiextensions-controllers` post-start hook
/// (apiserver.go:228-261: `go <controller>.Run(...)` for each, `return nil`).
pub fn start_controllers(
    storage: Arc<StorageBackend>,
    controllers: Arc<super::controller_queue::CrdControllers>,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(
        crate::bootstrap::APIEXTENSIONS_CONTROLLERS_HOOK,
        move || {
            start_controllers_loop(storage, controllers);
        },
    )
}

/// `PollUntilContextCancel(ctx, 100*time.Millisecond, true, ..)` over the
/// informer's `HasSynced` (apiserver.go:263-271): `synced` is tried at once,
/// then every 100ms, until it reports true. Upstream returns only on context
/// cancel (shutdown), which here is the task being dropped.
pub async fn wait_for_crd_informer_synced<F, Fut>(mut synced: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    while !synced().await {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// The `crd-informer-synced` post-start hook (apiserver.go:259-271):
/// "we don't want to report healthy until we can handle all CRDs that have
/// already been registered". Here the CRDs are read straight from storage
/// (there is no informer cache), so the informer's initial list completing
/// maps to a list of CRDs succeeding. The hook's `poststarthook` check is the
/// piece readyz/healthz expose.
///
/// Not ported: the `CRDInformerHasNotSynced` mux-and-discovery signal
/// (apiserver.go:133-139, closed at :266) which makes requests for custom
/// resource paths 503 rather than 404 until sync; custom resource routes here
/// are resolved per request from storage, with no install phase to guard.
pub fn spawn_crd_informer_synced_hook(storage: Arc<StorageBackend>) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_hook(crate::bootstrap::CRD_INFORMER_SYNCED_HOOK, async move {
        let rest = new_rest(storage);
        wait_for_crd_informer_synced(|| async { rest.all_names().await.is_ok() }).await;
        Ok::<(), std::convert::Infallible>(())
    })
}

/// One CRD informer event, as `cache.SharedIndexInformer` hands it to the
/// controllers' `ResourceEventHandlerFuncs`.
#[derive(Debug, Clone)]
pub enum CrdEvent {
    Add(Box<CustomResourceDefinition>),
    Update(Box<CustomResourceDefinition>, Box<CustomResourceDefinition>),
    Delete(Box<CustomResourceDefinition>),
}

/// The informer's local store (`DeltaFIFO` + indexer): turns a CRD's
/// presence in it into add vs update, and supplies the update's `oldObj`.
pub fn apply_crd(
    cache: &mut std::collections::HashMap<String, CustomResourceDefinition>,
    new: CustomResourceDefinition,
) -> CrdEvent {
    match cache.insert(new.metadata.name.clone(), new.clone()) {
        Some(old) => CrdEvent::Update(Box::new(old), Box::new(new)),
        None => CrdEvent::Add(Box::new(new)),
    }
}

/// `Replace` (shared_informer.go / delta_fifo.go): after a (re)list, every
/// listed object is an add/update against the cache and every cached object
/// that is no longer listed is a delete.
pub fn relist_events(
    cache: &mut std::collections::HashMap<String, CustomResourceDefinition>,
    listed: Vec<CustomResourceDefinition>,
) -> Vec<CrdEvent> {
    let names: std::collections::HashSet<String> =
        listed.iter().map(|c| c.metadata.name.clone()).collect();
    let mut events: Vec<CrdEvent> = Vec::new();
    let gone: Vec<String> = cache
        .keys()
        .filter(|n| !names.contains(*n))
        .cloned()
        .collect();
    for name in gone {
        if let Some(old) = cache.remove(&name) {
            events.push(CrdEvent::Delete(Box::new(old)));
        }
    }
    for crd in listed {
        events.push(apply_crd(cache, crd));
    }
    events
}

impl CrdRest {
    /// The controllers' event handlers for one informer event: the naming
    /// and establishing controllers' `AddFunc`/`UpdateFunc`/`DeleteFunc`
    /// (`queue.Add(key)`, naming_controller.go:343-379,
    /// establishing_controller.go:71-73) and the finalizer's
    /// `AddFunc`/`UpdateFunc` filters (crd_finalizer.go:332-366). The
    /// workqueues dedupe, so an event the write path already delivered is
    /// not processed twice.
    pub(super) fn handle_crd_event(&self, event: &CrdEvent) {
        match event {
            CrdEvent::Add(new) => {
                self.enqueue(&new.metadata.name);
                self.enqueue_conditions(&new.metadata.name);
                self.finalizer_event(None, new);
            }
            CrdEvent::Update(old, new) => {
                self.enqueue(&new.metadata.name);
                self.enqueue_conditions(&new.metadata.name);
                self.finalizer_event(Some(old), new);
            }
            CrdEvent::Delete(old) => {
                self.controllers.conditions.forget(&old.metadata.name);
                self.enqueue(&old.metadata.name);
            }
        }
    }
}

/// The CRD informer behind `s.Informers.Apiextensions().V1().
/// CustomResourceDefinitions()` (started by the `start-apiextensions-informers`
/// hook, apiserver.go:221-224): a storage watch opened BEFORE the initial
/// list so no event falls between them, feeding [`CrdRest::handle_crd_event`].
/// Controllers therefore react to every CRD change in storage, not only to
/// writes that went through this api-server's request path. A watch that
/// ends is re-established and re-listed (reflector `ListAndWatch`).
async fn run_crd_informer(
    storage: Arc<StorageBackend>,
    controllers: Arc<super::controller_queue::CrdControllers>,
) {
    use futures::StreamExt;
    use rusternetes_storage::WatchEvent;
    let rest = CrdRest::with_controllers(storage.clone(), controllers);
    let prefix = build_prefix("customresourcedefinitions", None);
    let mut cache = std::collections::HashMap::new();
    loop {
        match storage.watch(&prefix).await {
            Err(e) => warn!("crd informer: cannot watch CRDs: {e}"),
            Ok(mut stream) => match storage.list::<CustomResourceDefinition>(&prefix).await {
                Err(e) => warn!("crd informer: cannot list CRDs: {e}"),
                Ok(listed) => {
                    for ev in relist_events(&mut cache, listed) {
                        rest.handle_crd_event(&ev);
                    }
                    while let Some(ev) = stream.next().await {
                        let Ok(ev) = ev else { break };
                        let event = match ev {
                            WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                                match serde_json::from_str::<CustomResourceDefinition>(&v) {
                                    Ok(new) => apply_crd(&mut cache, new),
                                    Err(_) => continue,
                                }
                            }
                            WatchEvent::Deleted(_, v) => {
                                match serde_json::from_str::<CustomResourceDefinition>(&v) {
                                    Ok(old) => {
                                        cache.remove(&old.metadata.name);
                                        CrdEvent::Delete(Box::new(old))
                                    }
                                    Err(_) => continue,
                                }
                            }
                        };
                        rest.handle_crd_event(&event);
                    }
                }
            },
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

pub fn start_controllers_loop(
    storage: Arc<StorageBackend>,
    controllers: Arc<super::controller_queue::CrdControllers>,
) -> tokio::task::JoinHandle<()> {
    // `go namingController.RunWithContext`, `go establishingController...`,
    // `go finalizingController.Run(5, ..)` (apiserver.go:244-249): the
    // workers start with the hook, not on the first CRD write.
    controllers.ensure_started(storage.clone());
    // The informer whose events feed them (apiserver.go:221-224).
    tokio::spawn(run_crd_informer(storage.clone(), controllers.clone()));
    tokio::spawn(async move {
        // The one set of controllers (queue + memos) the request path uses.
        let rest = CrdRest::with_controllers(storage, controllers);
        // Each controller's `workqueue.DefaultTypedControllerRateLimiter`
        // (one per queue upstream; this timer task is the shared retry queue).
        let mut limiter = ControllerRateLimiter::new();
        let mut failing: std::collections::HashMap<String, tokio::time::Instant> =
            std::collections::HashMap::new();
        let mut resync = tokio::time::interval(CRD_RESYNC_PERIOD);
        let mut retry = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = resync.tick() => {
                    let failed = rest.resync().await;
                    // Forget what succeeded; back off what failed again.
                    failing.retain(|name, _| {
                        let keep = failed.contains(name);
                        if !keep {
                            limiter.forget(name);
                        }
                        keep
                    });
                    for name in failed {
                        let now = tokio::time::Instant::now();
                        failing.insert(name.clone(), now + limiter.when_at(&name, now));
                    }
                }
                _ = retry.tick() => {
                    let now = tokio::time::Instant::now();
                    let due: Vec<String> = failing
                        .iter()
                        .filter(|(_, at)| **at <= now)
                        .map(|(n, _)| n.clone())
                        .collect();
                    let results = run_workers(due, CRD_FINALIZER_WORKERS, |name| {
                        let rest = &rest;
                        async move {
                            let r = rest.resync_one(&name).await;
                            (name, r)
                        }
                    })
                    .await;
                    for (name, r) in results {
                        match r {
                            Ok(()) => {
                                failing.remove(&name);
                                limiter.forget(&name);
                            }
                            Err(e) => {
                                warn!("customresourcedefinition {name}: retry failed: {e}");
                                let now = tokio::time::Instant::now();
                                failing.insert(name.clone(), now + limiter.when_at(&name, now));
                            }
                        }
                    }
                }
            }
        }
    })
}

/// The CRD endpoint's storage: [`CrdRest`] over [`new_store`].
pub fn new_rest(storage: Arc<StorageBackend>) -> CrdRest {
    CrdRest::with_controllers(storage, Default::default())
}
