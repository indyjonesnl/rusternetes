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
//! - `GetResetFields` (managed-fields reset sets), `ShortNames` and
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
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend};
use tracing::warn;

use super::controllers::{
    self, condition, crd_has_finalizer, crd_remove_finalizer, set_crd_condition, ESTABLISHED,
    TERMINATING,
};
use crate::registry::generic::{CreateOptions, Deleted, Store, UpdateOptions};
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject, ValidateObjectUpdate,
};

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
fn schema_rule_errors(crd: &CustomResourceDefinition) -> ErrorList {
    let mut errs = Vec::new();
    for (i, version) in crd.spec.versions.iter().enumerate() {
        let Some(validation) = &version.schema else {
            continue;
        };
        if let Err(e) =
            crate::handlers::cel_validation::validate_crd_rules(&validation.open_apiv3_schema)
        {
            let detail = match e {
                Error::InvalidResource(msg) => msg,
                other => other.to_string(),
            };
            errs.push(FieldError::invalid(
                &Path::new("spec")
                    .child("versions")
                    .index(i)
                    .child("schema")
                    .child("openAPIV3Schema"),
                String::new(),
                detail,
            ));
        }
    }
    errs
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
        if let Some(schema) = &v.schema {
            collect_unrecognized_formats(&schema.open_apiv3_schema, &mut out);
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
        errs.extend(schema_rule_errors(obj));
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
        errs.extend(schema_rule_errors(obj));
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

/// An update whose new object is computed from the stored one — a controller
/// writing through `UpdateStatus`.
struct ControllerUpdate<'a> {
    compute: &'a (dyn Fn(&CustomResourceDefinition) -> CustomResourceDefinition + Send + Sync),
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
        Ok((self.compute)(old))
    }
}

/// `REST` (etcd.go:36-38, 84-176): the Store, with a `Delete` that starts the
/// CRD's termination, and the controllers that act on every write.
pub struct CrdRest {
    store: Store<CustomResourceDefinition, StorageBackend>,
    status_store: Store<CustomResourceDefinition, StorageBackend>,
}

impl CrdRest {
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
        let update = ControllerUpdate { compute };
        match self
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
            .await
        {
            Ok((out, _)) => Ok(Some(out)),
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

    async fn get_crd(&self, name: &str) -> Result<Option<CustomResourceDefinition>> {
        match self.store.get(&Self::ctx(), name).await {
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

    /// `requeueAllOtherGroupCRDs` (naming_controller.go:76-103): a CRD that
    /// changed or freed its names may unblock the others of its group.
    async fn sync_group_except(&self, group: &str, except: &str) -> Result<()> {
        for other in self.group_crds(group).await? {
            if other.metadata.name != except {
                self.sync_one(&other.metadata.name).await?;
            }
        }
        Ok(())
    }

    /// What the informer-driven controllers do after a CRD is written. They
    /// run independently of the request, so a failure here is logged, not
    /// returned.
    async fn run_controllers(&self, name: &str, group: &str) {
        let result = async {
            let names_changed = self.sync_one(name).await?;
            if names_changed {
                self.sync_group_except(group, name).await?;
            }
            Result::Ok(())
        }
        .await;
        if let Err(e) = result {
            warn!("customresourcedefinition {name}: controllers failed: {e}");
        }
    }

    /// One CRD as the informer-driven controllers see it on a resync: the
    /// naming and establishing controllers' `sync` (their `UpdateFunc`
    /// enqueues every update, resyncs included - naming_controller.go:359-363),
    /// then the finalizer's, which only acts on a CRD that is being deleted
    /// and still holds the cleanup finalizer (crd_finalizer.go:330-335).
    pub async fn resync_one(&self, name: &str) -> Result<()> {
        let Some(crd) = self.get_crd(name).await? else {
            return Ok(());
        };
        if self.sync_one(name).await? {
            self.sync_group_except(&crd.spec.group, name).await?;
        }
        self.finalize(name).await
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
        let mut failed = Vec::new();
        for name in names {
            if let Err(e) = self.resync_one(&name).await {
                warn!("customresourcedefinition {name}: resync failed: {e}");
                failed.push(name);
            }
        }
        failed
    }

    /// `deleteInstances` (crd_finalizer.go:181-): the stored instances of the
    /// CRD are removed straight from storage, where upstream issues
    /// `DeleteCollection` per namespace and then waits for the list to empty.
    async fn delete_instances(&self, crd: &CustomResourceDefinition) -> Result<()> {
        let resource_type = format!(
            "{}_{}",
            crd.spec.group.replace('.', "_"),
            crd.spec.names.plural
        );
        let namespaced = crd.spec.scope == rusternetes_common::resources::ResourceScope::Namespaced;
        // A stored instance does not record its namespace, only its key does,
        // so a namespaced CRD's instances are found namespace by namespace —
        // as upstream's finalizer issues one `DeleteCollection` per namespace
        // (crd_finalizer.go:219-235).
        let namespaces: Vec<Option<String>> = if namespaced {
            let all: Vec<serde_json::Value> = self
                .store
                .storage
                .list(&build_prefix("namespaces", None))
                .await?;
            all.iter()
                .filter_map(|ns| ns.pointer("/metadata/name")?.as_str().map(str::to_string))
                .map(Some)
                .collect()
        } else {
            vec![None]
        };
        for namespace in namespaces {
            let prefix = build_prefix(&resource_type, namespace.as_deref());
            let items: Vec<serde_json::Value> = self.store.storage.list(&prefix).await?;
            for item in items {
                let Some(name) = item.pointer("/metadata/name").and_then(|v| v.as_str()) else {
                    continue;
                };
                let key = build_key(&resource_type, namespace.as_deref(), name);
                match self.store.storage.delete(&key).await {
                    Ok(()) | Err(Error::NotFound(_)) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
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
        let terminating = if is_crd_condition_true(&crd, ESTABLISHED) {
            match self.delete_instances(&crd).await {
                Ok(()) => condition(
                    TERMINATING,
                    "False",
                    "InstanceDeletionCompleted",
                    "removed all instances",
                ),
                Err(e) => {
                    let failed = condition(
                        TERMINATING,
                        "True",
                        "InstanceDeletionFailed",
                        &format!("could not issue all deletes: {e}"),
                    );
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

/// The first-delete mutation of `REST.Delete` (etcd.go:127-148): the
/// deletion is stamped, the cleanup finalizer added and the CRD marked
/// `Terminating`.
fn start_deletion(crd: &mut CustomResourceDefinition) {
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

    async fn get(&self, ctx: &RequestContext, name: &str) -> Result<CustomResourceDefinition> {
        RestStorage::get(&self.store, ctx, name).await
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
        self.run_controllers(&created.metadata.name, &created.spec.group)
            .await;
        Ok(self
            .get_crd(&created.metadata.name)
            .await?
            .unwrap_or(created))
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
        let (updated, created) = RestStorage::update(
            &self.store,
            ctx,
            name,
            obj_info,
            create_validation,
            update_validation,
            force_allow_create,
            options,
        )
        .await?;
        if options.dry_run {
            return Ok((updated, created));
        }
        self.run_controllers(name, &updated.spec.group).await;
        Ok((self.get_crd(name).await?.unwrap_or(updated), created))
    }

    /// `REST.Delete` (etcd.go:84-176).
    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        mut options: DeleteOptions,
    ) -> Result<(Deleted<CustomResourceDefinition>, bool)> {
        let crd = RestStorage::get(&self.store, ctx, name).await?;

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
                // The CRD finalizer is informer-driven upstream; here the
                // delete that woke it runs it.
                if let Err(e) = self.finalize(name).await {
                    warn!("customresourcedefinition {name}: finalizer failed: {e}");
                }
                let _ = self.sync_group_except(&out.spec.group, name).await;
            }
            return Ok((Deleted::Object(out), false));
        }

        // A CRD whose first delete did not finish (the finalizer failed)
        // gets another go, as the controller's resync would give it.
        if !dry_run && crd_has_finalizer(&crd, CUSTOM_RESOURCE_CLEANUP_FINALIZER) {
            if let Err(e) = self.finalize(name).await {
                warn!("customresourcedefinition {name}: finalizer failed: {e}");
            }
            if self.get_crd(name).await?.is_none() {
                let _ = self.sync_group_except(&crd.spec.group, name).await;
                return Ok((Deleted::Object(crd), false));
            }
        }
        RestStorage::delete(&self.store, ctx, name, delete_validation, options).await
    }

    /// `Store.DeleteCollection` deletes through the embedded Store, not
    /// `REST.Delete` (store.go:1237-1384), so the finalizer is not added.
    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<CustomResourceDefinition>>,
        options: &DeleteOptions,
        list_options: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<CustomResourceDefinition>> {
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
            for crd in &deleted {
                let _ = self
                    .sync_group_except(&crd.spec.group, &crd.metadata.name)
                    .await;
            }
        }
        Ok(deleted)
    }
}

/// `NewSharedInformerFactory(crdClient, 5*time.Minute)` (apiserver.go:170).
pub const CRD_RESYNC_PERIOD: std::time::Duration = std::time::Duration::from_secs(300);

/// `workqueue.DefaultTypedControllerRateLimiter`'s per-item exponential
/// failure limiter: `NewTypedItemExponentialFailureRateLimiter(5ms, 1000s)`
/// (client-go/util/workqueue/default_rate_limiters.go). The overall bucket
/// limiter (10 qps) is not modelled: the loop below retries one key at a time.
fn requeue_delay(failures: u32) -> std::time::Duration {
    let base = std::time::Duration::from_millis(5);
    let max = std::time::Duration::from_secs(1000);
    base.checked_mul(1u32.checked_shl(failures).unwrap_or(u32::MAX))
        .map_or(max, |d| d.min(max))
}

/// The post-start hook that starts the CRD controllers
/// (apiserver.go:244-252), for the part that runs on a timer: a sweep of every
/// CRD at startup (the informer's initial list) and every
/// [`CRD_RESYNC_PERIOD`], and a rate-limited retry of each CRD whose sync
/// failed (`AddRateLimited`, crd_finalizer.go:307; polled every second like
/// `wait.UntilWithContext(ctx, c.runWorker, time.Second)`, :280-282). A CRD
/// left Terminating by a restart or a failed `delete_instances` is retried.
///
/// Deviation: upstream's controllers also react to each watch event; here the
/// write path runs them inline (see [`super::controllers`]), so only the
/// resync-driven half is a task.
pub fn spawn_resync(storage: Arc<StorageBackend>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let rest = new_rest(storage);
        let mut failing: std::collections::HashMap<String, (u32, tokio::time::Instant)> =
            std::collections::HashMap::new();
        let mut resync = tokio::time::interval(CRD_RESYNC_PERIOD);
        let mut retry = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = resync.tick() => {
                    let failed = rest.resync().await;
                    // Forget what succeeded; back off what failed again.
                    failing.retain(|name, _| failed.contains(name));
                    for name in failed {
                        let entry = failing
                            .entry(name)
                            .or_insert((0, tokio::time::Instant::now()));
                        entry.1 = tokio::time::Instant::now() + requeue_delay(entry.0);
                        entry.0 += 1;
                    }
                }
                _ = retry.tick() => {
                    let now = tokio::time::Instant::now();
                    let due: Vec<String> = failing
                        .iter()
                        .filter(|(_, (_, at))| *at <= now)
                        .map(|(n, _)| n.clone())
                        .collect();
                    for name in due {
                        match rest.resync_one(&name).await {
                            Ok(()) => {
                                failing.remove(&name);
                            }
                            Err(e) => {
                                warn!("customresourcedefinition {name}: retry failed: {e}");
                                if let Some((n, at)) = failing.get_mut(&name) {
                                    *at = tokio::time::Instant::now() + requeue_delay(*n);
                                    *n += 1;
                                }
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
    CrdRest {
        store: new_store(storage.clone()),
        status_store: new_status_store(storage),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crd(extra: serde_json::Value) -> CustomResourceDefinition {
        let mut body = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com", "generation": 4},
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {"plural": "widgets", "kind": "Widget"},
                "versions": [{"name": "v1", "served": true, "storage": true}]
            },
            "status": {"storedVersions": ["v0"]}
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        let mut crd: CustomResourceDefinition = serde_json::from_value(body).unwrap();
        convert_to_internal(&mut crd);
        crd
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    fn crd_with_props(props: serde_json::Value) -> CustomResourceDefinition {
        crd(serde_json::json!({"spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "kind": "Widget"},
            "versions": [{"name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "properties": props}}}]
        }}))
    }

    fn sorted(mut w: Vec<String>) -> Vec<String> {
        w.sort();
        w
    }

    /// `TestWarningsOnCreate` (strategy_test.go:1428-1584).
    #[test]
    fn warnings_on_create_name_unrecognized_formats() {
        let ok =
            crd_with_props(serde_json::json!({"f": {"type": "string", "format": "date-time"}}));
        assert!(Strategy.warnings_on_create(&ctx(), &ok).is_empty());

        let bad =
            crd_with_props(serde_json::json!({"f": {"type": "string", "format": "invalidformat"}}));
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &bad),
            vec![r#"unrecognized format "invalidformat""#.to_string()]
        );

        let nested = crd_with_props(serde_json::json!({"nested": {"type": "object",
            "properties": {"e": {"type": "string", "format": "invalidformat"}}}}));
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &nested),
            vec![r#"unrecognized format "invalidformat""#.to_string()]
        );

        let many = crd_with_props(serde_json::json!({
            "field1": {"type": "string", "format": "unknownformat1"},
            "field2": {"type": "string", "format": "unknownformat2"},
            "nested": {"type": "object",
                "properties": {"field3": {"type": "string", "format": "unknownformat3"}}}}));
        assert_eq!(
            sorted(Strategy.warnings_on_create(&ctx(), &many)),
            vec![
                r#"unrecognized format "unknownformat1""#.to_string(),
                r#"unrecognized format "unknownformat2""#.to_string(),
                r#"unrecognized format "unknownformat3""#.to_string(),
            ]
        );
    }

    /// `GetUnrecognizedFormats` (formats.go:104-119) only judges `type: string`
    /// schemas, and the k8s-short-name/k8s-long-name formats are recognised
    /// at 1.34+ (formats.go:36-52).
    #[test]
    fn warnings_only_judge_string_schemas_and_know_k8s_names() {
        let c = crd_with_props(serde_json::json!({
            "n": {"type": "integer", "format": "whatever"},
            "s": {"type": "string", "format": "k8s-short-name"},
            "l": {"type": "string", "format": "k8s-long-name"},
            "u": {"type": "string", "format": "uuid4"},
            "a": {"type": "array", "items": {"type": "string", "format": "nope"}}}));
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &c),
            vec![r#"unrecognized format "nope""#.to_string()]
        );
    }

    /// `TestWarningsOnUpdate`: only newly introduced formats warn (ratcheting,
    /// strategy.go:178-194).
    #[test]
    fn warnings_on_update_ratchet() {
        let old = crd_with_props(serde_json::json!({"a": {"type": "string", "format": "oldbad"}}));
        let new = crd_with_props(serde_json::json!({
            "a": {"type": "string", "format": "oldbad"},
            "b": {"type": "string", "format": "newbad"}}));
        assert_eq!(
            Strategy.warnings_on_update(&ctx(), &new, &old),
            vec![r#"unrecognized format "newbad""#.to_string()]
        );
        assert!(Strategy.warnings_on_update(&ctx(), &old, &old).is_empty());
        // statusStrategy.WarningsOnUpdate (strategy.go:292-294) is nil.
        assert!(StatusStrategy
            .warnings_on_update(&ctx(), &new, &old)
            .is_empty());
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!StatusStrategy.allow_unconditional_update());
    }

    /// `PrepareForCreate` (strategy.go:74-89), exercised by
    /// `TestPrepareForCreate`-style inputs: whatever status came in, the
    /// stored versions are the storage version only.
    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut c = crd(serde_json::json!({}));
        Strategy.prepare_for_create(&ctx(), &mut c);
        assert_eq!(c.metadata.generation, Some(1));
        let status = c.status.as_ref().unwrap();
        assert_eq!(status.stored_versions, Some(vec!["v1".to_string()]));
        assert!(status.conditions.is_none() && status.accepted_names.is_none());
    }

    /// `PrepareForUpdate` (strategy.go:91-118): old status wins, a spec
    /// change bumps the generation, a new storage version is appended after
    /// the versions already stored.
    #[test]
    fn prepare_for_update_keeps_status_and_tracks_spec_changes() {
        let old = crd(serde_json::json!({}));
        let mut same = old.clone();
        same.status = None;
        Strategy.prepare_for_update(&ctx(), &mut same, &old);
        assert_eq!(same.metadata.generation, Some(4));
        assert_eq!(
            same.status.as_ref().unwrap().stored_versions,
            Some(vec!["v0".to_string(), "v1".to_string()])
        );

        let mut changed = old.clone();
        changed.spec.names.short_names = Some(vec!["wd".into()]);
        Strategy.prepare_for_update(&ctx(), &mut changed, &old);
        assert_eq!(changed.metadata.generation, Some(5));
    }

    #[test]
    fn status_update_keeps_the_spec_but_not_the_metadata() {
        let old = crd(serde_json::json!({}));
        let mut new = old.clone();
        new.spec.names.short_names = Some(vec!["wd".into()]);
        new.metadata.finalizers = Some(vec![]);
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec, old.spec);
        assert_eq!(new.metadata.finalizers, Some(vec![]));
    }

    #[test]
    fn start_deletion_adds_the_finalizer_and_condition_once() {
        let mut c = crd(serde_json::json!({}));
        start_deletion(&mut c);
        start_deletion(&mut c);
        assert!(c.metadata.deletion_timestamp.is_some());
        assert_eq!(
            c.metadata.finalizers,
            Some(vec![CUSTOM_RESOURCE_CLEANUP_FINALIZER.to_string()])
        );
        let t = controllers::find_crd_condition(&c, TERMINATING).unwrap();
        assert_eq!(t.reason.as_deref(), Some("InstanceDeletionPending"));
        assert_eq!(c.status.unwrap().conditions.unwrap().len(), 1);
    }
}
