//! Port of `staging/src/k8s.io/apiserver/pkg/registry/rest/`: the strategy
//! interfaces a resource implements, and the `BeforeCreate` / `BeforeUpdate` /
//! `BeforeDelete` hooks that apply the rules common to every resource.
//!
//! Upstream passes a `context.Context` carrying the request namespace and a
//! warning recorder; [`RequestContext`] is that context. Where upstream uses
//! `runtime.Object` and `meta.Accessor`, this uses a typed `T:`[`Object`] and
//! [`HasMetadata`].

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::types::ObjectMeta;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_path_segment, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};

pub use crate::handlers::finalizers::HasMetadata;

/// What the registry needs from a stored type. Upstream gets all of it from
/// `runtime.Object` + `meta.Accessor`.
pub trait Object:
    HasMetadata + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}
impl<T> Object for T where
    T: HasMetadata + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

/// `schema.GroupResource`, the identity upstream threads through every error
/// constructor (`NewNotFound(qualifiedResource, name)` and friends).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupResource {
    pub group: String,
    pub resource: String,
}

impl GroupResource {
    pub fn new(group: &str, resource: &str) -> Self {
        Self {
            group: group.to_string(),
            resource: resource.to_string(),
        }
    }
}

impl std::fmt::Display for GroupResource {
    /// `GroupResource.String()` (apimachinery/pkg/runtime/schema/group_version.go:62):
    /// `resource` for the core group, `resource.group` otherwise.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.group.is_empty() {
            write!(f, "{}", self.resource)
        } else {
            write!(f, "{}.{}", self.resource, self.group)
        }
    }
}

/// The request-scoped values upstream reads out of `context.Context`:
/// `genericapirequest.NamespaceFrom(ctx)` and the `warning.AddWarning` sink.
#[derive(Debug, Default)]
pub struct RequestContext {
    /// The namespace from the request path; `None` (upstream's
    /// `metav1.NamespaceNone`, `""`) for a cluster-scoped request.
    pub namespace: Option<String>,
    warnings: Mutex<Vec<String>>,
}

impl RequestContext {
    pub fn new(namespace: Option<&str>) -> Self {
        Self {
            namespace: namespace.filter(|ns| !ns.is_empty()).map(str::to_string),
            warnings: Mutex::new(Vec::new()),
        }
    }

    /// `warning.AddWarning(ctx, "", w)`. Like upstream's recorder
    /// (endpoints/filters/warning.go:69-92) an empty warning is dropped and a
    /// repeated one is recorded once.
    pub fn add_warning(&self, warning: String) {
        if warning.is_empty() {
            return;
        }
        if let Ok(mut w) = self.warnings.lock() {
            if !w.contains(&warning) {
                w.push(warning);
            }
        }
    }

    /// The warnings recorded so far, for the handler to emit as
    /// `Warning: 299 - "..."` headers.
    pub fn warnings(&self) -> Vec<String> {
        self.warnings.lock().map(|w| w.clone()).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Strategy interfaces
// ---------------------------------------------------------------------------

/// `NamespaceScopedStrategy` (rest/create.go:180-184).
pub trait NamespaceScopedStrategy {
    fn namespace_scoped(&self) -> bool;
}

/// `RESTCreateStrategy` (rest/create.go:40-90).
///
/// `GenerateName` comes from the embedded `names.NameGenerator` upstream; every
/// in-tree strategy embeds `names.SimpleNameGenerator`, which is the default.
/// `WarningsOnCreate` and `Canonicalize` are no-ops in most strategies, so they
/// default to that here.
pub trait RestCreateStrategy<T>: NamespaceScopedStrategy + Send + Sync {
    fn generate_name(&self, base: &str) -> String {
        super::names::simple_name_generator(base)
    }
    /// Normalize the object before validation: clear status, set the initial
    /// generation, drop disabled fields.
    fn prepare_for_create(&self, ctx: &RequestContext, obj: &mut T);
    fn validate(&self, ctx: &RequestContext, obj: &T) -> ErrorList;
    fn warnings_on_create(&self, _ctx: &RequestContext, _obj: &T) -> Vec<String> {
        Vec::new()
    }
    fn canonicalize(&self, _obj: &mut T) {}
}

/// `RESTUpdateStrategy` (rest/update.go:39-84).
pub trait RestUpdateStrategy<T>: NamespaceScopedStrategy + Send + Sync {
    fn allow_create_on_update(&self) -> bool;
    /// Copy what the client may not change (e.g. status on the main resource,
    /// spec on `/status`) from `old`, and bump generation when warranted.
    fn prepare_for_update(&self, ctx: &RequestContext, obj: &mut T, old: &T);
    fn validate_update(&self, ctx: &RequestContext, obj: &T, old: &T) -> ErrorList;
    fn warnings_on_update(&self, _ctx: &RequestContext, _obj: &T, _old: &T) -> Vec<String> {
        Vec::new()
    }
    fn canonicalize(&self, _obj: &mut T) {}
    /// Whether an update without `metadata.resourceVersion` is applied to the
    /// latest stored object instead of being rejected.
    fn allow_unconditional_update(&self) -> bool;
}

/// `rest.GarbageCollectionPolicy` (rest/delete.go:40-47).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarbageCollectionPolicy {
    // No strategy on the Store returns it yet; the ReplicaSet-family
    // strategies will.
    #[allow(dead_code)]
    DeleteDependents,
    OrphanDependents,
    /// The resource does not support garbage collection: DELETE never adds a
    /// GC finalizer.
    Unsupported,
}

/// `RESTGracefulDeleteStrategy` (rest/delete.go:58-64).
pub trait RestGracefulDeleteStrategy<T>: Send + Sync {
    /// Whether the object should be deleted gracefully. May default
    /// `options.grace_period_seconds`.
    fn check_graceful_delete(
        &self,
        ctx: &RequestContext,
        obj: &T,
        options: &mut DeleteOptions,
    ) -> bool;
}

/// `RESTDeleteStrategy` (rest/delete.go:35-37) with upstream's two optional
/// extension interfaces folded in as provided methods: a Go strategy "is a"
/// `GarbageCollectionDeleteStrategy` / `RESTGracefulDeleteStrategy` by type
/// assertion; here it returns `Some`.
pub trait RestDeleteStrategy<T>: Send + Sync {
    /// `GarbageCollectionDeleteStrategy.DefaultGarbageCollectionPolicy`
    /// (rest/delete.go:51-54). `None` is a strategy that does not implement
    /// the interface — upstream's zero-value policy.
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        None
    }
    /// The graceful-delete extension, if this resource supports it.
    fn graceful(&self) -> Option<&dyn RestGracefulDeleteStrategy<T>> {
        None
    }
}

/// `rest.ValidateObjectFunc` (rest/rest.go): the admission chain's validating
/// phase, handed to the Store so it runs on the fully formed object — after
/// `BeforeCreate`, before the write.
#[async_trait]
pub trait ValidateObject<T>: Send + Sync {
    async fn validate(&self, ctx: &RequestContext, obj: &T) -> Result<()>;
}

/// `rest.ValidateObjectUpdateFunc` (rest/rest.go).
#[async_trait]
pub trait ValidateObjectUpdate<T>: Send + Sync {
    async fn validate(&self, ctx: &RequestContext, obj: &T, old: &T) -> Result<()>;
}

// ---------------------------------------------------------------------------
// ObjectMeta helpers (rest/meta.go)
// ---------------------------------------------------------------------------

/// `WipeObjectMetaSystemFields` (rest/meta.go:30-36): the fields a client may
/// not set on create. The create handler calls it before admission.
pub fn wipe_object_meta_system_fields(meta: &mut ObjectMeta) {
    meta.creation_timestamp = None;
    meta.uid = String::new();
    meta.deletion_timestamp = None;
    meta.deletion_grace_period_seconds = None;
}

/// `FillObjectMetaSystemFields` (rest/meta.go:39-42).
///
/// Upstream's `metav1.Time` serializes at second precision, so the timestamp
/// is truncated to whole seconds: what the client reads back must equal what
/// was stored.
pub fn fill_object_meta_system_fields(meta: &mut ObjectMeta) {
    meta.creation_timestamp = Some(now_seconds());
    meta.uid = uuid::Uuid::new_v4().to_string();
}

fn now_seconds() -> chrono::DateTime<Utc> {
    use chrono::SubsecRound;
    Utc::now().trunc_subsecs(0)
}

/// `metav1.HasObjectMetaSystemFieldValues`
/// (apimachinery/pkg/apis/meta/v1/meta.go:173-176).
fn has_object_meta_system_field_values(meta: &ObjectMeta) -> bool {
    !meta.uid.is_empty() && meta.creation_timestamp.is_some()
}

/// `EnsureObjectNamespaceMatchesRequestNamespace` (rest/meta.go:47-67).
pub fn ensure_object_namespace_matches_request_namespace(
    request_namespace: Option<&str>,
    meta: &mut ObjectMeta,
) -> Result<()> {
    let request_namespace = request_namespace.unwrap_or("");
    let object_namespace = meta.namespace.as_deref().unwrap_or("");
    if object_namespace == request_namespace {
        return Ok(());
    }
    if object_namespace.is_empty() {
        meta.namespace = Some(request_namespace.to_string());
        return Ok(());
    }
    if request_namespace.is_empty() {
        meta.namespace = None;
        return Ok(());
    }
    Err(Error::BadRequest(
        "the namespace of the provided object does not match the namespace sent on the request"
            .to_string(),
    ))
}

/// `ExpectedNamespaceForScope` (rest/meta.go:70-75).
pub fn expected_namespace_for_scope(
    request_namespace: Option<&str>,
    namespace_scoped: bool,
) -> Option<&str> {
    if namespace_scoped {
        request_namespace
    } else {
        None
    }
}

fn internal_error(msg: impl std::fmt::Display) -> Error {
    // `errors.NewInternalError(err)` (apimachinery/pkg/api/errors/errors.go:387).
    Error::Internal(format!("Internal error occurred: {msg}"))
}

// ---------------------------------------------------------------------------
// BeforeCreate / BeforeUpdate
// ---------------------------------------------------------------------------

/// `BeforeCreate` (rest/create.go:95-140).
///
/// Expects [`fill_object_meta_system_fields`] and name generation to have run
/// already — the Store does both before calling this, and this only checks.
pub fn before_create<T: Object>(
    strategy: &dyn RestCreateStrategy<T>,
    ctx: &RequestContext,
    obj: &mut T,
) -> Result<()> {
    let meta = obj.metadata();
    if !has_object_meta_system_field_values(meta) {
        return Err(internal_error("system metadata was not initialized"));
    }
    if meta.generate_name.as_deref().is_some_and(|g| !g.is_empty()) && meta.name.is_empty() {
        return Err(internal_error("metadata.name was not generated"));
    }

    ensure_object_namespace_matches_request_namespace(
        expected_namespace_for_scope(ctx.namespace.as_deref(), strategy.namespace_scoped()),
        obj.metadata_mut(),
    )?;

    strategy.prepare_for_create(ctx, obj);

    let errs = strategy.validate(ctx, obj);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }

    // "Do this *after* custom validation so that specific error messages are
    // shown whenever possible" (create.go:126-128). The name check here is
    // `path.ValidatePathSegmentName`, the loosest one; the strategy's own
    // validator has already applied the resource's real name rule.
    let errs = validate_object_meta(
        obj.metadata(),
        strategy.namespace_scoped(),
        name_is_path_segment,
        &Path::new("metadata"),
    );
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }

    for w in strategy.warnings_on_create(ctx, obj) {
        ctx.add_warning(w);
    }

    strategy.canonicalize(obj);
    Ok(())
}

/// `validateCommonFields` (rest/update.go:88-101).
fn validate_common_fields(meta: &ObjectMeta, old_meta: &ObjectMeta, namespaced: bool) -> ErrorList {
    let fld = Path::new("metadata");
    let mut errs = validate_object_meta(meta, namespaced, name_is_path_segment, &fld);
    errs.extend(validate_object_meta_update(meta, old_meta, &fld));
    errs
}

/// `BeforeUpdate` (rest/update.go:107-167).
pub fn before_update<T: Object>(
    strategy: &dyn RestUpdateStrategy<T>,
    ctx: &RequestContext,
    obj: &mut T,
    old: &T,
) -> Result<()> {
    ensure_object_namespace_matches_request_namespace(
        expected_namespace_for_scope(ctx.namespace.as_deref(), strategy.namespace_scoped()),
        obj.metadata_mut(),
    )?;

    // Ensure requests cannot update generation.
    let old_meta = old.metadata();
    obj.metadata_mut().generation = old_meta.generation;

    strategy.prepare_for_update(ctx, obj, old);

    let meta = obj.metadata_mut();
    // Use the existing UID if none is provided.
    if meta.uid.is_empty() {
        meta.uid = old_meta.uid.clone();
    }
    // Ignore changes to timestamp.
    if old_meta.creation_timestamp.is_some() {
        meta.creation_timestamp = old_meta.creation_timestamp;
    }
    // An update can never remove/change a deletion timestamp.
    if old_meta.deletion_timestamp.is_some() {
        meta.deletion_timestamp = old_meta.deletion_timestamp;
    }
    // An update can never remove/change grace period seconds.
    if old_meta.deletion_grace_period_seconds.is_some()
        && meta.deletion_grace_period_seconds.is_none()
    {
        meta.deletion_grace_period_seconds = old_meta.deletion_grace_period_seconds;
    }

    let mut errs = validate_common_fields(obj.metadata(), old_meta, strategy.namespace_scoped());
    errs.extend(strategy.validate_update(ctx, obj, old));
    if !errs.is_empty() {
        // Upstream also calls `RecordDuplicateValidationErrors` here, which
        // only feeds a metric; there is no metric to feed.
        return Err(Error::Invalid(errs));
    }

    for w in strategy.warnings_on_update(ctx, obj, old) {
        ctx.add_warning(w);
    }

    strategy.canonicalize(obj);
    Ok(())
}

/// `CheckGeneratedNameError` (rest/create.go:144-169): an `AlreadyExists` from
/// an object whose name the server generated is the server's failure to pick a
/// unique name, not the client's collision, and says so.
pub fn check_generated_name_error<T: Object>(
    qualified_resource: &GroupResource,
    err: Error,
    obj: &T,
) -> Error {
    if !matches!(err, Error::AlreadyExists(_)) {
        return err;
    }
    let meta = obj.metadata();
    if meta.generate_name.as_deref().unwrap_or("").is_empty() {
        return err;
    }
    // `errors.NewGenerateNameConflict(gr, name, 1)` (errors.go:176-191).
    Error::AlreadyExists(format!(
        "{qualified_resource} \"{}\" already exists, the server was not able to generate a unique name for the object",
        meta.name
    ))
}

// ---------------------------------------------------------------------------
// UpdatedObjectInfo (rest/rest.go:232-241, rest/update.go:170-228)
// ---------------------------------------------------------------------------

/// `rest.TransformFunc` (rest/update.go:170): produce the new object from the
/// one built so far (`None` when no object was supplied — a PATCH builds it
/// from `old`) and the currently stored object (`None` when it does not exist).
#[async_trait]
pub trait TransformFunc<T>: Send + Sync {
    async fn transform(&self, ctx: &RequestContext, new: Option<T>, old: Option<&T>) -> Result<T>;
}

/// `rest.UpdatedObjectInfo` (rest/rest.go:232-241).
#[async_trait]
pub trait UpdatedObjectInfo<T>: Send + Sync {
    /// Preconditions the stored object must satisfy before the update applies.
    fn preconditions(&self) -> Option<Preconditions>;
    /// The object to store, given the one currently stored. Called again on
    /// every retry of a conflicted write, so it must not consume its input.
    async fn updated_object(&self, ctx: &RequestContext, old: Option<&T>) -> Result<T>;
}

/// `rest.DefaultUpdatedObjectInfo` (rest/update.go:173-228).
///
/// The transformers may borrow request state (the admission chain, the
/// patch body) for `'a`, as upstream's closures capture it.
pub struct DefaultUpdatedObjectInfo<'a, T> {
    obj: Option<T>,
    transformers: Vec<Box<dyn TransformFunc<T> + 'a>>,
}

impl<'a, T> DefaultUpdatedObjectInfo<'a, T> {
    pub fn new(obj: Option<T>, transformers: Vec<Box<dyn TransformFunc<T> + 'a>>) -> Self {
        Self { obj, transformers }
    }
}

#[async_trait]
impl<T: Object> UpdatedObjectInfo<T> for DefaultUpdatedObjectInfo<'_, T> {
    /// `defaultUpdatedObjectInfo.Preconditions` (update.go:188-203): the UID of
    /// the supplied object, if it has one.
    fn preconditions(&self) -> Option<Preconditions> {
        let uid = &self.obj.as_ref()?.metadata().uid;
        if uid.is_empty() {
            return None;
        }
        Some(Preconditions {
            uid: Some(uid.clone()),
            resource_version: None,
        })
    }

    /// `defaultUpdatedObjectInfo.UpdatedObject` (update.go:207-228): a copy of
    /// the held object, passed through each transformer in order.
    async fn updated_object(&self, ctx: &RequestContext, old: Option<&T>) -> Result<T> {
        let mut new = self.obj.clone();
        for transformer in &self.transformers {
            new = Some(transformer.transform(ctx, new, old).await?);
        }
        new.ok_or_else(|| internal_error("no object was supplied and no transformer built one"))
    }
}

// ---------------------------------------------------------------------------
// BeforeDelete (rest/delete.go:75-175)
// ---------------------------------------------------------------------------

/// The zero `metav1.DeleteOptions`: every field unset.
///
/// Not `DeleteOptions::default()`, which pre-fills `propagationPolicy:
/// Background` and `gracePeriodSeconds: 30` — with those set, a DELETE that
/// named no policy would never fall through to the strategy's default
/// garbage-collection policy or the object's own GC finalizer, as upstream's
/// `shouldOrphanDependents` does (store.go:883-929).
pub fn zero_delete_options() -> DeleteOptions {
    DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: None,
        preconditions: None,
        orphan_dependents: None,
        dry_run: None,
        ignore_store_read_error_with_cluster_breaking_potential: None,
    }
}

/// What [`before_delete`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteDecision {
    /// The object must be updated with a deletion timestamp rather than
    /// removed now.
    pub graceful: bool,
    /// A graceful deletion is already under way and this request changes
    /// nothing.
    pub graceful_pending: bool,
}

/// `BeforeDelete` (rest/delete.go:75-175).
pub fn before_delete<T: Object>(
    strategy: &dyn RestDeleteStrategy<T>,
    qualified_resource: &GroupResource,
    ctx: &RequestContext,
    obj: &mut T,
    options: &mut DeleteOptions,
) -> Result<DeleteDecision> {
    const NOT_GRACEFUL: DeleteDecision = DeleteDecision {
        graceful: false,
        graceful_pending: false,
    };

    let errs = rusternetes_common::validation::metav1::validate_delete_options(options);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }

    // Checking the preconditions here to fail early. They are enforced again
    // when the deletion is actually written.
    if let Some(p) = &options.preconditions {
        let meta = obj.metadata();
        let conflict = |reason: String| {
            Error::Conflict(format!(
                "Operation cannot be fulfilled on {qualified_resource} \"{}\": {reason}",
                meta.name
            ))
        };
        if let Some(uid) = &p.uid {
            if *uid != meta.uid {
                return Err(conflict(format!(
                    "the UID in the precondition ({uid}) does not match the UID in record ({}). The object might have been deleted and then recreated",
                    meta.uid
                )));
            }
        }
        if let Some(rv) = &p.resource_version {
            let stored = meta.resource_version.as_deref().unwrap_or("");
            if rv != stored {
                return Err(conflict(format!(
                    "the ResourceVersion in the precondition ({rv}) does not match the ResourceVersion in record ({stored}). The object might have been modified"
                )));
            }
        }
    }

    // Negative values are treated as `1s` on the delete path.
    if options.grace_period_seconds.is_some_and(|g| g < 0) {
        options.grace_period_seconds = Some(1);
    }
    let meta = obj.metadata_mut();
    if meta.deletion_grace_period_seconds.is_some_and(|g| g < 0) {
        meta.deletion_grace_period_seconds = Some(1);
    }

    let Some(graceful_strategy) = strategy.graceful() else {
        // Not deleting gracefully, so there is no point updating generation:
        // the object is not updated before it is deleted.
        return Ok(NOT_GRACEFUL);
    };

    if let Some(deletion_timestamp) = meta.deletion_timestamp {
        // Already being deleted: the grace period may only be shortened. A
        // stored grace period of 0 means a previous delete updated the object
        // but failed to remove it; delete now to recover.
        let existing_period = match meta.deletion_grace_period_seconds {
            None | Some(0) => return Ok(NOT_GRACEFUL),
            Some(p) => p,
        };
        if let Some(mut period) = options.grace_period_seconds {
            if period >= existing_period {
                return Ok(DeleteDecision {
                    graceful: false,
                    graceful_pending: true,
                });
            }
            // Move the deletion timestamp back by the old grace period and
            // forward by the new one.
            let mut new_deletion_timestamp =
                deletion_timestamp - Duration::seconds(existing_period) + Duration::seconds(period);
            // Prevent shortening the grace period moving the timestamp into
            // the past.
            let now = now_seconds();
            if new_deletion_timestamp < now {
                new_deletion_timestamp = now;
                if period != 0 {
                    // A graceful deletion was requested but the whole grace
                    // period has already expired: shorten to the minimum while
                    // still treating this as graceful, so another actor makes
                    // the final delete with a zero grace period.
                    period = 1;
                }
            }
            meta.deletion_timestamp = Some(new_deletion_timestamp);
            meta.deletion_grace_period_seconds = Some(period);
            return Ok(DeleteDecision {
                graceful: true,
                graceful_pending: false,
            });
        }
        // Graceful deletion is pending; do nothing.
        options.grace_period_seconds = meta.deletion_grace_period_seconds;
        return Ok(DeleteDecision {
            graceful: false,
            graceful_pending: true,
        });
    }

    if !graceful_strategy.check_graceful_delete(ctx, obj, options) {
        return Ok(NOT_GRACEFUL);
    }
    let Some(period) = options.grace_period_seconds else {
        return Err(internal_error(
            "options.GracePeriodSeconds should not be nil",
        ));
    };

    let meta = obj.metadata_mut();
    meta.deletion_timestamp = Some(now_seconds() + Duration::seconds(period));
    meta.deletion_grace_period_seconds = Some(period);
    // The first graceful deletion sets the deletion timestamp, which changes
    // how the object's controllers behave, so it bumps generation (if set).
    // Non-graceful resources get this bump in the Store's `mark_as_deleting`.
    if let Some(generation) = meta.generation.filter(|g| *g > 0) {
        meta.generation = Some(generation + 1);
    }
    Ok(DeleteDecision {
        graceful: true,
        graceful_pending: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::ConfigMap;
    use rusternetes_common::validation::field::Error as FieldError;

    /// A strategy with no opinions, to exercise the common rules alone.
    struct Plain {
        namespaced: bool,
    }
    impl NamespaceScopedStrategy for Plain {
        fn namespace_scoped(&self) -> bool {
            self.namespaced
        }
    }
    impl RestCreateStrategy<ConfigMap> for Plain {
        fn prepare_for_create(&self, _: &RequestContext, _: &mut ConfigMap) {}
        fn validate(&self, _: &RequestContext, _: &ConfigMap) -> ErrorList {
            Vec::new()
        }
        fn warnings_on_create(&self, _: &RequestContext, _: &ConfigMap) -> Vec<String> {
            vec!["a warning".to_string()]
        }
    }
    impl RestUpdateStrategy<ConfigMap> for Plain {
        fn allow_create_on_update(&self) -> bool {
            false
        }
        fn prepare_for_update(&self, _: &RequestContext, _: &mut ConfigMap, _: &ConfigMap) {}
        fn validate_update(&self, _: &RequestContext, _: &ConfigMap, _: &ConfigMap) -> ErrorList {
            Vec::new()
        }
        fn allow_unconditional_update(&self) -> bool {
            true
        }
    }

    fn cm(name: &str) -> ConfigMap {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name},
        }))
        .unwrap()
    }

    /// `BeforeCreate` refuses an object the Store did not fill: it checks, it
    /// does not fill (create.go:102-104).
    #[test]
    fn before_create_requires_system_metadata() {
        let mut obj = cm("a");
        let err = before_create(
            &Plain { namespaced: true },
            &RequestContext::new(Some("ns")),
            &mut obj,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("system metadata was not initialized"),
            "{err}"
        );
    }

    /// The namespace comes from the request; a conflicting one in the body is
    /// a 400 (upstream `TestBeforeCreate`-style cases in meta_test.go).
    #[test]
    fn before_create_takes_the_request_namespace_and_rejects_a_mismatch() {
        let ctx = RequestContext::new(Some("ns"));
        let mut obj = cm("a");
        fill_object_meta_system_fields(&mut obj.metadata);
        before_create(&Plain { namespaced: true }, &ctx, &mut obj).unwrap();
        assert_eq!(obj.metadata.namespace.as_deref(), Some("ns"));
        assert_eq!(ctx.warnings(), vec!["a warning".to_string()]);

        let mut other = cm("b");
        other.metadata.namespace = Some("elsewhere".to_string());
        fill_object_meta_system_fields(&mut other.metadata);
        let err = before_create(&Plain { namespaced: true }, &ctx, &mut other).unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)), "{err:?}");
    }

    /// A cluster-scoped strategy clears a namespace the client sent.
    #[test]
    fn before_create_clears_the_namespace_of_a_cluster_scoped_object() {
        let mut obj = cm("a");
        obj.metadata.namespace = Some("ns".to_string());
        fill_object_meta_system_fields(&mut obj.metadata);
        before_create(
            &Plain { namespaced: false },
            &RequestContext::new(Some("ns")),
            &mut obj,
        )
        .unwrap();
        assert_eq!(obj.metadata.namespace, None);
    }

    /// `BeforeUpdate` restores what a client may not change, generation first
    /// (update.go:127-146).
    #[test]
    fn before_update_restores_server_owned_metadata() {
        let ctx = RequestContext::new(Some("ns"));
        let mut old = cm("a");
        old.metadata.namespace = Some("ns".to_string());
        old.metadata.uid = "uid-1".to_string();
        old.metadata.generation = Some(4);
        old.metadata.resource_version = Some("7".to_string());
        old.metadata.creation_timestamp = Some(now_seconds());
        old.metadata.deletion_timestamp = Some(now_seconds());
        old.metadata.deletion_grace_period_seconds = Some(30);

        let mut new = cm("a");
        new.metadata.resource_version = Some("7".to_string());
        new.metadata.generation = Some(1);
        before_update(&Plain { namespaced: true }, &ctx, &mut new, &old).unwrap();
        assert_eq!(new.metadata.uid, "uid-1");
        assert_eq!(new.metadata.generation, Some(4));
        assert_eq!(
            new.metadata.creation_timestamp,
            old.metadata.creation_timestamp
        );
        assert_eq!(
            new.metadata.deletion_timestamp,
            old.metadata.deletion_timestamp
        );
        assert_eq!(new.metadata.deletion_grace_period_seconds, Some(30));
    }

    /// A UID that does not match is not rewritten; it fails the common
    /// metadata validation as an immutable field.
    #[test]
    fn before_update_rejects_a_changed_uid() {
        let mut old = cm("a");
        old.metadata.namespace = Some("ns".to_string());
        old.metadata.uid = "uid-1".to_string();
        let mut new = cm("a");
        new.metadata.uid = "uid-2".to_string();
        new.metadata.resource_version = Some("1".to_string());
        let err = before_update(
            &Plain { namespaced: true },
            &RequestContext::new(Some("ns")),
            &mut new,
            &old,
        )
        .unwrap_err();
        let Error::Invalid(errs) = err else {
            panic!("{err:?}")
        };
        assert!(
            errs.iter().any(|e: &FieldError| e.field == "metadata.uid"),
            "{errs:?}"
        );
    }

    struct Graceful;
    impl RestGracefulDeleteStrategy<ConfigMap> for Graceful {
        fn check_graceful_delete(
            &self,
            _: &RequestContext,
            _: &ConfigMap,
            o: &mut DeleteOptions,
        ) -> bool {
            if o.grace_period_seconds.is_none() {
                o.grace_period_seconds = Some(30);
            }
            true
        }
    }
    struct GracefulDelete;
    impl RestDeleteStrategy<ConfigMap> for GracefulDelete {
        fn graceful(&self) -> Option<&dyn RestGracefulDeleteStrategy<ConfigMap>> {
            Some(&Graceful)
        }
    }
    struct PlainDelete;
    impl RestDeleteStrategy<ConfigMap> for PlainDelete {}

    fn gr() -> GroupResource {
        GroupResource::new("", "configmaps")
    }

    /// Upstream `TestBeforeDelete` (delete_test.go): a non-graceful strategy is
    /// never graceful, whatever the options say.
    #[test]
    fn before_delete_without_a_graceful_strategy_deletes_now() {
        let mut obj = cm("a");
        let mut opts = DeleteOptions {
            grace_period_seconds: Some(10),
            ..zero_delete_options()
        };
        let d = before_delete(
            &PlainDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap();
        assert_eq!(
            d,
            DeleteDecision {
                graceful: false,
                graceful_pending: false
            }
        );
        assert!(obj.metadata.deletion_timestamp.is_none());
    }

    /// First graceful delete stamps the timestamp and period and bumps a set
    /// generation.
    #[test]
    fn before_delete_first_graceful_delete_stamps_and_bumps_generation() {
        let mut obj = cm("a");
        obj.metadata.generation = Some(2);
        let mut opts = zero_delete_options();
        let d = before_delete(
            &GracefulDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap();
        assert!(d.graceful && !d.graceful_pending);
        assert_eq!(obj.metadata.deletion_grace_period_seconds, Some(30));
        assert!(obj.metadata.deletion_timestamp.is_some());
        assert_eq!(obj.metadata.generation, Some(3));
    }

    /// A pending graceful delete can be shortened, never lengthened.
    #[test]
    fn before_delete_only_shortens_a_pending_grace_period() {
        let start = now_seconds() + Duration::seconds(30);
        let mut obj = cm("a");
        obj.metadata.deletion_timestamp = Some(start);
        obj.metadata.deletion_grace_period_seconds = Some(30);

        let mut longer = DeleteOptions {
            grace_period_seconds: Some(60),
            ..zero_delete_options()
        };
        let d = before_delete(
            &GracefulDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut longer,
        )
        .unwrap();
        assert!(!d.graceful && d.graceful_pending);
        assert_eq!(obj.metadata.deletion_timestamp, Some(start));

        let mut shorter = DeleteOptions {
            grace_period_seconds: Some(10),
            ..zero_delete_options()
        };
        let d = before_delete(
            &GracefulDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut shorter,
        )
        .unwrap();
        assert!(d.graceful && !d.graceful_pending);
        assert_eq!(obj.metadata.deletion_grace_period_seconds, Some(10));
        assert_eq!(
            obj.metadata.deletion_timestamp,
            Some(start - Duration::seconds(20))
        );
    }

    /// A precondition that does not match the stored object is a Conflict,
    /// worded as upstream words it.
    #[test]
    fn before_delete_checks_preconditions() {
        let mut obj = cm("a");
        obj.metadata.uid = "real".to_string();
        let mut opts = DeleteOptions {
            preconditions: Some(Preconditions {
                uid: Some("stale".to_string()),
                resource_version: None,
            }),
            ..zero_delete_options()
        };
        let err = before_delete(
            &PlainDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Conflict: Operation cannot be fulfilled on configmaps \"a\": the UID in the precondition (stale) does not match the UID in record (real). The object might have been deleted and then recreated"
        );
    }

    #[test]
    fn group_resource_displays_like_upstream() {
        assert_eq!(
            GroupResource::new("", "configmaps").to_string(),
            "configmaps"
        );
        assert_eq!(
            GroupResource::new("apps", "deployments").to_string(),
            "deployments.apps"
        );
    }
}
