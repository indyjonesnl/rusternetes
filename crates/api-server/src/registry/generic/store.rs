//! Port of `genericregistry.Store`
//! (`staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`),
//! its dry-run storage wrapper (`dryrun.go`), and the retry loop of
//! `GuaranteedUpdate` it relies on (`staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go:463`).
//!
//! Upstream's storage layer offers a compare-and-swap `GuaranteedUpdate`;
//! Rusternetes' [`Storage`] trait offers `get` plus an `update` that is guarded
//! by the object's `resourceVersion` (etcd and rhino compare it; the memory
//! backend does not). [`Store::guaranteed_update`] builds the upstream loop out
//! of those two calls.
//!
//! Not yet ported, and added with the first resource that needs them:
//! `BeginCreate`/`AfterCreate`/`BeginUpdate`/`AfterUpdate`/`AfterDelete`/
//! `Decorator` hooks, TTLs, `ResetFieldsStrategy`, managed-fields timestamp
//! handling, and the `RetryGenerateName` retry loop.

use std::sync::Arc;

use async_trait::async_trait;

use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::types::{ObjectMeta, StatusDetails};
use rusternetes_common::validation::field::{Error as FieldError, Path};
use rusternetes_common::validation::objectmeta::name_is_path_segment;
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, Storage};
use tracing::debug;

use crate::registry::rest::{
    before_create, before_delete, before_update, check_generated_name_error,
    fill_object_meta_system_fields, zero_delete_options, GarbageCollectionPolicy, GroupResource,
    Object, RequestContext, RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    UpdatedObjectInfo, ValidateObject, ValidateObjectUpdate,
};

/// `OptimisticLockErrorMsg` (store.go:262).
pub const OPTIMISTIC_LOCK_ERROR_MSG: &str =
    "the object has been modified; please apply your changes to the latest version and try again";

/// `metav1.FinalizerOrphanDependents` (apimachinery/pkg/apis/meta/v1/types.go).
const FINALIZER_ORPHAN_DEPENDENTS: &str = "orphan";
/// `metav1.FinalizerDeleteDependents`.
const FINALIZER_DELETE_DEPENDENTS: &str = "foregroundDeletion";

/// The parts of `metav1.CreateOptions` the Store consults.
#[derive(Debug, Clone, Default)]
pub struct CreateOptions {
    /// `dryRun=All`.
    pub dry_run: bool,
}

/// The parts of `metav1.UpdateOptions` the Store consults.
#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    /// `dryRun=All`.
    pub dry_run: bool,
}

/// What `Store.Delete` returns: the object itself when it still exists
/// (pending finalizers or a graceful period) or when the store is configured
/// with `ReturnDeletedObject`, and otherwise the success `Status` built by
/// `finalizeDelete` (store.go:1386-1411).
#[derive(Debug, Clone)]
pub enum Deleted<T> {
    Object(T),
    Status(StatusDetails),
}

/// Store-specific extra condition for deleting during an update
/// (`Store.ShouldDeleteDuringUpdate`, store.go:183-186). Namespaces use it to
/// keep a namespace alive while `spec.finalizers` remain.
pub type ShouldDeleteDuringUpdateFn<T> = fn(&T, &T) -> bool;

/// `genericregistry.Store`, reduced to the fields a resource sets today.
pub struct Store<T: Object, S: Storage> {
    pub storage: Arc<S>,
    /// `DefaultQualifiedResource`: the identity in every error message.
    pub qualified_resource: GroupResource,
    /// The first segment of the storage key (`/registry/{this}/...`).
    /// Upstream derives it from the resource prefix; it is usually the
    /// resource name.
    pub storage_prefix: String,
    pub create_strategy: Arc<dyn RestCreateStrategy<T>>,
    pub update_strategy: Arc<dyn RestUpdateStrategy<T>>,
    pub delete_strategy: Arc<dyn RestDeleteStrategy<T>>,
    /// `EnableGarbageCollection`. The api-server always runs with the garbage
    /// collector enabled, which is upstream's default.
    pub enable_garbage_collection: bool,
    /// `ReturnDeletedObject`.
    pub return_deleted_object: bool,
    pub should_delete_during_update: Option<ShouldDeleteDuringUpdateFn<T>>,
}

impl<T: Object, S: Storage> Clone for Store<T, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            qualified_resource: self.qualified_resource.clone(),
            storage_prefix: self.storage_prefix.clone(),
            create_strategy: self.create_strategy.clone(),
            update_strategy: self.update_strategy.clone(),
            delete_strategy: self.delete_strategy.clone(),
            enable_garbage_collection: self.enable_garbage_collection,
            return_deleted_object: self.return_deleted_object,
            should_delete_during_update: self.should_delete_during_update,
        }
    }
}

/// Why the update callback stopped without producing an object to write. The
/// non-error variants are upstream's sentinel errors (store.go:873-875).
enum Abort<T> {
    Api(Error),
    /// `errEmptiedFinalizers`: the update removed the last finalizer from an
    /// object pending deletion, so it is deleted instead of written.
    EmptiedFinalizers(Box<T>),
    /// `errDeleteNow`.
    DeleteNow,
    /// `errAlreadyDeleting`.
    AlreadyDeleting,
}

impl<T> From<Error> for Abort<T> {
    fn from(e: Error) -> Self {
        Abort::Api(e)
    }
}

/// A storage-layer failure the Store raises itself, before `Interpret*Error`
/// turns it into the API error a client sees. Mirrors `storage.StorageError`
/// (`staging/src/k8s.io/apiserver/pkg/storage/errors.go:33-47`); the other
/// codes arrive from the [`Storage`] backend as [`Error`] variants.
enum StorageFailure {
    /// `ErrCodeInvalidObj`: a failed precondition.
    InvalidObj(String),
}

/// `storage.UpdateFunc`: the callback [`Store::guaranteed_update`] calls with
/// the current object. A trait rather than an async closure so the future it
/// returns is `Send` for every borrow of `existing` — an axum handler must be
/// `Send`, and rustc cannot prove that of an `AsyncFnMut` future yet
/// (rust-lang/rust#110338).
#[async_trait]
trait TryUpdate<T>: Send {
    async fn try_update(&mut self, existing: Option<&T>) -> std::result::Result<T, Abort<T>>;
}

impl<T: Object, S: Storage> Store<T, S> {
    /// A store for a resource whose create, update and delete strategies are
    /// the same object — the shape of every in-tree strategy.
    pub fn new<St>(storage: Arc<S>, qualified_resource: GroupResource, strategy: Arc<St>) -> Self
    where
        St: RestCreateStrategy<T> + RestUpdateStrategy<T> + RestDeleteStrategy<T> + 'static,
    {
        Self {
            storage,
            storage_prefix: qualified_resource.resource.clone(),
            qualified_resource,
            create_strategy: strategy.clone(),
            update_strategy: strategy.clone(),
            delete_strategy: strategy,
            enable_garbage_collection: true,
            return_deleted_object: false,
            should_delete_during_update: None,
        }
    }

    /// A copy of this store that updates with `strategy` — how a subresource
    /// store is built: `statusStore := *store; statusStore.UpdateStrategy =
    /// StatusStrategy` (e.g. apps/deployment/storage/storage.go:109-111).
    pub fn with_update_strategy(&self, strategy: Arc<dyn RestUpdateStrategy<T>>) -> Self {
        let mut store = self.clone();
        store.update_strategy = strategy;
        store
    }

    /// `KeyFunc`: `NamespaceKeyFunc` / `NoNamespaceKeyFunc` (store.go:280-307),
    /// chosen by the create strategy's scope as `CompleteWithOptions` does.
    pub fn key_func(&self, ctx: &RequestContext, name: &str) -> Result<String> {
        let namespace = if self.create_strategy.namespace_scoped() {
            match ctx.namespace.as_deref() {
                Some(ns) if !ns.is_empty() => Some(ns),
                _ => {
                    return Err(Error::BadRequest(
                        "Namespace parameter required.".to_string(),
                    ))
                }
            }
        } else {
            None
        };
        if name.is_empty() {
            return Err(Error::BadRequest("Name parameter required.".to_string()));
        }
        let msgs = name_is_path_segment(name, false);
        if !msgs.is_empty() {
            return Err(Error::BadRequest(format!(
                "Name parameter invalid: {name:?}: {}",
                msgs.join(";")
            )));
        }
        Ok(build_key(&self.storage_prefix, namespace, name))
    }

    // -- error interpretation (storage/errors/storage.go) -------------------

    pub(crate) fn not_found(&self, name: &str) -> Error {
        crate::registry::rest::not_found(&self.qualified_resource, name)
    }

    pub(crate) fn conflict(&self, name: &str, reason: impl std::fmt::Display) -> Error {
        crate::registry::rest::conflict(&self.qualified_resource, name, reason)
    }

    /// A storage error in the shape upstream's `StorageError.Error()` prints
    /// (storage/errors.go:123-126), for the conflict message that embeds it.
    fn storage_error_text(key: &str, failure: &StorageFailure) -> String {
        let (code, msg, extra) = match failure {
            StorageFailure::InvalidObj(detail) => (4, "invalid object", detail.as_str()),
        };
        format!(
            "StorageError: {msg}, Code: {code}, Key: {key}, ResourceVersion: 0, AdditionalErrorMsg: {extra}"
        )
    }

    /// Whether `err` came from the storage layer rather than being an API
    /// error already. Upstream tells them apart by type (`storage.IsNotFound`
    /// and friends match only a `*storage.StorageError`, so a `StatusError`
    /// raised by a strategy or admission passes through `Interpret*Error`
    /// untouched). Rusternetes' backends signal a storage error by carrying
    /// the raw `/registry/...` key as the message.
    fn is_storage_error(msg: &str) -> bool {
        msg.starts_with("/registry/")
    }

    /// `InterpretGetError` (storage/errors/storage.go:44-57).
    fn interpret_get_error(&self, err: Error, name: &str) -> Error {
        match err {
            Error::NotFound(msg) if Self::is_storage_error(&msg) => self.not_found(name),
            other => other,
        }
    }

    /// `InterpretCreateError` (storage/errors/storage.go:61-74).
    fn interpret_create_error(&self, err: Error, name: &str) -> Error {
        match err {
            // `errors.NewAlreadyExists(qualifiedResource, name)`.
            Error::AlreadyExists(msg) if Self::is_storage_error(&msg) => Error::AlreadyExists(
                format!("{} \"{name}\" already exists", self.qualified_resource),
            ),
            other => other,
        }
    }

    /// `InterpretUpdateError` (storage/errors/storage.go:78-93). Storage
    /// conflicts never reach here — [`Self::guaranteed_update`] retries them
    /// and raises failed preconditions as API conflicts itself.
    fn interpret_update_error(&self, err: Error, name: &str) -> Error {
        match err {
            Error::NotFound(msg) if Self::is_storage_error(&msg) => self.not_found(name),
            Error::AlreadyExists(msg) | Error::Conflict(msg) if Self::is_storage_error(&msg) => {
                self.conflict(name, msg)
            }
            other => other,
        }
    }

    /// `InterpretDeleteError` (storage/errors/storage.go:97-110): the same
    /// mapping as for an update.
    fn interpret_delete_error(&self, err: Error, name: &str) -> Error {
        self.interpret_update_error(err, name)
    }

    // -- storage primitives ------------------------------------------------

    /// `Preconditions.Check` (storage/interfaces.go:138-165). A missing object
    /// is checked as upstream's zero object: empty UID and resourceVersion.
    fn check_preconditions(
        preconditions: Option<&Preconditions>,
        obj: Option<&T>,
    ) -> std::result::Result<(), StorageFailure> {
        let Some(p) = preconditions else {
            return Ok(());
        };
        let empty = ObjectMeta::default();
        let meta = obj.map(|o| o.metadata()).unwrap_or(&empty);
        if let Some(uid) = &p.uid {
            if *uid != meta.uid {
                return Err(StorageFailure::InvalidObj(format!(
                    "Precondition failed: UID in precondition: {uid}, UID in object meta: {}",
                    meta.uid
                )));
            }
        }
        if let Some(rv) = &p.resource_version {
            let stored = meta.resource_version.as_deref().unwrap_or("");
            if rv != stored {
                return Err(StorageFailure::InvalidObj(format!(
                    "Precondition failed: ResourceVersion in precondition: {rv}, ResourceVersion in object meta: {stored}"
                )));
            }
        }
        Ok(())
    }

    /// `GuaranteedUpdate`, as upstream's storage offers it: read the current
    /// object, hand it to `try_update`, and write the result guarded by the
    /// revision that was read; when a concurrent writer wins, re-read and call
    /// `try_update` again (etcd3/store.go:463-640).
    ///
    /// With `dry_run` this is `DryRunnableStorage.GuaranteedUpdate`
    /// (dryrun.go:74-108): one read, the preconditions, one call, no write.
    ///
    /// A write that would store exactly what is already stored is skipped and
    /// the stored object returned, so a no-op update does not bump the
    /// resourceVersion (etcd3/store.go:553-577).
    async fn guaranteed_update(
        &self,
        key: &str,
        name: &str,
        ignore_not_found: bool,
        preconditions: Option<&Preconditions>,
        dry_run: bool,
        try_update: &mut dyn TryUpdate<T>,
    ) -> std::result::Result<T, Abort<T>> {
        loop {
            let current: Option<T> = match self.storage.get::<T>(key).await {
                Ok(obj) => Some(obj),
                Err(Error::NotFound(_)) if ignore_not_found => None,
                Err(Error::NotFound(_)) => return Err(Abort::Api(self.not_found(name))),
                Err(e) => return Err(Abort::Api(e)),
            };

            if let Err(failure) = Self::check_preconditions(preconditions, current.as_ref()) {
                return Err(Abort::Api(
                    self.conflict(name, Self::storage_error_text(key, &failure)),
                ));
            }

            let mut updated = try_update.try_update(current.as_ref()).await?;

            if dry_run {
                return Ok(updated);
            }

            let Some(current) = current else {
                match self.storage.create(key, &updated).await {
                    Ok(stored) => return Ok(stored),
                    // Someone created it between the read and the write.
                    Err(Error::AlreadyExists(_)) => continue,
                    Err(e) => return Err(Abort::Api(e)),
                }
            };

            // Guard the write with the revision that was read.
            updated.metadata_mut().resource_version = current.metadata().resource_version.clone();
            if is_same_object(&updated, &current) {
                return Ok(current);
            }
            match self.storage.update(key, &updated).await {
                Ok(stored) => return Ok(stored),
                Err(Error::Conflict(msg)) => {
                    debug!("{key}: write lost a race ({msg}); retrying on the current object");
                    continue;
                }
                // Deleted between the read and the write: the next read
                // decides whether that is NotFound or a create.
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(Abort::Api(e)),
            }
        }
    }

    /// `Storage.Delete` with preconditions and a validation callback
    /// (`etcd3/store.go` `conditionalDelete`, and `DryRunnableStorage.Delete`,
    /// dryrun.go:49-60).
    ///
    /// The [`Storage`] trait has no guarded delete, so the check and the
    /// delete are two calls; a write landing between them is deleted along
    /// with the object. Upstream closes that gap with a revision-guarded
    /// delete.
    async fn storage_delete(
        &self,
        key: &str,
        name: &str,
        ctx: &RequestContext,
        preconditions: Option<&Preconditions>,
        delete_validation: Option<&dyn ValidateObject<T>>,
        dry_run: bool,
    ) -> Result<T> {
        let current: T = self.storage.get(key).await?;
        if let Err(failure) = Self::check_preconditions(preconditions, Some(&current)) {
            return Err(self.conflict(name, Self::storage_error_text(key, &failure)));
        }
        if let Some(v) = delete_validation {
            v.validate(ctx, &current).await?;
        }
        if !dry_run {
            self.storage.delete(key).await?;
        }
        Ok(current)
    }

    // -- Get ---------------------------------------------------------------

    /// `Store.Get` (store.go:833-845).
    pub async fn get(&self, ctx: &RequestContext, name: &str) -> Result<T> {
        let key = self.key_func(ctx, name)?;
        self.storage
            .get(&key)
            .await
            .map_err(|e| self.interpret_get_error(e, name))
    }

    // -- Create ------------------------------------------------------------

    /// `Store.Create` → `Store.create` (store.go:446-558).
    ///
    /// `create_validation` is the validating half of admission; it sees the
    /// object after the strategy has prepared and validated it, just before
    /// the write.
    pub async fn create(
        &self,
        ctx: &RequestContext,
        mut obj: T,
        create_validation: Option<&dyn ValidateObject<T>>,
        options: &CreateOptions,
    ) -> Result<T> {
        // Init metadata as early as possible.
        {
            let meta = obj.metadata_mut();
            fill_object_meta_system_fields(meta);
            if let Some(base) = meta.generate_name.as_deref().filter(|g| !g.is_empty()) {
                if meta.name.is_empty() {
                    meta.name = self.create_strategy.generate_name(base);
                }
            }
        }

        before_create(self.create_strategy.as_ref(), ctx, &mut obj)?;

        // At this point the object is fully formed: run the validators the
        // handler chain wants to enforce.
        if let Some(v) = create_validation {
            v.validate(ctx, &obj).await?;
        }

        let name = obj.metadata().name.clone();
        let key = self.key_func(ctx, &name)?;

        let result = if options.dry_run {
            // `DryRunnableStorage.Create` (dryrun.go:39-47).
            match self.storage.get::<T>(&key).await {
                Ok(_) => Err(Error::AlreadyExists(key.clone())),
                Err(Error::NotFound(_)) => Ok(obj.clone()),
                Err(e) => Err(e),
            }
        } else {
            self.storage.create(&key, &obj).await
        };

        match result {
            Ok(out) => Ok(out),
            Err(err) => {
                let err = self.interpret_create_error(err, &name);
                let err = check_generated_name_error(&self.qualified_resource, err, &obj);
                let Error::AlreadyExists(msg) = err else {
                    return Err(err);
                };
                // Tell the client when the name is taken by an object that is
                // on its way out (store.go:532-542).
                match self.storage.get::<T>(&key).await {
                    Ok(existing) if existing.metadata().deletion_timestamp.is_some() => Err(
                        Error::AlreadyExists(format!("object is being deleted: {msg}")),
                    ),
                    _ => Err(Error::AlreadyExists(msg)),
                }
            }
        }
    }

    // -- Update ------------------------------------------------------------

    /// `Store.Update` (store.go:617-822). Returns the stored object and
    /// whether the update created it.
    ///
    /// `force_allow_create` is server-side apply's create-on-update, which
    /// applies even to resources whose strategy refuses it.
    #[allow(clippy::too_many_arguments)]
    pub async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<T>,
        create_validation: Option<&dyn ValidateObject<T>>,
        update_validation: Option<&dyn ValidateObjectUpdate<T>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(T, bool)> {
        let key = self.key_func(ctx, name)?;
        let preconditions = obj_info.preconditions();
        let allow_create = self.update_strategy.allow_create_on_update() || force_allow_create;

        let mut attempt = UpdateAttempt {
            store: self,
            ctx,
            name,
            obj_info,
            allow_create,
            create_validation,
            update_validation,
            creating: false,
            creating_obj: None,
        };
        let outcome = self
            .guaranteed_update(
                &key,
                name,
                allow_create,
                preconditions.as_ref(),
                options.dry_run,
                &mut attempt,
            )
            .await;
        let UpdateAttempt {
            creating,
            creating_obj,
            ..
        } = attempt;

        match outcome {
            Ok(out) => Ok((out, creating)),
            Err(Abort::EmptiedFinalizers(obj)) => {
                // `newDeleteOptionsFromUpdateOptions` (store.go:836-844).
                let delete_options = DeleteOptions {
                    dry_run: options.dry_run.then(|| vec!["All".to_string()]),
                    ..zero_delete_options()
                };
                self.delete_without_finalizers(
                    ctx,
                    name,
                    &key,
                    *obj,
                    preconditions.as_ref(),
                    &delete_options,
                )
                .await
                .map(|obj| (obj, false))
            }
            Err(Abort::Api(err)) => {
                if creating {
                    let err = self.interpret_create_error(err, name);
                    Err(match creating_obj.as_ref() {
                        Some(obj) => check_generated_name_error(&self.qualified_resource, err, obj),
                        None => err,
                    })
                } else {
                    Err(self.interpret_update_error(err, name))
                }
            }
            Err(Abort::DeleteNow | Abort::AlreadyDeleting) => {
                unreachable!("the update callback never returns a delete sentinel")
            }
        }
    }

    /// `deleteWithoutFinalizers` (store.go:588-613): the update emptied the
    /// finalizers of an object pending deletion, so it goes now. The request
    /// already passed admission as an UPDATE, so no delete validation runs.
    async fn delete_without_finalizers(
        &self,
        ctx: &RequestContext,
        name: &str,
        key: &str,
        obj: T,
        preconditions: Option<&Preconditions>,
        options: &DeleteOptions,
    ) -> Result<T> {
        match self
            .storage_delete(key, name, ctx, preconditions, None, is_dry_run(options))
            .await
        {
            // Clients expect the updated object from a successful PUT, not
            // the Status `finalizeDelete` would build.
            Ok(_) => Ok(obj),
            // Deletion is racy: several updates may each remove the last
            // finalizer.
            Err(Error::NotFound(_)) => Ok(obj),
            Err(e) => Err(self.interpret_delete_error(e, name)),
        }
    }

    // -- Delete ------------------------------------------------------------

    /// `Store.Delete` (store.go:1131-1220). Returns the response object and
    /// whether the object was removed from storage.
    ///
    /// `delete_validation` is the validating half of admission for DELETE; it
    /// sees the stored object.
    pub async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<T>>,
        mut options: DeleteOptions,
    ) -> Result<(Deleted<T>, bool)> {
        let key = self.key_func(ctx, name)?;
        let mut obj: T = self
            .storage
            .get(&key)
            .await
            .map_err(|e| self.interpret_delete_error(e, name))?;

        let mut preconditions = options.preconditions.clone();
        let decision = before_delete(
            self.delete_strategy.as_ref(),
            &self.qualified_resource,
            ctx,
            &mut obj,
            &mut options,
        )?;
        // Finalizers cannot be changed through DeleteOptions once a deletion
        // is pending.
        if decision.graceful_pending {
            return Ok((self.finalize_delete(obj), false));
        }

        let pending_finalizers = obj
            .metadata()
            .finalizers
            .as_ref()
            .is_some_and(|f| !f.is_empty());
        let mut ignore_not_found = false;
        let mut delete_immediately = true;
        let mut last_existing: Option<T> = None;
        let mut out: Option<T> = None;

        let (should_update_finalizers, _) =
            self.deletion_finalizers_for_garbage_collection(ctx, obj.metadata(), &options);
        if decision.graceful || pending_finalizers || should_update_finalizers {
            let r = self
                .update_for_graceful_deletion_and_finalizers(
                    ctx,
                    name,
                    &key,
                    &mut options,
                    preconditions.as_ref(),
                    delete_validation,
                    obj,
                )
                .await?;
            match r {
                GracefulOutcome::Finalized(deleted) => return Ok((deleted, false)),
                GracefulOutcome::Updated {
                    ignore_not_found: inf,
                    delete_immediately: di,
                    out: o,
                    last_existing: le,
                } => {
                    ignore_not_found = inf;
                    delete_immediately = di;
                    // The object was just written, so a resourceVersion
                    // precondition now refers to the new revision.
                    if delete_immediately {
                        if let Some(p) = preconditions.as_mut() {
                            if p.resource_version.is_some() {
                                p.resource_version = o.metadata().resource_version.clone();
                            }
                        }
                    }
                    out = Some(o);
                    last_existing = le;
                }
            }
        }

        if !delete_immediately {
            let out = out.expect("an update that defers deletion returns the object");
            return Ok((Deleted::Object(out), false));
        }

        // A dry-run update above already produced the object as it would be
        // just before deletion; reading storage again would lose its
        // deletionTimestamp and finalizers.
        if is_dry_run(&options) {
            if let Some(out) = out {
                return Ok((Deleted::Object(out), true));
            }
        }

        match self
            .storage_delete(
                &key,
                name,
                ctx,
                preconditions.as_ref(),
                delete_validation,
                is_dry_run(&options),
            )
            .await
        {
            Ok(deleted) => Ok((self.finalize_delete(deleted), true)),
            Err(Error::NotFound(_)) if ignore_not_found && last_existing.is_some() => {
                // Another component won a graceless-delete race; the last
                // state seen is the best approximation of what was deleted.
                Ok((self.finalize_delete(last_existing.unwrap()), true))
            }
            Err(e) => Err(self.interpret_delete_error(e, name)),
        }
    }

    /// `updateForGracefulDeletionAndFinalizers` (store.go:1031-1128).
    #[allow(clippy::too_many_arguments)]
    async fn update_for_graceful_deletion_and_finalizers(
        &self,
        ctx: &RequestContext,
        name: &str,
        key: &str,
        options: &mut DeleteOptions,
        preconditions: Option<&Preconditions>,
        delete_validation: Option<&dyn ValidateObject<T>>,
        input: T,
    ) -> Result<GracefulOutcome<T>> {
        let dry_run = is_dry_run(options);
        let mut attempt = GracefulAttempt {
            store: self,
            ctx,
            options,
            delete_validation,
            last_graceful: 0,
            pending_finalizers: false,
            last_existing: None,
        };
        let outcome = self
            .guaranteed_update(key, name, false, preconditions, dry_run, &mut attempt)
            .await;
        let GracefulAttempt {
            last_graceful,
            pending_finalizers,
            last_existing,
            ..
        } = attempt;

        match outcome {
            Ok(out) => {
                // Pending finalizers never delete immediately, nor does a
                // grace period that has not run out.
                if pending_finalizers || last_graceful > 0 {
                    return Ok(GracefulOutcome::Updated {
                        ignore_not_found: false,
                        delete_immediately: false,
                        out,
                        last_existing,
                    });
                }
                // A graceful strategy deleting gracelessly races other
                // components; tolerate NotFound (kubernetes#19403).
                Ok(GracefulOutcome::Updated {
                    ignore_not_found: true,
                    delete_immediately: true,
                    out,
                    last_existing,
                })
            }
            Err(Abort::DeleteNow) => {
                // Zero grace period (or already at 0): fall through to the
                // real delete. Upstream's `out` here is the empty object.
                Ok(GracefulOutcome::Updated {
                    ignore_not_found: false,
                    delete_immediately: true,
                    out: input,
                    last_existing,
                })
            }
            Err(Abort::AlreadyDeleting) => {
                Ok(GracefulOutcome::Finalized(self.finalize_delete(input)))
            }
            Err(Abort::Api(err)) => Err(self.interpret_update_error(err, name)),
            Err(Abort::EmptiedFinalizers(_)) => {
                unreachable!("the delete callback never empties finalizers")
            }
        }
    }

    /// `deletionFinalizersForGarbageCollection` (store.go:976-1004), with
    /// `shouldOrphanDependents` (store.go:883-929) and
    /// `shouldDeleteDependents` (store.go:935-969). Returns whether the
    /// finalizer list changed, and the list to store.
    fn deletion_finalizers_for_garbage_collection(
        &self,
        ctx: &RequestContext,
        meta: &ObjectMeta,
        options: &DeleteOptions,
    ) -> (bool, Vec<String>) {
        let existing: Vec<String> = meta.finalizers.clone().unwrap_or_default();
        if !self.enable_garbage_collection {
            return (false, Vec::new());
        }
        let policy = self.delete_strategy.default_garbage_collection_policy(ctx);
        let should_orphan = should_orphan_dependents(policy, &existing, options);
        let should_delete_dependents = should_delete_dependents(policy, &existing, options);

        let mut new_finalizers: Vec<String> = existing
            .iter()
            .filter(|f| {
                f.as_str() != FINALIZER_ORPHAN_DEPENDENTS
                    && f.as_str() != FINALIZER_DELETE_DEPENDENTS
            })
            .cloned()
            .collect();
        if should_orphan {
            new_finalizers.push(FINALIZER_ORPHAN_DEPENDENTS.to_string());
        }
        if should_delete_dependents {
            new_finalizers.push(FINALIZER_DELETE_DEPENDENTS.to_string());
        }

        let old: std::collections::BTreeSet<&String> = existing.iter().collect();
        let new: std::collections::BTreeSet<&String> = new_finalizers.iter().collect();
        if old == new {
            return (false, existing);
        }
        (true, new_finalizers)
    }

    /// `Store.DeleteCollection` (store.go:1237-1384): delete every listed
    /// item, ignoring the ones already gone, and return the list.
    ///
    /// Upstream lists through `Store.List` with the request's `ListOptions`;
    /// the caller passes the listed, selector-filtered items instead, since
    /// list and selector handling live in the handlers today. Items are
    /// deleted one at a time, which is upstream's default
    /// (`DeleteCollectionWorkers: 1`, server/options/etcd.go:82). Like
    /// upstream it is not atomic: an error stops the sweep with some items
    /// already deleted.
    pub async fn delete_collection(
        &self,
        ctx: &RequestContext,
        items: Vec<T>,
        delete_validation: Option<&dyn ValidateObject<T>>,
        options: &DeleteOptions,
    ) -> Result<Vec<T>> {
        for item in &items {
            // Each delete gets its own copy of the options: a graceful
            // strategy may rewrite them (store.go:1275-1279).
            match self
                .delete(
                    ctx,
                    &item.metadata().name,
                    delete_validation,
                    options.clone(),
                )
                .await
            {
                Ok(_) | Err(Error::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(items)
    }

    /// `finalizeDelete` (store.go:1386-1411).
    fn finalize_delete(&self, obj: T) -> Deleted<T> {
        if self.return_deleted_object {
            return Deleted::Object(obj);
        }
        let meta = obj.metadata();
        Deleted::Status(StatusDetails {
            name: Some(meta.name.clone()),
            group: Some(self.qualified_resource.group.clone()),
            // "Yes we set Kind field to resource." (store.go:1406)
            kind: Some(self.qualified_resource.resource.clone()),
            uid: (!meta.uid.is_empty()).then(|| meta.uid.clone()),
            causes: None,
            retry_after_seconds: None,
        })
    }
}

/// The `tryUpdate` closure of `Store.Update` (store.go:655-786).
struct UpdateAttempt<'a, T: Object, S: Storage> {
    store: &'a Store<T, S>,
    ctx: &'a RequestContext,
    name: &'a str,
    obj_info: &'a dyn UpdatedObjectInfo<T>,
    allow_create: bool,
    create_validation: Option<&'a dyn ValidateObject<T>>,
    update_validation: Option<&'a dyn ValidateObjectUpdate<T>>,
    /// Whether the last attempt was a create, and the object it created
    /// before `BeforeCreate` — for the error interpretation afterwards.
    creating: bool,
    creating_obj: Option<T>,
}

#[async_trait]
impl<T: Object, S: Storage> TryUpdate<T> for UpdateAttempt<'_, T, S> {
    async fn try_update(&mut self, existing: Option<&T>) -> std::result::Result<T, Abort<T>> {
        if existing.is_none() && !self.allow_create {
            return Err(Abort::Api(self.store.not_found(self.name)));
        }

        // Given the existing object, get the new object.
        let mut obj = self.obj_info.updated_object(self.ctx, existing).await?;

        // An update that names no resourceVersion is applied to
        // the latest object when the strategy allows it, and is
        // otherwise rejected; one that names a stale version
        // conflicts.
        let new_rv = parse_resource_version(obj.metadata())?;
        let unconditional = new_rv == 0 && self.store.update_strategy.allow_unconditional_update();

        let Some(existing) = existing else {
            // Create on update.
            fill_object_meta_system_fields(obj.metadata_mut());
            self.creating = true;
            self.creating_obj = Some(obj.clone());
            before_create(self.store.create_strategy.as_ref(), self.ctx, &mut obj)?;
            if let Some(v) = self.create_validation {
                v.validate(self.ctx, &obj).await?;
            }
            return Ok(obj);
        };

        self.creating = false;
        self.creating_obj = None;
        if unconditional {
            obj.metadata_mut().resource_version = existing.metadata().resource_version.clone();
        } else if new_rv == 0 {
            return Err(Abort::Api(Error::Invalid(vec![FieldError::invalid(
                &Path::new("metadata").child("resourceVersion"),
                0i64,
                "must be specified for an update",
            )])));
        } else if new_rv != parse_resource_version(existing.metadata())? {
            return Err(Abort::Api(
                self.store.conflict(self.name, OPTIMISTIC_LOCK_ERROR_MSG),
            ));
        }

        before_update(
            self.store.update_strategy.as_ref(),
            self.ctx,
            &mut obj,
            existing,
        )?;

        if let Some(v) = self.update_validation {
            v.validate(self.ctx, &obj, existing).await?;
        }

        if should_delete_during_update(&obj, existing)
            && self
                .store
                .should_delete_during_update
                .is_none_or(|extra| extra(&obj, existing))
        {
            return Err(Abort::EmptiedFinalizers(Box::new(obj)));
        }
        Ok(obj)
    }
}

/// The `tryUpdate` closure of `updateForGracefulDeletionAndFinalizers`
/// (store.go:1044-1098).
struct GracefulAttempt<'a, T: Object, S: Storage> {
    store: &'a Store<T, S>,
    ctx: &'a RequestContext,
    options: &'a mut DeleteOptions,
    delete_validation: Option<&'a dyn ValidateObject<T>>,
    last_graceful: i64,
    pending_finalizers: bool,
    last_existing: Option<T>,
}

#[async_trait]
impl<T: Object, S: Storage> TryUpdate<T> for GracefulAttempt<'_, T, S> {
    async fn try_update(&mut self, existing: Option<&T>) -> std::result::Result<T, Abort<T>> {
        let mut existing = existing
            .expect("the object exists: ignore_not_found is false")
            .clone();
        if let Some(v) = self.delete_validation {
            v.validate(self.ctx, &existing).await?;
        }
        let decision = before_delete(
            self.store.delete_strategy.as_ref(),
            &self.store.qualified_resource,
            self.ctx,
            &mut existing,
            self.options,
        )?;
        if decision.graceful_pending {
            return Err(Abort::AlreadyDeleting);
        }

        // Add/remove the GC finalizers as the options dictate.
        // This comes after the pending-graceful check, so the
        // finalizers cannot be changed once deletion has started.
        let (needs_update, new_finalizers) = self.store.deletion_finalizers_for_garbage_collection(
            self.ctx,
            existing.metadata(),
            self.options,
        );
        if needs_update {
            existing.metadata_mut().finalizers =
                (!new_finalizers.is_empty()).then_some(new_finalizers);
        }

        self.pending_finalizers = existing
            .metadata()
            .finalizers
            .as_ref()
            .is_some_and(|f| !f.is_empty());
        if !decision.graceful {
            // Not graceful but held by finalizers: mark it as
            // deleting with a zero grace period.
            if self.pending_finalizers {
                mark_as_deleting(existing.metadata_mut(), chrono::Utc::now());
                return Ok(existing);
            }
            return Err(Abort::DeleteNow);
        }
        self.last_graceful = self.options.grace_period_seconds.unwrap_or(0);
        self.last_existing = Some(existing.clone());
        Ok(existing)
    }
}

/// What `updateForGracefulDeletionAndFinalizers` hands back to `Delete`.
enum GracefulOutcome<T> {
    /// `errAlreadyDeleting`: the response is ready.
    Finalized(Deleted<T>),
    Updated {
        ignore_not_found: bool,
        delete_immediately: bool,
        out: T,
        last_existing: Option<T>,
    },
}

/// `dryrun.IsDryRun(options.DryRun)`.
fn is_dry_run(options: &DeleteOptions) -> bool {
    options.dry_run.as_ref().is_some_and(|d| !d.is_empty())
}

/// `APIObjectVersioner.ObjectResourceVersion` / `ParseResourceVersion`
/// (storage/api_object_versioner.go:74-101): `""` and `"0"` are unset.
fn parse_resource_version(meta: &ObjectMeta) -> Result<u64> {
    let rv = meta.resource_version.as_deref().unwrap_or("");
    if rv.is_empty() || rv == "0" {
        return Ok(0);
    }
    rv.parse::<u64>().map_err(|e| {
        Error::Invalid(vec![FieldError::invalid(
            &Path::new("resourceVersion"),
            rv.to_string(),
            e.to_string(),
        )])
    })
}

/// Whether two objects serialize identically — etcd3's `GuaranteedUpdate`
/// compares the encoded bytes to skip a no-op write. Compared as JSON values
/// rather than bytes because map fields (`labels`, `data`, ...) are Rust
/// `HashMap`s whose serialization order is not stable between two equal maps;
/// Go's encoder sorts map keys, so upstream's byte comparison is order-free too.
fn is_same_object<T: Object>(a: &T, b: &T) -> bool {
    match (serde_json::to_value(a), serde_json::to_value(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// `ShouldDeleteDuringUpdate` (store.go:565-586).
pub fn should_delete_during_update<T: Object>(obj: &T, existing: &T) -> bool {
    let new_meta = obj.metadata();
    let old_meta = existing.metadata();
    if new_meta.finalizers.as_ref().is_some_and(|f| !f.is_empty()) {
        return false;
    }
    if old_meta.deletion_timestamp.is_none() {
        return false;
    }
    matches!(old_meta.deletion_grace_period_seconds, None | Some(0))
}

/// `markAsDeleting` (store.go:1009-1029).
fn mark_as_deleting(meta: &mut ObjectMeta, now: chrono::DateTime<chrono::Utc>) {
    use chrono::SubsecRound;
    let now = now.trunc_subsecs(0);
    // The generation bump for resources without graceful deletion; graceful
    // ones get it in `before_delete`.
    if meta.deletion_timestamp.is_none() {
        if let Some(g) = meta.generation.filter(|g| *g > 0) {
            meta.generation = Some(g + 1);
        }
    }
    if meta
        .deletion_timestamp
        .is_none_or(|existing| existing > now)
    {
        meta.deletion_timestamp = Some(now);
    }
    meta.deletion_grace_period_seconds = Some(0);
}

/// `shouldOrphanDependents` (store.go:883-929).
fn should_orphan_dependents(
    policy: Option<GarbageCollectionPolicy>,
    finalizers: &[String],
    options: &DeleteOptions,
) -> bool {
    if policy == Some(GarbageCollectionPolicy::Unsupported) {
        return false;
    }
    // An explicit policy set at deletion time overrides everything.
    if let Some(orphan) = options.orphan_dependents {
        return orphan;
    }
    use rusternetes_common::types::DeletionPropagation;
    match options.propagation_policy {
        Some(DeletionPropagation::Orphan) => return true,
        Some(DeletionPropagation::Background | DeletionPropagation::Foreground) => return false,
        None => {}
    }
    // A finalizer already on the object overrides the default.
    for f in finalizers {
        match f.as_str() {
            FINALIZER_ORPHAN_DEPENDENTS => return true,
            FINALIZER_DELETE_DEPENDENTS => return false,
            _ => {}
        }
    }
    policy == Some(GarbageCollectionPolicy::OrphanDependents)
}

/// `shouldDeleteDependents` (store.go:935-969).
fn should_delete_dependents(
    policy: Option<GarbageCollectionPolicy>,
    finalizers: &[String],
    options: &DeleteOptions,
) -> bool {
    if policy == Some(GarbageCollectionPolicy::Unsupported) {
        return false;
    }
    if options.orphan_dependents.is_some() {
        return false;
    }
    use rusternetes_common::types::DeletionPropagation;
    match options.propagation_policy {
        Some(DeletionPropagation::Foreground) => return true,
        Some(DeletionPropagation::Background | DeletionPropagation::Orphan) => return false,
        None => {}
    }
    for f in finalizers {
        match f.as_str() {
            FINALIZER_DELETE_DEPENDENTS => return true,
            FINALIZER_ORPHAN_DEPENDENTS => return false,
            _ => {}
        }
    }
    false
}

/// The Store serves every verb (store.go's `Store` implements all of
/// `rest.StandardStorage`).
#[async_trait]
impl<T: Object, S: Storage + Send + Sync + 'static> crate::registry::rest::RestStorage<T>
    for Store<T, S>
{
    fn qualified_resource(&self) -> &GroupResource {
        &self.qualified_resource
    }

    fn namespace_scoped(&self) -> bool {
        self.create_strategy.namespace_scoped()
    }

    async fn get(&self, ctx: &RequestContext, name: &str) -> Result<T> {
        Store::get(self, ctx, name).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: T,
        create_validation: Option<&dyn ValidateObject<T>>,
        options: &CreateOptions,
    ) -> Result<T> {
        Store::create(self, ctx, obj, create_validation, options).await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<T>,
        create_validation: Option<&dyn ValidateObject<T>>,
        update_validation: Option<&dyn ValidateObjectUpdate<T>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(T, bool)> {
        Store::update(
            self,
            ctx,
            name,
            obj_info,
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
        delete_validation: Option<&dyn ValidateObject<T>>,
        options: DeleteOptions,
    ) -> Result<(Deleted<T>, bool)> {
        Store::delete(self, ctx, name, delete_validation, options).await
    }

    /// `Store.DeleteCollection` lists with the request's options, then
    /// deletes each item (store.go:1237-1384).
    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<T>>,
        options: &DeleteOptions,
        list_options: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<T>> {
        let prefix =
            rusternetes_storage::build_prefix(&self.storage_prefix, ctx.namespace.as_deref());
        let mut items: Vec<T> = self.storage.list(&prefix).await?;
        crate::handlers::filtering::apply_selectors(&mut items, list_options)?;
        Store::delete_collection(self, ctx, items, delete_validation, options).await
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
