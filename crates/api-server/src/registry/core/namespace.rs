//! Namespace strategies and storage — port of
//! `pkg/registry/core/namespace/strategy.go` and
//! `pkg/registry/core/namespace/storage/storage.go`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::resources::{Namespace, NamespaceSpec, NamespaceStatus};
use rusternetes_common::types::{DeletionPropagation, Phase};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::namespace::{
    validate_namespace, validate_namespace_finalize_update, validate_namespace_status_update,
    validate_namespace_update,
};
use rusternetes_common::Result;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::{CreateOptions, Deleted, Store, UpdateOptions};
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject, ValidateObjectUpdate,
};

/// `core.FinalizerKubernetes`.
pub const FINALIZER_KUBERNETES: &str = "kubernetes";
/// `v1.LabelMetadataName`.
const LABEL_METADATA_NAME: &str = "kubernetes.io/metadata.name";
/// `metav1.FinalizerOrphanDependents`.
const FINALIZER_ORPHAN_DEPENDENTS: &str = "orphan";
/// `metav1.FinalizerDeleteDependents`.
const FINALIZER_DELETE_DEPENDENTS: &str = "foregroundDeletion";

/// `SetDefaults_Namespace` and `SetDefaults_NamespaceStatus`
/// (pkg/apis/core/v1/defaults.go:326-345): a named namespace carries its
/// name label, and an empty phase is `Active`. `NamespaceStatus` is a struct
/// upstream, never absent.
pub fn convert_to_internal(ns: &mut Namespace) {
    set_name_label(ns);
    let status = ns.status.get_or_insert_with(NamespaceStatus::default);
    if status.phase.is_none() {
        status.phase = Some(Phase::Active);
    }
}

fn set_name_label(ns: &mut Namespace) {
    if !ns.metadata.name.is_empty() {
        let name = ns.metadata.name.clone();
        ns.metadata
            .labels
            .get_or_insert_with(HashMap::new)
            .insert(LABEL_METADATA_NAME.to_string(), name);
    }
}

fn spec_finalizers(ns: &Namespace) -> &[String] {
    ns.spec
        .as_ref()
        .and_then(|s| s.finalizers.as_deref())
        .unwrap_or_default()
}

/// `namespaceStrategy` (strategy.go:36-44).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

/// `Canonicalize` (strategy.go:106-131), which every namespace strategy
/// inherits: the name label is always the name.
fn canonicalize(ns: &mut Namespace) {
    set_name_label(ns);
}

impl RestCreateStrategy<Namespace> for Strategy {
    /// `PrepareForCreate` (strategy.go:62-85): status is `Active`, and the
    /// `kubernetes` finalizer is appended to `spec.finalizers`.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Namespace) {
        obj.status = Some(NamespaceStatus {
            phase: Some(Phase::Active),
            conditions: None,
        });
        let finalizers = obj
            .spec
            .get_or_insert_with(NamespaceSpec::default)
            .finalizers
            .get_or_insert_with(Vec::new);
        if !finalizers.iter().any(|f| f == FINALIZER_KUBERNETES) {
            finalizers.push(FINALIZER_KUBERNETES.to_string());
        }
    }

    /// strategy.go:95-99.
    fn validate(&self, _ctx: &RequestContext, obj: &Namespace) -> ErrorList {
        validate_namespace(obj)
    }

    fn canonicalize(&self, obj: &mut Namespace) {
        canonicalize(obj);
    }
}

impl RestUpdateStrategy<Namespace> for Strategy {
    /// strategy.go:132-136.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:87-93): `spec.finalizers` only change
    /// through `/finalize`, status only through `/status`.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Namespace, old: &Namespace) {
        let finalizers = old.spec.as_ref().and_then(|s| s.finalizers.clone());
        obj.spec
            .get_or_insert_with(NamespaceSpec::default)
            .finalizers = finalizers;
        obj.status = old.status.clone();
    }

    /// strategy.go:137-142.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &Namespace,
        old: &Namespace,
    ) -> ErrorList {
        let mut errs = validate_namespace(obj);
        errs.extend(validate_namespace_update(obj, old));
        errs
    }

    fn canonicalize(&self, obj: &mut Namespace) {
        canonicalize(obj);
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// Namespaces use the default delete strategy.
impl RestDeleteStrategy<Namespace> for Strategy {}

/// `namespaceStatusStrategy` (strategy.go:152-182): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<Namespace> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:167-172: only status may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Namespace, old: &Namespace) {
        obj.spec = old.spec.clone();
    }

    /// strategy.go:173-177.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &Namespace,
        old: &Namespace,
    ) -> ErrorList {
        validate_namespace_status_update(obj, old)
    }

    fn canonicalize(&self, obj: &mut Namespace) {
        canonicalize(obj);
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `namespaceFinalizeStrategy` (strategy.go:183-214): the update strategy of
/// `/finalize`, the one path that may change `spec.finalizers`.
pub struct FinalizeStrategy;

impl NamespaceScopedStrategy for FinalizeStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<Namespace> for FinalizeStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:208-214: status is kept.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Namespace, old: &Namespace) {
        obj.status = old.status.clone();
    }

    /// strategy.go:188-192.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &Namespace,
        old: &Namespace,
    ) -> ErrorList {
        validate_namespace_finalize_update(obj, old)
    }

    fn canonicalize(&self, obj: &mut Namespace) {
        canonicalize(obj);
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `ShouldDeleteNamespaceDuringUpdate` (storage/storage.go:258-266): a
/// namespace is only removed on update once `spec.finalizers` is empty too.
fn should_delete_namespace_during_update(obj: &Namespace, _existing: &Namespace) -> bool {
    spec_finalizers(obj).is_empty()
}

/// `shouldHaveOrphanFinalizer` (storage/storage.go:268-277).
fn should_have_orphan_finalizer(options: &DeleteOptions, have: bool) -> bool {
    if let Some(orphan) = options.orphan_dependents {
        return orphan;
    }
    if let Some(policy) = &options.propagation_policy {
        return *policy == DeletionPropagation::Orphan;
    }
    have
}

/// `shouldHaveDeleteDependentsFinalizer` (storage/storage.go:279-288).
fn should_have_delete_dependents_finalizer(options: &DeleteOptions, have: bool) -> bool {
    if let Some(orphan) = options.orphan_dependents {
        return !orphan;
    }
    if let Some(policy) = &options.propagation_policy {
        return *policy == DeletionPropagation::Foreground;
    }
    have
}

/// The first-delete mutation of `REST.Delete` (storage/storage.go:178-238):
/// stamp the deletion, move to `Terminating`, and set the garbage-collector
/// finalizers the options ask for.
fn start_termination(ns: &mut Namespace, options: &DeleteOptions) {
    if ns.metadata.deletion_timestamp.is_none() {
        ns.metadata.deletion_timestamp = Some(chrono::Utc::now());
    }
    let status = ns.status.get_or_insert_with(NamespaceStatus::default);
    if status.phase != Some(Phase::Terminating) {
        status.phase = Some(Phase::Terminating);
    }

    let current: Vec<String> = ns.metadata.finalizers.clone().unwrap_or_default();
    let has = |f: &str| current.iter().any(|c| c == f);
    let should_have = [
        (
            FINALIZER_ORPHAN_DEPENDENTS,
            should_have_orphan_finalizer(options, has(FINALIZER_ORPHAN_DEPENDENTS)),
        ),
        (
            FINALIZER_DELETE_DEPENDENTS,
            should_have_delete_dependents_finalizer(options, has(FINALIZER_DELETE_DEPENDENTS)),
        ),
    ];
    if should_have.iter().any(|(f, want)| has(f) != *want) {
        let mut new: Vec<String> = current
            .iter()
            .filter(|f| {
                !should_have
                    .iter()
                    .any(|(g, want)| f.as_str() == *g && !want)
            })
            .cloned()
            .collect();
        for (f, want) in should_have {
            if want && !new.iter().any(|c| c == f) {
                new.push(f.to_string());
            }
        }
        ns.metadata.finalizers = Some(new);
    }
}

/// Namespace's `REST` (storage/storage.go:45-57, 94-310): the Store, with a
/// `Delete` that enforces the namespace lifecycle. It serves no
/// DeleteCollection.
pub struct NamespaceRest {
    store: Store<Namespace, StorageBackend>,
}

#[async_trait]
impl RestStorage<Namespace> for NamespaceRest {
    fn qualified_resource(&self) -> &GroupResource {
        self.store.qualified_resource()
    }

    fn namespace_scoped(&self) -> bool {
        RestStorage::namespace_scoped(&self.store)
    }

    async fn get(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: &crate::registry::generic::GetOptions,
    ) -> Result<Namespace> {
        RestStorage::get(&self.store, ctx, name, options).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: Namespace,
        create_validation: Option<&dyn ValidateObject<Namespace>>,
        options: &CreateOptions,
    ) -> Result<Namespace> {
        RestStorage::create(&self.store, ctx, obj, create_validation, options).await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<Namespace>,
        create_validation: Option<&dyn ValidateObject<Namespace>>,
        update_validation: Option<&dyn ValidateObjectUpdate<Namespace>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(Namespace, bool)> {
        RestStorage::update(
            &self.store,
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

    /// `REST.Delete` (storage/storage.go:138-256).
    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<Namespace>>,
        mut options: DeleteOptions,
    ) -> Result<(Deleted<Namespace>, bool)> {
        let namespace = RestStorage::get(
            &self.store,
            ctx,
            name,
            &crate::registry::generic::GetOptions::default(),
        )
        .await?;

        // Ensure a UID precondition (:146-168).
        let preconditions = options
            .preconditions
            .get_or_insert_with(Preconditions::default);
        match &preconditions.uid {
            None => preconditions.uid = Some(namespace.metadata.uid.clone()),
            Some(uid) if *uid != namespace.metadata.uid => {
                return Err(self.store.conflict(
                    name,
                    format!(
                        "Precondition failed: UID in precondition: {uid}, UID in object meta: {}",
                        namespace.metadata.uid
                    ),
                ));
            }
            Some(_) => {}
        }
        if let Some(rv) = &preconditions.resource_version {
            let stored = namespace
                .metadata
                .resource_version
                .clone()
                .unwrap_or_default();
            if *rv != stored {
                return Err(self.store.conflict(
                    name,
                    format!(
                        "Precondition failed: ResourceVersion in precondition: {rv}, ResourceVersion in object meta: {stored}"
                    ),
                ));
            }
        }

        // The first delete request starts termination (:170-250).
        if namespace.metadata.deletion_timestamp.is_none() {
            let dry_run = options.dry_run.as_ref().is_some_and(|d| !d.is_empty());
            let preconditions = options.preconditions.clone();
            let out = self
                .store
                .guaranteed_update_for_delete(
                    ctx,
                    name,
                    preconditions.as_ref(),
                    dry_run,
                    delete_validation,
                    &|ns: &mut Namespace| start_termination(ns, &options),
                )
                .await?;
            return Ok((Deleted::Object(out), false));
        }

        // Final deletion waits for spec.finalizers to drain (:252-255).
        if !spec_finalizers(&namespace).is_empty() {
            return Ok((Deleted::Object(namespace), false));
        }
        RestStorage::delete(&self.store, ctx, name, delete_validation, options).await
    }
}

/// `NewREST` (storage/storage.go:59-91): the Namespace store, which returns
/// the deleted object and keeps a namespace while `spec.finalizers` remain.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Namespace, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("", "namespaces"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store.should_delete_during_update = Some(should_delete_namespace_during_update);
    store
}

/// The namespace endpoint's storage: [`NamespaceRest`] over [`new_store`].
pub fn new_rest(storage: Arc<StorageBackend>) -> NamespaceRest {
    NamespaceRest {
        store: new_store(storage),
    }
}

/// The `/status` store (storage/storage.go:82-84).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<Namespace, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// The `/finalize` store (storage/storage.go:86-88).
pub fn new_finalize_store(storage: Arc<StorageBackend>) -> Store<Namespace, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(FinalizeStrategy))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(json: serde_json::Value) -> Namespace {
        let mut ns: Namespace = serde_json::from_value(json).unwrap();
        convert_to_internal(&mut ns);
        ns
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!FinalizeStrategy.allow_create_on_update());
    }

    /// `TestNamespaceStrategy` (strategy_test.go:32-71).
    #[test]
    fn create_sets_active_and_the_kubernetes_finalizer() {
        let mut n = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "10"},
            "status": {"phase": "Terminating"}
        }));
        Strategy.prepare_for_create(&ctx(), &mut n);
        assert_eq!(n.status.as_ref().unwrap().phase, Some(Phase::Active));
        assert_eq!(spec_finalizers(&n), ["kubernetes"]);
        let errs = Strategy.validate(&ctx(), &n);
        assert!(errs.is_empty(), "{errs:?}");

        // An existing list keeps its order and gains `kubernetes` last.
        let mut n = ns(serde_json::json!({
            "metadata": {"name": "foo"},
            "spec": {"finalizers": ["example.com/a"]}
        }));
        Strategy.prepare_for_create(&ctx(), &mut n);
        assert_eq!(spec_finalizers(&n), ["example.com/a", "kubernetes"]);
    }

    /// `TestNamespaceStrategy`: an update keeps spec.finalizers and status.
    #[test]
    fn update_keeps_finalizers_and_status() {
        let old = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }));
        let mut new = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": []},
            "status": {"phase": "Terminating"}
        }));
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(spec_finalizers(&new), ["kubernetes"]);
        assert_eq!(new.status.as_ref().unwrap().phase, Some(Phase::Active));
    }

    /// `TestNamespaceFinalizeStrategy` (strategy_test.go:117-147).
    #[test]
    fn finalize_changes_finalizers_but_not_status() {
        let old = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": ["kubernetes", "example.com/org"]},
            "status": {"phase": "Active"}
        }));
        let mut new = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": ["example.com/foo"]},
            "status": {"phase": "Terminating"}
        }));
        FinalizeStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(spec_finalizers(&new), ["example.com/foo"]);
        assert_eq!(new.status.as_ref().unwrap().phase, Some(Phase::Active));
    }

    /// `Canonicalize` (strategy.go:106-131): a user may not remove or change
    /// the name label.
    #[test]
    fn canonicalize_restores_the_name_label() {
        let mut n = ns(serde_json::json!({"metadata": {"name": "foo"}}));
        n.metadata.labels = Some(HashMap::from([(
            LABEL_METADATA_NAME.to_string(),
            "bar".to_string(),
        )]));
        RestUpdateStrategy::canonicalize(&Strategy, &mut n);
        assert_eq!(n.metadata.labels.unwrap()[LABEL_METADATA_NAME], "foo");
    }

    /// `TestDeleteNamespaceWithIncompleteFinalizers` and friends
    /// (storage_test.go): the first delete sets the orphan / foreground
    /// finalizers the options ask for.
    #[test]
    fn termination_sets_the_requested_gc_finalizers() {
        let mut n = ns(serde_json::json!({"metadata": {"name": "foo"}}));
        let options = DeleteOptions {
            propagation_policy: Some(DeletionPropagation::Foreground),
            ..Default::default()
        };
        start_termination(&mut n, &options);
        assert!(n.metadata.deletion_timestamp.is_some());
        assert_eq!(n.status.as_ref().unwrap().phase, Some(Phase::Terminating));
        assert_eq!(
            n.metadata.finalizers,
            Some(vec!["foregroundDeletion".to_string()])
        );

        let mut n = ns(serde_json::json!({
            "metadata": {"name": "foo", "finalizers": ["foregroundDeletion"]}
        }));
        let options = DeleteOptions {
            orphan_dependents: Some(true),
            ..Default::default()
        };
        start_termination(&mut n, &options);
        assert_eq!(n.metadata.finalizers, Some(vec!["orphan".to_string()]));

        let mut n = ns(serde_json::json!({"metadata": {"name": "foo"}}));
        start_termination(&mut n, &DeleteOptions::default());
        assert_eq!(n.metadata.finalizers, None);
    }
}
