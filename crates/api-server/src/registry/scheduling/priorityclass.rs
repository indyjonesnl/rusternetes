//! PriorityClass strategy and storage — port of
//! `pkg/registry/scheduling/priorityclass/strategy.go` and
//! `pkg/registry/scheduling/priorityclass/storage/storage.go`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::deletion::DeleteOptions;
use rusternetes_common::resources::PriorityClass;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::priorityclass::{
    validate_priority_class, validate_priority_class_update,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::{CreateOptions, Deleted, Store, UpdateOptions};
use crate::registry::rest::{
    DeletedCollection, GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
    RestDeleteStrategy, RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject,
    ValidateObjectUpdate,
};

/// `SystemPriorityClassNames()` (pkg/apis/scheduling/v1/helpers.go:56-63),
/// in `systemPriorityClasses` order.
pub const SYSTEM_PRIORITY_CLASS_NAMES: [&str; 2] =
    ["system-node-critical", "system-cluster-critical"];

/// The v1 defaulting a decoded PriorityClass goes through:
/// `SetDefaults_PriorityClass` (pkg/apis/scheduling/v1/defaults.go).
pub fn convert_to_internal(pc: &mut PriorityClass) {
    if pc.preemption_policy.is_none() {
        pc.preemption_policy = Some("PreemptLowerPriority".to_string());
    }
}

/// `priorityClassStrategy` (strategy.go:32-38).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<PriorityClass> for Strategy {
    /// `PrepareForCreate` (strategy.go:46-50).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PriorityClass) {
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &PriorityClass) -> ErrorList {
        validate_priority_class(obj)
    }
}

impl RestUpdateStrategy<PriorityClass> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:53) does nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut PriorityClass,
        _old: &PriorityClass,
    ) {
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PriorityClass,
        old: &PriorityClass,
    ) -> ErrorList {
        validate_priority_class_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `priorityClassStrategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<PriorityClass> for Strategy {}

/// PriorityClass's `REST` (storage/storage.go:36-78): the Store, with a
/// `Delete` that refuses the system priority classes.
pub struct PriorityClassRest {
    store: Store<PriorityClass, StorageBackend>,
}

#[async_trait]
impl RestStorage<PriorityClass> for PriorityClassRest {
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
    ) -> Result<PriorityClass> {
        RestStorage::get(&self.store, ctx, name, options).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: PriorityClass,
        create_validation: Option<&dyn ValidateObject<PriorityClass>>,
        options: &CreateOptions,
    ) -> Result<PriorityClass> {
        RestStorage::create(&self.store, ctx, obj, create_validation, options).await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<PriorityClass>,
        create_validation: Option<&dyn ValidateObject<PriorityClass>>,
        update_validation: Option<&dyn ValidateObjectUpdate<PriorityClass>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(PriorityClass, bool)> {
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

    /// `REST.Delete` (storage/storage.go:69-78).
    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<PriorityClass>>,
        options: DeleteOptions,
    ) -> Result<(Deleted<PriorityClass>, bool)> {
        if SYSTEM_PRIORITY_CLASS_NAMES.contains(&name) {
            return Err(Error::Forbidden(format!(
                "{} \"{name}\" is forbidden: this is a system priority class and cannot be deleted",
                self.qualified_resource()
            )));
        }
        RestStorage::delete(&self.store, ctx, name, delete_validation, options).await
    }

    /// The embedded Store's `DeleteCollection`. It deletes each item through
    /// `Store.Delete` (registry/generic/registry/store.go `DeleteCollection`),
    /// not `REST.Delete`, so upstream does not refuse the system classes here.
    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<PriorityClass>>,
        options: &DeleteOptions,
        list_options: &HashMap<String, String>,
    ) -> Result<DeletedCollection<PriorityClass>> {
        RestStorage::delete_collection(&self.store, ctx, delete_validation, options, list_options)
            .await
    }
}

/// `NewREST` (storage/storage.go:40-60): the PriorityClass store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<PriorityClass, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("scheduling.k8s.io", "priorityclasses"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// `NewREST`'s `REST`: the store behind [`PriorityClassRest`].
pub fn new_rest(storage: Arc<StorageBackend>) -> PriorityClassRest {
    PriorityClassRest {
        store: new_store(storage),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pc(name: &str, value: i32) -> PriorityClass {
        let mut pc: PriorityClass = serde_json::from_value(serde_json::json!({
            "apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass",
            "metadata": {"name": name}, "value": value
        }))
        .unwrap();
        convert_to_internal(&mut pc);
        pc
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    /// `TestPriorityClassStrategy` (strategy_test.go).
    #[test]
    fn strategy_matches_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());

        let mut created = pc("valid-class", 10);
        Strategy.prepare_for_create(&ctx(), &mut created);
        assert_eq!(created.metadata.generation, Some(1));
        assert!(Strategy.validate(&ctx(), &created).is_empty());

        let mut changed = created.clone();
        changed.value = 11;
        Strategy.prepare_for_update(&ctx(), &mut changed, &created);
        assert!(!Strategy
            .validate_update(&ctx(), &changed, &created)
            .is_empty());
    }

    /// `SetDefaults_PriorityClass`: preemptionPolicy defaults to
    /// PreemptLowerPriority and an explicit value is kept.
    #[test]
    fn preemption_policy_defaults_to_preempt_lower_priority() {
        assert_eq!(
            pc("a", 1).preemption_policy.as_deref(),
            Some("PreemptLowerPriority")
        );
        let mut never = pc("a", 1);
        never.preemption_policy = Some("Never".into());
        convert_to_internal(&mut never);
        assert_eq!(never.preemption_policy.as_deref(), Some("Never"));
    }
}
