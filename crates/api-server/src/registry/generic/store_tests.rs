//! Ports of `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store_test.go`,
//! using a ConfigMap where upstream uses its `example.Pod` fixture.

use super::*;
use crate::registry::rest::{
    zero_delete_options, DefaultUpdatedObjectInfo, NamespaceScopedStrategy,
    RestGracefulDeleteStrategy, TransformFunc,
};
use async_trait::async_trait;
use rusternetes_common::resources::ConfigMap;
use rusternetes_common::types::DeletionPropagation;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::MemoryStorage;

/// `testRESTStrategy` (store_test.go:103-140): labels the object in
/// `PrepareForCreate` so tests can see the strategy ran.
#[derive(Clone, Copy)]
struct TestStrategy {
    namespace_scoped: bool,
    allow_create_on_update: bool,
    allow_unconditional_update: bool,
    graceful: bool,
    gc_policy: Option<GarbageCollectionPolicy>,
}

impl Default for TestStrategy {
    /// `NewTestGenericStoreRegistry`'s strategy:
    /// `testRESTStrategy{scheme, names.SimpleNameGenerator, true, false, true}`.
    fn default() -> Self {
        Self {
            namespace_scoped: true,
            allow_create_on_update: false,
            allow_unconditional_update: true,
            graceful: false,
            gc_policy: None,
        }
    }
}

impl NamespaceScopedStrategy for TestStrategy {
    fn namespace_scoped(&self) -> bool {
        self.namespace_scoped
    }
}
impl RestCreateStrategy<ConfigMap> for TestStrategy {
    fn prepare_for_create(&self, _: &RequestContext, obj: &mut ConfigMap) {
        obj.metadata
            .labels
            .get_or_insert_with(Default::default)
            .insert("prepare_create".to_string(), "true".to_string());
    }
    fn validate(&self, _: &RequestContext, _: &ConfigMap) -> ErrorList {
        Vec::new()
    }
}
impl RestUpdateStrategy<ConfigMap> for TestStrategy {
    fn allow_create_on_update(&self) -> bool {
        self.allow_create_on_update
    }
    fn prepare_for_update(&self, _: &RequestContext, _: &mut ConfigMap, _: &ConfigMap) {}
    fn validate_update(&self, _: &RequestContext, _: &ConfigMap, _: &ConfigMap) -> ErrorList {
        Vec::new()
    }
    fn allow_unconditional_update(&self) -> bool {
        self.allow_unconditional_update
    }
}
/// `testGracefulStrategy.CheckGracefulDelete` (store_test.go:80-82): always
/// graceful.
impl RestGracefulDeleteStrategy<ConfigMap> for TestStrategy {
    fn check_graceful_delete(
        &self,
        _: &RequestContext,
        _: &ConfigMap,
        _: &mut DeleteOptions,
    ) -> bool {
        true
    }
}
impl RestDeleteStrategy<ConfigMap> for TestStrategy {
    fn default_garbage_collection_policy(
        &self,
        _: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        self.gc_policy
    }
    fn graceful(&self) -> Option<&dyn RestGracefulDeleteStrategy<ConfigMap>> {
        self.graceful
            .then_some(self as &dyn RestGracefulDeleteStrategy<ConfigMap>)
    }
}

fn store(strategy: TestStrategy) -> Store<ConfigMap, MemoryStorage> {
    Store::new(
        Arc::new(MemoryStorage::new()),
        GroupResource::new("", "configmaps"),
        Arc::new(strategy),
    )
}

fn ctx() -> RequestContext {
    RequestContext::new(Some("test"))
}

fn cm(name: &str) -> ConfigMap {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": "test"},
        "data": {"node": "machine"},
    }))
    .unwrap()
}

fn with_node(mut obj: ConfigMap, node: &str) -> ConfigMap {
    obj.data = Some([("node".to_string(), node.to_string())].into());
    obj
}

/// `denyCreateValidation` / `denyUpdateValidation` (store_test.go).
struct Deny;
#[async_trait]
impl ValidateObject<ConfigMap> for Deny {
    async fn validate(&self, _: &RequestContext, _: &ConfigMap) -> Result<()> {
        Err(Error::Forbidden("admission denied".to_string()))
    }
}
#[async_trait]
impl ValidateObjectUpdate<ConfigMap> for Deny {
    async fn validate(&self, _: &RequestContext, _: &ConfigMap, _: &ConfigMap) -> Result<()> {
        Err(Error::Forbidden("admission denied".to_string()))
    }
}

fn info(obj: ConfigMap) -> DefaultUpdatedObjectInfo<ConfigMap> {
    DefaultUpdatedObjectInfo::new(Some(obj), Vec::new())
}

fn delete_options(grace: Option<i64>) -> DeleteOptions {
    DeleteOptions {
        grace_period_seconds: grace,
        ..zero_delete_options()
    }
}

fn dry_run_delete() -> DeleteOptions {
    DeleteOptions {
        dry_run: Some(vec!["All".to_string()]),
        ..zero_delete_options()
    }
}

async fn create(registry: &Store<ConfigMap, MemoryStorage>, obj: ConfigMap) -> ConfigMap {
    registry
        .create(&ctx(), obj, None, &CreateOptions::default())
        .await
        .unwrap()
}

async fn update(
    registry: &Store<ConfigMap, MemoryStorage>,
    obj: ConfigMap,
) -> Result<(ConfigMap, bool)> {
    let name = obj.metadata.name.clone();
    registry
        .update(
            &ctx(),
            &name,
            &info(obj),
            None,
            None,
            false,
            &UpdateOptions::default(),
        )
        .await
}

// -- Create -----------------------------------------------------------------

/// `TestStoreCreate` (store_test.go:318-399).
#[tokio::test]
async fn create_admits_then_rejects_a_duplicate_and_a_name_being_deleted() {
    let registry = store(TestStrategy {
        graceful: true,
        ..Default::default()
    });

    // Denying admission: nothing is written.
    let err = registry
        .create(&ctx(), cm("foo"), Some(&Deny), &CreateOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");
    assert!(registry.get(&ctx(), "foo").await.is_err());

    let created = create(&registry, cm("foo")).await;
    assert!(!created.metadata.uid.is_empty());
    assert!(created.metadata.creation_timestamp.is_some());
    assert_eq!(
        created.metadata.labels.as_ref().unwrap()["prepare_create"],
        "true",
        "the create strategy ran"
    );
    assert_eq!(registry.get(&ctx(), "foo").await.unwrap(), created);

    let err = registry
        .create(&ctx(), cm("foo"), None, &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Resource already exists: configmaps \"foo\" already exists"
    );

    // Delete gracefully, then try to create it again before the grace period
    // is over.
    registry
        .delete(&ctx(), "foo", None, delete_options(Some(50)))
        .await
        .unwrap();
    let err = registry
        .create(&ctx(), cm("foo"), None, &CreateOptions::default())
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("object is being deleted: configmaps \"foo\" already exists"),
        "{err}"
    );
}

/// A client-supplied uid never survives a create: `FillObjectMetaSystemFields`
/// overwrites it (store.go:484).
#[tokio::test]
async fn create_assigns_its_own_uid() {
    let registry = store(TestStrategy::default());
    let mut obj = cm("foo");
    obj.metadata.uid = "client-chosen".to_string();
    let created = create(&registry, obj).await;
    assert_ne!(created.metadata.uid, "client-chosen");
}

/// generateName is resolved by the Store, before the strategy runs
/// (store.go:485-486).
#[tokio::test]
async fn create_generates_a_name() {
    let registry = store(TestStrategy::default());
    let mut obj = cm("");
    obj.metadata.generate_name = Some("foo-".to_string());
    let created = create(&registry, obj).await;
    assert!(
        created.metadata.name.starts_with("foo-"),
        "{}",
        created.metadata.name
    );
    assert_eq!(created.metadata.name.len(), "foo-".len() + 5);
}

/// `TestStoreCreateGenerateNameConflict` (store_test.go:2999): an
/// AlreadyExists on a generated name is the server's failure.
#[tokio::test]
async fn a_generated_name_that_collides_says_the_server_could_not_pick_one() {
    struct Fixed;
    impl NamespaceScopedStrategy for Fixed {
        fn namespace_scoped(&self) -> bool {
            true
        }
    }
    impl RestCreateStrategy<ConfigMap> for Fixed {
        fn generate_name(&self, base: &str) -> String {
            format!("{base}fixed")
        }
        fn prepare_for_create(&self, _: &RequestContext, _: &mut ConfigMap) {}
        fn validate(&self, _: &RequestContext, _: &ConfigMap) -> ErrorList {
            Vec::new()
        }
    }
    let mut registry = store(TestStrategy::default());
    registry.create_strategy = Arc::new(Fixed);

    let mut obj = cm("");
    obj.metadata.generate_name = Some("foo-".to_string());
    create(&registry, obj.clone()).await;
    let err = registry
        .create(&ctx(), obj, None, &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Resource already exists: configmaps \"foo-fixed\" already exists, the server was not able to generate a unique name for the object"
    );
}

/// A dry-run create returns the object and writes nothing
/// (`DryRunnableStorage.Create`, dryrun.go:39-47).
#[tokio::test]
async fn dry_run_create_writes_nothing() {
    let registry = store(TestStrategy::default());
    let out = registry
        .create(&ctx(), cm("foo"), None, &CreateOptions { dry_run: true })
        .await
        .unwrap();
    assert_eq!(out.metadata.name, "foo");
    assert!(registry.get(&ctx(), "foo").await.is_err());
}

// -- Update -----------------------------------------------------------------

/// `TestStoreUpdate` (store_test.go:763-844), the not-found half.
#[tokio::test]
async fn update_is_not_found_unless_create_on_update_is_allowed() {
    let registry = store(TestStrategy::default());
    let err = registry
        .update(
            &ctx(),
            "foo",
            &info(cm("foo")),
            Some(&Deny),
            Some(&Deny),
            false,
            &UpdateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Resource not found: configmaps \"foo\" not found",
        "denying admission still answers NotFound"
    );
    let err = update(&registry, cm("foo")).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Resource not found: configmaps \"foo\" not found"
    );

    let creating = store(TestStrategy {
        allow_create_on_update: true,
        ..Default::default()
    });
    // Create on update still runs create admission.
    let err = creating
        .update(
            &ctx(),
            "foo",
            &info(cm("foo")),
            Some(&Deny),
            None,
            false,
            &UpdateOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");

    let (out, created) = update(&creating, cm("foo")).await.unwrap();
    assert!(created);
    assert!(!out.metadata.uid.is_empty());
    assert_eq!(
        out.metadata.labels.as_ref().unwrap()["prepare_create"],
        "true"
    );
}

/// Server-side apply's `forceAllowCreate` creates even when the strategy
/// refuses create-on-update (store.go:638).
#[tokio::test]
async fn force_allow_create_overrides_the_strategy() {
    let registry = store(TestStrategy::default());
    let (_, created) = registry
        .update(
            &ctx(),
            "foo",
            &info(cm("foo")),
            None,
            None,
            true,
            &UpdateOptions::default(),
        )
        .await
        .unwrap();
    assert!(created);
}

/// The rest of `TestStoreUpdate`: a stale resourceVersion conflicts, update
/// admission can deny, and a current one updates.
#[tokio::test]
async fn update_checks_the_resource_version_and_admission() {
    let registry = store(TestStrategy::default());
    let created = create(&registry, cm("foo")).await;

    let mut stale = cm("foo");
    stale.metadata.resource_version = Some("999999".to_string());
    let err = update(&registry, stale).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Conflict: Operation cannot be fulfilled on configmaps \"foo\": the object has been modified; please apply your changes to the latest version and try again"
    );

    let current = with_node(created.clone(), "machine2");
    let err = registry
        .update(
            &ctx(),
            "foo",
            &info(current.clone()),
            None,
            Some(&Deny),
            false,
            &UpdateOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");

    let (out, created_now) = update(&registry, current).await.unwrap();
    assert!(!created_now);
    assert_eq!(out.data.as_ref().unwrap()["node"], "machine2");
    assert_eq!(out.metadata.uid, created.metadata.uid);
    assert_ne!(
        out.metadata.resource_version,
        created.metadata.resource_version
    );
}

/// Without `AllowUnconditionalUpdate`, an update with no resourceVersion is
/// Invalid (store.go:726-733).
#[tokio::test]
async fn an_unconditional_update_needs_the_strategy_to_allow_it() {
    let registry = store(TestStrategy {
        allow_unconditional_update: false,
        ..Default::default()
    });
    create(&registry, cm("foo")).await;
    let err = update(&registry, cm("foo")).await.unwrap_err();
    let Error::Invalid(errs) = err else {
        panic!("{err:?}")
    };
    assert_eq!(errs[0].field, "metadata.resourceVersion");
    assert_eq!(errs[0].detail, "must be specified for an update");
}

/// `TestNoOpUpdates` (store_test.go:847-900): an update that changes nothing
/// returns the stored object and does not bump its resourceVersion.
#[tokio::test]
async fn a_no_op_update_is_not_written() {
    let registry = store(TestStrategy::default());
    let mut fresh = cm("foo");
    fresh
        .metadata
        .labels
        .get_or_insert_with(Default::default)
        .insert("prepare_create".to_string(), "true".to_string());
    let created = create(&registry, fresh.clone()).await;

    let (out, _) = update(&registry, fresh).await.unwrap();
    assert_eq!(out, created);
    let stored = registry.get(&ctx(), "foo").await.unwrap();
    assert_eq!(
        stored.metadata.resource_version,
        created.metadata.resource_version
    );
}

/// A write that loses a race is retried against the current object, and the
/// transformers run again (`TestStoreUpdateHooksInnerRetry`,
/// store_test.go:1180).
#[tokio::test]
async fn a_conflicted_write_is_retried_through_the_transformers() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Count(Arc<AtomicUsize>);
    #[async_trait]
    impl TransformFunc<ConfigMap> for Count {
        async fn transform(
            &self,
            _: &RequestContext,
            new: Option<ConfigMap>,
            _: Option<&ConfigMap>,
        ) -> Result<ConfigMap> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(new.unwrap())
        }
    }

    let storage = Arc::new(MemoryStorage::new());
    let registry: Store<ConfigMap, MemoryStorage> = Store::new(
        storage.clone(),
        GroupResource::new("", "configmaps"),
        Arc::new(TestStrategy::default()),
    );
    create(&registry, cm("foo")).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let obj_info = DefaultUpdatedObjectInfo::new(
        Some(with_node(cm("foo"), "machine2")),
        vec![Box::new(Count(calls.clone()))],
    );

    storage.inject_conflicts(2);
    let (out, _) = registry
        .update(
            &ctx(),
            "foo",
            &obj_info,
            None,
            None,
            false,
            &UpdateOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(out.data.as_ref().unwrap()["node"], "machine2");
    assert_eq!(calls.load(Ordering::SeqCst), 3, "one call per attempt");
}

/// A UID in the request body is a precondition the stored object must meet
/// (`defaultUpdatedObjectInfo.Preconditions`, update.go:188-203).
#[tokio::test]
async fn an_update_naming_another_uid_conflicts() {
    let registry = store(TestStrategy::default());
    create(&registry, cm("foo")).await;
    let mut other = cm("foo");
    other.metadata.uid = "someone-else".to_string();
    let msg = update(&registry, other).await.unwrap_err().to_string();
    assert!(
        msg.starts_with(
            "Conflict: Operation cannot be fulfilled on configmaps \"foo\": StorageError: invalid object, Code: 4"
        ),
        "{msg}"
    );
    assert!(
        msg.contains("Precondition failed: UID in precondition: someone-else"),
        "{msg}"
    );
}

/// A dry-run update runs the whole pipeline and writes nothing.
#[tokio::test]
async fn dry_run_update_writes_nothing() {
    let registry = store(TestStrategy::default());
    let created = create(&registry, cm("foo")).await;
    let (out, _) = registry
        .update(
            &ctx(),
            "foo",
            &info(with_node(cm("foo"), "machine2")),
            None,
            None,
            false,
            &UpdateOptions { dry_run: true },
        )
        .await
        .unwrap();
    assert_eq!(out.data.as_ref().unwrap()["node"], "machine2");
    assert_eq!(registry.get(&ctx(), "foo").await.unwrap(), created);
}

// -- Delete -----------------------------------------------------------------

/// `TestStoreDelete` (store_test.go:1310-1356), and `TestFinalizeDelete`
/// (:2516) for the Status it answers with.
#[tokio::test]
async fn delete_removes_the_object_and_returns_a_status() {
    let registry = store(TestStrategy::default());
    let err = registry
        .delete(&ctx(), "foo", None, zero_delete_options())
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Resource not found: configmaps \"foo\" not found"
    );

    let created = create(&registry, cm("foo")).await;
    let (out, deleted) = registry
        .delete(&ctx(), "foo", None, zero_delete_options())
        .await
        .unwrap();
    assert!(deleted);
    let Deleted::Status(details) = out else {
        panic!("expected a Status")
    };
    assert_eq!(details.name.as_deref(), Some("foo"));
    // "Yes we set Kind field to resource."
    assert_eq!(details.kind.as_deref(), Some("configmaps"));
    assert_eq!(details.uid.as_deref(), Some(created.metadata.uid.as_str()));
    assert!(registry.get(&ctx(), "foo").await.is_err());
}

/// `TestStoreGracefulDeleteWithResourceVersion` (store_test.go:1359-1417).
#[tokio::test]
async fn graceless_delete_with_a_resource_version_precondition() {
    let registry = store(TestStrategy {
        graceful: true,
        ..Default::default()
    });
    let created = create(&registry, cm("foo")).await;
    let mut options = delete_options(Some(0));
    options.preconditions = Some(Preconditions {
        uid: None,
        resource_version: created.metadata.resource_version.clone(),
    });
    let (_, deleted) = registry.delete(&ctx(), "foo", None, options).await.unwrap();
    assert!(deleted);
    assert!(registry.get(&ctx(), "foo").await.is_err());
}

/// Delete admission sees the stored object and can refuse.
#[tokio::test]
async fn delete_admission_can_refuse() {
    let registry = store(TestStrategy::default());
    create(&registry, cm("foo")).await;
    let err = registry
        .delete(&ctx(), "foo", Some(&Deny), zero_delete_options())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");
    assert!(registry.get(&ctx(), "foo").await.is_ok());
}

fn with_finalizer(name: &str) -> ConfigMap {
    let mut obj = cm(name);
    obj.metadata.finalizers = Some(vec!["foo.com/x".to_string()]);
    obj.metadata.generation = Some(1);
    obj
}

/// `TestGracefulStoreHandleFinalizers` (store_test.go:1453-1534).
#[tokio::test]
async fn graceful_store_waits_for_finalizers() {
    for gc in [true, false] {
        let mut registry = store(TestStrategy {
            graceful: true,
            ..Default::default()
        });
        registry.enable_garbage_collection = gc;
        create(&registry, with_finalizer("foo")).await;

        // Grace period 0 still waits for the finalizer.
        let (_, deleted) = registry
            .delete(&ctx(), "foo", None, delete_options(Some(0)))
            .await
            .unwrap();
        assert!(!deleted);
        assert!(registry.get(&ctx(), "foo").await.is_ok());

        // An update keeping the finalizer keeps the object.
        update(&registry, with_finalizer("foo")).await.unwrap();
        assert!(registry.get(&ctx(), "foo").await.is_ok());

        // Removing it deletes the object.
        update(&registry, with_node(cm("foo"), "anothermachine"))
            .await
            .unwrap();
        assert!(registry.get(&ctx(), "foo").await.is_err(), "gc={gc}");
    }
}

/// `TestNonGracefulStoreHandleFinalizers` (store_test.go:1537-1635).
#[tokio::test]
async fn non_graceful_store_marks_as_deleting_and_bumps_generation() {
    for gc in [true, false] {
        let mut registry = store(TestStrategy::default());
        registry.enable_garbage_collection = gc;
        create(&registry, with_finalizer("foo")).await;

        let (out, deleted) = registry
            .delete(&ctx(), "foo", None, zero_delete_options())
            .await
            .unwrap();
        assert!(!deleted);
        assert!(matches!(out, Deleted::Object(_)));

        let stored = registry.get(&ctx(), "foo").await.unwrap();
        assert!(stored.metadata.deletion_timestamp.is_some());
        assert_eq!(stored.metadata.deletion_grace_period_seconds, Some(0));
        assert!(
            stored.metadata.generation.unwrap() > 1,
            "deletion bumps generation"
        );

        // An update can never clear a deletion.
        update(&registry, with_finalizer("foo")).await.unwrap();
        let still = registry.get(&ctx(), "foo").await.unwrap();
        assert_eq!(
            still.metadata.deletion_timestamp,
            stored.metadata.deletion_timestamp
        );

        update(&registry, cm("foo")).await.unwrap();
        assert!(registry.get(&ctx(), "foo").await.is_err(), "gc={gc}");
    }
}

/// `TestStoreDeleteWithOrphanDependents` / `TestStoreDeletionPropagation`
/// (store_test.go:1638, :1909): the propagation policy decides the GC
/// finalizer, and an object held by it is only marked as deleting.
#[tokio::test]
async fn propagation_policy_sets_the_gc_finalizer() {
    for (policy, expected) in [
        (DeletionPropagation::Orphan, "orphan"),
        (DeletionPropagation::Foreground, "foregroundDeletion"),
    ] {
        let registry = store(TestStrategy::default());
        create(&registry, cm("foo")).await;
        let options = DeleteOptions {
            propagation_policy: Some(policy),
            ..zero_delete_options()
        };
        let (_, deleted) = registry.delete(&ctx(), "foo", None, options).await.unwrap();
        assert!(!deleted);
        let stored = registry.get(&ctx(), "foo").await.unwrap();
        assert_eq!(stored.metadata.finalizers, Some(vec![expected.to_string()]));
        assert!(stored.metadata.deletion_timestamp.is_some());
    }

    // Background deletes at once.
    let registry = store(TestStrategy::default());
    create(&registry, cm("foo")).await;
    let options = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Background),
        ..zero_delete_options()
    };
    let (_, deleted) = registry.delete(&ctx(), "foo", None, options).await.unwrap();
    assert!(deleted);
}

/// A strategy whose default policy orphans (`testOrphanDeleteStrategy`,
/// store_test.go:86-92) adds the orphan finalizer with no options at all;
/// `Unsupported` never adds one, whatever the options say.
#[tokio::test]
async fn the_strategy_default_gc_policy_applies_without_options() {
    let registry = store(TestStrategy {
        gc_policy: Some(GarbageCollectionPolicy::OrphanDependents),
        ..Default::default()
    });
    create(&registry, cm("foo")).await;
    let (_, deleted) = registry
        .delete(&ctx(), "foo", None, zero_delete_options())
        .await
        .unwrap();
    assert!(!deleted);
    assert_eq!(
        registry
            .get(&ctx(), "foo")
            .await
            .unwrap()
            .metadata
            .finalizers,
        Some(vec!["orphan".to_string()])
    );

    let registry = store(TestStrategy {
        gc_policy: Some(GarbageCollectionPolicy::Unsupported),
        ..Default::default()
    });
    create(&registry, cm("foo")).await;
    let options = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Orphan),
        ..zero_delete_options()
    };
    let (_, deleted) = registry.delete(&ctx(), "foo", None, options).await.unwrap();
    assert!(deleted);
}

/// A dry-run delete returns the object as it would be and changes nothing,
/// whether it would be marked as deleting or removed.
#[tokio::test]
async fn dry_run_delete_writes_nothing() {
    let registry = store(TestStrategy::default());
    let created = create(&registry, with_finalizer("foo")).await;
    let (out, _) = registry
        .delete(&ctx(), "foo", None, dry_run_delete())
        .await
        .unwrap();
    let Deleted::Object(out) = out else {
        panic!("expected the object")
    };
    assert!(out.metadata.deletion_timestamp.is_some());
    assert_eq!(registry.get(&ctx(), "foo").await.unwrap(), created);

    create(&registry, cm("bar")).await;
    registry
        .delete(&ctx(), "bar", None, dry_run_delete())
        .await
        .unwrap();
    assert!(registry.get(&ctx(), "bar").await.is_ok());
}

/// `TestMarkAsDeleting` (store_test.go:2720-2773): an earlier deletion
/// timestamp is kept, and only the first deletion bumps generation.
#[test]
fn mark_as_deleting_keeps_an_earlier_timestamp() {
    use chrono::SubsecRound;
    let now = chrono::Utc::now().trunc_subsecs(0);
    let earlier = now - chrono::Duration::seconds(60);
    let mut meta = ObjectMeta {
        deletion_timestamp: Some(earlier),
        generation: Some(3),
        ..Default::default()
    };
    mark_as_deleting(&mut meta, now);
    assert_eq!(meta.deletion_timestamp, Some(earlier));
    assert_eq!(meta.deletion_grace_period_seconds, Some(0));
    assert_eq!(meta.generation, Some(3), "already deleting: no bump");

    let mut fresh = ObjectMeta {
        generation: Some(3),
        ..Default::default()
    };
    mark_as_deleting(&mut fresh, now);
    assert_eq!(fresh.deletion_timestamp, Some(now));
    assert_eq!(fresh.generation, Some(4));
}

/// `TestDeletionFinalizersForGarbageCollection` (store_test.go:2652): the
/// deprecated `orphanDependents` wins over the existing finalizer, and GC
/// off never touches finalizers.
#[test]
fn gc_finalizers_follow_upstream_precedence() {
    let registry = store(TestStrategy::default());
    let meta = ObjectMeta {
        finalizers: Some(vec!["foregroundDeletion".to_string(), "x".to_string()]),
        ..Default::default()
    };
    let options = DeleteOptions {
        orphan_dependents: Some(true),
        ..zero_delete_options()
    };
    let (changed, finalizers) =
        registry.deletion_finalizers_for_garbage_collection(&ctx(), &meta, &options);
    assert!(changed);
    assert_eq!(finalizers, vec!["x".to_string(), "orphan".to_string()]);

    let mut off = store(TestStrategy::default());
    off.enable_garbage_collection = false;
    let (changed, _) = off.deletion_finalizers_for_garbage_collection(&ctx(), &meta, &options);
    assert!(!changed);
}

/// `NamespaceKeyFunc` (store.go:280-294).
#[test]
fn key_func_requires_a_namespace_and_a_valid_name() {
    let registry = store(TestStrategy::default());
    assert_eq!(
        registry.key_func(&ctx(), "foo").unwrap(),
        "/registry/configmaps/test/foo"
    );
    let err = registry
        .key_func(&RequestContext::new(None), "foo")
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Bad request: Namespace parameter required."
    );
    let err = registry.key_func(&ctx(), "").unwrap_err();
    assert_eq!(err.to_string(), "Bad request: Name parameter required.");
    assert!(registry.key_func(&ctx(), "..").is_err());
}
