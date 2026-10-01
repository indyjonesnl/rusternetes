//! VolumeSnapshotClass strategy and storage — the custom-resource strategy
//! (`customresource/strategy.go`) for the external-snapshotter CRD
//! `snapshot.storage.k8s.io_volumesnapshotclasses.yaml` (cluster-scoped, no
//! status subresource, `subresources: {}`), plus the snapshot validation
//! webhook's default-class rule.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::resources::VolumeSnapshotClass;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::volumesnapshot::validate_volume_snapshot_class;
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_prefix, Storage, StorageBackend};

use super::{same_json, GROUP};
use crate::registry::generic::store::{
    BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
use crate::registry::generic::Store;
use crate::registry::rbac::policybased::Noop;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `utils.IsDefaultSnapshotClassAnnotation` (external-snapshotter release-6.3
/// pkg/utils/util.go:80).
pub const IS_DEFAULT_SNAPSHOT_CLASS_ANNOTATION: &str =
    "snapshot.storage.kubernetes.io/is-default-class";

/// The `ValidatingWebhookConfiguration` name is chosen by whoever installs the
/// webhook (deploy/kubernetes/webhook-example/ ships none), so this follows the
/// name KEP-1900 documents; only the message after the colon is upstream's.
const WEBHOOK_NAME: &str = "validation-webhook.snapshot.storage.k8s.io";

fn is_default(class: &VolumeSnapshotClass) -> bool {
    class
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(IS_DEFAULT_SNAPSHOT_CLASS_ANNOTATION))
        .is_some_and(|v| v == "true")
}

/// `customResourceStrategy` for VolumeSnapshotClass.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<VolumeSnapshotClass> for Strategy {
    /// `PrepareForCreate` (strategy.go:117-129): no status subresource, so
    /// nothing to clear; the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut VolumeSnapshotClass) {
        obj.metadata.generation = Some(1);
    }

    /// `customResourceValidator.Validate` (validator.go:53).
    fn validate(&self, _ctx: &RequestContext, obj: &VolumeSnapshotClass) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            false,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_volume_snapshot_class(obj));
        errs
    }
}

impl RestUpdateStrategy<VolumeSnapshotClass> for Strategy {
    /// `AllowCreateOnUpdate` is false for custom resources (strategy.go:262).
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:132-165): "except for the changes to
    /// `metadata`, any other changes cause the generation to increment" — the
    /// class has no spec, so `driver`, `parameters` and `deletionPolicy`.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshotClass,
        old: &VolumeSnapshotClass,
    ) {
        let body = |c: &VolumeSnapshotClass| {
            (
                c.driver.clone(),
                c.parameters.clone(),
                c.deletion_policy.clone(),
            )
        };
        if !same_json(&body(obj), &body(old)) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &VolumeSnapshotClass,
        _old: &VolumeSnapshotClass,
    ) -> ErrorList {
        validate_volume_snapshot_class(obj)
    }

    /// `AllowUnconditionalUpdate` is false for custom resources
    /// (strategy.go:267-269).
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<VolumeSnapshotClass> for Strategy {}

/// `decideSnapshotClassV1` (external-snapshotter release-6.3
/// pkg/validation-webhook/snapshot.go:165-200) as the Store's `BeginCreate` /
/// `BeginUpdate` hook: a class annotated as the default is denied when another
/// default class already serves the same driver.
pub struct DefaultClassCheck {
    storage: Arc<StorageBackend>,
}

impl DefaultClassCheck {
    async fn decide(
        &self,
        class: &VolumeSnapshotClass,
        old: Option<&VolumeSnapshotClass>,
    ) -> Result<()> {
        // "Only Validate when a new snapClass is being set as a default."
        if !is_default(class) {
            return Ok(());
        }
        // "If Old snapshot class has this, then we can assume that it was
        // validated if driver is the same."
        if old.is_some_and(|o| is_default(o) && o.driver == class.driver) {
            return Ok(());
        }
        let existing: Vec<VolumeSnapshotClass> = self
            .storage
            .list(&build_prefix("volumesnapshotclasses", None))
            .await?;
        for other in existing {
            if !is_default(&other) {
                continue;
            }
            if other.driver == class.driver {
                // `ToStatusErr` (admission/plugin/webhook/errors/statuserror.go:28-55):
                // a denial is a 400 `admission webhook "<name>" denied the request: <message>`.
                return Err(Error::BadRequest(format!(
                    "admission webhook \"{WEBHOOK_NAME}\" denied the request: default snapshot class: {} already exists for driver: {}",
                    other.metadata.name, class.driver
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BeginCreate<VolumeSnapshotClass> for DefaultClassCheck {
    async fn begin_create(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshotClass,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        self.decide(obj, None).await?;
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl BeginUpdate<VolumeSnapshotClass> for DefaultClassCheck {
    async fn begin_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshotClass,
        old: &mut VolumeSnapshotClass,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        self.decide(obj, Some(old)).await?;
        Ok(Box::new(Noop))
    }
}

/// The store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<VolumeSnapshotClass, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new(GROUP, "volumesnapshotclasses"),
        Arc::new(Strategy),
    );
    let hook = Arc::new(DefaultClassCheck { storage });
    store.begin_create = Some(hook.clone());
    store.begin_update = Some(hook);
    store
}
