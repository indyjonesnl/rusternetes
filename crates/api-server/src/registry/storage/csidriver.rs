//! CSIDriver strategy and storage — port of
//! `pkg/registry/storage/csidriver/strategy.go` and
//! `pkg/registry/storage/csidriver/storage/storage.go`.
//!
//! The feature gates the strategy consults are all on in 1.35
//! (`SELinuxMountReadWriteOncePod`, `MutableCSINodeAllocatableCount`,
//! `CSIServiceAccountTokenSecrets`), so the field-clearing in
//! `PrepareForCreate` / `PrepareForUpdate` never fires and is not modelled.

use std::sync::Arc;

use rusternetes_common::resources::csi::{FSGroupPolicy, VolumeLifecycleMode};
use rusternetes_common::resources::CSIDriver;
use rusternetes_common::validation::csidriver::{validate_csi_driver, validate_csi_driver_update};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `warningServiceAccountTokenInSecretsRecommended` (strategy.go:33).
const WARNING_SERVICE_ACCOUNT_TOKEN_IN_SECRETS_RECOMMENDED: &str = "spec.serviceAccountTokenInSecrets is unset; if supported by this CSI driver, set to true to prevent possible logging of tokens in volume attributes";

/// The v1 defaulting a decoded CSIDriver goes through: `SetDefaults_CSIDriver`
/// (pkg/apis/storage/v1/defaults.go:43-71). Runs on create and update.
pub fn convert_to_internal(driver: &mut CSIDriver) {
    let s = &mut driver.spec;
    s.attach_required.get_or_insert(true);
    s.pod_info_on_mount.get_or_insert(false);
    s.storage_capacity.get_or_insert(false);
    s.fs_group_policy
        .get_or_insert(FSGroupPolicy::ReadWriteOnceWithFSType);
    if s.volume_lifecycle_modes
        .as_ref()
        .is_none_or(|v| v.is_empty())
    {
        s.volume_lifecycle_modes = Some(vec![VolumeLifecycleMode::Persistent]);
    }
    s.requires_republish.get_or_insert(false);
    // `SELinuxMountReadWriteOncePod` is on.
    s.se_linux_mount.get_or_insert(false);
}

/// `ValidateCSIDriver` (validation.go:427-432): ObjectMeta with
/// `ValidateCSIDriverName` (`NameIsDNSSubdomain`), then the spec.
fn validate(obj: &CSIDriver) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_csi_driver(obj));
    errs
}

fn token_requests_json(d: &CSIDriver) -> serde_json::Value {
    serde_json::to_value(&d.spec.token_requests).unwrap_or_default()
}

fn has_token_requests(d: &CSIDriver) -> bool {
    d.spec
        .token_requests
        .as_ref()
        .is_some_and(|t| !t.is_empty())
}

/// `csiDriverStrategy` (strategy.go:41-50).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<CSIDriver> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut CSIDriver) {}

    fn validate(&self, _ctx: &RequestContext, obj: &CSIDriver) -> ErrorList {
        validate(obj)
    }

    /// Warn when `tokenRequests` is set and `serviceAccountTokenInSecrets`
    /// is not (strategy.go:73-85).
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &CSIDriver) -> Vec<String> {
        if has_token_requests(obj) && obj.spec.service_account_token_in_secrets.is_none() {
            vec![WARNING_SERVICE_ACCOUNT_TOKEN_IN_SECRETS_RECOMMENDED.to_string()]
        } else {
            Vec::new()
        }
    }
}

impl RestUpdateStrategy<CSIDriver> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Any change to the spec increments the generation (strategy.go:114-117).
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut CSIDriver, old: &CSIDriver) {
        if serde_json::to_value(&obj.spec).ok() != serde_json::to_value(&old.spec).ok() {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateCSIDriverUpdate` validates the spec but not the name rule;
    /// the Store's common update validation covers ObjectMeta.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CSIDriver,
        old: &CSIDriver,
    ) -> ErrorList {
        validate_csi_driver_update(obj, old)
    }

    /// Warn when `tokenRequests` changes to a non-empty value and
    /// `serviceAccountTokenInSecrets` is unset (strategy.go:127-140).
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &CSIDriver,
        old: &CSIDriver,
    ) -> Vec<String> {
        if token_requests_json(old) != token_requests_json(obj)
            && has_token_requests(obj)
            && obj.spec.service_account_token_in_secrets.is_none()
        {
            vec![WARNING_SERVICE_ACCOUNT_TOKEN_IN_SECRETS_RECOMMENDED.to_string()]
        } else {
            Vec::new()
        }
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<CSIDriver> for Strategy {}

/// `NewREST` (storage/storage.go:45-52): `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<CSIDriver, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("storage.k8s.io", "csidrivers"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store
}
