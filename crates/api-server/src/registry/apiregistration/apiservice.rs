//! APIService strategies and storage -- port of
//! `staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/strategy.go`
//! and `.../apiservice/etcd/etcd.go`, with the validation of
//! `pkg/apis/apiregistration/validation/validation.go` (in
//! `rusternetes_common::validation::apiservice`).
//!
//! Not modelled: `GetResetFields` (managed-fields reset sets), `WarningsOn*`
//! (both return nil upstream), and `MatchAPIService` /
//! `ToSelectableFields`, which are the generic `metadata.name` field selector
//! the list handler already serves.
//!
//! `REST.Categories` (etcd.go:75-78) is served by `rusternetes_discovery`
//! (`api-extensions`) and `REST.ConvertToTable` (etcd.go:82-120) by
//! `rusternetes_middleware::table::printer_columns` / `printer_row_cells`.

use std::sync::Arc;

use rusternetes_common::resources::apiregistration::{
    new_local_available_condition, set_api_service_condition, set_defaults_api_service,
};
use rusternetes_common::resources::APIService;
use rusternetes_common::validation::apiservice::{
    validate_api_service, validate_api_service_status_update, validate_api_service_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `SetDefaults_ServiceReference` (v1/defaults.go), run by the codec on every
/// decode.
pub fn convert_to_internal(api_service: &mut APIService) {
    set_defaults_api_service(api_service);
}

/// `apiServerStrategy` (strategy.go:38-47).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:49-51.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<APIService> for Strategy {
    /// `PrepareForCreate` (strategy.go:68-76): the status is cleared, and a
    /// local APIService (no `spec.service`) is available immediately. A remote
    /// one gets its first condition from the availability controller.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut APIService) {
        obj.status = Default::default();
        if obj.spec.service.is_none() {
            set_api_service_condition(obj, new_local_available_condition());
        }
    }

    /// `Validate` (strategy.go:85-87).
    fn validate(&self, _ctx: &RequestContext, obj: &APIService) -> ErrorList {
        validate_api_service(obj)
    }
}

impl RestUpdateStrategy<APIService> for Strategy {
    /// strategy.go:94-96.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:78-83): the status is the stored one.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut APIService, old: &APIService) {
        obj.status = old.status.clone();
    }

    /// `ValidateUpdate` (strategy.go:103-105).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &APIService,
        old: &APIService,
    ) -> ErrorList {
        validate_api_service_update(obj, old)
    }

    /// strategy.go:98-100.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// APIServices use the default delete strategy (`DeleteStrategy: strategy`
/// implements no `GarbageCollectionDeleteStrategy`).
impl RestDeleteStrategy<APIService> for Strategy {}

/// `apiServerStatusStrategy` (strategy.go:118-166): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<APIService> for StatusStrategy {
    /// strategy.go:151-153.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:143-149): only the status may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut APIService, old: &APIService) {
        obj.spec = old.spec.clone();
        obj.metadata.labels = old.metadata.labels.clone();
        obj.metadata.annotations = old.metadata.annotations.clone();
        obj.metadata.finalizers = old.metadata.finalizers.clone();
        obj.metadata.owner_references = old.metadata.owner_references.clone();
    }

    /// `ValidateUpdate` (strategy.go:161-163).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &APIService,
        old: &APIService,
    ) -> ErrorList {
        validate_api_service_status_update(obj, old)
    }

    /// strategy.go:155-157.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `NewREST`'s store (etcd.go:41-62).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<APIService, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apiregistration.k8s.io", "apiservices"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store (etcd.go:117-127): `UpdateStrategy` swapped, create and
/// delete strategies dropped.
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<APIService, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc(remote: bool) -> APIService {
        let mut a = serde_json::from_value::<APIService>(serde_json::json!({
            "apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
            "metadata": {"name": "v1alpha1.wardle.example.com"},
            "spec": {
                "group": "wardle.example.com", "version": "v1alpha1",
                "groupPriorityMinimum": 2000, "versionPriority": 200,
            }
        }))
        .unwrap();
        if remote {
            a.spec.service = Some(Default::default());
            a.spec.service.as_mut().unwrap().namespace = "ns".into();
            a.spec.service.as_mut().unwrap().name = "svc".into();
        }
        convert_to_internal(&mut a);
        a
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!StatusStrategy.allow_unconditional_update());
    }

    /// strategy.go:68-76: a local APIService is available at once.
    #[test]
    fn a_local_apiservice_is_available_on_create() {
        let mut a = svc(false);
        a.status.conditions.push(Default::default());
        Strategy.prepare_for_create(&ctx(), &mut a);
        assert_eq!(a.status.conditions.len(), 1);
        let c = &a.status.conditions[0];
        assert_eq!(
            (c.type_.as_str(), c.status.as_str(), c.reason.as_str()),
            ("Available", "True", "Local")
        );
        assert_eq!(c.message, "Local APIServices are always available");
    }

    /// strategy.go:68-76: a remote one starts with an empty status.
    #[test]
    fn a_remote_apiservice_has_an_empty_status_on_create() {
        let mut a = svc(true);
        a.status.conditions.push(Default::default());
        Strategy.prepare_for_create(&ctx(), &mut a);
        assert!(a.status.conditions.is_empty());
    }

    /// v1/defaults.go: the service port defaults to 443.
    #[test]
    fn the_service_port_defaults_to_443() {
        assert_eq!(svc(true).spec.service.unwrap().port, Some(443));
    }

    /// strategy.go:78-83 and 143-149.
    #[test]
    fn spec_updates_keep_the_status_and_status_updates_keep_the_spec() {
        let mut old = svc(true);
        old.status
            .conditions
            .push(rusternetes_common::resources::APIServiceCondition {
                type_: "Available".into(),
                status: "True".into(),
                ..Default::default()
            });

        let mut new = svc(true);
        new.spec.group_priority_minimum = 5;
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status, old.status);
        assert_eq!(new.spec.group_priority_minimum, 5);

        let mut new = svc(true);
        new.spec.group_priority_minimum = 5;
        new.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
        new.status = Default::default();
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec, old.spec);
        assert_eq!(new.metadata.labels, old.metadata.labels);
        assert!(new.status.conditions.is_empty());
    }
}
