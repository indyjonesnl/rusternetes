//! PodTemplate strategy and storage — port of
//! `pkg/registry/core/podtemplate/strategy.go` and
//! `pkg/registry/core/podtemplate/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::PodTemplate;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::podtemplate::{
    validate_pod_template, validate_pod_template_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// The v1 defaulting a decoded PodTemplate goes through: the pod template's
/// `SetDefaults_PodSpec` / `SetDefaults_Container`.
pub fn convert_to_internal(pt: &mut PodTemplate) {
    crate::handlers::defaults::apply_pod_template_defaults(&mut pt.template);
}

/// `podTemplateStrategy` (strategy.go:33-40).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<PodTemplate> for Strategy {
    /// `PrepareForCreate` (strategy.go:48-52): the generation starts at 1.
    /// `DropDisabledTemplateFields` drops nothing we model (see the
    /// Deployment strategy).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PodTemplate) {
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &PodTemplate) -> ErrorList {
        validate_pod_template(obj)
    }

    // `WarningsOnCreate` (strategy.go:62-65) is only
    // `GetWarningsForPodTemplate`, which is not ported (#1996).
}

impl RestUpdateStrategy<PodTemplate> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:77-89): a template change bumps the
    /// generation.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut PodTemplate, old: &PodTemplate) {
        if !semantic_equal(&obj.template, &old.template) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PodTemplate,
        old: &PodTemplate,
    ) -> ErrorList {
        validate_pod_template_update(obj, old)
    }

    // `WarningsOnUpdate` (strategy.go:102-110) is only
    // `GetWarningsForPodTemplate`, which is not ported (#1996).

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// PodTemplate uses the default delete strategy.
impl RestDeleteStrategy<PodTemplate> for Strategy {}

/// `NewREST` (storage/storage.go:40-60), with `ReturnDeletedObject: true`: a
/// DELETE returns the deleted PodTemplate, not a Status.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<PodTemplate, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("", "podtemplates"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod_template() -> PodTemplate {
        let mut pt: PodTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PodTemplate",
            "metadata": {"name": "t", "namespace": "default", "generation": 3,
                         "resourceVersion": "1"},
            "template": {
                "metadata": {"labels": {"app": "t"}},
                "spec": {"containers": [{"name": "c", "image": "i"}]}
            }
        }))
        .unwrap();
        convert_to_internal(&mut pt);
        pt
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
    }

    #[test]
    fn prepare_for_create_starts_the_generation_at_one() {
        let mut pt = pod_template();
        Strategy.prepare_for_create(&ctx(), &mut pt);
        assert_eq!(pt.metadata.generation, Some(1));
        let errs = Strategy.validate(&ctx(), &pt);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `TestStrategy` (strategy_test.go): only a template change bumps.
    #[test]
    fn prepare_for_update_bumps_generation_on_template_change() {
        let old = pod_template();
        let mut labelled = old.clone();
        labelled.metadata.labels = Some([("l".to_string(), "v".to_string())].into());
        Strategy.prepare_for_update(&ctx(), &mut labelled, &old);
        assert_eq!(labelled.metadata.generation, Some(3));

        let mut reimaged = old.clone();
        reimaged.template.spec.containers[0].image = "j".into();
        Strategy.prepare_for_update(&ctx(), &mut reimaged, &old);
        assert_eq!(reimaged.metadata.generation, Some(4));
        let errs = Strategy.validate_update(&ctx(), &reimaged, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }
}
