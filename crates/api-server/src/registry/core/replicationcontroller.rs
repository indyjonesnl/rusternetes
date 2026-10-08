//! ReplicationController strategies and storage — port of
//! `pkg/registry/core/replicationcontroller/strategy.go` and
//! `pkg/registry/core/replicationcontroller/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::{
    ReplicationController, ReplicationControllerStatus, Scale, ScaleSpec, ScaleStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::field::{Error, ErrorList, Path};
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_common::validation::replicationcontroller::{
    validate_replication_controller, validate_replication_controller_status_update,
    validate_replication_controller_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};

/// `core.NonConvertibleAnnotationPrefix`
/// (pkg/apis/core/annotation_key_constants.go:78).
const NON_CONVERTIBLE_ANNOTATION_PREFIX: &str = "non-convertible.kubernetes.io";

/// The v1 defaulting a decoded ReplicationController goes through:
/// `SetDefaults_ReplicationController` (pkg/apis/core/v1/defaults.go:50-65)
/// and the pod template's defaults.
pub fn convert_to_internal(rc: &mut ReplicationController) {
    crate::handlers::defaults::apply_replicationcontroller_defaults(rc);
}

/// `rcStrategy` (strategy.go:51-58).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ReplicationController> for Strategy {
    /// `PrepareForCreate` (strategy.go:93-100): status is cleared and the
    /// generation starts at 1; `DropDisabledTemplateFields` is strategy.go:99.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ReplicationController) {
        obj.status = Some(ReplicationControllerStatus::default());
        obj.metadata.generation = Some(1);
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            None,
        );
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ReplicationController) -> ErrorList {
        validate_replication_controller(obj)
    }

    /// `WarningsOnCreate` (strategy.go:135-143). `GetWarningsForPodTemplate`
    /// is not ported (#1996); the name warning is.
    fn warnings_on_create(
        &self,
        _ctx: &RequestContext,
        obj: &ReplicationController,
    ) -> Vec<String> {
        let msgs = is_dns1123_label(&obj.metadata.name);
        if msgs.is_empty() {
            return Vec::new();
        }
        vec![format!(
            "metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: [{}]",
            msgs.join(" ")
        )]
    }
}

impl RestUpdateStrategy<ReplicationController> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:103-114): status is kept and a spec
    /// change bumps the generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ReplicationController,
        old: &ReplicationController,
    ) {
        obj.status = old.status.clone();
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            Some(&old.spec.template),
        );
        if !semantic_equal(&old.spec, &obj.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateUpdate` (strategy.go:156-182): `ValidateReplicationControllerUpdate`,
    /// and the fields the old object's `non-convertible.kubernetes.io/`
    /// annotations name. A non-convertible selector may not change; any other
    /// named field is NotFound.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ReplicationController,
        old: &ReplicationController,
    ) -> ErrorList {
        let mut errs = validate_replication_controller_update(obj, old);
        let non_convertible = old
            .metadata
            .annotations
            .iter()
            .flatten()
            .filter(|(k, _)| k.starts_with(NON_CONVERTIBLE_ANNOTATION_PREFIX));
        for (key, value) in non_convertible {
            let parts: Vec<&str> = key.split('/').collect();
            let [_, broken_field] = parts[..] else {
                continue;
            };
            if broken_field.contains("selector") {
                if !semantic_equal(&old.spec.selector, &obj.spec.selector) {
                    errs.push(Error::invalid(
                        &Path::new("spec").child("selector"),
                        format!("{:?}", obj.spec.selector),
                        "cannot update non-convertible selector",
                    ));
                }
            } else {
                let mut err = Error::not_found(&Path::new(broken_field), value.clone());
                err.detail = "unknown non-convertible field".to_string();
                errs.push(err);
            }
        }
        errs
    }

    // `WarningsOnUpdate` (strategy.go:184-192) is only
    // `GetWarningsForPodTemplate`, which is not ported (#1996).

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `DefaultGarbageCollectionPolicy` (strategy.go:61-73): `OrphanDependents`
/// for core `v1`, the only version served.
impl RestDeleteStrategy<ReplicationController> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::OrphanDependents)
    }
}

/// `rcStatusStrategy` (strategy.go:227-259): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<ReplicationController> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:244-249: only status may change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ReplicationController,
        old: &ReplicationController,
    ) {
        obj.spec = old.spec.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ReplicationController,
        old: &ReplicationController,
    ) -> ErrorList {
        validate_replication_controller_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:85-113): the ReplicationController store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ReplicationController, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "replicationcontrollers"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the store updating with [`StatusStrategy`]
/// (storage.go:106-108).
pub fn new_status_store(
    storage: Arc<StorageBackend>,
) -> Store<ReplicationController, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// `scaleFromRC` (storage/storage.go:239-257). The selector is
/// `labels.SelectorFromSet`, whose string form sorts the keys.
pub fn scale_from_rc(rc: &ReplicationController) -> Result<Scale, String> {
    let mut pairs: Vec<(&String, &String)> = rc.spec.selector.iter().flatten().collect();
    pairs.sort();
    let selector = pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    let meta = &rc.metadata;
    Ok(Scale {
        type_meta: TypeMeta {
            kind: "Scale".to_string(),
            api_version: "autoscaling/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: meta.name.clone(),
            namespace: meta.namespace.clone(),
            uid: meta.uid.clone(),
            resource_version: meta.resource_version.clone(),
            creation_timestamp: meta.creation_timestamp,
            ..ObjectMeta::default()
        },
        spec: ScaleSpec {
            replicas: rc.spec.replicas.unwrap_or(0),
        },
        status: ScaleStatus {
            replicas: rc.status.as_ref().map_or(0, |s| s.replicas),
            selector,
        },
    })
}

/// `ScaleREST{store: controllerREST.Store}` (storage/storage.go:76).
pub fn new_scale_rest(storage: Arc<StorageBackend>) -> ScaleRest<ReplicationController> {
    ScaleRest::new(
        new_store(storage),
        Scalable {
            to_scale: scale_from_rc,
            set_replicas: |rc, replicas| rc.spec.replicas = Some(replicas),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn rc() -> ReplicationController {
        let mut rc: ReplicationController = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": {"name": "r", "namespace": "default", "generation": 4,
                         "resourceVersion": "1"},
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "r", "tier": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut rc);
        rc
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert_eq!(
            Strategy.default_garbage_collection_policy(&ctx()),
            Some(GarbageCollectionPolicy::OrphanDependents)
        );
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// `SetDefaults_ReplicationController`: replicas, and the selector and
    /// labels from the template.
    #[test]
    fn defaults_come_from_the_template() {
        let rc = rc();
        assert_eq!(rc.spec.replicas, Some(1));
        let labels = HashMap::from([
            ("app".to_string(), "r".to_string()),
            ("tier".to_string(), "web".to_string()),
        ]);
        assert_eq!(rc.spec.selector.as_ref(), Some(&labels));
        assert_eq!(rc.metadata.labels.as_ref(), Some(&labels));
    }

    /// `SetDefaults_ReplicationController` (pkg/apis/core/v1/defaults.go:50-64):
    /// a set selector and labels are kept, and so are set replicas.
    #[test]
    fn explicit_values_are_not_defaulted() {
        let mut rc: ReplicationController = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "r", "labels": {"team": "x"}},
            "spec": {
                "replicas": 3,
                "selector": {"app": "explicit"},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        }))
        .unwrap();
        convert_to_internal(&mut rc);
        assert_eq!(rc.spec.replicas, Some(3));
        let selector = HashMap::from([("app".to_string(), "explicit".to_string())]);
        assert_eq!(rc.spec.selector.as_ref(), Some(&selector));
        let labels = HashMap::from([("team".to_string(), "x".to_string())]);
        assert_eq!(rc.metadata.labels.as_ref(), Some(&labels));
    }

    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut rc = rc();
        Strategy.prepare_for_create(&ctx(), &mut rc);
        assert_eq!(rc.status, Some(ReplicationControllerStatus::default()));
        assert_eq!(rc.metadata.generation, Some(1));
        let errs = Strategy.validate(&ctx(), &rc);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = rc();
        let mut touched = old.clone();
        touched.status = None;
        Strategy.prepare_for_update(&ctx(), &mut touched, &old);
        assert_eq!(touched.status, old.status);
        assert_eq!(touched.metadata.generation, Some(4));

        let mut wider = old.clone();
        wider.spec.replicas = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut wider, &old);
        assert_eq!(wider.metadata.generation, Some(5));
        let errs = Strategy.validate_update(&ctx(), &wider, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// strategy.go:162-179.
    #[test]
    fn a_non_convertible_selector_cannot_change() {
        let mut old = rc();
        old.metadata.annotations = Some(HashMap::from([(
            format!("{NON_CONVERTIBLE_ANNOTATION_PREFIX}/spec.selector"),
            "x".to_string(),
        )]));
        let mut new = old.clone();
        new.spec.selector = Some(HashMap::from([("app".to_string(), "r".to_string())]));
        let errs = Strategy.validate_update(&ctx(), &new, &old);
        assert!(
            errs.iter()
                .any(|e| e.detail == "cannot update non-convertible selector"),
            "{errs:?}"
        );
    }

    /// `scaleFromRC`: the selector string is sorted by key.
    #[test]
    fn scale_from_rc_sorts_the_selector() {
        let scale = scale_from_rc(&rc()).unwrap();
        assert_eq!(scale.status.selector, "app=r,tier=web");
        assert_eq!(scale.spec.replicas, 1);
        assert_eq!(scale.status.replicas, 1);
    }

    #[test]
    fn status_prepare_for_update_keeps_spec() {
        let old = rc();
        let mut new = old.clone();
        new.spec.replicas = Some(9);
        new.status = Some(ReplicationControllerStatus {
            replicas: 2,
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, old.spec.replicas);
        assert_eq!(new.status.unwrap().replicas, 2);
    }
}
