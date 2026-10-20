//! The small in-tree admission plugins of `plugin/pkg/admission/*` that
//! kube-apiserver registers (`pkg/kubeapiserver/options/plugins.go:69-111`):
//! `AlwaysAdmit`, `AlwaysDeny`, `DenyServiceExternalIPs`, `AlwaysPullImages`,
//! `LimitPodHardAntiAffinityTopology`, `PersistentVolumeClaimResize` and
//! `DefaultIngressClass`.

use std::sync::Arc;

use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
use rusternetes_common::resources::{
    Ingress, IngressClass, PersistentVolumeClaim, Pod, Service, StorageClass,
};
use rusternetes_common::types::{ObjectMeta, Status};
use rusternetes_common::validation::field;
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;

/// `admission.NewForbidden(a, err)` (apiserver/pkg/admission/errors.go:53-63):
/// `<resource> "<name>" is forbidden: <err>`.
pub fn new_forbidden(resource: &str, name: &str, err: impl std::fmt::Display) -> Error {
    Error::Forbidden(format!("{resource} \"{name}\" is forbidden: {err}"))
}

/// `extractResourceName` (errors.go:26-50) for an object's metadata: the
/// name, else its generateName, else `Unknown`.
pub fn attr_name(meta: &ObjectMeta) -> String {
    if !meta.name.is_empty() {
        meta.name.clone()
    } else if let Some(g) = meta.generate_name.as_deref().filter(|g| !g.is_empty()) {
        g.to_string()
    } else {
        "Unknown".to_string()
    }
}

/// `alwaysDeny.Admit` and `Validate` (plugin/pkg/admission/deny/admission.go:46-54).
pub fn always_deny(resource: &str, name: &str) -> Error {
    new_forbidden(
        resource,
        name,
        "admission control is denying all modifications",
    )
}

/// `externalIPsDenierPlugin.Validate` (plugin/pkg/admission/network/
/// denyserviceexternalips/admission.go:68-93) with `isSubset` (:96-112).
pub fn deny_service_external_ips(new: &Service, old: Option<&Service>) -> Result<()> {
    let new_ips = new.spec.external_ips.as_deref().unwrap_or_default();
    let subset = new_ips.is_empty()
        || old.is_some_and(|o| {
            let old_ips = o.spec.external_ips.as_deref().unwrap_or_default();
            new_ips.iter().all(|ip| old_ips.contains(ip))
        });
    if subset {
        return Ok(());
    }
    Err(new_forbidden(
        "services",
        &attr_name(&new.metadata),
        "Use of external IPs is denied by admission control",
    ))
}

/// `pods.VisitContainersWithPath` (pkg/apis/core/pods/helpers.go:35-57) over
/// the image pull policies: init, regular, then ephemeral containers.
fn visit_pull_policies(pod: &mut Pod, mut visit: impl FnMut(&mut Option<String>, field::Path)) {
    let Some(spec) = pod.spec.as_mut() else {
        return;
    };
    let base = field::Path::new("spec");
    for (i, c) in spec.init_containers.iter_mut().flatten().enumerate() {
        visit(
            &mut c.image_pull_policy,
            base.child("initContainers").index(i),
        );
    }
    for (i, c) in spec.containers.iter_mut().enumerate() {
        visit(&mut c.image_pull_policy, base.child("containers").index(i));
    }
    for (i, c) in spec.ephemeral_containers.iter_mut().flatten().enumerate() {
        visit(
            &mut c.image_pull_policy,
            base.child("ephemeralContainers").index(i),
        );
    }
}

fn container_images(pod: &Pod) -> Vec<&str> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    spec.init_containers
        .iter()
        .flatten()
        .map(|c| c.image.as_str())
        .chain(spec.containers.iter().map(|c| c.image.as_str()))
        .chain(
            spec.ephemeral_containers
                .iter()
                .flatten()
                .map(|c| c.image.as_str()),
        )
        .collect()
}

/// `isUpdateWithNoNewImages` (plugin/pkg/admission/alwayspullimages/
/// admission.go:126-159).
pub fn is_update_with_no_new_images(pod: &Pod, old: Option<&Pod>) -> bool {
    let Some(old) = old else {
        return false;
    };
    let old_images = container_images(old);
    container_images(pod)
        .iter()
        .all(|image| old_images.contains(image))
}

/// `AlwaysPullImages.Admit` (alwayspullimages/admission.go:53-76), after
/// `shouldIgnore` (:161-171), which the caller applies.
pub fn always_pull_images_admit(pod: &mut Pod) {
    visit_pull_policies(pod, |policy, _| *policy = Some("Always".to_string()));
    // See: https://kep.k8s.io/4639
    for v in pod
        .spec
        .iter_mut()
        .flat_map(|s| s.volumes.iter_mut().flatten())
    {
        if let Some(image) = v.image.as_mut() {
            image.pull_policy = Some("Always".to_string());
        }
    }
}

/// `AlwaysPullImages.Validate` (alwayspullimages/admission.go:79-124): every
/// violation is a `NewForbidden`, collected into a `utilerrors.Aggregate`.
/// That is not an `APIStatus`, so it renders as a 500 with `Error()` of the
/// aggregate (`apimachinery/pkg/util/errors/errors.go:70-96`): one error as
/// itself, several deduplicated as `[a, b]`.
pub fn always_pull_images_validate(resource: &str, pod: &Pod) -> Result<()> {
    let name = attr_name(&pod.metadata);
    let mut msgs: Vec<String> = Vec::new();
    let mut add = |path: field::Path, value: &str| {
        let e = field::Error::not_supported(&path, value, &["Always"]);
        let m = format!("{resource} \"{name}\" is forbidden: {e}");
        if !msgs.contains(&m) {
            msgs.push(m);
        }
    };
    // The visitor takes `&mut`; the checks are read-only, so walk a copy.
    let mut copy = pod.clone();
    visit_pull_policies(&mut copy, |policy, path| {
        if policy.as_deref() != Some("Always") {
            add(
                path.child("imagePullPolicy"),
                policy.as_deref().unwrap_or(""),
            );
        }
    });
    for (i, v) in pod
        .spec
        .iter()
        .flat_map(|s| s.volumes.iter().flatten())
        .enumerate()
    {
        if let Some(image) = v.image.as_ref() {
            if image.pull_policy.as_deref() != Some("Always") {
                add(
                    field::Path::new("spec")
                        .child("volumes")
                        .index(i)
                        .child("image")
                        .child("pullPolicy"),
                    image.pull_policy.as_deref().unwrap_or(""),
                );
            }
        }
    }
    match msgs.len() {
        0 => Ok(()),
        1 => Err(aggregate_error(msgs.remove(0))),
        _ => Err(aggregate_error(format!("[{}]", msgs.join(", ")))),
    }
}

/// `ErrorToAPIStatus`' default arm (responsewriters/status.go:60-82): a
/// non-`APIStatus` error is a 500, reason `StatusReasonUnknown`.
fn aggregate_error(message: String) -> Error {
    let mut status = Status::failure(message, "", 500);
    status.reason = None;
    Error::Status(Box::new(status))
}

/// `Plugin.Validate` (plugin/pkg/admission/antiaffinity/admission.go:55-84).
pub fn limit_pod_hard_anti_affinity_topology(pod: &Pod) -> Result<()> {
    let terms = pod
        .spec
        .as_ref()
        .and_then(|s| s.affinity.as_ref())
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|a| {
            a.required_during_scheduling_ignored_during_execution
                .as_deref()
        })
        .unwrap_or_default();
    for term in terms {
        if term.topology_key != "kubernetes.io/hostname" {
            return Err(new_forbidden(
                "pods",
                &pod.metadata.name,
                format!(
                    "affinity.PodAntiAffinity.RequiredDuringScheduling has TopologyKey {} but only key kubernetes.io/hostname is allowed",
                    term.topology_key
                ),
            ));
        }
    }
    Ok(())
}

/// `GetPersistentVolumeClaimClass` (staging/src/k8s.io/component-helpers/
/// storage/volume/helpers.go:43-54): the beta annotation first.
fn pvc_class(pvc: &PersistentVolumeClaim) -> &str {
    if let Some(class) = pvc
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("volume.beta.kubernetes.io/storage-class"))
    {
        return class;
    }
    pvc.spec.storage_class_name.as_deref().unwrap_or("")
}

fn requested_storage(pvc: &PersistentVolumeClaim) -> Quantity {
    pvc.spec
        .resources
        .requests
        .as_ref()
        .and_then(|r| r.get("storage"))
        .and_then(|q| Quantity::parse(q).ok())
        .unwrap_or_else(|| Quantity::from_value(0, Format::DecimalSI))
}

/// `persistentVolumeClaimResize.Validate` (plugin/pkg/admission/storage/
/// persistentvolume/resize/admission.go:73-111) with `allowResize` (:115-128).
pub async fn pvc_resize_validate<S: Storage>(
    storage: &Arc<S>,
    pvc: &PersistentVolumeClaim,
    old: &PersistentVolumeClaim,
) -> Result<()> {
    if requested_storage(pvc)
        .cmp_value(&requested_storage(old))
        .is_le()
    {
        return Ok(());
    }
    let name = attr_name(&pvc.metadata);
    let phase = old.status.as_ref().map(|s| &s.phase);
    if !matches!(phase, Some(PersistentVolumeClaimPhase::Bound)) {
        return Err(new_forbidden(
            "persistentvolumeclaims",
            &name,
            "Only bound persistent volume claims can be expanded",
        ));
    }
    // Growing a volume is only allowed when its StorageClass explicitly allows it.
    let (class, old_class) = (pvc_class(pvc), pvc_class(old));
    let allowed = if class.is_empty() || old_class.is_empty() || class != old_class {
        false
    } else {
        match storage
            .get::<StorageClass>(&format!("/registry/storageclasses/{class}"))
            .await
        {
            Ok(sc) => sc.allow_volume_expansion.unwrap_or(false),
            Err(_) => false,
        }
    };
    if !allowed {
        return Err(new_forbidden(
            "persistentvolumeclaims",
            &name,
            "only dynamically provisioned pvc can be resized and the storageclass that provisions the pvc must support resize",
        ));
    }
    Ok(())
}

/// `classDefaulterPlugin.Admit` (plugin/pkg/admission/network/
/// defaultingressclass/admission.go:84-129) with `getDefaultClass` (:132-158).
pub async fn default_ingress_class<S: Storage>(
    storage: &Arc<S>,
    ingress: &mut Ingress,
) -> Result<()> {
    // IngressClassName has been set, or the class annotation.
    let spec_class = ingress
        .spec
        .as_ref()
        .and_then(|s| s.ingress_class_name.as_ref());
    if spec_class.is_some()
        || ingress
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key("kubernetes.io/ingress.class"))
    {
        return Ok(());
    }
    let name = attr_name(&ingress.metadata);
    let classes: Vec<IngressClass> = storage
        .list("/registry/ingressclasses/")
        .await
        .map_err(|e| new_forbidden("ingresses", &name, e))?;
    let mut defaults: Vec<IngressClass> = classes
        .into_iter()
        .filter(|c| {
            c.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("ingressclass.kubernetes.io/is-default-class"))
                .is_some_and(|v| v == "true")
        })
        .collect();
    // Newest first, then by name.
    defaults.sort_by(|a, b| {
        b.metadata
            .creation_timestamp
            .cmp(&a.metadata.creation_timestamp)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });
    if let Some(default) = defaults.into_iter().next() {
        ingress
            .spec
            .get_or_insert_with(Default::default)
            .ingress_class_name = Some(default.metadata.name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
    use rusternetes_common::auth::{TokenManager, UserInfo};
    use rusternetes_common::authz::{AlwaysAllowAuthorizer, Authorizer};
    use rusternetes_common::observability::MetricsRegistry;
    use rusternetes_common::{Error, Result};
    use rusternetes_storage::{Storage, StorageBackend};
    use serde_json::{json, Value};

    use crate::admission::plugins::EnabledForTest;
    use crate::endpoints::handlers::admission::Admission;
    use crate::registry::rest::{Object, RequestContext};
    use crate::state::ApiServerState;

    fn state() -> ApiServerState {
        ApiServerState::new(
            Arc::new(StorageBackend::new_memory()),
            Arc::new(TokenManager::new(b"test-secret")),
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn Authorizer>,
            Arc::new(MetricsRegistry::new()),
            true,
        )
    }

    /// One request through the mutating then the validating chain, as
    /// `create.rs`/`update.rs` run it. Returns the admitted object.
    async fn run<T: Object>(
        state: &ApiServerState,
        (group, kind, resource): (&str, &str, &str),
        subresource: Option<&str>,
        obj: Value,
        old: Option<Value>,
    ) -> Result<T> {
        let gvk = GroupVersionKind {
            group: group.into(),
            version: "v1".into(),
            kind: kind.into(),
        };
        let gvr = GroupVersionResource {
            group: group.into(),
            version: "v1".into(),
            resource: resource.into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        };
        let admission = Admission {
            state,
            kind: &gvk,
            resource: &gvr,
            subresource,
            namespace: Some("default"),
            user: &user,
            dry_run: false,
            forbid_create: false,
        };
        let op = if old.is_some() {
            Operation::Update
        } else {
            Operation::Create
        };
        let old: Option<T> = old.map(|o| serde_json::from_value(o).unwrap());
        let obj: T = serde_json::from_value(obj).unwrap();
        let obj = admission.admit(op.clone(), obj, old.as_ref()).await?;
        admission
            .validate(
                &RequestContext::new(Some("default")),
                op,
                Some(&obj),
                old.as_ref(),
            )
            .await?;
        Ok(obj)
    }

    fn pod(containers: Value) -> Value {
        json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": containers}})
    }

    async fn run_pod(
        enabled: &[&str],
        sub: Option<&str>,
        obj: Value,
        old: Option<Value>,
    ) -> Result<rusternetes_common::resources::Pod> {
        let _g = EnabledForTest::new(enabled);
        run(&state(), ("", "Pod", "pods"), sub, obj, old).await
    }

    fn msg(e: Error) -> String {
        match e {
            Error::Forbidden(m) => m,
            Error::Status(s) => s.message.unwrap_or_default(),
            other => other.to_string(),
        }
    }

    // plugin/pkg/admission/deny/admission.go:46-54: AlwaysDeny denies every
    // request through NewForbidden.
    #[tokio::test]
    async fn always_deny_denies_everything() {
        let e = run_pod(
            &["AlwaysDeny"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(e, Error::Forbidden(_)), "{e:?}");
        assert_eq!(
            msg(e),
            "pods \"p\" is forbidden: admission control is denying all modifications"
        );
    }

    #[tokio::test]
    async fn always_admit_and_default_off() {
        run_pod(
            &["AlwaysAdmit"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await
        .unwrap();
        // AlwaysDeny is default-off.
        run_pod(
            &["ServiceAccount"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await
        .unwrap();
        let e = run_pod(
            &["AlwaysAdmit", "AlwaysDeny"],
            None,
            pod(json!([{"name":"c","image":"i"}])),
            None,
        )
        .await;
        assert!(e.is_err(), "AlwaysDeny still denies beside AlwaysAdmit");
    }

    fn svc(ips: &[&str]) -> Value {
        json!({"apiVersion":"v1","kind":"Service",
            "metadata":{"name":"s","namespace":"default"},
            "spec":{"externalIPs": ips}})
    }

    // denyserviceexternalips/admission_test.go TestAdmission (all nine cases).
    #[tokio::test]
    async fn deny_service_external_ips() {
        type Case<'a> = (&'a str, &'a [&'a str], Option<&'a [&'a str]>, bool);
        let cases: &[Case] = &[
            ("create: without externalIPs", &[], None, false),
            ("create: with externalIPs", &["1.1.1.1"], None, true),
            (
                "update: same",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["1.1.1.1", "2.2.2.2"]),
                false,
            ),
            (
                "update: reorder",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["2.2.2.2", "1.1.1.1"]),
                false,
            ),
            (
                "update: change",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["1.1.1.1", "3.3.3.3"]),
                true,
            ),
            (
                "update: add",
                &["1.1.1.1", "2.2.2.2"],
                Some(&["1.1.1.1"]),
                true,
            ),
            ("update: erase", &[], Some(&["1.1.1.1", "2.2.2.2"]), false),
            (
                "update: reduce from back",
                &["1.1.1.1"],
                Some(&["1.1.1.1", "2.2.2.2"]),
                false,
            ),
            (
                "update: reduce from front",
                &["2.2.2.2"],
                Some(&["1.1.1.1", "2.2.2.2"]),
                false,
            ),
        ];
        for (name, new, old, fail) in cases {
            let _g = EnabledForTest::new(&["DenyServiceExternalIPs"]);
            let r = run::<rusternetes_common::resources::Service>(
                &state(),
                ("", "Service", "services"),
                None,
                svc(new),
                old.map(svc),
            )
            .await;
            assert_eq!(r.is_err(), *fail, "{name}: {r:?}");
            if let Err(e) = r {
                assert_eq!(
                    msg(e),
                    "services \"s\" is forbidden: Use of external IPs is denied by admission control",
                    "{name}"
                );
            }
        }
        // Default-off: the same request passes.
        let _g = EnabledForTest::new(&[]);
        run::<rusternetes_common::resources::Service>(
            &state(),
            ("", "Service", "services"),
            None,
            svc(&["1.1.1.1"]),
            None,
        )
        .await
        .unwrap();
    }

    // alwayspullimages/admission_test.go TestAdmission: every init, regular
    // and ephemeral container (and image volume, KEP-4639) is set to Always.
    #[tokio::test]
    async fn always_pull_images_admit_sets_always() {
        let p = json!({"apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"default"},
            "spec":{
                "initContainers":[{"name":"i","image":"i","imagePullPolicy":"IfNotPresent"}],
                "containers":[{"name":"c","image":"i","imagePullPolicy":"Never"},{"name":"d","image":"i"}],
                "ephemeralContainers":[{"name":"e","image":"i","imagePullPolicy":"Never"}],
                "volumes":[{"name":"v","image":{"reference":"r","pullPolicy":"Never"}}]}});
        let got = run_pod(&["AlwaysPullImages"], None, p, None).await.unwrap();
        let spec = got.spec.unwrap();
        let always = Some("Always".to_string());
        assert_eq!(spec.init_containers.unwrap()[0].image_pull_policy, always);
        assert_eq!(spec.containers[0].image_pull_policy, always);
        assert_eq!(spec.containers[1].image_pull_policy, always);
        assert_eq!(
            spec.ephemeral_containers.unwrap()[0].image_pull_policy,
            always
        );
        assert_eq!(
            spec.volumes.unwrap()[0].image.as_ref().unwrap().pull_policy,
            always
        );
    }

    // alwayspullimages/admission_test.go TestOtherResources + TestUpdatePod.
    #[tokio::test]
    async fn always_pull_images_ignores_subresources_and_unchanged_images() {
        let never = pod(json!([{"name":"c","image":"image","imagePullPolicy":"Never"}]));
        let got = run_pod(&["AlwaysPullImages"], Some("exec"), never, None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().containers[0].image_pull_policy.as_deref(),
            Some("Never"),
            "pod subresource"
        );
        // An update that adds no image is left alone.
        let old = pod(json!([{"name":"c","image":"image","imagePullPolicy":"IfNotPresent"}]));
        let got = run_pod(&["AlwaysPullImages"], None, old.clone(), Some(old.clone()))
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().containers[0].image_pull_policy.as_deref(),
            Some("IfNotPresent")
        );
        // ... one that changes an image is mutated.
        let new = pod(json!([{"name":"c","image":"image2","imagePullPolicy":"IfNotPresent"}]));
        let got = run_pod(&["AlwaysPullImages"], None, new, Some(old))
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    // Validate (alwayspullimages/admission.go:92-124): the aggregate of
    // Forbidden/NotSupported errors is not an APIStatus, so it is a 500 with
    // the aggregate message (`Error()` of a multi-error aggregate is
    // `[a, b]`). Unreachable through the chain (Admit has just run), so the
    // validator is called directly.
    #[test]
    fn always_pull_images_validate_message() {
        let pod: rusternetes_common::resources::Pod = serde_json::from_value(json!({
            "apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},
            "spec":{"initContainers":[{"name":"i","image":"i","imagePullPolicy":"Never"}],
                    "containers":[{"name":"c","image":"i","imagePullPolicy":"IfNotPresent"}],
                    "volumes":[{"name":"v","image":{"reference":"r"}}]}}))
        .unwrap();
        let e = super::always_pull_images_validate("pods", &pod).unwrap_err();
        assert_eq!(
            msg(e),
            "[pods \"p\" is forbidden: spec.initContainers[0].imagePullPolicy: Unsupported value: \"Never\": supported values: \"Always\", \
pods \"p\" is forbidden: spec.containers[0].imagePullPolicy: Unsupported value: \"IfNotPresent\": supported values: \"Always\", \
pods \"p\" is forbidden: spec.volumes[0].image.pullPolicy: Unsupported value: \"\": supported values: \"Always\"]"
        );
    }

    // antiaffinity/admission_test.go TestInterPodAffinityAdmission.
    #[tokio::test]
    async fn limit_pod_hard_anti_affinity_topology() {
        let with = |key: &str| {
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"default"},
                "spec":{"containers":[{"name":"c","image":"i"}],
                "affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":
                    [{"labelSelector":{"matchLabels":{"a":"b"}},"topologyKey":key}]}}}})
        };
        let on = ["LimitPodHardAntiAffinityTopology"];
        run_pod(&on, None, with("kubernetes.io/hostname"), None)
            .await
            .unwrap();
        let e = run_pod(&on, None, with("zone"), None).await.unwrap_err();
        assert_eq!(
            msg(e),
            "pods \"p\" is forbidden: affinity.PodAntiAffinity.RequiredDuringScheduling has TopologyKey zone but only key kubernetes.io/hostname is allowed"
        );
        // Subresources are ignored; default-off.
        run_pod(&on, Some("status"), with("zone"), Some(with("zone")))
            .await
            .unwrap();
        run_pod(&[], None, with("zone"), None).await.unwrap();
        // No anti-affinity at all.
        run_pod(&on, None, pod(json!([{"name":"c","image":"i"}])), None)
            .await
            .unwrap();
    }

    fn pvc(size: &str, class: Option<&str>, phase: &str) -> Value {
        let mut spec = json!({"resources":{"requests":{"storage": size}}});
        if let Some(c) = class {
            spec["storageClassName"] = json!(c);
        }
        json!({"apiVersion":"v1","kind":"PersistentVolumeClaim",
            "metadata":{"name":"c","namespace":"default"},
            "spec":spec,"status":{"phase":phase}})
    }

    async fn storage_class(state: &ApiServerState, name: &str, expand: Option<bool>) {
        let mut sc = json!({"apiVersion":"storage.k8s.io/v1","kind":"StorageClass",
            "metadata":{"name":name},"provisioner":"kubernetes.io/glusterfs"});
        if let Some(e) = expand {
            sc["allowVolumeExpansion"] = json!(e);
        }
        state
            .storage
            .create(&format!("/registry/storageclasses/{name}"), &sc)
            .await
            .unwrap();
    }

    // resize/admission_test.go TestPVCResizeAdmission.
    #[tokio::test]
    async fn pvc_resize() {
        let _g = EnabledForTest::new(&["PersistentVolumeClaimResize"]);
        let state = state();
        storage_class(&state, "gold", Some(true)).await;
        storage_class(&state, "silver", Some(false)).await;
        storage_class(&state, "nil", None).await;
        let id = ("", "PersistentVolumeClaim", "persistentvolumeclaims");
        type P = rusternetes_common::resources::PersistentVolumeClaim;
        // gold allows expansion.
        run::<P>(
            &state,
            id,
            None,
            pvc("2Gi", Some("gold"), "Bound"),
            Some(pvc("1Gi", Some("gold"), "Bound")),
        )
        .await
        .unwrap();
        // silver and an unset flag do not.
        for class in ["silver", "nil"] {
            let e = run::<P>(
                &state,
                id,
                None,
                pvc("2Gi", Some(class), "Bound"),
                Some(pvc("1Gi", Some(class), "Bound")),
            )
            .await
            .unwrap_err();
            assert_eq!(
                msg(e),
                "persistentvolumeclaims \"c\" is forbidden: only dynamically provisioned pvc can be resized and the storageclass that provisions the pvc must support resize"
            );
        }
        // No class at all.
        let e = run::<P>(
            &state,
            id,
            None,
            pvc("2Gi", None, "Bound"),
            Some(pvc("1Gi", None, "Bound")),
        )
        .await
        .unwrap_err();
        assert!(msg(e).contains("only dynamically provisioned pvc"));
        // No change in size.
        run::<P>(
            &state,
            id,
            None,
            pvc("1Gi", Some("silver"), "Bound"),
            Some(pvc("1Gi", Some("silver"), "Pending")),
        )
        .await
        .unwrap();
        // Shrinking is not this plugin's business.
        run::<P>(
            &state,
            id,
            None,
            pvc("1Gi", Some("silver"), "Bound"),
            Some(pvc("2Gi", Some("silver"), "Bound")),
        )
        .await
        .unwrap();
        // Pending claim.
        let e = run::<P>(
            &state,
            id,
            None,
            pvc("2Gi", Some("silver"), "Pending"),
            Some(pvc("1Gi", Some("silver"), "Pending")),
        )
        .await
        .unwrap_err();
        assert!(msg(e).contains("Only bound persistent volume claims can be expanded"));
        // Subresource: ignored.
        run::<P>(
            &state,
            id,
            Some("status"),
            pvc("2Gi", Some("silver"), "Pending"),
            Some(pvc("1Gi", Some("silver"), "Pending")),
        )
        .await
        .unwrap();
    }

    // defaultingressclass/admission_test.go TestAdmission.
    #[tokio::test]
    async fn default_ingress_class() {
        let _g = EnabledForTest::new(&["DefaultIngressClass"]);
        let state = state();
        let ing = |spec: Value, ann: Option<Value>| {
            let mut v = json!({"apiVersion":"networking.k8s.io/v1","kind":"Ingress",
                "metadata":{"name":"i","namespace":"default"},"spec":spec});
            if let Some(a) = ann {
                v["metadata"]["annotations"] = a;
            }
            v
        };
        let id = ("networking.k8s.io", "Ingress", "ingresses");
        type I = rusternetes_common::resources::Ingress;
        let class = |name: &str, default: Option<&str>, created: Option<&str>| {
            let mut c = json!({"apiVersion":"networking.k8s.io/v1","kind":"IngressClass",
                "metadata":{"name":name},"spec":{"controller":"example.com/c"}});
            if let Some(d) = default {
                c["metadata"]["annotations"] =
                    json!({"ingressclass.kubernetes.io/is-default-class": d});
            }
            if let Some(t) = created {
                c["metadata"]["creationTimestamp"] = json!(t);
            }
            c
        };
        let put = |name: &str, c: Value| {
            let st = state.storage.clone();
            let key = format!("/registry/ingressclasses/{name}");
            async move { st.create(&key, &c).await.unwrap() }
        };
        // No default: untouched.
        put("nondefault1", class("nondefault1", Some("false"), None)).await;
        put("nodefault", class("nodefault", None, None)).await;
        put("emptydefault", class("emptydefault", Some(""), None)).await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(got.spec.unwrap().ingress_class_name, None);
        // One default.
        put("default1", class("default1", Some("true"), None)).await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().ingress_class_name.as_deref(),
            Some("default1")
        );
        // Explicit "" and "foo" are kept.
        for v in ["", "foo"] {
            let got: I = run(
                &state,
                id,
                None,
                ing(json!({"ingressClassName": v}), None),
                None,
            )
            .await
            .unwrap();
            assert_eq!(got.spec.unwrap().ingress_class_name.as_deref(), Some(v));
        }
        // The annotation counts as a class.
        let got: I = run(
            &state,
            id,
            None,
            ing(
                json!({}),
                Some(json!({"kubernetes.io/ingress.class":"foo"})),
            ),
            None,
        )
        .await
        .unwrap();
        assert_eq!(got.spec.unwrap().ingress_class_name, None);
        // Two defaults, same creation time: the lower name.
        put("default2", class("default2", Some("true"), None)).await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().ingress_class_name.as_deref(),
            Some("default1")
        );
        // Newer creation time wins.
        put(
            "newer",
            class("newer", Some("true"), Some("2099-01-01T00:00:00Z")),
        )
        .await;
        let got: I = run(&state, id, None, ing(json!({}), None), None)
            .await
            .unwrap();
        assert_eq!(
            got.spec.unwrap().ingress_class_name.as_deref(),
            Some("newer")
        );
        // Update (the plugin handles Create only).
        let got: I = run(
            &state,
            id,
            None,
            ing(json!({}), None),
            Some(ing(json!({}), None)),
        )
        .await
        .unwrap();
        assert_eq!(got.spec.unwrap().ingress_class_name, None);
    }
}
