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
