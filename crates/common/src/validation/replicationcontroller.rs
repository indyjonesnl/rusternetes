//! ReplicationController (core/v1) validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateReplicationControllerSpec`
//! (release-1.35).
//!
//! Intended to run *after* defaulting (the api-server defaults an absent
//! `selector` from the template labels), matching upstream where validation
//! sees the defaulted object.

use crate::resources::workloads::{
    ReplicationController, ReplicationControllerSpec, ReplicationControllerStatus,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_nonnegative_field, validate_object_meta,
    validate_object_meta_update,
};
use crate::validation::podtemplate::validate_pod_template_spec;

/// Validate a `ReplicationControllerSpec`. Mirrors upstream
/// `ValidateReplicationControllerSpec`: non-negative `replicas` /
/// `minReadySeconds`, a non-empty `selector`, and template labels that satisfy
/// it.
pub fn validate_replication_controller_spec(
    spec: &ReplicationControllerSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // Upstream `ValidateReplicationControllerSpec` (validation.go:7056-7060):
    // `replicas` is required, then must be non-negative.
    match spec.replicas {
        None => {
            errs.push(Error::required(&fld_path.child("replicas"), ""));
        }
        Some(r) if r < 0 => {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
        Some(_) => {}
    }
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    let selector_empty = spec.selector.as_ref().is_none_or(|s| s.is_empty());
    if selector_empty {
        errs.push(Error::required(&fld_path.child("selector"), ""));
    } else {
        let selector = spec.selector.as_ref().unwrap();
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        let matches = selector
            .iter()
            .all(|(k, v)| template_labels.get(k) == Some(v));
        if !matches {
            errs.push(Error::invalid(
                &fld_path.child("template").child("metadata").child("labels"),
                template_labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(","),
                "`selector` does not match template `labels`",
            ));
        }
    }

    // `ValidatePodTemplateSpecForRC` (validation.go:7026-7050) validates the
    // template as a pod template, and then holds it to the RC rules.
    errs.extend(validate_pod_template_spec(
        &spec.template,
        &fld_path.child("template"),
        false,
    ));

    // Upstream `ValidatePodTemplateSpecForRC` (validation.go:7041-7046): the RC
    // pod template must use `restartPolicy: Always`, and `activeDeadlineSeconds`
    // is forbidden.
    let template_path = fld_path.child("template").child("spec");
    let restart_policy = spec.template.spec.restart_policy.as_deref();
    // An absent restartPolicy defaults to Always upstream; only a present,
    // non-Always value is rejected here (defaulting runs before validation).
    if let Some(rp) = restart_policy {
        if rp != "Always" {
            errs.push(Error::not_supported(
                &template_path.child("restartPolicy"),
                rp.to_string(),
                &["Always"],
            ));
        }
    }
    if spec.template.spec.active_deadline_seconds.is_some() {
        errs.push(Error::forbidden(
            &template_path.child("activeDeadlineSeconds"),
            "activeDeadlineSeconds in ReplicationController is not Supported",
        ));
    }

    errs
}

/// Upstream `ValidateReplicationController` (validation.go:6969-6977). The
/// name rule is `LongName`, a DNS subdomain. Run after defaulting.
pub fn validate_replication_controller(rc: &ReplicationController) -> ErrorList {
    let mut errs = validate_object_meta(
        &rc.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_replication_controller_spec(
        &rc.spec,
        &Path::new("spec"),
    ));
    errs
}

/// Upstream `ValidateReplicationControllerUpdate` (validation.go:6979-6984).
pub fn validate_replication_controller_update(
    rc: &ReplicationController,
    old: &ReplicationController,
) -> ErrorList {
    let mut errs = validate_object_meta_update(&rc.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_replication_controller_spec(
        &rc.spec,
        &Path::new("spec"),
    ));
    errs
}

/// Upstream `ValidateReplicationControllerStatusUpdate` (validation.go:6986-6990).
pub fn validate_replication_controller_status_update(
    rc: &ReplicationController,
    old: &ReplicationController,
) -> ErrorList {
    let mut errs = validate_object_meta_update(&rc.metadata, &old.metadata, &Path::new("metadata"));
    let default = ReplicationControllerStatus::default();
    errs.extend(validate_replication_controller_status(
        rc.status.as_ref().unwrap_or(&default),
        &Path::new("status"),
    ));
    errs
}

/// Upstream `ValidateReplicationControllerStatus` (validation.go:6992-7014).
fn validate_replication_controller_status(
    status: &ReplicationControllerStatus,
    path: &Path,
) -> ErrorList {
    let replicas = status.replicas;
    let fully_labeled = status.fully_labeled_replicas.unwrap_or(0);
    let ready = status.ready_replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);
    let mut errs = validate_nonnegative_field(replicas as i64, &path.child("replicas"));
    errs.extend(validate_nonnegative_field(
        fully_labeled as i64,
        &path.child("fullyLabeledReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        ready as i64,
        &path.child("readyReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        available as i64,
        &path.child("availableReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        status.observed_generation.unwrap_or(0),
        &path.child("observedGeneration"),
    ));
    let msg = "cannot be greater than status.replicas";
    if fully_labeled > replicas {
        errs.push(Error::invalid(
            &path.child("fullyLabeledReplicas"),
            fully_labeled,
            msg,
        ));
    }
    if ready > replicas {
        errs.push(Error::invalid(&path.child("readyReplicas"), ready, msg));
    }
    if available > replicas {
        errs.push(Error::invalid(
            &path.child("availableReplicas"),
            available,
            msg,
        ));
    }
    if available > ready {
        errs.push(Error::invalid(
            &path.child("availableReplicas"),
            available,
            "cannot be greater than readyReplicas",
        ));
    }
    errs
}
