//! The `ServiceAccount` admission plugin.
//!
//! Port of `plugin/pkg/admission/serviceaccount/admission.go` (release-1.35):
//! `Admit`, `Validate`, `shouldIgnore`, `shouldAutomount`,
//! `enforceMountableSecrets`, `getServiceAccount`, `limitSecretReferences`,
//! `limitEphemeralContainerSecretReferences` and `mountServiceAccountToken`
//! (the last lives in `rusternetes_common::serviceaccount`, shared with the
//! controllers that write pods straight to storage). The `NewForbidden`
//! wrapping is done by the chain in `endpoints/handlers/admission.rs`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rusternetes_common::resources::pod::{Container, EnvFromSource, EnvVar, PodSpec};
use rusternetes_common::resources::{Pod, ServiceAccount};
use rusternetes_storage::Storage;

/// `DefaultServiceAccountName` (admission.go:46).
pub const DEFAULT_SERVICE_ACCOUNT_NAME: &str = "default";

/// `EnforceMountableSecretsAnnotation` (admission.go:50).
pub const ENFORCE_MOUNTABLE_SECRETS_ANNOTATION: &str = "kubernetes.io/enforce-mountable-secrets";

/// `NewServiceAccount` sets `LimitSecretReferences: false` (admission.go:112-113;
/// "TODO: enable this once we've swept secret usage"). There is no flag for it
/// upstream either; only the per-ServiceAccount annotation enables the check.
pub const LIMIT_SECRET_REFERENCES: bool = false;

/// `api.MirrorPodAnnotationKey`.
pub use crate::handlers::node_restriction::MIRROR_POD_ANNOTATION_KEY;

/// `shouldIgnore` (admission.go:238-252) minus the object type assertion,
/// which the chain does by decoding a `Pod`: only core `pods`, on the
/// resource itself or its `ephemeralcontainers` subresource.
pub fn should_ignore(group: &str, resource: &str, subresource: Option<&str>) -> bool {
    !(group.is_empty()
        && resource == "pods"
        && matches!(subresource, None | Some("") | Some("ephemeralcontainers")))
}

/// `shouldAutomount` (admission.go:254-266): the pod's preference wins, then
/// the ServiceAccount's, defaulting to true.
pub fn should_automount(sa: &ServiceAccount, pod: &Pod) -> bool {
    if let Some(v) = pod
        .spec
        .as_ref()
        .and_then(|s| s.automount_service_account_token)
    {
        return v;
    }
    sa.automount_service_account_token.unwrap_or(true)
}

/// `enforceMountableSecrets` (admission.go:270-281): the global setting
/// overrides the annotation; an unparsable annotation value is false
/// (`strconv.ParseBool`).
pub fn enforce_mountable_secrets(sa: &ServiceAccount, limit_secret_references: bool) -> bool {
    if limit_secret_references {
        return true;
    }
    match sa
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ENFORCE_MOUNTABLE_SECRETS_ANNOTATION))
    {
        Some(v) => parse_bool(v).unwrap_or(false),
        None => false,
    }
}

/// `strconv.ParseBool`.
fn parse_bool(v: &str) -> Option<bool> {
    match v {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `getServiceAccount` (admission.go:284-312): the ServiceAccount, or the
/// `NewNotFound(api.Resource("serviceaccount"), name)` message. Upstream reads
/// its informer cache first, then the live client, retrying the default
/// account up to 10 times (a controller creates it) with a random 100-199ms
/// interval; storage here is the one authoritative read, so only the retry
/// loop is carried over.
pub async fn get_service_account<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    name: &str,
) -> Result<ServiceAccount, String> {
    get_service_account_with(storage, namespace, name, None).await
}

/// [`get_service_account`] with the retry interval fixed (tests).
pub async fn get_service_account_with<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    name: &str,
    retry_interval: Option<Duration>,
) -> Result<ServiceAccount, String> {
    let key = rusternetes_storage::build_key("serviceaccounts", Some(namespace), name);
    let attempts = if name == DEFAULT_SERVICE_ACCOUNT_NAME {
        10
    } else {
        1
    };
    let interval =
        retry_interval.unwrap_or_else(|| Duration::from_millis(rand::random_range(100..200)));
    for i in 0..attempts {
        if i != 0 {
            tokio::time::sleep(interval).await;
        }
        match storage.get::<ServiceAccount>(&key).await {
            Ok(sa) => return Ok(sa),
            Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Err(format!("serviceaccount \"{name}\" not found"))
}

fn secret_names_of_envs(
    env: &Option<Vec<EnvVar>>,
    env_from: &Option<Vec<EnvFromSource>>,
) -> Vec<(Option<String>, String)> {
    // (env var name, secret name); env var name is None for envFrom.
    let mut out = Vec::new();
    for e in env.iter().flatten() {
        if let Some(r) = e
            .value_from
            .as_ref()
            .and_then(|v| v.secret_key_ref.as_ref())
        {
            out.push((Some(e.name.clone()), r.name.clone()));
        }
    }
    for e in env_from.iter().flatten() {
        if let Some(r) = e.secret_ref.as_ref() {
            out.push((None, r.name.clone()));
        }
    }
    out
}

/// `limitSecretReferences` (admission.go:315-376), message for message.
pub fn limit_secret_references(sa: &ServiceAccount, pod: &Pod) -> Result<(), String> {
    let Some(spec) = pod.spec.as_ref() else {
        return Ok(());
    };
    let sa_name = &sa.metadata.name;
    let mountable: HashSet<&str> = sa
        .secrets
        .iter()
        .flatten()
        .map(|s| s.name.as_deref().unwrap_or(""))
        .collect();
    for volume in spec.volumes.iter().flatten() {
        let Some(secret) = volume.secret.as_ref() else {
            continue;
        };
        let secret_name = secret.secret_name.as_deref().unwrap_or("");
        if !mountable.contains(secret_name) {
            return Err(format!("volume with secret.secretName=\"{secret_name}\" is not allowed because service account {sa_name} does not reference that secret"));
        }
    }
    for (kind, containers) in [
        (
            "init container",
            spec.init_containers.as_deref().unwrap_or(&[]),
        ),
        ("container", spec.containers.as_slice()),
    ] {
        for c in containers {
            check_container(kind, c, &mountable, sa_name)?;
        }
    }
    // limit pull secrets as well
    let pull: HashSet<&str> = sa
        .image_pull_secrets
        .iter()
        .flatten()
        .map(|s| s.name.as_str())
        .collect();
    for (i, r) in spec.image_pull_secrets.iter().flatten().enumerate() {
        if !pull.contains(r.name.as_str()) {
            return Err(format!("imagePullSecrets[{i}].name=\"{}\" is not allowed because service account {sa_name} does not reference that imagePullSecret", r.name));
        }
    }
    Ok(())
}

fn check_container(
    kind: &str,
    c: &Container,
    mountable: &HashSet<&str>,
    sa_name: &str,
) -> Result<(), String> {
    check_refs(kind, &c.name, &c.env, &c.env_from, mountable, sa_name)
}

/// The env then envFrom loops shared by every container class.
fn check_refs(
    kind: &str,
    container: &str,
    env: &Option<Vec<EnvVar>>,
    env_from: &Option<Vec<EnvFromSource>>,
    mountable: &HashSet<&str>,
    sa_name: &str,
) -> Result<(), String> {
    let refs = secret_names_of_envs(env, env_from);
    // Upstream checks every env before any envFrom.
    for (var, secret) in refs.iter().filter(|(v, _)| v.is_some()) {
        if !mountable.contains(secret.as_str()) {
            return Err(format!("{kind} {container} with envVar {} referencing secret.secretName=\"{secret}\" is not allowed because service account {sa_name} does not reference that secret", var.as_deref().unwrap_or("")));
        }
    }
    for (_, secret) in refs.iter().filter(|(v, _)| v.is_none()) {
        if !mountable.contains(secret.as_str()) {
            return Err(format!("{kind} {container} with envFrom referencing secret.secretName=\"{secret}\" is not allowed because service account {sa_name} does not reference that secret"));
        }
    }
    Ok(())
}

/// `limitEphemeralContainerSecretReferences` (admission.go:379-414) after the
/// service account lookup: the ephemeral containers' secret references must
/// be ones the service account lists.
pub fn limit_ephemeral_container_secret_references(
    sa: &ServiceAccount,
    spec: &PodSpec,
) -> Result<(), String> {
    let mountable: HashSet<&str> = sa
        .secrets
        .iter()
        .flatten()
        .map(|s| s.name.as_deref().unwrap_or(""))
        .collect();
    for c in spec.ephemeral_containers.iter().flatten() {
        check_refs(
            "ephemeral container",
            &c.name,
            &c.env,
            &c.env_from,
            &mountable,
            &sa.metadata.name,
        )?;
    }
    Ok(())
}

/// `podutil.VisitPodSecretNames(pod, ..., podutil.AllContainers)`
/// (pkg/api/pod/util.go:119-202), as "does the pod reference any secret":
/// empty names are skipped (`skipEmptyNames`).
pub fn pod_references_secrets(spec: &PodSpec) -> bool {
    let non_empty = |n: &str| !n.is_empty();
    if spec
        .image_pull_secrets
        .iter()
        .flatten()
        .any(|r| non_empty(&r.name))
    {
        return true;
    }
    let ephemeral = spec.ephemeral_containers.iter().flatten();
    let in_env = |env: &Option<Vec<EnvVar>>, env_from: &Option<Vec<EnvFromSource>>| {
        secret_names_of_envs(env, env_from)
            .iter()
            .any(|(_, n)| non_empty(n))
    };
    if spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .any(|c| in_env(&c.env, &c.env_from))
        || ephemeral.into_iter().any(|c| in_env(&c.env, &c.env_from))
    {
        return true;
    }
    let json_name = |v: &Option<serde_json::Value>, key: &str| {
        v.as_ref()
            .and_then(|v| v.get(key))
            .and_then(|r| r.get("name"))
            .and_then(|n| n.as_str())
            .is_some_and(non_empty)
    };
    for v in spec.volumes.iter().flatten() {
        let l = &v.legacy_sources;
        let found = l
            .azure_file
            .as_ref()
            .and_then(|a| a.get("secretName"))
            .and_then(|n| n.as_str())
            .is_some_and(non_empty)
            || json_name(&l.cephfs, "secretRef")
            || json_name(&l.cinder, "secretRef")
            || json_name(&l.flex_volume, "secretRef")
            || json_name(&l.rbd, "secretRef")
            || json_name(&l.scale_io, "secretRef")
            || json_name(&l.storageos, "secretRef")
            || v.projected.as_ref().is_some_and(|p| {
                p.sources.iter().flatten().any(|s| {
                    s.secret.as_ref().is_some_and(|s| {
                        // Upstream visits the name even when unset: skipEmptyNames
                        // drops it.
                        s.name.as_deref().is_some_and(non_empty)
                    })
                })
            })
            || v.secret
                .as_ref()
                .is_some_and(|s| s.secret_name.as_deref().is_some_and(non_empty))
            || v.iscsi
                .as_ref()
                .and_then(|i| i.secret_ref.as_ref())
                .and_then(|r| r.name.as_deref())
                .is_some_and(non_empty)
            || v.csi
                .as_ref()
                .and_then(|c| c.node_publish_secret_ref.as_ref())
                .and_then(|r| r.name.as_deref())
                .is_some_and(non_empty);
        if found {
            return true;
        }
    }
    false
}

/// `Validate`'s mirror pod branch (admission.go:195-217): a mirror pod may
/// reference no service account, no secrets and no ServiceAccountToken
/// volume projection.
pub fn validate_mirror_pod(pod: &Pod) -> Result<(), String> {
    let Some(spec) = pod.spec.as_ref() else {
        return Ok(());
    };
    if spec
        .service_account_name
        .as_deref()
        .is_some_and(|s| !s.is_empty())
    {
        return Err("a mirror pod may not reference service accounts".into());
    }
    if pod_references_secrets(spec) {
        return Err("a mirror pod may not reference secrets".into());
    }
    for v in spec.volumes.iter().flatten() {
        if v.projected.as_ref().is_some_and(|p| {
            p.sources
                .iter()
                .flatten()
                .any(|s| s.service_account_token.is_some())
        }) {
            return Err("a mirror pod may not use ServiceAccountToken volume projections".into());
        }
    }
    Ok(())
}

/// Whether the pod carries `api.MirrorPodAnnotationKey`.
pub fn is_mirror_pod(pod: &Pod) -> bool {
    pod.metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
}

/// `Plugin.Validate` (admission.go:178-236) after `shouldIgnore`, for a pod
/// on CREATE or on UPDATE of `ephemeralcontainers`. `Ok(Err(msg))` is a
/// `NewForbidden`, `Err(msg)` a plain error (the ephemeral path's
/// `limitSecretReferences` failure is returned bare upstream, :228-232).
pub async fn validate<S: Storage>(
    storage: &Arc<S>,
    create: bool,
    update: bool,
    subresource: Option<&str>,
    namespace: &str,
    pod: &Pod,
) -> Result<(), ValidateError> {
    let spec_default;
    let spec = match pod.spec.as_ref() {
        Some(s) => s,
        None => {
            spec_default = PodSpec::default();
            &spec_default
        }
    };
    if update && subresource == Some("ephemeralcontainers") {
        // limitEphemeralContainerSecretReferences
        let Some(sa_name) = spec
            .service_account_name
            .as_deref()
            .filter(|s| !s.is_empty())
        else {
            return Err(ValidateError::Forbidden(format!(
                "no service account specified for pod {namespace}/{}",
                pod.metadata.name
            )));
        };
        let sa = get_service_account(storage, namespace, sa_name)
            .await
            .map_err(|e| {
                ValidateError::Forbidden(format!(
                    "error looking up service account {namespace}/{sa_name}: {e}"
                ))
            })?;
        if !enforce_mountable_secrets(&sa, LIMIT_SECRET_REFERENCES) {
            return Ok(());
        }
        return limit_ephemeral_container_secret_references(&sa, spec)
            .map_err(ValidateError::Plain);
    }
    if !create {
        return Ok(());
    }
    if is_mirror_pod(pod) {
        return validate_mirror_pod(pod).map_err(ValidateError::Forbidden);
    }
    let Some(sa_name) = spec
        .service_account_name
        .as_deref()
        .filter(|s| !s.is_empty())
    else {
        return Err(ValidateError::Forbidden(format!(
            "no service account specified for pod {namespace}/{}",
            pod.metadata.name
        )));
    };
    let sa = get_service_account(storage, namespace, sa_name)
        .await
        .map_err(|e| {
            ValidateError::Forbidden(format!(
                "error looking up service account {namespace}/{sa_name}: {e}"
            ))
        })?;
    if enforce_mountable_secrets(&sa, LIMIT_SECRET_REFERENCES) {
        limit_secret_references(&sa, pod).map_err(ValidateError::Forbidden)?;
    }
    Ok(())
}

/// How `Validate` failed: `NewForbidden`, or a bare error.
#[derive(Debug, PartialEq, Eq)]
pub enum ValidateError {
    Forbidden(String),
    Plain(String),
}
