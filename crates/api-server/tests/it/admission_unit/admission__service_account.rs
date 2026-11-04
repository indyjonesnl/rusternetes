//! Tests for `src/admission/service_account.rs` and the `ServiceAccount`
//! plugin entry points, ported from
//! `plugin/pkg/admission/serviceaccount/admission_test.go` (release-1.35),
//! plus the `Validate` / ephemeral-container / `shouldIgnore` paths that file
//! does not cover (admission.go:138-236, :379-414).
#![allow(unused_imports, dead_code)]

use std::sync::Arc;
use std::time::Duration;

use rusternetes_api_server::admission::inject_service_account_token;
use rusternetes_api_server::admission::service_account::{
    enforce_mountable_secrets, get_service_account_with,
    limit_ephemeral_container_secret_references, limit_secret_references, should_automount,
    should_ignore, validate, ValidateError,
};
use rusternetes_common::resources::{Pod, ServiceAccount};
use rusternetes_storage::{MemoryStorage, Storage};
use serde_json::{json, Value};

const NS: &str = "myns";

fn sa(v: Value) -> ServiceAccount {
    let mut base = json!({"apiVersion":"v1","kind":"ServiceAccount",
        "metadata":{"name":"default","namespace":NS}});
    for (k, val) in v.as_object().unwrap() {
        if k == "annotations" {
            base["metadata"]["annotations"] = val.clone();
        } else {
            base[k] = val.clone();
        }
    }
    serde_json::from_value(base).unwrap()
}

fn pod(spec: Value) -> Pod {
    serde_json::from_value(json!({"apiVersion":"v1","kind":"Pod",
        "metadata":{"name":"myname","namespace":NS},"spec":spec}))
    .unwrap()
}

fn mirror(spec: Value) -> Pod {
    let mut p = pod(spec);
    p.metadata.annotations = Some(
        [(
            "kubernetes.io/config.mirror".to_string(),
            "true".to_string(),
        )]
        .into_iter()
        .collect(),
    );
    p
}

async fn store_with(sas: Vec<ServiceAccount>) -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    for s in sas {
        let key = format!(
            "/registry/serviceaccounts/{}/{}",
            s.metadata.namespace.clone().unwrap_or_default(),
            s.metadata.name
        );
        storage.create(&key, &s).await.unwrap();
    }
    storage
}

async fn admit(storage: &Arc<MemoryStorage>, p: &mut Pod) -> Result<(), String> {
    inject_service_account_token(storage, NS, p).await
}

async fn create_validate(storage: &Arc<MemoryStorage>, p: &Pod) -> Result<(), ValidateError> {
    validate(storage, true, false, None, NS, p).await
}

// --- shouldIgnore (TestIgnoresNonPodResource, subresources) ---

#[test]
fn ignores_non_pod_resources_and_foreign_subresources() {
    assert!(should_ignore("", "customresource", None));
    assert!(should_ignore("apps", "pods", None));
    assert!(should_ignore("", "pods", Some("status")));
    assert!(should_ignore("", "pods", Some("binding")));
    assert!(!should_ignore("", "pods", None));
    assert!(!should_ignore("", "pods", Some("ephemeralcontainers")));
}

// --- mirror pods ---

#[tokio::test]
async fn ignores_mirror_pod() {
    // TestIgnoresMirrorPod: no service account, no secrets; not mutated.
    let storage = store_with(vec![]).await;
    let mut p = mirror(json!({"containers":[{"name":"c","image":"i"}],"volumes":[{"name":"v"}]}));
    admit(&storage, &mut p).await.unwrap();
    assert!(p.spec.unwrap().service_account_name.is_none());
}

#[tokio::test]
async fn rejects_mirror_pod_with_service_account() {
    let storage = store_with(vec![sa(json!({}))]).await;
    let mut p = mirror(json!({"containers":[],"serviceAccountName":"default"}));
    assert_eq!(
        admit(&storage, &mut p).await.unwrap_err(),
        "a mirror pod may not reference service accounts"
    );
}

#[tokio::test]
async fn rejects_mirror_pod_with_secret_volumes() {
    let storage = store_with(vec![]).await;
    let mut p = mirror(
        json!({"containers":[],"volumes":[{"name":"v","secret":{"secretName":"mysecret"}}]}),
    );
    assert_eq!(
        admit(&storage, &mut p).await.unwrap_err(),
        "a mirror pod may not reference secrets"
    );
}

#[tokio::test]
async fn rejects_mirror_pod_with_other_secret_references() {
    let storage = store_with(vec![]).await;
    for spec in [
        json!({"containers":[],"imagePullSecrets":[{"name":"s"}]}),
        json!({"containers":[{"name":"c","env":[{"name":"E","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]}]}),
        json!({"containers":[{"name":"c","envFrom":[{"secretRef":{"name":"s"}}]}]}),
        json!({"containers":[],"volumes":[{"name":"v","projected":{"sources":[{"secret":{"name":"s"}}]}}]}),
        json!({"containers":[],"volumes":[{"name":"v","rbd":{"secretRef":{"name":"s"}}}]}),
    ] {
        let mut p = mirror(spec);
        assert_eq!(
            admit(&storage, &mut p).await.unwrap_err(),
            "a mirror pod may not reference secrets"
        );
    }
}

#[tokio::test]
async fn rejects_mirror_pod_with_service_account_token_projections() {
    let storage = store_with(vec![]).await;
    let mut p = mirror(json!({"containers":[],"volumes":[{"name":"v",
        "projected":{"sources":[{"serviceAccountToken":{"path":"token"}}]}}]}));
    assert_eq!(
        admit(&storage, &mut p).await.unwrap_err(),
        "a mirror pod may not use ServiceAccountToken volume projections"
    );
}

// --- Admit ---

#[tokio::test]
async fn assigns_default_service_account_and_bound_token() {
    // TestAutomountsAPIToken (container half).
    let storage = store_with(vec![sa(json!({}))]).await;
    let mut p = pod(json!({"containers":[{"name":"c","image":"i"}]}));
    admit(&storage, &mut p).await.unwrap();
    let spec = p.spec.unwrap();
    assert_eq!(spec.service_account_name.as_deref(), Some("default"));
    let volumes = spec.volumes.unwrap();
    assert_eq!(volumes.len(), 1);
    assert!(volumes[0].name.starts_with("kube-api-access"));
    assert_eq!(
        volumes[0]
            .projected
            .as_ref()
            .unwrap()
            .sources
            .as_ref()
            .unwrap()
            .len(),
        3
    );
    let mounts = spec.containers[0].volume_mounts.as_ref().unwrap();
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].name, volumes[0].name);
    assert_eq!(
        mounts[0].mount_path,
        "/var/run/secrets/kubernetes.io/serviceaccount"
    );
    assert_eq!(mounts[0].read_only, Some(true));
}

#[tokio::test]
async fn automounts_into_init_containers() {
    // TestAutomountsAPIToken (init container half).
    let storage = store_with(vec![sa(json!({}))]).await;
    let mut p = pod(json!({"containers":[],"initContainers":[{"name":"i","image":"i"}]}));
    admit(&storage, &mut p).await.unwrap();
    let spec = p.spec.unwrap();
    assert_eq!(spec.volumes.unwrap().len(), 1);
    assert_eq!(
        spec.init_containers.unwrap()[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn respects_existing_mount() {
    // TestRespectsExistingMount: no volume when no container needs it.
    let storage = store_with(vec![sa(json!({}))]).await;
    for spec in [
        json!({"containers":[{"name":"c","image":"i","volumeMounts":[
            {"name":"my-custom-mount","mountPath":"/var/run/secrets/kubernetes.io/serviceaccount"}]}]}),
        json!({"containers":[],"initContainers":[{"name":"c","image":"i","volumeMounts":[
            {"name":"my-custom-mount","mountPath":"/var/run/secrets/kubernetes.io/serviceaccount"}]}]}),
    ] {
        let mut p = pod(spec);
        admit(&storage, &mut p).await.unwrap();
        let spec = p.spec.unwrap();
        assert!(spec.volumes.is_none_or(|v| v.is_empty()));
        let c = spec
            .containers
            .first()
            .or(spec.init_containers.as_ref().and_then(|i| i.first()))
            .unwrap();
        let mounts = c.volume_mounts.as_ref().unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].name, "my-custom-mount");
        assert_ne!(mounts[0].read_only, Some(true));
    }
}

#[tokio::test]
async fn reuses_existing_token_volume_name() {
    // mountServiceAccountToken :418-427.
    let storage = store_with(vec![sa(json!({}))]).await;
    let mut p = pod(json!({"containers":[{"name":"c","image":"i"}],
        "volumes":[{"name":"kube-api-access-abc12","emptyDir":{}}]}));
    admit(&storage, &mut p).await.unwrap();
    let spec = p.spec.unwrap();
    assert_eq!(spec.volumes.as_ref().unwrap().len(), 1);
    assert_eq!(
        spec.containers[0].volume_mounts.as_ref().unwrap()[0].name,
        "kube-api-access-abc12"
    );
}

#[tokio::test]
async fn fetches_service_account_and_denies_invalid() {
    // TestFetchesUncachedServiceAccount / TestDeniesInvalidServiceAccount.
    let storage = store_with(vec![]).await;
    let mut p = pod(json!({"containers":[]}));
    p.spec.as_mut().unwrap().service_account_name = Some("nope".into());
    let err = admit(&storage, &mut p).await.unwrap_err();
    assert_eq!(
        err,
        "error looking up service account myns/nope: serviceaccount \"nope\" not found"
    );
}

#[tokio::test]
async fn default_service_account_is_retried_until_it_exists() {
    // getServiceAccount :293-300: the default account gets 10 attempts.
    let storage = store_with(vec![]).await;
    let s2 = storage.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        let a = sa(json!({}));
        s2.create("/registry/serviceaccounts/myns/default", &a)
            .await
            .unwrap();
    });
    let got =
        get_service_account_with(&storage, NS, "default", Some(Duration::from_millis(20))).await;
    writer.await.unwrap();
    assert!(got.is_ok());
    // A non-default account is looked up once.
    let t = std::time::Instant::now();
    assert!(
        get_service_account_with(&storage, NS, "other", Some(Duration::from_millis(500)))
            .await
            .is_err()
    );
    assert!(t.elapsed() < Duration::from_millis(400));
}

#[test]
fn automount_precedence() {
    // shouldAutomount :254-266.
    let on = pod(json!({"containers":[],"automountServiceAccountToken":true}));
    let off = pod(json!({"containers":[],"automountServiceAccountToken":false}));
    let unset = pod(json!({"containers":[]}));
    let sa_off = sa(json!({"automountServiceAccountToken":false}));
    let sa_on = sa(json!({"automountServiceAccountToken":true}));
    let sa_unset = sa(json!({}));
    assert!(should_automount(&sa_off, &on));
    assert!(!should_automount(&sa_on, &off));
    assert!(!should_automount(&sa_off, &unset));
    assert!(should_automount(&sa_on, &unset));
    assert!(should_automount(&sa_unset, &unset));
}

#[tokio::test]
async fn service_account_opt_out_skips_token_but_copies_pull_secrets() {
    let storage = store_with(vec![sa(json!({
        "automountServiceAccountToken": false,
        "imagePullSecrets":[{"name":"foo"}]}))])
    .await;
    let mut p = pod(json!({"containers":[{"name":"c","image":"i"}]}));
    admit(&storage, &mut p).await.unwrap();
    let spec = p.spec.unwrap();
    assert!(spec.volumes.is_none());
    assert_eq!(spec.image_pull_secrets.unwrap()[0].name, "foo");
}

#[tokio::test]
async fn add_and_do_not_add_image_pull_secrets() {
    // TestAddImagePullSecrets / TestDoNotAddImagePullSecrets.
    let storage = store_with(vec![sa(
        json!({"imagePullSecrets":[{"name":"foo"},{"name":"bar"}]}),
    )])
    .await;
    let mut p = pod(json!({"containers":[]}));
    admit(&storage, &mut p).await.unwrap();
    let names: Vec<_> = p
        .spec
        .unwrap()
        .image_pull_secrets
        .unwrap()
        .into_iter()
        .map(|r| r.name)
        .collect();
    assert_eq!(names, ["foo", "bar"]);
    let mut p = pod(json!({"containers":[],"imagePullSecrets":[{"name":"foo"}]}));
    admit(&storage, &mut p).await.unwrap();
    assert_eq!(p.spec.unwrap().image_pull_secrets.unwrap().len(), 1);
}

#[tokio::test]
async fn pod_without_spec_gets_default_service_account() {
    // TestFetchesUncachedServiceAccount: `pod := &api.Pod{}`.
    let storage = store_with(vec![sa(json!({}))]).await;
    let mut p: Pod = serde_json::from_value(json!({"apiVersion":"v1","kind":"Pod",
        "metadata":{"name":"myname"}}))
    .unwrap();
    admit(&storage, &mut p).await.unwrap();
    assert_eq!(
        p.spec.unwrap().service_account_name.as_deref(),
        Some("default")
    );
}

// --- enforceMountableSecrets / limitSecretReferences ---

#[test]
fn enforce_mountable_secrets_flag_and_annotation() {
    // TestAllowUnreferencedSecretVolumesForPermissiveSAs' counterpart.
    let ann = |v: &str| sa(json!({"annotations":{"kubernetes.io/enforce-mountable-secrets":v}}));
    assert!(enforce_mountable_secrets(&sa(json!({})), true));
    assert!(!enforce_mountable_secrets(&sa(json!({})), false));
    assert!(enforce_mountable_secrets(&ann("true"), false));
    assert!(enforce_mountable_secrets(&ann("1"), false));
    assert!(!enforce_mountable_secrets(&ann("false"), false));
    assert!(!enforce_mountable_secrets(&ann("yes"), false));
    assert!(enforce_mountable_secrets(&ann("false"), true));
}

#[test]
fn rejects_unreferenced_secret_volumes_and_env() {
    // TestRejectsUnreferencedSecretVolumes.
    let s = sa(json!({}));
    let e = limit_secret_references(
        &s,
        &pod(json!({"containers":[],"volumes":[{"name":"v","secret":{"secretName":"foo"}}]})),
    )
    .unwrap_err();
    assert_eq!(e, "volume with secret.secretName=\"foo\" is not allowed because service account default does not reference that secret");
    let e = limit_secret_references(
        &s,
        &pod(json!({"containers":[{"name":"container-1","env":[{"name":"env-1","valueFrom":{"secretKeyRef":{"name":"foo","key":"k"}}}]}]})),
    )
    .unwrap_err();
    assert_eq!(e, "container container-1 with envVar env-1 referencing secret.secretName=\"foo\" is not allowed because service account default does not reference that secret");
    let e = limit_secret_references(
        &s,
        &pod(
            json!({"containers":[{"name":"container-1","envFrom":[{"secretRef":{"name":"foo"}}]}]}),
        ),
    )
    .unwrap_err();
    assert_eq!(e, "container container-1 with envFrom referencing secret.secretName=\"foo\" is not allowed because service account default does not reference that secret");
    let e = limit_secret_references(
        &s,
        &pod(json!({"containers":[],"initContainers":[{"name":"i","envFrom":[{"secretRef":{"name":"foo"}}]}]})),
    )
    .unwrap_err();
    assert!(e.starts_with("init container i with envFrom"));
}

#[test]
fn allows_referenced_secrets() {
    // TestAllowsReferencedSecret.
    let s = sa(json!({"secrets":[{"name":"foo"}]}));
    limit_secret_references(
        &s,
        &pod(json!({"containers":[{"name":"c","env":[{"name":"E","valueFrom":{"secretKeyRef":{"name":"foo","key":"k"}}}],
            "envFrom":[{"secretRef":{"name":"foo"}}]}],
            "volumes":[{"name":"v","secret":{"secretName":"foo"}}]})),
    )
    .unwrap();
}

#[test]
fn image_pull_secrets_are_limited() {
    // TestAllowsReferencedImagePullSecrets / TestRejectsUnreferencedImagePullSecrets.
    let s = sa(json!({"imagePullSecrets":[{"name":"foo"}]}));
    limit_secret_references(
        &s,
        &pod(json!({"containers":[],"imagePullSecrets":[{"name":"foo"}]})),
    )
    .unwrap();
    let e = limit_secret_references(
        &sa(json!({})),
        &pod(json!({"containers":[],"imagePullSecrets":[{"name":"foo"}]})),
    )
    .unwrap_err();
    assert_eq!(e, "imagePullSecrets[0].name=\"foo\" is not allowed because service account default does not reference that imagePullSecret");
}

// --- Validate ---

#[tokio::test]
async fn validate_requires_service_account_name() {
    let storage = store_with(vec![sa(json!({}))]).await;
    let p = pod(json!({"containers":[]}));
    assert_eq!(
        create_validate(&storage, &p).await.unwrap_err(),
        ValidateError::Forbidden("no service account specified for pod myns/myname".into())
    );
}

#[tokio::test]
async fn validate_requires_existing_service_account() {
    let storage = store_with(vec![]).await;
    let p = pod(json!({"containers":[],"serviceAccountName":"nope"}));
    assert_eq!(
        create_validate(&storage, &p).await.unwrap_err(),
        ValidateError::Forbidden(
            "error looking up service account myns/nope: serviceaccount \"nope\" not found".into()
        )
    );
}

#[tokio::test]
async fn validate_enforces_annotated_service_account_only() {
    // TestAllowUnreferencedSecretVolumesForPermissiveSAs.
    let storage = store_with(vec![
        sa(json!({"annotations":{"kubernetes.io/enforce-mountable-secrets":"true"}})),
        {
            let mut s = sa(json!({}));
            s.metadata.name = "loose".into();
            s
        },
    ])
    .await;
    let volumes = json!([{"name":"v","secret":{"secretName":"foo"}}]);
    let strict = pod(json!({"containers":[],"serviceAccountName":"default","volumes":volumes}));
    assert!(matches!(
        create_validate(&storage, &strict).await,
        Err(ValidateError::Forbidden(_))
    ));
    let loose = pod(json!({"containers":[],"serviceAccountName":"loose","volumes":volumes}));
    create_validate(&storage, &loose).await.unwrap();
}

#[tokio::test]
async fn validate_mirror_pod_needs_no_service_account() {
    let storage = store_with(vec![]).await;
    create_validate(&storage, &mirror(json!({"containers":[]})))
        .await
        .unwrap();
    assert!(create_validate(
        &storage,
        &mirror(json!({"containers":[],"serviceAccountName":"x"}))
    )
    .await
    .is_err());
}

#[tokio::test]
async fn validate_ignores_plain_updates() {
    // "we only validate pod specs during create requests" (:190-193).
    let storage = store_with(vec![]).await;
    let p = pod(json!({"containers":[]}));
    validate(&storage, false, true, None, NS, &p).await.unwrap();
}

// --- ephemeral containers ---

fn eph_pod(secret: &str) -> Pod {
    pod(
        json!({"containers":[{"name":"c","image":"i"}],"serviceAccountName":"default",
        "ephemeralContainers":[{"name":"dbg","image":"i",
            "env":[{"name":"E","valueFrom":{"secretKeyRef":{"name":secret,"key":"k"}}}],
            "envFrom":[{"secretRef":{"name":secret}}]}]}),
    )
}

#[tokio::test]
async fn ephemeral_update_requires_service_account_and_lookup() {
    let storage = store_with(vec![]).await;
    let p = pod(json!({"containers":[]}));
    let u = |p: Pod| {
        let storage = storage.clone();
        async move { validate(&storage, false, true, Some("ephemeralcontainers"), NS, &p).await }
    };
    assert_eq!(
        u(p).await.unwrap_err(),
        ValidateError::Forbidden("no service account specified for pod myns/myname".into())
    );
    assert!(matches!(
        u(pod(json!({"containers":[],"serviceAccountName":"nope"}))).await,
        Err(ValidateError::Forbidden(m)) if m.starts_with("error looking up service account myns/nope")
    ));
}

#[tokio::test]
async fn ephemeral_update_limits_secrets_only_when_enforced() {
    let enforcing = sa(json!({"secrets":[{"name":"ok"}],
        "annotations":{"kubernetes.io/enforce-mountable-secrets":"true"}}));
    let storage = store_with(vec![enforcing]).await;
    let v = |p: Pod| {
        let storage = storage.clone();
        async move { validate(&storage, false, true, Some("ephemeralcontainers"), NS, &p).await }
    };
    v(eph_pod("ok")).await.unwrap();
    // The failure is a bare error upstream (admission.go:228-232), not Forbidden.
    match v(eph_pod("bad")).await {
        Err(ValidateError::Plain(m)) => assert_eq!(m, "ephemeral container dbg with envVar E referencing secret.secretName=\"bad\" is not allowed because service account default does not reference that secret"),
        other => panic!("{other:?}"),
    }
    let loose = store_with(vec![sa(json!({}))]).await;
    validate(
        &loose,
        false,
        true,
        Some("ephemeralcontainers"),
        NS,
        &eph_pod("bad"),
    )
    .await
    .unwrap();
}

#[test]
fn ephemeral_envfrom_message() {
    let s = sa(json!({}));
    let p = pod(
        json!({"containers":[],"ephemeralContainers":[{"name":"dbg","image":"i","envFrom":[{"secretRef":{"name":"x"}}]}]}),
    );
    let e = limit_ephemeral_container_secret_references(&s, p.spec.as_ref().unwrap()).unwrap_err();
    assert_eq!(e, "ephemeral container dbg with envFrom referencing secret.secretName=\"x\" is not allowed because service account default does not reference that secret");
}
