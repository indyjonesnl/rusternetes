//! Integration tests for ServiceAccountController
//!
//! Mirrors behaviour exercised by upstream `kubernetes/test/e2e/auth/serviceaccount.go`
//! against rusternetes' in-process controller. Extended coverage includes
//! automount preservation, imagePullSecrets retention, multi-namespace fan-out,
//! token-secret annotations, and RED-state gaps for bound-token projection and
//! pod-spec propagation of SA-level imagePullSecrets.

use rusternetes_common::resources::{
    namespace::{NamespaceSpec, NamespaceStatus},
    LocalObjectReference, Namespace, Secret, ServiceAccount,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::serviceaccount::ServiceAccountController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

/// Build an Active Namespace ready for use in tests.
fn active_namespace(name: &str) -> Namespace {
    Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    }
}

/// Build a ServiceAccount in `namespace` named `name`.
fn service_account(namespace: &str, name: &str) -> ServiceAccount {
    ServiceAccount {
        type_meta: TypeMeta {
            kind: "ServiceAccount".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: Some(true),
    }
}

#[tokio::test]
async fn test_serviceaccount_controller_creation() {
    let storage = Arc::new(MemoryStorage::new());
    let _controller = ServiceAccountController::new(storage);
}

#[tokio::test]
async fn test_serviceaccount_creates_default_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a test namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-sa-namespace".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-sa-namespace");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Reconcile should create default ServiceAccount
    controller.reconcile_all().await.unwrap();

    // Verify default ServiceAccount was created
    let sa_key = build_key("serviceaccounts", Some("test-sa-namespace"), "default");
    let sa: ServiceAccount = storage.get(&sa_key).await.unwrap();
    assert_eq!(sa.metadata.name, "default");
    assert_eq!(sa.metadata.namespace.as_ref().unwrap(), "test-sa-namespace");

    // Upstream (LegacyServiceAccountTokenNoAutoGeneration, GA 1.24) never mints
    // a `<sa>-token` Secret for a ServiceAccount.
    let secret_key = build_key("secrets", Some("test-sa-namespace"), "default-token");
    assert!(
        storage.get::<Secret>(&secret_key).await.is_err(),
        "the controller must not auto-mint a legacy token Secret"
    );

    // Clean up
    storage.delete(&sa_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

#[tokio::test]
async fn test_serviceaccount_does_not_recreate_existing() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-existing-sa".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-existing-sa");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Create default ServiceAccount manually
    let service_account = ServiceAccount {
        type_meta: TypeMeta {
            kind: "ServiceAccount".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "default".to_string(),
            namespace: Some("test-existing-sa".to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: Some(true),
    };

    let sa_key = build_key("serviceaccounts", Some("test-existing-sa"), "default");
    storage.create(&sa_key, &service_account).await.unwrap();

    // Reconcile should not recreate
    controller.reconcile_all().await.unwrap();

    // ServiceAccount should still exist with same UID
    let retrieved: ServiceAccount = storage.get(&sa_key).await.unwrap();
    assert_eq!(retrieved.metadata.uid, service_account.metadata.uid);

    // Clean up
    storage.delete(&sa_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

#[tokio::test]
async fn test_serviceaccount_skips_terminating_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a namespace that's being deleted
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-terminating".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: Some(chrono::Utc::now()), // Being deleted
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Terminating),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-terminating");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Reconcile should skip terminating namespace
    controller.reconcile_all().await.unwrap();

    // ServiceAccount should NOT be created
    let sa_key = build_key("serviceaccounts", Some("test-terminating"), "default");
    let result = storage.get::<ServiceAccount>(&sa_key).await;
    assert!(result.is_err()); // Should not exist

    // Clean up
    storage.delete(&ns_key).await.unwrap();
}

// ---------------------------------------------------------------------------
// Phase 6.2 extended coverage
// ---------------------------------------------------------------------------

/// Reconciling a ServiceAccount whose owner has disabled automount must not
/// silently flip the flag back to true. Mirrors the upstream e2e expectation
/// that `automountServiceAccountToken: false` is honoured for the lifetime of
/// the SA (see `kubernetes/test/e2e/auth/serviceaccount.go` "should mount an
/// API token into pods").
#[tokio::test]
async fn test_serviceaccount_automount_disable_is_preserved() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let ns_name = "test-sa-automount-disable";
    let namespace = active_namespace(ns_name);
    let ns_key = build_key("namespaces", None, ns_name);
    storage.create(&ns_key, &namespace).await.unwrap();

    // User-created SA explicitly opts out of token automounting.
    let mut sa = service_account(ns_name, "no-automount");
    sa.automount_service_account_token = Some(false);
    let sa_key = build_key("serviceaccounts", Some(ns_name), "no-automount");
    storage.create(&sa_key, &sa).await.unwrap();

    // `reconcile_all` only ensures the *default* SA per namespace, so to
    // exercise the preservation contract we must drive the per-SA reconcile
    // entry point directly.
    controller
        .reconcile_serviceaccount(ns_name, "no-automount")
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    let after: ServiceAccount = storage.get(&sa_key).await.unwrap();
    assert_eq!(
        after.automount_service_account_token,
        Some(false),
        "controller must not overwrite an explicit automount=false on existing SA"
    );

    storage.delete(&sa_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

/// SA-level `imagePullSecrets` set by the user must round-trip through reconcile
/// unchanged. Upstream Kubernetes additionally propagates these into pods via
/// the ServiceAccount admission plugin (`plugin/pkg/admission/serviceaccount`),
/// which rusternetes does not implement in the controller-manager. The
/// propagation half is marked `#[ignore]` as a RED-state spec.
#[tokio::test]
async fn test_serviceaccount_image_pull_secrets_persist_through_reconcile() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let ns_name = "test-sa-pullsecrets";
    storage
        .create(
            &build_key("namespaces", None, ns_name),
            &active_namespace(ns_name),
        )
        .await
        .unwrap();

    let mut sa = service_account(ns_name, "with-pull-secrets");
    sa.image_pull_secrets = Some(vec![
        LocalObjectReference {
            name: "registry-creds".to_string(),
        },
        LocalObjectReference {
            name: "backup-registry-creds".to_string(),
        },
    ]);
    let sa_key = build_key("serviceaccounts", Some(ns_name), "with-pull-secrets");
    storage.create(&sa_key, &sa).await.unwrap();

    // Drive the per-SA reconcile path explicitly — `reconcile_all` only walks
    // namespaces to seed default SAs and never visits user-created SAs.
    controller
        .reconcile_serviceaccount(ns_name, "with-pull-secrets")
        .await
        .unwrap();

    let after: ServiceAccount = storage.get(&sa_key).await.unwrap();
    let pull = after
        .image_pull_secrets
        .as_ref()
        .expect("imagePullSecrets must survive reconcile");
    assert_eq!(pull.len(), 2);
    assert_eq!(pull[0].name, "registry-creds");
    assert_eq!(pull[1].name, "backup-registry-creds");

    storage
        .delete(&build_key("namespaces", None, ns_name))
        .await
        .unwrap();
    storage.delete(&sa_key).await.unwrap();
}

// NOTE: SA `imagePullSecrets` propagation onto pods is an **admission** concern,
// not a controller one — upstream does it in the ServiceAccount admission plugin
// at pod CREATE (plugin/pkg/admission/serviceaccount/admission.go:167), not in a
// reconcile loop. In rusternetes it runs in
// `admission::inject_service_account_token` via
// `serviceaccount::propagate_image_pull_secrets`, and is covered end-to-end by
// `api-server/tests/integration_serviceaccount_token.rs::
// test_service_account_image_pull_secrets_propagate_on_pod_create`. The previous
// `#[ignore]`d controller test here asserted the wrong layer (controller doing
// the propagation) against a stale "absent in admission.rs" claim, so it has
// been removed.

/// `reconcile_all` must fan out default SA creation across every
/// active namespace it sees, and must do so idempotently. Mirrors the upstream
/// invariant exercised by the e2e suite when a fresh cluster spins up several
/// namespaces back-to-back.
#[tokio::test]
async fn test_serviceaccount_reconcile_fans_out_to_all_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let names = ["fanout-alpha", "fanout-beta", "fanout-gamma"];
    for name in &names {
        storage
            .create(
                &build_key("namespaces", None, name),
                &active_namespace(name),
            )
            .await
            .unwrap();
    }

    // Two passes — second pass must be a no-op (idempotency).
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();

    for name in &names {
        let sa: ServiceAccount = storage
            .get(&build_key("serviceaccounts", Some(name), "default"))
            .await
            .unwrap_or_else(|e| panic!("default SA missing in {name}: {e}"));
        assert_eq!(sa.metadata.name, "default");
        assert_eq!(sa.metadata.namespace.as_deref(), Some(*name));

        assert!(
            storage
                .get::<Secret>(&build_key("secrets", Some(name), "default-token"))
                .await
                .is_err(),
            "no legacy token Secret may be minted in {name}"
        );
    }

    for name in &names {
        let _ = storage
            .delete(&build_key("serviceaccounts", Some(name), "default"))
            .await;
        let _ = storage.delete(&build_key("namespaces", None, name)).await;
    }
}

// NOTE: Bound, audience-scoped tokens with caller-supplied `expirationSeconds`
// are an **api-server** concern served on-demand by the `TokenRequest`
// subresource (`handlers::authentication::create_token_request`), which sets the
// token's `exp` from `spec.expirationSeconds` and `aud` from `spec.audiences`.
// Upstream has no controller that writes a "bound token" Secret — bound tokens
// are minted per request and projected into pods via projected volumes. So
// there is no controller behaviour to test here; the TokenRequest contract is
// covered by `api-server/tests/tokenrequest_expiration_test.rs` and
// `api-server/tests/conformance_auth_rbac_serviceaccount.rs` (audiences). The
// previous `#[ignore]`d test asserted a non-upstream controller-written Secret
// and has been removed.

// ---------------------------------------------------------------------------
// Populate-only tokens controller
//
// Ported from upstream `pkg/controller/serviceaccount/tokens_controller_test.go`
// (release-1.35) `TestTokenCreation`: the controller never mints a Secret, it
// only populates token / ca.crt / namespace on a user-created
// `kubernetes.io/service-account-token` Secret, and deletes such a Secret when
// the ServiceAccount it names is gone or has a different UID.
// ---------------------------------------------------------------------------

const SA_TOKEN_TYPE: &str = "kubernetes.io/service-account-token";
const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----\n";

fn token_secret(
    namespace: &str,
    name: &str,
    sa_name: &str,
    sa_uid: Option<&str>,
    secret_type: &str,
    data: &[(&str, &str)],
) -> Secret {
    let mut annotations = std::collections::HashMap::new();
    annotations.insert(
        "kubernetes.io/service-account.name".to_string(),
        sa_name.to_string(),
    );
    if let Some(uid) = sa_uid {
        annotations.insert(
            "kubernetes.io/service-account.uid".to_string(),
            uid.to_string(),
        );
    }
    let mut meta = service_account(namespace, name).metadata;
    meta.annotations = Some(annotations);
    Secret {
        type_meta: TypeMeta {
            kind: "Secret".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: meta,
        secret_type: Some(secret_type.to_string()),
        data: Some(
            data.iter()
                .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                .collect(),
        ),
        string_data: None,
        immutable: None,
    }
}

/// Seed an SA with a real UID (the controller's own default SA has none).
async fn seed_sa(storage: &Arc<MemoryStorage>, ns: &str, name: &str) -> ServiceAccount {
    let sa = service_account(ns, name);
    storage
        .create(&build_key("serviceaccounts", Some(ns), name), &sa)
        .await
        .unwrap();
    sa
}

async fn seed_secret(storage: &Arc<MemoryStorage>, secret: &Secret) {
    storage
        .create(
            &build_key(
                "secrets",
                secret.metadata.namespace.as_deref(),
                &secret.metadata.name,
            ),
            secret,
        )
        .await
        .unwrap();
}

async fn get_secret(storage: &Arc<MemoryStorage>, ns: &str, name: &str) -> Option<Secret> {
    storage
        .get::<Secret>(&build_key("secrets", Some(ns), name))
        .await
        .ok()
}

/// "added token secret without token data" / "without ca data" /
/// "without namespace data".
#[tokio::test]
async fn test_tokens_controller_populates_user_created_token_secret() {
    let storage = Arc::new(MemoryStorage::new());
    let controller =
        ServiceAccountController::new(storage.clone()).with_ca_cert(Some(CA_PEM.to_string()));
    let sa = seed_sa(&storage, "ns1", "sa1").await;
    seed_secret(
        &storage,
        &token_secret(
            "ns1",
            "tok",
            "sa1",
            Some(&sa.metadata.uid),
            SA_TOKEN_TYPE,
            &[],
        ),
    )
    .await;

    controller.sync_token_secret("ns1", "tok").await.unwrap();

    let got = get_secret(&storage, "ns1", "tok").await.expect("kept");
    let data = got.data.expect("data populated");
    assert!(!data.get("token").map(|v| v.is_empty()).unwrap_or(true));
    assert_eq!(
        data.get("namespace").map(|v| v.as_slice()),
        Some(&b"ns1"[..])
    );
    assert_eq!(data.get("ca.crt"), Some(&CA_PEM.as_bytes().to_vec()));
    let ann = got.metadata.annotations.unwrap();
    assert_eq!(
        ann.get("kubernetes.io/service-account.name").unwrap(),
        "sa1"
    );
    assert_eq!(
        ann.get("kubernetes.io/service-account.uid").unwrap(),
        &sa.metadata.uid
    );
}

/// "added token secret with mismatched ca data" + "with custom namespace data":
/// a wrong CA is replaced, a present token and namespace are left alone.
#[tokio::test]
async fn test_tokens_controller_fixes_mismatched_ca_keeps_existing_data() {
    let storage = Arc::new(MemoryStorage::new());
    let controller =
        ServiceAccountController::new(storage.clone()).with_ca_cert(Some(CA_PEM.to_string()));
    let sa = seed_sa(&storage, "ns1", "sa1").await;
    seed_secret(
        &storage,
        &token_secret(
            "ns1",
            "tok",
            "sa1",
            Some(&sa.metadata.uid),
            SA_TOKEN_TYPE,
            &[
                ("token", "existing"),
                ("ca.crt", "stale"),
                ("namespace", "custom"),
            ],
        ),
    )
    .await;

    controller.sync_token_secret("ns1", "tok").await.unwrap();

    let data = get_secret(&storage, "ns1", "tok")
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(data.get("token").unwrap(), b"existing");
    assert_eq!(data.get("namespace").unwrap(), b"custom");
    assert_eq!(data.get("ca.crt").unwrap(), CA_PEM.as_bytes());
}

/// "added secret without serviceaccount": the token is deleted.
#[tokio::test]
async fn test_tokens_controller_deletes_token_when_sa_missing() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());
    seed_secret(
        &storage,
        &token_secret("ns1", "tok", "gone", None, SA_TOKEN_TYPE, &[]),
    )
    .await;

    controller.sync_token_secret("ns1", "tok").await.unwrap();

    assert!(get_secret(&storage, "ns1", "tok").await.is_none());
}

/// `getServiceAccount(.., uid, ..)`: an SA with a different UID is "missing".
#[tokio::test]
async fn test_tokens_controller_deletes_token_on_sa_uid_mismatch() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());
    seed_sa(&storage, "ns1", "sa1").await;
    seed_secret(
        &storage,
        &token_secret(
            "ns1",
            "tok",
            "sa1",
            Some("some-other-uid"),
            SA_TOKEN_TYPE,
            &[],
        ),
    )
    .await;

    controller.sync_token_secret("ns1", "tok").await.unwrap();

    assert!(get_secret(&storage, "ns1", "tok").await.is_none());
}

/// Only `kubernetes.io/service-account-token` Secrets are touched.
#[tokio::test]
async fn test_tokens_controller_ignores_other_secret_types() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());
    seed_secret(
        &storage,
        &token_secret("ns1", "opaque", "gone", None, "Opaque", &[]),
    )
    .await;

    controller.sync_token_secret("ns1", "opaque").await.unwrap();

    let got = get_secret(&storage, "ns1", "opaque")
        .await
        .expect("untouched");
    assert!(got.data.unwrap().is_empty());
}

/// "deleted serviceaccount with token secrets": the SA's tokens go with it,
/// other SAs' tokens stay.
#[tokio::test]
async fn test_tokens_controller_deletes_tokens_of_deleted_sa() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());
    let other = seed_sa(&storage, "ns1", "other").await;
    seed_secret(
        &storage,
        &token_secret(
            "ns1",
            "t-gone",
            "gone",
            None,
            SA_TOKEN_TYPE,
            &[("token", "x")],
        ),
    )
    .await;
    seed_secret(
        &storage,
        &token_secret(
            "ns1",
            "t-other",
            "other",
            Some(&other.metadata.uid),
            SA_TOKEN_TYPE,
            &[("token", "x")],
        ),
    )
    .await;

    controller
        .reconcile_serviceaccount("ns1", "gone")
        .await
        .unwrap();

    assert!(get_secret(&storage, "ns1", "t-gone").await.is_none());
    assert!(get_secret(&storage, "ns1", "t-other").await.is_some());
}
