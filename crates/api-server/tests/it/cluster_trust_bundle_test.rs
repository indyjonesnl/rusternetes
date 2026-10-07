//! `certificates.k8s.io/v1beta1` ClusterTrustBundle (#2539).
//!
//! Upstream: pkg/registry/certificates/clustertrustbundle/{strategy.go,
//! storage/storage.go} (+ their tests), pkg/registry/certificates/rest/
//! storage_certificates.go:91-104 (served only under the `ClusterTrustBundle`
//! gate, off in v1.35: kube_features.go:1199-1202),
//! pkg/apis/certificates/validation/validation.go, and
//! plugin/pkg/admission/certificates/ctbattest/admission{,_test}.go.

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CTB: &str = "/apis/certificates.k8s.io/v1beta1/clustertrustbundles";

fn pem(cn: &str, ca: bool) -> String {
    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    params.distinguished_name = dn;
    params.is_ca = if ca {
        IsCa::Ca(BasicConstraints::Unconstrained)
    } else {
        IsCa::ExplicitNoCa
    };
    let key = KeyPair::generate().unwrap();
    params.self_signed(&key).unwrap().pem()
}

fn ctb(name: &str, signer: &str, bundle: &str) -> Value {
    let mut spec = json!({"trustBundle": bundle});
    if !signer.is_empty() {
        spec["signerName"] = json!(signer);
    }
    json!({"apiVersion": "certificates.k8s.io/v1beta1", "kind": "ClusterTrustBundle",
           "metadata": {"name": name}, "spec": spec})
}

fn causes(out: &Value) -> Vec<String> {
    out["details"]["causes"]
        .as_array()
        .map(|c| {
            c.iter()
                .filter_map(|c| c["field"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// storage_certificates.go:94-103: with the gate off the storage is not
/// registered, so every verb is 404.
#[tokio::test]
#[serial_test::serial]
async fn not_served_while_the_gate_is_off() {
    let _g = with_feature(Feature::ClusterTrustBundle, false);
    let api = TestApiServer::new();
    let (status, _) = api.post(CTB, &ctb("foo", "", &pem("a", true))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = api.get(CTB).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = api.get(&format!("{CTB}/foo")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = api.get("/apis/certificates.k8s.io/v1beta1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, groups) = api.get("/apis").await;
    let certs = groups["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["name"] == "certificates.k8s.io")
        .unwrap();
    assert_eq!(certs["versions"].as_array().unwrap().len(), 1, "{certs}");
}

/// With the gate on, v1beta1 is advertised next to v1 (v1 stays preferred)
/// and lists clustertrustbundles.
#[tokio::test]
#[serial_test::serial]
async fn discovery_advertises_v1beta1_when_the_gate_is_on() {
    let _g = with_feature(Feature::ClusterTrustBundle, true);
    let api = TestApiServer::new();
    let (_, groups) = api.get("/apis").await;
    let certs = groups["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["name"] == "certificates.k8s.io")
        .unwrap()
        .clone();
    let versions: Vec<&str> = certs["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["version"].as_str().unwrap())
        .collect();
    assert_eq!(versions, vec!["v1", "v1beta1"], "{certs}");
    assert_eq!(certs["preferredVersion"]["version"], "v1");

    let (status, list) = api.get("/apis/certificates.k8s.io/v1beta1").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let res = list["resources"].as_array().unwrap();
    assert_eq!(res.len(), 1, "{list}");
    assert_eq!(res[0]["name"], "clustertrustbundles");
    assert_eq!(res[0]["kind"], "ClusterTrustBundle");
    assert_eq!(res[0]["namespaced"], false);

    // The trailing-slash route is the one backed by the built-in group table.
    let (status, group) = api.get("/apis/certificates.k8s.io/").await;
    assert_eq!(status, StatusCode::OK, "{group}");
    assert_eq!(group["versions"].as_array().unwrap().len(), 2, "{group}");
}

/// storage_test.go TestCreate/TestUpdate/TestDelete/TestGet/TestList plus the
/// strategy: `AllowCreateOnUpdate` false (strategy_test.go:32-36).
#[tokio::test]
#[serial_test::serial]
async fn crud() {
    let _g = with_feature(Feature::ClusterTrustBundle, true);
    let api = TestApiServer::new();
    let c1 = pem("root1", true);
    let c2 = pem("root2", true);

    let (status, out) = api.post(CTB, &ctb("ctb1", "", &c1)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["kind"], "ClusterTrustBundle");
    assert_eq!(out["apiVersion"], "certificates.k8s.io/v1beta1");

    let (status, got) = api.get(&format!("{CTB}/ctb1")).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["spec"]["trustBundle"], c1);

    // Invalid create: an empty bundle (storage_test.go TestCreate).
    let (status, out) = api.post(CTB, &ctb("ctb2", "", "")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(causes(&out), vec!["spec.trustBundle"], "{out}");

    // Valid update adds a certificate; an emptied bundle is invalid
    // (storage_test.go TestUpdate).
    let mut upd = got.clone();
    upd["spec"]["trustBundle"] = json!(format!("{c1}\n{c2}"));
    let (status, out) = api.put(&format!("{CTB}/ctb1"), &upd).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let mut bad = out.clone();
    bad["spec"]["trustBundle"] = json!("");
    let (status, out) = api.put(&format!("{CTB}/ctb1"), &bad).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    // AllowCreateOnUpdate is false.
    let (status, out) = api
        .put(&format!("{CTB}/absent"), &ctb("absent", "", &c1))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");

    let (status, list) = api.get(CTB).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list["kind"], "ClusterTrustBundleList");
    assert_eq!(list["items"].as_array().unwrap().len(), 1, "{list}");

    let (status, out) = api.delete(&format!("{CTB}/ctb1")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, _) = api.get(&format!("{CTB}/ctb1")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// validation.go ValidateClusterTrustBundle via the strategy
/// (strategy.go:46-49): name/signer coupling and the trust anchors, and
/// ValidateClusterTrustBundleUpdate's immutable signerName.
#[tokio::test]
#[serial_test::serial]
async fn validation_over_http() {
    let _g = with_feature(Feature::ClusterTrustBundle, true);
    let api = TestApiServer::new();
    let c1 = pem("root1", true);

    let (status, out) = api.post(CTB, &ctb("foo", "k8s.io/foo", &c1)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(causes(&out), vec!["metadata.name"], "{out}");

    let (status, out) = api.post(CTB, &ctb("k8s.io:bar:foo", "", &c1)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let (status, out) = api.post(CTB, &ctb("foo", "", &pem("not-ca", false))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(causes(&out), vec!["spec.trustBundle"], "{out}");

    let (status, out) = api
        .post(CTB, &ctb("k8s.io:foo:bar", "k8s.io/foo", &c1))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    // generateName is filled by SimpleNameGenerator (strategy.go:38).
    let mut gen = ctb("x", "k8s.io/foo", &c1);
    gen["metadata"] = json!({"generateName": "k8s.io:foo:g-"});
    let (status, out) = api.post(CTB, &gen).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(out["metadata"]["name"]
        .as_str()
        .unwrap()
        .starts_with("k8s.io:foo:g-"));

    // spec.signerName is immutable.
    let (_, mut got) = api.get(&format!("{CTB}/k8s.io:foo:bar")).await;
    got["spec"]["signerName"] = json!("k8s.io/baz");
    let (status, out) = api.put(&format!("{CTB}/k8s.io:foo:bar"), &got).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        causes(&out).contains(&"spec.signerName".to_string()),
        "{out}"
    );
}

/// storage.go:68-79 getAttrs: `spec.signerName` and the metadata fields are
/// selectable, and labels (storage_test.go TestWatch matching/not matching).
#[tokio::test]
#[serial_test::serial]
async fn list_filters_on_signer_name_field_and_labels() {
    let _g = with_feature(Feature::ClusterTrustBundle, true);
    let api = TestApiServer::new();
    let c1 = pem("root1", true);
    let mut labelled = ctb("k8s.io:foo:a", "k8s.io/foo", &c1);
    labelled["metadata"]["labels"] = json!({"foo": "bar"});
    for body in [
        labelled,
        ctb("k8s.io:foo:b", "k8s.io/foo", &c1),
        ctb("k8s.io:other:c", "k8s.io/other", &c1),
        ctb("plain", "", &c1),
    ] {
        let (status, out) = api.post(CTB, &body).await;
        assert_eq!(status, StatusCode::CREATED, "{out}");
    }
    let names = |list: &Value| -> Vec<String> {
        let mut n: Vec<String> = list["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
            .collect();
        n.sort();
        n
    };

    let (status, list) = api
        .get(&format!(
            "{CTB}?fieldSelector=spec.signerName%3Dk8s.io%2Ffoo"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(names(&list), vec!["k8s.io:foo:a", "k8s.io:foo:b"]);

    let (_, list) = api
        .get(&format!("{CTB}?fieldSelector=metadata.name%3Dplain"))
        .await;
    assert_eq!(names(&list), vec!["plain"]);

    let (_, list) = api.get(&format!("{CTB}?labelSelector=foo%3Dbar")).await;
    assert_eq!(names(&list), vec!["k8s.io:foo:a"]);
    let (_, list) = api.get(&format!("{CTB}?labelSelector=foo%3Dnope")).await;
    assert!(names(&list).is_empty());
}

// ---- ClusterTrustBundleAttest (ctbattest/admission_test.go) ----

mod attest {
    use super::*;
    use rusternetes_common::{
        resources::{
            ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, ServiceAccount, Subject,
        },
        types::{ObjectMeta, TypeMeta},
    };
    use rusternetes_storage::{build_key, Storage};

    const SECRET: &[u8] = b"ctb-attest-secret";

    fn token(uid: &str) -> String {
        let now = chrono::Utc::now();
        let claims = rusternetes_common::auth::ServiceAccountClaims {
            sub: "system:serviceaccount:kube-system:ctb-user".to_string(),
            namespace: "kube-system".to_string(),
            uid: uid.to_string(),
            iat: now.timestamp(),
            exp: (now + chrono::Duration::hours(1)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["https://kubernetes.default.svc".to_string()],
            kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                namespace: "kube-system".to_string(),
                svcacct: rusternetes_common::auth::KubeRef {
                    name: "ctb-user".to_string(),
                    uid: uid.to_string(),
                },
                pod: None,
                node: None,
                secret: None,
            }),
            pod_name: None,
            pod_uid: None,
            node_name: None,
            node_uid: None,
        };
        rusternetes_common::auth::TokenManager::new(SECRET)
            .generate_token(claims)
            .unwrap()
    }

    fn rule(verbs: &[&str], resources: &[&str], names: Option<&[&str]>) -> PolicyRule {
        PolicyRule {
            verbs: verbs.iter().map(|s| s.to_string()).collect(),
            api_groups: Some(vec!["certificates.k8s.io".to_string()]),
            resources: Some(resources.iter().map(|s| s.to_string()).collect()),
            resource_names: names.map(|n| n.iter().map(|s| s.to_string()).collect()),
            non_resource_urls: None,
        }
    }

    /// A server with a `ctb-user` who may write ClusterTrustBundles and holds
    /// `extra` rules (the `attest` grant under test).
    async fn setup(extra: Vec<PolicyRule>) -> (TestApiServer, String) {
        let api = TestApiServer::builder()
            .secret(SECRET)
            .rbac()
            .skip_auth(false)
            .build();
        let uid = "uid-ctb-user".to_string();
        let sa = ServiceAccount {
            type_meta: TypeMeta {
                kind: "ServiceAccount".into(),
                api_version: "v1".into(),
            },
            metadata: ObjectMeta {
                name: "ctb-user".into(),
                namespace: Some("kube-system".into()),
                uid: uid.clone(),
                ..Default::default()
            },
            secrets: None,
            image_pull_secrets: None,
            automount_service_account_token: None,
        };
        api.storage
            .create(
                &build_key("serviceaccounts", Some("kube-system"), "ctb-user"),
                &sa,
            )
            .await
            .unwrap();
        let mut rules = vec![rule(
            &["get", "list", "create", "update", "patch"],
            &["clustertrustbundles"],
            None,
        )];
        rules.extend(extra);
        let role = ClusterRole {
            type_meta: TypeMeta {
                kind: "ClusterRole".into(),
                api_version: "rbac.authorization.k8s.io/v1".into(),
            },
            metadata: ObjectMeta {
                name: "ctb-user".into(),
                ..Default::default()
            },
            rules,
            aggregation_rule: None,
        };
        api.storage
            .create(&build_key("clusterroles", None, "ctb-user"), &role)
            .await
            .unwrap();
        let binding = ClusterRoleBinding {
            type_meta: TypeMeta {
                kind: "ClusterRoleBinding".into(),
                api_version: "rbac.authorization.k8s.io/v1".into(),
            },
            metadata: ObjectMeta {
                name: "ctb-user".into(),
                ..Default::default()
            },
            subjects: vec![Subject {
                kind: "ServiceAccount".into(),
                name: "ctb-user".into(),
                api_group: Some(String::new()),
                namespace: Some("kube-system".into()),
            }],
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".into(),
                kind: "ClusterRole".into(),
                name: "ctb-user".into(),
            },
        };
        api.storage
            .create(
                &build_key("clusterrolebindings", None, "ctb-user"),
                &binding,
            )
            .await
            .unwrap();
        let token = token(&uid);
        (api, token)
    }

    async fn send(
        api: &TestApiServer,
        method: &str,
        uri: &str,
        token: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let auth = format!("Bearer {token}");
        let (status, _, _, out) = api
            .send_with_headers(
                method,
                uri,
                &[
                    ("content-type", "application/json"),
                    ("authorization", &auth),
                ],
                body.map(|b| serde_json::to_vec(b).unwrap()),
            )
            .await;
        (status.as_u16(), out)
    }

    fn attest(names: &[&str]) -> PolicyRule {
        rule(&["attest"], &["signers"], Some(names))
    }

    /// admission_test.go "should allow create if no signer name is specified".
    #[tokio::test]
    #[serial_test::serial]
    async fn no_signer_name_needs_no_attest() {
        let _g = with_feature(Feature::ClusterTrustBundle, true);
        let (api, token) = setup(vec![]).await;
        let body = ctb("plain", "", &pem("a", true));
        let (status, out) = send(&api, "POST", CTB, &token, Some(&body)).await;
        assert_eq!(status, 201, "{out}");
    }

    /// "should deny create if user does not have permission for this signerName".
    #[tokio::test]
    #[serial_test::serial]
    async fn create_with_signer_without_attest_is_forbidden() {
        let _g = with_feature(Feature::ClusterTrustBundle, true);
        let (api, token) = setup(vec![]).await;
        let body = ctb("k8s.io:foo:a", "k8s.io/foo", &pem("a", true));
        let (status, out) = send(&api, "POST", CTB, &token, Some(&body)).await;
        assert_eq!(status, 403, "{out}");
        assert_eq!(
            out["message"],
            "clustertrustbundles.certificates.k8s.io \"k8s.io:foo:a\" is forbidden: user not permitted to attest for signerName \"k8s.io/foo\"",
            "{out}"
        );
    }

    /// "should allow create if user is authorized for specific signerName" and
    /// "... with wildcard"; another signer's grant does not help.
    #[tokio::test]
    #[serial_test::serial]
    async fn create_with_attest_on_the_signer_or_wildcard() {
        let _g = with_feature(Feature::ClusterTrustBundle, true);
        for (grant, want) in [
            ("k8s.io/foo", 201),
            ("k8s.io/*", 201),
            ("k8s.io/other", 403),
        ] {
            let (api, token) = setup(vec![attest(&[grant])]).await;
            let body = ctb("k8s.io:foo:a", "k8s.io/foo", &pem("a", true));
            let (status, out) = send(&api, "POST", CTB, &token, Some(&body)).await;
            assert_eq!(status, want, "{grant}: {out}");
        }
    }

    /// "should deny update ...", "should always allow no-op update" and
    /// "should always allow finalizer update".
    #[tokio::test]
    #[serial_test::serial]
    async fn update_needs_attest_unless_only_gc_fields_change() {
        let _g = with_feature(Feature::ClusterTrustBundle, true);
        let (api, token) = setup(vec![]).await;
        let c1 = pem("a", true);
        // Stored out of band, as a migration or a prior grant would leave it.
        let obj = rusternetes_common::resources::ClusterTrustBundle::new(
            "k8s.io:foo:a",
            "k8s.io/foo",
            &c1,
        );
        api.storage
            .create(
                &build_key("clustertrustbundles", None, "k8s.io:foo:a"),
                &obj,
            )
            .await
            .unwrap();
        let uri = format!("{CTB}/k8s.io:foo:a");
        let (_, got) = send(&api, "GET", &uri, &token, None).await;

        let (status, out) = send(&api, "PUT", &uri, &token, Some(&got)).await;
        assert_eq!(status, 200, "no-op update: {out}");

        let mut fin = out.clone();
        fin["metadata"]["finalizers"] = json!(["example.com/f"]);
        let (status, out) = send(&api, "PUT", &uri, &token, Some(&fin)).await;
        assert_eq!(status, 200, "finalizer update: {out}");

        let mut changed = out.clone();
        changed["spec"]["trustBundle"] = json!(format!("{c1}\n{}", pem("b", true)));
        let (status, out) = send(&api, "PUT", &uri, &token, Some(&changed)).await;
        assert_eq!(status, 403, "{out}");
        assert!(
            out["message"]
                .as_str()
                .unwrap()
                .ends_with("user not permitted to attest for signerName \"k8s.io/foo\""),
            "{out}"
        );
    }
}
