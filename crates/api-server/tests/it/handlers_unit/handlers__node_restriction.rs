//! Unit tests moved from `src/handlers/node_restriction.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::resources::csi::CSIDriver;
    pub use rusternetes_common::resources::pod::Volume;
    pub use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
    pub use rusternetes_common::resources::{
        Node, PersistentVolume, PersistentVolumeClaim, Pod, ServiceAccount, TokenRequest,
    };
    pub use rusternetes_common::types::ObjectMeta;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_middleware::AuthContext;
}

/// Ports of the `serviceaccounts/token` rows of `TestAdmit`
/// (`plugin/pkg/admission/noderestriction/admission_test.go:1342-1520`).
mod service_account_token_tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::node_restriction::*;
    use rusternetes_common::authz::{AlwaysAllowAuthorizer, AlwaysDenyAuthorizer};
    use rusternetes_storage::{MemoryStorage, Storage};
    use serde_json::{json, Value};

    fn user(name: &str, groups: &[&str]) -> AuthContext {
        AuthContext {
            user: rusternetes_common::auth::UserInfo {
                username: name.to_string(),
                uid: String::new(),
                groups: groups.iter().map(|g| g.to_string()).collect(),
                extra: Default::default(),
            },
        }
    }

    fn pod(name: &str, uid: &str, node: &str, volumes: Value) -> Value {
        json!({"apiVersion":"v1","kind":"Pod",
            "metadata":{"name":name,"namespace":"ns","uid":uid},
            "spec":{"nodeName":node,"containers":[{"name":"c","image":"i"}],"volumes":volumes}})
    }

    async fn put<T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync>(
        s: &MemoryStorage,
        resource: &str,
        ns: Option<&str>,
        name: &str,
        v: Value,
    ) {
        let obj: T = serde_json::from_value(v).unwrap();
        s.create(&rusternetes_storage::build_key(resource, ns, name), &obj)
            .await
            .unwrap();
    }

    fn tr(pod: Option<(&str, &str)>, audiences: &[&str]) -> TokenRequest {
        let mut spec = json!({"audiences": audiences});
        if let Some((n, u)) = pod {
            let mut r = json!({"apiVersion":"v1","kind":"Pod","name":n});
            if !u.is_empty() {
                r["uid"] = json!(u);
            }
            spec["boundObjectRef"] = r;
        }
        serde_json::from_value(json!({"spec": spec})).unwrap()
    }

    fn driver(name: &str, audience: &str) -> Value {
        json!({"apiVersion":"storage.k8s.io/v1","kind":"CSIDriver","metadata":{"name":name},
            "spec":{"tokenRequests":[{"audience":audience}]}})
    }

    async fn run(s: &MemoryStorage, t: &TokenRequest, gate: bool, allow: bool) -> Result<()> {
        let auth = user("system:node:mynode", &["system:nodes"]);
        if allow {
            admit_service_account_token(s, &AlwaysAllowAuthorizer, &auth, "ns", "mysa", t, gate)
                .await
        } else {
            admit_service_account_token(s, &AlwaysDenyAuthorizer, &auth, "ns", "mysa", t, gate)
                .await
        }
    }

    fn msg(r: Result<()>) -> String {
        match r.unwrap_err() {
            Error::Forbidden(m) | Error::Internal(m) | Error::NotFound(m) => m,
            e => e.to_string(),
        }
    }

    const NOT_AUTHORIZED: &str = "not found in pod spec volume, system:node:mynode is not authorized to request tokens for this audience";

    async fn base() -> MemoryStorage {
        let s = MemoryStorage::new();
        let own = pod("mypod", "u1", "mynode", json!([]));
        let other = pod("other", "u2", "othernode", json!([]));
        put::<Pod>(&s, "pods", Some("ns"), "mypod", own).await;
        put::<Pod>(&s, "pods", Some("ns"), "other", other).await;
        s
    }

    #[tokio::test]
    async fn non_node_is_not_restricted() {
        let s = base().await;
        let auth = user("alice", &[]);
        let t = tr(None, &["foo"]);
        admit_service_account_token(&s, &AlwaysDenyAuthorizer, &auth, "ns", "mysa", &t, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn binding_rules() {
        let s = base().await;
        // forbid create of unbound token
        assert_eq!(
            msg(run(&s, &tr(None, &["foo"]), false, true).await),
            "node requested token not bound to a pod"
        );
        // forbid create of token bound to nonexistent pod
        let r = run(&s, &tr(Some(("nopod", "someuid")), &["foo"]), false, true).await;
        assert!(matches!(r, Err(Error::NotFound(_))));
        // forbid create of token bound to pod without uid
        assert_eq!(
            msg(run(&s, &tr(Some(("mypod", "")), &["foo"]), false, true).await),
            "node requested token with a pod binding without a uid"
        );
        // uid mismatch
        let m = msg(run(&s, &tr(Some(("mypod", "zzz")), &["foo"]), false, true).await);
        assert!(m.starts_with("the UID in the bound object reference (zzz) does not match"));
        // forbid create of token bound to pod scheduled on another node
        assert_eq!(
            msg(run(&s, &tr(Some(("other", "u2")), &["foo"]), false, true).await),
            "node requested token bound to a pod scheduled on a different node"
        );
        // allow create of token bound to pod scheduled this node
        run(&s, &tr(Some(("mypod", "u1")), &["foo"]), false, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn audience_in_projected_volume() {
        let s = base().await;
        let vols = |a: &str| json!([{"name":"p","projected":{"sources":[{"serviceAccountToken":{"path":"t","audience":a}}]}}]);
        let foo = pod("psat", "u3", "mynode", vols("foo"));
        let empty = pod("psat-empty", "u4", "mynode", vols(""));
        put::<Pod>(&s, "pods", Some("ns"), "psat", foo).await;
        put::<Pod>(&s, "pods", Some("ns"), "psat-empty", empty).await;
        run(&s, &tr(Some(("psat", "u3")), &["foo"]), true, false)
            .await
            .unwrap();
        // no requested audience matches the empty projected audience
        run(&s, &tr(Some(("psat-empty", "u4")), &[]), true, false)
            .await
            .unwrap();
        // multiple audiences
        assert_eq!(
            msg(run(&s, &tr(Some(("psat", "u3")), &["foo", "bar"]), true, false).await),
            "node may only request 0 or 1 audiences"
        );
        // audience not in pod spec
        assert_eq!(
            msg(run(&s, &tr(Some(("mypod", "u1")), &["foo"]), true, false).await),
            format!("audience \"foo\" {NOT_AUTHORIZED}")
        );
        // ... unless the authorizer allows request-serviceaccounts-token-audience
        run(&s, &tr(Some(("mypod", "u1")), &["foo"]), true, true)
            .await
            .unwrap();
        // gate off: the audience is not checked at all
        run(&s, &tr(Some(("mypod", "u1")), &["foo"]), false, false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn audience_via_csi_driver() {
        let s = base().await;
        let p = pod(
            "csipod",
            "u5",
            "mynode",
            json!([{"name":"v","csi":{"driver":"d.example"}}]),
        );
        put::<Pod>(&s, "pods", Some("ns"), "csipod", p).await;
        let t = |a: &str| tr(Some(("csipod", "u5")), &[a]);
        // csidriver not found
        let m = msg(run(&s, &t("foo"), true, false).await);
        assert!(m.starts_with("error validating audience \"foo\": "), "{m}");
        put::<CSIDriver>(
            &s,
            "csidrivers",
            None,
            "d.example",
            driver("d.example", "foo"),
        )
        .await;
        run(&s, &t("foo"), true, false).await.unwrap();
        assert_eq!(
            msg(run(&s, &t("bar"), true, false).await),
            format!("audience \"bar\" {NOT_AUTHORIZED}")
        );
    }

    fn pvc(name: &str, pv: &str) -> Value {
        json!({"apiVersion":"v1","kind":"PersistentVolumeClaim",
            "metadata":{"name":name,"namespace":"ns"},"spec":{"volumeName":pv}})
    }

    fn csi_pv(name: &str) -> Value {
        json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":name},
            "spec":{"csi":{"driver":"d.example","volumeHandle":"h"}}})
    }

    #[tokio::test]
    async fn audience_via_pvc_pv_csi() {
        let s = base().await;
        let vol = json!([{"name":"v","persistentVolumeClaim":{"claimName":"c1"}}]);
        put::<Pod>(
            &s,
            "pods",
            Some("ns"),
            "pvcpod",
            pod("pvcpod", "u6", "mynode", vol),
        )
        .await;
        let t = |a: &str| tr(Some(("pvcpod", "u6")), &[a]);
        put::<CSIDriver>(
            &s,
            "csidrivers",
            None,
            "d.example",
            driver("d.example", "foo"),
        )
        .await;
        // pvc not found
        let m = msg(run(&s, &t("foo"), true, false).await);
        assert!(m.starts_with("error validating audience \"foo\": "), "{m}");
        put::<PersistentVolumeClaim>(
            &s,
            "persistentvolumeclaims",
            Some("ns"),
            "c1",
            pvc("c1", "pv1"),
        )
        .await;
        // pv not found
        let m = msg(run(&s, &t("foo"), true, false).await);
        assert!(m.starts_with("error validating audience \"foo\": "), "{m}");
        put::<PersistentVolume>(&s, "persistentvolumes", None, "pv1", csi_pv("pv1")).await;
        run(&s, &t("foo"), true, false).await.unwrap();
        assert_eq!(
            msg(run(&s, &t("bar"), true, false).await),
            format!("audience \"bar\" {NOT_AUTHORIZED}")
        );
    }

    #[tokio::test]
    async fn audience_via_ephemeral_and_inline_migratable() {
        let s = base().await;
        let eph = json!([{"name":"e","ephemeral":{"volumeClaimTemplate":{"spec":{}}}}]);
        put::<Pod>(
            &s,
            "pods",
            Some("ns"),
            "ephpod",
            pod("ephpod", "u7", "mynode", eph),
        )
        .await;
        put::<CSIDriver>(
            &s,
            "csidrivers",
            None,
            "d.example",
            driver("d.example", "foo"),
        )
        .await;
        put::<PersistentVolumeClaim>(
            &s,
            "persistentvolumeclaims",
            Some("ns"),
            "ephpod-e",
            pvc("ephpod-e", "pv2"),
        )
        .await;
        put::<PersistentVolume>(&s, "persistentvolumes", None, "pv2", csi_pv("pv2")).await;
        run(&s, &tr(Some(("ephpod", "u7")), &["foo"]), true, false)
            .await
            .unwrap();

        // In-tree inline volume translated to CSI (admission.go:748).
        let ebs =
            json!([{"name":"v","awsElasticBlockStore":{"volumeID":"vol-123","fsType":"ext4"}}]);
        put::<Pod>(
            &s,
            "pods",
            Some("ns"),
            "ebspod",
            pod("ebspod", "u8", "mynode", ebs),
        )
        .await;
        put::<CSIDriver>(
            &s,
            "csidrivers",
            None,
            "ebs.csi.aws.com",
            driver("ebs.csi.aws.com", "bar"),
        )
        .await;
        run(&s, &tr(Some(("ebspod", "u8")), &["bar"]), true, false)
            .await
            .unwrap();
        assert!(run(&s, &tr(Some(("ebspod", "u8")), &["baz"]), true, false)
            .await
            .is_err());
    }
}
