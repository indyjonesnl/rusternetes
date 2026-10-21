//! `/statusz` and `/flagz` on the kube-apiserver router (#3436).
//!
//! Upstream installs them in `GenericAPIServer.PrepareRun`
//! (`genericapiserver.go:466`) behind `ComponentStatusz`, and in
//! `completedConfig.New` (`pkg/controlplane/apiserver/server.go:157`) behind
//! `ComponentFlagz`. Both are non-resource URLs that go through authorization.

use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_common::flagz::{Flag, NamedFlagSetsReader};
use rusternetes_test_support::harness::TestApiServer;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn not_served_when_gates_are_off() {
    let _a = with_feature(Feature::ComponentStatusz, false);
    let _b = with_feature(Feature::ComponentFlagz, false);
    let api = TestApiServer::new();
    assert_eq!(api.get("/statusz").await.0.as_u16(), 404);
    assert_eq!(api.get("/flagz").await.0.as_u16(), 404);
}

#[tokio::test]
#[serial]
async fn statusz_served_when_gate_is_on() {
    let _a = with_feature(Feature::ComponentStatusz, true);
    let api = TestApiServer::new();
    let (status, body, _) = api.send_raw("GET", "/statusz?v=1", None, None).await;
    assert_eq!(status.as_u16(), 200);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("apiserver statusz"), "{text}");
}

#[tokio::test]
#[serial]
async fn flagz_served_with_a_reader_when_gate_is_on() {
    let _a = with_feature(Feature::ComponentFlagz, true);
    rusternetes_api_server::zpages::set_flagz_reader(NamedFlagSetsReader {
        flags: vec![Flag {
            name: "v".into(),
            value: "2".into(),
            classified: false,
        }],
    });
    let api = TestApiServer::new();
    let (status, body, _) = api.send_raw("GET", "/flagz", None, None).await;
    assert_eq!(status.as_u16(), 200);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("kube-apiserver flagz"), "{text}");
}

/// The routes sit behind the authorization filter: a denying authorizer
/// answers 403, the allow-all one 200.
#[tokio::test]
#[serial]
async fn statusz_is_authorized_not_exempt() {
    let _a = with_feature(Feature::ComponentStatusz, true);
    let api = TestApiServer::builder()
        .authorizer(std::sync::Arc::new(
            rusternetes_common::authz::AlwaysDenyAuthorizer,
        ))
        .build();
    assert_eq!(api.get("/statusz").await.0.as_u16(), 403);
}
