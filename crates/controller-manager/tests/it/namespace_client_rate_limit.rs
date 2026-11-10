//! The namespace controller's api client is scaled like upstream's
//! (`newNamespaceController`, `cmd/kube-controller-manager/app/core.go:646-647`:
//! `QPS *= 20`, `Burst *= 100`); every other controller keeps the skeleton's.

use rusternetes_client::http::ApiClient;
use rusternetes_controller_manager::controller_client;

fn skeleton() -> ApiClient {
    ApiClient::new("http://127.0.0.1:1", true, None)
        .unwrap()
        .with_rate_limit(20.0, 30.0)
}

#[test]
fn namespace_client_is_scaled_20x_qps_100x_burst() {
    let c = controller_client(&skeleton(), "Namespace");
    assert_eq!(c.qps(), 400.0);
    assert_eq!(c.burst(), 3000.0);
}

#[test]
fn other_controllers_keep_the_skeleton_rate() {
    let c = controller_client(&skeleton(), "Deployment");
    assert_eq!(c.qps(), 20.0);
    assert_eq!(c.burst(), 30.0);
}

#[test]
fn namespace_client_has_its_own_limiter() {
    let s = skeleton();
    let a = controller_client(&s, "Namespace");
    assert!(!a.shares_limiter_with(&s));
    assert!(!a.shares_limiter_with(&controller_client(&s, "Deployment")));
}

#[test]
fn an_unthrottled_skeleton_stays_unthrottled() {
    let s = ApiClient::new("http://127.0.0.1:1", true, None).unwrap();
    assert!(controller_client(&s, "Namespace").qps() <= 0.0);
}
