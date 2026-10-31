//! Ports of `pkg/kubelet/token/token_manager_test.go`
//! (`TestTokenCachingAndExpiration`, `TestRequiresRefresh`,
//! `TestDeleteServiceAccountToken`, `TestCleanup`, `TestKeyFunc`,
//! `TestServiceAccountRecreationCacheInvalidation`).

use chrono::{DateTime, Duration, TimeZone, Utc};
use rusternetes_kubelet::token_manager::{
    key_func, BoundObjectReference, Clock, GetTokenFn, Manager, TokenRequest, TokenRequestSpec,
    TokenRequestStatus,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct FakeClock(Mutex<DateTime<Utc>>);

impl FakeClock {
    fn new(t: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(t)))
    }
    fn set_time(&self, t: DateTime<Utc>) {
        *self.0.lock().unwrap() = t;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

fn thirty_days() -> DateTime<Utc> {
    Utc.timestamp_opt(30 * 24 * 3600, 0).unwrap()
}

/// `fakeTokenGetter`: counts calls, returns `tr` or `err`.
struct FakeTokenGetter {
    count: AtomicUsize,
    tr: Option<TokenRequest>,
    err: Option<&'static str>,
}

impl FakeTokenGetter {
    fn get_token(self: &Arc<Self>) -> GetTokenFn {
        let me = self.clone();
        Arc::new(move |_name, _ns, _tr| {
            me.count.fetch_add(1, Ordering::SeqCst);
            let out = match me.err {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok(me.tr.clone().unwrap()),
            };
            Box::pin(async move { out })
        })
    }
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

fn spec(uid: &str, name: &str) -> TokenRequestSpec {
    TokenRequestSpec {
        audiences: vec!["foo1".into(), "foo2".into()],
        expiration_seconds: Some(2000),
        bound_object_ref: Some(BoundObjectReference {
            kind: "pod".into(),
            name: name.into(),
            uid: uid.into(),
            ..Default::default()
        }),
    }
}

fn token_request() -> TokenRequest {
    TokenRequest {
        spec: spec("foo-uid", "foo-pod"),
        ..Default::default()
    }
}

fn noop_get_token() -> GetTokenFn {
    Arc::new(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("unused")) }))
}

fn mgr_at(clock: Arc<FakeClock>) -> Arc<Manager> {
    Manager::new_with_clock(noop_get_token(), clock)
}

fn issued(exp_secs: i64, clock: &FakeClock) -> TokenRequest {
    TokenRequest {
        spec: TokenRequestSpec {
            expiration_seconds: Some(exp_secs),
            ..Default::default()
        },
        status: TokenRequestStatus {
            token: "foo".into(),
            expiration_timestamp: clock.now() + Duration::seconds(exp_secs),
        },
        ..Default::default()
    }
}

async fn setup(exp: Duration) -> (Arc<Manager>, Arc<FakeClock>, Arc<FakeTokenGetter>) {
    let clock = FakeClock::new(thirty_days());
    let tg = Arc::new(FakeTokenGetter {
        count: AtomicUsize::new(0),
        tr: Some(issued(exp.num_seconds(), &clock)),
        err: None,
    });
    let mgr = Manager::new_with_clock(tg.get_token(), clock.clone());
    mgr.get_service_account_token("a", "b", token_request())
        .await
        .unwrap();
    assert_eq!(tg.count(), 1, "unexpected client call");
    mgr.get_service_account_token("a", "b", token_request())
        .await
        .unwrap();
    assert_eq!(tg.count(), 1, "expected token to be served from cache");
    (mgr, clock, tg)
}

// TestTokenCachingAndExpiration
#[tokio::test]
async fn rotate_hour_token_in_the_last_12_minutes() {
    let (mgr, clock, tg) = setup(Duration::hours(1)).await;
    clock.set_time(clock.now() + Duration::minutes(50));
    mgr.get_service_account_token("a", "b", token_request())
        .await
        .unwrap();
    assert_eq!(tg.count(), 2, "expected token to be refreshed");
}

#[tokio::test]
async fn rotate_24_hour_cap_for_a_40_hour_token() {
    let (mgr, clock, tg) = setup(Duration::hours(40)).await;
    clock.set_time(clock.now() + Duration::hours(25));
    mgr.get_service_account_token("a", "b", token_request())
        .await
        .unwrap();
    assert_eq!(tg.count(), 2, "expected token to be refreshed");
}

#[tokio::test]
async fn refresh_failure_with_valid_old_token_returns_old_token() {
    let (mgr, clock, _tg) = setup(Duration::hours(1)).await;
    clock.set_time(clock.now() + Duration::minutes(50));
    let failing = Arc::new(FakeTokenGetter {
        count: AtomicUsize::new(0),
        tr: None,
        err: Some("err"),
    });
    mgr.set_get_token(failing.get_token());
    let tr = mgr
        .get_service_account_token("a", "b", token_request())
        .await
        .unwrap();
    assert_eq!(tr.status.token, "foo");
}

#[tokio::test]
async fn refresh_failure_with_expired_token_is_an_error() {
    let (mgr, clock, _tg) = setup(Duration::hours(1)).await;
    clock.set_time(clock.now() + Duration::minutes(70));
    let failing = Arc::new(FakeTokenGetter {
        count: AtomicUsize::new(0),
        tr: None,
        err: Some("err"),
    });
    mgr.set_get_token(failing.get_token());
    let err = mgr
        .get_service_account_token("a", "b", token_request())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("expired and refresh failed: err"), "{err}");
}

#[tokio::test]
async fn first_fetch_failure_is_an_error() {
    let clock = FakeClock::new(thirty_days());
    let failing = Arc::new(FakeTokenGetter {
        count: AtomicUsize::new(0),
        tr: None,
        err: Some("boom"),
    });
    let mgr = Manager::new_with_clock(failing.get_token(), clock);
    let err = mgr
        .get_service_account_token("a", "b", token_request())
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(err, "failed to fetch token: boom");
}

fn uid_request(uid: &str) -> TokenRequest {
    TokenRequest {
        uid: uid.into(),
        spec: spec("foo-uid", "foo-pod"),
        ..Default::default()
    }
}

#[tokio::test]
async fn service_account_recreated_is_a_cache_miss() {
    let (mgr, _clock, tg) = setup(Duration::hours(1)).await;
    mgr.get_service_account_token("a", "b", uid_request("service-account-uid-1"))
        .await
        .unwrap();
    assert_eq!(tg.count(), 2);
    mgr.get_service_account_token("a", "b", uid_request("service-account-uid-2"))
        .await
        .unwrap();
    assert_eq!(
        tg.count(),
        3,
        "expected cache miss due to different service account UID"
    );
}

#[tokio::test]
async fn consistent_service_account_uid_is_a_cache_hit() {
    let (mgr, _clock, tg) = setup(Duration::hours(1)).await;
    for _ in 0..2 {
        mgr.get_service_account_token("a", "b", uid_request("consistent"))
            .await
            .unwrap();
    }
    assert_eq!(tg.count(), 2, "setup call + first call, second is a hit");
}

// TestRequiresRefresh
#[test]
fn requires_refresh_cases() {
    let start = Utc.timestamp_opt(1_000_000, 0).unwrap();
    let cases: [(Duration, Duration, bool, bool); 5] = [
        (Duration::minutes(10), Duration::minutes(60), false, true),
        (Duration::minutes(50), Duration::minutes(60), true, true),
        (Duration::hours(25), Duration::hours(60), true, true),
        (Duration::minutes(70), Duration::minutes(60), true, true),
        // expirationSeconds nil: never refresh.
        (Duration::minutes(0), Duration::minutes(60), false, false),
    ];
    for (i, (now, exp, expect, has_secs)) in cases.into_iter().enumerate() {
        let mgr = mgr_at(FakeClock::new(start + now));
        let tr = TokenRequest {
            spec: TokenRequestSpec {
                expiration_seconds: has_secs.then(|| exp.num_seconds()),
                ..Default::default()
            },
            status: TokenRequestStatus {
                expiration_timestamp: start + exp,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(mgr.requires_refresh(&tr), expect, "case {i}");
    }
}

// TestDeleteServiceAccountToken
#[tokio::test]
async fn delete_service_account_token_evicts_only_that_pod() {
    struct Req {
        name: &'static str,
        namespace: &'static str,
        tr: TokenRequest,
        should_fail: bool,
    }
    let req = |n: &'static str, uid: &'static str, fail: bool| Req {
        name: n,
        namespace: "ns",
        tr: TokenRequest {
            spec: TokenRequestSpec {
                bound_object_ref: Some(BoundObjectReference {
                    uid: uid.into(),
                    name: n.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        },
        should_fail: fail,
    };
    let requests = [
        req("n1", "fake-uid-1", false),
        req("n2", "fake-uid-2", false),
        req("n3", "fake-uid-3", false),
        req("n4", "fake-uid-4", true),
    ];
    #[allow(clippy::type_complexity)]
    let cases: [(&str, &[usize], &[&str], &[usize]); 5] = [
        ("delete none", &[0, 1, 2], &[], &[0, 1, 2]),
        ("delete one", &[0, 1, 2], &["fake-uid-1"], &[1, 2]),
        (
            "delete two",
            &[0, 1, 2],
            &["fake-uid-1", "fake-uid-3"],
            &[1],
        ),
        (
            "delete all",
            &[0, 1, 2],
            &["fake-uid-1", "fake-uid-2", "fake-uid-3"],
            &[],
        ),
        (
            "failed request is not cached",
            &[0, 1, 2, 3],
            &[],
            &[0, 1, 2],
        ),
    ];
    for (name, idx, delete, left) in cases {
        let clock = FakeClock::new(thirty_days());
        let mgr = mgr_at(clock.clone());
        let c2 = clock.clone();
        let success: GetTokenFn = Arc::new(move |_, _, mut tr| {
            tr.status = TokenRequestStatus {
                expiration_timestamp: c2.now() + Duration::hours(10),
                ..Default::default()
            };
            Box::pin(async move { Ok(tr) })
        });
        let fail: GetTokenFn =
            Arc::new(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("fail tr")) }));
        for &i in idx {
            let r = &requests[i];
            mgr.set_get_token(if r.should_fail {
                fail.clone()
            } else {
                success.clone()
            });
            let _ = mgr
                .get_service_account_token(r.namespace, r.name, r.tr.clone())
                .await;
        }
        for uid in delete {
            mgr.delete_service_account_token(uid);
        }
        assert_eq!(mgr.cache_len(), left.len(), "{name}");
        for &i in left {
            let r = &requests[i];
            assert!(
                mgr.get(&key_func(r.name, r.namespace, &r.tr)).is_some(),
                "{name}: request {i} should remain cached"
            );
        }
    }
}

// TestCleanup
#[test]
fn cleanup_drops_only_expired_tokens() {
    for (rel, want) in [(-1, 0usize), (1, 1)] {
        let clock = FakeClock::new(Utc.timestamp_opt(24 * 3600, 0).unwrap());
        let mgr = mgr_at(clock.clone());
        mgr.set(
            "key",
            TokenRequest {
                status: TokenRequestStatus {
                    expiration_timestamp: clock.now() + Duration::hours(rel),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        mgr.cleanup();
        assert_eq!(mgr.cache_len(), want, "relativeExp {rel}h");
    }
}

// TestKeyFunc
#[test]
fn key_func_distinguishes_every_input() {
    let base = ("foo-sa", "foo-ns", token_request());
    let key = |n: &str, ns: &str, tr: &TokenRequest| key_func(n, ns, tr);
    let same = key(base.0, base.1, &token_request());
    assert_eq!(key(base.0, base.1, &base.2), same, "hit");
    assert_ne!(key("foo-sa2", base.1, &base.2), same, "name");
    assert_ne!(key(base.0, "ns2", &base.2), same, "namespace");
    let mut t = token_request();
    t.spec.audiences = vec!["foo1".into()];
    assert_ne!(key(base.0, base.1, &t), same, "audiences");
    let mut t = token_request();
    t.spec.expiration_seconds = Some(2001);
    assert_ne!(key(base.0, base.1, &t), same, "expirationSeconds");
    let mut t = token_request();
    t.spec.bound_object_ref.as_mut().unwrap().uid = "other".into();
    assert_ne!(key(base.0, base.1, &t), same, "boundObjectRef");
    // A UID precondition changes the key; the same UID does not.
    let mut with_uid = token_request();
    with_uid.uid = "service-account-uid-123".into();
    assert_ne!(key(base.0, base.1, &with_uid), same, "uid vs none");
    let mut with_uid2 = token_request();
    with_uid2.uid = "service-account-uid-123".into();
    assert_eq!(
        key(base.0, base.1, &with_uid),
        key(base.0, base.1, &with_uid2),
        "same uid"
    );
}
