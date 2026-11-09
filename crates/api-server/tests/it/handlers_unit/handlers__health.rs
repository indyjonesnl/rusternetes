//! Ported from `staging/src/k8s.io/apiserver/pkg/server/healthz/healthz_test.go`
//! (`testMultipleChecks`): the root and per-check responses of the handlers.
#![allow(unused_imports, dead_code)]

use axum::body::to_bytes;
use axum::http::{header, StatusCode};
use axum::response::Response;
use rusternetes_api_server::handlers::health::*;

fn checks(add_bad: bool) -> Vec<HealthChecker> {
    let mut c = vec![HealthChecker::ready("ping", Ok(()))];
    if add_bad {
        c.push(HealthChecker::ready("bad", Err("this will fail".into())));
    }
    c
}

async fn body(resp: Response) -> (StatusCode, String, String) {
    let status = resp.status();
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, ct, String::from_utf8(bytes.to_vec()).unwrap())
}

/// `testMultipleChecks`' table; `path` is the suffix after the endpoint.
#[tokio::test]
async fn multiple_checks_table() {
    let name = "healthz";
    let ok = StatusCode::OK;
    let fail = StatusCode::INTERNAL_SERVER_ERROR;
    let cases: Vec<(&str, String, StatusCode, bool)> = vec![
        ("?verbose", format!("[+]ping ok\n{name} check passed\n"), ok, false),
        ("?exclude=dontexist", "ok".into(), ok, false),
        ("?exclude=bad", "ok".into(), ok, true),
        (
            "?verbose=true&exclude=bad",
            format!("[+]ping ok\n[+]bad excluded: ok\n{name} check passed\n"),
            ok,
            true,
        ),
        (
            "?verbose=true&exclude=dontexist",
            format!(
                "[+]ping ok\nwarn: some health checks cannot be excluded: no matches for \"dontexist\"\n{name} check passed\n"
            ),
            ok,
            false,
        ),
        ("/ping", "ok".into(), ok, false),
        ("", "ok".into(), ok, false),
        (
            "?verbose",
            format!("[+]ping ok\n[-]bad failed: reason withheld\n{name} check failed\n"),
            fail,
            true,
        ),
        ("/ping", "ok".into(), ok, true),
        ("/bad", "internal server error: this will fail\n".into(), fail, true),
        (
            "",
            format!("[+]ping ok\n[-]bad failed: reason withheld\n{name} check failed\n"),
            fail,
            true,
        ),
    ];
    for (i, (path, want, status, add_bad)) in cases.into_iter().enumerate() {
        let c = checks(add_bad);
        let resp = match path.strip_prefix('/') {
            Some(check) => handle_single_check(&c, check).await,
            None => handle_root_health(name, &c, path.strip_prefix('?')).await,
        };
        let (got_status, ct, got) = body(resp).await;
        assert_eq!(got_status, status, "case[{i}] {path}");
        assert_eq!(ct, "text/plain; charset=utf-8", "case[{i}] {path}");
        assert_eq!(got, want, "case[{i}] {path}");
    }
}

#[tokio::test]
async fn unknown_check_is_404() {
    let (status, _, got) = body(handle_single_check(&checks(false), "nope").await).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(got, "404 page not found\n");
}

/// `postStartHookHealthz` (hooks.go:222-246) through the check list: 500
/// `not finished` until the hook is done.
#[tokio::test]
async fn poststarthook_check_fails_until_finished() {
    use rusternetes_api_server::post_start_hooks::PostStartHooks;
    use std::sync::atomic::Ordering;
    let reg = PostStartHooks::new();
    let n = "scheduling/bootstrap-system-priority-classes";
    let done = reg.register(n);
    let list = |reg: &PostStartHooks| -> Vec<HealthChecker> {
        reg.checks()
            .into_iter()
            .map(|(name, r)| HealthChecker::ready(name, r.map_err(str::to_string)))
            .collect()
    };
    let path = format!("poststarthook/{n}");
    let (s, _, b) = body(handle_single_check(&list(&reg), &path).await).await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(b.contains("not finished"), "{b}");
    done.store(true, Ordering::SeqCst);
    let (s, _, b) = body(handle_single_check(&list(&reg), &path).await).await;
    assert_eq!((s, b.as_str()), (StatusCode::OK, "ok"));
}
