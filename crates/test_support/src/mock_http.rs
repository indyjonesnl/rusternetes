//! Tiny in-process HTTP backends for tests (admission webhooks, aggregated
//! API servers). Replaces the `warp` dev-dependency with the axum the
//! workspace already builds.

use axum::extract::Json;
use axum::routing::post;
use axum::Router;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::net::SocketAddr;

/// A JSON reply; the common return type of the mock handlers.
pub fn reply_json<T: serde::Serialize + ?Sized>(value: &T) -> Json<serde_json::Value> {
    Json(serde_json::to_value(value).expect("mock reply serialises"))
}

/// Answers a POST to any path with `f(body)`, the body decoded as JSON.
pub fn mock_post<Req, F>(f: F) -> Router
where
    Req: DeserializeOwned + Send + 'static,
    F: Fn(Req) -> Json<serde_json::Value> + Clone + Send + Sync + 'static,
{
    mock_post_async(move |req| {
        let f = f.clone();
        async move { f(req) }
    })
}

/// As [`mock_post`], for a handler that awaits.
pub fn mock_post_async<Req, F, Fut>(f: F) -> Router
where
    Req: DeserializeOwned + Send + 'static,
    F: Fn(Req) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Json<serde_json::Value>> + Send + 'static,
{
    Router::new().fallback_service(post(move |Json(req): Json<Req>| {
        let f = f.clone();
        async move { f(req).await }
    }))
}

/// Serves `app` on an ephemeral loopback port until `shutdown` resolves.
/// Must be called from within a tokio runtime.
pub fn serve_graceful<S>(app: Router, shutdown: S) -> SocketAddr
where
    S: Future<Output = ()> + Send + 'static,
{
    serve_graceful_handle(app, shutdown).0
}

/// As [`serve_graceful`], also returning the server task so a test can wait
/// for the graceful shutdown to finish.
pub fn serve_graceful_handle<S>(
    app: Router,
    shutdown: S,
) -> (SocketAddr, tokio::task::JoinHandle<()>)
where
    S: Future<Output = ()> + Send + 'static,
{
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("local addr");
    let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .ok();
    });
    (addr, handle)
}
