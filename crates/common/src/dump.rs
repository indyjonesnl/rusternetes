//! Payload-dump instrumentation for conformance debugging.
//!
//! When `RUSTERNETES_DUMP_PAYLOADS=1`, panics, 5xx responses, and JSON decode
//! failures emit a `tracing::error!` containing the offending request body
//! (with Secret data redacted). All entrypoints are no-ops when the env var
//! is unset.

use std::borrow::Cow;
use std::sync::OnceLock;

static DUMPS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns true iff `RUSTERNETES_DUMP_PAYLOADS=1` was set when this process
/// started.
pub fn dumps_enabled() -> bool {
    *DUMPS_ENABLED
        .get_or_init(|| std::env::var("RUSTERNETES_DUMP_PAYLOADS").is_ok_and(|v| v == "1"))
}

/// Length of a base64-encoded payload's decoded bytes, or the encoded length
/// if decoding fails. Used for the redaction marker so the dump still hints
/// at the value's size.
fn decoded_len(b64: &str) -> usize {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map(|v| v.len())
        .unwrap_or_else(|_| b64.len())
}

fn redact_data_map(map: &mut serde_json::Map<String, serde_json::Value>) {
    for v in map.values_mut() {
        if let Some(s) = v.as_str() {
            let n = decoded_len(s);
            *v = serde_json::Value::String(format!("<redacted len={n}>"));
        }
    }
}

fn redact_string_data_map(map: &mut serde_json::Map<String, serde_json::Value>) {
    for v in map.values_mut() {
        if let Some(s) = v.as_str() {
            let n = s.len();
            *v = serde_json::Value::String(format!("<redacted len={n}>"));
        }
    }
}

fn redact_one(value: &mut serde_json::Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let kind = obj
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("")
        .to_string();
    match kind.as_str() {
        "Secret" => {
            if let Some(serde_json::Value::Object(m)) = obj.get_mut("data") {
                redact_data_map(m);
            }
            if let Some(serde_json::Value::Object(m)) = obj.get_mut("stringData") {
                redact_string_data_map(m);
            }
        }
        "SecretList" => {
            if let Some(serde_json::Value::Array(items)) = obj.get_mut("items") {
                for item in items {
                    redact_one(item);
                }
            }
        }
        "AdmissionReview" => {
            if let Some(serde_json::Value::Object(req)) = obj.get_mut("request") {
                for key in ["object", "oldObject"] {
                    if let Some(v) = req.get_mut(key) {
                        redact_one(v);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Replace `data` / `stringData` values in any embedded `Secret` with
/// `"<redacted len=N>"`. Pass-through for non-JSON, non-Secret bodies.
pub fn redact_secret_like(bytes: &[u8]) -> Cow<'_, [u8]> {
    let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Cow::Borrowed(bytes);
    };
    let before = v.clone();
    redact_one(&mut v);
    if v == before {
        return Cow::Borrowed(bytes);
    }
    match serde_json::to_vec(&v) {
        Ok(out) => Cow::Owned(out),
        Err(_) => Cow::Borrowed(bytes),
    }
}

use std::cell::RefCell;
use std::sync::Once;

static INSTALL: Once = Once::new();

/// Install a panic hook that, when a panic fires inside a `with_payload`
/// scope, emits one `tracing::error!` with the component name and the
/// redacted payload. Chains over (does not replace) the previous hook so
/// the default backtrace continues to print. Safe to call multiple times;
/// only the first call wins.
pub fn install_panic_hook(component: &'static str) {
    INSTALL.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = CURRENT_PAYLOAD
                .try_with(|cell| cell.borrow().clone())
                .ok()
                .flatten();
            if let Some(body) = payload {
                let redacted = redact_secret_like(&body);
                let preview = String::from_utf8_lossy(&redacted);
                tracing::error!(
                    component = component,
                    panic = %info,
                    payload = %preview,
                    "panic with in-flight payload"
                );
            }
            prev(info);
        }));
    });
}

tokio::task_local! {
    pub static CURRENT_PAYLOAD: RefCell<Option<bytes::Bytes>>;
}

/// Run `fut` with `body` accessible via `CURRENT_PAYLOAD` for the duration
/// of the future (and any tasks it spawns that inherit the task-local).
pub async fn with_payload<F, T>(body: bytes::Bytes, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT_PAYLOAD.scope(RefCell::new(Some(body)), fut).await
}

#[cfg(feature = "axum-support")]
use axum::{
    body::{Body, Bytes as AxumBytes},
    extract::{FromRequest, Request},
    response::{IntoResponse, Response},
    Json,
};

/// Drop-in replacement for `axum::Json<T>` that, when payload dumps are
/// enabled, buffers the request body into `CURRENT_PAYLOAD` before
/// delegating to `axum::Json<T>` and logs the body on decode failure.
#[cfg(feature = "axum-support")]
#[derive(Debug)]
pub struct DumpingJson<T>(pub T);

#[cfg(feature = "axum-support")]
#[async_trait::async_trait]
impl<T, S> FromRequest<S> for DumpingJson<T>
where
    T: serde::de::DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = DumpingJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        // The body is buffered unconditionally. `axum::Json` buffers it anyway
        // (`Bytes::from_request`), and `Bytes` clones are refcount bumps, so
        // the only cost is cloning the header map — while the gain is that the
        // rejection path still HAS the body, which is what upstream's
        // `transformDecodeError` needs to name the kind and version the client
        // sent (endpoints/handlers/rest.go:245-256).
        let (parts, body) = req.into_parts();
        let bytes = AxumBytes::from_request(Request::from_parts(parts.clone(), body), state)
            .await
            .map_err(|_| DumpingJsonRejection::BodyRead)?;

        // Stash in task-local for the panic-hook path.
        if dumps_enabled() {
            let _ = CURRENT_PAYLOAD.try_with(|cell| {
                *cell.borrow_mut() = Some(bytes.clone());
            });
        }

        let rebuilt = Request::from_parts(parts, Body::from(bytes.clone()));
        match Json::<T>::from_request(rebuilt, state).await {
            Ok(Json(t)) => Ok(DumpingJson(t)),
            Err(rej) => {
                if dumps_enabled() {
                    let redacted = redact_secret_like(&bytes);
                    tracing::error!(
                        rejection = %rej,
                        payload = %String::from_utf8_lossy(&redacted),
                        "JSON body decode failed"
                    );
                }
                Err(DumpingJsonRejection::Decode(transform_decode_error::<T>(
                    &rej.body_text(),
                    &bytes,
                )))
            }
        }
    }
}

/// Decode a client-supplied request body, answering a failure the way upstream
/// does: `transformDecodeError` → `errors.NewBadRequest`, i.e. a `Status` with
/// `reason: BadRequest` and HTTP 400
/// (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go:245-256).
///
/// **Use this for every request body decoded by hand.** Handlers that take a
/// raw `Bytes` body and call `serde_json::from_slice` themselves used to map
/// the failure to `Error::InvalidResource`, which renders as 422/Invalid — the
/// code reserved for an object that decoded and then failed validation. Eight
/// create handlers did that while the rest of the server answered 400, so the
/// response shape depended on which handler you happened to hit (#1915).
pub fn decode_request_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> crate::Result<T> {
    serde_json::from_slice(body)
        .map_err(|e| crate::Error::BadRequest(transform_decode_error::<T>(&e.to_string(), body)))
}

/// The structural check a typed decode would have performed, for a handler that
/// keeps its body as an untyped `serde_json::Value`.
///
/// Upstream types every resource, so `{"metadata": "not-an-object"}` never
/// reaches a handler — the decoder rejects it with `transformDecodeError` →
/// 400/BadRequest. A handler that decodes into `Value` has no such step, so a
/// malformed body sails past and surfaces later as a confusing validation error
/// about a missing name (#1915; the untyped handlers themselves are #1911).
///
/// Checks only what every Kubernetes object declares as an object: `metadata`,
/// `spec` and `status`. `kind` and `apiVersion` are strings and are checked
/// where they are read.
pub fn require_object_shape(value: &serde_json::Value, kind: &str) -> crate::Result<()> {
    for field in ["metadata", "spec", "status"] {
        match value.get(field) {
            None | Some(serde_json::Value::Null) => {}
            Some(v) if v.is_object() => {}
            Some(v) => {
                let got = match v {
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Bool(_) => "bool",
                    serde_json::Value::Array(_) => "array",
                    _ => "value",
                };
                return Err(crate::Error::BadRequest(format!(
                    "{kind} in version \"v1\" cannot be handled as a {kind}: \
                     json: cannot unmarshal {got} into Go struct field {kind}.{field} \
                     of type v1.{field}"
                )));
            }
        }
    }
    Ok(())
}

/// Port of upstream `transformDecodeError`
/// (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go:245-256).
///
/// Every decode failure becomes `errors.NewBadRequest(...)` — a `Status` with
/// `reason: BadRequest` and HTTP 400. Which of the two messages you get depends
/// on whether the body named a `kind`:
///
/// ```go
/// if gvk != nil && len(gvk.Kind) > 0 {
///     return errors.NewBadRequest(fmt.Sprintf("%s in version %q cannot be handled as a %s: %v", gvk.Kind, gvk.Version, objGVK.Kind, baseErr))
/// }
/// summary := summarizeData(body, 30)
/// return errors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized (must be of type %s): %v (%s)", objGVK.Kind, baseErr, summary))
/// ```
///
/// `objGVK.Kind` is the type the handler is decoding INTO, which is `T` here.
fn transform_decode_error<T>(base_err: &str, body: &[u8]) -> String {
    let target = target_kind::<T>();

    // `gvk` upstream comes from the body's own apiVersion/kind, not the path.
    let parsed: Option<serde_json::Value> = serde_json::from_slice(body).ok();
    let kind = parsed
        .as_ref()
        .and_then(|v| v.get("kind"))
        .and_then(|k| k.as_str())
        .filter(|k| !k.is_empty());

    match kind {
        Some(kind) => {
            // `gvk.Version` is the version alone: "apps/v1" contributes "v1".
            let api_version = parsed
                .as_ref()
                .and_then(|v| v.get("apiVersion"))
                .and_then(|a| a.as_str())
                .unwrap_or("");
            let version = api_version.rsplit('/').next().unwrap_or("");
            format!("{kind} in version {version:?} cannot be handled as a {target}: {base_err}")
        }
        None => format!(
            "the object provided is unrecognized (must be of type {target}): {base_err} ({})",
            summarize_data(body, 30)
        ),
    }
}

/// The bare kind name for `T`, standing in for upstream's
/// `typer.ObjectKinds(into)`. `std::any::type_name` yields a fully qualified
/// path, and the last segment is the struct name — which is exactly the
/// Kubernetes kind for every resource struct in `crate::resources`.
fn target_kind<T>() -> String {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("Object")
        .to_string()
}

/// Upstream `summarizeData` (endpoints/handlers/rest.go:355-370): JSON-looking
/// data is shown as text and truncated with a trailing " ...", anything else is
/// hex-encoded so a binary body cannot smuggle control characters into an error
/// message.
fn summarize_data(data: &[u8], max_length: usize) -> String {
    if data.is_empty() {
        return "<empty>".to_string();
    }
    if data[0] == b'{' {
        if data.len() > max_length {
            return format!("{} ...", String::from_utf8_lossy(&data[..max_length]));
        }
        return String::from_utf8_lossy(data).into_owned();
    }
    let hex = |d: &[u8]| d.iter().map(|b| format!("{b:02x}")).collect::<String>();
    if data.len() > max_length {
        format!("{} ...", hex(&data[..max_length]))
    } else {
        hex(data)
    }
}

#[cfg(feature = "axum-support")]
#[derive(Debug)]
pub enum DumpingJsonRejection {
    /// A body the decoder could not turn into `T`, already rendered into
    /// upstream's `transformDecodeError` wording.
    Decode(String),
    BodyRead,
}

#[cfg(feature = "axum-support")]
impl IntoResponse for DumpingJsonRejection {
    /// Both arms answer with a `Status`, through the same `Error` rendering
    /// every other failure in the server uses.
    ///
    /// Axum's own `JsonRejection` used to be returned verbatim: `text/plain`,
    /// no `kind`, no `reason`, no `code`, and HTTP **422**. A client-go client
    /// cannot classify that, `kubectl` prints the raw string, and the status
    /// code was wrong besides — 422/Invalid is for an object that decoded and
    /// then failed validation, while a body that never decoded is
    /// 400/BadRequest (#1915).
    fn into_response(self) -> Response {
        match self {
            Self::Decode(message) => crate::Error::BadRequest(message).into_response(),
            Self::BodyRead => {
                crate::Error::BadRequest("failed to read request body".to_string()).into_response()
            }
        }
    }
}

#[cfg(feature = "axum-support")]
impl std::fmt::Display for DumpingJsonRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(m) => write!(f, "{m}"),
            Self::BodyRead => write!(f, "failed to read request body"),
        }
    }
}

#[cfg(feature = "axum-support")]
impl std::error::Error for DumpingJsonRejection {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumps_enabled_reads_env_once() {
        // Cannot reliably mutate process env across tests, so just assert
        // the function does not panic and returns a stable bool.
        let a = dumps_enabled();
        let b = dumps_enabled();
        assert_eq!(a, b);
    }

    #[test]
    fn redact_passthrough_for_plain_pod() {
        let input = br#"{"kind":"Pod","metadata":{"name":"p"}}"#;
        assert_eq!(&*redact_secret_like(input), input);
    }

    #[test]
    fn redact_replaces_secret_data_values() {
        let input = br#"{"kind":"Secret","data":{"token":"YWJjZA==","empty":""}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["token"], "<redacted len=4>");
        assert_eq!(v["data"]["empty"], "<redacted len=0>");
    }

    #[test]
    fn redact_replaces_secret_string_data_values() {
        let input = br#"{"kind":"Secret","stringData":{"pw":"hunter2"}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["stringData"]["pw"], "<redacted len=7>");
    }

    #[test]
    fn redact_walks_secret_list_items() {
        let input = br#"{"kind":"SecretList","items":[
        {"kind":"Secret","data":{"k":"YWI="}},
        {"kind":"Secret","stringData":{"k":"v"}}
    ]}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["items"][0]["data"]["k"], "<redacted len=2>");
        assert_eq!(v["items"][1]["stringData"]["k"], "<redacted len=1>");
    }

    #[test]
    fn redact_walks_admission_review_object() {
        let input = br#"{"kind":"AdmissionReview","request":{
        "object":{"kind":"Secret","data":{"k":"YWI="}},
        "oldObject":{"kind":"Secret","stringData":{"k":"x"}}
    }}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["request"]["object"]["data"]["k"], "<redacted len=2>");
        assert_eq!(
            v["request"]["oldObject"]["stringData"]["k"],
            "<redacted len=1>"
        );
    }

    #[test]
    fn redact_passthrough_for_malformed_json() {
        let input = b"not json at all";
        assert_eq!(&*redact_secret_like(input), input);
    }

    #[test]
    fn redact_leaves_configmap_alone() {
        let input = br#"{"kind":"ConfigMap","data":{"k":"v"}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["k"], "v");
    }

    #[tokio::test]
    async fn with_payload_makes_bytes_visible_inside_scope() {
        let body = bytes::Bytes::from_static(b"hello");
        let seen = with_payload(body.clone(), async {
            CURRENT_PAYLOAD.with(|cell| cell.borrow().clone())
        })
        .await;
        assert_eq!(seen.as_deref(), Some(b"hello".as_ref()));
    }

    #[tokio::test]
    async fn current_payload_outside_scope_returns_err() {
        let res = CURRENT_PAYLOAD.try_with(|cell| cell.borrow().clone());
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn panic_hook_logs_payload_under_scope() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default, Clone)]
        struct Capture(Arc<Mutex<Vec<String>>>);

        impl<S> tracing_subscriber::Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut visitor = StrVisitor::default();
                event.record(&mut visitor);
                self.0.lock().unwrap().push(visitor.0);
            }
        }

        #[derive(Default)]
        struct StrVisitor(String);
        impl tracing::field::Visit for StrVisitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, " {}={:?}", field.name(), value);
            }
        }

        let capture = Capture::default();
        let sub = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(sub);

        install_panic_hook("test-component");

        let body = bytes::Bytes::from_static(br#"{"kind":"Pod"}"#);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            futures::executor::block_on(with_payload(body, async {
                panic!("boom");
            }))
        }));

        let logs = capture.0.lock().unwrap().clone();
        assert!(
            logs.iter()
                .any(|l| l.contains("test-component") && l.contains("Pod")),
            "no dump log captured; got: {logs:?}"
        );
    }

    #[cfg(feature = "axum-support")]
    #[tokio::test]
    async fn dumping_json_decodes_valid_body() {
        use axum::body::Body;
        use axum::extract::FromRequest;
        use axum::http::{header, Request};

        #[derive(serde::Deserialize)]
        struct Foo {
            x: i32,
        }

        let req = Request::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"x":7}"#))
            .unwrap();
        let DumpingJson(foo) = DumpingJson::<Foo>::from_request(req, &()).await.unwrap();
        assert_eq!(foo.x, 7);
    }

    #[cfg(feature = "axum-support")]
    #[tokio::test]
    async fn dumping_json_rejects_invalid_body_with_same_status_as_json() {
        use axum::body::Body;
        use axum::extract::FromRequest;
        use axum::http::{header, Request, StatusCode};
        use axum::response::IntoResponse;

        #[derive(Debug, serde::Deserialize)]
        struct Foo {
            _x: i32,
        }

        let req = Request::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("not json"))
            .unwrap();
        let err = DumpingJson::<Foo>::from_request(req, &())
            .await
            .unwrap_err();
        let resp = err.into_response();
        assert!(
            resp.status() == StatusCode::BAD_REQUEST
                || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
        );
    }
}
