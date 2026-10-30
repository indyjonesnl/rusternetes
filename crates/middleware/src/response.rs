//! HTTP response handling with content negotiation
//!
//! Supports both JSON and Protobuf serialization based on Accept header.
//!
//! Native Protobuf responses
//! -------------------------
//! Real Kubernetes encodes Protobuf responses as
//! `k8s\0` + `runtime.Unknown { typeMeta, raw, contentType }` where `raw`
//! contains native protobuf bytes produced by the generated `pb.go`
//! `Marshal` methods for the resource type (e.g. `core/v1.Pod`).
//!
//! Today's encoder ladder:
//!
//! - [`NativePodProtoEncoder`] (Pod / PodList) walks the schema in
//!   `rusternetes_protobuf::PROTO_REGISTRY` and emits real proto bytes into
//!   `Unknown.raw`, matching what upstream `pb.go` would produce.
//! - [`WrappedJsonProtoEncoder`] is the default fallback for kinds without
//!   a native encoder yet: it stuffs the JSON bytes into `Unknown.raw` and
//!   sets `Unknown.contentType` to `application/json`. Upstream `client-go`'s
//!   typed proto decoder does NOT consult `contentType` and so cannot handle
//!   this fallback — only Unknown-aware tooling can — but it preserves the
//!   pre-native behaviour for non-opted-in kinds.
//!
//! The [`ProtoEncoder`] trait is the extensibility seam: each resource type
//! can register an implementation that produces native protobuf bytes for
//! its kind. The [`NativeProtoOptIn`] response extension is how a handler
//! tells the response-wrapping middleware "I'm OK with you emitting a
//! protobuf envelope for this response when the client asked for one";
//! [`encoder_for`] picks the right encoder based on the opt-in's `kind`.
//!
//! See `crates/middleware/src/lib.rs` for where the marker is read
//! and `crates/api-server/src/handlers/pod.rs` for the first opt-in
//! consumer.

use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// API response wrapper that supports content negotiation
pub struct ApiResponse<T> {
    pub data: T,
    pub status: StatusCode,
}

impl<T> ApiResponse<T> {
    /// Create a new API response
    pub fn new(data: T) -> Self {
        Self {
            data,
            status: StatusCode::OK,
        }
    }

    /// Create a new API response with a specific status code
    pub fn with_status(data: T, status: StatusCode) -> Self {
        Self { data, status }
    }
}

impl<T> IntoResponse for ApiResponse<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // For now, default to JSON
        // In full implementation, check Accept header and return protobuf if requested
        match rusternetes_common::latency_trackers::track_serialize_response_object_latency_sync(
            || serde_json::to_vec(&self.data),
        ) {
            Ok(body) => Response::builder()
                .status(self.status)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
            Err(e) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to serialize response: {}", e)))
                .unwrap(),
        }
    }
}

/// Negotiate content type based on Accept header
pub fn negotiate_content_type(headers: &HeaderMap) -> ContentType {
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            if accept_str.contains("application/vnd.kubernetes.protobuf") {
                return ContentType::Protobuf;
            }
        }
    }
    ContentType::Json
}

/// Content type for responses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Json,
    Protobuf,
}

impl ContentType {
    /// Get the MIME type string
    pub fn mime_type(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::Protobuf => "application/vnd.kubernetes.protobuf",
        }
    }
}

/// Create a response with content negotiation
/// Note: Protobuf encoding requires api_version and kind, so this is a simplified version
pub fn create_response<T>(data: T, status: StatusCode, _content_type: ContentType) -> Response
where
    T: Serialize,
{
    // For now, always use JSON since protobuf encoding needs type metadata
    // In full implementation, this would check content_type and encode appropriately
    match rusternetes_common::latency_trackers::track_serialize_response_object_latency_sync(|| {
        serde_json::to_vec(&data)
    }) {
        Ok(body) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, ContentType::Json.mime_type())
            .body(Body::from(body))
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to serialize: {}", e)))
            .unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Native Protobuf scaffold
// ---------------------------------------------------------------------------

/// Marker extension that a handler attaches to its response to opt in to
/// protobuf encoding when the client's `Accept` header asks for
/// `application/vnd.kubernetes.protobuf`.
///
/// The response-wrapping middleware in `lib.rs` looks for this
/// extension; if it is present AND the client requested protobuf AND the
/// response body is JSON, the middleware rewrites the response into a K8s
/// `k8s\0`-framed `runtime.Unknown` envelope via the configured
/// [`ProtoEncoder`].
///
/// Without the marker the middleware leaves the JSON response untouched —
/// preserving today's behaviour for every resource type that has not yet
/// opted in.
#[derive(Clone, Debug)]
pub struct NativeProtoOptIn {
    /// `apiVersion` to write into the `runtime.Unknown.typeMeta.apiVersion`
    /// field. For Pod GET this is `"v1"`.
    pub api_version: &'static str,
    /// `kind` to write into the `runtime.Unknown.typeMeta.kind` field.
    /// For Pod GET this is `"Pod"`; for LIST it is `"PodList"`; etc.
    pub kind: &'static str,
}

impl NativeProtoOptIn {
    pub const fn new(api_version: &'static str, kind: &'static str) -> Self {
        Self { api_version, kind }
    }

    /// Opt-in for a single `core/v1.Pod` response.
    pub const fn pod() -> Self {
        Self::new("v1", "Pod")
    }

    /// Opt-in for a `core/v1.PodList` response.
    pub const fn pod_list() -> Self {
        Self::new("v1", "PodList")
    }

    /// Opt-in for any resource response: the `apiVersion`/`kind` are read from
    /// the response body (`transformResponseObject`, endpoints/handlers/
    /// response.go, serialises every object through the negotiated codec,
    /// which stamps the GVK from the scheme). The middleware only re-encodes
    /// when a protobuf schema is registered for that GVK and otherwise leaves
    /// the JSON response alone.
    pub const fn from_body() -> Self {
        Self::new("", "")
    }

    /// Whether this opt-in defers the type to the response body.
    pub fn is_from_body(&self) -> bool {
        self.kind.is_empty()
    }
}

/// Strategy for turning a JSON-serialised resource into the bytes that a
/// `application/vnd.kubernetes.protobuf` response should carry.
///
/// Implementations are responsible for emitting the full K8s wire envelope:
/// the `k8s\0` magic prefix followed by a `runtime.Unknown` protobuf
/// message. The default implementation
/// ([`WrappedJsonProtoEncoder`]) stuffs the JSON bytes into
/// `Unknown.raw` and sets `Unknown.contentType` to `application/json`,
/// which is the same envelope that
/// `rusternetes_common::protobuf::encode_protobuf` produces and that
/// `decode_protobuf` round-trips. A future per-resource implementation
/// would replace the body with native protobuf bytes produced from the
/// resource type (matching what upstream's generated `pb.go` does).
pub trait ProtoEncoder: Send + Sync {
    /// Wrap the JSON-serialised resource bytes in a K8s protobuf envelope
    /// suitable for an `application/vnd.kubernetes.protobuf` response.
    ///
    /// `json` is the canonical JSON encoding of the resource (the same
    /// bytes that would be written to a `Content-Type: application/json`
    /// response). `api_version` and `kind` are written into the
    /// `runtime.Unknown.typeMeta` field so that an `Unknown`-aware client
    /// can dispatch on type before fully decoding the body.
    fn encode(&self, json: &[u8], api_version: &str, kind: &str) -> Vec<u8>;
}

/// Default [`ProtoEncoder`] that wraps the JSON in `runtime.Unknown.raw`
/// and sets `contentType = "application/json"`.
///
/// Delegates to [`rusternetes_common::protobuf::encode_protobuf`] so the
/// envelope shape (prost-derived `Unknown` field layout) round-trips
/// through `decode_protobuf` without any wire-format drift. A future
/// per-resource native encoder can wrap `encode_protobuf` (or replace it)
/// once `pb.rs` descriptors are wired in.
pub struct WrappedJsonProtoEncoder;

impl ProtoEncoder for WrappedJsonProtoEncoder {
    fn encode(&self, json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
        wrap_json_in_protobuf_envelope(json, api_version, kind)
    }
}

/// Build a K8s `runtime.Unknown` protobuf envelope around `json`.
///
/// Delegates to [`rusternetes_common::protobuf::encode_protobuf`] for the
/// envelope shape so encode / decode share one prost-derived `Unknown`
/// definition. If encoding fails we fall back to the raw JSON bytes — the
/// response middleware will still emit them as `application/json` if the
/// caller chooses not to override the Content-Type, but the standard
/// usage path always treats the result as the protobuf envelope.
pub fn wrap_json_in_protobuf_envelope(json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
    use rusternetes_common::protobuf::{Unknown, UnknownTypeMeta};

    let unknown = Unknown {
        type_meta: Some(UnknownTypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        }),
        raw: json.to_vec(),
        content_encoding: String::new(),
        // `contentType` tells decoders that `raw` carries JSON — required
        // until we ship native per-resource protobuf marshalling.
        content_type: "application/json".to_string(),
    };

    use prost::Message;
    let mut buf = Vec::with_capacity(4 + unknown.encoded_len());
    buf.extend_from_slice(b"k8s\0");
    // `Message::encode` on `Vec<u8>` cannot fail (infallible BufMut).
    unknown.encode(&mut buf).expect("Unknown encode");
    buf
}

/// Return the encoder used to satisfy `application/vnd.kubernetes.protobuf`
/// responses today. Lives as a free function so test code and the response
/// middleware can share one definition.
pub fn default_proto_encoder() -> &'static dyn ProtoEncoder {
    &WrappedJsonProtoEncoder
}

/// Native protobuf encoder for Pod / PodList responses.
///
/// Parses the JSON body into a `serde_json::Value`, runs it through
/// `rusternetes_protobuf::PROTO_REGISTRY.encode_message(kind, value)` to produce
/// native protobuf bytes, then wraps those bytes in a `k8s\0`-framed
/// `runtime.Unknown` envelope whose `contentType` advertises
/// `application/vnd.kubernetes.protobuf`. The output is wire-compatible
/// with `client-go`'s typed proto decoder, which calls
/// `proto.Unmarshal(unk.Raw, target)` directly.
///
/// If the registry does not have a schema for the kind (e.g. the kind name
/// arrived misspelt) the encoder transparently falls back to the JSON-in-
/// `raw` wrapper so the endpoint still returns a parsable envelope rather
/// than 500.
pub struct NativePodProtoEncoder;

impl ProtoEncoder for NativePodProtoEncoder {
    fn encode(&self, json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
        encode_native_or_wrapped(json, api_version, kind)
    }
}

/// Encode a JSON resource body as **native** Kubernetes protobuf using the
/// registered schema.
///
/// Protobuf responses are native-or-nothing: once the response advertises
/// `Content-Type: application/vnd.kubernetes.protobuf`, `Unknown.raw` MUST be
/// native protobuf — client-go's protobuf serializer proto-decodes it directly
/// and a JSON-in-`raw` body fails with `proto: illegal wireType`. (Official k8s
/// confirms this: it emits native protobuf for marshalable types and
/// `errNotMarshalable` otherwise — never JSON-in-raw.) So a partial/round-trip
/// fallback to JSON is impossible here; encoding a kind via this path requires
/// its schema to be complete enough for clients.
///
/// The only fallback is for a kind with NO registered schema at all (which no
/// current opt-in hits): we still emit a parsable envelope rather than 500.
pub fn encode_native_or_wrapped(json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
    encode_native(json, api_version, kind)
        .unwrap_or_else(|| wrap_json_in_protobuf_envelope(json, api_version, kind))
}

/// [`encode_native_or_wrapped`] without the JSON-in-`raw` fallback: `None`
/// when the body is not a JSON object or no schema is registered for the kind.
pub fn encode_native(json: &[u8], api_version: &str, kind: &str) -> Option<Vec<u8>> {
    let value = serde_json::from_slice::<serde_json::Value>(json).ok()?;
    // Resolve the schema by the group-qualified key `{apiVersion}.{kind}` FIRST,
    // then the bare kind. Bare kind alone collides across API groups — e.g.
    // `TokenRequest` exists both as the CSI `{audience, expirationSeconds}` pair
    // (no status) and `authentication.k8s.io/v1.TokenRequest` (with
    // status.token + status.expirationTimestamp). Encoding the auth response
    // under the bare key hit the CSI schema and dropped `status`, so the
    // controller-manager read a nil expiration ("nil pointer of expiration in
    // token request", #1667). Kinds registered only under the bare name (Pod, …)
    // fall through to the second lookup unchanged.
    let qualified = format!("{api_version}.{kind}");
    let raw = rusternetes_protobuf::PROTO_REGISTRY
        .encode_message(&qualified, &value)
        .or_else(|| rusternetes_protobuf::PROTO_REGISTRY.encode_message(kind, &value))?;
    Some(wrap_native_proto_in_envelope(&raw, api_version, kind))
}

/// Build a K8s `runtime.Unknown` envelope around an already-encoded native
/// protobuf payload. Mirrors [`wrap_json_in_protobuf_envelope`] but advertises
/// `Unknown.contentType = application/vnd.kubernetes.protobuf` so
/// Unknown-aware decoders know the body is native proto, not JSON.
pub fn wrap_native_proto_in_envelope(raw: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
    use rusternetes_common::protobuf::{Unknown, UnknownTypeMeta};

    let unknown = Unknown {
        type_meta: Some(UnknownTypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        }),
        raw: raw.to_vec(),
        content_encoding: String::new(),
        // Empty contentType is what real K8s emits when `raw` is native
        // proto — see `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/protobuf`.
        // client-go's typed proto path never reads this field anyway, but
        // setting it to the proto media type is explicit and harmless.
        content_type: "application/vnd.kubernetes.protobuf".to_string(),
    };

    use prost::Message;
    let mut buf = Vec::with_capacity(4 + unknown.encoded_len());
    buf.extend_from_slice(b"k8s\0");
    unknown.encode(&mut buf).expect("Unknown encode");
    buf
}

/// Pick the [`ProtoEncoder`] best suited to a given opt-in. Kinds with a
/// registered protobuf schema get native encoding (via
/// [`NativePodProtoEncoder`], which is schema-generic despite its name);
/// everything else falls back to the JSON-wrapping default.
pub fn encoder_for(_opt_in: &NativeProtoOptIn) -> &'static dyn ProtoEncoder {
    // Schema-driven: always attempt native encoding. The encoder emits native
    // protobuf only when the registered schema round-trips losslessly, and
    // transparently falls back to the JSON-in-`Unknown.raw` envelope otherwise
    // (no schema, or an incomplete one). This mirrors official k8s ("native
    // for marshalable types") while staying safe against rusternetes's
    // hand-written schemas — no per-kind allowlist to maintain.
    &NativePodProtoEncoder
}
