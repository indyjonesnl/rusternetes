/// Kubernetes Protobuf serialization support
///
/// This module implements protobuf encoding/decoding for Kubernetes API objects
/// following the format used by k8s.io/apimachinery/pkg/runtime/serializer/protobuf.
///
/// Key aspects:
/// - Uses a magic number prefix (0x6b 0x38 0x73 = "k8s") to identify protobuf messages
/// - Wraps objects with metadata (apiVersion, kind, etc.) using the Unknown wrapper
/// - Supports both "protobuf" (with type wrapper) and "raw-protobuf" (without wrapper)
use prost::Message;
use serde::{Deserialize, Serialize};

/// Magic number that prefixes all Kubernetes protobuf messages: "k8s" in ASCII
pub const PROTOBUF_MAGIC: &[u8] = &[0x6b, 0x38, 0x73, 0x00];

/// Content types for protobuf serialization
pub const CONTENT_TYPE_PROTOBUF: &str = "application/vnd.kubernetes.protobuf";
pub const CONTENT_TYPE_PROTOBUF_STREAM: &str = "application/vnd.kubernetes.protobuf;stream=watch";

/// TypeMeta as a nested protobuf message. Sits inside `Unknown.typeMeta`
/// at field 1; itself has `apiVersion=1`, `kind=2`. Mirrors
/// `k8s.io/apimachinery/pkg/runtime/generated.proto::TypeMeta`.
#[derive(Clone, PartialEq, Message)]
pub struct UnknownTypeMeta {
    #[prost(string, tag = "1")]
    pub api_version: String,

    #[prost(string, tag = "2")]
    pub kind: String,
}

/// Unknown wraps objects with type metadata for protobuf serialization.
///
/// Mirrors `k8s.io/apimachinery/pkg/runtime.Unknown` exactly:
///
/// ```text
/// message Unknown {
///   optional TypeMeta typeMeta = 1;
///   optional bytes raw = 2;
///   optional string contentEncoding = 3;
///   optional string contentType = 4;
/// }
/// ```
///
/// Before 2026-05-24 this struct flattened TypeMeta in at tags 1-2 and
/// shifted the other fields to 3-5. Encoder and our own decoder agreed,
/// so internal round-trips passed — but any upstream client (kubectl,
/// hydrophone, client-go) decoding the envelope hit
/// `proto: illegal wireType 6` reading the apiVersion bytes as a nested
/// TypeMeta message (run 26360429763, post-#766 Pod CREATE).
#[derive(Clone, PartialEq, Message)]
pub struct Unknown {
    #[prost(message, optional, tag = "1")]
    pub type_meta: Option<UnknownTypeMeta>,

    #[prost(bytes, tag = "2")]
    pub raw: Vec<u8>,

    #[prost(string, tag = "3")]
    pub content_encoding: String,

    #[prost(string, tag = "4")]
    pub content_type: String,
}

/// TypeMeta describes an individual object in an API response
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeMeta {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
}

impl TypeMeta {
    pub fn new(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
        }
    }
}

/// Encode a Kubernetes API object to protobuf format
///
/// This function:
/// 1. Serializes the object to JSON (since we don't have generated protobuf schemas)
/// 2. Wraps it in an Unknown message with type metadata
/// 3. Encodes the Unknown message to protobuf
/// 4. Prefixes with the magic number
///
/// Note: In a full implementation, objects would be serialized directly to protobuf
/// using generated schemas. For conformance testing, wrapping JSON in protobuf
/// messages with the correct magic number is sufficient.
pub fn encode_protobuf<T: Serialize>(
    obj: &T,
    api_version: &str,
    kind: &str,
) -> Result<Vec<u8>, String> {
    // Serialize the object to JSON
    // In a full implementation, this would use generated protobuf schemas
    let json_bytes =
        serde_json::to_vec(obj).map_err(|e| format!("Failed to serialize to JSON: {}", e))?;

    // Create Unknown wrapper with type metadata
    let unknown = Unknown {
        type_meta: Some(UnknownTypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        }),
        raw: json_bytes,
        content_encoding: String::new(),
        content_type: "application/json".to_string(), // Indicates raw contains JSON
    };

    // Encode Unknown to protobuf
    let mut buf = Vec::with_capacity(PROTOBUF_MAGIC.len() + unknown.encoded_len());
    buf.extend_from_slice(PROTOBUF_MAGIC);

    unknown
        .encode(&mut buf)
        .map_err(|e| format!("Failed to encode protobuf: {}", e))?;

    Ok(buf)
}

/// Decode a Kubernetes API object from protobuf format
///
/// This function:
/// 1. Verifies the magic number prefix
/// 2. Decodes the Unknown wrapper
/// 3. Deserializes the object from the raw bytes (JSON in our case)
pub fn decode_protobuf<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<(T, TypeMeta), String> {
    // Verify magic number
    if data.len() < PROTOBUF_MAGIC.len() {
        return Err("Data too short to contain magic number".to_string());
    }

    if &data[0..PROTOBUF_MAGIC.len()] != PROTOBUF_MAGIC {
        return Err("Invalid magic number".to_string());
    }

    // Decode Unknown wrapper
    let unknown = Unknown::decode(&data[PROTOBUF_MAGIC.len()..])
        .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

    // Extract type metadata from the nested message (defaults if absent —
    // matches upstream Unknown decode which treats typeMeta as optional).
    let type_meta = unknown
        .type_meta
        .as_ref()
        .map(|tm| TypeMeta {
            api_version: tm.api_version.clone(),
            kind: tm.kind.clone(),
        })
        .unwrap_or_else(|| TypeMeta {
            api_version: String::new(),
            kind: String::new(),
        });

    // Deserialize the object from raw bytes
    // In our implementation, raw contains JSON
    let obj: T = serde_json::from_slice(&unknown.raw)
        .map_err(|e| format!("Failed to deserialize from JSON: {}", e))?;

    Ok((obj, type_meta))
}

/// Decode a k8s `Unknown` envelope into its `TypeMeta` and the raw inner
/// payload bytes, WITHOUT assuming the payload is JSON.
///
/// [`decode_protobuf`] deserializes `Unknown.raw` as JSON, which only holds for
/// kinds with no registered protobuf schema. For schema-registered kinds (Scale,
/// Pod, …) the api-server now emits NATIVE protobuf in `raw` — decode those via
/// the proto registry. This helper exposes the envelope split so callers choose
/// the right inner decoder.
pub fn decode_unknown_raw(data: &[u8]) -> Result<(TypeMeta, Vec<u8>), String> {
    if data.len() < PROTOBUF_MAGIC.len() || &data[0..PROTOBUF_MAGIC.len()] != PROTOBUF_MAGIC {
        return Err("Invalid magic number".to_string());
    }

    let unknown = Unknown::decode(&data[PROTOBUF_MAGIC.len()..])
        .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

    let type_meta = unknown
        .type_meta
        .as_ref()
        .map(|tm| TypeMeta {
            api_version: tm.api_version.clone(),
            kind: tm.kind.clone(),
        })
        .unwrap_or_else(|| TypeMeta {
            api_version: String::new(),
            kind: String::new(),
        });

    Ok((type_meta, unknown.raw))
}

/// Check if data appears to be protobuf-encoded (has magic number)
pub fn is_protobuf(data: &[u8]) -> bool {
    data.len() >= PROTOBUF_MAGIC.len() && &data[0..PROTOBUF_MAGIC.len()] == PROTOBUF_MAGIC
}

/// Extract TypeMeta from protobuf data without full deserialization
pub fn extract_type_meta(data: &[u8]) -> Result<TypeMeta, String> {
    if !is_protobuf(data) {
        return Err("Not protobuf data".to_string());
    }

    let unknown = Unknown::decode(&data[PROTOBUF_MAGIC.len()..])
        .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

    Ok(unknown
        .type_meta
        .map(|tm| TypeMeta {
            api_version: tm.api_version,
            kind: tm.kind,
        })
        .unwrap_or_else(|| TypeMeta {
            api_version: String::new(),
            kind: String::new(),
        }))
}
