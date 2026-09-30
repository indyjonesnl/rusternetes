//! `RangeAllocation` (`staging/src/k8s.io/api/core/v1/types.go:8325-8340`):
//! the persisted snapshot of a range allocator — the service NodePort
//! bitmap. It is not served by the API; the api-server keeps it in storage
//! under `/registry/ranges/`.

use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeAllocation {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// The range `data` covers, e.g. `30000-32767`.
    #[serde(default)]
    pub range: String,
    /// The allocation bit array, base64 in JSON as Go encodes `[]byte`.
    #[serde(
        default,
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub data: Vec<u8>,
}

fn serialize_bytes<S: Serializer>(data: &[u8], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        data,
    ))
}

fn deserialize_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    // Go marshals a nil `[]byte` as `null`.
    let Some(encoded) = Option::<String>::deserialize(d)? else {
        return Ok(Vec::new());
    };
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_round_trips_as_base64() {
        let r = RangeAllocation {
            range: "30000-32767".to_string(),
            data: vec![0x01, 0x80],
            ..Default::default()
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["data"], "AYA=");
        assert_eq!(json["range"], "30000-32767");
        let back: RangeAllocation = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
        let nil: RangeAllocation =
            serde_json::from_value(serde_json::json!({"range": "", "data": null})).unwrap();
        assert!(nil.data.is_empty());
    }
}
