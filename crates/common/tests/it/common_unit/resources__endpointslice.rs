//! Unit tests moved from `src/resources/endpointslice.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod null_field_tests {
    use super::__parent::*;
    use rusternetes_common::resources::endpointslice::*;

    /// An api-server that holds an EndpointSlice with no endpoints serialises
    /// the field as an explicit `null`, not `[]`:
    ///
    /// ```json
    /// {"kind":"EndpointSlice","apiVersion":"discovery.k8s.io/v1","endpoints":null}
    /// ```
    ///
    /// `#[serde(default)]` alone does NOT cover that — it only fills in an
    /// absent key — so decoding failed. The EndpointSlice controller reads the
    /// existing slice to decide update-vs-create, and a decode failure is
    /// indistinguishable from "not found": it retried `create` every reconcile
    /// and logged `endpointslices.discovery.k8s.io "test" already exists`
    /// forever while the slice never got updated.
    #[test]
    fn endpoints_null_decodes_as_empty() {
        let slice: EndpointSlice = serde_json::from_str(
            r#"{
                "kind": "EndpointSlice",
                "apiVersion": "discovery.k8s.io/v1",
                "metadata": { "name": "test", "namespace": "sts-check" },
                "addressType": "IPv4",
                "endpoints": null,
                "ports": [{ "name": "web", "port": 80, "protocol": "TCP" }]
            }"#,
        )
        .expect("an explicit null endpoints list must decode, as it does in Go");

        assert!(slice.endpoints.is_empty());
        assert_eq!(slice.ports.len(), 1);
    }

    /// The same for a null ports list, and for both at once — the shape a
    /// freshly created, endpoint-less slice comes back as.
    #[test]
    fn both_lists_null_decode_as_empty() {
        let slice: EndpointSlice = serde_json::from_str(
            r#"{
                "kind": "EndpointSlice",
                "apiVersion": "discovery.k8s.io/v1",
                "metadata": { "name": "test", "namespace": "sts-check" },
                "addressType": "IPv4",
                "endpoints": null,
                "ports": null
            }"#,
        )
        .expect("null lists must decode");

        assert!(slice.endpoints.is_empty());
        assert!(slice.ports.is_empty());
    }
}
