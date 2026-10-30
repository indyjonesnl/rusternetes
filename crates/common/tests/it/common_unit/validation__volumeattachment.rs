//! Unit tests moved from `src/validation/volumeattachment.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::csi::{
        VolumeAttachment, VolumeAttachmentStatus, VolumeError,
    };
    pub use rusternetes_common::validation::csinode::validate_csi_driver_name;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_subdomain;
    pub use rusternetes_common::validation::persistentvolume::validate_persistent_volume_spec;
}

mod inline_spec_tests {
    use super::__parent::*;
    use rusternetes_common::validation::volumeattachment::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let va: VolumeAttachment = serde_json::from_value(json).unwrap();
        validate_volume_attachment(&va)
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    // Build a VolumeAttachment whose inlineVolumeSpec is `spec` (merged into a
    // minimal valid inline spec base when used by the per-delta tests).
    fn inline(spec: serde_json::Value) -> Vec<String> {
        errs(serde_json::json!({
            "metadata": {"name": "va"},
            "spec": {
                "attacher": "csi.example.com",
                "nodeName": "node-1",
                "source": {"inlineVolumeSpec": spec}
            }
        }))
    }

    #[test]
    fn valid_inline_csi_spec_passes() {
        // A valid inline spec has NO capacity (forbidden inline), a CSI source,
        // accessModes, and otherwise no standalone-PV-only fields.
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1"}
        }));
        assert!(e.iter().all(|m| !m.contains("inlineVolumeSpec")), "{e:?}");
    }

    #[test]
    fn invalid_inline_spec_is_validated() {
        // empty inline spec: inline validation flags the missing CSI source.
        let e = inline(serde_json::json!({}));
        assert!(
            e.iter().any(|m| m.contains("inlineVolumeSpec")),
            "expected inline spec errors, got {e:?}"
        );
    }

    #[test]
    fn inline_capacity_forbidden() {
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "capacity": {"storage": "1Gi"}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("capacity") && m.contains("inline")),
            "{e:?}"
        );
    }

    #[test]
    fn inline_csi_required() {
        let e = inline(serde_json::json!({ "accessModes": ["ReadWriteOnce"] }));
        assert!(
            e.iter().any(|m| m.contains("csi") && m.contains("inline")),
            "{e:?}"
        );
    }

    #[test]
    fn inline_claim_ref_forbidden() {
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "claimRef": {"name": "pvc-1", "namespace": "default"}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("claimRef") && m.contains("inline")),
            "{e:?}"
        );
    }

    #[test]
    fn inline_reclaim_policy_must_be_retain() {
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "persistentVolumeReclaimPolicy": "Delete"
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("persistentVolumeReclaimPolicy") && m.contains("Retain")),
            "{e:?}"
        );
        // Retain passes.
        let ok = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "persistentVolumeReclaimPolicy": "Retain"
        }));
        assert!(
            !ok.iter()
                .any(|m| m.contains("persistentVolumeReclaimPolicy")),
            "{ok:?}"
        );
    }

    #[test]
    fn inline_node_affinity_forbidden() {
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "nodeAffinity": {"required": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": "k", "operator": "Exists"}]}
            ]}}
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("nodeAffinity") && m.contains("inline")),
            "{e:?}"
        );
    }

    #[test]
    fn inline_volume_mode_must_be_filesystem() {
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "volumeMode": "Block"
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("volumeMode") && m.contains("Filesystem")),
            "{e:?}"
        );
    }

    #[test]
    fn inline_storage_class_name_forbidden() {
        let e = inline(serde_json::json!({
            "accessModes": ["ReadWriteOnce"],
            "csi": {"driver": "d", "volumeHandle": "v"},
            "storageClassName": "standard"
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("storageClassName") && m.contains("inline")),
            "{e:?}"
        );
    }
}

mod parity_tests {
    use super::__parent::*;
    use rusternetes_common::validation::volumeattachment::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let va: VolumeAttachment = serde_json::from_value(json).unwrap();
        // A create runs `ValidateVolumeAttachment` then `ValidateVolumeAttachmentV1`.
        let mut all = validate_volume_attachment(&va);
        all.extend(validate_volume_attachment_v1(&va));
        all.into_iter().map(|e| e.to_string()).collect()
    }

    /// Base PV-name source: a fully valid VA we mutate per test.
    fn va(attacher: &str, pv_name: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "va"},
            "spec": {
                "attacher": attacher,
                "nodeName": "node-1",
                "source": {"persistentVolumeName": pv_name}
            }
        })
    }

    // --- spec.attacher: valid CSI driver name (DNS-subdomain ≤63) ---

    #[test]
    fn valid_attacher_passes() {
        let e = errs(va("csi.example.com", "pv-1"));
        assert!(e.is_empty(), "expected no errors, got {e:?}");
    }

    #[test]
    fn attacher_with_invalid_chars_is_rejected() {
        let e = errs(va("Bad Attacher!", "pv-1"));
        // uppercase is lowercased by upstream, but the space / '!' are invalid.
        assert!(
            e.iter().any(|m| m.contains("spec.attacher")),
            "expected attacher format error, got {e:?}"
        );
    }

    #[test]
    fn attacher_over_63_chars_is_too_long() {
        let long = format!("{}.example.com", "a".repeat(60));
        let e = errs(va(&long, "pv-1"));
        assert!(
            e.iter()
                .any(|m| m.contains("spec.attacher") && m.contains("more than 63")),
            "expected too-long attacher error, got {e:?}"
        );
    }

    #[test]
    fn empty_attacher_is_required() {
        let e = errs(va("", "pv-1"));
        assert!(
            e.iter()
                .any(|m| m.contains("spec.attacher") && m.contains("Required")
                    || m.contains("spec.attacher") && m.contains("required")),
            "expected required attacher error, got {e:?}"
        );
    }

    // --- source.persistentVolumeName: valid PV name (DNS-subdomain) ---

    #[test]
    fn invalid_pv_name_is_rejected() {
        let e = errs(va("csi.example.com", "Invalid_PV_Name"));
        assert!(
            e.iter()
                .any(|m| m.contains("spec.source.persistentVolumeName")),
            "expected pv-name format error, got {e:?}"
        );
    }

    #[test]
    fn empty_pv_name_is_required_not_format() {
        let e = errs(va("csi.example.com", ""));
        assert!(
            e.iter()
                .any(|m| m.contains("persistentVolumeName") && m.contains("non empty")),
            "expected non-empty pv-name error, got {e:?}"
        );
    }

    // --- status validation ---

    fn va_with_status(status: serde_json::Value) -> serde_json::Value {
        let mut base = va("csi.example.com", "pv-1");
        base["status"] = status;
        base
    }

    #[test]
    fn small_attachment_metadata_passes() {
        let e = errs(va_with_status(serde_json::json!({
            "attached": true,
            "attachmentMetadata": {"key": "value"}
        })));
        assert!(
            e.iter().all(|m| !m.contains("attachmentMetadata")),
            "expected no metadata error, got {e:?}"
        );
    }

    #[test]
    fn oversized_attachment_metadata_is_too_long() {
        let big = "x".repeat(256 * 1024 + 1);
        let e = errs(va_with_status(serde_json::json!({
            "attached": true,
            "attachmentMetadata": {"k": big}
        })));
        assert!(
            e.iter()
                .any(|m| m.contains("status.attachmentMetadata") && m.contains("more than")),
            "expected too-long metadata error, got {e:?}"
        );
    }

    #[test]
    fn oversized_attach_error_message_is_too_long() {
        let msg = "e".repeat(1025);
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "attachError": {"message": msg}
        })));
        assert!(
            e.iter()
                .any(|m| m.contains("status.attachError.message") && m.contains("more than")),
            "expected too-long attachError message, got {e:?}"
        );
    }

    #[test]
    fn ok_detach_error_message_passes() {
        let msg = "e".repeat(1024);
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "detachError": {"message": msg}
        })));
        assert!(
            e.iter().all(|m| !m.contains("detachError")),
            "expected no detachError error, got {e:?}"
        );
    }

    #[test]
    fn negative_error_code_is_rejected() {
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "attachError": {"message": "boom", "errorCode": -5}
        })));
        assert!(
            e.iter().any(|m| m.contains("status.attachError.errorCode")
                && m.contains("between 0 and 2147483647")),
            "expected negative errorCode error, got {e:?}"
        );
    }

    #[test]
    fn nonnegative_error_code_passes() {
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "detachError": {"message": "boom", "errorCode": 12}
        })));
        assert!(
            e.iter().all(|m| !m.contains("errorCode")),
            "expected no errorCode error, got {e:?}"
        );
    }
}
