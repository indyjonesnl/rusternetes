//! Unit tests moved from `src/controllers/attach_detach.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::csi_migration::{
        translate_in_tree_spec_to_csi, PluginManager, VolumeSpec,
    };
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::resources::csi::{
        CSIDriver, CSINode, VolumeAttachment, VolumeAttachmentSource, VolumeAttachmentSpec,
    };
    pub use rusternetes_common::resources::node::AttachedVolume;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::{
        PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeSpec,
    };
    pub use rusternetes_common::resources::{EventSource, EventType};
    pub use rusternetes_common::resources::{Node, PersistentVolume, PersistentVolumeClaim, Pod};
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::Error;
    pub use rusternetes_controller_manager::controllers::pv_binder::is_pod_terminated;
    pub use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage};
    pub use sha2::{Digest, Sha256};
    pub use std::collections::{BTreeSet, HashMap, HashSet};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tracing::{debug, info, warn};
}

mod tests {
    use rusternetes_common::csi_migration::PluginManager;
    use rusternetes_common::resources::csi::CSINode;
    use rusternetes_common::resources::csi::{CSIDriver, VolumeAttachment};
    use rusternetes_controller_manager::controllers::attach_detach::{
        migration_supported_on_node, multi_attach_messages, volume_is_attached,
        MIGRATED_PLUGINS_ANNOTATION_KEY,
    };

    fn csi_node(ann: Option<&str>) -> CSINode {
        let mut n: CSINode = serde_json::from_value(
            serde_json::json!({"metadata": {"name": "n"}, "spec": {"drivers": []}}),
        )
        .unwrap();
        if let Some(a) = ann {
            n.metadata.annotations =
                Some([(MIGRATED_PLUGINS_ANNOTATION_KEY.to_string(), a.to_string())].into());
        }
        n
    }

    /// `isCSIMigrationSupportedOnNode` (`attachdetach/util/util.go:292-346`):
    /// complete plugins need no CSINode, others consult the annotation, a
    /// missing CSINode is an error.
    #[test]
    fn migration_supported_on_node_matches_upstream() {
        let pm = PluginManager::new();
        let gce = "kubernetes.io/gce-pd";
        assert_eq!(migration_supported_on_node(&pm, gce, None), Ok(true));
        assert_eq!(migration_supported_on_node(&pm, "", None), Ok(false));
        // Portworx migration enabled but not complete: the CSINode decides.
        let px = "kubernetes.io/portworx-volume";
        let mut pm = PluginManager::new();
        pm.csi_migration_portworx = true;
        assert!(migration_supported_on_node(&pm, px, None).is_err());
        assert_eq!(
            migration_supported_on_node(&pm, px, Some(&csi_node(None))),
            Ok(false)
        );
        assert_eq!(
            migration_supported_on_node(
                &pm,
                px,
                Some(&csi_node(Some("a,kubernetes.io/portworx-volume")))
            ),
            Ok(true)
        );
        assert_eq!(
            migration_supported_on_node(&pm, px, Some(&csi_node(Some("kubernetes.io/aws-ebs")))),
            Ok(false)
        );
    }

    fn va(attached: Option<bool>) -> VolumeAttachment {
        let mut v = serde_json::json!({"metadata": {"name": "a"},
            "spec": {"attacher": "d", "nodeName": "n", "source": {"persistentVolumeName": "pv"}}});
        if let Some(a) = attached {
            v["status"] = serde_json::json!({"attached": a});
        }
        serde_json::from_value(v).unwrap()
    }

    fn driver(attach_required: Option<bool>) -> CSIDriver {
        let mut spec = serde_json::json!({});
        if let Some(a) = attach_required {
            spec["attachRequired"] = a.into();
        }
        serde_json::from_value(serde_json::json!({"metadata": {"name": "d"}, "spec": spec}))
            .unwrap()
    }

    /// `TestAttacherVolumesAreAttached` (`csi_attacher_test.go:652`): the
    /// attachment's own status; a missing attachment and an absent status are
    /// not attached; `attachRequired: false` is pretended attached.
    #[test]
    fn volume_is_attached_matches_csi_attacher() {
        assert!(volume_is_attached(None, Some(&va(Some(true)))));
        assert!(!volume_is_attached(None, Some(&va(Some(false)))));
        assert!(!volume_is_attached(None, Some(&va(None))));
        assert!(!volume_is_attached(None, None));
        // CSIDriver without attachRequired, or true: attach is required.
        assert!(!volume_is_attached(
            Some(&driver(None)),
            Some(&va(Some(false)))
        ));
        assert!(!volume_is_attached(Some(&driver(Some(true))), None));
        assert!(volume_is_attached(Some(&driver(Some(false))), None));
        assert!(volume_is_attached(
            Some(&driver(Some(false))),
            Some(&va(Some(false)))
        ));
    }

    /// `Test_ReportMultiAttachError` (`reconciler_test.go:1279`): ns1/pod1 is
    /// the pod being attached, `others` use the volume on other nodes.
    fn run(others: &[(&str, &str)]) -> Vec<String> {
        let holders: Vec<(String, String)> = others
            .iter()
            .map(|(n, p)| (n.to_string(), p.to_string()))
            .collect();
        multi_attach_messages("volume-name", &[("ns1".into(), "pod1".into())], &holders)
    }

    const PFX: &str = "Multi-Attach error for volume \"volume-name\" ";

    #[test]
    fn no_pods_use_the_volume() {
        assert_eq!(
            run(&[]),
            vec![format!("{PFX}Volume is already exclusively attached to one node and can't be attached to another")]
        );
    }

    #[test]
    fn pods_in_the_same_namespace() {
        assert_eq!(
            run(&[("ns1", "pod2")]),
            vec![format!("{PFX}Volume is already used by pod(s) pod2")]
        );
    }

    #[test]
    fn pods_in_another_namespace() {
        assert_eq!(
            run(&[("ns2", "pod2")]),
            vec![format!(
                "{PFX}Volume is already used by 1 pod(s) in different namespaces"
            )]
        );
    }

    #[test]
    fn pods_in_same_and_another_namespace() {
        assert_eq!(
            run(&[("ns2", "pod2"), ("ns1", "pod3")]),
            vec![format!(
                "{PFX}Volume is already used by pod(s) pod3 and 1 pod(s) in different namespaces"
            )]
        );
    }
}
