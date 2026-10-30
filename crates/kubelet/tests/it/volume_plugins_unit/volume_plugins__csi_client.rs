//! Unit tests moved from `src/volume_plugins/csi_client.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use hyper_util::rt::TokioIo;
    pub use proto::node_client::NodeClient;
    pub use proto::node_service_capability::rpc::Type as NodeRpcType;
    pub use proto::volume_capability::access_mode::Mode as AccessModeKind;
    pub use proto::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
    pub use proto::{
        CapacityRange, NodeExpandVolumeRequest, NodeGetCapabilitiesRequest, NodeGetInfoRequest,
        NodeGetVolumeStatsRequest, NodePublishVolumeRequest, NodeStageVolumeRequest,
        NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest, VolumeCapability,
    };
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::PersistentVolumeAccessMode;
    pub use rusternetes_csi::normalize_endpoint;
    pub use rusternetes_csi::proto;
    pub use rusternetes_kubelet::volume_plugins::csi_drivers_store::csi_drivers;
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, Spec};
    pub use rusternetes_kubelet::volume_plugins::util::metrics::record_csi_operation_latency_metrics;
    pub use std::collections::HashMap;
    pub use std::path::PathBuf;
    pub use std::time::Duration;
    pub use tonic::transport::{Channel, Endpoint, Uri};
    pub use tower::service_fn;
}

mod tests {
    use super::__parent::*;
    use crate::csi_fake::*;
    use rusternetes_common::quantity::Format;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::{NodeGetInfoResponse, Topology};
    use rusternetes_kubelet::volume_plugins::csi_client::*;

    fn caps(c: &[NodeRpcType]) -> FakeDriver {
        FakeDriver::with_capabilities(c)
    }

    /// `TestClientNodeGetInfo` (`csi_client_test.go`): `NodeGetInfo` returns the
    /// driver's node id, its max volumes per node and the topology segments
    /// (`csi_client.go:172-209`, `nodeGetInfoV1`).
    #[tokio::test]
    async fn node_get_info_returns_id_limit_and_topology_segments() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        *d.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
            node_id: "com.example.csi/csi-node1".into(),
            max_volumes_per_node: 10,
            accessible_topology: Some(Topology {
                segments: HashMap::from([("com.example.csi/zone".into(), "zoneA".into())]),
            }),
        }));
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let info = c.node_get_info().await.unwrap();
        assert_eq!(info.node_id, "com.example.csi/csi-node1");
        assert_eq!(info.max_volumes_per_node, 10);
        assert_eq!(info.accessible_topology["com.example.csi/zone"], "zoneA");
        assert_eq!(d.calls.lock().unwrap().node_get_info_calls, 1);
    }

    /// No `accessible_topology` in the response: upstream returns a nil map
    /// (`csi_client.go:203-206`).
    #[tokio::test]
    async fn node_get_info_without_topology_is_empty_and_errors_propagate() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        *d.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
            node_id: "n".into(),
            ..Default::default()
        }));
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        assert!(c
            .node_get_info()
            .await
            .unwrap()
            .accessible_topology
            .is_empty());

        *d.node_info.lock().unwrap() = Some(Err(tonic::Code::Unavailable));
        assert!(c.node_get_info().await.is_err());
    }

    /// `TestClientNodePublishVolume` (`csi_client_test.go`): the request carries
    /// volume id, target path, readonly, contexts, secrets, fs type and flags.
    #[tokio::test]
    async fn node_publish_volume_builds_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let target = dir.path().join("target");
        c.node_publish_volume(
            "vol-1",
            true,
            "/stage",
            target.to_str().unwrap(),
            &PersistentVolumeAccessMode::ReadWriteOnce,
            HashMap::from([("pc".into(), "1".into())]),
            HashMap::from([("vc".into(), "2".into())]),
            HashMap::from([("s".into(), "3".into())]),
            "ext4",
            &["noatime".to_string()],
            Some(1000),
        )
        .await
        .unwrap();

        let calls = d.calls.lock().unwrap();
        let req = &calls.publish[0];
        assert_eq!(req.volume_id, "vol-1");
        assert_eq!(req.target_path, target.to_str().unwrap());
        assert!(req.readonly);
        assert_eq!(req.staging_target_path, "/stage");
        assert_eq!(req.publish_context["pc"], "1");
        assert_eq!(req.volume_context["vc"], "2");
        assert_eq!(req.secrets["s"], "3");
        let cap = req.volume_capability.as_ref().unwrap();
        assert_eq!(
            cap.access_mode.as_ref().unwrap().mode,
            AccessModeKind::SingleNodeWriter as i32
        );
        match cap.access_type.as_ref().unwrap() {
            AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime"]);
                assert_eq!(m.volume_mount_group, "1000");
            }
            other => panic!("expected mount access type, got {other:?}"),
        }
    }

    /// `fsType == "block"` selects the block access type (`csi_client.go:259`).
    #[tokio::test]
    async fn node_publish_volume_block_fs_type_is_block_access() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        c.node_publish_volume(
            "v",
            false,
            "",
            dir.path().join("t").to_str().unwrap(),
            &PersistentVolumeAccessMode::ReadWriteOnce,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            "block",
            &[],
            None,
        )
        .await
        .unwrap();
        let calls = d.calls.lock().unwrap();
        assert!(matches!(
            calls.publish[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_type,
            Some(AccessType::Block(_))
        ));
    }

    /// Missing volume id / target path are rejected before any RPC
    /// (`csi_client.go:224-229`).
    #[tokio::test]
    async fn node_publish_volume_validates_arguments() {
        let c = CsiDriverClient::with_endpoint("drv", "/nonexistent.sock");
        let e = c
            .node_publish_volume(
                "",
                false,
                "",
                "/t",
                &PersistentVolumeAccessMode::ReadWriteOnce,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                "",
                &[],
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "missing volume id");
        let e = c
            .node_publish_volume(
                "v",
                false,
                "",
                "",
                &PersistentVolumeAccessMode::ReadWriteOnce,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                "",
                &[],
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "missing target path");
    }

    /// A driver advertising SINGLE_NODE_MULTI_WRITER gets the capable mapper
    /// (`getNodeV1AccessModeMapper`, `csi_client.go:490`).
    #[tokio::test]
    async fn access_mode_mapper_follows_driver_capability() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[NodeRpcType::SingleNodeMultiWriter]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        c.node_publish_volume(
            "v",
            false,
            "",
            dir.path().join("t").to_str().unwrap(),
            &PersistentVolumeAccessMode::ReadWriteOncePod,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            "ext4",
            &[],
            None,
        )
        .await
        .unwrap();
        let calls = d.calls.lock().unwrap();
        assert_eq!(
            calls.publish[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_mode
                .as_ref()
                .unwrap()
                .mode,
            AccessModeKind::SingleNodeSingleWriter as i32
        );
    }

    /// `isFinalError` table (`csi_client_test.go::TestIsFinalError`): a
    /// non-final publish error is uncertain-progress, a final one is not, and
    /// only a final error is an "operation finished" error.
    #[tokio::test]
    async fn publish_error_classification() {
        for (code, finished) in [
            (tonic::Code::InvalidArgument, true),
            (tonic::Code::NotFound, true),
            (tonic::Code::Internal, true),
            (tonic::Code::Aborted, false),
            (tonic::Code::Unavailable, false),
            (tonic::Code::DeadlineExceeded, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let sock = dir.path().join("csi.sock");
            let d = caps(&[]);
            *d.publish_error.lock().unwrap() = Some(code);
            let _srv = serve(d, &sock);
            let c = CsiDriverClient::with_endpoint("drv", &sock);
            let e = c
                .node_publish_volume(
                    "v",
                    false,
                    "",
                    dir.path().join("t").to_str().unwrap(),
                    &PersistentVolumeAccessMode::ReadWriteOnce,
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    "ext4",
                    &[],
                    None,
                )
                .await
                .unwrap_err();
            assert_eq!(e.is_operation_finished(), finished, "{code:?}");
            assert_eq!(
                matches!(e, CsiError::UncertainProgress(_)),
                !finished,
                "{code:?}"
            );
        }
    }

    #[tokio::test]
    async fn stage_unstage_unpublish_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[NodeRpcType::StageUnstageVolume]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        assert!(c.node_supports_stage_unstage().await.unwrap());
        assert!(!c.node_supports_volume_mount_group().await.unwrap());

        c.node_stage_volume(
            "v",
            HashMap::new(),
            "/stage",
            "xfs",
            &PersistentVolumeAccessMode::ReadWriteMany,
            HashMap::from([("k".into(), "v".into())]),
            HashMap::new(),
            &["ro".to_string()],
            None,
        )
        .await
        .unwrap();
        c.node_unstage_volume("v", "/stage").await.unwrap();
        c.node_unpublish_volume("v", "/target").await.unwrap();

        let calls = d.calls.lock().unwrap();
        assert_eq!(calls.stage[0].staging_target_path, "/stage");
        assert_eq!(calls.stage[0].secrets["k"], "v");
        assert_eq!(
            calls.stage[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_mode
                .as_ref()
                .unwrap()
                .mode,
            AccessModeKind::MultiNodeMultiWriter as i32
        );
        assert_eq!(calls.unstage[0].staging_target_path, "/stage");
        assert_eq!(calls.unpublish[0].target_path, "/target");
    }

    fn resize_opts() -> CsiResizeOptions {
        CsiResizeOptions {
            volume_id: "vol-abcde".into(),
            volume_path: "/foo/bar".into(),
            staging_target_path: String::new(),
            fs_type: "ext4".into(),
            access_mode: PersistentVolumeAccessMode::ReadWriteOnce,
            new_size: Quantity::parse("10Gi").unwrap(),
            mount_options: vec!["noatime".into()],
            secrets: HashMap::new(),
        }
    }

    /// `TestClientNodeSupportsNodeExpand` (`csi_client_test.go:712`):
    /// `NodeSupportsNodeExpand` is the `EXPAND_VOLUME` capability.
    #[tokio::test]
    async fn node_supports_node_expand_follows_capability() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let _srv = serve(caps(&[NodeRpcType::StageUnstageVolume]), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        assert!(!c.node_supports_node_expand().await.unwrap());

        let sock2 = dir.path().join("csi2.sock");
        let _srv2 = serve(caps(&[NodeRpcType::ExpandVolume]), &sock2);
        let c2 = CsiDriverClient::with_endpoint("drv", &sock2);
        assert!(c2.node_supports_node_expand().await.unwrap());
    }

    /// `TestNodeExpandVolume` (`csi_client_test.go:776`) "with all correct
    /// values", plus the request shape `NodeExpandVolume` builds
    /// (`csi_client.go:316-345`).
    #[tokio::test]
    async fn node_expand_volume_builds_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let mut o = resize_opts();
        o.staging_target_path = "/stage".into();
        o.secrets = HashMap::from([("k".into(), "v".into())]);
        let got = c.node_expand_volume(&o).await.unwrap();
        // The returned quantity is the driver's `CapacityBytes`.
        assert_eq!(got.value(), 10 * 1024 * 1024 * 1024);

        let calls = d.calls.lock().unwrap();
        let r = &calls.expand[0];
        assert_eq!(r.volume_id, "vol-abcde");
        assert_eq!(r.volume_path, "/foo/bar");
        assert_eq!(r.staging_target_path, "/stage");
        assert_eq!(r.capacity_range.as_ref().unwrap().required_bytes, 10 << 30);
        assert_eq!(r.secrets["k"], "v");
        let cap = r.volume_capability.as_ref().unwrap();
        assert_eq!(
            cap.access_mode.as_ref().unwrap().mode,
            AccessModeKind::SingleNodeWriter as i32
        );
        match cap.access_type.as_ref().unwrap() {
            AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
            }
            other => panic!("expected mount access type, got {other:?}"),
        }
    }

    /// `fsType == "block"` selects the block access type
    /// (`csi_client.go:333-343`), and an unset staging path stays unset
    /// (`csi_client.go:326-330`).
    #[tokio::test]
    async fn node_expand_volume_block_access_type() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        let mut o = resize_opts();
        o.fs_type = FS_TYPE_BLOCK_NAME.into();
        c.node_expand_volume(&o).await.unwrap();
        let calls = d.calls.lock().unwrap();
        let r = &calls.expand[0];
        assert!(r.staging_target_path.is_empty());
        assert!(matches!(
            r.volume_capability.as_ref().unwrap().access_type,
            Some(AccessType::Block(_))
        ));
    }

    /// `TestNodeExpandVolume` failing rows: missing volume id, missing volume
    /// path, negative size. None reaches the driver.
    #[tokio::test]
    async fn node_expand_volume_validates_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let mut o = resize_opts();
        o.volume_id.clear();
        assert!(matches!(c.node_expand_volume(&o).await,
            Err(NodeExpandError::Failed(m)) if m == "missing volume id"));
        let mut o = resize_opts();
        o.volume_path.clear();
        assert!(matches!(c.node_expand_volume(&o).await,
            Err(NodeExpandError::Failed(m)) if m == "missing volume path"));
        let mut o = resize_opts();
        o.new_size = Quantity::from_value(-10, Format::DecimalSI);
        assert!(matches!(c.node_expand_volume(&o).await,
            Err(NodeExpandError::Failed(m)) if m == "size can not be less than 0"));
        assert!(d.calls.lock().unwrap().expand.is_empty());
    }

    /// `NodeExpandVolume`'s error branch (`csi_client.go:347-353`): a final
    /// gRPC error is returned as-is (so `inUseError`/`isInfeasibleError` can
    /// read its code); a non-final one is uncertain progress.
    #[tokio::test]
    async fn node_expand_volume_error_classification() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        *d.expand_error.lock().unwrap() = Some(tonic::Code::InvalidArgument);
        match c.node_expand_volume(&resize_opts()).await {
            Err(NodeExpandError::Grpc(s)) => assert_eq!(s.code(), tonic::Code::InvalidArgument),
            other => panic!("expected a final gRPC error, got {other:?}"),
        }
        *d.expand_error.lock().unwrap() = Some(tonic::Code::Unavailable);
        assert!(matches!(
            c.node_expand_volume(&resize_opts()).await,
            Err(NodeExpandError::UncertainProgress(_))
        ));
    }

    /// `newCsiDriverClient` (`csi_client.go:153-170`) for an unregistered driver.
    #[test]
    fn unregistered_driver_is_an_error() {
        let e = CsiDriverClient::new("never.registered.example.com")
            .err()
            .unwrap();
        assert_eq!(
            e.to_string(),
            "driver name never.registered.example.com not found in the list of registered CSI drivers"
        );
    }

    fn spec_with_migrated(
        migrated: bool,
    ) -> rusternetes_kubelet::volume_plugins::plugin::OwnedSpec {
        rusternetes_kubelet::volume_plugins::plugin::OwnedSpec {
            volume: serde_json::from_value(serde_json::json!({"name": "v"})).unwrap(),
            persistent_volume: None,
            read_only: false,
            migrated,
            inline_volume_spec_for_csi_migration: false,
        }
    }

    /// `TestCreateCSIOperationContext` (`csi_util_test.go:160-205`): nil spec
    /// and `Migrated: false` give "false", `Migrated: true` gives "true".
    #[test]
    fn create_csi_operation_context_migrated_follows_spec() {
        assert!(!csi_operation_migrated(None));
        let migrated = spec_with_migrated(true);
        assert!(csi_operation_migrated(Some(&migrated.as_spec())));
        let not_migrated = spec_with_migrated(false);
        assert!(!csi_operation_migrated(Some(&not_migrated.as_spec())));
        let c = CsiDriverClient::with_endpoint("drv", "/x")
            .with_operation_spec(Some(&migrated.as_spec()));
        assert!(c.migrated());
        assert!(!CsiDriverClient::with_endpoint("drv", "/x").migrated());
    }

    /// `RecordMetricsInterceptor` (`csi_metrics.go:112-136`): an RPC made by a
    /// client of a migrated operation is recorded with `migrated="true"`, a
    /// plain one with `"false"`.
    #[tokio::test]
    async fn rpc_metrics_carry_the_operations_migrated_label() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let _srv = serve(caps(&[NodeRpcType::StageUnstageVolume]), &sock);
        let migrated = spec_with_migrated(true);
        let c = CsiDriverClient::with_endpoint("migrated.metrics.drv", &sock)
            .with_operation_spec(Some(&migrated.as_spec()));
        c.node_supports_stage_unstage().await.unwrap();
        let c = CsiDriverClient::with_endpoint("plain.metrics.drv", &sock);
        c.node_supports_stage_unstage().await.unwrap();
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains("csi_operations_seconds_count{driver_name=\"migrated.metrics.drv\",grpc_status_code=\"OK\",method_name=\"/csi.v1.Node/NodeGetCapabilities\",migrated=\"true\"} 1"), "{body}");
        assert!(body.contains("csi_operations_seconds_count{driver_name=\"plain.metrics.drv\",grpc_status_code=\"OK\",method_name=\"/csi.v1.Node/NodeGetCapabilities\",migrated=\"false\"} 1"), "{body}");
    }
}
