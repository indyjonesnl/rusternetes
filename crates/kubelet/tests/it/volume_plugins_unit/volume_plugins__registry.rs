//! Unit tests moved from `src/volume_plugins/registry.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        BlockVolumePlugin, DeviceMountableVolumePlugin, NodeExpandableVolumePlugin, Spec,
        VolumePlugin,
    };
}

mod tests {
    use super::__parent::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use rusternetes_common::resources::{Pod, Volume};
    use rusternetes_kubelet::volume_plugins::plugin::{
        BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper, Mounter,
        NodeExpandableVolumePlugin, NodeResizeOptions, OwnedSpec, Spec, VolumePlugin,
    };
    use rusternetes_kubelet::volume_plugins::registry::*;
    use serde_json::json;

    /// A plugin that matches when the volume's name starts with its prefix.
    /// Stands in for a real plugin so the registry can be tested without one.
    struct PrefixPlugin {
        name: &'static str,
        prefix: &'static str,
    }

    #[async_trait]
    impl VolumePlugin for PrefixPlugin {
        fn name(&self) -> &'static str {
            self.name
        }
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            Ok(spec.volume.name.clone())
        }
        fn can_support(&self, spec: &Spec<'_>) -> bool {
            spec.volume.name.starts_with(self.prefix)
        }
        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            false
        }
        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(false)
        }
        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            unimplemented!("registry tests never mount")
        }

        fn new_unmounter(
            &self,
            _vol_name: &str,
            _pod_uid: &str,
        ) -> Result<Box<dyn rusternetes_kubelet::volume_plugins::Unmounter>> {
            unimplemented!("test plugin never unmounts")
        }

        fn construct_volume_spec(
            &self,
            _vol_name: &str,
            _mount_path: &str,
        ) -> Result<rusternetes_kubelet::volume_plugins::ReconstructedVolume> {
            unimplemented!("test plugin never reconstructs")
        }
    }

    fn volume(name: &str) -> Volume {
        serde_json::from_value(json!({ "name": name })).unwrap()
    }

    fn mgr() -> VolumePluginMgr {
        VolumePluginMgr::new(vec![
            Box::new(PrefixPlugin {
                name: "kubernetes.io/a",
                prefix: "a",
            }),
            Box::new(PrefixPlugin {
                name: "kubernetes.io/b",
                prefix: "b",
            }),
            Box::new(PrefixPlugin {
                name: "kubernetes.io/ab",
                prefix: "ab",
            }),
        ])
    }

    #[test]
    fn one_match_returns_that_plugin() {
        let v = volume("b-volume");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert_eq!(
            mgr().find_plugin_by_spec(&spec).unwrap().name(),
            "kubernetes.io/b"
        );
    }

    #[test]
    fn no_match_is_no_plugin_matched() {
        let v = volume("zzz");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(matches!(
            mgr().find_plugin_by_spec(&spec),
            Err(PluginLookupError::NoPluginMatched)
        ));
    }

    /// Upstream errors rather than letting first-match-wins pick one
    /// (`pkg/volume/plugins.go:659-662`). The ordered if-else chain this
    /// registry replaced picked silently; the registry must not.
    #[test]
    fn two_matches_is_an_error_naming_both() {
        let v = volume("ab-volume");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let err = mgr().find_plugin_by_spec(&spec).err().unwrap();
        let msg = err.to_string();
        assert!(msg.contains("multiple volume plugins matched"), "{msg}");
        assert!(msg.contains("kubernetes.io/a"), "{msg}");
        assert!(msg.contains("kubernetes.io/ab"), "{msg}");
    }

    /// A plugin that is also a [`BlockVolumePlugin`] — the Rust spelling of
    /// upstream's `volumePlugin.(BlockVolumePlugin)` type assertion
    /// (`pkg/volume/plugins.go:906`).
    struct BlockPlugin;

    #[async_trait]
    impl VolumePlugin for BlockPlugin {
        fn name(&self) -> &'static str {
            "kubernetes.io/block"
        }
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            Ok(spec.volume.name.clone())
        }
        fn can_support(&self, spec: &Spec<'_>) -> bool {
            spec.volume.name.starts_with("blk")
        }
        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            false
        }
        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(false)
        }
        fn as_block_volume_plugin(&self) -> Option<&dyn BlockVolumePlugin> {
            Some(self)
        }
        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            unimplemented!()
        }

        fn new_unmounter(
            &self,
            _vol_name: &str,
            _pod_uid: &str,
        ) -> Result<Box<dyn rusternetes_kubelet::volume_plugins::Unmounter>> {
            unimplemented!("test plugin never unmounts")
        }

        fn construct_volume_spec(
            &self,
            _vol_name: &str,
            _mount_path: &str,
        ) -> Result<rusternetes_kubelet::volume_plugins::ReconstructedVolume> {
            unimplemented!("test plugin never reconstructs")
        }
    }

    impl BlockVolumePlugin for BlockPlugin {
        fn new_block_volume_mapper(
            &self,
            _spec: &Spec<'_>,
            _pod: &Pod,
        ) -> Result<Box<dyn BlockVolumeMapper>> {
            unimplemented!()
        }
        fn new_block_volume_unmapper(
            &self,
            _name: &str,
            _pod_uid: &str,
        ) -> Result<Box<dyn BlockVolumeUnmapper>> {
            unimplemented!()
        }
        fn construct_block_volume_spec(
            &self,
            _pod_uid: &str,
            volume_name: &str,
            _volume_path: &str,
        ) -> Result<OwnedSpec> {
            Ok(OwnedSpec {
                volume: volume(volume_name),
                persistent_volume: None,
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            })
        }
    }

    fn block_mgr() -> VolumePluginMgr {
        VolumePluginMgr::new(vec![
            Box::new(PrefixPlugin {
                name: "kubernetes.io/a",
                prefix: "a",
            }),
            Box::new(BlockPlugin),
        ])
    }

    /// `FindMapperPluginBySpec` (`plugins.go:900-911`): a matched plugin that is
    /// not a `BlockVolumePlugin` is `(nil, nil)`, a matched block plugin is
    /// returned, an unmatched spec is `(nil, err)`.
    #[test]
    fn find_mapper_plugin_by_spec() {
        let m = block_mgr();
        let blk = volume("blk-1");
        let spec = Spec {
            volume: &blk,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let found = m.find_mapper_plugin_by_spec(&spec).unwrap().unwrap();
        assert_eq!(found.name(), "kubernetes.io/block");

        let a = volume("a-1");
        let spec = Spec {
            volume: &a,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(m.find_mapper_plugin_by_spec(&spec).unwrap().is_none());

        let z = volume("zzz");
        let spec = Spec {
            volume: &z,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(matches!(
            m.find_mapper_plugin_by_spec(&spec),
            Err(PluginLookupError::NoPluginMatched)
        ));
    }

    /// `FindMapperPluginByName` (`plugins.go:913-924`) over
    /// `FindPluginByName` (`plugins.go:669-689`).
    #[test]
    fn find_mapper_plugin_by_name() {
        let m = block_mgr();
        let found = m
            .find_mapper_plugin_by_name("kubernetes.io/block")
            .unwrap()
            .unwrap();
        assert_eq!(found.name(), "kubernetes.io/block");
        assert!(m
            .find_mapper_plugin_by_name("kubernetes.io/a")
            .unwrap()
            .is_none());
        assert!(matches!(
            m.find_mapper_plugin_by_name("kubernetes.io/nope"),
            Err(PluginLookupError::NoPluginMatched)
        ));
    }

    /// A plugin that is also a [`NodeExpandableVolumePlugin`] — the Rust
    /// spelling of `volumePlugin.(NodeExpandableVolumePlugin)`
    /// (`pkg/volume/plugins.go:931`).
    struct ExpandPlugin;

    #[async_trait]
    impl VolumePlugin for ExpandPlugin {
        fn name(&self) -> &'static str {
            "kubernetes.io/expand"
        }
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            Ok(spec.volume.name.clone())
        }
        fn can_support(&self, spec: &Spec<'_>) -> bool {
            spec.volume.name.starts_with("exp")
        }
        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            false
        }
        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(false)
        }
        fn as_node_expandable_plugin(&self) -> Option<&dyn NodeExpandableVolumePlugin> {
            Some(self)
        }
        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            unimplemented!()
        }

        fn new_unmounter(
            &self,
            _vol_name: &str,
            _pod_uid: &str,
        ) -> Result<Box<dyn rusternetes_kubelet::volume_plugins::Unmounter>> {
            unimplemented!("test plugin never unmounts")
        }

        fn construct_volume_spec(
            &self,
            _vol_name: &str,
            _mount_path: &str,
        ) -> Result<rusternetes_kubelet::volume_plugins::ReconstructedVolume> {
            unimplemented!("test plugin never reconstructs")
        }
    }

    #[async_trait]
    impl NodeExpandableVolumePlugin for ExpandPlugin {
        fn requires_fs_resize(&self) -> bool {
            true
        }
        async fn node_expand(&self, _opts: NodeResizeOptions<'_>) -> Result<bool> {
            Ok(true)
        }
    }

    fn expand_mgr() -> VolumePluginMgr {
        VolumePluginMgr::new(vec![
            Box::new(PrefixPlugin {
                name: "kubernetes.io/a",
                prefix: "a",
            }),
            Box::new(ExpandPlugin),
        ])
    }

    /// `FindNodeExpandablePluginBySpec` (`plugins.go:926-935`): a matched
    /// plugin that is not node-expandable is `(nil, nil)`, a matched
    /// expandable plugin is returned, an unmatched spec is `(nil, err)`.
    #[test]
    fn find_node_expandable_plugin_by_spec() {
        let m = expand_mgr();
        let e = volume("exp-1");
        let spec = Spec {
            volume: &e,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let found = m
            .find_node_expandable_plugin_by_spec(&spec)
            .unwrap()
            .unwrap();
        assert_eq!(found.name(), "kubernetes.io/expand");
        assert!(found.requires_fs_resize());

        let a = volume("a-1");
        let spec = Spec {
            volume: &a,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(m
            .find_node_expandable_plugin_by_spec(&spec)
            .unwrap()
            .is_none());

        let z = volume("zzz");
        let spec = Spec {
            volume: &z,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(matches!(
            m.find_node_expandable_plugin_by_spec(&spec),
            Err(PluginLookupError::NoPluginMatched)
        ));
    }

    /// `FindNodeExpandablePluginByName` (`plugins.go:937-949`).
    #[test]
    fn find_node_expandable_plugin_by_name() {
        let m = expand_mgr();
        let found = m
            .find_node_expandable_plugin_by_name("kubernetes.io/expand")
            .unwrap()
            .unwrap();
        assert_eq!(found.name(), "kubernetes.io/expand");
        assert!(m
            .find_node_expandable_plugin_by_name("kubernetes.io/a")
            .unwrap()
            .is_none());
        assert!(matches!(
            m.find_node_expandable_plugin_by_name("kubernetes.io/nope"),
            Err(PluginLookupError::NoPluginMatched)
        ));
    }
}
