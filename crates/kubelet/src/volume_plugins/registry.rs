use crate::volume_plugins::plugin::{BlockVolumePlugin, Spec, VolumePlugin};

/// Port of `ErrNoPluginMatched` (`pkg/volume/plugins.go:71-72`) plus the
/// multiple-match error (`plugins.go:662`). A typed error, not a string,
/// because `create_volume` must distinguish "nothing matched" (apply the
/// unsupported-kind fallback) from "several matched" (a malformed volume that
/// must fail).
#[derive(Debug, thiserror::Error)]
pub enum PluginLookupError {
    #[error("no volume plugin matched")]
    NoPluginMatched,
    #[error("multiple volume plugins matched: {0}")]
    MultipleMatched(String),
}

/// Port of `VolumePluginMgr` (`pkg/volume/plugins.go:425`).
///
/// Upstream also carries probed (dynamically discovered) plugins and a mutex.
/// Our set is fixed at construction and the registry is immutable afterwards,
/// so neither is ported.
pub struct VolumePluginMgr {
    plugins: Vec<Box<dyn VolumePlugin>>,
}

impl VolumePluginMgr {
    pub fn new(plugins: Vec<Box<dyn VolumePlugin>>) -> Self {
        Self { plugins }
    }

    /// Port of `FindPluginBySpec` (`pkg/volume/plugins.go:634-665`).
    ///
    /// Collects EVERY match rather than returning the first. Two plugins
    /// matching means the volume declares two sources, which upstream rejects
    /// outright — first-match-wins would silently provision one of them.
    pub fn find_plugin_by_spec(
        &self,
        spec: &Spec<'_>,
    ) -> Result<&dyn VolumePlugin, PluginLookupError> {
        let matched: Vec<&dyn VolumePlugin> = self
            .plugins
            .iter()
            .filter(|p| p.can_support(spec))
            .map(|p| p.as_ref())
            .collect();

        match matched.len() {
            0 => Err(PluginLookupError::NoPluginMatched),
            1 => Ok(matched[0]),
            _ => Err(PluginLookupError::MultipleMatched(
                matched
                    .iter()
                    .map(|p| p.name())
                    .collect::<Vec<_>>()
                    .join(","),
            )),
        }
    }

    /// Port of `FindAttachablePluginBySpec` (`pkg/volume/plugins.go:805-818`).
    ///
    /// Upstream returns `(nil, nil)` when a plugin matched but is not
    /// attachable, and `(nil, err)` when no plugin matched at all; both
    /// collapse to `None` here because the sole caller,
    /// `util::is_attachable_volume` (`pkg/volume/util/util.go:635-645`),
    /// discards the error and tests only for a non-nil plugin.
    pub fn find_attachable_plugin_by_spec(&self, spec: &Spec<'_>) -> Option<&dyn VolumePlugin> {
        let plugin = self.find_plugin_by_spec(spec).ok()?;
        plugin.can_attach(spec).then_some(plugin)
    }

    /// Port of `FindDeviceMountablePluginBySpec` (`pkg/volume/plugins.go:836-849`).
    /// Same `(nil, nil)` / `(nil, err)` collapse as
    /// [`VolumePluginMgr::find_attachable_plugin_by_spec`], for the sole caller
    /// `util::is_device_mountable_volume` (`util.go:648-658`).
    pub fn find_device_mountable_plugin_by_spec(
        &self,
        spec: &Spec<'_>,
    ) -> Option<&dyn VolumePlugin> {
        let plugin = self.find_plugin_by_spec(spec).ok()?;
        plugin.can_device_mount(spec).then_some(plugin)
    }

    /// Port of `FindPluginByName` (`pkg/volume/plugins.go:669-689`). Probed
    /// plugins are not ported (see the type comment), so the multiple-match arm
    /// cannot arise from a probed duplicate; two registered plugins sharing a
    /// name still error.
    pub fn find_plugin_by_name(&self, name: &str) -> Result<&dyn VolumePlugin, PluginLookupError> {
        let matched: Vec<&dyn VolumePlugin> = self
            .plugins
            .iter()
            .filter(|p| p.name() == name)
            .map(|p| p.as_ref())
            .collect();
        match matched.len() {
            0 => Err(PluginLookupError::NoPluginMatched),
            1 => Ok(matched[0]),
            _ => Err(PluginLookupError::MultipleMatched(name.to_string())),
        }
    }

    /// Port of `FindMapperPluginBySpec` (`pkg/volume/plugins.go:900-911`).
    ///
    /// `Ok(None)` is upstream's `(nil, nil)` — a plugin matched but is not a
    /// `BlockVolumePlugin`; the type assertion is
    /// [`VolumePlugin::as_block_volume_plugin`].
    pub fn find_mapper_plugin_by_spec(
        &self,
        spec: &Spec<'_>,
    ) -> Result<Option<&dyn BlockVolumePlugin>, PluginLookupError> {
        Ok(self.find_plugin_by_spec(spec)?.as_block_volume_plugin())
    }

    /// Port of `FindMapperPluginByName` (`pkg/volume/plugins.go:913-924`).
    pub fn find_mapper_plugin_by_name(
        &self,
        name: &str,
    ) -> Result<Option<&dyn BlockVolumePlugin>, PluginLookupError> {
        Ok(self.find_plugin_by_name(name)?.as_block_volume_plugin())
    }

    /// Port of `FindNodeExpandablePluginBySpec`(`pkg/volume/plugins.go:926-935`).
    ///
    /// Upstream returns `(nil, nil)` when a plugin matched but is not
    /// node-expandable and `(nil, err)` when none matched; both collapse to
    /// `None` here because the sole caller,
    /// `ActualStateOfWorld::volume_needs_expansion`
    /// (`actual_state_of_world.go:981-985`), logs and treats either as "no
    /// expansion". The `NodeExpandableVolumePlugin` type assertion is folded
    /// into [`VolumePlugin::requires_fs_resize`]; see its comment.
    pub fn find_node_expandable_plugin_by_spec(
        &self,
        spec: &Spec<'_>,
    ) -> Option<&dyn VolumePlugin> {
        self.find_plugin_by_spec(spec).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::plugin::{
        BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper, Mounter, OwnedSpec, Spec,
        VolumePlugin,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use rusternetes_common::resources::{Pod, Volume};
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
        };
        let found = m.find_mapper_plugin_by_spec(&spec).unwrap().unwrap();
        assert_eq!(found.name(), "kubernetes.io/block");

        let a = volume("a-1");
        let spec = Spec {
            volume: &a,
            persistent_volume: None,
        };
        assert!(m.find_mapper_plugin_by_spec(&spec).unwrap().is_none());

        let z = volume("zzz");
        let spec = Spec {
            volume: &z,
            persistent_volume: None,
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
}
