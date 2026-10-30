use crate::volume_plugins::plugin::{
    BlockVolumePlugin, DeviceMountableVolumePlugin, NodeExpandableVolumePlugin, Spec, VolumePlugin,
};

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
        matches!(plugin.can_attach(spec), Ok(true)).then_some(plugin)
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

    /// Port of `FindDeviceMountablePluginByName` (`pkg/volume/plugins.go:852-861`).
    ///
    /// `Ok(None)` is upstream's `(nil, nil)` — a plugin matched but is not a
    /// `DeviceMountableVolumePlugin`; the type assertion is
    /// [`VolumePlugin::as_device_mountable_plugin`].
    pub fn find_device_mountable_plugin_by_name(
        &self,
        name: &str,
    ) -> Result<Option<&dyn DeviceMountableVolumePlugin>, PluginLookupError> {
        Ok(self.find_plugin_by_name(name)?.as_device_mountable_plugin())
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

    /// Port of `FindNodeExpandablePluginBySpec` (`pkg/volume/plugins.go:926-935`).
    ///
    /// `Ok(None)` is upstream's `(nil, nil)` — a plugin matched but is not a
    /// `NodeExpandableVolumePlugin`; the type assertion is
    /// [`VolumePlugin::as_node_expandable_plugin`].
    pub fn find_node_expandable_plugin_by_spec(
        &self,
        spec: &Spec<'_>,
    ) -> Result<Option<&dyn NodeExpandableVolumePlugin>, PluginLookupError> {
        Ok(self.find_plugin_by_spec(spec)?.as_node_expandable_plugin())
    }

    /// Port of `FindNodeExpandablePluginByName` (`pkg/volume/plugins.go:937-949`).
    pub fn find_node_expandable_plugin_by_name(
        &self,
        name: &str,
    ) -> Result<Option<&dyn NodeExpandableVolumePlugin>, PluginLookupError> {
        Ok(self.find_plugin_by_name(name)?.as_node_expandable_plugin())
    }
}
