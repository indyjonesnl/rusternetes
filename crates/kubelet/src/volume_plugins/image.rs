use crate::volume_plugins::{
    Attributes, Mounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumePlugin,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;

/// Port of `imagePlugin` (`pkg/volume/image/image.go:27-91`), registered by
/// `image.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`).
///
/// "[A] stub to provide the functionality the volume manager expects. The
/// real volume source implementation is part of the kubelet code and gated by
/// the Kubernetes feature `ImageVolume`" (KEP-4639): the kubelet pulls the
/// image and the runtime mounts it through the CRI `Mount.image` field, see
/// `cri_runtime::runtime::get_image_volumes` and
/// `cri_runtime::translate::mounts_with`.
///
/// The embedded `volume.MetricsNil` (`image.go:33`) is the
/// [`MetricsProvider`](crate::volume_plugins::plugin::MetricsProvider) impl
/// on `ImageVolume`.
pub struct ImagePlugin;

impl ImagePlugin {
    /// `ProbeVolumePlugins` (`image.go:43-46`); `Init` is a no-op
    /// (`image.go:48`).
    pub fn new() -> Self {
        Self
    }
}

impl Default for ImagePlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl VolumePlugin for ImagePlugin {
    /// `GetPluginName` (`image.go:49`).
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::IMAGE
    }

    /// `GetVolumeName` (`image.go:50-57`): the image reference.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        match spec.volume.image.as_ref() {
            Some(image) => Ok(image.reference.clone().unwrap_or_default()),
            None => Err(anyhow!(
                "the volumeSpec does not reference an Image volume type"
            )),
        }
    }

    /// `CanSupport` (`image.go:59-61`): the inline source only.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.image.is_some()
    }

    /// `RequiresRemount` (`image.go:85`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`image.go:89`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `NewMounter` (`image.go:63-65`): the plugin is its own mounter.
    async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(ImageVolume))
    }

    /// `NewUnmounter` (`image.go:67-69`).
    fn new_unmounter(&self, _vol_name: &str, _pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(ImageVolume))
    }

    /// `ConstructVolumeSpec` (`image.go:71-73`): `volume.ReconstructedVolume{}`,
    /// no error: a nil `Spec`, which `reconstructVolume` rejects with "got nil"
    /// (`reconstruct_common.go:292-295`).
    fn construct_volume_spec(
        &self,
        _vol_name: &str,
        _mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        Ok(ReconstructedVolume {
            volume: None,
            selinux_mount_context: String::new(),
            persistent_volume: None,
        })
    }
}

/// The `imagePlugin` value upstream returns as `Mounter`, `Unmounter` and
/// `Volume` alike (`image.go:36-39`).
struct ImageVolume;

#[async_trait]
impl Mounter for ImageVolume {
    /// `GetPath` (`image.go:83`): empty.
    fn get_path(&self) -> String {
        String::new()
    }

    /// `SetUpAt` (`image.go:87`): nothing.
    async fn set_up_at(&self, _dir: &str, _args: &MounterArgs) -> Result<()> {
        Ok(())
    }

    /// `GetAttributes` (`image.go:75-81`): `{ReadOnly: true, Managed: false,
    /// SELinuxRelabel: false}`.
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: true,
            managed: false,
            selinux_relabel: false,
        }
    }
}

#[async_trait]
impl Unmounter for ImageVolume {
    fn get_path(&self) -> String {
        String::new()
    }

    /// `TearDownAt` (`image.go:91`): nothing.
    async fn tear_down_at(&self, _dir: &str) -> Result<()> {
        Ok(())
    }
}

impl crate::volume_plugins::plugin::MetricsProvider for ImageVolume {
    /// `volume.MetricsNil` (`image.go:33`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        crate::volume_plugins::plugin::MetricsProvider::get_metrics(
            &crate::volume_plugins::util::metrics::MetricsNil,
        )
    }
}
