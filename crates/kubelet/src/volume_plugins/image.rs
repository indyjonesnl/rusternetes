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
/// Not ported: the embedded `volume.MetricsNil` (no plugin here exposes
/// volume metrics yet, as for the nfs plugin).
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

#[cfg(test)]
mod tests {
    //! Port of `pkg/volume/image/image_test.go` (`TestCanSupport`) plus the
    //! stub's remaining contract from `pkg/volume/image/image.go:48-91`.
    use super::*;
    use crate::volume_plugins::{Spec, VolumePluginMgr};
    use rusternetes_common::resources::Volume;
    use serde_json::json;

    fn vol(v: serde_json::Value) -> Volume {
        serde_json::from_value(v).unwrap()
    }

    fn spec(volume: &Volume) -> Spec<'_> {
        Spec {
            volume,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        }
    }

    fn pod() -> rusternetes_common::resources::Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "uid": "poduid"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    /// `TestCanSupport` (`image_test.go:26-46`): found by name, an image
    /// volume (even with an empty reference) is supported, a bare spec is not.
    #[test]
    fn can_support() {
        let mgr = VolumePluginMgr::new(vec![Box::new(ImagePlugin::new())]);
        let plug = mgr.find_plugin_by_name("kubernetes.io/image").unwrap();
        assert_eq!(plug.name(), "kubernetes.io/image");
        let with_image = vol(json!({"name": "v", "image": {"reference": ""}}));
        assert!(plug.can_support(&spec(&with_image)));
        let bare = vol(json!({"name": "v"}));
        assert!(!plug.can_support(&spec(&bare)));
    }

    /// `CanSupport` reads `spec.Volume.Image` only (`image.go:59-61`): a PV
    /// is never an image volume.
    #[test]
    fn a_persistent_volume_spec_is_not_supported() {
        let bare = vol(json!({"name": "v"}));
        let pv = serde_json::from_value(json!({
            "metadata": {"name": "pv"}, "spec": {"nfs": {"server": "s", "path": "/p"}}
        }))
        .unwrap();
        let s = Spec {
            volume: &bare,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!ImagePlugin::new().can_support(&s));
    }

    /// `GetVolumeName` (`image.go:50-57`): the reference, or the verbatim
    /// error for a spec that is not an image volume.
    #[test]
    fn get_volume_name() {
        let p = ImagePlugin::new();
        let v = vol(json!({"name": "v", "image": {"reference": "quay.io/a/b:1"}}));
        assert_eq!(p.get_volume_name(&spec(&v)).unwrap(), "quay.io/a/b:1");
        let bare = vol(json!({"name": "v"}));
        assert_eq!(
            p.get_volume_name(&spec(&bare)).unwrap_err().to_string(),
            "the volumeSpec does not reference an Image volume type"
        );
    }

    /// `imagePlugin` is its own mounter and unmounter (`image.go:63-69`) and
    /// all of it is a stub: `GetPath` is empty (`:83`), `GetAttributes` is
    /// read-only/unmanaged/no relabel (`:75-81`), `SetUpAt`/`TearDownAt` do
    /// nothing (`:87`, `:91`), and nothing needs a remount or a SELinux
    /// mount (`:85`, `:89`).
    #[tokio::test]
    async fn the_stub_does_nothing_and_reports_read_only() {
        let p = ImagePlugin::new();
        let v = vol(json!({"name": "v", "image": {"reference": "r"}}));
        let s = spec(&v);
        assert!(!p.requires_remount(&s));
        assert!(!p.supports_selinux_context_mount(&s).unwrap());

        let m = p.new_mounter(&s, &pod()).await.unwrap();
        assert_eq!(m.get_path(), "");
        let a = m.get_attributes();
        assert!(a.read_only && !a.managed && !a.selinux_relabel);
        m.set_up().await.unwrap();
        m.set_up_at("/nonexistent/dir", &Default::default())
            .await
            .unwrap();

        let u = p.new_unmounter("v", "poduid").unwrap();
        assert_eq!(u.get_path(), "");
        u.tear_down().await.unwrap();
        u.tear_down_at("/nonexistent/dir").await.unwrap();
    }

    /// `ConstructVolumeSpec` returns the zero `ReconstructedVolume`
    /// (`image.go:71-73`) without error.
    #[test]
    fn construct_volume_spec_succeeds_without_a_spec() {
        let rv = ImagePlugin::new().construct_volume_spec("v", "/m").unwrap();
        assert!(rv.volume.is_none());
        assert!(rv.persistent_volume.is_none());
    }
}
