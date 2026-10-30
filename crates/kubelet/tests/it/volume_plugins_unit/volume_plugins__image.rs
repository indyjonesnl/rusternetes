//! Unit tests moved from `src/volume_plugins/image.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumePlugin,
    };
}

mod tests {
    //! Port of `pkg/volume/image/image_test.go` (`TestCanSupport`) plus the
    //! stub's remaining contract from `pkg/volume/image/image.go:48-91`.
    use super::__parent::*;
    use rusternetes_common::resources::Volume;
    use rusternetes_kubelet::volume_plugins::image::*;
    use rusternetes_kubelet::volume_plugins::{Spec, VolumePluginMgr};
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
