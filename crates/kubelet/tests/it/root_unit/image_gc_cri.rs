//! Unit tests moved from `src/image_gc_cri.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{EventSource, EventType, ObjectReference};
    pub use rusternetes_cri::{v1, CriClient};
    pub use rusternetes_kubelet::image_gc::{
        FsStats, GcContainer, GcPod, Image, ImageGcRuntime, ImageSpec, NodeEventSink, StatsProvider,
    };
    pub use rusternetes_kubelet::labels::{POD_NAMESPACE_LABEL, POD_NAME_LABEL, POD_UID_LABEL};
    pub use rusternetes_storage::{EventRecorder, StorageBackend};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::image_gc_cri::*;

    /// `toKubeContainer` (`kuberuntime/helpers.go:101-123`): `ImageID` is
    /// `c.ImageId`, falling back to `c.ImageRef` for pre-1.30 runtimes; a
    /// container without an id or image is skipped.
    #[test]
    fn to_gc_container_follows_to_kube_container() {
        let base = v1::Container {
            id: "c1".into(),
            image: Some(v1::ImageSpec {
                image: "busybox".into(),
                runtime_handler: "h".into(),
                ..Default::default()
            }),
            image_ref: "sha256:ref".into(),
            ..Default::default()
        };
        let c = to_gc_container(&base).unwrap();
        assert_eq!("sha256:ref", c.image_id);
        assert_eq!("h", c.image_runtime_handler);

        let with_id = v1::Container {
            image_id: "sha256:id".into(),
            ..base.clone()
        };
        assert_eq!("sha256:id", to_gc_container(&with_id).unwrap().image_id);

        assert!(to_gc_container(&v1::Container {
            id: String::new(),
            ..base.clone()
        })
        .is_none());
        assert!(to_gc_container(&v1::Container {
            image: None,
            ..base
        })
        .is_none());
    }
}
