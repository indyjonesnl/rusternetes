//! Unit tests moved from `src/image_gc.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use chrono::{DateTime, TimeZone, Utc};
    pub use prometheus::{IntCounterVec, Opts};
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::{Arc, LazyLock, Mutex, Once};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use chrono::Duration as CDuration;
    use rusternetes_kubelet::image_gc::*;
    use serial_test::serial;

    // ---- fakes (containertest.FakeRuntime, statstest.MockProvider, record.FakeRecorder)

    #[derive(Default)]
    struct FakeRuntimeState {
        image_list: Vec<Image>,
        all_pod_list: Vec<GcPod>,
        image_mounts: Vec<String>,
        err: Option<String>,
    }

    #[derive(Default, Clone)]
    struct FakeRuntime(Arc<Mutex<FakeRuntimeState>>);

    impl FakeRuntime {
        fn set_images(&self, images: Vec<Image>) {
            self.0.lock().unwrap().image_list = images;
        }
        fn set_pods(&self, containers: Vec<GcContainer>) {
            self.0.lock().unwrap().all_pod_list = vec![GcPod {
                containers,
                ..Default::default()
            }];
        }
        fn clear_pods(&self) {
            self.0.lock().unwrap().all_pod_list = vec![];
        }
        fn image_len(&self) -> usize {
            self.0.lock().unwrap().image_list.len()
        }
    }

    #[async_trait]
    impl ImageGcRuntime for FakeRuntime {
        async fn list_images(&self) -> anyhow::Result<Vec<Image>> {
            let s = self.0.lock().unwrap();
            match &s.err {
                Some(e) => Err(anyhow::anyhow!(e.clone())),
                None => Ok(s.image_list.clone()),
            }
        }
        async fn get_pods(&self) -> anyhow::Result<Vec<GcPod>> {
            Ok(self.0.lock().unwrap().all_pod_list.clone())
        }
        async fn container_image_mounts(&self, _: &str, _: &str) -> anyhow::Result<Vec<String>> {
            Ok(self.0.lock().unwrap().image_mounts.clone())
        }
        // FakeRuntime.RemoveImage (container/testing/fake_runtime.go:425-440):
        // index defaults to 0 when nothing matches.
        async fn remove_image(&self, image: &ImageSpec) -> anyhow::Result<()> {
            let mut s = self.0.lock().unwrap();
            let mut index = 0;
            for (i, im) in s.image_list.iter().enumerate() {
                if im.id == image.image {
                    index = i;
                    break;
                }
            }
            if !s.image_list.is_empty() {
                s.image_list.remove(index);
            }
            match &s.err {
                Some(e) => Err(anyhow::anyhow!(e.clone())),
                None => Ok(()),
            }
        }
    }

    struct MockStats(Mutex<Option<anyhow::Result<FsStats>>>);

    impl MockStats {
        fn none() -> Arc<Self> {
            Arc::new(Self(Mutex::new(None)))
        }
        fn returning(r: anyhow::Result<FsStats>) -> Arc<Self> {
            Arc::new(Self(Mutex::new(Some(r))))
        }
    }

    #[async_trait]
    impl StatsProvider for MockStats {
        async fn image_fs_stats(&self) -> anyhow::Result<FsStats> {
            self.0
                .lock()
                .unwrap()
                .take()
                .expect("unexpected ImageFsStats call")
        }
    }

    #[derive(Default)]
    struct FakeRecorder(Mutex<Vec<String>>);

    impl NodeEventSink for FakeRecorder {
        fn warning(&self, reason: &str, message: &str) {
            // record.FakeRecorder: "<type> <reason> <message>".
            self.0
                .lock()
                .unwrap()
                .push(format!("Warning {reason} {message}"));
        }
    }

    fn manager(
        policy: ImageGcPolicy,
        stats: Arc<MockStats>,
    ) -> (ImageGcManager, FakeRuntime, Arc<FakeRecorder>) {
        let rt = FakeRuntime::default();
        let rec = Arc::new(FakeRecorder::default());
        let m = ImageGcManager::new(Arc::new(rt.clone()), stats, vec![], rec.clone(), policy)
            .expect("valid policy");
        (m, rt, rec)
    }

    fn image_id(id: i32) -> String {
        format!("image-{id}")
    }
    fn image_name(id: i32) -> String {
        format!("{}-name", image_id(id))
    }
    fn make_image(id: i32, size: i64) -> Image {
        Image {
            id: image_id(id),
            size,
            ..Default::default()
        }
    }
    fn make_image_with_handler(id: i32, size: i64, handler: &str) -> Image {
        Image {
            runtime_handler: handler.to_string(),
            ..make_image(id, size)
        }
    }
    fn make_container(id: i32) -> GcContainer {
        GcContainer {
            id: format!("container-{id}"),
            image: image_name(id),
            image_id: image_id(id),
            ..Default::default()
        }
    }
    fn raw_image(id: &str, size: i64, pinned: bool) -> Image {
        Image {
            id: id.to_string(),
            size,
            pinned,
            ..Default::default()
        }
    }

    async fn get_images_and_free_space(
        im: &ImageGcManager,
        rt: &FakeRuntime,
        space_to_free: i64,
        expected_space_freed: i64,
        images_len: usize,
        free_time: DateTime<Utc>,
    ) {
        let images = im.images_in_eviction_order(free_time).await.unwrap();
        let (_, space_freed) = im
            .free_space(space_to_free, free_time, images)
            .await
            .map_err(|(_, e)| e)
            .unwrap();
        assert_eq!(expected_space_freed, space_freed);
        assert_eq!(images_len, rt.image_len());
    }

    // ---- TestDetectImages*

    #[tokio::test]
    async fn detect_images_initial_detect() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![
            make_image(0, 1024),
            make_image(1, 2048),
            make_image(2, 2048),
        ]);
        rt.set_pods(vec![
            GcContainer {
                id: "container-1".into(),
                image_id: image_id(1),
                // The image field is not set to simulate a no-name image.
                ..Default::default()
            },
            GcContainer {
                id: "container-2".into(),
                image: image_name(2),
                image_id: image_id(2),
                ..Default::default()
            },
        ]);

        let start_time = Utc::now() - CDuration::milliseconds(1);
        m.detect_images(zero_time()).await.unwrap();
        assert_eq!(3, m.image_records_len());
        let no_container = m.get_image_record(&image_id(0)).unwrap();
        assert_eq!(zero_time(), no_container.first_detected);
        assert_eq!(zero_time(), no_container.last_used);
        let with_no_name = m.get_image_record(&image_id(1)).unwrap();
        assert_eq!(zero_time(), with_no_name.first_detected);
        assert!(with_no_name.last_used > start_time);
        let with_container = m.get_image_record(&image_id(2)).unwrap();
        assert_eq!(zero_time(), with_container.first_detected);
        assert!(with_container.last_used > start_time);
    }

    #[tokio::test]
    #[serial]
    async fn detect_images_initial_detect_with_runtime_handler_in_image_cri_api() {
        let _g = rusternetes_common::feature_gates::with_feature(
            Feature::RuntimeClassInImageCriApi,
            true,
        );
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![
            make_image_with_handler(0, 1024, ""),
            make_image_with_handler(1, 2048, "handler-1"),
            make_image_with_handler(2, 2048, "handler-2"),
            make_image_with_handler(2, 2048, ""),
        ]);
        rt.set_pods(vec![
            GcContainer {
                id: "container-1".into(),
                image_id: image_id(1),
                image_runtime_handler: "handler-1".into(),
                ..Default::default()
            },
            GcContainer {
                id: "container-2".into(),
                image: image_name(2),
                image_id: image_id(2),
                image_runtime_handler: "handler-2".into(),
                ..Default::default()
            },
        ]);

        let start_time = Utc::now() - CDuration::milliseconds(1);
        m.detect_images(zero_time()).await.unwrap();
        assert_eq!(4, m.image_records_len());
        let no_container = m.get_image_record(&image_id(0)).unwrap();
        assert_eq!(zero_time(), no_container.first_detected);
        assert_eq!(zero_time(), no_container.last_used);
        let with_no_name = m.get_image_record("image-1,handler-1").unwrap();
        assert_eq!(zero_time(), with_no_name.first_detected);
        assert!(with_no_name.last_used > start_time);
        assert_eq!("handler-1", with_no_name.runtime_handler_used_to_pull_image);
        let with_container = m.get_image_record("image-2,handler-2").unwrap();
        assert!(with_container.last_used > start_time);
        assert_eq!(
            "handler-2",
            with_container.runtime_handler_used_to_pull_image
        );
        // Same image id, no handler: indexed by the bare id and not in use.
        let unused = m.get_image_record(&image_id(2)).unwrap();
        assert_eq!(zero_time(), unused.last_used);
    }

    #[tokio::test]
    async fn detect_images_with_new_image() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);

        m.detect_images(zero_time()).await.unwrap();
        assert_eq!(2, m.image_records_len());

        rt.set_images(vec![
            make_image(0, 1024),
            make_image(1, 1024),
            make_image(2, 1024),
        ]);
        let detected_time = zero_time() + CDuration::seconds(1);
        let start_time = Utc::now() - CDuration::milliseconds(1);
        m.detect_images(detected_time).await.unwrap();
        assert_eq!(3, m.image_records_len());
        let no_container = m.get_image_record(&image_id(0)).unwrap();
        assert_eq!(zero_time(), no_container.first_detected);
        assert_eq!(zero_time(), no_container.last_used);
        assert_eq!("", no_container.runtime_handler_used_to_pull_image);
        let with_container = m.get_image_record(&image_id(1)).unwrap();
        assert_eq!(zero_time(), with_container.first_detected);
        assert!(with_container.last_used > start_time);
        let new_container = m.get_image_record(&image_id(2)).unwrap();
        assert_eq!(detected_time, new_container.first_detected);
    }

    #[tokio::test]
    async fn detect_images_container_stopped() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);

        m.detect_images(zero_time()).await.unwrap();
        assert_eq!(2, m.image_records_len());
        let with_container = m.get_image_record(&image_id(1)).unwrap();

        // Simulate container being stopped.
        rt.clear_pods();
        m.detect_images(Utc::now()).await.unwrap();
        assert_eq!(2, m.image_records_len());
        let c1 = m.get_image_record(&image_id(0)).unwrap();
        assert_eq!(zero_time(), c1.first_detected);
        assert_eq!(zero_time(), c1.last_used);
        let c2 = m.get_image_record(&image_id(1)).unwrap();
        assert_eq!(zero_time(), c2.first_detected);
        assert_eq!(with_container.last_used, c2.last_used);
    }

    #[tokio::test]
    async fn detect_images_with_removed_images() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);

        m.detect_images(zero_time()).await.unwrap();
        assert_eq!(2, m.image_records_len());

        // Simulate both images being removed.
        rt.set_images(vec![]);
        m.detect_images(Utc::now()).await.unwrap();
        assert_eq!(0, m.image_records_len());
    }

    // ---- pinned images

    #[tokio::test]
    async fn delete_unused_images_exempt_sandbox_image() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![raw_image(
            "registry.k8s.io/pause-amd64:latest",
            1024,
            true,
        )]);
        m.delete_unused_images().await.unwrap();
        assert_eq!(1, rt.image_len());
    }

    #[tokio::test]
    async fn delete_pinned_image() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![
            raw_image("registry.k8s.io/pause-amd64:latest", 1024, true),
            raw_image("registry.k8s.io/pause-amd64:latest", 1024, false),
        ]);
        m.delete_unused_images().await.unwrap();
        assert_eq!(1, rt.image_len());
    }

    #[tokio::test]
    async fn do_not_delete_pinned_image() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![
            raw_image("1", 1024, true),
            raw_image("2", 1024, false),
        ]);
        get_images_and_free_space(&m, &rt, 4096, 1024, 1, Utc::now()).await;
    }

    #[tokio::test]
    async fn delete_unpinned_image() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![
            raw_image("1", 1024, false),
            raw_image("2", 1024, false),
        ]);
        get_images_and_free_space(&m, &rt, 2048, 2048, 0, Utc::now()).await;
    }

    #[tokio::test]
    async fn all_pinned_images() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![raw_image("1", 1024, true), raw_image("2", 1024, true)]);
        get_images_and_free_space(&m, &rt, 2048, 0, 2, Utc::now()).await;
    }

    // ---- freeSpace

    #[tokio::test]
    async fn free_space_images_in_use_containers_are_ignored() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);
        get_images_and_free_space(&m, &rt, 2048, 1024, 1, Utc::now()).await;
    }

    #[tokio::test]
    async fn delete_unused_images_remove_all_unused_images() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![
            make_image(0, 1024),
            make_image(1, 2048),
            make_image(2, 2048),
        ]);
        rt.set_pods(vec![make_container(2)]);
        m.delete_unused_images().await.unwrap();
        assert_eq!(1, rt.image_len());
    }

    #[tokio::test]
    async fn free_space_remove_by_least_recently_used() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(0), make_container(1)]);

        // Make 1 be more recently used than 0.
        m.detect_images(zero_time()).await.unwrap();
        rt.set_pods(vec![make_container(1)]);
        m.detect_images(Utc::now()).await.unwrap();
        rt.set_pods(vec![]);
        m.detect_images(Utc::now()).await.unwrap();
        assert_eq!(2, m.image_records_len());

        // The delete time is one minute in the future, so the time the image
        // was first detected and the delete time differ.
        get_images_and_free_space(&m, &rt, 1024, 1024, 1, Utc::now() + CDuration::minutes(1)).await;
    }

    #[tokio::test]
    async fn free_space_ties_broken_by_detected_time() {
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024)]);
        rt.set_pods(vec![make_container(0)]);

        // Make 1 more recently detected but used at the same time as 0.
        m.detect_images(zero_time()).await.unwrap();
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        m.detect_images(Utc::now()).await.unwrap();
        rt.clear_pods();
        m.detect_images(Utc::now()).await.unwrap();
        assert_eq!(2, m.image_records_len());

        get_images_and_free_space(&m, &rt, 1024, 2048, 1, Utc::now()).await;
    }

    #[tokio::test]
    async fn delete_unused_images_limit_by_image_live_time() {
        let policy = ImageGcPolicy {
            min_age: Duration::from_secs(3),
            ..Default::default()
        };
        let (m, rt, _) = manager(policy, MockStats::none());
        rt.set_images(vec![
            make_image(0, 1024),
            make_image(1, 2048),
            make_image(2, 2048),
        ]);
        rt.set_pods(vec![make_container(2)]);
        // Start() runs detectImages once, recording firstDetected = now.
        m.detect_images(Utc::now()).await.unwrap();
        // Images are not old enough, so none is deleted.
        m.delete_unused_images().await.unwrap();
        assert_eq!(3, rt.image_len());
        // Three seconds later they are old enough.
        tokio::time::sleep(Duration::from_secs(3)).await;
        m.delete_unused_images().await.unwrap();
        assert_eq!(1, rt.image_len());
    }

    // ---- GarbageCollect

    fn policy_90_80() -> ImageGcPolicy {
        ImageGcPolicy {
            high_threshold_percent: 90,
            low_threshold_percent: 80,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn garbage_collect_below_low_threshold() {
        // Expect 40% usage.
        let stats = MockStats::returning(Ok(FsStats {
            available_bytes: Some(600),
            capacity_bytes: Some(1000),
            ..Default::default()
        }));
        let (m, _, _) = manager(policy_90_80(), stats);
        m.garbage_collect(Utc::now()).await.unwrap();
    }

    #[tokio::test]
    async fn garbage_collect_cadvisor_failure() {
        let stats = MockStats::returning(Err(anyhow::anyhow!("error")));
        let (m, _, _) = manager(policy_90_80(), stats);
        assert!(m.garbage_collect(Utc::now()).await.is_err());
    }

    #[tokio::test]
    async fn garbage_collect_below_success() {
        // Expect 95% usage and most of it gets freed.
        let stats = MockStats::returning(Ok(FsStats {
            available_bytes: Some(50),
            capacity_bytes: Some(1000),
            ..Default::default()
        }));
        let (m, rt, _) = manager(policy_90_80(), stats);
        rt.set_images(vec![make_image(0, 450)]);
        m.garbage_collect(Utc::now()).await.unwrap();
        assert_eq!(0, rt.image_len());
    }

    #[tokio::test]
    async fn garbage_collect_not_enough_freed() {
        // Initial state: 95% usage, of which 5% is garbage images.
        let capacity = 10u64 * 1024 * 1024 * 1024;
        let stats = MockStats::returning(Ok(FsStats {
            available_bytes: Some(capacity / 20),
            capacity_bytes: Some(capacity),
            ..Default::default()
        }));
        let (m, rt, rec) = manager(policy_90_80(), stats);
        // Unused and eligible, but smaller than the amount to free.
        rt.set_images(vec![make_image(0, 500 * 1024 * 1024)]);

        assert!(m.garbage_collect(Utc::now()).await.is_err());

        let expected = "Warning FreeDiskSpaceFailed Insufficient free disk space on the node's image filesystem (95% of 10.0 GiB used). Failed to free sufficient space by deleting unused images (freed 524288000 bytes). Investigate disk usage, as it could be used by active images, logs, volumes, or other data.";
        assert_eq!(vec![expected.to_string()], *rec.0.lock().unwrap());
    }

    #[tokio::test]
    async fn garbage_collect_invalid_capacity_emits_event() {
        let stats = MockStats::returning(Ok(FsStats::default()));
        let (m, _, rec) = manager(policy_90_80(), stats);
        let err = m.garbage_collect(Utc::now()).await.unwrap_err();
        assert_eq!("invalid capacity 0 on image filesystem", err.to_string());
        assert_eq!(
            vec!["Warning InvalidDiskCapacity invalid capacity 0 on image filesystem".to_string()],
            *rec.0.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn garbage_collect_image_not_old_enough() {
        let policy = ImageGcPolicy {
            min_age: Duration::from_secs(60),
            ..policy_90_80()
        };
        let (m, rt, _) = manager(policy.clone(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        // 1 image is in use, and another one is not old enough.
        rt.set_pods(vec![make_container(1)]);

        let mut clock = Utc::now();
        m.detect_images(clock).await.unwrap();
        assert_eq!(2, m.image_records_len());
        // No space freed: one image is in use, the other is not old enough.
        get_images_and_free_space(&m, &rt, 1024, 0, 2, clock).await;

        // Move the clock by minAge, then 1 image will be garbage collected.
        clock += CDuration::from_std(policy.min_age).unwrap();
        get_images_and_free_space(&m, &rt, 1024, 1024, 1, clock).await;
    }

    #[tokio::test]
    async fn garbage_collect_image_too_old() {
        let policy = ImageGcPolicy {
            max_age: Duration::from_secs(60),
            ..policy_90_80()
        };
        let (m, rt, _) = manager(policy.clone(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);

        let mut clock = Utc::now();
        let mut images = m.images_in_eviction_order(clock).await.unwrap();
        assert_eq!(1, images.len());
        // Simulate pod having just used this image, but having been GC'd.
        images[0].record.last_used = clock;

        // First GC round should not GC the remaining image: used too recently.
        let old_start_time = clock;
        let images = m
            .free_old_images(images, old_start_time, old_start_time)
            .await
            .unwrap();
        assert_eq!(1, images.len());
        assert_eq!(2, rt.image_len());

        // A nanosecond past maxAge (Go: Step(MaxAge + 1)), 1 image is collected.
        clock += CDuration::from_std(policy.max_age).unwrap() + CDuration::nanoseconds(1);
        let images = m
            .free_old_images(images, clock, old_start_time)
            .await
            .unwrap();
        assert!(images.is_empty());
        assert_eq!(1, rt.image_len());
    }

    #[tokio::test]
    async fn garbage_collect_image_max_age_disabled() {
        let (m, rt, _) = manager(policy_90_80(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);

        let clock = Utc::now();
        let images = m.images_in_eviction_order(clock).await.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(2, rt.image_len());

        let old_start_time = clock;
        let images = m
            .free_old_images(images, old_start_time, old_start_time)
            .await
            .unwrap();
        assert_eq!(1, images.len());
        assert_eq!(2, rt.image_len());

        // Move the clock by a lot: nothing is collected with MaxAge == 0.
        let far = Utc.timestamp_opt(253_402_300_799, 999_999_999).unwrap();
        let images = m
            .free_old_images(images, far, old_start_time)
            .await
            .unwrap();
        assert_eq!(1, images.len());
        assert_eq!(2, rt.image_len());
    }

    /// The max-age pass ahead of the disk-usage pass (`GarbageCollect`): an
    /// old unused image goes even when the filesystem is nowhere near full, and
    /// is counted under reason="age".
    #[tokio::test]
    async fn garbage_collect_frees_old_images_regardless_of_disk_usage() {
        let policy = ImageGcPolicy {
            max_age: Duration::from_secs(60),
            ..policy_90_80()
        };
        // 40% usage: below the high threshold.
        let stats = MockStats::returning(Ok(FsStats {
            available_bytes: Some(600),
            capacity_bytes: Some(1000),
            ..Default::default()
        }));
        let (m, rt, _) = manager(policy, stats);
        rt.set_images(vec![make_image(0, 1024)]);
        let began = Utc::now() - CDuration::hours(1);
        // The image was last used long ago (kubelet has been up for an hour).
        m.detect_images(began).await.unwrap();
        register_metrics();
        let before = IMAGE_GARBAGE_COLLECTED_TOTAL
            .with_label_values(&[REASON_AGE])
            .get();
        m.garbage_collect(began).await.unwrap();
        assert_eq!(0, rt.image_len());
        assert_eq!(
            before + 1,
            IMAGE_GARBAGE_COLLECTED_TOTAL
                .with_label_values(&[REASON_AGE])
                .get()
        );
    }

    // ---- handleImageVolumes (image_gc_manager.go:325-346)

    #[tokio::test]
    #[serial]
    async fn image_volume_mounts_count_as_in_use() {
        let _g = rusternetes_common::feature_gates::with_feature(Feature::ImageVolume, true);
        let (m, rt, _) = manager(ImageGcPolicy::default(), MockStats::none());
        rt.set_images(vec![make_image(0, 1024), make_image(1, 2048)]);
        rt.set_pods(vec![make_container(1)]);
        rt.0.lock().unwrap().image_mounts = vec![image_id(0)];
        let in_use = m.detect_images(zero_time()).await.unwrap();
        assert!(in_use.contains(&image_id(0)));
        assert!(in_use.contains(&image_id(1)));
        get_images_and_free_space(&m, &rt, 4096, 0, 2, Utc::now()).await;
    }

    // ---- TestValidateImageGCPolicy

    #[test]
    fn validate_image_gc_policy() {
        let cases: Vec<(&str, ImageGcPolicy, Option<&str>)> = vec![
            (
                "low < high",
                ImageGcPolicy {
                    high_threshold_percent: 2,
                    low_threshold_percent: 1,
                    ..Default::default()
                },
                None,
            ),
            (
                "high < 0",
                ImageGcPolicy {
                    high_threshold_percent: -1,
                    ..Default::default()
                },
                Some("invalid HighThresholdPercent -1, must be in range [0-100]"),
            ),
            (
                "high > 100",
                ImageGcPolicy {
                    high_threshold_percent: 101,
                    ..Default::default()
                },
                Some("invalid HighThresholdPercent 101, must be in range [0-100]"),
            ),
            (
                "low < 0",
                ImageGcPolicy {
                    low_threshold_percent: -1,
                    ..Default::default()
                },
                Some("invalid LowThresholdPercent -1, must be in range [0-100]"),
            ),
            (
                "low > 100",
                ImageGcPolicy {
                    low_threshold_percent: 101,
                    ..Default::default()
                },
                Some("invalid LowThresholdPercent 101, must be in range [0-100]"),
            ),
            (
                "low > high",
                ImageGcPolicy {
                    high_threshold_percent: 1,
                    low_threshold_percent: 2,
                    ..Default::default()
                },
                Some("LowThresholdPercent 2 can not be higher than HighThresholdPercent 1"),
            ),
        ];
        for (name, policy, expect) in cases {
            let got = ImageGcManager::new(
                Arc::new(FakeRuntime::default()),
                MockStats::none(),
                vec![],
                Arc::new(FakeRecorder::default()),
                policy,
            );
            match (got, expect) {
                (Ok(_), None) => {}
                (Err(e), Some(want)) => assert_eq!(want, e.to_string(), "{name}"),
                (Ok(_), Some(want)) => panic!("{name}: expected error {want:?}"),
                (Err(e), None) => panic!("{name}: unexpected error {e}"),
            }
        }
    }

    #[test]
    fn format_size_matches_upstream() {
        assert_eq!("10.0 GiB", format_size(10 * 1024 * 1024 * 1024));
        assert_eq!("512 B", format_size(512));
        assert_eq!("1.5 KiB", format_size(1536));
    }

    // `imageCache.set` / `GetImageList` (image_gc_manager.go:143-172, 240):
    // the cache is sorted by ByImageSize (size desc, ties by id desc) on write.
    #[tokio::test]
    async fn image_cache_refresh_sorts_by_size_descending() {
        let (m, rt, _) = manager(policy_90_80(), MockStats::returning(Ok(FsStats::default())));
        assert!(m.get_image_list().unwrap().is_empty());
        rt.set_images(vec![
            make_image(1, 3),
            make_image(2, 1),
            make_image(3, 4),
            make_image(4, 3),
        ]);
        m.refresh_image_cache().await;
        let ids: Vec<String> = m
            .get_image_list()
            .unwrap()
            .into_iter()
            .map(|i| i.id)
            .collect();
        assert_eq!(ids, vec!["image-3", "image-4", "image-1", "image-2"]);
    }

    #[tokio::test]
    async fn image_cache_replaced_by_each_refresh() {
        let (m, rt, _) = manager(policy_90_80(), MockStats::returning(Ok(FsStats::default())));
        rt.set_images(vec![make_image(1, 3)]);
        m.refresh_image_cache().await;
        rt.set_images(vec![]);
        m.refresh_image_cache().await;
        assert!(m.get_image_list().unwrap().is_empty());
    }
}
