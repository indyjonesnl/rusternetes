//! Port of `pkg/kubelet/images/image_gc_manager.go` (`realImageGCManager`).
//!
//! Tracks every image the CRI runtime holds, records when each was first
//! detected and last used, and garbage-collects unused ones: by age
//! (`imageMaximumGCAge`) and, once the image filesystem crosses
//! `imageGCHighThresholdPercent`, least-recently-used first down to
//! `imageGCLowThresholdPercent`, never touching an image younger than
//! `imageMinimumGCAge`, in use, or pinned.
//!
//! The CRI contract is the only way this touches the runtime: `ListImages`,
//! `RemoveImage`, `ImageFsInfo` and the container list/status calls, all behind
//! the [`ImageGcRuntime`] / [`StatsProvider`] traits (upstream `container.Runtime`
//! / `StatsProvider`) so the manager is unit-tested against a fake runtime exactly
//! as `image_gc_manager_test.go` does.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, Once};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use prometheus::{IntCounterVec, Opts};
use rusternetes_common::feature_gates::{enabled, Feature};

/// `ImageGCPeriod` (`pkg/kubelet/kubelet.go:230`): how often `GarbageCollect` runs.
pub const IMAGE_GC_PERIOD: Duration = Duration::from_secs(5 * 60);

/// `imageIndexTupleFormat` (`image_gc_manager.go`): with `RuntimeClassInImageCriAPI`
/// an imageRecord is indexed as `imageId,runtimeHandler`.
const IMAGE_INDEX_TUPLE_SEPARATOR: &str = ",";

/// `ImageGarbageCollectedTotalReasonAge` (`image_gc_manager.go`).
pub const REASON_AGE: &str = "age";
/// `ImageGarbageCollectedTotalReasonSpace` (`image_gc_manager.go`).
pub const REASON_SPACE: &str = "space";

/// `events.InvalidDiskCapacity` (`pkg/kubelet/events/event.go:92`).
pub const INVALID_DISK_CAPACITY: &str = "InvalidDiskCapacity";
/// `events.FreeDiskSpaceFailed` (`pkg/kubelet/events/event.go:93`).
pub const FREE_DISK_SPACE_FAILED: &str = "FreeDiskSpaceFailed";
/// `events.InvalidImageID` (`pkg/kubelet/events/event.go:91`).
pub const INVALID_IMAGE_ID: &str = "InvalidImageID";
/// `events.ImageGCFailed` (`pkg/kubelet/events/event.go:77`).
pub const IMAGE_GC_FAILED: &str = "ImageGCFailed";

/// `metrics.ImageGarbageCollectedTotal` (`pkg/kubelet/metrics/metrics.go:1005-1013`),
/// `StabilityLevel: ALPHA` so the served help carries the `[ALPHA] ` prefix.
pub static IMAGE_GARBAGE_COLLECTED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "kubelet_image_garbage_collected_total",
            "[ALPHA] Total number of images garbage collected by the kubelet, whether through disk usage or image age.",
        ),
        &["reason"],
    )
    .expect("valid image gc counter definition")
});

static REGISTER_METRICS: Once = Once::new();

pub fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        prometheus::default_registry()
            .register(Box::new(IMAGE_GARBAGE_COLLECTED_TOTAL.clone()))
            .expect("image gc counter registered once");
    });
}

/// `container.ImageSpec` (the fields the GC reads/writes).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageSpec {
    pub image: String,
    pub runtime_handler: String,
}

/// `container.Image`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Image {
    pub id: String,
    pub size: i64,
    pub runtime_handler: String,
    pub pinned: bool,
    /// `Image.RepoTags` / `RepoDigests` (reported as node.status.images names).
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
}

/// The `container.Container` fields `detectImages` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcContainer {
    pub id: String,
    pub name: String,
    pub image: String,
    pub image_id: String,
    pub image_ref: String,
    pub image_runtime_handler: String,
}

/// The `container.Pod` fields `detectImages` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcPod {
    pub id: String,
    pub namespace: String,
    pub name: String,
    pub containers: Vec<GcContainer>,
}

/// `statsapi.FsStats` (only the fields `GarbageCollect` reads).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsStats {
    pub capacity_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    /// `FsStats.Inodes` / `InodesFree` (the `imagefs.inodesFree` eviction signal).
    pub inodes: Option<u64>,
    pub inodes_free: Option<u64>,
    /// Device id of the filesystem (cadvisor `FsInfo.Device`), used to decide
    /// whether the image filesystem is dedicated (`HasDedicatedImageFs`).
    pub device: Option<u64>,
}

/// The subset of `container.Runtime` the image GC manager calls.
#[async_trait]
pub trait ImageGcRuntime: Send + Sync {
    /// `Runtime.ListImages`.
    async fn list_images(&self) -> anyhow::Result<Vec<Image>>;
    /// `Runtime.GetPods(ctx, all=true)`.
    async fn get_pods(&self) -> anyhow::Result<Vec<GcPod>>;
    /// `Runtime.GetContainerStatus(...).Mounts`, reduced to the `Mount.Image.Image`
    /// of every image mount (`handleImageVolumes`).
    async fn container_image_mounts(
        &self,
        pod_id: &str,
        container_id: &str,
    ) -> anyhow::Result<Vec<String>>;
    /// `Runtime.RemoveImage`.
    async fn remove_image(&self, image: &ImageSpec) -> anyhow::Result<()>;
}

/// `images.StatsProvider`.
#[async_trait]
pub trait StatsProvider: Send + Sync {
    /// `ImageFsStats` (the image filesystem's stats).
    async fn image_fs_stats(&self) -> anyhow::Result<FsStats>;
}

/// `record.EventRecorder` bound to the node (`im.recorder` + `im.nodeRef`).
pub trait NodeEventSink: Send + Sync {
    fn warning(&self, reason: &str, message: &str);
}

/// `PostImageGCHook`: called with the images remaining after a GC.
pub type PostImageGcHook = Box<dyn Fn(&[String], DateTime<Utc>) + Send + Sync>;

/// `ImageGCPolicy`.
#[derive(Debug, Clone, Default)]
pub struct ImageGcPolicy {
    pub high_threshold_percent: i32,
    pub low_threshold_percent: i32,
    pub min_age: Duration,
    pub max_age: Duration,
}

impl ImageGcPolicy {
    /// `images.ImageGCPolicy{MinAge, HighThresholdPercent, LowThresholdPercent, MaxAge}`
    /// from the KubeletConfiguration (`pkg/kubelet/kubelet.go:509-515`).
    pub fn from_config(c: &crate::config::KubeletConfiguration) -> Self {
        Self {
            high_threshold_percent: c.effective_image_gc_high_threshold_percent(),
            low_threshold_percent: c.effective_image_gc_low_threshold_percent(),
            min_age: c.effective_image_minimum_gc_age(),
            max_age: c.effective_image_maximum_gc_age(),
        }
    }
}

/// `imageRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRecord {
    pub runtime_handler_used_to_pull_image: String,
    pub first_detected: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub size: i64,
    pub pinned: bool,
}

/// `evictionInfo`.
#[derive(Debug, Clone)]
pub struct EvictionInfo {
    pub id: String,
    pub record: ImageRecord,
}

/// Go's zero `time.Time` (0001-01-01T00:00:00Z).
pub fn zero_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("valid zero time")
}

/// `formatSize` (`image_gc_manager.go`): a human-readable size.
pub fn format_size(size_bytes: i64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    const TIB: f64 = 1024.0 * GIB;
    let size = size_bytes as f64;
    if size < KIB {
        format!("{} B", size as i64)
    } else if size < MIB {
        format!("{:.1} KiB", size / KIB)
    } else if size < GIB {
        format!("{:.1} MiB", size / MIB)
    } else if size < TIB {
        format!("{:.1} GiB", size / GIB)
    } else {
        format!("{:.1} TiB", size / TIB)
    }
}

/// `realImageGCManager`.
pub struct ImageGcManager {
    runtime: Arc<dyn ImageGcRuntime>,
    policy: ImageGcPolicy,
    stats_provider: Arc<dyn StatsProvider>,
    recorder: Arc<dyn NodeEventSink>,
    post_gc_hooks: Vec<PostImageGcHook>,
    image_records: Mutex<HashMap<String, ImageRecord>>,
    /// `imageCache`: the latest `ListImages` result, sorted by size.
    image_cache: Mutex<Vec<Image>>,
}

/// `getImageTuple`: with `RuntimeClassInImageCriAPI` records are indexed by
/// `imageID,runtimeHandler`; no handler means the bare image id.
fn get_image_tuple(image_id: &str, runtime_handler: &str) -> String {
    if runtime_handler.is_empty() {
        return image_id.to_string();
    }
    format!("{image_id}{IMAGE_INDEX_TUPLE_SEPARATOR}{runtime_handler}")
}

/// `getImageIDFromTuple`.
fn get_image_id_from_tuple(image: &str) -> &str {
    image
        .split(IMAGE_INDEX_TUPLE_SEPARATOR)
        .next()
        .unwrap_or("")
}

/// Go `time.Duration` for a std duration, saturating (a `chrono::Duration` can
/// not hold the full `u64` seconds range).
fn to_chrono(d: Duration) -> chrono::Duration {
    chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)
}

impl ImageGcManager {
    /// `NewImageGCManager`.
    pub fn new(
        runtime: Arc<dyn ImageGcRuntime>,
        stats_provider: Arc<dyn StatsProvider>,
        post_gc_hooks: Vec<PostImageGcHook>,
        recorder: Arc<dyn NodeEventSink>,
        policy: ImageGcPolicy,
    ) -> anyhow::Result<Self> {
        // Validate policy.
        if !(0..=100).contains(&policy.high_threshold_percent) {
            anyhow::bail!(
                "invalid HighThresholdPercent {}, must be in range [0-100]",
                policy.high_threshold_percent
            );
        }
        if !(0..=100).contains(&policy.low_threshold_percent) {
            anyhow::bail!(
                "invalid LowThresholdPercent {}, must be in range [0-100]",
                policy.low_threshold_percent
            );
        }
        if policy.low_threshold_percent > policy.high_threshold_percent {
            anyhow::bail!(
                "LowThresholdPercent {} can not be higher than HighThresholdPercent {}",
                policy.low_threshold_percent,
                policy.high_threshold_percent
            );
        }
        register_metrics();
        Ok(Self {
            runtime,
            policy,
            stats_provider,
            recorder,
            post_gc_hooks,
            image_records: Mutex::new(HashMap::new()),
            image_cache: Mutex::new(Vec::new()),
        })
    }

    /// `ImageFsStats` of the manager's stats provider; the eviction manager
    /// observes the `imagefs.*` signals from it (`makeSignalObservations`).
    pub async fn image_fs_stats(&self) -> anyhow::Result<FsStats> {
        self.stats_provider.image_fs_stats().await
    }

    pub fn image_records_len(&self) -> usize {
        self.image_records.lock().unwrap().len()
    }

    pub fn get_image_record(&self, name: &str) -> Option<ImageRecord> {
        self.image_records.lock().unwrap().get(name).cloned()
    }

    /// `imageCache.set`: sorts by `ByImageSize` (size descending, ties by id
    /// descending) and replaces the cache.
    pub fn set_image_cache(&self, mut images: Vec<Image>) {
        images.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| b.id.cmp(&a.id)));
        *self.image_cache.lock().unwrap() = images;
    }

    /// `GetImageList`: the cached image list, sorted by size descending.
    pub fn get_image_list(&self) -> anyhow::Result<Vec<Image>> {
        Ok(self.image_cache.lock().unwrap().clone())
    }

    /// `Start`'s second goroutine (`image_gc_manager.go:Start`): refresh the
    /// image cache from `ListImages` every 30 seconds (first run immediately,
    /// as `wait.Until`).
    pub async fn run_image_cache_loop(self: Arc<Self>) {
        let mut t = tokio::time::interval(Duration::from_secs(30));
        loop {
            t.tick().await;
            self.refresh_image_cache().await;
        }
    }

    /// One iteration of the cache-refresh goroutine.
    pub async fn refresh_image_cache(&self) {
        match self.runtime.list_images().await {
            Ok(images) => self.set_image_cache(images),
            Err(err) => tracing::warn!(%err, "Failed to update image list"),
        }
    }

    /// `Start`'s first goroutine (`image_gc_manager.go:Start`): `detectImages`
    /// every 5 minutes, forever.
    pub async fn run_detect_loop(self: Arc<Self>) {
        let mut t = tokio::time::interval(Duration::from_secs(5 * 60));
        loop {
            t.tick().await;
            if let Err(err) = self.detect_images(Utc::now()).await {
                tracing::info!(%err, "Failed to monitor images");
            }
        }
    }

    /// The image half of `StartGarbageCollection` (`pkg/kubelet/kubelet.go:1644-1673`):
    /// `GarbageCollect` every [`IMAGE_GC_PERIOD`] (first run immediately, as
    /// `wait.Until`), with an `ImageGCFailed` event only for repeated failures.
    pub async fn run_gc_loop(self: Arc<Self>) {
        // When the high threshold is set to 100, and the max age is 0 (or the
        // max age feature is disabled) stub the image GC manager.
        if self.policy.high_threshold_percent == 100 && self.policy.max_age.is_zero() {
            tracing::debug!(
                "ImageGCHighThresholdPercent is set 100 and ImageMaximumGCAge is 0, Disable image GC"
            );
            return;
        }

        let mut prev_image_gc_failed = false;
        let began_gc = Utc::now();
        let mut t = tokio::time::interval(IMAGE_GC_PERIOD);
        loop {
            t.tick().await;
            match self.garbage_collect(began_gc).await {
                Err(err) => {
                    if prev_image_gc_failed {
                        tracing::error!(%err, "Image garbage collection failed multiple times in a row");
                        // Only create an event for repeated failures.
                        self.recorder.warning(IMAGE_GC_FAILED, &err.to_string());
                    } else {
                        tracing::error!(%err, "Image garbage collection failed once. Stats initialization may not have completed yet");
                    }
                    prev_image_gc_failed = true;
                }
                Ok(()) => {
                    prev_image_gc_failed = false;
                    tracing::debug!("Image garbage collection succeeded");
                }
            }
        }
    }

    /// `detectImages`.
    pub async fn detect_images(
        &self,
        detect_time: DateTime<Utc>,
    ) -> anyhow::Result<HashSet<String>> {
        let runtime_class_in_image_cri_api = enabled(Feature::RuntimeClassInImageCriApi);
        let mut images_in_use: HashSet<String> = HashSet::new();

        let images = self.runtime.list_images().await?;
        let pods = self.runtime.get_pods().await?;

        // Make a set of images in use by containers.
        for pod in &pods {
            for container in &pod.containers {
                self.handle_image_volumes(&mut images_in_use, container, pod, &images)
                    .await?;

                if !runtime_class_in_image_cri_api {
                    tracing::trace!(pod = %pod.name, container = %container.name, image = %container.image, image_id = %container.image_id, image_ref = %container.image_ref, "Container uses image");
                    images_in_use.insert(container.image_id.clone());
                } else {
                    let image_key =
                        get_image_tuple(&container.image_id, &container.image_runtime_handler);
                    tracing::trace!(pod = %pod.name, container = %container.name, image = %container.image, image_id = %container.image_id, image_ref = %container.image_ref, %image_key, "Container uses image");
                    images_in_use.insert(image_key);
                }
            }
        }

        // Add new images and record those being used.
        let now = Utc::now();
        let mut current_images: HashSet<String> = HashSet::new();
        let mut records = self.image_records.lock().unwrap();
        for image in &images {
            let image_key = if !runtime_class_in_image_cri_api {
                image.id.clone()
            } else {
                get_image_tuple(&image.id, &image.runtime_handler)
            };

            current_images.insert(image_key.clone());

            // New image, set it as detected now.
            let record = records.entry(image_key.clone()).or_insert_with(|| {
                tracing::trace!(image_id = %image_key, runtime_handler = %image.runtime_handler, "Image ID is new");
                ImageRecord {
                    first_detected: detect_time,
                    runtime_handler_used_to_pull_image: image.runtime_handler.clone(),
                    last_used: zero_time(),
                    size: 0,
                    pinned: false,
                }
            });

            // Set last used time to now if the image is being used.
            if images_in_use.contains(&image_key) {
                tracing::trace!(image_id = %image_key, last_used = %now, "Setting Image ID lastUsed");
                record.last_used = now;
            }

            record.size = image.size;
            record.pinned = image.pinned;
        }

        // Remove old images from our records.
        records.retain(|image, _| {
            let keep = current_images.contains(image);
            if !keep {
                tracing::trace!(image_id = %image, "Image ID is no longer present; removing from imageRecords");
            }
            keep
        });

        Ok(images_in_use)
    }

    /// `handleImageVolumes`: image volumes count as images in use.
    async fn handle_image_volumes(
        &self,
        images_in_use: &mut HashSet<String>,
        container: &GcContainer,
        pod: &GcPod,
        images: &[Image],
    ) -> anyhow::Result<()> {
        if !enabled(Feature::ImageVolume) {
            return Ok(());
        }

        let mounts = self
            .runtime
            .container_image_mounts(&pod.id, &container.id)
            .await
            .map_err(|e| anyhow::anyhow!("get container status: {e}"))?;

        for mount_image in &mounts {
            for image in images {
                if *mount_image == image.id {
                    tracing::trace!(pod = %pod.name, container = %container.name, image_id = %image.id, "Container uses image as mount");
                    images_in_use.insert(image.id.clone());
                }
            }
        }

        Ok(())
    }

    /// `GarbageCollect`.
    pub async fn garbage_collect(&self, began_gc: DateTime<Utc>) -> anyhow::Result<()> {
        let free_time = Utc::now();
        let images = self.images_in_eviction_order(free_time).await?;

        let images = self.free_old_images(images, free_time, began_gc).await?;

        // Get disk usage on disk holding images.
        let fs_stats = self.stats_provider.image_fs_stats().await?;

        // `capacity = int64(*fsStats.CapacityBytes)`.
        let capacity: i64 = fs_stats.capacity_bytes.unwrap_or(0) as i64;
        let mut available: i64 = fs_stats.available_bytes.unwrap_or(0) as i64;

        if available > capacity {
            tracing::info!(available, capacity, "Availability is larger than capacity");
            available = capacity;
        }

        // Check valid capacity.
        if capacity == 0 {
            let err = anyhow::anyhow!("invalid capacity 0 on image filesystem");
            self.recorder
                .warning(INVALID_DISK_CAPACITY, &err.to_string());
            return Err(err);
        }

        // If over the max threshold, free enough to place us at the lower threshold.
        let usage_percent = 100 - (available * 100 / capacity) as i32;
        if usage_percent >= self.policy.high_threshold_percent {
            let amount_to_free =
                capacity * i64::from(100 - self.policy.low_threshold_percent) / 100 - available;
            tracing::info!(
                usage = usage_percent,
                high_threshold = self.policy.high_threshold_percent,
                amount_to_free,
                low_threshold = self.policy.low_threshold_percent,
                "Disk usage on image filesystem is over the high threshold, trying to free bytes down to the low threshold"
            );
            // Failed to delete images, eg due to a read-only filesystem.
            let (remaining_images, freed) = self
                .free_space(amount_to_free, free_time, images)
                .await
                .map_err(|(_, e)| e)?;

            self.run_post_gc_hooks(&remaining_images, free_time);

            if freed < amount_to_free {
                // This usually means the disk is full for reasons other than container
                // images, such as logs, volumes, or other files. However, it could also
                // be due to an unusually large number or size of in-use container images.
                let message = format!(
                    "Insufficient free disk space on the node's image filesystem ({usage_percent}% of {} used). Failed to free sufficient space by deleting unused images (freed {freed} bytes). Investigate disk usage, as it could be used by active images, logs, volumes, or other data.",
                    format_size(capacity)
                );
                self.recorder.warning(FREE_DISK_SPACE_FAILED, &message);
                anyhow::bail!("{message}");
            }
        }

        Ok(())
    }

    /// `runPostGCHooks`.
    fn run_post_gc_hooks(&self, remaining_images: &[String], gc_start_time: DateTime<Utc>) {
        for h in &self.post_gc_hooks {
            h(remaining_images, gc_start_time);
        }
    }

    /// `freeOldImages`.
    pub async fn free_old_images(
        &self,
        images: Vec<EvictionInfo>,
        free_time: DateTime<Utc>,
        began_gc: DateTime<Utc>,
    ) -> anyhow::Result<Vec<EvictionInfo>> {
        if self.policy.max_age.is_zero() {
            return Ok(images);
        }

        // Wait until the MaxAge has passed since the Kubelet has started,
        // or else we risk prematurely garbage collecting images.
        let max_age = to_chrono(self.policy.max_age);
        if free_time - began_gc <= max_age {
            return Ok(images);
        }
        let mut deletion_errors: Vec<anyhow::Error> = Vec::new();
        let mut remaining_images = Vec::new();
        for image in images {
            tracing::trace!(image_id = %image.id, "Evaluating image ID for possible garbage collection based on image age");
            // Evaluate whether image is older than MaxAge.
            if free_time - image.record.last_used > max_age {
                if let Err(err) = self.free_image(&image, REASON_AGE).await {
                    deletion_errors.push(err);
                    remaining_images.push(image);
                    continue;
                }
                continue;
            }
            remaining_images.push(image);
        }
        if !deletion_errors.is_empty() {
            anyhow::bail!(
                "wanted to free images older than {:?}, encountered errors in image deletion: {}",
                self.policy.max_age,
                aggregate(&deletion_errors)
            );
        }
        Ok(remaining_images)
    }

    /// `DeleteUnusedImages`.
    pub async fn delete_unused_images(&self) -> anyhow::Result<()> {
        tracing::info!("Attempting to delete unused images");
        let free_time = Utc::now();

        let images = self.images_in_eviction_order(free_time).await?;

        let (remaining_images, _) = self
            .free_space(i64::MAX, free_time, images)
            .await
            .map_err(|(_, e)| e)?;

        self.run_post_gc_hooks(&remaining_images, free_time);
        Ok(())
    }

    /// `freeSpace`: tries to free `bytes_to_free` worth of images. Returns the
    /// images still available and the bytes freed; on error the bytes freed are
    /// returned alongside it (upstream returns them in every case).
    pub async fn free_space(
        &self,
        bytes_to_free: i64,
        free_time: DateTime<Utc>,
        images: Vec<EvictionInfo>,
    ) -> Result<(Vec<String>, i64), (i64, anyhow::Error)> {
        // Delete unused images until we've freed up enough space.
        let mut deletion_errors: Vec<anyhow::Error> = Vec::new();
        let mut space_freed: i64 = 0;
        let mut images_left: Vec<String> = Vec::new();
        for image in &images {
            tracing::trace!(image_id = %image.id, runtime_handler = %image.record.runtime_handler_used_to_pull_image, "Evaluating image ID for possible garbage collection based on disk usage");
            // Images that are currently in used were given a newer lastUsed.
            if image.record.last_used >= free_time {
                images_left.push(image.id.clone());
                tracing::trace!(image_id = %image.id, last_used = %image.record.last_used, %free_time, "Image ID was used too recently, not eligible for garbage collection");
                continue;
            }

            // Avoid garbage collect the image if the image is not old enough.
            // In such a case, the image may have just been pulled down, and will be used by a container right away.
            if free_time - image.record.first_detected < to_chrono(self.policy.min_age) {
                images_left.push(image.id.clone());
                tracing::trace!(image_id = %image.id, min_age = ?self.policy.min_age, "Image ID's age is less than the policy's minAge, not eligible for garbage collection");
                continue;
            }

            if let Err(err) = self.free_image(image, REASON_SPACE).await {
                deletion_errors.push(err);
                images_left.push(image.id.clone());
                continue;
            }
            space_freed += image.record.size;

            if space_freed >= bytes_to_free {
                break;
            }
        }

        if !deletion_errors.is_empty() {
            return Err((
                space_freed,
                anyhow::anyhow!(
                    "wanted to free {bytes_to_free} bytes, but freed {space_freed} bytes space with errors in image deletion: {}",
                    aggregate(&deletion_errors)
                ),
            ));
        }
        Ok((images_left, space_freed))
    }

    /// `freeImage`.
    async fn free_image(&self, image: &EvictionInfo, reason: &str) -> anyhow::Result<()> {
        let runtime_class_in_image_cri_api = enabled(Feature::RuntimeClassInImageCriApi);
        // Remove image. Continue despite errors.
        tracing::info!(image_id = %image.id, size = image.record.size, runtime_handler = %image.record.runtime_handler_used_to_pull_image, "Removing image to free bytes");
        self.runtime
            .remove_image(&ImageSpec {
                image: image.id.clone(),
                runtime_handler: image.record.runtime_handler_used_to_pull_image.clone(),
            })
            .await?;

        let image_key = if runtime_class_in_image_cri_api {
            get_image_tuple(&image.id, &image.record.runtime_handler_used_to_pull_image)
        } else {
            image.id.clone()
        };

        self.image_records.lock().unwrap().remove(&image_key);

        IMAGE_GARBAGE_COLLECTED_TOTAL
            .with_label_values(&[reason])
            .inc();
        Ok(())
    }

    /// `imagesInEvictionOrder`: every image record not in use and not pinned,
    /// sorted by last used (ties broken by first detected).
    pub async fn images_in_eviction_order(
        &self,
        free_time: DateTime<Utc>,
    ) -> anyhow::Result<Vec<EvictionInfo>> {
        let runtime_class_in_image_cri_api = enabled(Feature::RuntimeClassInImageCriApi);
        let images_in_use = self.detect_images(free_time).await?;

        let records = self.image_records.lock().unwrap();

        // Get all images in eviction order.
        let mut images: Vec<EvictionInfo> = Vec::with_capacity(records.len());
        for (image, record) in records.iter() {
            if images_in_use.contains(image) {
                tracing::trace!(image_id = %image, "Image ID is being used");
                continue;
            }
            // Check if image is pinned, prevent garbage collection.
            if record.pinned {
                tracing::trace!(image_id = %image, "Image is pinned, skipping garbage collection");
                continue;
            }
            if !runtime_class_in_image_cri_api {
                images.push(EvictionInfo {
                    id: image.clone(),
                    record: record.clone(),
                });
            } else {
                let image_id = get_image_id_from_tuple(image);
                // Ensure imageID is valid or else continue.
                if image_id.is_empty() {
                    self.recorder.warning(
                        INVALID_IMAGE_ID,
                        &format!("ImageID is not valid, skipping, image: {image}"),
                    );
                    continue;
                }
                images.push(EvictionInfo {
                    id: image_id.to_string(),
                    record: record.clone(),
                });
            }
        }
        // `byLastUsedAndDetected`.
        images.sort_by(|a, b| {
            a.record
                .last_used
                .cmp(&b.record.last_used)
                .then(a.record.first_detected.cmp(&b.record.first_detected))
        });
        Ok(images)
    }
}

/// `errors.NewAggregate(...).Error()`: `[e1, e2]` (a single error prints bare).
fn aggregate(errs: &[anyhow::Error]) -> String {
    let msgs: Vec<String> = errs.iter().map(|e| e.to_string()).collect();
    if msgs.len() == 1 {
        msgs[0].clone()
    } else {
        format!("[{}]", msgs.join(", "))
    }
}
