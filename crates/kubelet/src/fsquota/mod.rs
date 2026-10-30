//! Port of `pkg/volume/util/fsquota` (`quota.go`, `quota_linux.go`): project
//! quota (XFS / ext4 `prjquota`) accounting for local ephemeral volumes.
//!
//! Gated by `LocalStorageCapacityIsolationFSQuotaMonitoring`
//! (`enabledQuotasForMonitoring`, `quota.go:53-55`). Consumers:
//! `fs.DiskUsage` (`pkg/volume/util/fs/fs.go:77,85`) reads
//! [`get_consumption`]/[`get_inodes`] and falls back to `du` when they return
//! `None` or fail; emptyDir `assignQuota`/`teardownDefault`
//! (`pkg/volume/emptydir/empty_dir.go:295-315,527-542`) call
//! [`supports_quotas`]/[`assign_quota`]/[`clear_quota`].
//!
//! Deviations from Go: package-level maps and locks live in an [`FsQuota`]
//! value (a process-wide instance via [`global`]) so tests can run the
//! upstream cases against a mock applier; the four fine-grained locks guarding
//! `supportsQuotasMap`/`backingDevMap`/`mountpointMap`/`dirApplierMap`/
//! `devApplierMap` are one mutex (`fs`), always taken after `quotaLock`
//! (`quota`), which is upstream's order; `resource.Quantity` is an `i64` of
//! bytes (`Quantity.Value()`).

pub mod common;
mod project;

use anyhow::{anyhow, Result};
use common::{
    mount_parse, LinuxVolumeQuotaApplier, LinuxVolumeQuotaProvider, QuotaID, VolumeProvider,
    BAD_QUOTA_ID, MOUNTS_FILE,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::{debug, info, warn};

/// The `mount.Interface` method fsquota uses (`IsLikelyNotMountPoint`).
pub trait MountInterface {
    fn is_likely_not_mount_point(&self, path: &str) -> std::io::Result<bool>;
}

/// The host's mounter (`mount.New("")`).
pub struct RealMounter;

impl MountInterface for RealMounter {
    fn is_likely_not_mount_point(&self, path: &str) -> std::io::Result<bool> {
        crate::pod_dirs::is_likely_not_mount_point(std::path::Path::new(path))
    }
}

type Applier = Arc<dyn LinuxVolumeQuotaApplier>;

/// The maps guarded by `quotaLock`.
#[derive(Default)]
pub struct QuotaMaps {
    /// Pod -> External Pod UID
    pub pod_uid_map: HashMap<String, String>,
    /// Pod -> ID
    pub pod_quota_map: HashMap<String, QuotaID>,
    /// Dir -> ID (for convenience)
    pub dir_quota_map: HashMap<String, QuotaID>,
    /// ID -> pod
    pub quota_pod_map: HashMap<QuotaID, String>,
    /// Directory -> pod
    pub dir_pod_map: HashMap<String, String>,
    /// Pod -> refcount
    pub pod_dir_count_map: HashMap<String, i32>,
    /// ID -> size
    pub quota_size_map: HashMap<QuotaID, i64>,
}

/// The maps guarded by `dirApplierLock`, `supportsQuotasLock`,
/// `backingDevLock` and `mountpointLock`.
#[derive(Default)]
pub struct FsMaps {
    /// Backing device -> applier. This is *not* cleaned up; its size will be
    /// bounded.
    pub dev_applier_map: HashMap<String, Applier>,
    /// Directory -> applier
    pub dir_applier_map: HashMap<String, Applier>,
    pub supports_quotas_map: HashMap<String, bool>,
    /// Directory -> backingDev
    pub backing_dev_map: HashMap<String, String>,
    pub mountpoint_map: HashMap<String, String>,
}

/// The state of the `fsquota` package.
pub struct FsQuota {
    pub providers: Vec<Box<dyn LinuxVolumeQuotaProvider>>,
    pub projects_file: PathBuf,
    pub projid_file: PathBuf,
    /// `quotaIDLock`.
    pub quota_id_lock: Mutex<()>,
    /// `quotaLock`.
    pub quota: Mutex<QuotaMaps>,
    pub fs: Mutex<FsMaps>,
    /// `enabledQuotasForMonitoring`.
    pub enabled: Box<dyn Fn() -> bool + Send + Sync>,
}

/// `enabledQuotasForMonitoring` (`quota.go:53-55`).
fn gate_enabled() -> bool {
    rusternetes_common::feature_gates::gate_enabled(
        "LocalStorageCapacityIsolationFSQuotaMonitoring",
    )
    .unwrap_or(false)
}

/// The process-wide instance (the Go package's globals).
pub fn global() -> &'static FsQuota {
    static G: OnceLock<FsQuota> = OnceLock::new();
    G.get_or_init(|| {
        FsQuota::new(
            vec![Box::new(VolumeProvider)],
            project::PROJECTS_FILE.into(),
            project::PROJID_FILE.into(),
            Box::new(gate_enabled),
        )
    })
}

/// `GetConsumption` of the process-wide instance.
pub fn get_consumption(path: &str) -> Result<Option<i64>> {
    global().get_consumption(path)
}

/// `GetInodes` of the process-wide instance.
pub fn get_inodes(path: &str) -> Result<Option<i64>> {
    global().get_inodes(path)
}

/// `SupportsQuotas` of the process-wide instance.
pub fn supports_quotas(path: &str, user_namespaces_enabled: bool) -> Result<bool> {
    global().supports_quotas(&RealMounter, path, user_namespaces_enabled)
}

/// `AssignQuota` of the process-wide instance.
pub fn assign_quota(
    path: &str,
    poduid: &str,
    bytes: Option<i64>,
    user_namespaces_enabled: bool,
) -> Result<()> {
    global().assign_quota(&RealMounter, path, poduid, bytes, user_namespaces_enabled)
}

/// `ClearQuota` of the process-wide instance.
pub fn clear_quota(path: &str, user_namespaces_enabled: bool) -> Result<()> {
    global().clear_quota(&RealMounter, path, user_namespaces_enabled)
}

/// `detectBackingDevInternal`: separate the innards for ease of testing.
pub fn detect_backing_dev_internal(mountpoint: &str, mounts: &str) -> Result<String> {
    let data = std::fs::read_to_string(mounts)?;
    for line in data.lines() {
        let (device, mount) = mount_parse(line);
        if mount == mountpoint {
            return Ok(device.to_string());
        }
    }
    Err(anyhow!("couldn't find backing device for {mountpoint}"))
}

/// `detectMountpointInternal`: assumes that the path has been fully
/// canonicalized. Breaking this up helps with testing.
pub fn detect_mountpoint_internal(m: &dyn MountInterface, path: &str) -> Result<String> {
    let mut path = path.to_string();
    while !path.is_empty() && path != "/" {
        // per k8s.io/mount-utils/mount_linux this detects all but a bind
        // mount from one part of a mount to another. For our purposes that's
        // fine; we simply want the "true" mount point.
        //
        // IsNotMountPoint proved much more troublesome; it actually scans
        // the mounts, and when a lot of mount/unmount activity takes place,
        // it is not able to get a consistent view of /proc/self/mounts,
        // causing it to time out and report incorrectly.
        let is_not_mount = m.is_likely_not_mount_point(&path)?;
        if !is_not_mount {
            return Ok(path);
        }
        path = match std::path::Path::new(&path).parent() {
            Some(p) => p.to_string_lossy().into_owned(),
            None => String::new(),
        };
    }
    Ok("/".to_string())
}

/// `detectMountpoint`.
fn detect_mountpoint(m: &dyn MountInterface, path: &str) -> Result<String> {
    let abs = std::path::absolute(path)?;
    let xpath = std::fs::canonicalize(abs)?;
    detect_mountpoint_internal(m, &xpath.to_string_lossy())
}

impl FsQuota {
    pub fn new(
        providers: Vec<Box<dyn LinuxVolumeQuotaProvider>>,
        projects_file: PathBuf,
        projid_file: PathBuf,
        enabled: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self {
            providers,
            projects_file,
            projid_file,
            quota_id_lock: Mutex::new(()),
            quota: Mutex::new(QuotaMaps::default()),
            fs: Mutex::new(FsMaps::default()),
            enabled,
        }
    }

    /// `getFSInfo`: returns the mountpoint and backing device, caching both
    /// for the path.
    fn get_fs_info_locked(
        fs: &mut FsMaps,
        m: &dyn MountInterface,
        path: &str,
    ) -> Result<(String, String)> {
        let mountpoint = match fs.mountpoint_map.get(path) {
            Some(mp) => mp.clone(),
            None => detect_mountpoint(m, path)
                .map_err(|e| anyhow!("cannot determine mountpoint for {path}: {e}"))?,
        };
        let backing_dev = match fs.backing_dev_map.get(path) {
            Some(d) => d.clone(),
            None => detect_backing_dev_internal(&mountpoint, MOUNTS_FILE)
                .map_err(|e| anyhow!("cannot determine backing device for {path}: {e}"))?,
        };
        fs.mountpoint_map
            .insert(path.to_string(), mountpoint.clone());
        fs.backing_dev_map
            .insert(path.to_string(), backing_dev.clone());
        Ok((mountpoint, backing_dev))
    }

    fn get_fs_info(&self, m: &dyn MountInterface, path: &str) -> Result<(String, String)> {
        Self::get_fs_info_locked(&mut self.fs.lock().unwrap(), m, path)
    }

    /// `clearFSInfo`.
    fn clear_fs_info(&self, path: &str) {
        let mut fs = self.fs.lock().unwrap();
        fs.mountpoint_map.remove(path);
        fs.backing_dev_map.remove(path);
    }

    /// `getApplier`.
    fn get_applier(&self, path: &str) -> Option<Applier> {
        self.fs.lock().unwrap().dir_applier_map.get(path).cloned()
    }

    fn clear_applier(&self, path: &str) {
        self.fs.lock().unwrap().dir_applier_map.remove(path);
    }

    fn clear_supports_quotas(&self, path: &str) {
        self.fs.lock().unwrap().supports_quotas_map.remove(path);
    }

    /// `setQuotaOnDir`.
    fn set_quota_on_dir(&self, path: &str, id: QuotaID, bytes: i64) -> Result<()> {
        self.get_applier(path)
            .ok_or_else(|| anyhow!("no quota applier for {path}"))?
            .set_quota_on_dir(path, id, bytes)
    }

    /// `GetQuotaOnDir`: the quota ID (if any) that applies to this directory.
    pub fn get_quota_on_dir(&self, m: &dyn MountInterface, path: &str) -> Result<QuotaID> {
        self.get_fs_info(m, path)?;
        self.get_applier(path)
            .ok_or_else(|| anyhow!("no quota applier for {path}"))?
            .get_quota_on_dir(path)
    }

    /// `clearQuotaOnDir`.
    fn clear_quota_on_dir(
        &self,
        m: &dyn MountInterface,
        path: &str,
        user_namespaces_enabled: bool,
    ) -> Result<()> {
        // Since we may be called without path being in the map, we
        // explicitly have to check in this case.
        debug!("clearQuotaOnDir {path}");
        let supports = match self.supports_quotas(m, path, user_namespaces_enabled) {
            Ok(s) => s,
            Err(e) => {
                // Log-and-continue instead of returning an error for now due
                // to unspecified backwards compatibility concerns (a subject
                // to revise).
                info!("Attempt to check for quota support failed: {e}");
                false
            }
        };
        if !supports {
            return Ok(());
        }
        match self.get_quota_on_dir(m, path) {
            Ok(projid) if projid != BAD_QUOTA_ID => {
                // This means that we have a quota on the directory but we
                // can't clear it. That's not good.
                let err = self.set_quota_on_dir(path, projid, 0);
                if let Err(e) = &err {
                    info!("Attempt to clear quota failed: {e}");
                }
                // Even if clearing the quota failed, we still need to try to
                // remove the project ID, or that may be left dangling.
                let err1 = self.remove_project_id(path, projid);
                if let Err(e) = &err1 {
                    info!("Attempt to remove quota ID from system files failed: {e}");
                }
                self.clear_fs_info(path);
                err?;
                err1
            }
            other => {
                // If we couldn't get a quota, that's fine -- there may never
                // have been one, and we have no way to know otherwise.
                info!(
                    "clearQuotaOnDir fails {}",
                    other.err().map(|e| e.to_string()).unwrap_or_default()
                );
                Ok(())
            }
        }
    }

    /// `SupportsQuotas`: does the path support quotas. Caches the applier for
    /// paths that support quotas. For paths that don't, don't cache the result
    /// because nothing will clean it up. However, do cache the
    /// device->applier map; the number of devices is bounded. User namespaces
    /// prevent changes to project IDs on the filesystem, ensuring xfs-quota
    /// metrics' reliability; hence `user_namespaces_enabled` is checked.
    pub fn supports_quotas(
        &self,
        m: &dyn MountInterface,
        path: &str,
        user_namespaces_enabled: bool,
    ) -> Result<bool> {
        if !(self.enabled)() {
            info!("SupportsQuotas called, but quotas disabled");
            return Ok(false);
        }
        if !user_namespaces_enabled {
            info!("SupportQuotas called and LocalStorageCapacityIsolationFSQuotaMonitoring enabled, but pod is not in a user namespace");
            return Ok(false);
        }
        let mut fs = self.fs.lock().unwrap();
        if let Some(s) = fs.supports_quotas_map.get(path) {
            return Ok(*s);
        }
        let (mount, dev) = Self::get_fs_info_locked(&mut fs, m, path)?;
        // Do we know about this device?
        let mut applier: Option<Applier> = fs.dev_applier_map.get(&mount).cloned();
        if applier.is_none() {
            for provider in &self.providers {
                if let Some(a) = provider.get_quota_applier(&mount, &dev) {
                    let a: Applier = Arc::from(a);
                    fs.dev_applier_map.insert(mount.clone(), a.clone());
                    applier = Some(a);
                    break;
                }
            }
        }
        if let Some(a) = applier {
            fs.supports_quotas_map.insert(path.to_string(), true);
            fs.dir_applier_map.insert(path.to_string(), a);
            return Ok(true);
        }
        fs.backing_dev_map.remove(path);
        fs.mountpoint_map.remove(path);
        Ok(false)
    }

    /// `AssignQuota`: assign a quota to the specified directory. Chooses the
    /// quota ID based on the pod UID and path. If the pod UID is identical to
    /// another one known, it may (but presently doesn't) choose the same quota
    /// ID as other volumes in the pod.
    pub fn assign_quota(
        &self,
        m: &dyn MountInterface,
        path: &str,
        poduid: &str,
        bytes: Option<i64>,
        user_namespaces_enabled: bool,
    ) -> Result<()> {
        let Some(ibytes) = bytes else {
            return Err(anyhow!("attempting to assign null quota to {path}"));
        };
        match self.supports_quotas(m, path, user_namespaces_enabled) {
            Ok(true) => {}
            Ok(false) => return Err(anyhow!("quotas not supported on {path}: <nil>")),
            Err(e) => return Err(anyhow!("quotas not supported on {path}: {e}")),
        }
        let mut q = self.quota.lock().unwrap();
        // Current policy is to set individual quotas on each volume, for each
        // new volume we generate a random UUID and we use that as the
        // internal pod uid. From fsquota point of view each volume is
        // attached to a single unique pod. If we decide later that we want to
        // assign one quota for all volumes in a pod, we can simply use
        // poduid parameter directly. If and when we decide permanently that
        // we're going to adopt one quota per volume, we can rip all of the
        // pod code out.
        let external_pod_uid = poduid.to_string();
        let internal_pod_uid = match q.dir_pod_map.get(path) {
            Some(internal) => {
                let existing = q.pod_uid_map.get(internal).cloned().unwrap_or_default();
                if existing != external_pod_uid {
                    return Err(anyhow!(
                        "requesting quota on existing directory {path} but different pod {existing} {external_pod_uid}"
                    ));
                }
                internal.clone()
            }
            None => uuid::Uuid::new_v4().to_string(),
        };
        let oid = match q.pod_quota_map.get(&internal_pod_uid) {
            Some(&oid) => {
                let old = q.quota_size_map.get(&oid).copied().unwrap_or(0);
                if old != ibytes {
                    return Err(anyhow!(
                        "requesting quota of different size: old {old} new {ibytes}"
                    ));
                }
                if q.dir_pod_map.contains_key(path) {
                    return Ok(());
                }
                oid
            }
            None => BAD_QUOTA_ID,
        };
        let id = match self.create_project_id(path, oid) {
            Ok(id) => id,
            Err(e) => return Err(anyhow!("assign quota FAILED {e}")),
        };
        if oid != BAD_QUOTA_ID && oid != id {
            return Err(anyhow!("attempt to reassign quota {oid} to {id}"));
        }
        // When enforcing quotas are enabled, we'll condition this on their
        // being disabled also.
        let mut fsbytes = ibytes;
        if fsbytes > 0 {
            fsbytes = -1;
        }
        match self.set_quota_on_dir(path, id, fsbytes) {
            Ok(()) => {
                q.quota_pod_map.insert(id, internal_pod_uid.clone());
                q.quota_size_map.insert(id, ibytes);
                q.pod_quota_map.insert(internal_pod_uid.clone(), id);
                q.dir_quota_map.insert(path.to_string(), id);
                q.dir_pod_map
                    .insert(path.to_string(), internal_pod_uid.clone());
                q.pod_uid_map
                    .insert(internal_pod_uid.clone(), external_pod_uid);
                *q.pod_dir_count_map.entry(internal_pod_uid).or_insert(0) += 1;
                debug!("Assigning quota ID {id} (request limit {ibytes}, actual limit {fsbytes}) to {path}");
                Ok(())
            }
            Err(e) => {
                let _ = self.remove_project_id(path, id);
                Err(anyhow!("assign quota FAILED {e}"))
            }
        }
    }

    /// `GetConsumption`: retrieve the consumption (in bytes) of the
    /// directory. `Ok(None)` means the directory is not under quota
    /// management.
    pub fn get_consumption(&self, path: &str) -> Result<Option<i64>> {
        // Note that we actually need to hold the lock at least through
        // running the quota command, so it can't get recycled behind our
        // back.
        let q = self.quota.lock().unwrap();
        // No applier means directory is not under quota management.
        let Some(applier) = self.get_applier(path) else {
            return Ok(None);
        };
        let id = q.dir_quota_map.get(path).copied().unwrap_or(BAD_QUOTA_ID);
        Ok(Some(applier.get_consumption(path, id)?))
    }

    /// `GetInodes`: retrieve the number of inodes in use under the directory.
    pub fn get_inodes(&self, path: &str) -> Result<Option<i64>> {
        let q = self.quota.lock().unwrap();
        let Some(applier) = self.get_applier(path) else {
            return Ok(None);
        };
        let id = q.dir_quota_map.get(path).copied().unwrap_or(BAD_QUOTA_ID);
        Ok(Some(applier.get_inodes(path, id)?))
    }

    /// `ClearQuota`: remove the quota assigned to a directory.
    pub fn clear_quota(
        &self,
        m: &dyn MountInterface,
        path: &str,
        user_namespaces_enabled: bool,
    ) -> Result<()> {
        info!("ClearQuota {path}");
        if !(self.enabled)() {
            return Err(anyhow!("clearQuota called, but quotas disabled"));
        }
        let mut q = self.quota.lock().unwrap();
        let Some(poduid) = q.dir_pod_map.get(path).cloned() else {
            // Nothing in the map either means that there was no quota to
            // begin with or that we're clearing a stale directory, so if we
            // find a quota, just remove it. The process of clearing the quota
            // requires that an applier be found, which needs to be cleaned
            // up.
            let r = self.clear_quota_on_dir(m, path, user_namespaces_enabled);
            // `defer clearSupportsQuotas; defer clearApplier`: LIFO.
            self.clear_applier(path);
            self.clear_supports_quotas(path);
            return r;
        };
        if !q.pod_quota_map.contains_key(&poduid) {
            return Err(anyhow!("clearQuota: No quota available for {path}"));
        }
        let dir_id = q.dir_quota_map.get(path).copied().unwrap_or(BAD_QUOTA_ID);
        let projid = match self.get_quota_on_dir(m, path) {
            Ok(id) => id,
            Err(e) => {
                // Log-and-continue instead of returning an error for now due
                // to unspecified backwards compatibility concerns.
                info!("Attempt to check quota ID {dir_id} on dir {path} failed: {e}");
                BAD_QUOTA_ID
            }
        };
        if projid != dir_id {
            return Err(anyhow!(
                "expected quota ID {dir_id} on dir {path} does not match actual {projid}"
            ));
        }
        let count = q.pod_dir_count_map.get(&poduid).copied();
        let err: Result<()>;
        if count.is_none_or(|c| c <= 1) {
            err = self.clear_quota_on_dir(m, path, user_namespaces_enabled);
            // This error should be noted; we still need to clean up and
            // otherwise handle in the same way.
            if let Err(e) = &err {
                info!("Unable to clear quota {dir_id} {path}: {e}");
            }
            if let Some(qid) = q.pod_quota_map.get(&poduid).copied() {
                q.quota_size_map.remove(&qid);
                q.quota_pod_map.remove(&qid);
            }
            q.pod_dir_count_map.remove(&poduid);
            q.pod_quota_map.remove(&poduid);
            q.pod_uid_map.remove(&poduid);
        } else {
            err = self.remove_project_id(path, projid);
            let c = q.pod_dir_count_map.entry(poduid.clone()).or_insert(0);
            *c -= 1;
            debug!(
                "Not clearing quota for pod {poduid}; still {} dirs outstanding",
                *c
            );
        }
        q.dir_pod_map.remove(path);
        q.dir_quota_map.remove(path);
        self.clear_supports_quotas(path);
        self.clear_applier(path);
        err.map_err(|e| {
            warn!("unable to clear quota for {path}: {e}");
            anyhow!("unable to clear quota for {path}: {e}")
        })
    }
}
