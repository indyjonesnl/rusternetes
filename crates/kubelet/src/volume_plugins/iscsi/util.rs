//! Port of `pkg/volume/iscsi/iscsi_util.go`: `ISCSIUtil` (`AttachDisk`,
//! `DetachDisk`, `DetachBlockISCSIDisk`, `persistISCSI`) and the `iscsiadm`
//! sequences behind them.

use super::{
    uncertain_progress, DiskManager, IscsiDisk, IscsiDiskMounter, IscsiDiskUnmapper,
    IscsiDiskUnmounter, ISCSI_PLUGIN_NAME,
};
use crate::volume_plugins::util::device_util::DeviceUtil;
use crate::volume_plugins::util::exec::ExecError;
use crate::volume_plugins::util::mount_utils::path_exists;
use crate::volume_plugins::VolumeHost;
use anyhow::{anyhow, bail, Result};
use rusternetes_common::resources::volume::PersistentVolumeMode;
use std::collections::BTreeMap;
use std::io;
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{error, info, warn};

/// Minimum number of paths that the volume plugin considers enough when a
/// multipath volume is requested (`iscsi_util.go:45`).
const MIN_MULTIPATH_COUNT: usize = 2;

/// Minimal number of attempts to attach all paths of a multipath volumes. If
/// at least `MIN_MULTIPATH_COUNT` paths are available after this nr. of
/// attempts, the volume plugin continues with mounting the volume
/// (`iscsi_util.go:49`).
const MIN_ATTACH_ATTEMPTS: usize = 2;

/// Total number of attempts to attach at least `MIN_MULTIPATH_COUNT` paths. If
/// there are less than `MIN_MULTIPATH_COUNT`, the volume plugin tries to attach
/// the remaining paths at least this number of times in total. After
/// `MAX_ATTACH_ATTEMPTS` attempts, it mounts even a single path
/// (`iscsi_util.go:54`).
const MAX_ATTACH_ATTEMPTS: usize = 5;

/// How many seconds to wait for a multipath device if at least two paths are
/// available (`iscsi_util.go:57`).
const MULTIPATH_DEVICE_TIMEOUT: usize = 10;

/// How many seconds to wait for a device/path to appear before giving up
/// (`iscsi_util.go:60`).
const DEVICE_DISCOVERY_TIMEOUT: usize = 30;

/// 'iscsiadm' error code stating that a session is logged in (`iscsi_util.go:64`).
/// See <https://github.com/open-iscsi/open-iscsi/blob/7d121d12ad6ba7783308c25ffd338a9fa0cc402b/include/iscsi_err.h#L37-L38>
const ISCSIADM_ERROR_SESS_EXISTS: i32 = 15;

/// iscsiadm exit code for "session could not be found" (`iscsi_util.go:67`).
const EXIT_ISCSI_ERR_SESS_NOT_FOUND: i32 = 2;
/// iscsiadm exit code for "no records/targets/sessions/portals found to
/// execute operation on." (`iscsi_util.go:69`).
const EXIT_ISCSI_ERR_NO_OBJS_FOUND: i32 = 21;

/// `chapSt` (`iscsi_util.go:73-77`).
const CHAP_ST: [&str; 4] = [
    "discovery.sendtargets.auth.username",
    "discovery.sendtargets.auth.password",
    "discovery.sendtargets.auth.username_in",
    "discovery.sendtargets.auth.password_in",
];
/// `chapSess` (`iscsi_util.go:78-82`).
const CHAP_SESS: [&str; 4] = [
    "node.session.auth.username",
    "node.session.auth.password",
    "node.session.auth.username_in",
    "node.session.auth.password_in",
];

/// `kubeletconfig.DefaultKubeletVolumeDevicesDirName`
/// (`pkg/kubelet/kubeletconfig/defaults.go:24`).
pub const DEFAULT_KUBELET_VOLUME_DEVICES_DIR_NAME: &str = "volumeDevices";

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// `execWithLog` (`iscsi_util.go:977-986`): `CombinedOutput` with a V(5) log.
fn exec_with_log(
    exec: &dyn crate::volume_plugins::util::exec::Exec,
    cmd: &str,
    args: &[String],
) -> (String, Result<(), ExecError>) {
    let start = std::time::Instant::now();
    let (out, res) = exec.combined_output(cmd, args);
    tracing::trace!(
        "Executed {cmd} {args:?} in {:?}, err: {:?}",
        start.elapsed(),
        res
    );
    tracing::trace!("Output: {out}");
    (out, res)
}

/// `updateISCSIDiscoverydb` (`iscsi_util.go:87-107`).
fn update_iscsi_discoverydb(b: &IscsiDiskMounter, tp: &str) -> Result<()> {
    if !b.disk.chap_discovery {
        return Ok(());
    }
    let d = &b.disk;
    let (out, res) = exec_with_log(
        b.exec.as_ref(),
        "iscsiadm",
        &strs(&[
            "-m",
            "discoverydb",
            "-t",
            "sendtargets",
            "-p",
            tp,
            "-I",
            &d.iface,
            "-o",
            "update",
            "-n",
            "discovery.sendtargets.auth.authmethod",
            "-v",
            "CHAP",
        ]),
    );
    if res.is_err() {
        bail!("iscsi: failed to update discoverydb with CHAP, output: {out}");
    }

    for k in CHAP_ST {
        let v = d.secret.get(k).cloned().unwrap_or_default();
        if !v.is_empty() {
            // explicitly not using execWithLog so secrets are not logged
            let (out, res) = b.exec.combined_output(
                "iscsiadm",
                &strs(&[
                    "-m",
                    "discoverydb",
                    "-t",
                    "sendtargets",
                    "-p",
                    tp,
                    "-I",
                    &d.iface,
                    "-o",
                    "update",
                    "-n",
                    k,
                    "-v",
                    &v,
                ]),
            );
            if res.is_err() {
                bail!("iscsi: failed to update discoverydb key {k:?} error: {out}");
            }
        }
    }
    Ok(())
}

/// `updateISCSINode` (`iscsi_util.go:109-137`).
fn update_iscsi_node(b: &IscsiDiskMounter, tp: &str) -> Result<()> {
    let d = &b.disk;
    // setting node.session.scan to manual to handle https://github.com/kubernetes/kubernetes/issues/90982
    let (out, res) = exec_with_log(
        b.exec.as_ref(),
        "iscsiadm",
        &strs(&[
            "-m",
            "node",
            "-p",
            tp,
            "-T",
            &d.iqn,
            "-I",
            &d.iface,
            "-o",
            "update",
            "-n",
            "node.session.scan",
            "-v",
            "manual",
        ]),
    );
    if res.is_err() {
        // don't fail if iscsiadm fails or the version does not support node.session.scan - log a warning to highlight the potential exposure
        warn!(
            "iscsi: failed to update node with node.session.scan=manual, possible exposure to issue 90982: {out}"
        );
    }

    if !d.chap_session {
        return Ok(());
    }

    let (out, res) = exec_with_log(
        b.exec.as_ref(),
        "iscsiadm",
        &strs(&[
            "-m",
            "node",
            "-p",
            tp,
            "-T",
            &d.iqn,
            "-I",
            &d.iface,
            "-o",
            "update",
            "-n",
            "node.session.auth.authmethod",
            "-v",
            "CHAP",
        ]),
    );
    if res.is_err() {
        bail!("iscsi: failed to update node with CHAP, output: {out}");
    }

    for k in CHAP_SESS {
        let v = d.secret.get(k).cloned().unwrap_or_default();
        if !v.is_empty() {
            // explicitly not using execWithLog so secrets are not logged
            let (out, res) = b.exec.combined_output(
                "iscsiadm",
                &strs(&[
                    "-m", "node", "-p", tp, "-T", &d.iqn, "-I", &d.iface, "-o", "update", "-n", k,
                    "-v", &v,
                ]),
            );
            if res.is_err() {
                bail!("iscsi: failed to update node session key {k:?} error: {out}");
            }
        }
    }
    Ok(())
}

/// `waitForPathToExist` (`iscsi_util.go:144-147`): stat a path, if not
/// exists, retry `max_retries` times. When iscsi transports other than default
/// are used, use glob instead as pci id of device is unknown. "This makes unit
/// testing a lot easier": the seams are [`wait_for_path_to_exist_internal`]'s.
pub(crate) fn wait_for_path_to_exist(
    device_path: &mut String,
    max_retries: usize,
    device_transport: &str,
) -> bool {
    wait_for_path_to_exist_internal(
        device_path,
        max_retries,
        device_transport,
        &|p| std::fs::metadata(p).map(|_| ()),
        &filepath_glob,
    )
}

/// `waitForPathToExistInternal` (`iscsi_util.go:149-181`).
pub fn wait_for_path_to_exist_internal(
    device_path: &mut String,
    max_retries: usize,
    device_transport: &str,
    os_stat: &dyn Fn(&str) -> io::Result<()>,
    filepath_glob: &dyn Fn(&str) -> Vec<String>,
) -> bool {
    for i in 0..max_retries {
        let err: Option<io::Error> = if device_transport == "tcp" {
            os_stat(device_path).err()
        } else {
            let fpath = filepath_glob(device_path);
            if fpath.is_empty() {
                Some(io::Error::from(io::ErrorKind::NotFound))
            } else {
                // There might be a case that fpath contains multiple device paths if
                // multiple PCI devices connect to same iscsi target. We handle this
                // case at subsequent logic. Pick up only first path here.
                *device_path = fpath[0].clone();
                None
            }
        };
        let Some(err) = err else {
            return true;
        };
        if err.kind() != io::ErrorKind::NotFound {
            return false;
        }
        if i == max_retries - 1 {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    false
}

/// `filepath.Glob` (`path/filepath/match.go`) for the one shape this plugin
/// passes it: `*` and `?` wildcards in the final path element of an
/// otherwise literal directory (`/dev/disk/by-path/pci-*-ip-...`). Returns the
/// matches in lexical order; no match, or an unreadable directory, is empty
/// (Go's `nil`, which `waitForPathToExistInternal` reads as "does not exist").
pub fn filepath_glob(pattern: &str) -> Vec<String> {
    let path = Path::new(pattern);
    let (Some(dir), Some(base)) = (path.parent(), path.file_name().and_then(|b| b.to_str())) else {
        return Vec::new();
    };
    if !base.contains(['*', '?']) {
        // `Glob` of a literal pattern: the path itself if it exists.
        return if std::fs::symlink_metadata(pattern).is_ok() {
            vec![pattern.to_string()]
        } else {
            Vec::new()
        };
    }
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = rd
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| wildcard_match(base.as_bytes(), n.as_bytes()))
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|n| dir.join(n).to_string_lossy().into_owned())
        .collect()
}

/// `*` (any run of non-`/` bytes) and `?` (any single byte) matching.
fn wildcard_match(pat: &[u8], s: &[u8]) -> bool {
    match pat.split_first() {
        None => s.is_empty(),
        Some((b'*', rest)) => (0..=s.len()).any(|i| wildcard_match(rest, &s[i..])),
        Some((b'?', rest)) => !s.is_empty() && wildcard_match(rest, &s[1..]),
        Some((c, rest)) => s.first() == Some(c) && wildcard_match(rest, &s[1..]),
    }
}

/// `makePDNameInternal` (`iscsi_util.go:184-186`): make a directory like
/// `/var/lib/kubelet/plugins/kubernetes.io/iscsi/iface_name/portal-some_iqn-lun-lun_id`.
pub(crate) fn make_pd_name_internal(
    host: &dyn VolumeHost,
    portal: &str,
    iqn: &str,
    lun: &str,
    iface: &str,
) -> String {
    PathBuf::from(host.get_plugin_dir(ISCSI_PLUGIN_NAME))
        .join(format!("iface-{iface}"))
        .join(format!("{portal}-{iqn}-lun-{lun}"))
        .to_string_lossy()
        .into_owned()
}

/// `makeVDPDNameInternal` (`iscsi_util.go:189-191`): make a directory like
/// `/var/lib/kubelet/plugins/kubernetes.io/iscsi/volumeDevices/iface_name/portal-some_iqn-lun-lun_id`.
pub(crate) fn make_vdpd_name_internal(
    host: &dyn VolumeHost,
    portal: &str,
    iqn: &str,
    lun: &str,
    iface: &str,
) -> String {
    PathBuf::from(host.get_volume_device_plugin_dir(ISCSI_PLUGIN_NAME))
        .join(format!("iface-{iface}"))
        .join(format!("{portal}-{iqn}-lun-{lun}"))
        .to_string_lossy()
        .into_owned()
}

/// `ISCSIUtil` (`iscsi_util.go:193`).
#[derive(Clone, Copy, Debug, Default)]
pub struct IscsiUtil;

/// The fields of `iscsiDisk` `encoding/json` writes: the exported ones
/// (`iscsi_util.go:189-217`). `MetricsProvider` is the embedded interface
/// (`iscsi.go:329`); `AttachDisk` runs on a disk built by `createISCSIDisk`,
/// where it is nil, so it encodes as `null`.
#[derive(serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "PascalCase", default)]
struct IscsiDiskFile {
    vol_name: Option<String>,
    portals: Option<Vec<String>>,
    iqn: Option<String>,
    lun: Option<String>,
    init_iface: Option<String>,
    iface: Option<String>,
    initiator_name: Option<String>,
    metrics_provider: Option<serde_json::Value>,
}

impl IscsiUtil {
    /// `persistISCSIFile` (`iscsi_util.go:207-219`): saves iSCSI volume
    /// configuration for DetachDisk into given directory.
    pub fn persist_iscsi_file(&self, conf: &IscsiDisk, mnt: &str) -> Result<()> {
        let file = Path::new(mnt).join("iscsi.json");
        let mut fp = std::fs::File::create(&file)
            .map_err(|e| anyhow!("iscsi: create {} err {e}", file.display()))?;
        let rec = IscsiDiskFile {
            vol_name: Some(conf.vol_name.clone()),
            portals: Some(conf.portals.clone()),
            iqn: Some(conf.iqn.clone()),
            lun: Some(conf.lun.clone()),
            init_iface: Some(conf.init_iface.clone()),
            iface: Some(conf.iface.clone()),
            initiator_name: Some(conf.initiator_name.clone()),
            metrics_provider: None,
        };
        // `json.Encoder.Encode` appends a newline.
        let mut data = serde_json::to_vec(&rec).map_err(|e| anyhow!("iscsi: encode err: {e}"))?;
        data.push(b'\n');
        fp.write_all(&data)
            .map_err(|e| anyhow!("iscsi: encode err: {e}"))?;
        Ok(())
    }

    /// `loadISCSI` (`iscsi_util.go:221-233`): decodes `iscsi.json` into
    /// `conf`; a field absent from the file keeps its value, as
    /// `json.Decoder.Decode` into an existing struct does.
    pub fn load_iscsi(&self, conf: &mut IscsiDisk, mnt: &str) -> Result<()> {
        let file = Path::new(mnt).join("iscsi.json");
        let data =
            std::fs::read(&file).map_err(|e| anyhow!("iscsi: open {} err {e}", file.display()))?;
        let rec: IscsiDiskFile =
            serde_json::from_slice(&data).map_err(|e| anyhow!("iscsi: decode err: {e}"))?;
        if let Some(v) = rec.vol_name {
            conf.vol_name = v;
        }
        if let Some(v) = rec.portals {
            conf.portals = v;
        }
        if let Some(v) = rec.iqn {
            conf.iqn = v;
        }
        if let Some(v) = rec.lun {
            conf.lun = v;
        }
        if let Some(v) = rec.init_iface {
            conf.init_iface = v;
        }
        if let Some(v) = rec.iface {
            conf.iface = v;
        }
        if let Some(v) = rec.initiator_name {
            conf.initiator_name = v;
        }
        Ok(())
    }

    /// `persistISCSI` (`iscsi_util.go:487-518`): saves iSCSI volume
    /// configuration for DetachDisk into global mount / map directory.
    fn persist_iscsi(&self, b: &IscsiDiskMounter) -> Result<()> {
        tracing::trace!("iscsi: AttachDisk volumeMode: {:?}", b.volume_mode);
        let global_pd_path = if b.volume_mode == Some(PersistentVolumeMode::Block) {
            b.disk.manager.make_global_vdpd_name(&b.disk)
        } else {
            b.disk.manager.make_global_pd_name(&b.disk)
        };

        if let Err(e) = std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(&global_pd_path)
        {
            error!("iscsi: failed to mkdir {global_pd_path}, error");
            return Err(e.into());
        }

        if b.volume_mode == Some(PersistentVolumeMode::Filesystem) {
            let not_mnt = b
                .mounter
                .mounter
                .is_likely_not_mount_point(&global_pd_path)?;
            if !not_mnt {
                // The volume is already mounted, therefore the previous WaitForAttach must have
                // persisted the volume metadata. In addition, the metadata is actually *inside*
                // globalPDPath and we can't write it here, because it was shadowed by the volume
                // mount.
                tracing::debug!(
                    "Skipping persistISCSI, the volume is already mounted at {global_pd_path}"
                );
                return Ok(());
            }
        }

        // Persist iscsi disk config to json file for DetachDisk path
        self.persist_iscsi_file(&b.disk, &global_pd_path)
    }

    /// `detachISCSIDisk` (`iscsi_util.go:726-763`).
    // Mirrors upstream's 8-parameter `detachISCSIDisk` signature.
    #[allow(clippy::too_many_arguments)]
    fn detach_iscsi_disk(
        &self,
        exec: &dyn crate::volume_plugins::util::exec::Exec,
        portals: &[String],
        iqn: &str,
        iface: &str,
        vol_name: &str,
        initiator_name: &str,
        found: bool,
    ) -> Result<()> {
        for portal in portals {
            let mut logout_args = strs(&["-m", "node", "-p", portal, "-T", iqn, "--logout"]);
            let mut delete_args = strs(&["-m", "node", "-p", portal, "-T", iqn, "-o", "delete"]);
            if found {
                logout_args.extend(strs(&["-I", iface]));
                delete_args.extend(strs(&["-I", iface]));
            }
            info!("iscsi: log out target {portal} iqn {iqn} iface {iface}");
            let (out, res) = exec.combined_output("iscsiadm", &logout_args);
            if let Err(e) = ignore_exit_codes(
                res,
                &[EXIT_ISCSI_ERR_NO_OBJS_FOUND, EXIT_ISCSI_ERR_SESS_NOT_FOUND],
            ) {
                error!("iscsi: failed to detach disk Error: {out}");
                return Err(e.into());
            }
            // Delete the node record
            info!("iscsi: delete node record target {portal} iqn {iqn}");
            let (out, res) = exec.combined_output("iscsiadm", &delete_args);
            if let Err(e) = ignore_exit_codes(
                res,
                &[EXIT_ISCSI_ERR_NO_OBJS_FOUND, EXIT_ISCSI_ERR_SESS_NOT_FOUND],
            ) {
                error!("iscsi: failed to delete node record Error: {out}");
                return Err(e.into());
            }
        }
        // Delete the iface after all sessions have logged out
        // If the iface is not created via iscsi plugin, skip to delete
        if !initiator_name.is_empty() && found && iface == format!("{}:{vol_name}", portals[0]) {
            let delete_args = strs(&["-m", "iface", "-I", iface, "-o", "delete"]);
            let (out, res) = exec.combined_output("iscsiadm", &delete_args);
            if let Err(e) = ignore_exit_codes(
                res,
                &[EXIT_ISCSI_ERR_NO_OBJS_FOUND, EXIT_ISCSI_ERR_SESS_NOT_FOUND],
            ) {
                error!("iscsi: failed to delete iface Error: {out}");
                return Err(e.into());
            }
        }

        Ok(())
    }
}

/// `scanOneLun` (`iscsi_util.go:243-261`): scans a single LUN on one SCSI
/// bus. Use this to avoid scanning the whole SCSI bus for all of the LUNs,
/// which would result in the kernel on this node discovering LUNs that it
/// shouldn't know about. Extraneous LUNs cause problems because they may get
/// deleted without us getting notified, since we were never supposed to know
/// about them. When LUNs are deleted without proper cleanup in the kernel, I/O
/// errors and timeouts result, which can noticeably degrade performance of
/// future operations.
fn scan_one_lun(host_number: i32, lun_number: i32) -> io::Result<()> {
    let filename = format!("/sys/class/scsi_host/host{host_number}/scan");
    let mut fd = std::fs::OpenOptions::new().write(true).open(&filename)?;

    // Channel/Target are always 0 for iSCSI
    let scan_cmd = format!("0 0 {lun_number}");
    let written = fd.write(scan_cmd.as_bytes())?;
    if written == 0 {
        return Err(io::Error::other(format!(
            "no data written to file: {filename}"
        )));
    }

    tracing::debug!("Scanned SCSI host {host_number} LUN {lun_number}");
    Ok(())
}

/// `waitForMultiPathToExist` (`iscsi_util.go:263-286`).
fn wait_for_multipath_to_exist(
    device_paths: &[String],
    max_retries: usize,
    device_util: &dyn DeviceUtil,
) -> String {
    if device_paths.is_empty() {
        return String::new();
    }

    for i in 0..max_retries {
        for path in device_paths {
            // There shouldn't be any empty device paths. However adding this check
            // for safer side to avoid the possibility of an empty entry.
            if path.is_empty() {
                continue;
            }
            // check if the dev is using mpio and if so mount it via the dm-XX device
            let mapped = device_util.find_multipath_device_for_device(path);
            if !mapped.is_empty() {
                return mapped;
            }
        }
        if i == max_retries - 1 {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    String::new()
}

/// Delete 1 block device of the form "sd*" (`iscsi_util.go:521-537`).
fn delete_device(device_name: &str) -> io::Result<()> {
    let filename = format!("/sys/block/{device_name}/device/delete");
    let Ok(mut fd) = std::fs::OpenOptions::new().write(true).open(filename) else {
        // The file was not present, so just return without error
        return Ok(());
    };

    let written = fd.write("1".as_bytes())?;
    if written == 0 {
        return Err(io::Error::other(format!(
            "no data written to file: /sys/block/{device_name}/device/delete"
        )));
    }
    tracing::debug!("Deleted block device: {device_name}");
    Ok(())
}

/// `deleteDevices` (`iscsi_util.go:541-580`): tries to remove all the block
/// devices and multipath map devices associated with a given iscsi device.
fn delete_devices(c: &IscsiDiskUnmounter) -> Result<()> {
    let lun_number: i32 = c.disk.lun.parse().map_err(|e| {
        error!(
            "iscsi delete devices: lun is not a number: {}\nError: {e}",
            c.disk.lun
        );
        anyhow!("{e}")
    })?;
    // Enumerate the devices so we can delete them
    let device_names = c
        .device_util
        .find_devices_for_iscsi_lun(&c.disk.iqn, lun_number)
        .map_err(|e| {
            error!(
                "iscsi delete devices: could not get devices associated with LUN {lun_number} on target {}\nError: {e}",
                c.disk.iqn
            );
            e
        })?;
    // Find the multipath device path(s)
    let mut mpath_devices: Vec<String> = Vec::new();
    for device_name in &device_names {
        let path = format!("/dev/{device_name}");
        // check if the dev is using mpio and if so mount it via the dm-XX device
        let mapped = c.device_util.find_multipath_device_for_device(&path);
        if !mapped.is_empty() && !mpath_devices.contains(&mapped) {
            mpath_devices.push(mapped);
        }
    }
    // Flush any multipath device maps
    for mpath_device in &mpath_devices {
        let (_, res) = c
            .exec
            .combined_output("multipath", &strs(&["-f", mpath_device]));
        if let Err(e) = res {
            warn!("Warning: Failed to flush multipath device map: {mpath_device}\nError: {e}");
            // Fall through -- keep deleting the block devices
        }
        tracing::debug!("Flushed multipath device: {mpath_device}");
    }
    for device_name in &device_names {
        if let Err(e) = delete_device(device_name) {
            warn!("Warning: Failed to delete block device: {device_name}\nError: {e}");
            // Fall through -- keep deleting other block devices
        }
    }
    Ok(())
}

impl DiskManager for IscsiUtil {
    /// `MakeGlobalPDName` (`iscsi_util.go:196-198`): returns path of global
    /// plugin dir.
    fn make_global_pd_name(&self, iscsi: &IscsiDisk) -> String {
        make_pd_name_internal(
            iscsi.plugin.host.as_ref(),
            &iscsi.portals[0],
            &iscsi.iqn,
            &iscsi.lun,
            &iscsi.iface,
        )
    }

    /// `MakeGlobalVDPDName` (`iscsi_util.go:201-203`): returns path of global
    /// volume device plugin dir.
    fn make_global_vdpd_name(&self, iscsi: &IscsiDisk) -> String {
        make_vdpd_name_internal(
            iscsi.plugin.host.as_ref(),
            &iscsi.portals[0],
            &iscsi.iqn,
            &iscsi.lun,
            &iscsi.iface,
        )
    }

    /// `AttachDisk` (`iscsi_util.go:289-483`): returns devicePath of volume if
    /// attach succeeded otherwise returns error.
    fn attach_disk(&self, b: &IscsiDiskMounter) -> Result<String> {
        let d = &b.disk;
        // `devicePaths := map[string]string{}`: keyed by portal; kept in
        // insertion order, one valid order of Go's randomized map iteration.
        let mut device_paths: Vec<(String, String)> = Vec::new();
        let mut last_err: Option<String> = None;

        let (out, res) = exec_with_log(
            b.exec.as_ref(),
            "iscsiadm",
            &strs(&["-m", "iface", "-I", &d.init_iface, "-o", "show"]),
        );
        if let Err(e) = res {
            error!("iscsi: could not read iface {} error: {out}", d.init_iface);
            return Err(e.into());
        }

        let iscsi_transport = extract_transportname(&out);

        let bkp_portal = &d.portals;

        // If the initiator name was set, the iface isn't created yet,
        // so create it and copy parameters from the pre-configured one
        if !d.initiator_name.is_empty() {
            if let Err(e) = clone_iface(b) {
                error!("iscsi: failed to clone iface: {} error: {e}", d.init_iface);
                return Err(e);
            }
        }

        // Lock the target while we login to avoid races between 2 volumes that share the same
        // target both logging in or one logging out while another logs in.
        let _target_lock = d.plugin.target_locks.lock_key(&d.iqn);

        // Build a map of SCSI hosts for each target portal. We will need this to
        // issue the bus rescans.
        let portal_host_map = b.device_util.get_iscsi_portal_host_map_for_target(&d.iqn)?;
        tracing::debug!(
            "AttachDisk portal->host map for {} is {portal_host_map:?}",
            d.iqn
        );

        for i in 1..=MAX_ATTACH_ATTEMPTS {
            for tp in bkp_portal {
                if device_paths.iter().any(|(t, _)| t == tp) {
                    tracing::debug!("Device for portal {tp:?} already known");
                    continue;
                }

                let (mut host_number, mut logged_in) = match portal_host_map.get(tp) {
                    Some(h) => (*h, true),
                    None => (0, false),
                };
                if !logged_in {
                    tracing::debug!(
                        "Could not get SCSI host number for portal {tp}, will attempt login"
                    );

                    // build discoverydb and discover iscsi target
                    let _ = exec_with_log(
                        b.exec.as_ref(),
                        "iscsiadm",
                        &strs(&[
                            "-m",
                            "discoverydb",
                            "-t",
                            "sendtargets",
                            "-p",
                            tp,
                            "-I",
                            &d.iface,
                            "-o",
                            "new",
                        ]),
                    );

                    // update discoverydb with CHAP secret
                    if let Err(e) = update_iscsi_discoverydb(b, tp) {
                        last_err = Some(format!(
                            "iscsi: failed to update discoverydb to portal {tp} error: {e}"
                        ));
                        continue;
                    }

                    let (out, res) = exec_with_log(
                        b.exec.as_ref(),
                        "iscsiadm",
                        &strs(&[
                            "-m",
                            "discoverydb",
                            "-t",
                            "sendtargets",
                            "-p",
                            tp,
                            "-I",
                            &d.iface,
                            "--discover",
                        ]),
                    );
                    if let Err(e) = res {
                        // delete discoverydb record
                        let _ = exec_with_log(
                            b.exec.as_ref(),
                            "iscsiadm",
                            &strs(&[
                                "-m",
                                "discoverydb",
                                "-t",
                                "sendtargets",
                                "-p",
                                tp,
                                "-I",
                                &d.iface,
                                "-o",
                                "delete",
                            ]),
                        );
                        last_err = Some(format!(
                            "iscsi: failed to sendtargets to portal {tp} output: {out}, err {e}"
                        ));
                        continue;
                    }

                    if let Err(e) = update_iscsi_node(b, tp) {
                        // failure to update node db is rare. But deleting record will likely impact those who already start using it.
                        last_err = Some(format!(
                            "iscsi: failed to update iscsi node to portal {tp} error: {e}"
                        ));
                        continue;
                    }

                    // login to iscsi target
                    let (out, res) = exec_with_log(
                        b.exec.as_ref(),
                        "iscsiadm",
                        &strs(&[
                            "-m", "node", "-p", tp, "-T", &d.iqn, "-I", &d.iface, "--login",
                        ]),
                    );
                    if let Err(e) = res {
                        // delete the node record from database
                        let _ = exec_with_log(
                            b.exec.as_ref(),
                            "iscsiadm",
                            &strs(&[
                                "-m", "node", "-p", tp, "-I", &d.iface, "-T", &d.iqn, "-o",
                                "delete",
                            ]),
                        );
                        last_err =
                            Some(format!("iscsi: failed to attach disk: Error: {out} ({e})"));
                        continue;
                    }

                    // in case of node failure/restart, explicitly set to manual login so it doesn't hang on boot
                    let (_, res) = exec_with_log(
                        b.exec.as_ref(),
                        "iscsiadm",
                        &strs(&[
                            "-m",
                            "node",
                            "-p",
                            tp,
                            "-T",
                            &d.iqn,
                            "-o",
                            "update",
                            "-n",
                            "node.startup",
                            "-v",
                            "manual",
                        ]),
                    );
                    if let Err(e) = res {
                        // don't fail if we can't set startup mode, but log warning so there is a clue
                        warn!("Warning: Failed to set iSCSI login mode to manual. Error: {e}");
                    }

                    // Rebuild the host map after logging in. Go's `:=` here
                    // shadows the outer `portalHostMap`, so the outer map is
                    // NOT refreshed for the next attempt (`iscsi_util.go:380`).
                    let refreshed = b.device_util.get_iscsi_portal_host_map_for_target(&d.iqn)?;
                    tracing::trace!("AttachDisk portal->host map for {} is {refreshed:?}", d.iqn);

                    match refreshed.get(tp) {
                        Some(h) => {
                            host_number = *h;
                            logged_in = true;
                        }
                        None => {
                            host_number = 0;
                            logged_in = false;
                        }
                    }
                    if !logged_in {
                        warn!("Could not get SCSI host number for portal {tp} after logging in");
                        continue;
                    }
                }

                tracing::trace!("AttachDisk: scanning SCSI host {host_number} LUN {}", d.lun);
                let lun_number: i32 = d.lun.parse().map_err(|e| {
                    anyhow!("AttachDisk: lun is not a number: {}\nError: {e}", d.lun)
                })?;

                // Scan the iSCSI bus for the LUN
                scan_one_lun(host_number, lun_number)?;

                if iscsi_transport.is_empty() {
                    error!("iscsi: could not find transport name in iface {}", d.iface);
                    bail!("could not parse iface file for {}", d.iface);
                }

                let mut addr = tp.clone();
                if tp.starts_with('[') {
                    // Delete [] from IP address, links in /dev/disk/by-path do not have it.
                    addr = tp.replace(['[', ']'], "");
                }
                let mut device_path = if iscsi_transport == "tcp" {
                    [
                        "/dev/disk/by-path/ip",
                        &addr,
                        "iscsi",
                        &d.iqn,
                        "lun",
                        &d.lun,
                    ]
                    .join("-")
                } else {
                    [
                        "/dev/disk/by-path/pci",
                        "*",
                        "ip",
                        &addr,
                        "iscsi",
                        &d.iqn,
                        "lun",
                        &d.lun,
                    ]
                    .join("-")
                };

                if !wait_for_path_to_exist(
                    &mut device_path,
                    DEVICE_DISCOVERY_TIMEOUT,
                    &iscsi_transport,
                ) {
                    let msg = format!(
                        "Timed out waiting for device at path {device_path} after {DEVICE_DISCOVERY_TIMEOUT}s"
                    );
                    error!("{msg}");
                    // update last error
                    last_err = Some(msg);
                    continue;
                } else {
                    device_paths.push((tp.clone(), device_path.clone()));
                }
            }
            tracing::debug!(
                "iscsi: tried all devices for {:?} {i} times, {} paths found",
                d.iqn,
                device_paths.len()
            );
            if device_paths.is_empty() {
                // No path attached, report error and stop trying. kubelet will try again in a short while
                // delete cloned iface
                let _ = exec_with_log(
                    b.exec.as_ref(),
                    "iscsiadm",
                    &strs(&["-m", "iface", "-I", &d.iface, "-o", "delete"]),
                );
                let last = last_err.clone().unwrap_or_else(|| "<nil>".to_string());
                error!("iscsi: failed to get any path for iscsi disk, last err seen:\n{last}");
                bail!("failed to get any path for iscsi disk, last err seen:\n{last}");
            }
            if device_paths.len() == bkp_portal.len() {
                // We have all paths
                tracing::debug!("iscsi: all devices for {:?} found", d.iqn);
                break;
            }
            if device_paths.len() >= MIN_MULTIPATH_COUNT && i >= MIN_ATTACH_ATTEMPTS {
                // We have at least two paths for multipath and we tried the other paths long enough
                tracing::debug!("{} devices found for {:?}", device_paths.len(), d.iqn);
                break;
            }
        }

        if let Some(e) = &last_err {
            error!("iscsi: last error occurred during iscsi init:\n{e}");
        }

        let device_path_list: Vec<String> = device_paths.iter().map(|(_, p)| p.clone()).collect();
        // Try to find a multipath device for the volume
        let mut device_path = if bkp_portal.len() > 1 {
            // Multipath volume was requested. Wait up to multipathDeviceTimeout seconds for the multipath device to appear.
            wait_for_multipath_to_exist(
                &device_path_list,
                MULTIPATH_DEVICE_TIMEOUT,
                b.device_util.as_ref(),
            )
        } else {
            // For PVs with 1 portal, just try one time to find the multipath device. This
            // avoids a long pause when the multipath device will never get created, and
            // matches legacy behavior.
            wait_for_multipath_to_exist(&device_path_list, 1, b.device_util.as_ref())
        };

        // When no multipath device is found, just use the first (and presumably only) device
        if device_path.is_empty() {
            device_path = device_path_list[0].clone();
        }

        tracing::trace!("iscsi: AttachDisk devicePath: {device_path}");

        if let Err(e) = self.persist_iscsi(b) {
            // Return uncertain error so kubelet calls Unmount / Unmap when the pod
            // is deleted.
            return Err(uncertain_progress(e.to_string()));
        }
        Ok(device_path)
    }

    /// `DetachDisk` (`iscsi_util.go:583-656`): unmounts and detaches a volume
    /// from node.
    fn detach_disk(&self, c: &mut IscsiDiskUnmounter, mnt_path: &str) -> Result<()> {
        let (exists, path_err) = path_exists(mnt_path);
        if let Some(e) = path_err {
            bail!("error checking if path exists: {e}");
        } else if !exists {
            warn!("Warning: Unmount skipped because path does not exist: {mnt_path}");
            return Ok(());
        }

        let not_mnt = c.mounter.is_likely_not_mount_point(mnt_path)?;
        if !not_mnt {
            if let Err(e) = c.mounter.unmount(mnt_path) {
                error!("iscsi detach disk: failed to unmount: {mnt_path}\nError: {e}");
                return Err(e.into());
            }
        }

        // if device is no longer used, see if need to logout the target
        let (device, _) = extract_device_and_prefix(mnt_path)?;

        let bkp_portal: Vec<String>;
        let iqn: String;
        let iface: String;
        let vol_name: String;
        let initiator_name: String;
        let found: bool;

        // load iscsi disk config from json file
        if self.load_iscsi(&mut c.disk, mnt_path).is_ok() {
            bkp_portal = c.disk.portals.clone();
            iqn = c.disk.iqn.clone();
            iface = c.disk.iface.clone();
            vol_name = c.disk.vol_name.clone();
            initiator_name = c.disk.initiator_name.clone();
            found = true;
        } else {
            // If the iscsi disk config is not found, fall back to the original behavior.
            // This portal/iqn/iface is no longer referenced, log out.
            // Extract the portal and iqn from device path.
            let (p, q) = extract_portal_and_iqn(&device)?;
            bkp_portal = vec![p];
            iqn = q;
            // Extract the iface from the mountPath and use it to log out. If the iface
            // is not found, maintain the previous behavior to facilitate kubelet upgrade.
            // Logout may fail as no session may exist for the portal/IQN on the specified interface.
            let (i, f) = extract_iface(mnt_path);
            iface = i;
            found = f;
            // `volName`, `initiatorName` stay Go's zero values.
            vol_name = String::new();
            initiator_name = String::new();
        }

        // Delete all the scsi devices and any multipath devices after unmounting
        if let Err(e) = delete_devices(c) {
            warn!("iscsi detach disk: failed to delete devices\nError: {e}");
            // Fall through -- even if deleting fails, a logout may fix problems
        }

        // Lock the target while we determine if we can safely log out or not
        let _target_lock = c.disk.plugin.target_locks.lock_key(&iqn);

        let portals = remove_duplicate(&bkp_portal);
        if portals.is_empty() {
            bail!("iscsi detach disk: failed to detach iscsi disk. Couldn't get connected portals from configurations");
        }

        // If device is no longer used, see if need to logout the target
        if is_session_busy(c.disk.plugin.host.as_ref(), &portals[0], &iqn) {
            return Ok(());
        }

        self.detach_iscsi_disk(
            c.exec.as_ref(),
            &portals,
            &iqn,
            &iface,
            &vol_name,
            &initiator_name,
            found,
        )
        .map_err(|e| anyhow!("failed to finish detachISCSIDisk, err: {e}"))
    }

    /// `DetachBlockISCSIDisk` (`iscsi_util.go:659-724`): removes loopback
    /// device for a volume and detaches a volume from node.
    fn detach_block_iscsi_disk(&self, c: &mut IscsiDiskUnmapper, map_path: &str) -> Result<()> {
        let (exists, path_err) = path_exists(map_path);
        if let Some(e) = path_err {
            bail!("error checking if path exists: {e}");
        } else if !exists {
            warn!("Warning: Unmap skipped because path does not exist: {map_path}");
            return Ok(());
        }
        // If we arrive here, device is no longer used, see if need to logout the target
        // device: 192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0
        let (device, _) = extract_device_and_prefix(map_path)?;
        let bkp_portal: Vec<String>;
        let iqn: String;
        let lun: String;
        let iface: String;
        let vol_name: String;
        let initiator_name: String;
        let found: bool;
        // load iscsi disk config from json file
        if self.load_iscsi(&mut c.disk, map_path).is_ok() {
            bkp_portal = c.disk.portals.clone();
            iqn = c.disk.iqn.clone();
            lun = c.disk.lun.clone();
            iface = c.disk.iface.clone();
            vol_name = c.disk.vol_name.clone();
            initiator_name = c.disk.initiator_name.clone();
            found = true;
        } else {
            // If the iscsi disk config is not found, fall back to the original behavior.
            // This portal/iqn/iface is no longer referenced, log out.
            // Extract the portal and iqn from device path.
            let (p, q) = extract_portal_and_iqn(&device)?;
            bkp_portal = vec![p];
            iqn = q;
            let arr: Vec<&str> = device.split("-lun-").collect();
            if arr.len() < 2 {
                bail!("failed to retrieve lun from mapPath: {map_path}");
            }
            lun = arr[1].to_string();
            // Extract the iface from the mountPath and use it to log out. If the iface
            // is not found, maintain the previous behavior to facilitate kubelet upgrade.
            // Logout may fail as no session may exist for the portal/IQN on the specified interface.
            let (i, f) = extract_iface(map_path);
            iface = i;
            found = f;
            vol_name = String::new();
            initiator_name = String::new();
        }
        let portals = remove_duplicate(&bkp_portal);
        if portals.is_empty() {
            bail!("iscsi detach disk: failed to detach iscsi disk. Couldn't get connected portals from configurations");
        }

        let device_path = get_dev_by_path(&portals[0], &iqn, &lun);
        tracing::trace!("iscsi: devicePath: {device_path}");
        if std::fs::metadata(&device_path).is_err() {
            bail!("failed to validate devicePath: {device_path}");
        }

        // Lock the target while we determine if we can safely log out or not
        let _target_lock = c.disk.plugin.target_locks.lock_key(&iqn);

        // If device is no longer used, see if need to logout the target
        if is_session_busy(c.disk.plugin.host.as_ref(), &portals[0], &iqn) {
            return Ok(());
        }

        // Detach a volume from kubelet node
        self.detach_iscsi_disk(
            c.exec.as_ref(),
            &portals,
            &iqn,
            &iface,
            &vol_name,
            &initiator_name,
            found,
        )
        .map_err(|e| anyhow!("failed to finish detachISCSIDisk, err: {e}"))
    }
}

/// `getDevByPath` (`iscsi_util.go:765-767`).
fn get_dev_by_path(portal: &str, iqn: &str, lun: &str) -> String {
    format!("/dev/disk/by-path/ip-{portal}-iscsi-{iqn}-lun-{lun}")
}

/// `extractTransportname` (`iscsi_util.go:769-781`): the `ifaceTransportNameRe`
/// (`iface.transport_name = (.*)\n`) capture; `<empty>` is `tcp`.
pub fn extract_transportname(iface_output: &str) -> String {
    const KEY: &str = "iface.transport_name = ";
    let Some(start) = iface_output.find(KEY) else {
        return String::new();
    };
    let rest = &iface_output[start + KEY.len()..];
    // `(.*)\n`: `.` does not match a newline, and the match needs one.
    let Some(end) = rest.find('\n') else {
        return String::new();
    };
    let iscsi_transport = &rest[..end];

    // While iface.transport_name is a required parameter, handle it being unspecified anyways
    if iscsi_transport == "<empty>" {
        return "tcp".to_string();
    }
    iscsi_transport.to_string()
}

/// `extractDeviceAndPrefix` (`iscsi_util.go:783-796`).
pub fn extract_device_and_prefix(mnt_path: &str) -> Result<(String, String)> {
    let Some(ind) = mnt_path.rfind('/') else {
        bail!("iscsi detach disk: malformatted mnt path: {mnt_path}");
    };
    let device = mnt_path[ind + 1..].to_string();
    // strip -lun- from mount path
    let Some(ind) = mnt_path.rfind("-lun-") else {
        bail!("iscsi detach disk: malformatted mnt path: {mnt_path}");
    };
    let prefix = mnt_path[..ind].to_string();
    Ok((device, prefix))
}

/// `extractIface` (`iscsi_util.go:798-805`): `ifaceRe` is
/// `.+/iface-([^/]+)/.+`; greedy `.+` makes the *last* `/iface-<name>/` that
/// is followed by at least one more character the match.
pub fn extract_iface(mnt_path: &str) -> (String, bool) {
    const MARK: &str = "/iface-";
    let mut search_end = mnt_path.len();
    while let Some(i) = mnt_path[..search_end].rfind(MARK) {
        // `.+` before the marker needs at least one character.
        if i > 0 {
            let after = &mnt_path[i + MARK.len()..];
            if let Some(slash) = after.find('/') {
                let name = &after[..slash];
                // `([^/]+)/.+`
                if !name.is_empty() && slash + 1 < after.len() {
                    return (name.to_string(), true);
                }
            }
        }
        search_end = i;
    }
    (String::new(), false)
}

/// `extractPortalAndIqn` (`iscsi_util.go:807-823`).
pub fn extract_portal_and_iqn(device: &str) -> Result<(String, String)> {
    let Some(ind1) = device.find('-') else {
        bail!("iscsi detach disk: no portal in {device}");
    };
    let portal = device[..ind1].to_string();
    let ind2 = device.find("iqn.").or_else(|| device.find("eui."));
    let Some(ind2) = ind2 else {
        bail!("iscsi detach disk: no iqn in {device}");
    };
    // `strings.LastIndex` is -1 when absent, which Go then panics slicing on;
    // a `device` without `-lun-` is rejected by every caller's earlier split.
    let ind = device
        .rfind("-lun-")
        .ok_or_else(|| anyhow!("iscsi detach disk: no lun in {device}"))?;
    if ind2 > ind {
        bail!("iscsi detach disk: no iqn in {device}");
    }
    let iqn = device[ind2..ind].to_string();
    Ok((portal, iqn))
}

/// Remove duplicates or string (`iscsi_util.go:826-836`): drops empty entries
/// and keeps the first of each value, in order.
pub fn remove_duplicate(s: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for v in s {
        if !v.is_empty() && seen.insert(v.clone()) {
            out.push(v.clone());
        }
    }
    out
}

/// `parseIscsiadmShow` (`iscsi_util.go:838-856`).
pub fn parse_iscsiadm_show(output: &str) -> Result<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    for line in output.split('\n') {
        if !line.starts_with("iface.") || line.contains("<empty>") {
            continue;
        }
        let iface: Vec<&str> = line.split_whitespace().collect();
        if iface.len() != 3 || iface[1] != "=" {
            bail!("error: invalid iface setting: {iface:?}");
        }
        // iscsi_ifacename is immutable once the iface is created
        if iface[0] == "iface.iscsi_ifacename" {
            continue;
        }
        params.insert(iface[0].to_string(), iface[2].to_string());
    }
    Ok(params)
}

/// `cloneIface` (`iscsi_util.go:858-904`).
pub fn clone_iface(b: &IscsiDiskMounter) -> Result<()> {
    let d = &b.disk;
    let mut last_err: Option<anyhow::Error> = None;
    if d.init_iface == d.iface {
        bail!("iscsi: cannot clone iface with same name: {}", d.init_iface);
    }
    // get pre-configured iface records
    let (out, res) = exec_with_log(
        b.exec.as_ref(),
        "iscsiadm",
        &strs(&["-m", "iface", "-I", &d.init_iface, "-o", "show"]),
    );
    if let Err(e) = res {
        bail!("iscsi: failed to show iface records: {out} ({e})");
    }
    // parse obtained records
    let mut params = parse_iscsiadm_show(&out)
        .map_err(|e| anyhow!("iscsi: failed to parse iface records: {out} ({e})"))?;
    // update initiatorname
    params.insert("iface.initiatorname".to_string(), d.initiator_name.clone());
    // create new iface
    let (out, res) = exec_with_log(
        b.exec.as_ref(),
        "iscsiadm",
        &strs(&["-m", "iface", "-I", &d.iface, "-o", "new"]),
    );
    if let Err(e) = res {
        if e.exit_status() == Some(ISCSIADM_ERROR_SESS_EXISTS) {
            info!(
                "iscsi: there is a session already logged in with iface {}",
                d.iface
            );
        } else {
            bail!("iscsi: failed to create new iface: {out} ({e})");
        }
    }
    // Get and sort keys to maintain a stable iteration order (a BTreeMap
    // iterates sorted).
    // update new iface records
    for (key, value) in &params {
        let (_, res) = exec_with_log(
            b.exec.as_ref(),
            "iscsiadm",
            &strs(&[
                "-m", "iface", "-I", &d.iface, "-o", "update", "-n", key, "-v", value,
            ]),
        );
        if let Err(e) = res {
            let _ = exec_with_log(
                b.exec.as_ref(),
                "iscsiadm",
                &strs(&["-m", "iface", "-I", &d.iface, "-o", "delete"]),
            );
            last_err = Some(anyhow!(
                "iscsi: failed to update iface records: {out} ({e}). iface({}) will be used",
                d.init_iface
            ));
            break;
        }
    }
    match last_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `isSessionBusy` (`iscsi_util.go:907-923`): determines if the iSCSI session
/// is busy by counting both FS and block volumes in use.
pub(crate) fn is_session_busy(host: &dyn VolumeHost, portal: &str, iqn: &str) -> bool {
    let fs_dir = host.get_plugin_dir(ISCSI_PLUGIN_NAME);
    let count_fs = match get_vol_count(&fs_dir, portal, iqn) {
        Ok(c) => c,
        Err(e) => {
            error!("iscsi: could not determine FS volumes in use: {e}");
            return true;
        }
    };

    let block_dir = host.get_volume_device_plugin_dir(ISCSI_PLUGIN_NAME);
    let count_block = match get_vol_count(&block_dir, portal, iqn) {
        Ok(c) => c,
        Err(e) => {
            error!("iscsi: could not determine block volumes in use: {e}");
            return true;
        }
    };

    count_fs + count_block > 1
}

/// `getVolCount` (`iscsi_util.go:927-961`): returns the number of volumes in
/// use by the kubelet. It does so by counting the number of directories
/// prefixed by the given portal and IQN.
pub fn get_vol_count(dir: &str, portal: &str, iqn: &str) -> io::Result<usize> {
    // For FileSystem volumes, the topmost dirs are named after the ifaces, e.g., iface-default or iface-127.0.0.1:3260:pv0.
    // For Block volumes, the default topmost dir is volumeDevices.
    let contents = match std::fs::read_dir(dir) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    // Inside each iface dir, we look for volume dirs prefixed by the given
    // portal + iqn, e.g., 127.0.0.1:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-2
    let mut counter = 0;
    for c in contents {
        let c = c?;
        // `ioutil.ReadDir` uses `Lstat`: a symlink to a directory is not a dir.
        let is_dir = c.file_type()?.is_dir();
        if !is_dir || c.file_name() == DEFAULT_KUBELET_VOLUME_DEVICES_DIR_NAME {
            continue;
        }

        let mounts = std::fs::read_dir(c.path())?;
        for m in mounts {
            let volume_mount = m?.file_name().to_string_lossy().into_owned();
            let prefix = format!("{portal}-{iqn}");
            if volume_mount.starts_with(&prefix) {
                counter += 1;
            }
        }
    }

    Ok(counter)
}

/// `ignoreExitCodes` (`iscsi_util.go:963-975`): an error that is not an
/// `ExitError` (including `nil`) is returned as is; an `ExitError` whose
/// status is listed is swallowed.
pub fn ignore_exit_codes(
    res: Result<(), ExecError>,
    ignored_exit_codes: &[i32],
) -> Result<(), ExecError> {
    let Err(err) = res else {
        return Ok(());
    };
    let Some(status) = err.exit_status() else {
        return Err(err);
    };
    if ignored_exit_codes.contains(&status) {
        tracing::debug!("ignored iscsiadm exit code {status}");
        return Ok(());
    }
    Err(err)
}
