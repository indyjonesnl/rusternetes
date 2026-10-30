//! Port of `pkg/volume/util/fsquota/common` (`quota_common.go`,
//! `quota_common_linux.go`, `quota_common_linux_impl.go`).

use anyhow::{anyhow, Result};
use regex::Regex;
use std::io::Write;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use tracing::{debug, warn};

/// `QuotaID` (`quota_common.go`): data type based on quotactl(2).
pub type QuotaID = i32;
/// `UnknownQuotaID`: cannot determine whether a quota is in force.
pub const UNKNOWN_QUOTA_ID: QuotaID = -1;
/// `BadQuotaID`: invalid quota.
pub const BAD_QUOTA_ID: QuotaID = 0;

/// `QuotaType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaType {
    /// `FSQuotaAccounting`: quotas for accounting only.
    Accounting = 1,
    /// `FSQuotaEnforcing`: quotas for enforcement.
    Enforcing = 2,
}

/// `FirstQuota`: the quota ID we start with.
pub const FIRST_QUOTA: QuotaID = 1048577;

/// `MountsFile`: the location of the system mount data.
pub const MOUNTS_FILE: &str = "/proc/self/mounts";

/// `MountParseRegexp.FindStringSubmatch` on one `/proc/self/mounts` line
/// (`^([^ ]*)[ \t]*([^ ]*)[ \t]*([^ ]*)`, options ignored): `(device, mount)`.
pub fn mount_parse(line: &str) -> (&str, &str) {
    let dev_end = line.find(' ').unwrap_or(line.len());
    let device = &line[..dev_end];
    let rest = line[dev_end..].trim_start_matches([' ', '\t']);
    let mount_end = rest.find(' ').unwrap_or(rest.len());
    (device, &rest[..mount_end])
}

/// `LinuxVolumeQuotaProvider`: returns an appropriate quota applier object if
/// we can support quotas on this device.
pub trait LinuxVolumeQuotaProvider: Send + Sync {
    /// `GetQuotaApplier`: `None` if this provider cannot support quotas on
    /// the device.
    fn get_quota_applier(
        &self,
        mountpoint: &str,
        backing_dev: &str,
    ) -> Option<Box<dyn LinuxVolumeQuotaApplier>>;
}

/// `LinuxVolumeQuotaApplier`: a generic interface to any quota mechanism
/// supported by Linux.
pub trait LinuxVolumeQuotaApplier: Send + Sync {
    /// `GetQuotaOnDir`: the quota ID (if any) that applies to this directory.
    fn get_quota_on_dir(&self, path: &str) -> Result<QuotaID>;
    /// `SetQuotaOnDir`. A negative `bytes` means a non-enforcing quota should
    /// be applied (perhaps by setting a quota too large to be hit).
    fn set_quota_on_dir(&self, path: &str, id: QuotaID, bytes: i64) -> Result<()>;
    /// `QuotaIDIsInUse`. Implementations should not check /etc/project or
    /// /etc/projid, only whether their underlying mechanism already has the
    /// ID in use.
    fn quota_id_is_in_use(&self, id: QuotaID) -> Result<bool>;
    /// `GetConsumption` (bytes) via the quota mechanism; an error lets higher
    /// layers enumerate the directory.
    fn get_consumption(&self, path: &str, id: QuotaID) -> Result<i64>;
    /// `GetInodes` via the quota mechanism.
    fn get_inodes(&self, path: &str, id: QuotaID) -> Result<i64>;
}

/// `linuxFilesystemType`: if we later get a filesystem that uses project quota
/// semantics other than XFS, we'll need to change this.
pub struct LinuxFilesystemType {
    /// Filesystem magic number, per statfs(2).
    pub type_magic: i64,
    pub max_quota: i64,
    /// Accept empty output from the "quota" command.
    pub allow_empty_output: bool,
}

const BITS_PER_WORD: u32 = usize::BITS;

pub fn linux_supported_filesystems() -> [LinuxFilesystemType; 2] {
    let max_word = (1i64 << (BITS_PER_WORD - 1)).wrapping_sub(1);
    [
        // XFS
        LinuxFilesystemType {
            type_magic: 0x58465342,
            max_quota: max_word,
            // XFS filesystems report nothing if a quota is not present.
            allow_empty_output: true,
        },
        // ext4fs
        LinuxFilesystemType {
            type_magic: 0xef53,
            max_quota: max_word & ((1i64 << 58) - 1),
            // ext4 filesystems always report something even if a quota is not present.
            allow_empty_output: false,
        },
    ]
}

/// `VolumeProvider`: supplies a quota applier to the generic code.
#[derive(Default)]
pub struct VolumeProvider;

const QUOTA_CMDS: [&str; 3] = ["/sbin/xfs_quota", "/usr/sbin/xfs_quota", "/bin/xfs_quota"];
const LSATTR_CMD: &str = "/usr/bin/lsattr";

// Go's POSIX regexps (`MustCompilePOSIX`) have no `OneLine` flag, so `^`/`$`
// match at line boundaries: `(?m)`.
fn quota_parse_regexp() -> &'static Regex {
    // quotaParseRegexp = "^[^ \t]*[ \t]*([0-9]+)"
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"(?m)^[^ \t]*[ \t]*([0-9]+)").unwrap())
}

fn lsattr_parse_regexp() -> &'static Regex {
    // lsattrParseRegexp = "^ *([0-9]+) [^ ]+ (.*)$"
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"(?m)^ *([0-9]+) [^ ]+ (.*)$").unwrap())
}

impl LinuxVolumeQuotaProvider for VolumeProvider {
    /// `GetQuotaApplier`: does this backing device support quotas that can be
    /// applied to directories?
    fn get_quota_applier(
        &self,
        mountpoint: &str,
        backing_dev: &str,
    ) -> Option<Box<dyn LinuxVolumeQuotaApplier>> {
        for fs_type in linux_supported_filesystems() {
            if is_filesystem_of_type(mountpoint, backing_dev, fs_type.type_magic) {
                return Some(Box::new(LinuxVolumeQuotaApplierImpl {
                    mountpoint: mountpoint.to_string(),
                    max_quota: fs_type.max_quota,
                    allow_empty_output: fs_type.allow_empty_output,
                }));
            }
        }
        None
    }
}

struct LinuxVolumeQuotaApplierImpl {
    mountpoint: String,
    max_quota: i64,
    allow_empty_output: bool,
}

/// `getXFSQuotaCmd`: the result of the search is cached, failure included
/// (upstream returns `quotaCmd, nil` with an empty command after a failed
/// first search, which then fails at exec).
fn get_xfs_quota_cmd() -> Result<String> {
    static CMD: Mutex<Option<String>> = Mutex::new(None);
    let mut g = CMD.lock().unwrap();
    if let Some(c) = g.as_ref() {
        return Ok(c.clone());
    }
    for program in QUOTA_CMDS {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(program) {
            if md.permissions().mode() & (1 << 6) != 0 {
                debug!("Found xfs_quota program {program}");
                *g = Some(program.to_string());
                return Ok(program.to_string());
            }
        }
    }
    *g = Some(String::new());
    Err(anyhow!("no xfs_quota program found"))
}

/// `doRunXFSQuotaCommand`.
fn do_run_xfs_quota_command(mountpoint: &str, mounts_file: &str, command: &str) -> Result<String> {
    let quota_cmd = get_xfs_quota_cmd()?;
    // We're using numeric project IDs directly; no need to scan
    // /etc/projects or /etc/projid.
    debug!(
        "runXFSQuotaCommand {quota_cmd} -t {mounts_file} -P/dev/null -D/dev/null -x -f {mountpoint} -c {command}"
    );
    let out = Command::new(&quota_cmd)
        .args([
            "-t",
            mounts_file,
            "-P/dev/null",
            "-D/dev/null",
            "-x",
            "-f",
            mountpoint,
            "-c",
            command,
        ])
        .output()?;
    if !out.status.success() {
        return Err(anyhow!("{}", out.status));
    }
    let data = String::from_utf8_lossy(&out.stdout).into_owned();
    debug!("runXFSQuotaCommand output {data:?}");
    Ok(data)
}

/// `runXFSQuotaCommand`: extract the mountpoint we care about into a
/// temporary mounts file so that xfs_quota does not attempt to scan every
/// mount on the filesystem, which could hang if e.g. a stuck NFS mount is
/// present (https://bugzilla.redhat.com/show_bug.cgi?id=237120).
fn run_xfs_quota_command(mountpoint: &str, command: &str) -> Result<String> {
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let tmp_name = std::env::temp_dir().join(format!("mounts{}", uuid::Uuid::new_v4().simple()));
    let mut tmp = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_name)
        .map_err(|e| anyhow!("cannot create temporary mount file: {e}"))?;
    let _cleanup = Cleanup(tmp_name.clone());

    let mounts = std::fs::read_to_string(MOUNTS_FILE)
        .map_err(|e| anyhow!("cannot open mounts file {MOUNTS_FILE}: {e}"))?;
    for line in mounts.lines() {
        let (_, mount) = mount_parse(line);
        if mount == mountpoint {
            writeln!(tmp, "{line}")
                .map_err(|e| anyhow!("cannot write temporary mounts file: {e}"))?;
            tmp.sync_all()
                .map_err(|e| anyhow!("cannot sync temporary mounts file: {e}"))?;
            return do_run_xfs_quota_command(mountpoint, tmp_name.to_str().unwrap(), command);
        }
    }
    Err(anyhow!(
        "cannot run xfs_quota: cannot find mount point {mountpoint} in {MOUNTS_FILE}"
    ))
}

/// `SupportsQuotas` (common): determines whether the filesystem supports
/// quotas.
pub fn supports_quotas(mountpoint: &str, q_type: QuotaType) -> Result<bool> {
    let data = run_xfs_quota_command(mountpoint, "state -p")?;
    if q_type == QuotaType::Enforcing {
        return Ok(data.contains("Enforcement: ON"));
    }
    Ok(data.contains("Accounting: ON"))
}

/// `isFilesystemOfType`.
fn is_filesystem_of_type(mountpoint: &str, _backing_dev: &str, type_magic: i64) -> bool {
    let Ok(c) = std::ffi::CString::new(mountpoint) else {
        return false;
    };
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut buf) } != 0 {
        warn!(
            "Warning: Unable to statfs {mountpoint}: {}",
            std::io::Error::last_os_error()
        );
        return false;
    }
    if buf.f_type as i64 != type_magic {
        return false;
    }
    matches!(supports_quotas(mountpoint, QuotaType::Accounting), Ok(true))
}

/// `getQuantity`.
fn get_quantity(
    mountpoint: &str,
    id: QuotaID,
    xfs_quota_arg: &str,
    multiplier: i64,
    allow_empty_output: bool,
) -> Result<i64> {
    let data = run_xfs_quota_command(
        mountpoint,
        &format!("quota -p -N -n -v {xfs_quota_arg} {id}"),
    )
    .map_err(|e| anyhow!("unable to run xfs_quota: {e}"))?;
    parse_quantity(&data, multiplier, allow_empty_output)
}

/// The output half of `getQuantity`.
pub fn parse_quantity(data: &str, multiplier: i64, allow_empty_output: bool) -> Result<i64> {
    if data.is_empty() && allow_empty_output {
        return Ok(0);
    }
    let caps = quota_parse_regexp()
        .captures(data)
        .ok_or_else(|| anyhow!("unable to parse quota output '{data}'"))?;
    let size: i64 = caps[1].parse().map_err(|e| {
        anyhow!(
            "unable to parse data size '{}' from '{data}': {e}",
            &caps[1]
        )
    })?;
    Ok(size.wrapping_mul(multiplier))
}

impl LinuxVolumeQuotaApplier for LinuxVolumeQuotaApplierImpl {
    /// `GetQuotaOnDir`: retrieves the quota ID (if any) associated with the
    /// specified directory (`lsattr -pd`).
    fn get_quota_on_dir(&self, path: &str) -> Result<QuotaID> {
        let out = Command::new(LSATTR_CMD)
            .args(["-pd", path])
            .output()
            .map_err(|e| anyhow!("cannot run lsattr: {e}"))?;
        if !out.status.success() {
            return Err(anyhow!("cannot run lsattr: {}", out.status));
        }
        parse_lsattr(path, &String::from_utf8_lossy(&out.stdout))
    }

    /// `SetQuotaOnDir`: applies a quota to the specified directory under the
    /// specified mountpoint.
    fn set_quota_on_dir(&self, path: &str, id: QuotaID, bytes: i64) -> Result<()> {
        let bytes = if bytes < 0 || bytes > self.max_quota {
            self.max_quota
        } else {
            bytes
        };
        run_xfs_quota_command(
            &self.mountpoint,
            &format!("limit -p bhard={bytes} bsoft={bytes} {id}"),
        )?;
        run_xfs_quota_command(&self.mountpoint, &format!("project -s -p {path} {id}"))?;
        Ok(())
    }

    fn quota_id_is_in_use(&self, id: QuotaID) -> Result<bool> {
        let bytes = self.get_consumption(&self.mountpoint, id)?;
        if bytes > 0 {
            return Ok(true);
        }
        Ok(self.get_inodes(&self.mountpoint, id)? > 0)
    }

    /// `GetConsumption`: bytes; `xfs_quota` reports KiB.
    fn get_consumption(&self, _path: &str, id: QuotaID) -> Result<i64> {
        get_quantity(&self.mountpoint, id, "-b", 1024, self.allow_empty_output)
    }

    fn get_inodes(&self, _path: &str, id: QuotaID) -> Result<i64> {
        get_quantity(&self.mountpoint, id, "-i", 1, self.allow_empty_output)
    }
}

/// The output half of `GetQuotaOnDir`.
pub fn parse_lsattr(path: &str, data: &str) -> Result<QuotaID> {
    let caps = lsattr_parse_regexp()
        .captures(data)
        .ok_or_else(|| anyhow!("unable to parse lsattr -pd {path} output {data}"))?;
    if &caps[2] != path {
        return Err(anyhow!(
            "mismatch between supplied and returned path ({path} != {})",
            &caps[2]
        ));
    }
    caps[1]
        .parse::<i32>()
        .map_err(|e| anyhow!("unable to parse project ID from {} ({e})", &caps[1]))
}
