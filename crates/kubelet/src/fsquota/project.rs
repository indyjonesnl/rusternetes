//! Port of `pkg/volume/util/fsquota/project.go`: maintenance of the
//! `/etc/projects` and `/etc/projid` files.

use super::common::{QuotaID, BAD_QUOTA_ID, FIRST_QUOTA};
use super::FsQuota;
use anyhow::{anyhow, Result};
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::OnceLock;

/// `maxUnusedQuotasToSearch`: don't go into an infinite loop searching for an
/// unused quota.
const MAX_UNUSED_QUOTAS_TO_SEARCH: usize = 128;

pub(super) const PROJECTS_FILE: &str = "/etc/projects";
pub(super) const PROJID_FILE: &str = "/etc/projid";

fn projects_parse_regexp() -> &'static Regex {
    // "^([[:digit:]]+):(.*)$"
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^([0-9]+):(.*)$").unwrap())
}

fn projid_parse_regexp() -> &'static Regex {
    // "^([^#][^:]*):([[:digit:]]+)$"
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^([^#][^:]*):([0-9]+)$").unwrap())
}

/// `projectType`.
#[derive(Debug, Clone)]
pub(super) struct ProjectType {
    /// False if we need to remove this line.
    is_valid: bool,
    id: QuotaID,
    /// Project name (projid) or directory (projects).
    data: String,
    line: String,
}

/// `projectsList`.
#[derive(Debug, Default)]
pub(super) struct ProjectsList {
    projects: Vec<ProjectType>,
    projid: Vec<ProjectType>,
}

/// `projFilesAreOK`.
fn proj_files_are_ok(projects_file: &Path, projid_file: &Path) -> Result<()> {
    let regular = |p: &Path| std::fs::symlink_metadata(p).map(|m| m.file_type().is_file());
    // `err != nil || IsRegular()`
    if regular(projects_file).unwrap_or(true) {
        if regular(projid_file).unwrap_or(true) {
            return Ok(());
        }
        return Err(anyhow!(
            "{} exists but is not a plain file, cannot continue",
            projid_file.display()
        ));
    }
    Err(anyhow!(
        "{} exists but is not a plain file, cannot continue",
        projects_file.display()
    ))
}

fn flock(file: &File, op: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::flock(file.as_raw_fd(), op) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// `openAndLockProjectFiles`: opens /etc/projects and /etc/projid locked.
/// Creates them if they don't exist.
fn open_and_lock_project_files(projects_file: &Path, projid_file: &Path) -> Result<(File, File)> {
    // Make sure neither project-related file is a symlink!
    proj_files_are_ok(projects_file, projid_file)
        .map_err(|e| anyhow!("system project files failed verification: {e}"))?;
    // We don't actually modify the original files; we create temporaries and
    // move them over the originals.
    let f_projects = open_rdonly_create(projects_file)
        .map_err(|e| anyhow!("unable to open {}: {e}", projects_file.display()))?;
    let f_projid = open_rdonly_create(projid_file)
        .map_err(|e| anyhow!("unable to open {}: {e}", projid_file.display()))?;
    // Check once more, to ensure nothing got changed out from under us.
    proj_files_are_ok(projects_file, projid_file)
        .map_err(|e| anyhow!("system project files failed re-verification: {e}"))?;
    flock(&f_projects, libc::LOCK_EX)
        .map_err(|e| anyhow!("unable to lock {}: {e}", projects_file.display()))?;
    if let Err(e) = flock(&f_projid, libc::LOCK_EX) {
        let _ = flock(&f_projects, libc::LOCK_UN);
        return Err(anyhow!("unable to lock {}: {e}", projid_file.display()));
    }
    Ok((f_projects, f_projid))
}

/// `os.OpenFile(name, O_RDONLY|O_CREATE, 0644)`.
fn open_rdonly_create(path: &Path) -> std::io::Result<File> {
    let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let fd = unsafe {
        libc::open(
            c.as_ptr(),
            libc::O_RDONLY | libc::O_CREAT | libc::O_CLOEXEC,
            0o644 as libc::c_uint,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    use std::os::fd::FromRawFd;
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// `parseProject`.
fn parse_project(l: &str) -> ProjectType {
    if let Some(m) = projects_parse_regexp().captures(l) {
        if let Ok(i) = m[1].parse::<i64>() {
            return ProjectType {
                is_valid: true,
                id: i as QuotaID,
                data: m[2].to_string(),
                line: l.to_string(),
            };
        }
    }
    ProjectType {
        is_valid: true,
        id: BAD_QUOTA_ID,
        data: String::new(),
        line: l.to_string(),
    }
}

/// `parseProjid`.
fn parse_projid(l: &str) -> ProjectType {
    if let Some(m) = projid_parse_regexp().captures(l) {
        if let Ok(i) = m[2].parse::<i64>() {
            return ProjectType {
                is_valid: true,
                id: i as QuotaID,
                data: m[1].to_string(),
                line: l.to_string(),
            };
        }
    }
    ProjectType {
        is_valid: true,
        id: BAD_QUOTA_ID,
        data: String::new(),
        line: l.to_string(),
    }
}

fn parse_proj_file(f: &File, parser: fn(&str) -> ProjectType) -> Vec<ProjectType> {
    BufReader::new(f)
        .lines()
        .map_while(|l| l.ok())
        .map(|l| parser(&l))
        .collect()
}

/// `readProjectFiles`.
fn read_project_files(projects: &File, projid: &File) -> ProjectsList {
    ProjectsList {
        projects: parse_proj_file(projects, parse_project),
        projid: parse_proj_file(projid, parse_projid),
    }
}

impl FsQuota {
    /// `findAvailableQuota`: finds the next available quota from the
    /// FirstQuota; it returns an error if QuotaIDIsInUse returns an error when
    /// getting a quota id in use; it searches at most
    /// maxUnusedQuotasToSearch (128) times.
    fn find_available_quota(&self, path: &str, id_map: &HashSet<QuotaID>) -> Result<QuotaID> {
        let mut unused_quotas_searched = 0;
        let mut id = FIRST_QUOTA;
        loop {
            if !id_map.contains(&id) {
                let applier = self
                    .get_applier(path)
                    .ok_or_else(|| anyhow!("no quota applier for {path}"))?;
                if !applier.quota_id_is_in_use(id)? {
                    return Ok(id);
                }
                unused_quotas_searched += 1;
                if unused_quotas_searched > MAX_UNUSED_QUOTAS_TO_SEARCH {
                    break;
                }
            }
            id += 1;
        }
        Err(anyhow!("cannot find available quota ID"))
    }

    /// `addDirToProject`: `(id, needToAddProjid)`.
    fn add_dir_to_project(
        &self,
        path: &str,
        mut id: QuotaID,
        list: &mut ProjectsList,
    ) -> Result<(QuotaID, bool)> {
        let mut id_map: HashSet<QuotaID> = HashSet::new();
        for project in &list.projects {
            if project.data == path {
                if id != BAD_QUOTA_ID && id != project.id {
                    return Err(anyhow!("attempt to reassign project ID for {path}"));
                }
                // Trying to reassign a directory to the project it's already
                // in. Maybe this should be an error, but for now treat it as
                // an idempotent operation.
                return Ok((project.id, false));
            }
            id_map.insert(project.id);
        }
        let mut need_to_add_projid = true;
        for projid in &list.projid {
            id_map.insert(projid.id);
            if projid.id == id && id != BAD_QUOTA_ID {
                need_to_add_projid = false;
            }
        }
        if id == BAD_QUOTA_ID {
            id = self.find_available_quota(path, &id_map)?;
            need_to_add_projid = true;
        }
        if need_to_add_projid {
            let name = format!("volume{id}");
            let line = format!("{name}:{id}");
            list.projid.push(ProjectType {
                is_valid: true,
                id,
                data: name,
                line,
            });
        }
        let line = format!("{id}:{path}");
        list.projects.push(ProjectType {
            is_valid: true,
            id,
            data: path.to_string(),
            line,
        });
        Ok((id, need_to_add_projid))
    }

    /// `createProjectID`: if ID is BadQuotaID, generate a new project id if
    /// the dir is not in a project.
    pub(super) fn create_project_id(&self, path: &str, id: QuotaID) -> Result<QuotaID> {
        let _g = self.quota_id_lock.lock().unwrap();
        let mut id = id;
        let res: Result<QuotaID> = (|| {
            let (f_projects, f_projid) =
                open_and_lock_project_files(&self.projects_file, &self.projid_file)?;
            let mut list = read_project_files(&f_projects, &f_projid);
            let (new_id, write_projid) = self.add_dir_to_project(path, id, &mut list)?;
            id = new_id;
            if new_id == BAD_QUOTA_ID {
                return Err(anyhow!("<nil>"));
            }
            write_project_files(
                &f_projects,
                &self.projects_file,
                &f_projid,
                &self.projid_file,
                write_projid,
                &list,
            )?;
            Ok(new_id)
        })();
        res.map_err(|e| anyhow!("createProjectID {path} {id} failed {e}"))
    }

    /// `removeProjectID`.
    pub(super) fn remove_project_id(&self, path: &str, id: QuotaID) -> Result<()> {
        if id == BAD_QUOTA_ID {
            return Err(anyhow!("attempting to remove invalid quota ID {id}"));
        }
        let _g = self.quota_id_lock.lock().unwrap();
        let res: Result<()> = (|| {
            let (f_projects, f_projid) =
                open_and_lock_project_files(&self.projects_file, &self.projid_file)?;
            let mut list = read_project_files(&f_projects, &f_projid);
            let write_projid = remove_dir_from_project(path, id, &mut list)?;
            write_project_files(
                &f_projects,
                &self.projects_file,
                &f_projid,
                &self.projid_file,
                write_projid,
                &list,
            )
        })();
        res.map_err(|e| anyhow!("removeProjectID {path} {id} failed {e}"))
    }
}

/// `removeDirFromProject`.
fn remove_dir_from_project(path: &str, id: QuotaID, list: &mut ProjectsList) -> Result<bool> {
    if id == BAD_QUOTA_ID {
        return Err(anyhow!("attempt to remove invalid quota ID from {path}"));
    }
    let mut found_at: Option<usize> = None;
    let mut count_by_id: HashMap<QuotaID, i32> = HashMap::new();
    for i in 0..list.projects.len() {
        let project_id = list.projects[i].id;
        if list.projects[i].data == path {
            if id != project_id {
                return Err(anyhow!(
                    "attempting to remove quota ID {id} from path {path}, but expecting ID {project_id}"
                ));
            } else if found_at.is_some() {
                return Err(anyhow!("found multiple quota IDs for path {path}"));
            }
            // Faster and easier than deleting an element.
            list.projects[i].is_valid = false;
            found_at = Some(i);
        }
        *count_by_id.entry(project_id).or_insert(0) += 1;
    }
    if found_at.is_none() {
        return Err(anyhow!("cannot find quota associated with path {path}"));
    }
    if count_by_id.get(&id).copied().unwrap_or(0) <= 1 {
        // Removing the last entry means that we're no longer using the quota
        // ID, so remove that as well.
        for projid in list.projid.iter_mut() {
            if projid.id == id {
                projid.is_valid = false;
            }
        }
        return Ok(true);
    }
    Ok(false)
}

/// `writeProjectFile`: writes a temporary file next to `base` and returns its
/// name.
fn write_project_file(base: &File, base_name: &Path, projects: &[ProjectType]) -> Result<String> {
    let mode = base.metadata()?.permissions().mode() & 0o777;
    let dir = base_name.parent().unwrap_or_else(|| Path::new("."));
    let base_file = base_name
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    // os.CreateTemp(filepath.Dir(oname), filepath.Base(oname))
    let (mut f, filename) = loop {
        let candidate = dir.join(format!(
            "{base_file}{}",
            uuid::Uuid::new_v4().as_u128() as u32
        ));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)
        {
            Ok(f) => break (f, candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    };
    if let Err(e) = std::fs::set_permissions(&filename, std::fs::Permissions::from_mode(mode)) {
        let _ = std::fs::remove_file(&filename);
        return Err(e.into());
    }
    for proj in projects {
        if proj.is_valid {
            if let Err(e) = writeln!(f, "{}", proj.line) {
                let _ = std::fs::remove_file(&filename);
                return Err(e.into());
            }
        }
    }
    if let Err(e) = f.sync_all() {
        let _ = std::fs::remove_file(&filename);
        return Err(e.into());
    }
    Ok(filename.to_string_lossy().into_owned())
}

/// `writeProjectFiles`.
///
/// Upstream shadows `err` in the projid branch (`tmpProjid, err := ...`), so a
/// failure writing or renaming /etc/projid is not reported; only the
/// /etc/projects write decides the result. Kept as is.
fn write_project_files(
    f_projects: &File,
    projects_name: &Path,
    f_projid: &File,
    projid_name: &Path,
    write_projid: bool,
    list: &ProjectsList,
) -> Result<()> {
    let tmp_projects = write_project_file(f_projects, projects_name, &list.projects)
        .map_err(|e| anyhow!("unable to write project files: {e}"))?;
    // Ensure that both files are written before we try to rename either.
    if write_projid {
        if let Ok(tmp_projid) = write_project_file(f_projid, projid_name, &list.projid) {
            if std::fs::rename(&tmp_projid, projid_name).is_err() {
                let _ = std::fs::remove_file(&tmp_projid);
            }
        }
    }
    match std::fs::rename(&tmp_projects, projects_name) {
        Ok(()) => Ok(()),
        Err(e) => {
            // We're in a bit of trouble here; at this point we've
            // successfully renamed tmpProjid to the real thing, but renaming
            // tmpProjects to the real file failed. There's not much we can do
            // in this position.
            let _ = std::fs::remove_file(&tmp_projects);
            Err(anyhow!("unable to write project files: {e}"))
        }
    }
}
