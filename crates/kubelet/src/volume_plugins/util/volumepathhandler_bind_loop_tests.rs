//! Bind-mount mapping (`volume_path_handler.go:111-150`, `:184-214`),
//! `GetDeviceBindMountRefs` (`:284-300`) and the losetup functions
//! (`volume_path_handler_linux.go`), with `FakeMounter` / `FakeExec`
//! standing in for `mount.New("")` / `exec.Command`; `cleanBackingFilePath`
//! cases from `volume_path_handler_linux_test.go:TestCleanBackingFilePath`.
use super::super::exec::testing::{script, FakeExec};
use super::super::exec::ExecError;
use super::super::mount_interface::testing::{FakeMounter, MountPoint};
use super::*;
use tempfile::tempdir;

#[test]
fn map_device_bind_mount_creates_file_and_bind_mounts() {
    let tmp = tempdir().unwrap();
    let fm = FakeMounter::new();
    VolumePathHandler
        .map_device_with(
            &fm,
            "/dev/fakeDevice",
            tmp.path().to_str().unwrap(),
            "bind",
            true,
        )
        .unwrap();
    let link = tmp.path().join("bind");
    assert!(link.is_file());
    let mps = fm.mount_points.lock().unwrap();
    assert_eq!(mps.len(), 1);
    assert_eq!(mps[0].device, "/dev/fakeDevice");
    assert_eq!(mps[0].opts, vec!["bind".to_string()]);
    assert_eq!(
        mps[0].path,
        fs::canonicalize(&link).unwrap().to_string_lossy()
    );
}

#[test]
fn map_device_bind_mount_failure_is_wrapped() {
    let tmp = tempdir().unwrap();
    let fm = FakeMounter::new();
    fm.mount_errs.lock().unwrap().push(Some("boom".into()));
    let e = VolumePathHandler
        .map_device_with(&fm, "/dev/x", tmp.path().to_str().unwrap(), "b", true)
        .unwrap_err()
        .to_string();
    assert!(
        e.starts_with("failed to bind mount devicePath: /dev/x to linkPath ")
            && e.ends_with(": boom"),
        "{e}"
    );
}

#[test]
fn unmap_device_bind_mount_not_a_device_removes_file_without_unmount() {
    let tmp = tempdir().unwrap();
    fs::write(tmp.path().join("bindFile"), b"").unwrap();
    let fm = FakeMounter::new();
    fm.mount_points.lock().unwrap().push(MountPoint {
        device: "d".into(),
        path: "/elsewhere".into(),
        fstype: String::new(),
        opts: vec![],
    });
    VolumePathHandler
        .unmap_device_with(&fm, tmp.path().to_str().unwrap(), "bindFile", true)
        .unwrap();
    assert!(!tmp.path().join("bindFile").exists());
    assert_eq!(fm.mount_points.lock().unwrap().len(), 1);
}

#[test]
fn get_device_bind_mount_refs_skips_non_devices() {
    let tmp = tempdir().unwrap();
    fs::write(tmp.path().join("regular"), b"").unwrap();
    symlink("/dev/null", tmp.path().join("link")).unwrap();
    let refs = VolumePathHandler
        .get_device_bind_mount_refs("/dev/x", tmp.path().to_str().unwrap())
        .unwrap();
    assert!(refs.is_empty());
    assert!(VolumePathHandler
        .get_device_bind_mount_refs("/dev/x", tmp.path().join("missing").to_str().unwrap())
        .is_err());
}

#[test]
fn get_device_bind_mount_refs_lists_device_nodes() {
    // /dev holds char devices (null, zero) we can list without root.
    let refs = VolumePathHandler
        .get_device_bind_mount_refs("/dev/null", "/dev")
        .unwrap();
    assert!(refs.contains(&"/dev/null".to_string()), "{refs:?}");
}

#[test]
fn clean_backing_file_path_cases() {
    let p = "/var/lib/kubelet/dev/uuid";
    assert_eq!(clean_backing_file_path(p), p);
    assert_eq!(clean_backing_file_path(&format!("{p}\r\t\n ")), p);
    assert_eq!(clean_backing_file_path(&format!("{p}(deleted)")), p);
    assert_eq!(clean_backing_file_path(&format!("{p} (deleted)\t")), p);
}

fn fake_sys_block(root: &Path, loop_name: &str, backing: &str) {
    let d = root.join(loop_name).join("loop");
    fs::create_dir_all(&d).unwrap();
    fs::write(d.join("backing_file"), backing).unwrap();
}

#[test]
fn get_loop_device_finds_backing_file_and_reports_not_found() {
    let tmp = tempdir().unwrap();
    let file = tmp.path().join("img");
    fs::write(&file, b"x").unwrap();
    let sys = tmp.path().join("sys");
    fake_sys_block(&sys, "loop0", "/other\n");
    fake_sys_block(&sys, "loop3", &format!("{} (deleted)\n", file.display()));
    assert_eq!(
        get_loop_device_with(&sys, file.to_str().unwrap()).unwrap(),
        "/dev/loop3"
    );
    let e = get_loop_device_with(&sys, tmp.path().join("nope").to_str().unwrap()).unwrap_err();
    assert_eq!(e.to_string(), ERR_DEVICE_NOT_FOUND);
    let other = tmp.path().join("other");
    fs::write(&other, b"x").unwrap();
    let e = get_loop_device_with(&sys, other.to_str().unwrap()).unwrap_err();
    assert_eq!(e.to_string(), ERR_DEVICE_NOT_FOUND);
}

#[test]
fn attach_file_device_reuses_existing_loop_device_without_losetup() {
    let tmp = tempdir().unwrap();
    let file = tmp.path().join("img");
    fs::write(&file, b"x").unwrap();
    let sys = tmp.path().join("sys");
    fake_sys_block(&sys, "loop1", file.to_str().unwrap());
    let ex = FakeExec::new(vec![]);
    assert_eq!(
        attach_file_device_with(&ex, &sys, file.to_str().unwrap()).unwrap(),
        "/dev/loop1"
    );
    assert_eq!(ex.command_calls(), 0);
}

#[test]
fn attach_file_device_runs_losetup_when_unattached() {
    let tmp = tempdir().unwrap();
    let file = tmp.path().join("img");
    fs::write(&file, b"x").unwrap();
    let sys = tmp.path().join("sys");
    fs::create_dir_all(&sys).unwrap();
    // `losetup -f` makes the kernel create loop2 backed by the file.
    struct Attaching(std::path::PathBuf, String, FakeExec);
    impl Exec for Attaching {
        fn combined_output(&self, cmd: &str, args: &[String]) -> (String, Result<(), ExecError>) {
            let r = self.2.combined_output(cmd, args);
            fake_sys_block(&self.0, "loop2", &self.1);
            r
        }
    }
    let ex = Attaching(
        sys.clone(),
        file.to_str().unwrap().to_string(),
        FakeExec::new(vec![script("losetup", &["-f", file.to_str().unwrap()])]),
    );
    assert_eq!(
        attach_file_device_with(&ex, &sys, file.to_str().unwrap()).unwrap(),
        "/dev/loop2"
    );
}

#[test]
fn detach_file_device_cases() {
    let tmp = tempdir().unwrap();
    let file = tmp.path().join("img");
    fs::write(&file, b"x").unwrap();
    let sys = tmp.path().join("sys");
    // Not attached: skipped without running losetup.
    fs::create_dir_all(&sys).unwrap();
    let ex = FakeExec::new(vec![]);
    detach_file_device_with(&ex, &sys, file.to_str().unwrap()).unwrap();
    assert_eq!(ex.command_calls(), 0);
    // Attached: losetup -d <device>.
    fake_sys_block(&sys, "loop4", file.to_str().unwrap());
    let ex = FakeExec::new(vec![script("losetup", &["-d", "/dev/loop4"])]);
    detach_file_device_with(&ex, &sys, file.to_str().unwrap()).unwrap();
    assert_eq!(ex.command_calls(), 1);
}
