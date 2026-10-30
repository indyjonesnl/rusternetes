//! Unit tests moved from `src/volume_plugins/util/hostutil.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::io;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::hostutil::*;
    use std::os::unix::fs::MetadataExt;

    /// `TestGetSELinuxSupport` (`hostutil_linux_test.go:137`).
    #[test]
    fn get_selinux_support_cases() {
        let info = "62 0 253:0 / / rw,relatime shared:1 - ext4 /dev/mapper/ssd-root rw,seclabel,data=ordered
78 62 0:41 / /tmp rw,nosuid,nodev shared:30 - tmpfs tmpfs rw,seclabel
83 63 0:44 / /var/lib/bar rw,relatime - tmpfs tmpfs rw
227 62 253:0 /var/lib/docker/devicemapper /var/lib/docker/devicemapper rw,relatime - ext4 /dev/mapper/ssd-root rw,seclabel,data=ordered
150 23 1:58 / /media/nfs_vol rw,relatime shared:89 - nfs4 172.18.4.223:/srv/nfs rw,vers=4.0,rsize=524288,wsize=524288,namlen=255,hard,proto=tcp,port=0,timeo=600,retrans=2,sec=sys,clientaddr=172.18.4.223,local_lock=none,addr=172.18.4.223
";
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("mountinfo");
        std::fs::write(&f, info).unwrap();
        let f = f.to_str().unwrap();
        for (name, mp, enabled, want) in [
            ("ext4 on / with disabled SELinux", "/", false, false),
            ("ext4 on /", "/", true, true),
            ("tmpfs on /var/lib/bar", "/var/lib/bar", true, false),
            ("nfsv4", "/media/nfs_vol", true, false),
        ] {
            assert_eq!(get_selinux(mp, f, || enabled).unwrap(), want, "{name}");
        }
    }

    #[test]
    fn get_owner_follows_symlinks() {
        let d = tempfile::tempdir().unwrap();
        let target = d.path().join("t");
        std::fs::create_dir(&target).unwrap();
        let link = d.path().join("l");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let md = std::fs::metadata(&target).unwrap();
        assert_eq!(
            get_owner(link.to_str().unwrap()).unwrap(),
            (md.uid() as i64, md.gid() as i64)
        );
    }

    /// `PathIsDevice` (`hostutil_linux.go:63-67`) over `getFileType`
    /// (`hostutil.go:80-110`): a character device is a device, a directory and
    /// a regular file are not, and a missing path errors with
    /// `path %q does not exist`.
    #[test]
    fn path_is_device_classifies_by_file_type() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let hu = HostUtil;
        assert!(hu.path_is_device("/dev/null").unwrap());
        assert!(!hu.path_is_device(d.path().to_str().unwrap()).unwrap());
        assert!(!hu.path_is_device(file.to_str().unwrap()).unwrap());
        let missing = d.path().join("missing");
        let err = hu.path_is_device(missing.to_str().unwrap()).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("path {:?} does not exist", missing.to_str().unwrap())
        );
    }

    /// `ExclusiveOpenFailsOnDevice` (`hostutil_linux.go:70-110`): a missing
    /// path is an error (`PathIsDevice failed for path %q: %v`); a path that
    /// is not a device is `false`; a device nobody holds exclusively is
    /// `false`.
    #[test]
    fn device_opened_follows_exclusive_open() {
        let d = tempfile::tempdir().unwrap();
        let hu = HostUtil;
        assert!(!hu.device_opened(d.path().to_str().unwrap()).unwrap());
        assert!(!hu.device_opened("/dev/null").unwrap());
        let missing = d.path().join("missing");
        let err = hu.device_opened(missing.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().starts_with(&format!(
                "PathIsDevice failed for path {:?}: ",
                missing.to_str().unwrap()
            )),
            "{err}"
        );
    }

    #[test]
    fn get_owner_missing_errs() {
        assert!(get_owner("/definitely/not/here").is_err());
    }
}
