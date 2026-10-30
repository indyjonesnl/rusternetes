//! Unit tests moved from `src/fsquota/common.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use regex::Regex;
    pub use std::io::Write;
    pub use std::process::Command;
    pub use std::sync::{Mutex, OnceLock};
    pub use tracing::{debug, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::fsquota::common::*;

    #[test]
    fn max_quota_matches_upstream_on_64_bit() {
        let fs = linux_supported_filesystems();
        assert_eq!(fs[0].max_quota, i64::MAX);
        assert_eq!(fs[1].max_quota, (1i64 << 58) - 1);
        assert!(fs[0].allow_empty_output && !fs[1].allow_empty_output);
    }

    #[test]
    fn mount_parse_splits_device_and_mountpoint() {
        assert_eq!(
            mount_parse("/dev/sdb1 /virt xfs rw 0 0"),
            ("/dev/sdb1", "/virt")
        );
    }

    #[test]
    fn xfs_quota_output_is_parsed_like_get_quantity() {
        // `quota -p -N -n -v -b <id>` prints "#<id> <used> <soft> <hard> ..."
        assert_eq!(
            parse_quantity("#1048577   8   0   0   00 [--------]\n", 1024, true).unwrap(),
            8 * 1024
        );
        // XFS reports nothing when no quota is present.
        assert_eq!(parse_quantity("", 1024, true).unwrap(), 0);
        // ext4 always reports something; empty output is a parse failure.
        assert!(parse_quantity("", 1024, false).is_err());
        assert!(parse_quantity("garbage", 1, true).is_err());
    }

    #[test]
    fn lsattr_output_is_parsed_and_path_checked() {
        // Real `lsattr -pd` output ends in a newline; POSIX `$` matches
        // before it.
        let out = " 1048577 --------------e------- /var/lib/x\n";
        assert_eq!(parse_lsattr("/var/lib/x", out).unwrap(), 1048577);
        assert!(parse_lsattr("/other", out).is_err());
        assert!(parse_lsattr("/x", "nonsense").is_err());
    }
}
