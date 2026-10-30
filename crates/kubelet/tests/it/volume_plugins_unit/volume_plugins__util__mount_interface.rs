//! Unit tests moved from `src/volume_plugins/util/mount_interface.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::volume_plugins::util::exec::{Exec, ExecError};
    pub use rusternetes_kubelet::volume_plugins::util::mount_ops::{
        sanitized_options_for_logging, Mounter,
    };
    pub use std::io;
    pub use std::sync::{Arc, Condvar, Mutex};
    pub use std::time::Duration;
    pub use tracing::{error, info, warn};
}

mod tests {
    //! Port of `TestSafeFormatAndMount` (`safe_format_and_mount_test.go:55-262`).
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::exec::testing::{CommandScript, FakeExec};
    use rusternetes_kubelet::volume_plugins::util::mount_interface::testing::FakeMounter;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::*;

    const BLKID: [&str; 8] = [
        "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo",
    ];

    fn blkid(output: &str, status: i32) -> CommandScript {
        CommandScript {
            cmd: "blkid",
            args: BLKID.iter().map(|s| s.to_string()).collect(),
            output: output.to_string(),
            return_code: status,
            error: None,
        }
    }

    fn cmd(cmd: &'static str, args: &[&str], status: i32) -> CommandScript {
        CommandScript {
            cmd,
            args: args.iter().map(|s| s.to_string()).collect(),
            output: String::new(),
            return_code: status,
            error: None,
        }
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    const EXT4: &str = "DEVNAME=/dev/foo\nTYPE=ext4\n";

    struct Case {
        description: &'static str,
        fstype: &'static str,
        mount_options: Vec<String>,
        sensitive: Vec<String>,
        format_options: Vec<String>,
        scripts: Vec<CommandScript>,
        mount_errs: Vec<Option<String>>,
        exp: Option<MountErrorType>,
    }

    fn case(description: &'static str, fstype: &'static str, scripts: Vec<CommandScript>) -> Case {
        Case {
            description,
            fstype,
            mount_options: vec![],
            sensitive: vec![],
            format_options: vec![],
            scripts,
            mount_errs: vec![],
            exp: None,
        }
    }

    #[test]
    fn safe_format_and_mount() {
        let unknown_fs = Some("unknown filesystem type '(null)'".to_string());
        let mut cases = vec![
            Case {
                mount_options: s(&["ro"]),
                ..case(
                    "Test a read only mount of an already formatted device",
                    "ext4",
                    vec![blkid(EXT4, 0)],
                )
            },
            case(
                "Test a normal mount of an already formatted device",
                "ext4",
                vec![blkid(EXT4, 0), cmd("fsck", &["-a", "/dev/foo"], 0)],
            ),
            Case {
                mount_options: s(&["ro"]),
                exp: Some(MountErrorType::UnformattedReadOnly),
                ..case(
                    "Test a read only mount of unformatted device",
                    "ext4",
                    vec![blkid("", 2)],
                )
            },
            case(
                "Test a normal mount of unformatted device",
                "ext4",
                vec![blkid("", 2), cmd("mkfs.ext4", &["-F", "-m0", "/dev/foo"], 0)],
            ),
            Case {
                exp: Some(MountErrorType::HasFilesystemErrors),
                ..case(
                    "Test 'fsck' fails with exit status 4",
                    "ext4",
                    vec![blkid(EXT4, 0), cmd("fsck", &["-a", "/dev/foo"], 4)],
                )
            },
            case(
                "Test 'fsck' fails with exit status 1 (errors found and corrected)",
                "ext4",
                vec![blkid(EXT4, 0), cmd("fsck", &["-a", "/dev/foo"], 1)],
            ),
            case(
                "Test 'fsck' fails with exit status other than 1 and 4 (likely unformatted device)",
                "ext4",
                vec![blkid(EXT4, 0), cmd("fsck", &["-a", "/dev/foo"], 8)],
            ),
            Case {
                mount_errs: vec![unknown_fs.clone()],
                exp: Some(MountErrorType::FilesystemMismatch),
                ..case(
                    "Test that 'blkid' is called and fails",
                    "ext4",
                    vec![
                        blkid("DEVNAME=/dev/foo\nPTTYPE=dos\n", 0),
                        cmd("fsck", &["-a", "/dev/foo"], 0),
                    ],
                )
            },
            case(
                "Test that 'blkid' is called and confirms unformatted disk, format passes, mount passes",
                "ext4",
                vec![blkid("", 2), cmd("mkfs.ext4", &["-F", "-m0", "/dev/foo"], 0)],
            ),
            case(
                "... with ext3",
                "ext3",
                vec![blkid("", 2), cmd("mkfs.ext3", &["-F", "-m0", "/dev/foo"], 0)],
            ),
            case(
                "test that none ext4 fs does not get called with ext4 options.",
                "xfs",
                vec![blkid("", 2), cmd("mkfs.xfs", &["-f", "/dev/foo"], 0)],
            ),
            case(
                "Test that 'blkid' is called and reports ext4 partition",
                "ext4",
                vec![blkid(EXT4, 0), cmd("fsck", &["-a", "/dev/foo"], 0)],
            ),
            Case {
                mount_errs: vec![unknown_fs.clone(), None],
                exp: Some(MountErrorType::GetDiskFormatFailed),
                ..case(
                    "Test that 'blkid' is called but has some usage or other errors (an exit code of 4 is returned)",
                    "xfs",
                    vec![blkid("", 4), cmd("mkfs.xfs", &["-f", "/dev/foo"], 0)],
                )
            },
            Case {
                format_options: s(&["-b", "1024"]),
                ..case(
                    "format passes, mount passes (with format options)",
                    "ext4",
                    vec![
                        blkid("", 2),
                        cmd("mkfs.ext4", &["-b", "1024", "-F", "-m0", "/dev/foo"], 0),
                    ],
                )
            },
            Case {
                mount_errs: vec![unknown_fs.clone()],
                exp: Some(MountErrorType::UnknownMountError),
                ..case(
                    "format passes, second mount fails",
                    "ext4",
                    vec![blkid("", 2), cmd("mkfs.ext4", &["-F", "-m0", "/dev/foo"], 0)],
                )
            },
        ];
        // "format fails" (`formatting failed` is a non-exit error) with and
        // without sensitive options.
        for sensitive in [vec![], s(&["mySecret"])] {
            cases.push(Case {
                sensitive,
                mount_errs: vec![unknown_fs.clone()],
                exp: Some(MountErrorType::FormatFailed),
                ..case(
                    "blkid confirms unformatted disk, format fails",
                    "ext4",
                    vec![
                        blkid("", 2),
                        // `fmt.Errorf("formatting failed")`: not an ExitError.
                        CommandScript {
                            cmd: "mkfs.ext4",
                            args: s(&["-F", "-m0", "/dev/foo"]),
                            error: Some("formatting failed"),
                            ..Default::default()
                        },
                    ],
                )
            });
        }

        for c in cases {
            let mounter = Arc::new(FakeMounter::new());
            *mounter.mount_errs.lock().unwrap() = c.mount_errs.clone();
            let exec = Arc::new(FakeExec::new(c.scripts.clone()));
            let sfm = SafeFormatAndMount {
                mounter: mounter.clone(),
                exec: exec.clone(),
                format_limit: Default::default(),
            };
            let dest = tempfile::tempdir().unwrap();
            let dest = dest.path().to_str().unwrap();
            let res = sfm.format_and_mount_sensitive_with_format_options(
                "/dev/foo",
                dest,
                c.fstype,
                &c.mount_options,
                &c.sensitive,
                &c.format_options,
            );
            match c.exp {
                None => {
                    res.unwrap_or_else(|e| panic!("{}: unexpected error: {e}", c.description));
                    let mps = mounter.mount_points.lock().unwrap();
                    assert!(
                        mps.iter().any(|m| m.device == "/dev/foo"),
                        "{}: the directory was not mounted",
                        c.description
                    );
                }
                Some(kind) => {
                    let e = res.expect_err(c.description);
                    assert_eq!(e.kind, kind, "{}: {e}", c.description);
                    if c.sensitive.is_empty() {
                        assert!(!e.to_string().contains("<masked>"), "{}", c.description);
                    } else {
                        assert!(e.to_string().contains("<masked>"), "{}: {e}", c.description);
                        for sens in &c.sensitive {
                            assert!(!e.to_string().contains(sens.as_str()), "{}", c.description);
                        }
                    }
                }
            }
        }
    }
}

mod format_limit_tests {
    //! Ports of `TestWithMaxConcurrentFormat` (`mount_test.go:181-218`),
    //! `TestSafeFormatAndMount_format` concurrency (`mount_linux_test.go:
    //! 655-755`) and `TestFormatTimeout` (`:757-818`).
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::testing::FakeMounter;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::*;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn with_max_concurrent_format() {
        let timeout = Duration::from_secs(60);
        for (max, want_sem) in [(0, false), (-1, false), (1, true), (3, true)] {
            let l = FormatLimit::with_max_concurrent_format(max, timeout);
            assert_eq!(l.has_sem(), want_sem, "max={max}");
            if want_sem {
                assert_eq!(l.capacity(), Some(max as usize));
                assert_eq!(l.timeout(), Some(timeout));
            }
        }
    }

    /// An `Exec` whose command bumps `concurrent` then blocks on `witness`
    /// (the upstream `makeFakeCommandAction` hook).
    struct BlockingExec {
        concurrent: Arc<Mutex<usize>>,
        witness: Mutex<mpsc::Receiver<()>>,
    }

    impl Exec for BlockingExec {
        fn combined_output(&self, _cmd: &str, _args: &[String]) -> (String, Result<(), ExecError>) {
            *self.concurrent.lock().unwrap() += 1;
            // `<-witness`: a value releases one command; a closed channel
            // releases all of them.
            let _ = self.witness.lock().unwrap().recv();
            *self.concurrent.lock().unwrap() -= 1;
            ("complete".to_string(), Ok(()))
        }
    }

    fn setup(
        max: i64,
        timeout: Duration,
    ) -> (Arc<SafeFormatAndMount>, Arc<Mutex<usize>>, mpsc::Sender<()>) {
        let concurrent = Arc::new(Mutex::new(0));
        let (tx, rx) = mpsc::channel();
        let exec = Arc::new(BlockingExec {
            concurrent: concurrent.clone(),
            witness: Mutex::new(rx),
        });
        let sfm = SafeFormatAndMount::new(Arc::new(FakeMounter::new()), exec)
            .with_max_concurrent_format(max, timeout);
        (Arc::new(sfm), concurrent, tx)
    }

    fn spawn_formats(sfm: &Arc<SafeFormatAndMount>, n: usize) {
        for _ in 0..n {
            let sfm = sfm.clone();
            thread::spawn(move || {
                let (_, r) = sfm.format("ext4", &[]);
                r.expect("format");
            });
        }
    }

    fn settle() {
        thread::sleep(Duration::from_millis(100));
    }

    #[test]
    fn format_concurrency_is_bounded() {
        for max in [2usize, 3, 4] {
            let (sfm, concurrent, witness) = setup(max as i64, Duration::from_secs(60));
            // max+1 formatters: only max may run, one waits for a token.
            spawn_formats(&sfm, max + 1);
            settle();
            assert_eq!(*concurrent.lock().unwrap(), max, "max={max}");
            // Let max commands finish; the pending one then runs.
            for _ in 0..max {
                witness.send(()).unwrap();
            }
            settle();
            assert_eq!(*concurrent.lock().unwrap(), 1, "max={max}");
            drop(witness);
            thread::sleep(Duration::from_millis(50));
            assert_eq!(*concurrent.lock().unwrap(), 0, "max={max}");
        }
    }

    #[test]
    fn format_timeout_releases_the_token() {
        let max = 4usize;
        let timeout = Duration::from_millis(200);
        let (sfm, concurrent, witness) = setup(max as i64, timeout);
        spawn_formats(&sfm, max + 1);
        thread::sleep(timeout + Duration::from_millis(100));
        // The token is released at the timeout although the command still
        // runs, so the (max+1)th formatter was admitted.
        assert_eq!(*concurrent.lock().unwrap(), max + 1);
        drop(witness);
        settle();
        assert_eq!(*concurrent.lock().unwrap(), 0);
    }
}
