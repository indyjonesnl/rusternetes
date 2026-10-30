//! Unit tests moved from `src/node_log_query.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, FixedOffset, SecondsFormat};
    pub use regex::Regex;
    pub use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
    pub use std::fs::File;
    pub use std::io::{self, Read, Write};
    pub use std::path::{Component, PathBuf};
    pub use std::process::{Command, Stdio};
    pub use std::sync::{mpsc, LazyLock};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::node_log_query::*;

    fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn far() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    /// `Test_getLoggingCmd` (linux arm).
    #[test]
    #[cfg(target_os = "linux")]
    fn get_logging_cmd_matches_upstream() {
        let base = ["--utc", "--no-pager", "--output=short-precise"];
        let cases: Vec<(&str, Vec<&str>, Vec<&str>)> = vec![
            ("basic", vec![], vec![]),
            (
                "two providers",
                vec!["p1", "p2"],
                vec!["--unit=p1", "--unit=p2"],
            ),
            (
                "empty provider",
                vec!["p1", "", "p2"],
                vec!["--unit=p1", "--unit=p2"],
            ),
        ];
        for (name, services, extra) in cases {
            let (cmd, got) = get_logging_cmd(&NodeLogQuery::default(), &s(&services)).expect(name);
            let mut want: Vec<String> = base.iter().map(|x| x.to_string()).collect();
            want.extend(extra.iter().map(|x| x.to_string()));
            assert_eq!(cmd, "journalctl");
            assert_eq!(got, want, "{name}");
        }
    }

    /// Every option, in upstream's argument order; `dateLayout` is not
    /// zero-padded for month/day/minute/second.
    #[test]
    #[cfg(target_os = "linux")]
    fn get_logging_cmd_all_options() {
        let n = NodeLogQuery {
            services: s(&["kubelet"]),
            files: vec![],
            options: Options {
                since_time: Some(DateTime::parse_from_rfc3339("2019-01-04T02:03:04Z").unwrap()),
                until_time: Some(DateTime::parse_from_rfc3339("2019-12-14T12:13:14Z").unwrap()),
                tail_lines: Some(50),
                boot: Some(-1),
                pattern: "err.*".into(),
            },
        };
        let (_, got) = get_logging_cmd(&n, &n.services).unwrap();
        assert_eq!(
            got,
            s(&[
                "--utc",
                "--no-pager",
                "--output=short-precise",
                "--since=2019-1-4 02:3:4",
                "--until=2019-12-14 12:13:14",
                "--pager-end",
                "--lines=50",
                "--unit=kubelet",
                "--grep=err.*",
                "--boot",
                "-1",
            ])
        );
    }

    /// `Test_newNodeLogQuery`.
    #[test]
    fn new_node_log_query_matches_upstream() {
        let valid = "2019-12-04T02:00:00Z";
        let t = DateTime::parse_from_rfc3339(valid).unwrap();
        let opts = |f: &dyn Fn(&mut Options)| {
            let mut o = Options::default();
            f(&mut o);
            Some(NodeLogQuery {
                options: o,
                ..Default::default()
            })
        };
        type Case = (
            Vec<(&'static str, &'static str)>,
            Option<Option<NodeLogQuery>>,
        );
        let cases: Vec<Case> = vec![
            (vec![], Some(None)),
            (vec![("unknown", "true")], Some(None)),
            (vec![("sinceTime", "")], Some(None)),
            (vec![("sinceTime", "2019-12-04 02:00:00")], None),
            (vec![("sinceTime", "2019-12-04 02:00:00.000")], None),
            (vec![("sinceTime", "2019-12-04 02")], None),
            (vec![("sinceTime", "2019-12-04 02:00")], None),
            (
                vec![("sinceTime", valid)],
                Some(opts(&|o| o.since_time = Some(t))),
            ),
            (vec![("untilTime", "")], Some(None)),
            (vec![("untilTime", "2019-12-04 02:00:00")], None),
            (vec![("untilTime", "2019-12-04 02:00:00.000")], None),
            (vec![("untilTime", "2019-12-04 02")], None),
            (vec![("untilTime", "2019-12-04 02:00")], None),
            (
                vec![("untilTime", valid)],
                Some(opts(&|o| o.until_time = Some(t))),
            ),
            (
                vec![("tailLines", "100")],
                Some(opts(&|o| o.tail_lines = Some(100))),
            ),
            (vec![("tailLines", "foo")], None),
            (vec![("tailLines", " ")], None),
            (
                vec![("pattern", "foo")],
                Some(opts(&|o| o.pattern = "foo".into())),
            ),
            (vec![("boot", "")], Some(None)),
            (vec![("boot", "0")], Some(opts(&|o| o.boot = Some(0)))),
            (vec![("boot", "-23")], Some(opts(&|o| o.boot = Some(-23)))),
            (vec![("boot", "foo")], None),
            (vec![("boot", " ")], None),
            (vec![("query", "")], None),
            (vec![("query", "   "), ("query", "    ")], None),
            (
                vec![("query", "foo")],
                Some(Some(NodeLogQuery {
                    services: s(&["foo"]),
                    ..Default::default()
                })),
            ),
            (
                vec![("query", "foo"), ("query", "bar")],
                Some(Some(NodeLogQuery {
                    services: s(&["foo", "bar"]),
                    ..Default::default()
                })),
            ),
            (
                vec![("query", "foo"), ("query", "/bar")],
                Some(Some(NodeLogQuery {
                    services: s(&["foo"]),
                    files: s(&["/bar"]),
                    ..Default::default()
                })),
            ),
            (
                vec![("query", "/foo"), ("query", "\\bar")],
                Some(Some(NodeLogQuery {
                    files: s(&["/foo", "\\bar"]),
                    ..Default::default()
                })),
            ),
        ];
        for (query, want) in cases {
            let got = new_node_log_query(&q(&query));
            match want {
                None => assert!(got.is_err(), "{query:?}: want error, got {got:?}"),
                Some(w) => assert_eq!(got.unwrap(), w, "{query:?}"),
            }
        }
    }

    #[test]
    fn new_node_log_query_error_text() {
        let e = new_node_log_query(&q(&[("tailLines", "foo")])).unwrap_err();
        assert_eq!(
            aggregate_message(&e),
            "tailLines: Invalid value: \"foo\": strconv.Atoi: parsing \"foo\": invalid syntax"
        );
        let e = new_node_log_query(&q(&[("query", " ")])).unwrap_err();
        assert_eq!(
            aggregate_message(&e),
            "query: Invalid value: [\" \"]: may not be empty"
        );
        let e = new_node_log_query(&q(&[("tailLines", "x"), ("boot", "y")])).unwrap_err();
        assert!(aggregate_message(&e).starts_with('['));
    }

    /// `Test_validateServices`.
    #[test]
    fn validate_services_matches_upstream() {
        let long = "svc1".repeat(100);
        let cases: Vec<(&str, Vec<String>, bool)> = vec![
            ("one service", s(&["svc1"]), false),
            ("two services", s(&["svc1", "svc2"]), false),
            ("dot service", s(&["svc.foo"]), false),
            ("underscore service", s(&["svc_foo"]), false),
            ("at service", s(&["svc@foo"]), false),
            ("colon service", s(&["svc:foo"]), false),
            ("invalid service new line", s(&["svc\n"]), true),
            ("invalid service with dot", s(&["svc.foo\n"]), true),
            ("invalid service with slash", s(&["svc/foo"]), true),
            ("long service", vec![long], true),
            (
                "max number of services",
                s(&["svc1", "svc2", "svc.foo", "svc_foo", "svc@foo"]),
                true,
            ),
        ];
        for (name, services, want_err) in cases {
            assert_eq!(!validate_services(&services).is_empty(), want_err, "{name}");
        }
    }

    /// `Test_nodeLogQuery_validate`.
    #[test]
    fn validate_matches_upstream() {
        let since = DateTime::parse_from_rfc3339("2023-01-04T02:00:00Z").unwrap();
        let until = DateTime::parse_from_rfc3339("2023-02-04T02:00:00Z").unwrap();
        let svc = |o: Options| NodeLogQuery {
            services: s(&["svc1"]),
            options: o,
            ..Default::default()
        };
        let cases: Vec<(&str, NodeLogQuery, bool)> = vec![
            ("empty", NodeLogQuery::default(), true),
            (
                "empty with options",
                NodeLogQuery {
                    options: Options {
                        since_time: Some(since),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                true,
            ),
            ("one service", svc(Options::default()), false),
            (
                "two services",
                NodeLogQuery {
                    services: s(&["svc1", "svc2"]),
                    ..Default::default()
                },
                false,
            ),
            (
                "one service one file",
                NodeLogQuery {
                    services: s(&["svc1"]),
                    files: s(&["/test1.log"]),
                    ..Default::default()
                },
                true,
            ),
            (
                "two files",
                NodeLogQuery {
                    files: s(&["/test1.log", "/test2.log"]),
                    ..Default::default()
                },
                true,
            ),
            (
                "one file options",
                NodeLogQuery {
                    files: s(&["/test1.log"]),
                    options: Options {
                        pattern: "foo".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                true,
            ),
            (
                "invalid pattern",
                svc(Options {
                    pattern: "foo\\".into(),
                    ..Default::default()
                }),
                true,
            ),
            (
                "since",
                svc(Options {
                    since_time: Some(since),
                    ..Default::default()
                }),
                false,
            ),
            (
                "until",
                svc(Options {
                    until_time: Some(until),
                    ..Default::default()
                }),
                false,
            ),
            (
                "since until",
                svc(Options {
                    since_time: Some(until),
                    until_time: Some(since),
                    ..Default::default()
                }),
                true,
            ),
            (
                "boot",
                svc(Options {
                    boot: Some(-1),
                    ..Default::default()
                }),
                cfg!(windows),
            ),
            (
                "boot out of range",
                svc(Options {
                    boot: Some(1),
                    ..Default::default()
                }),
                true,
            ),
            (
                "tailLines",
                svc(Options {
                    tail_lines: Some(100),
                    ..Default::default()
                }),
                false,
            ),
            (
                "tailLines at max (inclusive)",
                svc(Options {
                    tail_lines: Some(100_000),
                    ..Default::default()
                }),
                false,
            ),
            (
                "tailLines out of range",
                svc(Options {
                    tail_lines: Some(100_001),
                    ..Default::default()
                }),
                true,
            ),
        ];
        for (name, n, want_err) in cases {
            let errs = n.validate();
            assert_eq!(!errs.is_empty(), want_err, "{name}: {errs:?}");
        }
    }

    /// The single-file arm of `validate` stats inside the log root only.
    #[test]
    fn validate_single_file_is_confined_to_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.log"), "x").unwrap();
        let root = dir.path().to_str().unwrap();
        let file = |f: &str| NodeLogQuery {
            files: s(&[f]),
            ..Default::default()
        };
        assert!(file("a.log").validate_in(root).is_empty());
        assert!(!file("missing.log").validate_in(root).is_empty());
        // os.Root rejects absolute names and `..` above the root.
        assert!(!file("/a.log").validate_in(root).is_empty());
        assert!(!file("../a.log").validate_in(root).is_empty());
        assert!(!file("sub/../../a.log").validate_in(root).is_empty());
    }

    #[test]
    fn symlink_out_of_root_is_rejected() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), "s").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), dir.path().join("l")).unwrap();
        let root = dir.path().to_str().unwrap();
        let mut out = Vec::new();
        heuristics_copy_file_logs(far(), &mut out, root, "l");
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("error getting log for l"), "{out}");
    }

    /// `Test_heuristicsCopyFileLogs`.
    #[test]
    fn heuristics_copy_file_logs_matches_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let run = |svc: &str| {
            let mut out = Vec::new();
            heuristics_copy_file_logs(far(), &mut out, root, svc);
            String::from_utf8(out).unwrap()
        };

        assert!(run("service.log").contains("log not found for service.log"));
        assert!(run("service").contains("log not found for service"));

        std::fs::write(dir.path().join("service.log"), "valid logs").unwrap();
        assert_eq!(run("service.log"), "valid logs");
        assert_eq!(run("service"), "valid logs");

        // A directory doesn't cause errors.
        std::fs::create_dir(dir.path().join("service")).unwrap();
        assert_eq!(run("service"), "valid logs");

        // Service logs return the first matching file.
        std::fs::write(dir.path().join("service").join("service.log"), "error").unwrap();
        assert_eq!(run("service"), "valid logs");
    }

    #[test]
    fn heuristics_falls_through_to_service_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("svc")).unwrap();
        std::fs::write(dir.path().join("svc").join("svc.log"), "nested").unwrap();
        let mut out = Vec::new();
        heuristics_copy_file_logs(far(), &mut out, dir.path().to_str().unwrap(), "svc");
        assert_eq!(String::from_utf8(out).unwrap(), "nested");
    }

    #[test]
    fn run_command_merges_stdout_and_stderr() {
        let mut out = Vec::new();
        let args = s(&["-c", "echo out; echo err 1>&2; exit 3"]);
        let status = run_command("sh", &args, far(), &mut out).unwrap();
        assert_eq!(status.code(), Some(3));
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("out") && text.contains("err"), "{text}");
    }

    #[test]
    fn run_command_kills_at_deadline() {
        let mut out = Vec::new();
        let start = Instant::now();
        let status = run_command(
            "sh",
            &s(&["-c", "exec sleep 20"]),
            Instant::now() + Duration::from_millis(300),
            &mut out,
        )
        .unwrap();
        assert!(!status.success());
        assert!(start.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn run_command_missing_binary_is_an_error() {
        let mut out = Vec::new();
        assert!(run_command("definitely-not-a-binary-3890", &[], far(), &mut out).is_err());
    }
}
