//! Unit tests moved from `src/volume_plugins/git_repo.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumeHost,
        VolumePlugin,
    };
    pub use serde::Deserialize;
    pub use std::path::Path;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::feature_gates::{reset_to_defaults, set_from_str};
    use rusternetes_common::resources::Volume;
    use rusternetes_kubelet::volume_plugins::git_repo::*;
    use rusternetes_kubelet::volume_plugins::{KubeletVolumeHost, MounterArgs, VolumePluginMgr};
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    const GIT_URL: &str = "https://github.com/kubernetes/kubernetes.git";
    const REVISION: &str = "2a30ce65c5ab586b98916d83385c5983edd353a1";

    /// `newTestHost` (`git_repo_test.go:42-48`): a host rooted in a temp dir.
    fn new_test_host() -> (tempfile::TempDir, Arc<dyn VolumeHost>) {
        let root = tempfile::tempdir().unwrap();
        let host: Arc<dyn VolumeHost> = Arc::new(KubeletVolumeHost::new_for_tests(
            root.path().to_string_lossy().into_owned(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        ));
        (root, host)
    }

    fn volume(source: serde_json::Value) -> Volume {
        serde_json::from_value(json!({"name": "vol1", "gitRepo": source})).unwrap()
    }

    fn spec(v: &Volume) -> Spec<'_> {
        Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        }
    }

    fn pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "uid": "poduid"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    /// Resets the process-global feature gates on drop, as
    /// `featuregatetesting.SetFeatureGateDuringTest`'s cleanup does.
    struct GateGuard;
    impl GateGuard {
        fn set(enabled: bool) -> Self {
            reset_to_defaults();
            set_from_str(&format!("GitRepoVolumeDriver={enabled}")).unwrap();
            GateGuard
        }
    }
    impl Drop for GateGuard {
        fn drop(&mut self) {
            reset_to_defaults();
        }
    }

    /// `TestCanSupport` (`git_repo_test.go:50-66`).
    #[test]
    fn can_support() {
        let (_root, host) = new_test_host();
        let mgr = VolumePluginMgr::new(vec![Box::new(GitRepoPlugin::new(host))]);
        let plug = mgr
            .find_plugin_by_name("kubernetes.io/git-repo")
            .expect("Can't find the plugin by name");
        assert_eq!(plug.name(), "kubernetes.io/git-repo");
        let v = volume(json!({}));
        assert!(plug.can_support(&spec(&v)));
        let other: Volume = serde_json::from_value(json!({"name": "x", "emptyDir": {}})).unwrap();
        assert!(!plug.can_support(&spec(&other)));
    }

    /// `GetVolumeName` (`git_repo.go:65-76`).
    #[test]
    fn volume_name_is_repository_revision_directory() {
        let (_root, host) = new_test_host();
        let p = GitRepoPlugin::new(host);
        let v = volume(json!({"repository": "r", "revision": "v", "directory": "d"}));
        assert_eq!(p.get_volume_name(&spec(&v)).unwrap(), "r:v:d");
        let other: Volume = serde_json::from_value(json!({"name": "x", "emptyDir": {}})).unwrap();
        assert_eq!(
            p.get_volume_name(&spec(&other)).unwrap_err().to_string(),
            "Spec does not reference a Git repo volume type"
        );
    }

    #[test]
    fn plugin_properties_match_upstream() {
        let (_root, host) = new_test_host();
        let p = GitRepoPlugin::new(host);
        let v = volume(json!({}));
        assert!(!p.requires_remount(&spec(&v)));
        assert!(!p.supports_selinux_context_mount(&spec(&v)).unwrap());
    }

    /// `ConstructVolumeSpec` (`git_repo.go:123-135`): an empty `GitRepo`
    /// source named after the volume.
    #[test]
    fn construct_volume_spec_is_an_empty_git_repo_source() {
        let (_root, host) = new_test_host();
        let p = GitRepoPlugin::new(host);
        let r = p.construct_volume_spec("vol1", "/ignored").unwrap();
        let volume = r.volume.unwrap();
        assert_eq!(volume.name, "vol1");
        assert!(volume.legacy_sources.git_repo.is_some());
    }

    /// `expectedCommand` (`git_repo_test.go:69-74`).
    struct Expected {
        cmd: Vec<String>,
        dir: &'static str,
    }

    fn exp(cmd: &[&str], dir: &'static str) -> Expected {
        Expected {
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
            dir,
        }
    }

    /// `fakeexec.FakeExec` + `FakeCmd` for the test: records every command and
    /// directory, and a `git clone` creates the directory the real one would
    /// (`doTestSetUp`, `git_repo_test.go:556-562`).
    #[derive(Default)]
    struct FakeExec {
        log: Mutex<Vec<(Vec<String>, String)>>,
    }

    #[async_trait::async_trait]
    impl Exec for FakeExec {
        async fn combined_output(
            &self,
            command: &str,
            args: &[String],
            dir: &str,
        ) -> (Vec<u8>, anyhow::Result<()>) {
            let mut cmd = vec![command.to_string()];
            cmd.extend(args.iter().cloned());
            if args[0] == "clone" {
                // path.Base of the last arg: `target_dir` -> target_dir,
                // the URL -> `kubernetes.git` -> kubernetes, `.`/`./.` -> `.`.
                let last = args.last().unwrap();
                let mut sub = match last.trim_end_matches('/').rsplit('/').next() {
                    Some(s) if !s.is_empty() => s.to_string(),
                    _ => ".".to_string(),
                };
                if sub == "kubernetes.git" {
                    sub = "kubernetes".into();
                }
                std::fs::create_dir_all(Path::new(dir).join(sub)).unwrap();
            }
            self.log.lock().unwrap().push((cmd, dir.to_string()));
            (Vec::new(), Ok(()))
        }
    }

    struct Scenario {
        name: &'static str,
        source: serde_json::Value,
        expecteds: Vec<Expected>,
        is_expected_failure: bool,
        plugin_disabled: bool,
    }

    fn scenarios() -> Vec<Scenario> {
        let src = |dir: Option<&str>, rev: Option<&str>| {
            let mut m = json!({"repository": GIT_URL});
            if let Some(d) = dir {
                m["directory"] = json!(d);
            }
            if let Some(r) = rev {
                m["revision"] = json!(r);
            }
            m
        };
        let ok = |name, source: serde_json::Value, expecteds: Vec<Expected>| {
            vec![
                Scenario {
                    name,
                    source: source.clone(),
                    expecteds,
                    is_expected_failure: false,
                    plugin_disabled: false,
                },
                Scenario {
                    name,
                    source,
                    expecteds: vec![],
                    is_expected_failure: true,
                    plugin_disabled: true,
                },
            ]
        };
        let bad = |name, source: serde_json::Value| {
            [false, true]
                .into_iter()
                .map(|d| Scenario {
                    name,
                    source: source.clone(),
                    expecteds: vec![],
                    is_expected_failure: true,
                    plugin_disabled: d,
                })
                .collect::<Vec<_>>()
        };
        let clone = |extra: &[&str]| {
            let mut c = vec!["git", "clone", "--", GIT_URL];
            c.extend_from_slice(extra);
            exp(&c, "")
        };
        let checkout = |dir| exp(&["git", "checkout", REVISION], dir);
        let reset = |dir| exp(&["git", "reset", "--hard"], dir);
        let mut all = vec![];
        all.extend(ok(
            "target-dir",
            src(Some("target_dir"), Some(REVISION)),
            vec![
                clone(&["target_dir"]),
                checkout("/target_dir"),
                reset("/target_dir"),
            ],
        ));
        all.extend(ok(
            "target-dir-no-revision",
            src(Some("target_dir"), None),
            vec![clone(&["target_dir"])],
        ));
        all.extend(ok("only-git-clone", src(None, None), vec![clone(&[])]));
        all.extend(ok(
            "no-target-dir",
            src(Some(""), Some(REVISION)),
            vec![clone(&[]), checkout("/kubernetes"), reset("/kubernetes")],
        ));
        all.extend(ok(
            "current-dir",
            src(Some("."), Some(REVISION)),
            vec![clone(&["."]), checkout(""), reset("")],
        ));
        all.extend(ok(
            "current-dir-mess",
            src(Some("./."), Some(REVISION)),
            vec![clone(&["./."]), checkout(""), reset("")],
        ));
        all.extend(bad("invalid-repository", json!({"repository": "--foo"})));
        all.extend(bad("invalid-revision", src(None, Some("--bar"))));
        all.extend(bad("invalid-directory", src(Some("-b"), None)));
        all.extend(bad(
            "invalid-revision-directory-combo",
            json!({"repository": GIT_URL, "revision": "main", "directory": "foo/bar"}),
        ));
        all
    }

    /// `TestPlugin` (`git_repo_test.go:84-443`) over `doTestPlugin` and
    /// `doTestPlugin` (`git_repo_test.go:445-535`) and `doTestSetUp` (`:537-615`).
    #[tokio::test]
    #[serial_test::serial]
    async fn plugin_scenarios() {
        for sc in scenarios() {
            let label = format!("{}/gitRepoPluginDisabled:{}", sc.name, sc.plugin_disabled);
            let _gate = GateGuard::set(!sc.plugin_disabled);
            let errs = do_test_plugin(&sc).await;
            assert!(
                !(errs.is_empty() && sc.is_expected_failure),
                "Unexpected success for scenario: {label}"
            );
            assert!(
                !(!errs.is_empty() && !sc.is_expected_failure),
                "Unexpected failure for scenario: {label} - {errs:?}"
            );
        }
    }

    async fn do_test_plugin(sc: &Scenario) -> Vec<String> {
        let mut errs = vec![];
        let (root, host) = new_test_host();
        let plugin = GitRepoPlugin::new(host);
        let v = volume(sc.source.clone());
        let fake = Arc::new(FakeExec::default());
        let mounter = match plugin
            .new_mounter_with_exec(&spec(&v), &pod(), fake.clone())
            .await
        {
            Ok(m) => m,
            Err(e) => return vec![format!("failed to make a new Mounter: {e}")],
        };

        let path = mounter.get_path();
        let suffix = Path::new("pods/poduid/volumes/kubernetes.io~git-repo").join("vol1");
        if !Path::new(&path).ends_with(&suffix) {
            return vec![format!("got unexpected path: {path}")];
        }

        // Test setUp()
        if let Err(e) = mounter.set_up_with(&MounterArgs::default()).await {
            errs.push(e.to_string());
        }
        let log = fake.log.lock().unwrap().clone();
        if log.len() != sc.expecteds.len() {
            errs.push(format!(
                "unexpected command calls in scenario: expected {}, saw: {}",
                sc.expecteds.len(),
                log.len()
            ));
        }
        let want_cmds: Vec<&Vec<String>> = sc.expecteds.iter().map(|e| &e.cmd).collect();
        let got_cmds: Vec<&Vec<String>> = log.iter().map(|(c, _)| c).collect();
        if want_cmds != got_cmds {
            errs.push(format!(
                "unexpected commands: {got_cmds:?}, expected: {want_cmds:?}"
            ));
        }
        let want_dirs: Vec<PathBuf> = sc
            .expecteds
            .iter()
            .map(|e| Path::new(&path).join(e.dir.trim_start_matches('/')))
            .collect();
        let got_dirs: Vec<PathBuf> = log.iter().map(|(_, d)| PathBuf::from(d)).collect();
        if want_dirs != got_dirs {
            errs.push(format!(
                "unexpected directories: {got_dirs:?}, expected: {want_dirs:?}"
            ));
        }

        if let Err(e) = std::fs::metadata(&path) {
            errs.push(format!(
                "SetUp() failed, volume path not created: {path}: {e}"
            ));
            return errs;
        }

        // gitRepo volume should create its own empty wrapper path
        let wrapper_meta = root
            .path()
            .join("pods/poduid/plugins/kubernetes.io~empty-dir/wrapped_vol1");
        if std::fs::metadata(&wrapper_meta).is_err() {
            errs.push(format!(
                "SetUp() failed, empty-dir wrapper path is not created: {}",
                wrapper_meta.display()
            ));
        }

        let unmounter = match plugin.new_unmounter("vol1", "poduid") {
            Ok(u) => u,
            Err(e) => {
                errs.push(format!("failed to make a new Unmounter: {e}"));
                return errs;
            }
        };
        if let Err(e) = unmounter.tear_down().await {
            errs.push(format!("expected success, got: {e}"));
            return errs;
        }
        if std::fs::metadata(&path).is_ok() {
            errs.push(format!(
                "TearDown() failed, volume path still exists: {path}"
            ));
        }
        errs
    }

    /// `if volumeutil.IsReady(b.getMetaDir()) { return nil }` (`git_repo.go:183-185`):
    /// a second SetUp of a completed volume runs no git command.
    #[tokio::test]
    #[serial_test::serial]
    async fn second_set_up_of_a_ready_volume_is_a_no_op() {
        let _gate = GateGuard::set(true);
        let (_root, host) = new_test_host();
        let plugin = GitRepoPlugin::new(host);
        let v = volume(json!({"repository": GIT_URL}));
        let fake = Arc::new(FakeExec::default());
        let m = plugin
            .new_mounter_with_exec(&spec(&v), &pod(), fake.clone())
            .await
            .unwrap();
        m.set_up_with(&MounterArgs::default()).await.unwrap();
        m.set_up_with(&MounterArgs::default()).await.unwrap();
        assert_eq!(fake.log.lock().unwrap().len(), 1);
    }

    /// `GetAttributes` (`git_repo.go:164-170`).
    #[tokio::test]
    async fn attributes_match_upstream() {
        let (_root, host) = new_test_host();
        let plugin = GitRepoPlugin::new(host);
        let v = volume(json!({"repository": GIT_URL}));
        let m = plugin.new_mounter(&spec(&v), &pod()).await.unwrap();
        assert_eq!(
            m.get_attributes(),
            Attributes {
                read_only: false,
                managed: true,
                selinux_relabel: true
            }
        );
    }

    /// The `fmt.Errorf` of `SetUp` (`git_repo.go:174-176`).
    #[tokio::test]
    #[serial_test::serial]
    async fn disabled_gate_error_message() {
        let _gate = GateGuard::set(false);
        let (_root, host) = new_test_host();
        let plugin = GitRepoPlugin::new(host);
        let v = volume(json!({"repository": GIT_URL}));
        let m = plugin.new_mounter(&spec(&v), &pod()).await.unwrap();
        assert_eq!(
            m.set_up_with(&MounterArgs::default())
                .await
                .unwrap_err()
                .to_string(),
            "git-repo volume plugin has been disabled; if necessary, it may be re-enabled by enabling the feature-gate `GitRepoVolumeDriver`"
        );
    }

    /// Upstream's default at 1.35: `Default: false, PreRelease: Deprecated`
    /// (`kube_features.go:1325-1328`), so an unconfigured kubelet refuses.
    #[tokio::test]
    #[serial_test::serial]
    async fn gate_is_off_by_default() {
        reset_to_defaults();
        let (_root, host) = new_test_host();
        let plugin = GitRepoPlugin::new(host);
        let v = volume(json!({"repository": GIT_URL}));
        let m = plugin.new_mounter(&spec(&v), &pod()).await.unwrap();
        assert!(m.set_up_with(&MounterArgs::default()).await.is_err());
    }

    /// `exec.Cmd.CombinedOutput`: stdout and stderr in one stream, a non-zero
    /// exit is an error, and the command runs in the given directory.
    #[tokio::test]
    async fn real_exec_combines_output_and_reports_exit_status() {
        let dir = tempfile::tempdir().unwrap();
        let args: Vec<String> = ["-c", "echo out; echo err 1>&2; pwd; exit 3"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (out, res) = RealExec
            .combined_output("sh", &args, &dir.path().to_string_lossy())
            .await;
        let out = String::from_utf8(out).unwrap();
        assert!(out.starts_with("out\nerr\n"), "{out:?}");
        assert!(out.contains(dir.path().file_name().unwrap().to_str().unwrap()));
        assert_eq!(res.unwrap_err().to_string(), "exit status 3");
        let (_, res) = RealExec
            .combined_output("no-such-binary-xyz", &[], &dir.path().to_string_lossy())
            .await;
        assert!(res.is_err());
    }

    /// `filepath.Clean` cases the plugin depends on.
    #[test]
    fn go_clean_matches_filepath_clean() {
        for (i, o) in [
            ("", "."),
            (".", "."),
            ("./.", "."),
            ("a/../b", "b"),
            ("a/b/", "a/b"),
            ("/../a", "/a"),
            ("../a", "../a"),
            ("a//b", "a/b"),
            ("foo/bar", "foo/bar"),
        ] {
            assert_eq!(go_path_clean(i), o, "Clean({i:?})");
        }
    }
}
