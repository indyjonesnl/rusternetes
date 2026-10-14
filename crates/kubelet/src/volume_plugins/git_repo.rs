use crate::volume_plugins::{
    Attributes, Mounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumeHost,
    VolumePlugin,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, Volume};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;

/// Port of `pkg/volume/git_repo/git_repo.go` (`gitRepoPlugin`): gitRepo
/// volumes are directories pre-filled from a git repository. They do not
/// persist beyond the lifetime of a pod. The plugin is deprecated upstream and
/// gated by `GitRepoVolumeDriver` (`kube_features.go:1325-1328`: default
/// `false` since 1.33), which `SetUp` checks.
///
/// `ProbeVolumePlugins` (`git_repo.go:36-38`) is registered by the kubelet in
/// `cmd/kubelet/app/plugins.go:61`.
pub struct GitRepoPlugin {
    host: Arc<dyn VolumeHost>,
}

impl GitRepoPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }

    /// `NewMounter` (`git_repo.go:94-111`) with the `exec` field injected, the
    /// seam `doTestSetUp` (`git_repo_test.go:584-585`) uses by assigning
    /// `g.exec = fake`.
    pub(crate) async fn new_mounter_with_exec(
        &self,
        spec: &Spec<'_>,
        pod: &Pod,
        exec: Arc<dyn Exec>,
    ) -> Result<Box<dyn Mounter>> {
        // `validateVolume(spec.Volume.GitRepo)`.
        let source = get_volume_source(spec)
            .map(|(s, _)| s)
            .ok_or_else(|| anyhow!("Spec does not reference a Git repo volume type"))?;
        validate_volume(&source)?;

        Ok(Box::new(GitRepoVolumeMounter {
            host: self.host.clone(),
            vol_name: spec.name().to_string(),
            pod_uid: pod.metadata.uid.clone(),
            pod: pod.clone(),
            source: source.repository,
            revision: source.revision,
            target: source.directory,
            exec,
        }))
    }
}

/// `v1.GitRepoVolumeSource` (`staging/src/k8s.io/api/core/v1/types.go`). The
/// common crate keeps the deprecated source as opaque JSON
/// ([`rusternetes_common::resources::LegacyVolumeSources`]); the plugin reads
/// its three fields. Absent strings are Go's zero value `""`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct GitRepoVolumeSource {
    repository: String,
    revision: String,
    directory: String,
}

/// `wrappedVolumeSpec` (`git_repo.go:46-50`): a plain `EmptyDir{}`.
fn wrapped_volume(vol_name: &str) -> Volume {
    serde_json::from_value(serde_json::json!({"name": vol_name, "emptyDir": {}}))
        .expect("static emptyDir volume")
}

/// `getVolumeSource` (`git_repo.go:291-301`).
fn get_volume_source(spec: &Spec<'_>) -> Option<(GitRepoVolumeSource, bool)> {
    let raw = spec.volume.legacy_sources.git_repo.as_ref()?;
    let source = serde_json::from_value(raw.clone()).unwrap_or_default();
    Some((source, spec.read_only))
}

/// `exec.Interface` (`k8s.io/utils/exec`) reduced to the one call the plugin
/// makes, `b.exec.Command(command, args...)` + `cmd.SetDir(dir)` +
/// `cmd.CombinedOutput()` (`git_repo.go:249-253`). Returns the combined
/// stdout+stderr alongside the error, as Go's `CombinedOutput` does.
#[async_trait]
pub(crate) trait Exec: Send + Sync {
    async fn combined_output(
        &self,
        command: &str,
        args: &[String],
        dir: &str,
    ) -> (Vec<u8>, Result<()>);
}

/// `exec.New()` (`git_repo.go:109`): runs the real process.
struct RealExec;

#[async_trait]
impl Exec for RealExec {
    async fn combined_output(
        &self,
        command: &str,
        args: &[String],
        dir: &str,
    ) -> (Vec<u8>, Result<()>) {
        let (command, args, dir) = (command.to_string(), args.to_vec(), dir.to_string());
        tokio::task::spawn_blocking(move || run_combined(&command, &args, &dir))
            .await
            .unwrap_or_else(|e| (Vec::new(), Err(anyhow!("exec task failed: {e}"))))
    }
}

/// `exec.Cmd.CombinedOutput`: stdout and stderr share one pipe, so the
/// interleaving is the process's own.
fn run_combined(command: &str, args: &[String], dir: &str) -> (Vec<u8>, Result<()>) {
    use std::io::Read;
    let (mut reader, writer) = match std::io::pipe() {
        Ok(p) => p,
        Err(e) => return (Vec::new(), Err(e.into())),
    };
    let writer_err = match writer.try_clone() {
        Ok(w) => w,
        Err(e) => return (Vec::new(), Err(e.into())),
    };
    let mut cmd = std::process::Command::new(command);
    cmd.args(args)
        .current_dir(dir)
        .stdout(writer)
        .stderr(writer_err);
    let spawned = cmd.spawn();
    // The parent's copies of the write ends live in `cmd`; drop them so the
    // read below sees EOF when the child exits.
    drop(cmd);
    let mut child = match spawned {
        Ok(c) => c,
        Err(e) => return (Vec::new(), Err(anyhow!("exec: \"{command}\": {e}"))),
    };
    let mut out = Vec::new();
    let _ = reader.read_to_end(&mut out);
    match child.wait() {
        Ok(status) if status.success() => (out, Ok(())),
        Ok(status) => (
            out,
            Err(anyhow!(
                "exit status {}",
                status
                    .code()
                    .map_or("signal".to_string(), |c| c.to_string())
            )),
        ),
        Err(e) => (out, Err(e.into())),
    }
}

#[async_trait]
impl VolumePlugin for GitRepoPlugin {
    /// `GetPluginName` (`git_repo.go:61-63`).
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::GIT_REPO
    }

    /// `GetVolumeName` (`git_repo.go:65-76`): `"<repository>:<revision>:<directory>"`.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let Some((source, _)) = get_volume_source(spec) else {
            return Err(anyhow!("Spec does not reference a Git repo volume type"));
        };
        Ok(format!(
            "{}:{}:{}",
            source.repository, source.revision, source.directory
        ))
    }

    /// `CanSupport` (`git_repo.go:78-80`):
    /// `spec.Volume != nil && spec.Volume.GitRepo != nil`.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.legacy_sources.git_repo.is_some()
    }

    /// `RequiresRemount` (`git_repo.go:82-84`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`git_repo.go:90-92`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        self.new_mounter_with_exec(spec, pod, Arc::new(RealExec))
            .await
    }

    /// `NewUnmounter` (`git_repo.go:113-121`). `TearDownAt` is
    /// `volumeutil.UnmountViaEmptyDir` (`git_repo.go:287-289`), the same body
    /// the other wrapper plugins share.
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(
            crate::volume_plugins::util::WrappedEmptyDirUnmounter {
                host: self.host.clone(),
                plugin_name: self.name(),
                vol_name: vol_name.to_string(),
                pod_uid: pod_uid.to_string(),
            },
        ))
    }

    /// `ConstructVolumeSpec` (`git_repo.go:123-135`): a volume named after
    /// `volumeName` with an empty `GitRepoVolumeSource{}`.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"gitRepo": {}}),
        )
    }
}

/// `gitRepoVolumeMounter` (`git_repo.go:152-162`) over its embedded
/// `gitRepoVolume` (`volName`, `podUID`, `plugin`).
struct GitRepoVolumeMounter {
    host: Arc<dyn VolumeHost>,
    vol_name: String,
    pod_uid: String,
    pod: Pod,
    source: String,
    revision: String,
    target: String,
    exec: Arc<dyn Exec>,
}

impl GitRepoVolumeMounter {
    /// `getMetaDir` (`git_repo.go:245-247`):
    /// `filepath.Join(host.GetPodPluginDir(podUID, EscapeQualifiedName(name)), volName)`.
    fn get_meta_dir(&self) -> String {
        let escaped = crate::pod_dirs::escape_qualified_name(crate::pod_dirs::plugin::GIT_REPO);
        Path::new(&self.host.get_pod_plugin_dir(&self.pod_uid, &escaped))
            .join(&self.vol_name)
            .to_string_lossy()
            .into_owned()
    }

    /// `execCommand` (`git_repo.go:249-253`).
    async fn exec_command(&self, command: &str, args: &[&str], dir: &str) -> (Vec<u8>, Result<()>) {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.exec.combined_output(command, &args, dir).await
    }
}

#[async_trait]
impl Mounter for GitRepoVolumeMounter {
    /// `gitRepoVolume.GetPath` (`git_repo.go:146-149`).
    fn get_path(&self) -> String {
        self.host.get_pod_volume_dir(
            &self.pod_uid,
            crate::pod_dirs::plugin::GIT_REPO,
            &self.vol_name,
        )
    }

    /// `GetAttributes` (`git_repo.go:164-170`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: false,
            managed: true,
            selinux_relabel: true,
        }
    }

    /// `SetUp` (`git_repo.go:173-179`): refuses while the deprecated
    /// `GitRepoVolumeDriver` gate is off, then `SetUpAt(b.GetPath(), args)`.
    async fn set_up_with(&self, args: &MounterArgs) -> Result<()> {
        if !rusternetes_common::feature_gates::gate_enabled("GitRepoVolumeDriver").unwrap_or(false)
        {
            return Err(anyhow!(
                "git-repo volume plugin has been disabled; if necessary, it may be re-enabled by enabling the feature-gate `GitRepoVolumeDriver`"
            ));
        }
        self.set_up_at(&self.get_path(), args).await
    }

    /// `SetUpAt` (`git_repo.go:182-243`): creates the directory through the
    /// wrapped emptyDir and clones the repository into it.
    async fn set_up_at(&self, dir: &str, args: &MounterArgs) -> Result<()> {
        if crate::volume_plugins::util::is_ready(&self.get_meta_dir()) {
            return Ok(());
        }

        // Wrap EmptyDir, let it do the setup
        // (`NewWrapperMounter`, `volume_host.go:190-199`: name
        // `wrapped_<volName>`, then the plugin the manager finds for the spec
        // — emptyDir).
        let wrapped_name = format!("wrapped_{}", self.vol_name);
        let wrapped_volume = wrapped_volume(&wrapped_name);
        let wrapped = crate::volume_plugins::empty_dir::EmptyDirPlugin::new(self.host.clone())
            .new_mounter(
                &Spec {
                    volume: &wrapped_volume,
                    persistent_volume: None,
                    read_only: false,
                },
                &self.pod,
            )
            .await?;
        wrapped.set_up_at(dir, args).await?;
        // The wrapped emptyDir's `SetUpAt` ends with
        // `volumeutil.SetReady(ed.getMetaDir())` (`empty_dir.go:284`); our
        // emptyDir mounter holds no host, so the caller records it, as the
        // secret plugin does (`TestPlugin`, `git_repo_test.go:497-508`).
        crate::volume_plugins::util::set_ready(
            &crate::volume_plugins::util::wrapped_empty_dir_meta_dir(
                &self.host,
                &self.vol_name,
                &self.pod_uid,
            ),
        );

        let mut clone_args = vec!["clone", "--", self.source.as_str()];
        if !self.target.is_empty() {
            clone_args.push(self.target.as_str());
        }
        let (output, result) = self.exec_command("git", &clone_args, dir).await;
        if let Err(err) = result {
            return Err(anyhow!(
                "failed to exec 'git {}': {}: {}",
                clone_args.join(" "),
                String::from_utf8_lossy(&output),
                err
            ));
        }

        // `ioutil.ReadDir(dir)`: entries sorted by filename.
        let mut files: Vec<String> = std::fs::read_dir(dir)?
            .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<_>>()?;
        files.sort();

        if self.revision.is_empty() {
            // Done!
            crate::volume_plugins::util::set_ready(&self.get_meta_dir());
            return Ok(());
        }

        let subdir = if !self.target.is_empty() && go_path_clean(&self.target) == "." {
            // if target dir is '.', use the current dir
            go_path_clean(dir)
        } else if files.len() == 1 {
            // if target is not '.', use the generated folder
            go_path_clean(&format!("{dir}/{}", files[0]))
        } else {
            // if target is not '.', but generated many files, it's wrong
            return Err(anyhow!("unexpected directory contents: {:?}", files));
        };

        let (output, result) = self
            .exec_command("git", &["checkout", self.revision.as_str()], &subdir)
            .await;
        if let Err(err) = result {
            return Err(anyhow!(
                "failed to exec 'git checkout {}': {}: {}",
                self.revision,
                String::from_utf8_lossy(&output),
                err
            ));
        }
        let (output, result) = self
            .exec_command("git", &["reset", "--hard"], &subdir)
            .await;
        if let Err(err) = result {
            return Err(anyhow!(
                "failed to exec 'git reset --hard': {}: {}",
                String::from_utf8_lossy(&output),
                err
            ));
        }

        // `volume.NewVolumeOwnership(b, dir, mounterArgs.FsGroup, nil, ..)
        // .ChangePermissions()`: "We do not care about return value, this
        // plugin is deprecated" (`git_repo.go:239-241`). `GetAttributes` is
        // `ReadOnly: false`.
        let _ = crate::volume_ownership::set_volume_ownership(Path::new(dir), args.fs_group, false);
        crate::volume_plugins::util::set_ready(&self.get_meta_dir());
        Ok(())
    }
}

/// `validateVolume` (`git_repo.go:255-273`).
fn validate_volume(src: &GitRepoVolumeSource) -> Result<()> {
    validate_non_flag_argument(&src.repository, "repository")?;
    validate_non_flag_argument(&src.revision, "revision")?;
    validate_non_flag_argument(&src.directory, "directory")?;
    if !src.revision.is_empty() && !src.directory.is_empty() {
        let cleaned_dir = go_path_clean(&src.directory);
        if cleaned_dir.contains('/') || cleaned_dir.contains('\\') {
            return Err(anyhow!(
                "{:?} is not a valid directory, it must not contain a directory separator",
                src.directory
            ));
        }
    }
    Ok(())
}

/// `validateNonFlagArgument` (`git_repo.go:303-308`).
fn validate_non_flag_argument(arg: &str, arg_name: &str) -> Result<()> {
    if arg.starts_with('-') {
        return Err(anyhow!("{:?} is an invalid value for {}", arg, arg_name));
    }
    Ok(())
}

/// `filepath.Clean` (`path/filepath/path.go`, Unix): the lexical shortest
/// equivalent path — repeated separators and `.` elements dropped, `..`
/// resolved against the preceding element, `..` at the root dropped.
fn go_path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let rooted = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            p => out.push(p),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::{KubeletVolumeHost, MounterArgs, VolumePluginMgr};
    use rusternetes_common::feature_gates::{reset_to_defaults, set_from_str};
    use rusternetes_common::resources::Volume;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    const GIT_URL: &str = "https://github.com/kubernetes/kubernetes.git";
    const REVISION: &str = "2a30ce65c5ab586b98916d83385c5983edd353a1";

    /// `newTestHost` (`git_repo_test.go:42-48`): a host rooted in a temp dir.
    fn new_test_host() -> (tempfile::TempDir, Arc<dyn VolumeHost>) {
        let root = tempfile::tempdir().unwrap();
        let host: Arc<dyn VolumeHost> = Arc::new(KubeletVolumeHost::new(
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
        assert_eq!(r.volume.name, "vol1");
        assert!(r.volume.legacy_sources.git_repo.is_some());
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
