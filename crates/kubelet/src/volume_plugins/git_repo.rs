use crate::volume_plugins::{
    Attributes, Mounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumeHost,
    VolumePlugin,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
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
    pub async fn new_mounter_with_exec(
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
pub trait Exec: Send + Sync {
    async fn combined_output(
        &self,
        command: &str,
        args: &[String],
        dir: &str,
    ) -> (Vec<u8>, Result<()>);
}

/// `exec.New()` (`git_repo.go:109`): runs the real process.
pub struct RealExec;

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
                delete_service_account_token: false,
                metrics: Box::new(crate::volume_plugins::util::metrics::MetricsNil),
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

        // Wrap EmptyDir, let it do the setup (`NewWrapperMounter`,
        // `volume_host.go:190-199`); its `SetUpAt` ends with
        // `volumeutil.SetReady(ed.getMetaDir())` (`empty_dir.go:284`).
        let wrapped = crate::volume_plugins::util::new_wrapper_mounter(
            &self.host,
            &self.vol_name,
            None,
            &self.pod,
        )
        .await?;
        wrapped.set_up_at(dir, args).await?;

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
pub fn go_path_clean(path: &str) -> String {
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

impl crate::volume_plugins::plugin::MetricsProvider for GitRepoVolumeMounter {
    /// `volume.MetricsNil` (`git_repo.go:141`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        crate::volume_plugins::plugin::MetricsProvider::get_metrics(
            &crate::volume_plugins::util::metrics::MetricsNil,
        )
    }
}
