use rusternetes_common::quantity::parse_resource_value;
use rusternetes_common::resources::Pod;
use tracing::{info, warn};

/// Set up an EmptyDir volume directory with mode 0o777, matching upstream
/// Kubernetes (pkg/volume/emptydir/empty_dir.go setupDir).
///
/// The chmod is idempotent: it runs after `create_dir_all` regardless of whether
/// the directory was newly created or already existed (e.g. from a prior pod run
/// that left stale state). This guarantees the host-side directory always exposes
/// mode 0o777 to bind-mount consumers.
///
/// On Linux — where Kubernetes conformance runs — bind mounts preserve these mode
/// bits inside the container. On macOS dev VMs (Podman Machine / Docker Desktop
/// virtiofs) the host mode bits are NOT propagated through the shared-filesystem
/// layer; that is a known dev-env limitation and not a kubelet bug. The
/// `[Conformance] EmptyDir.*(mode|0644|0666|0777)` tests are all `[LinuxOnly]`,
/// so the chmod path here is the production code path.
pub fn setup_emptydir_dir(path: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Mirrors pkg/volume/emptydir/empty_dir.go setupDir: chmod is
        // best-effort. A failed chmod (e.g., non-root host, restricted
        // filesystem, immutable uid mapping) must not prevent the pod from
        // starting — containers bind-mount as root, and mode bits set inside
        // the container are sufficient for the pod to function. On Linux
        // (where conformance runs) the chmod always succeeds, so this only
        // affects edge-cases and dev VMs.
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777)) {
            warn!(
                "Failed to set mode 0777 on emptyDir volume dir {}: {} (pods will still start)",
                path, e
            );
        }
    }
    Ok(())
}

/// `mount.Interface.MountSensitiveWithoutSystemd`
/// (`staging/src/k8s.io/mount-utils/mount_linux.go`): `mount -t <fstype>
/// [-o <options>] <source> <target>`, reporting the command output on failure.
pub(crate) fn mount_fs(
    source: &str,
    target: &str,
    fstype: &str,
    options: &[String],
) -> anyhow::Result<()> {
    let mut cmd = std::process::Command::new("mount");
    cmd.args(["-t", fstype]);
    if !options.is_empty() {
        cmd.args(["-o", &options.join(",")]);
    }
    let out = cmd
        .args([source, target])
        .output()
        .map_err(|e| anyhow::anyhow!("could not exec mount for {fstype} at {target}: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "mount failed: exit status {}\nMounting command: mount\nMounting arguments: -t {fstype} -o {} {source} {target}\nOutput: {}",
            out.status,
            options.join(","),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    info!("Mounted {} at {}", fstype, target);
    Ok(())
}

/// Unmount `target`: `mount.Interface.Unmount`
/// (`staging/src/k8s.io/mount-utils/mount_linux.go:401`), which runs
/// `umount <target>` and reports its output on failure.
///
/// The counterpart of [`mount_tmpfs_for_emptydir`], which nothing used to
/// undo: a `medium: Memory` emptyDir's tmpfs outlived its pod and blocked the
/// removal of the pod directory (#2537).
pub(crate) fn unmount_path(target: &str) -> anyhow::Result<()> {
    let out = std::process::Command::new("umount")
        .arg(target)
        .output()
        .map_err(|e| anyhow::anyhow!("could not exec umount for {target}: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "unmount failed: exit status {}\nUnmounting arguments: {target}\nOutput: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    info!("Unmounted {}", target);
    Ok(())
}

/// Parse a Kubernetes `resource.Quantity` (e.g. `1Gi`, `512Mi`, `0.5Gi`) into a
/// byte count. Returns `None` on input that upstream `ParseQuantity` rejects,
/// and on a negative quantity — a `sizeLimit` below zero is not a size, and
/// letting it through as a `u64` would either wrap or clamp to a zero-byte cap.
///
/// The suffix table lives in [`parse_resource_value`]; the local one here was
/// missing `Pi`/`Ei`/`P`/`E` and accepted a non-upstream `K`.
pub fn parse_quantity_bytes(q: &str) -> Option<u64> {
    u64::try_from(parse_resource_value(q, "memory").ok()?).ok()
}

/// Create the host-side termination-log file with world-writable permissions so
/// a container running as a non-root UID can write its termination message
/// through the bind mount.
///
/// Mirrors upstream `pkg/kubelet/kuberuntime/kuberuntime_container.go::makeMounts`
/// in release-1.35 (around lines 502-531), which does:
///
/// ```text
/// fs, err := m.osInterface.Create(containerLogPath)
/// ...
/// fs.Close()
/// // Chmod is needed because os.Create() ends up calling open(2) to create
/// // the file, so the final mode used is "mode & ~umask". But we want to
/// // make sure the specified mode is used in the file no matter what the
/// // umask is.
/// if err := m.osInterface.Chmod(containerLogPath, 0666); err != nil { ... }
/// ```
///
/// Without the explicit chmod, `std::fs::write` (which calls `open(2)` with
/// the default `0o666` requested mode) yields `0o664` on typical hosts after
/// the process umask (`0o002`) is applied — root-owned, group-writable only.
/// The conformance test
/// `[sig-node] Container Runtime blackbox test on terminated container
/// should report termination message if TerminationMessagePath is set as
/// non-root user and at a non-default path` runs the container as UID 10000
/// and shells `echo -n DONE > /dev/termination-custom-log`. With a `0o664`
/// root-owned file the redirect fails (`Permission denied`), the container
/// exits non-zero, and the pod is reported `Failed` instead of `Succeeded`.
///
/// Idempotent: the chmod runs whether the file was just created or already
/// existed (e.g. from a prior pod incarnation), and `std::fs::write("")`
/// truncates a pre-existing file so we never read back a stale message.
pub fn setup_termination_message_file(path: &str) -> std::io::Result<()> {
    std::fs::write(path, "")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // 0o666 mirrors upstream's `os.Chmod(containerLogPath, 0666)`. Do not
        // narrow this — the container's RunAsUser is arbitrary (any non-root
        // UID upstream chooses to test with).
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
    }
    Ok(())
}

/// Unix special-file kinds that HostPath validates separately from
/// regular files / directories. Internal helper for `check_host_path_type`.
#[derive(Debug, Clone, Copy)]
enum UnixKind {
    Socket,
    CharDevice,
    BlockDevice,
}

fn check_unix_special(
    meta_res: std::io::Result<std::fs::Metadata>,
    kind: UnixKind,
    noun: &str,
    path: &str,
) -> Result<(), (HostPathCheck, String)> {
    let failed = |check| {
        (
            check,
            format!("hostPath type check failed: {path} is not {noun}"),
        )
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        match meta_res {
            Ok(meta) => {
                let ft = meta.file_type();
                let matched = match kind {
                    UnixKind::Socket => ft.is_socket(),
                    UnixKind::CharDevice => ft.is_char_device(),
                    UnixKind::BlockDevice => ft.is_block_device(),
                };
                if matched {
                    Ok(())
                } else {
                    Err(failed(HostPathCheck::WrongKind))
                }
            }
            Err(_) => Err(failed(HostPathCheck::Missing)),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (meta_res, kind);
        Err(failed(HostPathCheck::WrongKind))
    }
}

/// Result of validating a HostPath volume against its declared `type` per
/// upstream Kubernetes `pkg/volume/host_path/host_path.go::checkType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPathCheck {
    /// The host path exists (or was created) and matches the declared type.
    Ok,
    /// The declared type requires the path to pre-exist but it does not.
    Missing,
    /// The path exists but is not the kind the declared type requires
    /// (e.g. `File` but the path is a directory).
    WrongKind,
    /// The declared type string is not one of the supported variants.
    UnsupportedType,
}

/// Pure-function mirror of upstream `pkg/volume/host_path/host_path.go`
/// HostPath `type` validation + "OrCreate" creation, made available so
/// scoped conformance tests can pin the [sig-storage] HostPath type
/// semantics without spinning up a real kubelet.
///
/// Semantics (matches Kubernetes v1.35):
/// - `None` or `Some("")` → accept anything (legacy unchecked behavior).
/// - `Some("Directory")` → path must exist and be a directory.
/// - `Some("DirectoryOrCreate")` → create the directory (and any missing
///   parents) if absent; otherwise it must be a directory.
/// - `Some("File")` → path must exist and be a regular file.
/// - `Some("FileOrCreate")` → create an empty file if absent; otherwise it
///   must be a regular file. Parent directory must already exist.
/// - `Some("Socket")` → path must exist and be a Unix socket.
/// - `Some("CharDevice")` / `Some("BlockDevice")` → must exist and be the
///   matching device kind (treated as `WrongKind` on non-Unix targets).
/// - Anything else → `UnsupportedType`.
pub fn check_host_path_type(path: &str, type_: Option<&str>) -> HostPathCheck {
    match check_host_path_type_msg(path, type_) {
        Ok(()) => HostPathCheck::Ok,
        Err((check, _)) => check,
    }
}

/// Port of `checkTypeInternal` (`pkg/volume/hostpath/host_path.go:463-494`)
/// with `makeDir`/`makeFile` (`:504-526`). Returns upstream's exact error
/// string alongside the classification. Like upstream's
/// `hostutil.GetFileType`/`PathExists` (`hostutil.go:82`,
/// `hostutil_linux.go:152-154`) it uses `stat`, so symlinks are followed.
/// A missing path fails exactly like a wrong-kind one (`IsDir()`/`IsFile()`
/// are false when the path does not exist, `host_path.go:395-401`).
pub fn check_host_path_type_msg(
    path: &str,
    type_: Option<&str>,
) -> Result<(), (HostPathCheck, String)> {
    let kind = match type_ {
        None | Some("") => return Ok(()),
        Some(k) => k,
    };

    let meta_res = std::fs::metadata(path);
    let failed = |check, noun: &str| {
        Err((
            check,
            format!("hostPath type check failed: {path} is not {noun}"),
        ))
    };
    let missing_or_wrong = |meta: &std::io::Result<std::fs::Metadata>| match meta {
        Ok(_) => HostPathCheck::WrongKind,
        Err(_) => HostPathCheck::Missing,
    };

    match kind {
        "DirectoryOrCreate" | "Directory" => {
            if kind == "DirectoryOrCreate" && meta_res.is_err() && !path_exists(path) {
                return make_dir(path).map_err(|e| (HostPathCheck::Missing, e.to_string()));
            }
            match &meta_res {
                Ok(m) if m.is_dir() => Ok(()),
                _ => failed(missing_or_wrong(&meta_res), "a directory"),
            }
        }
        "FileOrCreate" | "File" => {
            if kind == "FileOrCreate" && meta_res.is_err() && !path_exists(path) {
                return make_file(path).map_err(|e| (HostPathCheck::Missing, e.to_string()));
            }
            match &meta_res {
                Ok(m) if m.is_file() => Ok(()),
                _ => failed(missing_or_wrong(&meta_res), "a file"),
            }
        }
        "Socket" => check_unix_special(meta_res, UnixKind::Socket, "a socket file", path),
        "CharDevice" => {
            check_unix_special(meta_res, UnixKind::CharDevice, "a character device", path)
        }
        "BlockDevice" => {
            check_unix_special(meta_res, UnixKind::BlockDevice, "a block device", path)
        }
        other => Err((
            HostPathCheck::UnsupportedType,
            format!("{other} is an invalid volume type"),
        )),
    }
}

/// `PathExists` (`hostutil_linux.go:152-154`): `os.Stat`, not-found => false.
/// (A dangling symlink therefore does not exist, and `makeFile`'s
/// `O_CREATE` open then creates its target, as in Go.)
fn path_exists(path: &str) -> bool {
    !matches!(std::fs::metadata(path), Err(e) if e.kind() == std::io::ErrorKind::NotFound)
}

/// `makeDir` (`host_path.go:504-514`): `os.MkdirAll(pathname, 0755)`.
fn make_dir(path: &str) -> std::io::Result<()> {
    let mut b = std::fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o755);
    }
    b.create(path)
}

/// `makeFile` (`host_path.go:516-526`): `os.OpenFile(O_CREATE, 0644)`.
fn make_file(path: &str) -> std::io::Result<()> {
    let mut o = std::fs::OpenOptions::new();
    // Rust refuses `create` without write/append; no truncate, so an existing
    // file is untouched (Go: bare O_CREATE).
    o.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o644);
    }
    o.open(path).map(|_| ())
}

/// Compute the supplementary group IDs that must be added to a container's
/// GIDs so a non-root `runAsUser` can read volume files we chowned to
/// `:fsGroup` in `create_pod_volumes`.
///
/// Per `PodSecurityContext` semantics: `fsGroup` is appended first (when
/// set), followed by `securityContext.supplementalGroups`. Duplicates are
/// elided to keep the runtime arg list compact. Returns `None` when there
/// are no GIDs to add — Docker's `HostConfig.group_add` treats `None` and
/// an empty list identically, but `None` avoids a wasted allocation.
///
/// This is a pure function and is unit-tested in this module's test block.
/// `pub` so integration tests in the `tests/` crate can assert that the
/// container-arg layer is wired to `securityContext.fsGroup`.
pub fn compute_group_add(pod: &Pod) -> Option<Vec<String>> {
    let pod_sc = pod
        .spec
        .as_ref()
        .and_then(|s| s.security_context.as_ref())?;
    let mut gids: Vec<String> = Vec::new();
    if let Some(fs_group) = pod_sc.fs_group {
        gids.push(fs_group.to_string());
    }
    if let Some(ref supplemental) = pod_sc.supplemental_groups {
        for gid in supplemental {
            let gid_str = gid.to_string();
            if !gids.contains(&gid_str) {
                gids.push(gid_str);
            }
        }
    }
    if gids.is_empty() {
        None
    } else {
        Some(gids)
    }
}

/// Runtime observation of a single init container, derived from the
/// container runtime's inspect call (or test fixtures).
///
/// The `decide_next_init_action` helper consumes a slice of these (one per
/// declared init container, in declaration order) and is the single place
/// that encodes K8s init-container restart semantics: failed inits on a
/// `restartPolicy=Never` pod are terminal (not retried), failed inits on
/// `Always` / `OnFailure` pods retry, sidecar inits (per-container
/// `restartPolicy=Always`, KEP-753) do not gate app-container startup, and
/// init containers run sequentially.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitContainerObserved {
    /// Container does not yet exist (never created/started).
    NotStarted,
    /// Container is currently running.
    Running,
    /// Container has exited with the given exit code.
    Exited(i32),
}

/// Decision the kubelet's reconcile loop should take for a pod's init
/// containers, as a structured value. Mirrors the historical
/// `(all_init_done, next_index, should_retry)` tuple returned from
/// `compute_init_container_actions`, but in a form that's easier to read
/// and test exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitAction {
    /// True iff every non-sidecar init container has completed successfully
    /// (exit code 0). When true the kubelet may start app containers.
    pub all_init_done: bool,
    /// Index (into the pod's `init_containers` slice) of the next init
    /// container to start or retry, if any.
    pub next_index: Option<usize>,
    /// True iff `next_index` refers to a failed init container that should
    /// be retried under the pod's restart policy.
    pub should_retry: bool,
}

/// Decide the next init-container action for `pod` given a snapshot of how
/// each declared init container is currently observed by the runtime.
///
/// `observed[i]` corresponds to `pod.spec.init_containers[i]`. Missing
/// entries (a short slice) are treated as `NotStarted`, matching the prior
/// behaviour for inspect errors and unknown container states.
///
/// Semantics (matches upstream `pkg/kubelet/kuberuntime`):
/// - Sidecar init containers (per-container `restartPolicy=Always`) are
///   skipped from the gating check — they run alongside app containers
///   and must not block them, even while still Running or after they exit.
/// - The first regular init container that isn't successfully terminated
///   determines the action.
/// - A Running regular init container blocks advancement (`next_index =
///   None`) — the kubelet should wait, not start another container.
/// - A non-zero-exit regular init container is a retry candidate when the
///   pod's `restartPolicy` is `Always` or `OnFailure`; with `Never` the
///   pod is terminal (`next_index = None`).
/// - A NotStarted regular init container returns its index without
///   `should_retry` — the kubelet should create and start it.
///
/// This is a pure function: it makes no I/O calls and is the unit covered
/// by `tests/init_container_restart_test.rs`.
pub fn decide_next_init_action(pod: &Pod, observed: &[InitContainerObserved]) -> InitAction {
    let init_containers = match pod.spec.as_ref().and_then(|s| s.init_containers.as_ref()) {
        Some(ics) if !ics.is_empty() => ics,
        _ => {
            return InitAction {
                all_init_done: true,
                next_index: None,
                should_retry: false,
            };
        }
    };

    // K8s rule: init containers are retried on pod restart policies
    // "Always" and "OnFailure"; "Never" is terminal on failure.
    // Default (unset) is "Always".
    let restart_on_failure = pod
        .spec
        .as_ref()
        .and_then(|s| s.restart_policy.as_deref())
        .unwrap_or("Always")
        != "Never";

    for (i, ic) in init_containers.iter().enumerate() {
        // Sidecar init containers (KEP-753) run alongside app containers
        // and do NOT gate app-container startup, so skip them here.
        let is_sidecar = ic.restart_policy.as_deref() == Some("Always");
        if is_sidecar {
            continue;
        }

        let obs = observed
            .get(i)
            .copied()
            .unwrap_or(InitContainerObserved::NotStarted);

        match obs {
            InitContainerObserved::Running => {
                // Current init container is still running — wait for it.
                return InitAction {
                    all_init_done: false,
                    next_index: None,
                    should_retry: false,
                };
            }
            InitContainerObserved::Exited(0) => {
                // Completed successfully — advance to the next init container.
                continue;
            }
            InitContainerObserved::Exited(_) => {
                // Failed with a non-zero exit code.
                return if restart_on_failure {
                    InitAction {
                        all_init_done: false,
                        next_index: Some(i),
                        should_retry: true,
                    }
                } else {
                    // RestartPolicy=Never: pod is terminal. The kubelet
                    // sync loop will mark the pod as Failed.
                    InitAction {
                        all_init_done: false,
                        next_index: None,
                        should_retry: false,
                    }
                };
            }
            InitContainerObserved::NotStarted => {
                // Container needs to be created/started — not a retry.
                return InitAction {
                    all_init_done: false,
                    next_index: Some(i),
                    should_retry: false,
                };
            }
        }
    }

    // Every non-sidecar init container completed successfully.
    InitAction {
        all_init_done: true,
        next_index: None,
        should_retry: false,
    }
}

/// Parse a Kubernetes memory quantity string (e.g. `"128Mi"`, `"0.5Gi"`,
/// `"1000000"`) into bytes. Input that upstream `ParseQuantity` rejects reads
/// as 0; the callers (`downward_api.rs`, `volumes.rs`, `kubelet.rs`) substitute
/// their own default.
///
/// Parsing is [`parse_resource_value`], over the port of
/// `k8s.io/apimachinery/pkg/api/resource/quantity.go`. The `trim_end_matches`
/// chain this replaced parsed the digits with `parse::<i64>()`, so every
/// quantity carrying a decimal point read as 0 — a container exposing
/// `limits.memory: 0.5Gi` through a `resourceFieldRef` was handed `"0"`. It
/// also had no `Ti`/`Pi`/`Ei`/`T`/`P`/`E` (all 0), accepted a non-upstream `K`,
/// and stripped *repeated* suffixes, so `"1GiGi"` parsed as 1Gi.
pub fn parse_memory_quantity(s: &str) -> i64 {
    parse_resource_value(s, "memory").unwrap_or(0)
}

/// Parse a Kubernetes CPU quantity string (e.g. `"500m"`, `"1"`, `"0.5"`) into
/// millicores.
///
/// Millicores for cpu and base units for everything else are what upstream
/// accounts each resource in
/// (`Resource.Add`, `../kubernetes/pkg/scheduler/framework/types.go:917-918`),
/// and both round up away from zero, so a container asking for a sliver of a
/// resource never reports none. The branch this replaced parsed the pre-`m`
/// digits with `parse::<i64>()`, so `"0.5m"` read as 0, and cast an unbounded
/// f64, so `"inf"` saturated to `i64::MAX`.
pub fn parse_cpu_quantity(s: &str) -> i64 {
    parse_resource_value(s, "cpu").unwrap_or(0)
}
