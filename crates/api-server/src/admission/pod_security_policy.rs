//! The Pod Security Admission `policy` package: the versioned check
//! registry and the Pod Security Standards checks.
//!
//! Port of `staging/src/k8s.io/pod-security-admission/policy/`
//! (release-1.35): `checks.go` (`Check`, `VersionedCheck`, `CheckResult`,
//! `AggregateCheckResults`), `registry.go` (`NewEvaluator`, `populate`,
//! `EvaluatePod`), `helpers.go`, `visitor.go` and every `check_*.go`. The
//! tests port `registry_test.go`, `checks_test.go` and the `check_*_test.go`
//! cases.

use super::pod_security_api::{Level, LevelVersion, Version};
use rusternetes_common::resources::pod::{
    ContainerPort, EphemeralContainer, Lifecycle, LifecycleHandler, PodSpec, Probe,
    SecurityContext, Volume,
};
use rusternetes_common::types::ObjectMeta;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// `policy.CheckID` (checks.go:78).
pub type CheckId = &'static str;

/// `policy.CheckResult` (checks.go:80-96).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckResult {
    pub allowed: bool,
    /// Must be set if `allowed` is false; always output.
    pub forbidden_reason: String,
    /// Optional detail naming the disallowed values.
    pub forbidden_detail: String,
}

impl CheckResult {
    fn allowed() -> Self {
        Self {
            allowed: true,
            ..Default::default()
        }
    }

    fn forbidden(reason: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            allowed: false,
            forbidden_reason: reason.into(),
            forbidden_detail: detail.into(),
        }
    }
}

/// `policy.CheckPodFn` (checks.go:73).
pub type CheckPodFn = Arc<dyn Fn(&ObjectMeta, &PodSpec) -> CheckResult + Send + Sync>;

/// `policy.VersionedCheck` (checks.go:50-71).
#[derive(Clone)]
pub struct VersionedCheck {
    /// The first policy version this check applies to; never `latest`.
    pub minimum_version: Version,
    pub check_pod: CheckPodFn,
    /// Checks skipped when this one runs. Only restricted checks may set
    /// overrides, and only of baseline checks.
    pub override_check_ids: Vec<CheckId>,
}

/// `policy.Check` (checks.go:33-48).
#[derive(Clone)]
pub struct Check {
    pub id: CheckId,
    /// Baseline or Restricted.
    pub level: Level,
    /// Strictly increasing `minimum_version`s.
    pub versions: Vec<VersionedCheck>,
}

fn versioned(minor: u32, f: fn(&ObjectMeta, &PodSpec) -> CheckResult) -> VersionedCheck {
    VersionedCheck {
        minimum_version: Version::major_minor(1, minor),
        check_pod: Arc::new(f),
        override_check_ids: Vec::new(),
    }
}

fn overriding(
    minor: u32,
    f: fn(&ObjectMeta, &PodSpec) -> CheckResult,
    overrides: &[CheckId],
) -> VersionedCheck {
    VersionedCheck {
        minimum_version: Version::major_minor(1, minor),
        check_pod: Arc::new(f),
        override_check_ids: overrides.to_vec(),
    }
}

/// `policy.AggregateCheckResult` (checks.go:98-117).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregateCheckResult {
    pub allowed: bool,
    pub forbidden_reasons: Vec<String>,
    pub forbidden_details: Vec<String>,
}

impl AggregateCheckResult {
    /// `ForbiddenReason` (checks.go:119-121).
    #[allow(dead_code)]
    pub fn forbidden_reason(&self) -> String {
        self.forbidden_reasons.join(", ")
    }

    /// `ForbiddenDetail` (checks.go:123-141): `reason (detail), reason, ...`.
    pub fn forbidden_detail(&self) -> String {
        let mut b = String::new();
        for (i, reason) in self.forbidden_reasons.iter().enumerate() {
            b.push_str(reason);
            if !self.forbidden_details[i].is_empty() {
                b.push_str(" (");
                b.push_str(&self.forbidden_details[i]);
                b.push(')');
            }
            if i != self.forbidden_reasons.len() - 1 {
                b.push_str(", ");
            }
        }
        b
    }
}

/// `policy.UnknownForbiddenReason` (checks.go:143).
pub const UNKNOWN_FORBIDDEN_REASON: &str = "unknown forbidden reason";

/// `policy.AggregateCheckResults` (checks.go:145-170).
pub fn aggregate_check_results(results: &[CheckResult]) -> AggregateCheckResult {
    let mut reasons = Vec::new();
    let mut details = Vec::new();
    for result in results.iter().filter(|r| !r.allowed) {
        if result.forbidden_reason.is_empty() {
            reasons.push(UNKNOWN_FORBIDDEN_REASON.to_string());
        } else {
            reasons.push(result.forbidden_reason.clone());
        }
        details.push(result.forbidden_detail.clone());
    }
    AggregateCheckResult {
        allowed: reasons.is_empty(),
        forbidden_reasons: reasons,
        forbidden_details: details,
    }
}

type VersionKey = (u32, u32);

fn key(v: Version) -> VersionKey {
    (v.major, v.minor)
}

/// `policy.checkRegistry` (registry.go:44-52), the `Evaluator`.
pub struct CheckRegistry {
    /// Policy version -> checks registered for that version, in evaluation
    /// order.
    baseline_checks: HashMap<VersionKey, Vec<CheckPodFn>>,
    restricted_checks: HashMap<VersionKey, Vec<CheckPodFn>>,
    /// The maximum cached version, at least the max `minimum_version` of all
    /// registered checks.
    max_version: Version,
}

/// `nextMinor` (registry.go:229-234).
fn next_minor(v: Version) -> Version {
    if v.latest {
        return v;
    }
    Version::major_minor(v.major, v.minor + 1)
}

impl CheckRegistry {
    /// `NewEvaluator` (registry.go:54-79).
    pub fn new(checks: Vec<Check>, emulation_version: Option<Version>) -> Result<Self, String> {
        validate_checks(&checks)?;
        let mut r = CheckRegistry {
            baseline_checks: HashMap::new(),
            restricted_checks: HashMap::new(),
            max_version: Version::major_minor(0, 0),
        };
        populate(&mut r, &checks);
        // Lower the max version if we're emulating an older minor.
        if let Some(emu) = emulation_version {
            if emu.older(&r.max_version) {
                r.max_version = emu;
            }
        }
        Ok(r)
    }

    /// `EvaluatePod` (registry.go:81-104): the results of every check that
    /// applies to the level and version.
    pub fn evaluate_pod(
        &self,
        lv: LevelVersion,
        pod_metadata: &ObjectMeta,
        pod_spec: &PodSpec,
    ) -> Vec<CheckResult> {
        if lv.level == Level::Privileged {
            return Vec::new();
        }
        let mut version = lv.version;
        if self.max_version.older(&version) {
            version = self.max_version;
        }
        let checks = if lv.level == Level::Baseline {
            self.baseline_checks.get(&key(version))
        } else {
            // Includes the non-overridden baseline checks.
            self.restricted_checks.get(&key(version))
        };
        checks
            .into_iter()
            .flatten()
            .map(|check| check(pod_metadata, pod_spec))
            .collect()
    }
}

/// `validateChecks` (registry.go:106-160).
fn validate_checks(checks: &[Check]) -> Result<(), String> {
    let mut ids: HashMap<CheckId, Level> = HashMap::new();
    for check in checks {
        if ids.contains_key(check.id) {
            return Err(format!("multiple checks registered for ID {}", check.id));
        }
        ids.insert(check.id, check.level);
        if check.level != Level::Baseline && check.level != Level::Restricted {
            return Err(format!("check {}: invalid level {}", check.id, check.level));
        }
        if check.versions.is_empty() {
            return Err(format!("check {}: empty", check.id));
        }
        let mut max_version = Version::major_minor(0, 0);
        for c in &check.versions {
            if c.minimum_version == Version::major_minor(0, 0) {
                return Err(format!("check {}: undefined version found", check.id));
            }
            if c.minimum_version.latest {
                return Err(format!("check {}: version cannot be 'latest'", check.id));
            }
            if max_version == c.minimum_version {
                return Err(format!(
                    "check {}: duplicate version {}",
                    check.id, c.minimum_version
                ));
            }
            if !max_version.older(&c.minimum_version) {
                return Err(format!(
                    "check {}: versions must be strictly increasing",
                    check.id
                ));
            }
            max_version = c.minimum_version;
        }
    }
    // Second pass to validate overrides.
    for check in checks {
        for c in &check.versions {
            if c.override_check_ids.is_empty() {
                continue;
            }
            if check.level != Level::Restricted {
                return Err(format!(
                    "check {}: only restricted checks may set overrides",
                    check.id
                ));
            }
            for over in &c.override_check_ids {
                if let Some(level) = ids.get(over) {
                    if *level != Level::Baseline {
                        return Err(format!(
                            "check {}: overrides {} check {}",
                            check.id, level, over
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

type Versioned = HashMap<VersionKey, BTreeMap<CheckId, VersionedCheck>>;

/// `populate` (registry.go:162-216).
fn populate(r: &mut CheckRegistry, valid_checks: &[Check]) {
    // Find the max(MinimumVersion) across all checks.
    for c in valid_checks {
        let last = c.versions[c.versions.len() - 1].minimum_version;
        if r.max_version.older(&last) {
            r.max_version = last;
        }
    }

    let mut restricted_versioned: Versioned = HashMap::new();
    let mut baseline_versioned: Versioned = HashMap::new();
    let mut baseline_ids: Vec<CheckId> = Vec::new();
    let mut restricted_ids: Vec<CheckId> = Vec::new();
    for c in valid_checks {
        if c.level == Level::Restricted {
            restricted_ids.push(c.id);
            inflate_versions(c, &mut restricted_versioned, r.max_version);
        } else {
            baseline_ids.push(c.id);
            inflate_versions(c, &mut baseline_versioned, r.max_version);
        }
    }

    // Sort the IDs to maintain consistent error messages.
    restricted_ids.sort();
    baseline_ids.sort();
    // Baseline checks first, then restricted.
    let ordered_ids: Vec<CheckId> = baseline_ids.into_iter().chain(restricted_ids).collect();

    let end = next_minor(r.max_version);
    let mut v = Version::major_minor(1, 0);
    while v.older(&end) {
        // Aggregate all the overridden baseline check ids.
        let mut overrides: BTreeSet<CheckId> = BTreeSet::new();
        if let Some(restricted) = restricted_versioned.get(&key(v)) {
            for c in restricted.values() {
                overrides.extend(c.override_check_ids.iter().copied());
            }
        }
        // Add the filtered baseline checks to restricted.
        if let Some(baseline) = baseline_versioned.get(&key(v)) {
            for (id, c) in baseline {
                if overrides.contains(id) {
                    continue; // Overridden check: skip it.
                }
                restricted_versioned
                    .entry(key(v))
                    .or_default()
                    .insert(id, c.clone());
            }
        }
        r.restricted_checks.insert(
            key(v),
            map_check_pod_fns(restricted_versioned.get(&key(v)), &ordered_ids),
        );
        r.baseline_checks.insert(
            key(v),
            map_check_pod_fns(baseline_versioned.get(&key(v)), &ordered_ids),
        );
        v = next_minor(v);
    }
}

/// `inflateVersions` (registry.go:218-239): every policy version from the
/// check's minimum to the next check's minimum (or `max_version`++).
fn inflate_versions(check: &Check, versions: &mut Versioned, max_version: Version) {
    for (i, c) in check.versions.iter().enumerate() {
        let next_version = if i + 1 < check.versions.len() {
            check.versions[i + 1].minimum_version
        } else {
            // Assumes only 1 Major version.
            next_minor(max_version)
        };
        let mut v = c.minimum_version;
        while v.older(&next_version) {
            versions
                .entry(key(v))
                .or_default()
                .insert(check.id, check.versions[i].clone());
            v = next_minor(v);
        }
    }
}

/// `mapCheckPodFns` (registry.go:241-251).
fn map_check_pod_fns(
    checks: Option<&BTreeMap<CheckId, VersionedCheck>>,
    ordered_ids: &[CheckId],
) -> Vec<CheckPodFn> {
    let Some(checks) = checks else {
        return Vec::new();
    };
    ordered_ids
        .iter()
        .filter_map(|id| checks.get(id).map(|c| c.check_pod.clone()))
        .collect()
}

// --- helpers.go, visitor.go -------------------------------------------

/// `joinQuote` (helpers.go:25-30).
fn join_quote(items: &[String]) -> String {
    if items.is_empty() {
        return String::new();
    }
    format!("\"{}\"", items.join("\", \""))
}

fn join_quote_set(items: &BTreeSet<String>) -> String {
    join_quote(&items.iter().cloned().collect::<Vec<_>>())
}

/// `pluralize` (helpers.go:32-37).
fn pluralize(singular: &'static str, plural: &'static str, count: usize) -> &'static str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

/// `relaxPolicyForUserNamespacePod` (helpers.go:39-43).
fn relax_policy_for_user_namespace_pod(spec: &PodSpec) -> bool {
    spec.host_users == Some(false)
}

/// One container as `visitContainers` hands it to a visitor. An
/// `EphemeralContainerCommon` carries the same fields as a `Container`.
struct ContainerView<'a> {
    name: &'a str,
    security_context: Option<&'a SecurityContext>,
    ports: &'a [ContainerPort],
    probes: [Option<&'a Probe>; 3],
    lifecycle: Option<&'a Lifecycle>,
}

fn container_view(c: &rusternetes_common::resources::pod::Container) -> ContainerView<'_> {
    ContainerView {
        name: c.name.as_str(),
        security_context: c.security_context.as_ref(),
        ports: c.ports.as_deref().unwrap_or(&[]),
        probes: [
            c.liveness_probe.as_ref(),
            c.readiness_probe.as_ref(),
            c.startup_probe.as_ref(),
        ],
        lifecycle: c.lifecycle.as_ref(),
    }
}

fn ephemeral_view(c: &EphemeralContainer) -> ContainerView<'_> {
    ContainerView {
        name: c.name.as_str(),
        security_context: c.security_context.as_ref(),
        ports: c.ports.as_deref().unwrap_or(&[]),
        probes: [
            c.liveness_probe.as_ref(),
            c.readiness_probe.as_ref(),
            c.startup_probe.as_ref(),
        ],
        lifecycle: c.lifecycle.as_ref(),
    }
}

/// `visitContainers` (visitor.go:30-40): init, regular, then ephemeral.
fn visit_containers(spec: &PodSpec) -> Vec<ContainerView<'_>> {
    spec.init_containers
        .iter()
        .flatten()
        .map(container_view)
        .chain(spec.containers.iter().map(container_view))
        .chain(
            spec.ephemeral_containers
                .iter()
                .flatten()
                .map(ephemeral_view),
        )
        .collect()
}

fn is_windows(spec: &PodSpec) -> bool {
    spec.os.as_ref().is_some_and(|os| os.name == "windows")
}

// --- DefaultChecks (checks.go:172-185) --------------------------------

/// `policy.DefaultChecks`: the checks enabled by default (every `check_*.go`
/// that assigns a version).
pub fn default_checks() -> Vec<Check> {
    vec![
        // check_allowPrivilegeEscalation.go:44-62
        Check {
            id: "allowPrivilegeEscalation",
            level: Level::Restricted,
            versions: vec![
                versioned(8, allow_privilege_escalation_1_8),
                versioned(25, allow_privilege_escalation_1_25),
            ],
        },
        // check_appArmorProfile.go:54-65
        Check {
            id: "appArmorProfile",
            level: Level::Baseline,
            versions: vec![versioned(0, app_armor_profile_1_0)],
        },
        // check_capabilities_baseline.go:48-59
        Check {
            id: CHECK_CAPABILITIES_BASELINE_ID,
            level: Level::Baseline,
            versions: vec![versioned(0, capabilities_baseline_1_0)],
        },
        // check_capabilities_restricted.go:59-77
        Check {
            id: "capabilities_restricted",
            level: Level::Restricted,
            versions: vec![
                overriding(
                    22,
                    capabilities_restricted_1_22,
                    &[CHECK_CAPABILITIES_BASELINE_ID],
                ),
                overriding(
                    25,
                    capabilities_restricted_1_25,
                    &[CHECK_CAPABILITIES_BASELINE_ID],
                ),
            ],
        },
        // check_hostNamespaces.go:120-130
        Check {
            id: "hostNamespaces",
            level: Level::Baseline,
            versions: vec![versioned(0, host_namespaces_1_0)],
        },
        // check_hostPathVolumes.go:202-212
        Check {
            id: CHECK_HOST_PATH_VOLUMES_ID,
            level: Level::Baseline,
            versions: vec![versioned(0, host_path_volumes_1_0)],
        },
        // check_hostPorts.go:280-290
        Check {
            id: "hostPorts",
            level: Level::Baseline,
            versions: vec![versioned(0, host_ports_1_0)],
        },
        // check_hostProbesAndhostLifecycle.go:63-73
        Check {
            id: "hostProbesAndHostLifecycle",
            level: Level::Baseline,
            versions: vec![versioned(34, host_probes_and_host_lifecycle_1_34)],
        },
        // check_privileged.go:43-53
        Check {
            id: "privileged",
            level: Level::Baseline,
            versions: vec![versioned(0, privileged_1_0)],
        },
        // check_procMount_baseline.go:48-62
        Check {
            id: "procMount",
            level: Level::Baseline,
            versions: vec![
                versioned(0, proc_mount_1_0),
                versioned(35, proc_mount_1_35_baseline),
            ],
        },
        // check_procMount_restricted.go:40-56
        Check {
            id: "procMount_restricted",
            level: Level::Restricted,
            versions: vec![overriding(35, proc_mount_1_0, &["procMount"])],
        },
        // check_restrictedVolumes.go:74-86
        Check {
            id: "restrictedVolumes",
            level: Level::Restricted,
            versions: vec![overriding(
                0,
                restricted_volumes_1_0,
                &[CHECK_HOST_PATH_VOLUMES_ID],
            )],
        },
        // check_runAsNonRoot.go:50-65
        Check {
            id: "runAsNonRoot",
            level: Level::Restricted,
            versions: vec![
                versioned(0, run_as_non_root_1_0),
                versioned(35, run_as_non_root_1_35),
            ],
        },
        // check_runAsUser.go:51-66
        Check {
            id: "runAsUser",
            level: Level::Restricted,
            versions: vec![
                versioned(23, run_as_user_1_23),
                versioned(35, run_as_user_1_35),
            ],
        },
        // check_seLinuxOptions.go:60-75
        Check {
            id: "seLinuxOptions",
            level: Level::Baseline,
            versions: vec![
                versioned(0, se_linux_options_1_0),
                versioned(31, se_linux_options_1_31),
            ],
        },
        // check_seccompProfile_baseline.go:58-73
        Check {
            id: CHECK_SECCOMP_BASELINE_ID,
            level: Level::Baseline,
            versions: vec![
                versioned(0, seccomp_profile_baseline_1_0),
                versioned(19, seccomp_profile_baseline_1_19),
            ],
        },
        // check_seccompProfile_restricted.go:48-66
        Check {
            id: "seccompProfile_restricted",
            level: Level::Restricted,
            versions: vec![
                overriding(
                    19,
                    seccomp_profile_restricted_1_19,
                    &[CHECK_SECCOMP_BASELINE_ID],
                ),
                overriding(
                    25,
                    seccomp_profile_restricted_1_25,
                    &[CHECK_SECCOMP_BASELINE_ID],
                ),
            ],
        },
        // check_sysctls.go:61-83
        Check {
            id: "sysctls",
            level: Level::Baseline,
            versions: vec![
                versioned(0, sysctls_1_0),
                versioned(27, sysctls_1_27),
                versioned(29, sysctls_1_29),
                versioned(32, sysctls_1_32),
            ],
        },
        // check_windowsHostProcess.go:46-57
        Check {
            id: "windowsHostProcess",
            level: Level::Baseline,
            versions: vec![versioned(0, windows_host_process_1_0)],
        },
    ]
}

const CHECK_CAPABILITIES_BASELINE_ID: CheckId = "capabilities_baseline";
const CHECK_HOST_PATH_VOLUMES_ID: CheckId = "hostPathVolumes";
const CHECK_SECCOMP_BASELINE_ID: CheckId = "seccompProfile_baseline";

// --- check_privileged.go ----------------------------------------------

fn privileged_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let bad: Vec<String> = visit_containers(spec)
        .iter()
        .filter(|c| {
            c.security_context
                .is_some_and(|sc| sc.privileged == Some(true))
        })
        .map(|c| c.name.to_string())
        .collect();
    if bad.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "privileged",
        format!(
            "{} {} must not set securityContext.privileged=true",
            pluralize("container", "containers", bad.len()),
            join_quote(&bad)
        ),
    )
}

// --- check_hostNamespaces.go ------------------------------------------

fn host_namespaces_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut host = Vec::new();
    if spec.host_network == Some(true) {
        host.push("hostNetwork=true");
    }
    if spec.host_pid == Some(true) {
        host.push("hostPID=true");
    }
    if spec.host_ipc == Some(true) {
        host.push("hostIPC=true");
    }
    if host.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden("host namespaces", host.join(", "))
}

// --- check_hostPathVolumes.go -----------------------------------------

fn host_path_volumes_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let bad: Vec<String> = spec
        .volumes
        .iter()
        .flatten()
        .filter(|v| v.host_path.is_some())
        .map(|v| v.name.clone())
        .collect();
    if bad.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "hostPath volumes",
        format!(
            "{} {}",
            pluralize("volume", "volumes", bad.len()),
            join_quote(&bad)
        ),
    )
}

// --- check_hostPorts.go -----------------------------------------------

fn host_ports_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_containers = Vec::new();
    // `sets.NewString().List()` is sorted lexically, not numerically.
    let mut forbidden: BTreeSet<String> = BTreeSet::new();
    for c in visit_containers(spec) {
        let mut valid = true;
        for p in c.ports {
            if p.host_port.is_some_and(|hp| hp != 0) {
                valid = false;
                forbidden.insert(p.host_port.unwrap_or_default().to_string());
            }
        }
        if !valid {
            bad_containers.push(c.name.to_string());
        }
    }
    if bad_containers.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "hostPort",
        format!(
            "{} {} {} {} {}",
            pluralize("container", "containers", bad_containers.len()),
            join_quote(&bad_containers),
            pluralize("uses", "use", bad_containers.len()),
            pluralize("hostPort", "hostPorts", forbidden.len()),
            forbidden.iter().cloned().collect::<Vec<_>>().join(", ")
        ),
    )
}

// --- check_hostProbesAndhostLifecycle.go ------------------------------

fn forbidden_host_probe(probe: Option<&Probe>) -> Vec<String> {
    let mut bad = Vec::new();
    let Some(probe) = probe else { return bad };
    if let Some(host) = probe
        .http_get
        .as_ref()
        .and_then(|h| h.host.as_deref())
        .filter(|h| !h.is_empty())
    {
        bad.push(host.to_string());
    }
    if let Some(host) = probe
        .tcp_socket
        .as_ref()
        .and_then(|h| h.host.as_deref())
        .filter(|h| !h.is_empty())
    {
        bad.push(host.to_string());
    }
    bad
}

fn forbidden_host_lifecycle(handler: Option<&LifecycleHandler>) -> Vec<String> {
    let mut bad = Vec::new();
    let Some(handler) = handler else { return bad };
    if let Some(host) = handler
        .http_get
        .as_ref()
        .and_then(|h| h.host.as_deref())
        .filter(|h| !h.is_empty())
    {
        bad.push(host.to_string());
    }
    if let Some(host) = handler
        .tcp_socket
        .as_ref()
        .and_then(|h| h.host.as_deref())
        .filter(|h| !h.is_empty())
    {
        bad.push(host.to_string());
    }
    bad
}

fn host_probes_and_host_lifecycle_1_34(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_containers: BTreeSet<String> = BTreeSet::new();
    let mut forbidden: BTreeSet<String> = BTreeSet::new();
    for c in visit_containers(spec) {
        let mut hosts: Vec<String> = c
            .probes
            .iter()
            .flat_map(|p| forbidden_host_probe(*p))
            .collect();
        if let Some(l) = c.lifecycle {
            hosts.extend(forbidden_host_lifecycle(l.post_start.as_ref()));
            hosts.extend(forbidden_host_lifecycle(l.pre_stop.as_ref()));
        }
        if !hosts.is_empty() {
            bad_containers.insert(c.name.to_string());
            forbidden.extend(hosts);
        }
    }
    if bad_containers.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "probe or lifecycle host",
        format!(
            "{} {} {} {} {}",
            pluralize("container", "containers", bad_containers.len()),
            join_quote_set(&bad_containers),
            pluralize("uses", "use", bad_containers.len()),
            pluralize(
                "probe or lifecycle host",
                "probe or lifecycle hosts",
                forbidden.len()
            ),
            join_quote_set(&forbidden)
        ),
    )
}

// --- check_capabilities_*.go ------------------------------------------

const CAPABILITIES_ALLOWED_1_0: [&str; 13] = [
    "AUDIT_WRITE",
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "KILL",
    "MKNOD",
    "NET_BIND_SERVICE",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYS_CHROOT",
];

fn capabilities_baseline_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_containers = Vec::new();
    let mut non_default: BTreeSet<String> = BTreeSet::new();
    for c in visit_containers(spec) {
        if let Some(caps) = c.security_context.and_then(|sc| sc.capabilities.as_ref()) {
            let mut valid = true;
            for cap in caps.add.iter().flatten() {
                if !CAPABILITIES_ALLOWED_1_0.contains(&cap.as_str()) {
                    valid = false;
                    non_default.insert(cap.clone());
                }
            }
            if !valid {
                bad_containers.push(c.name.to_string());
            }
        }
    }
    if bad_containers.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "non-default capabilities",
        format!(
            "{} {} must not include {} in securityContext.capabilities.add",
            pluralize("container", "containers", bad_containers.len()),
            join_quote(&bad_containers),
            join_quote_set(&non_default)
        ),
    )
}

fn capabilities_restricted_1_22(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut missing_drop_all = Vec::new();
    let mut adding_forbidden = Vec::new();
    let mut forbidden_caps: BTreeSet<String> = BTreeSet::new();
    for c in visit_containers(spec) {
        let Some(caps) = c.security_context.and_then(|sc| sc.capabilities.as_ref()) else {
            missing_drop_all.push(c.name.to_string());
            continue;
        };
        if !caps.drop.iter().flatten().any(|d| d == "ALL") {
            missing_drop_all.push(c.name.to_string());
        }
        let mut added_forbidden = false;
        for cap in caps.add.iter().flatten() {
            if cap != "NET_BIND_SERVICE" {
                added_forbidden = true;
                forbidden_caps.insert(cap.clone());
            }
        }
        if added_forbidden {
            adding_forbidden.push(c.name.to_string());
        }
    }
    let mut details = Vec::new();
    if !missing_drop_all.is_empty() {
        details.push(format!(
            "{} {} must set securityContext.capabilities.drop=[\"ALL\"]",
            pluralize("container", "containers", missing_drop_all.len()),
            join_quote(&missing_drop_all)
        ));
    }
    if !adding_forbidden.is_empty() {
        details.push(format!(
            "{} {} must not include {} in securityContext.capabilities.add",
            pluralize("container", "containers", adding_forbidden.len()),
            join_quote(&adding_forbidden),
            join_quote_set(&forbidden_caps)
        ));
    }
    if details.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden("unrestricted capabilities", details.join("; "))
}

fn capabilities_restricted_1_25(m: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    // Pod API validation would have failed if podOS == Windows and if
    // capabilities have been set.
    if is_windows(spec) {
        return CheckResult::allowed();
    }
    capabilities_restricted_1_22(m, spec)
}

// --- check_allowPrivilegeEscalation.go --------------------------------

fn allow_privilege_escalation_1_8(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let bad: Vec<String> = visit_containers(spec)
        .iter()
        .filter(|c| {
            c.security_context
                .and_then(|sc| sc.allow_privilege_escalation)
                != Some(false)
        })
        .map(|c| c.name.to_string())
        .collect();
    if bad.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "allowPrivilegeEscalation != false",
        format!(
            "{} {} must set securityContext.allowPrivilegeEscalation=false",
            pluralize("container", "containers", bad.len()),
            join_quote(&bad)
        ),
    )
}

fn allow_privilege_escalation_1_25(m: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    if is_windows(spec) {
        return CheckResult::allowed();
    }
    allow_privilege_escalation_1_8(m, spec)
}

// --- check_procMount_*.go ---------------------------------------------

fn proc_mount_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_containers = Vec::new();
    let mut forbidden: BTreeSet<String> = BTreeSet::new();
    for c in visit_containers(spec) {
        let Some(pm) = c.security_context.and_then(|sc| sc.proc_mount.as_ref()) else {
            continue;
        };
        if pm != "Default" {
            bad_containers.push(c.name.to_string());
            forbidden.insert(pm.clone());
        }
    }
    if bad_containers.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "procMount",
        format!(
            "{} {} must not set securityContext.procMount to {}",
            pluralize("container", "containers", bad_containers.len()),
            join_quote(&bad_containers),
            join_quote_set(&forbidden)
        ),
    )
}

/// Blocks unmasked procMount for pods that are not in a user namespace.
fn proc_mount_1_35_baseline(m: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    if relax_policy_for_user_namespace_pod(spec) {
        return CheckResult::allowed();
    }
    proc_mount_1_0(m, spec)
}

// --- check_restrictedVolumes.go ---------------------------------------

fn volume_is_allowed(v: &Volume) -> bool {
    v.config_map.is_some()
        || v.csi.is_some()
        || v.downward_api.is_some()
        || v.empty_dir.is_some()
        || v.ephemeral.is_some()
        || v.image.is_some()
        || v.persistent_volume_claim.is_some()
        || v.projected.is_some()
        || v.secret.is_some()
}

fn restricted_volumes_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_volumes = Vec::new();
    let mut bad_types: BTreeSet<String> = BTreeSet::new();
    for v in spec.volumes.iter().flatten() {
        if volume_is_allowed(v) {
            continue;
        }
        bad_volumes.push(v.name.clone());
        let l = &v.legacy_sources;
        // The switch order of check_restrictedVolumes.go:108-150.
        let ty = [
            (v.host_path.is_some(), "hostPath"),
            (l.gce_persistent_disk.is_some(), "gcePersistentDisk"),
            (l.aws_elastic_block_store.is_some(), "awsElasticBlockStore"),
            (l.git_repo.is_some(), "gitRepo"),
            (v.nfs.is_some(), "nfs"),
            (v.iscsi.is_some(), "iscsi"),
            (l.glusterfs.is_some(), "glusterfs"),
            (l.rbd.is_some(), "rbd"),
            (l.flex_volume.is_some(), "flexVolume"),
            (l.cinder.is_some(), "cinder"),
            (l.cephfs.is_some(), "cephfs"),
            (l.flocker.is_some(), "flocker"),
            (l.fc.is_some(), "fc"),
            (l.azure_file.is_some(), "azureFile"),
            (l.vsphere_volume.is_some(), "vsphereVolume"),
            (l.quobyte.is_some(), "quobyte"),
            (l.azure_disk.is_some(), "azureDisk"),
            (l.photon_persistent_disk.is_some(), "photonPersistentDisk"),
            (l.portworx_volume.is_some(), "portworxVolume"),
            (l.scale_io.is_some(), "scaleIO"),
            (l.storageos.is_some(), "storageos"),
        ]
        .into_iter()
        .find(|(set, _)| *set)
        .map_or("unknown", |(_, name)| name);
        bad_types.insert(ty.to_string());
    }
    if bad_volumes.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "restricted volume types",
        format!(
            "{} {} {} {} {}",
            pluralize("volume", "volumes", bad_volumes.len()),
            join_quote(&bad_volumes),
            pluralize("uses", "use", bad_volumes.len()),
            pluralize(
                "restricted volume type",
                "restricted volume types",
                bad_types.len()
            ),
            join_quote_set(&bad_types)
        ),
    )
}

// --- check_runAsNonRoot.go / check_runAsUser.go -----------------------

fn run_as_non_root_1_35(m: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    // KEP-127: a pod in a user namespace is relaxed unconditionally here.
    if relax_policy_for_user_namespace_pod(spec) {
        return CheckResult::allowed();
    }
    run_as_non_root_1_0(m, spec)
}

fn run_as_non_root_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    // Things that explicitly set runAsNonRoot=false.
    let mut bad_setters: Vec<String> = Vec::new();
    let mut pod_run_as_non_root = false;
    if let Some(v) = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.run_as_non_root)
    {
        if !v {
            bad_setters.push("pod".to_string());
        } else {
            pod_run_as_non_root = true;
        }
    }
    let mut explicitly_bad = Vec::new();
    let mut implicitly_bad = Vec::new();
    for c in visit_containers(spec) {
        match c.security_context.and_then(|sc| sc.run_as_non_root) {
            Some(false) => explicitly_bad.push(c.name.to_string()),
            Some(true) => {}
            None => {
                if !pod_run_as_non_root {
                    implicitly_bad.push(c.name.to_string());
                }
            }
        }
    }
    if !explicitly_bad.is_empty() {
        bad_setters.push(format!(
            "{} {}",
            pluralize("container", "containers", explicitly_bad.len()),
            join_quote(&explicitly_bad)
        ));
    }
    if !bad_setters.is_empty() {
        return CheckResult::forbidden(
            "runAsNonRoot != true",
            format!(
                "{} must not set securityContext.runAsNonRoot=false",
                bad_setters.join(" and ")
            ),
        );
    }
    if !implicitly_bad.is_empty() {
        return CheckResult::forbidden(
            "runAsNonRoot != true",
            format!(
                "pod or {} {} must set securityContext.runAsNonRoot=true",
                pluralize("container", "containers", implicitly_bad.len()),
                join_quote(&implicitly_bad)
            ),
        );
    }
    CheckResult::allowed()
}

fn run_as_user_1_35(m: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    if relax_policy_for_user_namespace_pod(spec) {
        return CheckResult::allowed();
    }
    run_as_user_1_23(m, spec)
}

fn run_as_user_1_23(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_setters: Vec<String> = Vec::new();
    if spec.security_context.as_ref().and_then(|sc| sc.run_as_user) == Some(0) {
        bad_setters.push("pod".to_string());
    }
    let bad: Vec<String> = visit_containers(spec)
        .iter()
        .filter(|c| c.security_context.and_then(|sc| sc.run_as_user) == Some(0))
        .map(|c| c.name.to_string())
        .collect();
    if !bad.is_empty() {
        bad_setters.push(format!(
            "{} {}",
            pluralize("container", "containers", bad.len()),
            join_quote(&bad)
        ));
    }
    if bad_setters.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "runAsUser=0",
        format!("{} must not set runAsUser=0", bad_setters.join(" and ")),
    )
}

// --- check_seLinuxOptions.go ------------------------------------------

const SELINUX_ALLOWED_TYPES_1_0: [&str; 4] =
    ["", "container_t", "container_init_t", "container_kvm_t"];
const SELINUX_ALLOWED_TYPES_1_31: [&str; 5] = [
    "",
    "container_t",
    "container_init_t",
    "container_kvm_t",
    "container_engine_t",
];

fn se_linux_options_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    se_linux_options(spec, &SELINUX_ALLOWED_TYPES_1_0)
}

fn se_linux_options_1_31(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    se_linux_options(spec, &SELINUX_ALLOWED_TYPES_1_31)
}

fn se_linux_options(spec: &PodSpec, allowed_types: &[&str]) -> CheckResult {
    use rusternetes_common::resources::pod::SELinuxOptions;
    let mut bad_setters: Vec<String> = Vec::new();
    let mut bad_types: BTreeSet<String> = BTreeSet::new();
    let mut set_user = false;
    let mut set_role = false;

    let mut valid = |opts: &SELinuxOptions| {
        let mut valid = true;
        let ty = opts.type_.as_deref().unwrap_or("");
        if !allowed_types.contains(&ty) {
            valid = false;
            bad_types.insert(ty.to_string());
        }
        if opts.user.as_deref().is_some_and(|u| !u.is_empty()) {
            valid = false;
            set_user = true;
        }
        if opts.role.as_deref().is_some_and(|r| !r.is_empty()) {
            valid = false;
            set_role = true;
        }
        valid
    };

    if let Some(opts) = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.se_linux_options.as_ref())
    {
        if !valid(opts) {
            bad_setters.push("pod".to_string());
        }
    }
    let mut bad_containers = Vec::new();
    for c in visit_containers(spec) {
        if let Some(opts) = c
            .security_context
            .and_then(|sc| sc.se_linux_options.as_ref())
        {
            if !valid(opts) {
                bad_containers.push(c.name.to_string());
            }
        }
    }
    if !bad_containers.is_empty() {
        bad_setters.push(format!(
            "{} {}",
            pluralize("container", "containers", bad_containers.len()),
            join_quote(&bad_containers)
        ));
    }
    if bad_setters.is_empty() {
        return CheckResult::allowed();
    }
    let mut bad_data = Vec::new();
    if !bad_types.is_empty() {
        bad_data.push(format!(
            "{} {}",
            pluralize("type", "types", bad_types.len()),
            join_quote_set(&bad_types)
        ));
    }
    if set_user {
        bad_data.push("user may not be set".to_string());
    }
    if set_role {
        bad_data.push("role may not be set".to_string());
    }
    CheckResult::forbidden(
        "seLinuxOptions",
        format!(
            "{} set forbidden securityContext.seLinuxOptions: {}",
            bad_setters.join(" and "),
            bad_data.join("; ")
        ),
    )
}

// --- check_seccompProfile_*.go ----------------------------------------

const ANNOTATION_KEY_POD: &str = "seccomp.security.alpha.kubernetes.io/pod";
const ANNOTATION_KEY_CONTAINER_PREFIX: &str = "container.seccomp.security.alpha.kubernetes.io/";

fn valid_seccomp(t: &str) -> bool {
    t == "Localhost" || t == "RuntimeDefault"
}

/// `validSeccompAnnotationValue`: `runtime/default`, `docker/default`
/// (core/v1 `SeccompProfileRuntimeDefault`, `DeprecatedSeccompProfileDockerDefault`)
/// or the `localhost/` prefix (`SeccompLocalhostProfileNamePrefix`).
fn valid_seccomp_annotation_value(v: &str) -> bool {
    v == "runtime/default" || v == "docker/default" || v.starts_with("localhost/")
}

/// Rust's `{:?}` for a `str` is Go's `%q` for the values seen here (no
/// control characters), including the double quotes.
fn quoted(s: &str) -> String {
    format!("{s:?}")
}

fn annotation<'a>(meta: &'a ObjectMeta, key: &str) -> Option<&'a String> {
    meta.annotations.as_ref().and_then(|a| a.get(key))
}

/// Checks baseline policy on the seccomp alpha annotation.
fn seccomp_profile_baseline_1_0(meta: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut forbidden: BTreeSet<String> = BTreeSet::new();
    if let Some(val) = annotation(meta, ANNOTATION_KEY_POD) {
        if !valid_seccomp_annotation_value(val) {
            forbidden.insert(format!("{ANNOTATION_KEY_POD}={}", quoted(val)));
        }
    }
    for c in visit_containers(spec) {
        let key = format!("{ANNOTATION_KEY_CONTAINER_PREFIX}{}", c.name);
        if let Some(val) = annotation(meta, &key) {
            if !valid_seccomp_annotation_value(val) {
                forbidden.insert(format!("{key}={}", quoted(val)));
            }
        }
    }
    if forbidden.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "seccompProfile",
        format!(
            "forbidden {} {}",
            pluralize("annotation", "annotations", forbidden.len()),
            forbidden.iter().cloned().collect::<Vec<_>>().join(", ")
        ),
    )
}

/// Checks baseline policy on the securityContext.seccompProfile field.
fn seccomp_profile_baseline_1_19(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_setters: Vec<String> = Vec::new();
    let mut bad_values: BTreeSet<String> = BTreeSet::new();
    if let Some(p) = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.seccomp_profile.as_ref())
    {
        if !valid_seccomp(&p.r#type) {
            bad_setters.push("pod".to_string());
            bad_values.insert(p.r#type.clone());
        }
    }
    let mut explicitly_bad = Vec::new();
    for c in visit_containers(spec) {
        if let Some(p) = c
            .security_context
            .and_then(|sc| sc.seccomp_profile.as_ref())
        {
            if !valid_seccomp(&p.r#type) {
                explicitly_bad.push(c.name.to_string());
                bad_values.insert(p.r#type.clone());
            }
        }
    }
    if !explicitly_bad.is_empty() {
        bad_setters.push(format!(
            "{} {}",
            pluralize("container", "containers", explicitly_bad.len()),
            join_quote(&explicitly_bad)
        ));
    }
    if bad_setters.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "seccompProfile",
        format!(
            "{} must not set securityContext.seccompProfile.type to {}",
            bad_setters.join(" and "),
            join_quote_set(&bad_values)
        ),
    )
}

fn seccomp_profile_restricted_1_19(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_setters: Vec<String> = Vec::new();
    let mut bad_values: BTreeSet<String> = BTreeSet::new();
    let mut pod_seccomp_set = false;
    if let Some(p) = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.seccomp_profile.as_ref())
    {
        if !valid_seccomp(&p.r#type) {
            bad_setters.push("pod".to_string());
            bad_values.insert(p.r#type.clone());
        } else {
            pod_seccomp_set = true;
        }
    }
    let mut explicitly_bad = Vec::new();
    let mut implicitly_bad = Vec::new();
    for c in visit_containers(spec) {
        match c
            .security_context
            .and_then(|sc| sc.seccomp_profile.as_ref())
        {
            Some(p) => {
                if !valid_seccomp(&p.r#type) {
                    explicitly_bad.push(c.name.to_string());
                    bad_values.insert(p.r#type.clone());
                }
            }
            None => {
                if !pod_seccomp_set {
                    implicitly_bad.push(c.name.to_string());
                }
            }
        }
    }
    if !explicitly_bad.is_empty() {
        bad_setters.push(format!(
            "{} {}",
            pluralize("container", "containers", explicitly_bad.len()),
            join_quote(&explicitly_bad)
        ));
    }
    if !bad_setters.is_empty() {
        return CheckResult::forbidden(
            "seccompProfile",
            format!(
                "{} must not set securityContext.seccompProfile.type to {}",
                bad_setters.join(" and "),
                join_quote_set(&bad_values)
            ),
        );
    }
    if !implicitly_bad.is_empty() {
        return CheckResult::forbidden(
            "seccompProfile",
            format!(
                "pod or {} {} must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\"",
                pluralize("container", "containers", implicitly_bad.len()),
                join_quote(&implicitly_bad)
            ),
        );
    }
    CheckResult::allowed()
}

fn seccomp_profile_restricted_1_25(m: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    if is_windows(spec) {
        return CheckResult::allowed();
    }
    seccomp_profile_restricted_1_19(m, spec)
}

// --- check_appArmorProfile.go -----------------------------------------

const APPARMOR_BETA_CONTAINER_ANNOTATION_KEY_PREFIX: &str =
    "container.apparmor.security.beta.kubernetes.io/";

fn allowed_annotation_value(profile: &str) -> bool {
    profile.is_empty() || profile == "runtime/default" || profile.starts_with("localhost/")
}

fn allowed_profile_type(profile: &str) -> bool {
    profile == "RuntimeDefault" || profile == "Localhost"
}

fn app_armor_profile_1_0(meta: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let mut bad_setters: Vec<String> = Vec::new();
    let mut bad_values: BTreeSet<String> = BTreeSet::new();
    if let Some(p) = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.app_armor_profile.as_ref())
    {
        if !allowed_profile_type(&p.type_) {
            bad_setters.push("pod".to_string());
            bad_values.insert(p.type_.clone());
        }
    }
    let mut bad_containers = Vec::new();
    for c in visit_containers(spec) {
        if let Some(p) = c
            .security_context
            .and_then(|sc| sc.app_armor_profile.as_ref())
        {
            if !allowed_profile_type(&p.type_) {
                bad_containers.push(c.name.to_string());
                bad_values.insert(p.type_.clone());
            }
        }
    }
    if !bad_containers.is_empty() {
        bad_setters.push(format!(
            "{} {}",
            pluralize("container", "containers", bad_containers.len()),
            join_quote(&bad_containers)
        ));
    }
    let mut forbidden_annotations: Vec<String> = meta
        .annotations
        .iter()
        .flatten()
        .filter(|(k, v)| {
            k.starts_with(APPARMOR_BETA_CONTAINER_ANNOTATION_KEY_PREFIX)
                && !allowed_annotation_value(v)
        })
        .map(|(k, v)| format!("{k}={}", quoted(v)))
        .collect();
    let mut bad_value_list: Vec<String> = bad_values.into_iter().collect();
    if !forbidden_annotations.is_empty() {
        forbidden_annotations.sort();
        let n = forbidden_annotations.len();
        bad_value_list.extend(forbidden_annotations);
        bad_setters.push(pluralize("annotation", "annotations", n).to_string());
    }
    if bad_setters.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        pluralize(
            "forbidden AppArmor profile",
            "forbidden AppArmor profiles",
            bad_value_list.len(),
        ),
        format!(
            "{} must not set AppArmor profile type to {}",
            bad_setters.join(" and "),
            join_quote(&bad_value_list)
        ),
    )
}

// --- check_sysctls.go -------------------------------------------------

const SYSCTLS_ALLOWED_1_0: [&str; 5] = [
    "kernel.shm_rmid_forced",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.tcp_syncookies",
    "net.ipv4.ping_group_range",
    "net.ipv4.ip_unprivileged_port_start",
];
const SYSCTLS_ADDED_1_27: [&str; 1] = ["net.ipv4.ip_local_reserved_ports"];
const SYSCTLS_ADDED_1_29: [&str; 4] = [
    "net.ipv4.tcp_keepalive_time",
    "net.ipv4.tcp_fin_timeout",
    "net.ipv4.tcp_keepalive_intvl",
    "net.ipv4.tcp_keepalive_probes",
];
const SYSCTLS_ADDED_1_32: [&str; 2] = ["net.ipv4.tcp_rmem", "net.ipv4.tcp_wmem"];

fn sysctls_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    sysctls(spec, &[&SYSCTLS_ALLOWED_1_0])
}
fn sysctls_1_27(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    sysctls(spec, &[&SYSCTLS_ALLOWED_1_0, &SYSCTLS_ADDED_1_27])
}
fn sysctls_1_29(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    sysctls(
        spec,
        &[
            &SYSCTLS_ALLOWED_1_0,
            &SYSCTLS_ADDED_1_27,
            &SYSCTLS_ADDED_1_29,
        ],
    )
}
fn sysctls_1_32(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    sysctls(
        spec,
        &[
            &SYSCTLS_ALLOWED_1_0,
            &SYSCTLS_ADDED_1_27,
            &SYSCTLS_ADDED_1_29,
            &SYSCTLS_ADDED_1_32,
        ],
    )
}

fn sysctls(spec: &PodSpec, allowed_sets: &[&[&str]]) -> CheckResult {
    let forbidden: Vec<String> = spec
        .security_context
        .iter()
        .flat_map(|sc| sc.sysctls.iter().flatten())
        .filter(|s| {
            !allowed_sets
                .iter()
                .any(|set| set.contains(&s.name.as_str()))
        })
        .map(|s| s.name.clone())
        .collect();
    if forbidden.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden("forbidden sysctls", forbidden.join(", "))
}

// --- check_windowsHostProcess.go --------------------------------------

fn windows_host_process_1_0(_: &ObjectMeta, spec: &PodSpec) -> CheckResult {
    let bad: Vec<String> = visit_containers(spec)
        .iter()
        .filter(|c| {
            c.security_context
                .and_then(|sc| sc.windows_options.as_ref())
                .and_then(|w| w.host_process)
                == Some(true)
        })
        .map(|c| c.name.to_string())
        .collect();
    let pod_forbidden = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.windows_options.as_ref())
        .and_then(|w| w.host_process)
        == Some(true);
    let mut setters: Vec<String> = Vec::new();
    if pod_forbidden {
        setters.push("pod".to_string());
    }
    if !bad.is_empty() {
        setters.push(format!(
            "{} {}",
            pluralize("container", "containers", bad.len()),
            join_quote(&bad)
        ));
    }
    if setters.is_empty() {
        return CheckResult::allowed();
    }
    CheckResult::forbidden(
        "hostProcess",
        format!(
            "{} must not set securityContext.windowsOptions.hostProcess=true",
            setters.join(" and ")
        ),
    )
}

#[cfg(test)]
#[path = "pod_security_policy_cases.rs"]
mod cases;

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::Pod;

    fn lv(level: Level, v: &str) -> LevelVersion {
        let (version, err) = super::super::pod_security_api::parse_version(v);
        assert!(err.is_ok(), "{v}");
        LevelVersion::new(level, version)
    }

    /// registry_test.go `generateCheck`: a check whose result is always
    /// forbidden with reason `<id>:<minimum version>`.
    fn generate_check(id: CheckId, level: Level, versions: &[&str]) -> Check {
        Check {
            id,
            level,
            versions: versions
                .iter()
                .map(|v| {
                    let ver = super::super::pod_security_api::parse_version(v).0;
                    let reason = format!("{id}:{v}");
                    VersionedCheck {
                        minimum_version: ver,
                        check_pod: Arc::new(move |_, _| CheckResult {
                            allowed: false,
                            forbidden_reason: reason.clone(),
                            forbidden_detail: String::new(),
                        }),
                        override_check_ids: Vec::new(),
                    }
                })
                .collect(),
        }
    }

    fn with_overrides(mut c: Check, overrides: &[CheckId]) -> Check {
        for v in &mut c.versions {
            v.override_check_ids = overrides.to_vec();
        }
        c
    }

    fn run_case(reg: &CheckRegistry, level: Level, version: &str, want: &[&str]) {
        let results = reg.evaluate_pod(
            lv(level, version),
            &ObjectMeta::default(),
            &PodSpec::default(),
        );
        let got: Vec<&str> = results
            .iter()
            .map(|r| r.forbidden_reason.as_str())
            .collect();
        assert_eq!(got, want, "{level}:{version}");
    }

    /// registry_test.go `TestCheckRegistry`.
    #[test]
    fn check_registry() {
        let mut checks = vec![
            generate_check("a", Level::Baseline, &["v1.0"]),
            generate_check("b", Level::Baseline, &["v1.10"]),
            generate_check("c", Level::Baseline, &["v1.0", "v1.5", "v1.10"]),
            generate_check("d", Level::Baseline, &["v1.11", "v1.15", "v1.20"]),
            generate_check("e", Level::Restricted, &["v1.0"]),
            generate_check("f", Level::Restricted, &["v1.12", "v1.16", "v1.21"]),
            with_overrides(generate_check("g", Level::Restricted, &["v1.10"]), &["a"]),
            with_overrides(generate_check("h", Level::Restricted, &["v1.0"]), &["b"]),
        ];
        let mut multi = generate_check("i", Level::Restricted, &["v1.10", "v1.21"]);
        multi.versions[0].override_check_ids = vec!["c"];
        multi.versions[1].override_check_ids = vec!["d"];
        checks.push(multi);
        let reg = CheckRegistry::new(checks, None).unwrap();

        let cases: &[(Level, &str, &[&str])] = &[
            (Level::Privileged, "v1.0", &[]),
            (Level::Privileged, "latest", &[]),
            (Level::Baseline, "v1.0", &["a:v1.0", "c:v1.0"]),
            (Level::Baseline, "v1.4", &["a:v1.0", "c:v1.0"]),
            (Level::Baseline, "v1.5", &["a:v1.0", "c:v1.5"]),
            (Level::Baseline, "v1.10", &["a:v1.0", "b:v1.10", "c:v1.10"]),
            (
                Level::Baseline,
                "v1.11",
                &["a:v1.0", "b:v1.10", "c:v1.10", "d:v1.11"],
            ),
            (
                Level::Baseline,
                "latest",
                &["a:v1.0", "b:v1.10", "c:v1.10", "d:v1.20"],
            ),
            (
                Level::Restricted,
                "v1.0",
                &["a:v1.0", "c:v1.0", "e:v1.0", "h:v1.0"],
            ),
            (
                Level::Restricted,
                "v1.4",
                &["a:v1.0", "c:v1.0", "e:v1.0", "h:v1.0"],
            ),
            (
                Level::Restricted,
                "v1.5",
                &["a:v1.0", "c:v1.5", "e:v1.0", "h:v1.0"],
            ),
            (
                Level::Restricted,
                "v1.10",
                &["e:v1.0", "g:v1.10", "h:v1.0", "i:v1.10"],
            ),
            (
                Level::Restricted,
                "v1.11",
                &["d:v1.11", "e:v1.0", "g:v1.10", "h:v1.0", "i:v1.10"],
            ),
            (
                Level::Restricted,
                "latest",
                &[
                    "c:v1.10", "e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0", "i:v1.21",
                ],
            ),
            (
                Level::Restricted,
                "v1.10000",
                &[
                    "c:v1.10", "e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0", "i:v1.21",
                ],
            ),
        ];
        for (level, version, want) in cases {
            run_case(&reg, *level, version, want);
        }
    }

    /// registry_test.go `TestCheckRegistry_NoBaseline`.
    #[test]
    fn check_registry_no_baseline() {
        let checks = vec![
            generate_check("e", Level::Restricted, &["v1.0"]),
            generate_check("f", Level::Restricted, &["v1.12", "v1.16", "v1.21"]),
            with_overrides(generate_check("g", Level::Restricted, &["v1.10"]), &["a"]),
            with_overrides(generate_check("h", Level::Restricted, &["v1.0"]), &["b"]),
        ];
        let reg = CheckRegistry::new(checks, None).unwrap();
        let cases: &[(Level, &str, &[&str])] = &[
            (Level::Privileged, "v1.0", &[]),
            (Level::Privileged, "latest", &[]),
            (Level::Baseline, "v1.0", &[]),
            (Level::Baseline, "v1.10", &[]),
            (Level::Baseline, "latest", &[]),
            (Level::Restricted, "v1.0", &["e:v1.0", "h:v1.0"]),
            (Level::Restricted, "v1.10", &["e:v1.0", "g:v1.10", "h:v1.0"]),
            (
                Level::Restricted,
                "latest",
                &["e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0"],
            ),
            (
                Level::Restricted,
                "v1.10000",
                &["e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0"],
            ),
        ];
        for (level, version, want) in cases {
            run_case(&reg, *level, version, want);
        }
    }

    /// registry_test.go `TestCheckRegistry_NoRestricted`.
    #[test]
    fn check_registry_no_restricted() {
        let checks = vec![
            generate_check("a", Level::Baseline, &["v1.0"]),
            generate_check("b", Level::Baseline, &["v1.10"]),
            generate_check("c", Level::Baseline, &["v1.0", "v1.5", "v1.10"]),
            generate_check("d", Level::Baseline, &["v1.11", "v1.15", "v1.20"]),
        ];
        let reg = CheckRegistry::new(checks, None).unwrap();
        let cases: &[(&str, &[&str])] = &[
            ("v1.0", &["a:v1.0", "c:v1.0"]),
            ("v1.4", &["a:v1.0", "c:v1.0"]),
            ("v1.5", &["a:v1.0", "c:v1.5"]),
            ("v1.10", &["a:v1.0", "b:v1.10", "c:v1.10"]),
            ("v1.11", &["a:v1.0", "b:v1.10", "c:v1.10", "d:v1.11"]),
            ("latest", &["a:v1.0", "b:v1.10", "c:v1.10", "d:v1.20"]),
        ];
        for (version, want) in cases {
            run_case(&reg, Level::Baseline, version, want);
            // Restricted results should be identical to baseline.
            run_case(&reg, Level::Restricted, version, want);
        }
    }

    /// registry_test.go `TestCheckRegistry_Empty`.
    #[test]
    fn check_registry_empty() {
        let reg = CheckRegistry::new(Vec::new(), None).unwrap();
        for level in [Level::Privileged, Level::Baseline, Level::Restricted] {
            run_case(&reg, level, "latest", &[]);
        }
    }

    /// An emulation version lowers the cached max version
    /// (registry.go:70-76).
    #[test]
    fn check_registry_emulation_version_caps_latest() {
        let checks = vec![generate_check("a", Level::Baseline, &["v1.0", "v1.20"])];
        let reg = CheckRegistry::new(checks, Some(Version::major_minor(1, 10))).unwrap();
        run_case(&reg, Level::Baseline, "latest", &["a:v1.0"]);
    }

    /// registry.go `validateChecks` rejections.
    #[test]
    fn validate_checks_rejections() {
        let err = |checks: Vec<Check>| CheckRegistry::new(checks, None).err().unwrap();
        assert_eq!(
            err(vec![
                generate_check("a", Level::Baseline, &["v1.0"]),
                generate_check("a", Level::Baseline, &["v1.0"])
            ]),
            "multiple checks registered for ID a"
        );
        assert_eq!(
            err(vec![generate_check("a", Level::Privileged, &["v1.0"])]),
            "check a: invalid level privileged"
        );
        assert_eq!(
            err(vec![generate_check("a", Level::Baseline, &[])]),
            "check a: empty"
        );
        assert_eq!(
            err(vec![generate_check("a", Level::Baseline, &["latest"])]),
            "check a: version cannot be 'latest'"
        );
        assert_eq!(
            err(vec![generate_check(
                "a",
                Level::Baseline,
                &["v1.2", "v1.2"]
            )]),
            "check a: duplicate version v1.2"
        );
        assert_eq!(
            err(vec![generate_check(
                "a",
                Level::Baseline,
                &["v1.3", "v1.2"]
            )]),
            "check a: versions must be strictly increasing"
        );
        assert_eq!(
            err(vec![with_overrides(
                generate_check("a", Level::Baseline, &["v1.0"]),
                &["b"]
            )]),
            "check a: only restricted checks may set overrides"
        );
        assert_eq!(
            err(vec![
                generate_check("b", Level::Restricted, &["v1.0"]),
                with_overrides(generate_check("a", Level::Restricted, &["v1.0"]), &["b"])
            ]),
            "check a: overrides restricted check b"
        );
    }

    /// checks_test.go `TestValidChecks`: the registered checks are valid and
    /// every override names an existing check.
    #[test]
    fn default_checks_are_valid() {
        let all = default_checks();
        validate_checks(&all).unwrap();
        let ids: BTreeSet<CheckId> = all.iter().map(|c| c.id).collect();
        for check in &all {
            for c in &check.versions {
                for o in &c.override_check_ids {
                    assert!(ids.contains(o), "{} overrides missing check {o}", check.id);
                }
            }
        }
    }

    // --- check_*_test.go ports -----------------------------------------

    fn pod(spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"}, "spec": spec,
        }))
        .unwrap()
    }

    fn pod_with_annotations(spec: serde_json::Value, annotations: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "annotations": annotations}, "spec": spec,
        }))
        .unwrap()
    }

    fn run(f: fn(&ObjectMeta, &PodSpec) -> CheckResult, p: &Pod) -> CheckResult {
        f(&p.metadata, p.spec.as_ref().unwrap())
    }

    #[track_caller]
    fn expect(r: CheckResult, reason: &str, detail: &str) {
        assert!(!r.allowed, "expected disallowed");
        assert_eq!(r.forbidden_reason, reason);
        assert_eq!(r.forbidden_detail, detail);
    }

    #[track_caller]
    fn expect_allowed(r: CheckResult) {
        assert!(r.allowed, "expected allowed, got {r:?}");
    }

    fn c(name: &str, sc: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"name": name, "image": "i", "securityContext": sc})
    }

    /// check_privileged_test.go `TestPrivileged`.
    #[test]
    fn privileged() {
        let p = pod(serde_json::json!({
            "containers": [
                {"name": "a", "image": "i"},
                c("b", serde_json::json!({})),
                c("c", serde_json::json!({"privileged": false})),
            ],
            "initContainers": [
                c("d", serde_json::json!({"privileged": true})),
            ],
        }));
        let mut p2 = p.clone();
        p2.spec
            .as_mut()
            .unwrap()
            .containers
            .push(serde_json::from_value(c("e", serde_json::json!({"privileged": true}))).unwrap());
        expect(
            run(privileged_1_0, &p2),
            "privileged",
            r#"containers "d", "e" must not set securityContext.privileged=true"#,
        );
        expect(
            run(privileged_1_0, &p),
            "privileged",
            r#"container "d" must not set securityContext.privileged=true"#,
        );
    }

    /// check_hostNamespaces_test.go.
    #[test]
    fn host_namespaces() {
        let p = pod(serde_json::json!({
            "hostNetwork": true, "hostPID": true, "hostIPC": true,
            "containers": [{"name": "a", "image": "i"}],
        }));
        expect(
            run(host_namespaces_1_0, &p),
            "host namespaces",
            "hostNetwork=true, hostPID=true, hostIPC=true",
        );
        let ok = pod(serde_json::json!({"hostNetwork": false, "containers": []}));
        expect_allowed(run(host_namespaces_1_0, &ok));
    }

    /// check_hostPathVolumes_test.go.
    #[test]
    fn host_path_volumes() {
        let p = pod(serde_json::json!({
            "containers": [],
            "volumes": [
                {"name": "a", "hostPath": {"path": "/a"}},
                {"name": "b", "hostPath": {"path": "/b"}},
                {"name": "c", "emptyDir": {}},
            ],
        }));
        expect(
            run(host_path_volumes_1_0, &p),
            "hostPath volumes",
            r#"volumes "a", "b""#,
        );
    }

    /// check_hostPorts_test.go: ports are sorted lexically (sets.String).
    #[test]
    fn host_ports() {
        let one = pod(serde_json::json!({"containers": [
            {"name": "a", "image": "i", "ports": [{"containerPort": 1}]},
            {"name": "b", "image": "i", "ports": [{"containerPort": 1, "hostPort": 20}]},
        ]}));
        expect(
            run(host_ports_1_0, &one),
            "hostPort",
            r#"container "b" uses hostPort 20"#,
        );
        let many = pod(serde_json::json!({"containers": [
            {"name": "a", "image": "i", "ports": [{"containerPort": 1}]},
            {"name": "b", "image": "i", "ports": [{"containerPort": 1, "hostPort": 10},
                                                  {"containerPort": 2, "hostPort": 20}]},
            {"name": "c", "image": "i", "ports": [{"containerPort": 1, "hostPort": 20},
                                                  {"containerPort": 2, "hostPort": 30}]},
        ]}));
        expect(
            run(host_ports_1_0, &many),
            "hostPort",
            r#"containers "b", "c" use hostPorts 10, 20, 30"#,
        );
    }

    /// check_hostProbesAndhostLifecycle_test.go: the check starts at v1.34,
    /// and lists distinct containers and hosts (sorted).
    #[test]
    fn host_probes_and_lifecycle() {
        let p = pod(serde_json::json!({"containers": [
            {"name": "b", "image": "i",
             "livenessProbe": {"httpGet": {"port": 80, "host": "h1"}},
             "lifecycle": {"preStop": {"tcpSocket": {"port": 80, "host": "h2"}}}},
            {"name": "a", "image": "i",
             "readinessProbe": {"tcpSocket": {"port": 80, "host": "h1"}}},
            {"name": "c", "image": "i",
             "startupProbe": {"httpGet": {"port": 80, "host": ""}}},
        ]}));
        expect(
            run(host_probes_and_host_lifecycle_1_34, &p),
            "probe or lifecycle host",
            r#"containers "a", "b" use probe or lifecycle hosts "h1", "h2""#,
        );
        let reg = CheckRegistry::new(default_checks(), None).unwrap();
        let results = |v: &str| {
            aggregate_check_results(&reg.evaluate_pod(
                lv(Level::Baseline, v),
                &p.metadata,
                p.spec.as_ref().unwrap(),
            ))
        };
        assert!(results("v1.33").allowed);
        assert!(!results("v1.34").allowed);
    }

    /// check_hostProbesAndhostLifecycle_test.go
    /// `TestHostProbesAndHostLifecycleEmulation`: the host check only
    /// exists from 1.34, and an emulation version below that removes it
    /// even for `latest`/`1.34` evaluations.
    #[test]
    fn host_probes_and_lifecycle_emulation() {
        let p = pod(serde_json::json!({"containers": [
            {"name": "", "startupProbe": {"httpGet": {"port": 80, "host": "localhost"}}},
        ]}));
        // (emulate, hostCheckActive)
        for (emulate, active) in [
            (None, true),
            (Some(Version::major_minor(1, 34)), true),
            (Some(Version::major_minor(1, 33)), false),
        ] {
            let reg = CheckRegistry::new(default_checks(), emulate).unwrap();
            let allowed = |v: &str| {
                aggregate_check_results(&reg.evaluate_pod(
                    lv(Level::Baseline, v),
                    &p.metadata,
                    p.spec.as_ref().unwrap(),
                ))
                .allowed
            };
            assert_eq!(allowed("latest"), !active, "latest, emulate {emulate:?}");
            assert_eq!(allowed("v1.34"), !active, "1.34, emulate {emulate:?}");
            assert!(allowed("v1.33"), "1.33, emulate {emulate:?}");
        }
    }

    /// check_hostProbesAndhostLifecycle_test.go `TestHostProbesAndHostLifecycle`
    /// table, verbatim names and details.
    #[test]
    fn host_probes_and_lifecycle_upstream_table() {
        use serde_json::json;
        let http = |h: &str| json!({"httpGet": {"port": 80, "host": h}});
        let tcp = |h: &str| json!({"tcpSocket": {"port": 80, "host": h}});
        let ctr = |name: &str, extra: serde_json::Value| {
            let mut m = json!({"name": name, "image": "i"});
            m.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            m
        };
        let cases: Vec<(&str, serde_json::Value, Option<&str>)> = vec![
            (
                "valid pod with unset hosts",
                json!({"containers": [ctr("a", json!({
                    "livenessProbe": {"httpGet": {"port": 80}},
                    "readinessProbe": {"tcpSocket": {"port": 80}},
                    "lifecycle": {"postStart": {"httpGet": {"port": 80}}},
                }))]}),
                None,
            ),
            (
                "invalid pod with local host IP as probe host",
                json!({"containers": [ctr("a", json!({
                    "livenessProbe": http("127.0.0.1"),
                    "readinessProbe": tcp("::1"),
                    "startupProbe": http("localhost"),
                }))]}),
                Some(
                    r#"container "a" uses probe or lifecycle hosts "127.0.0.1", "::1", "localhost""#,
                ),
            ),
            (
                "invalid httpget host in liveness probe",
                json!({"containers": [ctr("a", json!({"livenessProbe": http("invalid.host")}))]}),
                Some(r#"container "a" uses probe or lifecycle host "invalid.host""#),
            ),
            (
                "invalid tcpsocket host in readiness probe",
                json!({"containers": [ctr("b", json!({"readinessProbe": tcp("invalid.host")}))]}),
                Some(r#"container "b" uses probe or lifecycle host "invalid.host""#),
            ),
            (
                "invalid httpget host in startup probe",
                json!({"containers": [ctr("c", json!({"startupProbe": http("invalid.host")}))]}),
                Some(r#"container "c" uses probe or lifecycle host "invalid.host""#),
            ),
            (
                "invalid poststart tcpsocket host",
                json!({"containers": [ctr("d", json!({"lifecycle": {"postStart": tcp("invalid.host")}}))]}),
                Some(r#"container "d" uses probe or lifecycle host "invalid.host""#),
            ),
            (
                "invalid prestop httpget host",
                json!({"containers": [ctr("e", json!({"lifecycle": {"preStop": http("another.invalid.host")}}))]}),
                Some(r#"container "e" uses probe or lifecycle host "another.invalid.host""#),
            ),
            (
                "multiple containers with multiple invalid hosts",
                json!({"containers": [
                    ctr("valid", json!({})),
                    ctr("invalid1", json!({"livenessProbe": http("a.com")})),
                    ctr("invalid2", json!({
                        "lifecycle": {"preStop": tcp("b.com")},
                        "startupProbe": http("a.com"),
                    })),
                ]}),
                Some(
                    r#"containers "invalid1", "invalid2" use probe or lifecycle hosts "a.com", "b.com""#,
                ),
            ),
            (
                "invalid ipv4 host in probe",
                json!({"containers": [ctr("a", json!({"livenessProbe": http("8.8.8.8")}))]}),
                Some(r#"container "a" uses probe or lifecycle host "8.8.8.8""#),
            ),
            (
                "invalid ipv6 host in probe",
                json!({"containers": [ctr("a", json!({"livenessProbe": tcp("2001:4860:4860::8888")}))]}),
                Some(r#"container "a" uses probe or lifecycle host "2001:4860:4860::8888""#),
            ),
            (
                "invalid host in initcontainer",
                json!({"containers": [], "initContainers": [
                    ctr("init", json!({"livenessProbe": http("invalid.init.host")}))]}),
                Some(r#"container "init" uses probe or lifecycle host "invalid.init.host""#),
            ),
        ];
        for (name, spec, detail) in cases {
            let r = run(host_probes_and_host_lifecycle_1_34, &pod(spec));
            match detail {
                None => assert!(r.allowed, "{name}: expected allowed, got {r:?}"),
                Some(d) => {
                    assert!(!r.allowed, "{name}: expected forbidden");
                    assert_eq!(r.forbidden_reason, "probe or lifecycle host", "{name}");
                    assert_eq!(r.forbidden_detail, d, "{name}");
                }
            }
        }
    }

    /// check_capabilities_baseline_test.go.
    #[test]
    fn capabilities_baseline() {
        let p = pod(serde_json::json!({"containers": [
            c("a", serde_json::json!({"capabilities": {"add": ["NET_RAW", "CHOWN"]}})),
            c("b", serde_json::json!({"capabilities": {"add": ["NET_RAW", "SYS_ADMIN"]}})),
            c("c", serde_json::json!({"capabilities": {"add": ["CHOWN"]}})),
        ]}));
        expect(
            run(capabilities_baseline_1_0, &p),
            "non-default capabilities",
            r#"containers "a", "b" must not include "NET_RAW", "SYS_ADMIN" in securityContext.capabilities.add"#,
        );
    }

    /// check_capabilities_restricted_test.go.
    #[test]
    fn capabilities_restricted() {
        let spec = serde_json::json!({"containers": [
            c("a", serde_json::json!({"capabilities": {"add": ["FOO", "BAR"]}})),
            c("b", serde_json::json!({"capabilities": {"add": ["BAR", "BAZ"]}})),
            c("c", serde_json::json!({"capabilities": {"add": ["NET_BIND_SERVICE", "CHOWN"], "drop": ["ALL", "FOO"]}})),
        ]});
        let detail = r#"containers "a", "b" must set securityContext.capabilities.drop=["ALL"]; containers "a", "b", "c" must not include "BAR", "BAZ", "CHOWN", "FOO" in securityContext.capabilities.add"#;
        expect(
            run(capabilities_restricted_1_25, &pod(spec.clone())),
            "unrestricted capabilities",
            detail,
        );
        expect(
            run(capabilities_restricted_1_22, &pod(spec)),
            "unrestricted capabilities",
            detail,
        );
        // windows pod, admit without checking capabilities
        expect_allowed(run(
            capabilities_restricted_1_25,
            &pod(serde_json::json!({"os": {"name": "windows"},
                "containers": [{"name": "a", "image": "i"}]})),
        ));
        // linux pod, reject if security context is not set
        expect(
            run(
                capabilities_restricted_1_25,
                &pod(serde_json::json!({"os": {"name": "linux"},
                    "containers": [{"name": "a", "image": "i"}]})),
            ),
            "unrestricted capabilities",
            r#"container "a" must set securityContext.capabilities.drop=["ALL"]"#,
        );
    }

    /// check_allowPrivilegeEscalation_test.go.
    #[test]
    fn allow_privilege_escalation() {
        let p = pod(serde_json::json!({"containers": [
            {"name": "a", "image": "i"},
            c("b", serde_json::json!({})),
            c("c", serde_json::json!({"allowPrivilegeEscalation": true})),
            c("d", serde_json::json!({"allowPrivilegeEscalation": false})),
        ]}));
        let detail =
            r#"containers "a", "b", "c" must set securityContext.allowPrivilegeEscalation=false"#;
        expect(
            run(allow_privilege_escalation_1_25, &p),
            "allowPrivilegeEscalation != false",
            detail,
        );
        expect(
            run(allow_privilege_escalation_1_8, &p),
            "allowPrivilegeEscalation != false",
            detail,
        );
        expect_allowed(run(
            allow_privilege_escalation_1_25,
            &pod(serde_json::json!({"os": {"name": "windows"},
                "containers": [{"name": "a", "image": "i"}]})),
        ));
        expect(
            run(
                allow_privilege_escalation_1_25,
                &pod(serde_json::json!({"os": {"name": "linux"},
                    "containers": [{"name": "a", "image": "i"}]})),
            ),
            "allowPrivilegeEscalation != false",
            r#"container "a" must set securityContext.allowPrivilegeEscalation=false"#,
        );
    }

    /// check_procMount_{baseline,restricted}_test.go.
    #[test]
    fn proc_mount() {
        let spec = serde_json::json!({"containers": [
            {"name": "a", "image": "i"},
            c("b", serde_json::json!({})),
            c("c", serde_json::json!({"procMount": "Default"})),
            c("d", serde_json::json!({"procMount": "Unmasked"})),
            c("e", serde_json::json!({"procMount": "other"})),
        ]});
        let detail =
            r#"containers "d", "e" must not set securityContext.procMount to "Unmasked", "other""#;
        expect(run(proc_mount_1_0, &pod(spec.clone())), "procMount", detail);
        expect(
            run(proc_mount_1_35_baseline, &pod(spec.clone())),
            "procMount",
            detail,
        );
        // procMount with userns: baseline 1.35 relaxes, 1.0 and restricted do not.
        let mut with_userns = spec;
        with_userns["hostUsers"] = serde_json::json!(false);
        expect_allowed(run(proc_mount_1_35_baseline, &pod(with_userns.clone())));
        expect(run(proc_mount_1_0, &pod(with_userns)), "procMount", detail);
    }

    /// The restricted procMount check overrides the baseline one from v1.35,
    /// so a user-namespace pod is still refused at `restricted:latest`
    /// (check_procMount_restricted.go:46-49).
    #[test]
    fn proc_mount_restricted_overrides_baseline_relaxation() {
        let p = pod(serde_json::json!({
            "hostUsers": false,
            "containers": [c("a", serde_json::json!({"procMount": "Unmasked"}))],
        }));
        let reg = CheckRegistry::new(default_checks(), None).unwrap();
        let eval = |level, v: &str| {
            aggregate_check_results(&reg.evaluate_pod(
                lv(level, v),
                &p.metadata,
                p.spec.as_ref().unwrap(),
            ))
            .forbidden_detail()
        };
        assert_eq!(eval(Level::Baseline, "latest"), "");
        assert!(eval(Level::Restricted, "latest").contains("procMount (container"));
        assert!(eval(Level::Restricted, "v1.34").contains("procMount (container"));
    }

    /// check_runAsUser_test.go.
    #[test]
    fn run_as_user() {
        expect(
            run(
                run_as_user_1_23,
                &pod(serde_json::json!({"securityContext": {"runAsUser": 0},
                    "containers": [{"name": "a", "image": "i"}]})),
            ),
            "runAsUser=0",
            "pod must not set runAsUser=0",
        );
        expect_allowed(run(
            run_as_user_1_23,
            &pod(serde_json::json!({"securityContext": {"runAsUser": 1},
                "containers": [{"name": "a", "image": "i"}]})),
        ));
        let spec = serde_json::json!({"containers": [
            {"name": "a", "image": "i"},
            c("b", serde_json::json!({"runAsUser": 1})),
            c("c", serde_json::json!({"runAsUser": 0})),
        ], "initContainers": [c("d", serde_json::json!({"runAsUser": 0}))]});
        expect(
            run(run_as_user_1_23, &pod(spec.clone())),
            "runAsUser=0",
            r#"containers "d", "c" must not set runAsUser=0"#,
        );
        // host users false allowed (1.35+)
        let mut userns = spec;
        userns["hostUsers"] = serde_json::json!(false);
        expect_allowed(run(run_as_user_1_35, &pod(userns)));
    }

    /// check_runAsNonRoot_test.go.
    #[test]
    fn run_as_non_root() {
        expect(
            run(
                run_as_non_root_1_0,
                &pod(serde_json::json!({"containers": [{"name": "a", "image": "i"}]})),
            ),
            "runAsNonRoot != true",
            r#"pod or container "a" must set securityContext.runAsNonRoot=true"#,
        );
        expect(
            run(
                run_as_non_root_1_0,
                &pod(
                    serde_json::json!({"securityContext": {"runAsNonRoot": false},
                    "containers": [{"name": "a", "image": "i"}]}),
                ),
            ),
            "runAsNonRoot != true",
            "pod must not set securityContext.runAsNonRoot=false",
        );
        expect(
            run(
                run_as_non_root_1_0,
                &pod(serde_json::json!({"containers": [
                    c("c", serde_json::json!({"runAsNonRoot": false})),
                    c("d", serde_json::json!({"runAsNonRoot": false})),
                    c("e", serde_json::json!({"runAsNonRoot": true})),
                ]})),
            ),
            "runAsNonRoot != true",
            r#"containers "c", "d" must not set securityContext.runAsNonRoot=false"#,
        );
        // pod nil, container fallthrough
        expect(
            run(
                run_as_non_root_1_0,
                &pod(serde_json::json!({"containers": [
                    {"name": "a", "image": "i"},
                    c("b", serde_json::json!({})),
                    c("c", serde_json::json!({"runAsNonRoot": true})),
                ]})),
            ),
            "runAsNonRoot != true",
            r#"pod or containers "a", "b" must set securityContext.runAsNonRoot=true"#,
        );
        // pod-level true covers containers that do not set it
        expect_allowed(run(
            run_as_non_root_1_0,
            &pod(
                serde_json::json!({"securityContext": {"runAsNonRoot": true},
                "containers": [{"name": "a", "image": "i"}]}),
            ),
        ));
        // host users false allowed (1.35+) but not at 1.0
        let userns = pod(serde_json::json!({"hostUsers": false,
            "containers": [{"name": "a", "image": "i"}]}));
        expect_allowed(run(run_as_non_root_1_35, &userns));
        assert!(!run(run_as_non_root_1_0, &userns).allowed);
    }

    /// check_sysctls_test.go: each added sysctl is forbidden before its
    /// version and allowed from it.
    #[test]
    fn sysctls_by_version() {
        let with = |name: &str| {
            pod(serde_json::json!({"securityContext": {"sysctls": [
                {"name": name, "value": "1"}]}, "containers": []}))
        };
        expect(
            run(
                sysctls_1_0,
                &pod(serde_json::json!({"securityContext": {"sysctls": [
                    {"name": "a", "value": "1"}, {"name": "b", "value": "1"},
                    {"name": "kernel.shm_rmid_forced", "value": "1"}]}, "containers": []})),
            ),
            "forbidden sysctls",
            "a, b",
        );
        for (name, f_before, f_after) in [
            (
                "net.ipv4.ip_local_reserved_ports",
                sysctls_1_0 as fn(&ObjectMeta, &PodSpec) -> CheckResult,
                sysctls_1_27 as fn(&ObjectMeta, &PodSpec) -> CheckResult,
            ),
            ("net.ipv4.tcp_keepalive_time", sysctls_1_27, sysctls_1_29),
            ("net.ipv4.tcp_fin_timeout", sysctls_1_27, sysctls_1_29),
            ("net.ipv4.tcp_keepalive_intvl", sysctls_1_27, sysctls_1_29),
            ("net.ipv4.tcp_keepalive_probes", sysctls_1_27, sysctls_1_29),
            ("net.ipv4.tcp_rmem", sysctls_1_29, sysctls_1_32),
            ("net.ipv4.tcp_wmem", sysctls_1_29, sysctls_1_32),
        ] {
            expect(run(f_before, &with(name)), "forbidden sysctls", name);
            expect_allowed(run(f_after, &with(name)));
        }
    }

    /// check_windowsHostProcess_test.go.
    #[test]
    fn windows_host_process() {
        let p = pod(serde_json::json!({
            "securityContext": {"windowsOptions": {"hostProcess": true}},
            "containers": [
                c("a", serde_json::json!({"windowsOptions": {"hostProcess": false}})),
                c("e", serde_json::json!({"windowsOptions": {"hostProcess": true}})),
                c("f", serde_json::json!({"windowsOptions": {"hostProcess": true}})),
            ],
        }));
        expect(
            run(windows_host_process_1_0, &p),
            "hostProcess",
            r#"pod and containers "e", "f" must not set securityContext.windowsOptions.hostProcess=true"#,
        );
    }

    /// check_seLinuxOptions_test.go: `container_engine_t` is allowed from
    /// v1.31.
    #[test]
    fn se_linux_options() {
        let p = pod(serde_json::json!({
            "securityContext": {"seLinuxOptions": {"type": "bar", "user": "u"}},
            "containers": [
                c("a", serde_json::json!({"seLinuxOptions": {"type": "container_t"}})),
                c("b", serde_json::json!({"seLinuxOptions": {"role": "r", "type": "foo"}})),
            ],
        }));
        expect(
            run(se_linux_options_1_0, &p),
            "seLinuxOptions",
            r#"pod and container "b" set forbidden securityContext.seLinuxOptions: types "bar", "foo"; user may not be set; role may not be set"#,
        );
        let engine = pod(serde_json::json!({"containers": [
            c("a", serde_json::json!({"seLinuxOptions": {"type": "container_engine_t"}}))]}));
        expect(
            run(se_linux_options_1_0, &engine),
            "seLinuxOptions",
            r#"container "a" set forbidden securityContext.seLinuxOptions: type "container_engine_t""#,
        );
        expect_allowed(run(se_linux_options_1_31, &engine));
    }

    /// check_seccompProfile_baseline_test.go.
    #[test]
    fn seccomp_baseline() {
        let p = pod_with_annotations(
            serde_json::json!({"containers": [{"name": "a", "image": "i"},
                {"name": "b", "image": "i"}]}),
            serde_json::json!({
                "seccomp.security.alpha.kubernetes.io/pod": "unconfined",
                "container.seccomp.security.alpha.kubernetes.io/a": "localhost/x",
                "container.seccomp.security.alpha.kubernetes.io/b": "unconfined",
            }),
        );
        expect(
            run(seccomp_profile_baseline_1_0, &p),
            "seccompProfile",
            r#"forbidden annotations container.seccomp.security.alpha.kubernetes.io/b="unconfined", seccomp.security.alpha.kubernetes.io/pod="unconfined""#,
        );
        let p = pod(serde_json::json!({
            "securityContext": {"seccompProfile": {"type": "Unconfined"}},
            "containers": [
                c("a", serde_json::json!({"seccompProfile": {"type": "RuntimeDefault"}})),
                c("b", serde_json::json!({"seccompProfile": {"type": "Foo"}})),
                {"name": "c", "image": "i"},
            ],
        }));
        expect(
            run(seccomp_profile_baseline_1_19, &p),
            "seccompProfile",
            r#"pod and container "b" must not set securityContext.seccompProfile.type to "Foo", "Unconfined""#,
        );
    }

    /// check_seccompProfile_restricted_test.go.
    #[test]
    fn seccomp_restricted() {
        let p = pod(serde_json::json!({"containers": [
            {"name": "a", "image": "i"},
            c("b", serde_json::json!({"seccompProfile": {"type": "RuntimeDefault"}})),
        ]}));
        expect(
            run(seccomp_profile_restricted_1_19, &p),
            "seccompProfile",
            r#"pod or container "a" must set securityContext.seccompProfile.type to "RuntimeDefault" or "Localhost""#,
        );
        // a valid pod-level profile covers unset containers
        expect_allowed(run(
            seccomp_profile_restricted_1_19,
            &pod(serde_json::json!({
                "securityContext": {"seccompProfile": {"type": "Localhost"}},
                "containers": [{"name": "a", "image": "i"}]})),
        ));
        // windows pods are exempt from 1.25
        let win = pod(serde_json::json!({"os": {"name": "windows"},
            "containers": [{"name": "a", "image": "i"}]}));
        expect_allowed(run(seccomp_profile_restricted_1_25, &win));
        assert!(!run(seccomp_profile_restricted_1_19, &win).allowed);
    }

    /// check_appArmorProfile_test.go.
    #[test]
    fn app_armor() {
        let p = pod_with_annotations(
            serde_json::json!({
            "securityContext": {"appArmorProfile": {"type": "Unconfined"}},
            "containers": [
                c("a", serde_json::json!({"appArmorProfile": {"type": "RuntimeDefault"}})),
                c("b", serde_json::json!({"appArmorProfile": {"type": "Foo"}})),
            ]}),
            serde_json::json!({
                "container.apparmor.security.beta.kubernetes.io/a": "unconfined",
                "container.apparmor.security.beta.kubernetes.io/b": "localhost/ok",
                "other": "x",
            }),
        );
        expect(
            run(app_armor_profile_1_0, &p),
            "forbidden AppArmor profiles",
            r#"pod and container "b" and annotation must not set AppArmor profile type to "Foo", "Unconfined", "container.apparmor.security.beta.kubernetes.io/a="unconfined"""#,
        );
        let one = pod(serde_json::json!({"securityContext":
            {"appArmorProfile": {"type": "Unconfined"}}, "containers": []}));
        expect(
            run(app_armor_profile_1_0, &one),
            "forbidden AppArmor profile",
            r#"pod must not set AppArmor profile type to "Unconfined""#,
        );
    }

    /// check_restrictedVolumes_test.go.
    #[test]
    fn restricted_volumes() {
        let p = pod(serde_json::json!({"containers": [], "volumes": [
            {"name": "a", "hostPath": {"path": "/"}},
            {"name": "b", "emptyDir": {}},
            {"name": "c", "nfs": {"server": "s", "path": "/"}},
            {"name": "d", "configMap": {"name": "x"}},
            {"name": "e"},
        ]}));
        expect(
            run(restricted_volumes_1_0, &p),
            "restricted volume types",
            r#"volumes "a", "c", "e" use restricted volume types "hostPath", "nfs", "unknown""#,
        );
    }

    /// check_restrictedVolumes.go:108-153: every deprecated in-tree source is
    /// named by its own type; only a source-less volume is "unknown".
    #[test]
    fn restricted_volumes_names_every_legacy_type() {
        let cases = [
            "gcePersistentDisk",
            "awsElasticBlockStore",
            "gitRepo",
            "glusterfs",
            "rbd",
            "flexVolume",
            "cinder",
            "cephfs",
            "flocker",
            "fc",
            "azureFile",
            "vsphereVolume",
            "quobyte",
            "azureDisk",
            "photonPersistentDisk",
            "portworxVolume",
            "scaleIO",
            "storageos",
        ];
        for ty in cases {
            let p = pod(serde_json::json!({"containers": [], "volumes": [
                {"name": "v", ty: {}},
            ]}));
            let want = format!(r#"volume "v" uses restricted volume type "{ty}""#);
            expect(
                run(restricted_volumes_1_0, &p),
                "restricted volume types",
                &want,
            );
        }
    }

    /// visitor.go:30-40 visits ephemeral containers, so a hostPort on one is
    /// seen by hostPorts.
    #[test]
    fn host_ports_sees_ephemeral_container_ports() {
        let p = pod(
            serde_json::json!({"containers": [], "ephemeralContainers": [
                {"name": "dbg", "image": "x", "ports": [{"containerPort": 80, "hostPort": 80}]},
            ]}),
        );
        expect(
            run(host_ports_1_0, &p),
            "hostPort",
            r#"container "dbg" uses hostPort 80"#,
        );
    }

    /// Restricted overrides hostPathVolumes: a hostPath pod is reported once,
    /// as restricted volume types (registry.go `populate` overrides).
    #[test]
    fn restricted_volumes_overrides_host_path_volumes() {
        let p = pod(serde_json::json!({"containers": [], "volumes": [
            {"name": "a", "hostPath": {"path": "/"}}]}));
        let reg = CheckRegistry::new(default_checks(), None).unwrap();
        let run_level = |level| {
            aggregate_check_results(&reg.evaluate_pod(
                lv(level, "latest"),
                &p.metadata,
                p.spec.as_ref().unwrap(),
            ))
        };
        assert_eq!(
            run_level(Level::Baseline).forbidden_reasons,
            vec!["hostPath volumes"]
        );
        assert_eq!(
            run_level(Level::Restricted).forbidden_reasons,
            vec!["restricted volume types"]
        );
    }

    /// checks.go `AggregateCheckResults` / `ForbiddenDetail`.
    #[test]
    fn aggregate_formats_reason_with_detail() {
        let agg = aggregate_check_results(&[
            CheckResult::forbidden("a", "x"),
            CheckResult::allowed(),
            CheckResult::forbidden("b", ""),
            CheckResult::forbidden("", "y"),
        ]);
        assert!(!agg.allowed);
        assert_eq!(agg.forbidden_reason(), "a, b, unknown forbidden reason");
        assert_eq!(
            agg.forbidden_detail(),
            "a (x), b, unknown forbidden reason (y)"
        );
        assert!(aggregate_check_results(&[CheckResult::allowed()]).allowed);
    }
}
