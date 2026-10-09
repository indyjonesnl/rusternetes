//! Fixture runner: port of upstream pod-security-admission `test/run.go`
//! (`Run`) and `test/fixtures_test.go` (`TestFixtures`), executed against the
//! in-process evaluator instead of a live cluster.
//!
//! The pod fixtures under `testdata/pod-security-admission/` are copied
//! verbatim from release-1.35
//! `staging/src/k8s.io/pod-security-admission/test/testdata/` (upstream
//! generates them from `test/fixtures_*.go`; the generated YAML is the
//! language-neutral form of every check's pass/fail/fail-variation pods, per
//! level and per version). For every level (baseline, restricted) and every
//! policy version v1.0..=v1.35 (`newestMinorVersionToTest`, run.go:40):
//!
//! * every `pass/*.yaml` pod must be allowed by ALL checks (run.go:341-376,
//!   `createPod(..., expectSuccess=true)`);
//! * every `fail/<checkid><n>.yaml` pod must be forbidden by the check named
//!   by the file stem (run.go:398-405; upstream asserts the warning contains
//!   the check's `expectErrorSubstring`, which we model as "that check's own
//!   result is forbidden");
//! * every check applicable at the level/version has fail fixtures
//!   (fixtures_test.go:75-88).
//!
//! Child module of `pod_security_policy` so it reaches the private registry
//! internals.
use super::*;
use crate::admission::pod_security_api::{Level, LevelVersion, Version};
use rusternetes_common::resources::pod::Pod;
use std::path::{Path, PathBuf};

/// run.go:40 `newestMinorVersionToTest`.
const NEWEST_MINOR_VERSION_TO_TEST: u32 = 35;

fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/pod-security-admission")
}

fn load(dir: &Path) -> Vec<(String, Pod)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut paths: Vec<PathBuf> = rd.map(|e| e.unwrap().path()).collect();
    paths.sort();
    for p in paths {
        let text = std::fs::read_to_string(&p).unwrap();
        let mut pod: Pod = serde_yaml::from_str(&text)
            .unwrap_or_else(|e| panic!("{} does not decode as a Pod: {e}", p.display()));
        // run.go creates the pod through the apiserver, which applies the
        // v1 defaults before admission runs (e.g. SetDefaults_Volume turns the
        // fixtures' source-less `volume0` into an emptyDir).
        crate::handlers::defaults::apply_pod_spec_defaults(pod.spec.as_mut().unwrap());
        let stem = p.file_stem().unwrap().to_string_lossy().to_string();
        out.push((stem, pod));
    }
    out
}

fn evaluate(reg: &CheckRegistry, level: Level, minor: u32, pod: &Pod) -> Vec<CheckResult> {
    reg.evaluate_pod(
        LevelVersion::new(level, Version::major_minor(1, minor)),
        &pod.metadata,
        pod.spec.as_ref().expect("fixture has a spec"),
    )
}

/// `fail/<checkid><n>` -> lowercase check id (fixtures_test.go:96).
fn check_stem(file_stem: &str) -> &str {
    file_stem.trim_end_matches(|c: char| c.is_ascii_digit())
}

/// `checksForLevelAndVersion` (test/helpers.go): the checks that apply at a
/// level and version.
fn checks_for(level: Level, minor: u32) -> Vec<Check> {
    default_checks()
        .into_iter()
        .filter(|c| level == Level::Restricted || c.level == Level::Baseline)
        .filter(|c| c.versions.iter().any(|v| v.minimum_version.minor <= minor))
        .collect()
}

#[test]
fn pod_security_fixtures() {
    let reg = CheckRegistry::new(default_checks(), None).unwrap();
    let mut pass_pods = 0;
    let mut fail_pods = 0;
    for level in [Level::Baseline, Level::Restricted] {
        for minor in 0..=NEWEST_MINOR_VERSION_TO_TEST {
            let dir = testdata()
                .join(level.to_string())
                .join(format!("v1.{minor}"));
            let pass = load(&dir.join("pass"));
            let fail = load(&dir.join("fail"));
            assert!(
                pass.iter().any(|(n, _)| n == "base"),
                "{level}/1.{minor}: no minimal valid pod fixture"
            );

            // run.go `_pass_*`: every check allows every pass pod.
            for (name, pod) in &pass {
                for r in evaluate(&reg, level, minor, pod) {
                    assert!(
                        r.allowed,
                        "{level}/1.{minor} pass/{name}: unexpectedly forbidden: {} {}",
                        r.forbidden_reason, r.forbidden_detail
                    );
                }
                pass_pods += 1;
            }

            // run.go `_fail_*`: the named check forbids the pod.
            let applicable = checks_for(level, minor);
            for (name, pod) in &fail {
                let stem = check_stem(name);
                let check = applicable
                    .iter()
                    .find(|c| c.id.to_lowercase() == stem)
                    .unwrap_or_else(|| {
                        panic!("{level}/1.{minor} fail/{name}: no applicable check {stem}")
                    });
                let single = CheckRegistry::new(vec![check.clone()], None).unwrap();
                let results = evaluate(&single, level, minor, pod);
                let bad: Vec<_> = results.iter().filter(|r| !r.allowed).collect();
                assert!(
                    !bad.is_empty(),
                    "{level}/1.{minor} fail/{name}: {} allowed the pod",
                    check.id
                );
                for r in bad {
                    assert!(
                        !r.forbidden_reason.is_empty(),
                        "{level}/1.{minor} fail/{name}: forbidden without a reason"
                    );
                }
                fail_pods += 1;
            }

            // fixtures_test.go:75-88: every applicable check has fixtures.
            for c in &applicable {
                let id = c.id.to_lowercase();
                assert!(
                    fail.iter().any(|(n, _)| check_stem(n) == id),
                    "{level}/1.{minor}: no fail fixtures for check {}",
                    c.id
                );
            }
        }
    }
    // Guard against an empty / mis-pathed testdata tree.
    assert!(
        pass_pods > 500 && fail_pods > 1000,
        "{pass_pods}/{fail_pods}"
    );
}

/// `expectErrorSubstring` of the fixture generators (`test/fixtures_*.go`);
/// `getFixtures` (fixtures.go:260-266) defaults it to the check ID.
fn expect_error_substring(id: &str) -> &str {
    match id {
        "hostPorts" => "hostPort",
        "hostProbesAndHostLifecycle" => "probe or lifecycle host",
        "hostPathVolumes" => "hostPath",
        "appArmorProfile" => "forbidden AppArmor profile",
        "capabilities_baseline" => "capabilities",
        "capabilities_restricted" => "unrestricted capabilities",
        "windowsHostProcess" => "hostProcess",
        "hostNamespaces" => "host namespaces",
        "procMount" | "procMount_restricted" => "procMount",
        "seLinuxOptions" => "seLinuxOptions",
        "seccompProfile_baseline" | "seccompProfile_restricted" => "seccompProfile",
        "sysctls" => "forbidden sysctl",
        "restrictedVolumes" => "restricted volume types",
        other => other,
    }
}

/// run.go:273-337 `createController`: the same fixture pod wrapped in a
/// Deployment (run.go:285-294, label `test=true`) and dry-run created in a
/// namespace labelled enforce+warn at the level/version under test. A
/// controller is never denied; pass pods must produce no warning and fail
/// pods a warning naming the failing check (run.go:316-336).
///
/// `failRequiresError` (run.go:302-307; only procMount at 1.35 baseline,
/// fixtures_procMount.go:93) means API validation rejects the pod outright,
/// which is outside PodSecurity itself, so those scenarios are not replayed
/// here.
#[tokio::test]
async fn pod_security_fixtures_controller_scenarios() {
    use rusternetes_common::resources::Namespace;
    use rusternetes_storage::{build_key, MemoryStorage, Storage};
    use std::sync::Arc;

    let psa = crate::admission::PodSecurityAdmission::new();
    let storage = Arc::new(MemoryStorage::new());
    let mut controllers = 0;

    // run.go:285-294 wraps `pod.ObjectMeta`/`pod.Spec` in a Deployment with
    // `test=true` added to the labels (:278-281).
    let deployment = |pod: &Pod| {
        let mut meta = serde_json::to_value(&pod.metadata).unwrap();
        meta["labels"] = serde_json::json!({"test": "true"});
        serde_json::json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "test"},
            "spec": {
                "selector": {"matchLabels": {"test": "true"}},
                "template": {"metadata": meta, "spec": pod.spec},
            },
        })
    };

    for level in [Level::Baseline, Level::Restricted] {
        for minor in 0..=NEWEST_MINOR_VERSION_TO_TEST {
            // run.go:212-219
            let ns = format!("podsecurity-{level}-1-{minor}");
            let mut labels = std::collections::BTreeMap::new();
            for mode in ["enforce", "warn"] {
                labels.insert(
                    format!("pod-security.kubernetes.io/{mode}"),
                    level.to_string(),
                );
                labels.insert(
                    format!("pod-security.kubernetes.io/{mode}-version"),
                    format!("v1.{minor}"),
                );
            }
            let n: Namespace = serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Namespace",
                "metadata": {"name": ns, "labels": labels}}))
            .unwrap();
            storage
                .create(&build_key("namespaces", None, &ns), &n)
                .await
                .unwrap();

            let dir = testdata()
                .join(level.to_string())
                .join(format!("v1.{minor}"));
            let run = |pod: &Pod| {
                let obj = deployment(pod);
                let (psa, storage, ns) = (&psa, &storage, ns.clone());
                async move {
                    psa.validate_pod_controller(
                        storage,
                        &ns,
                        None,
                        "apps",
                        "deployments",
                        &obj,
                        "tester",
                    )
                    .await
                }
            };

            for (name, pod) in load(&dir.join("pass")) {
                let out = run(&pod).await;
                assert!(
                    out.warnings.is_empty(),
                    "{ns} pass/{name}: unexpected warning {:?}",
                    out.warnings
                );
                controllers += 1;
            }

            let applicable = checks_for(level, minor);
            for (name, pod) in load(&dir.join("fail")) {
                let stem = check_stem(&name).to_string();
                if stem == "procmount" && minor >= 35 {
                    continue; // failRequiresError, see above
                }
                let check = applicable
                    .iter()
                    .find(|c| c.id.to_lowercase() == stem)
                    .unwrap_or_else(|| panic!("{ns} fail/{name}: no applicable check {stem}"));
                let want = expect_error_substring(check.id);
                let out = run(&pod).await;
                let text = out.warnings.join("; ");
                assert!(!text.is_empty(), "{ns} fail/{name}: expected a warning");
                assert!(
                    !text.contains(UNKNOWN_FORBIDDEN_REASON),
                    "{ns} fail/{name}: unknown forbidden reason: {text}"
                );
                assert!(
                    text.contains(want),
                    "{ns} fail/{name}: warning {text:?} lacks {want:?}"
                );
                controllers += 1;
            }
        }
    }
    assert!(controllers > 1500, "{controllers}");
}
