//! Port of upstream pod-security-admission fixture generation:
//! `test/fixtures.go` (`minimalValidPods` ladder, `registerFixtureGenerator`,
//! `getFixtures`, `checkKey`), `test/helpers.go` (`tweak`,
//! `ensureSecurityContext`, `ensureAnnotation`), `test/run.go`
//! (`checksForLevelAndVersion`, `computeVersionsToTest`) and
//! `test/fixtures_test.go` `TestFixtures`: the in-memory fixtures are compared
//! with the copied `testdata/pod-security-admission` YAML, and the set of files
//! must equal the set the generators produce.
//!
//! Pods are `serde_json::Value`s (the language-neutral form of the YAML), so a
//! Go `ptr.To(false)` is `json!(false)` and a Go `nil` is an absent key.
//!
//! Ported generators: `privileged`, `allowPrivilegeEscalation`,
//! `runAsNonRoot`, `runAsUser`, `hostNamespaces`, `appArmorProfile`. The
//! remaining generators (fixtures_*.go) are tracked in #2904; until they are
//! ported, files belonging to those checks are exempt from the extra-file check
//! (see [`UNPORTED_CHECKS`]).
use super::*;
use crate::admission::pod_security_api::{Level, Version};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// run.go:40-41.
const NEWEST_MINOR_VERSION_TO_TEST: u32 = 35;
const POD_OS_BASED_RESTRICTION_ENABLED_VERSION: u32 = 29;

type Pod = Value;

/// helpers.go `tweak`: copy, mutate, return.
fn tweak(p: &Pod, f: impl FnOnce(&mut Pod)) -> Pod {
    let mut out = p.clone();
    f(&mut out);
    out
}

/// `p.Spec.X = v` for a JSON pointer, creating intermediate objects (a Go
/// `&corev1.SecurityContext{}` is created on first use).
fn set(p: &mut Pod, ptr: &str, v: Value) {
    let mut cur = p;
    let mut parts = ptr.trim_start_matches('/').split('/').peekable();
    while let Some(part) = parts.next() {
        let last = parts.peek().is_none();
        cur = if let Ok(i) = part.parse::<usize>() {
            &mut cur[i]
        } else {
            &mut cur[part]
        };
        if last {
            *cur = v;
            return;
        }
        if cur.is_null() {
            *cur = json!({});
        }
    }
}

/// `p.Spec.X = nil`.
fn unset(p: &mut Pod, ptr: &str) {
    let (parent, key) = ptr.rsplit_once('/').unwrap();
    if let Some(Value::Object(m)) = p.pointer_mut(parent) {
        m.remove(key);
    }
}

/// helpers.go `ensureSecurityContext`.
fn ensure_security_context(p: &Pod) -> Pod {
    tweak(p, |p| {
        if p.pointer("/spec/securityContext").is_none() {
            set(p, "/spec/securityContext", json!({}));
        }
        for list in ["containers", "initContainers"] {
            let n = p["spec"][list].as_array().map_or(0, |a| a.len());
            for i in 0..n {
                let ptr = format!("/spec/{list}/{i}/securityContext");
                if p.pointer(&ptr).is_none() {
                    set(p, &ptr, json!({}));
                }
            }
        }
    })
}

/// helpers.go `ensureAnnotation`.
fn ensure_annotation(p: &Pod) -> Pod {
    tweak(p, |p| {
        if p.pointer("/metadata/annotations").is_none() {
            set(p, "/metadata/annotations", json!({}));
        }
    })
}

// ---------------------------------------------------------------- fixtures.go

type PodsByVersion = BTreeMap<u32, Pod>;

struct MinimalValidPods {
    neutral: BTreeMap<Level, PodsByVersion>,
    linux: BTreeMap<Level, PodsByVersion>,
    windows: BTreeMap<Level, PodsByVersion>,
}

fn add_os(p: &Pod, os: &str) -> Pod {
    tweak(p, |p| set(p, "/spec/os", json!({ "name": os })))
}

/// fixtures.go:50-110 `init`.
fn minimal_valid_pods() -> MinimalValidPods {
    let mut m = MinimalValidPods {
        neutral: BTreeMap::new(),
        linux: BTreeMap::new(),
        windows: BTreeMap::new(),
    };
    for l in [Level::Baseline, Level::Restricted] {
        m.neutral.insert(l, PodsByVersion::new());
    }
    m.linux.insert(Level::Restricted, PodsByVersion::new());
    m.windows.insert(Level::Restricted, PodsByVersion::new());

    // This must remain valid for all versions.
    let baseline_1_0 = json!({"spec": {
        "initContainers": [{"name": "initcontainer1", "image": "registry.k8s.io/pause"}],
        "containers": [{"name": "container1", "image": "registry.k8s.io/pause"}]}});
    m.neutral
        .get_mut(&Level::Baseline)
        .unwrap()
        .insert(0, baseline_1_0.clone());

    // 1.0+: baseline + runAsNonRoot=true
    let restricted_1_0 = tweak(&baseline_1_0, |p| {
        set(p, "/spec/securityContext", json!({"runAsNonRoot": true}))
    });
    let mut restricted = |minor: u32, p: &Pod| {
        m.neutral
            .get_mut(&Level::Restricted)
            .unwrap()
            .insert(minor, p.clone());
        m.linux
            .get_mut(&Level::Restricted)
            .unwrap()
            .insert(minor, add_os(p, "linux"));
        m.windows
            .get_mut(&Level::Restricted)
            .unwrap()
            .insert(minor, add_os(p, "windows"));
    };
    restricted(0, &restricted_1_0);

    // 1.8+: allowPrivilegeEscalation=false
    let restricted_1_8 = tweak(&restricted_1_0, |p| {
        set(
            p,
            "/spec/containers/0/securityContext",
            json!({"allowPrivilegeEscalation": false}),
        );
        set(
            p,
            "/spec/initContainers/0/securityContext",
            json!({"allowPrivilegeEscalation": false}),
        );
    });
    restricted(8, &restricted_1_8);

    // 1.19+: seccompProfile.type=RuntimeDefault
    let restricted_1_19 = tweak(&restricted_1_8, |p| {
        unset(p, "/metadata/annotations");
        set(
            p,
            "/spec/securityContext/seccompProfile",
            json!({"type": "RuntimeDefault"}),
        );
    });
    restricted(19, &restricted_1_19);

    // 1.22+: capabilities.drop=["ALL"]
    let restricted_1_22 = tweak(&restricted_1_19, |p| {
        set(
            p,
            "/spec/containers/0/securityContext/capabilities",
            json!({"drop": ["ALL"]}),
        );
        set(
            p,
            "/spec/initContainers/0/securityContext/capabilities",
            json!({"drop": ["ALL"]}),
        );
    });
    restricted(22, &restricted_1_22);

    // 1.25+: OS specific changes. None of the restricted requirements added
    // between 1.0 and 1.25 apply to the pods that are explicitly Windows.
    m.neutral
        .get_mut(&Level::Restricted)
        .unwrap()
        .insert(25, restricted_1_22.clone());
    m.linux
        .get_mut(&Level::Restricted)
        .unwrap()
        .insert(25, add_os(&restricted_1_22, "linux"));
    m.windows
        .get_mut(&Level::Restricted)
        .unwrap()
        .insert(25, add_os(&restricted_1_0, "windows"));
    m
}

/// `GetMinimalValidPod` / `...LinuxPod` / `...WindowsPod`: the newest ladder
/// entry at or below `minor`.
fn get_minimal(
    table: &BTreeMap<Level, PodsByVersion>,
    level: Level,
    minor: u32,
) -> Result<Pod, String> {
    table
        .get(&level)
        .and_then(|v| v.range(..=minor).next_back())
        .map(|(_, p)| p.clone())
        .ok_or_else(|| {
            format!(
                "no valid pod fixture found in specified or older versions for {level}/1.{minor}"
            )
        })
}

struct FixtureGenerator {
    generate_pass: fn(&Pod, Level) -> Vec<Pod>,
    generate_fail: fn(&Pod) -> Vec<Pod>,
}

/// fixtures.go `fixtureKey` (version is the minor).
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct FixtureKey {
    level: Level,
    minor: u32,
    check: &'static str,
}

struct FixtureData {
    pass: Vec<Pod>,
    fail: Vec<Pod>,
}

/// fixtures.go `checkKey`.
fn check_key(key: &FixtureKey) -> Result<(), String> {
    if key.level != Level::Baseline && key.level != Level::Restricted {
        return Err(format!(
            "invalid key, level must be baseline or restricted: {key:?}"
        ));
    }
    if key.check.is_empty() {
        return Err("invalid key, check must not be empty".into());
    }
    if !default_checks().iter().any(|c| c.id == key.check) {
        return Err(format!(
            "invalid key {key:?}, check does not exist at version"
        ));
    }
    Ok(())
}

struct Generators(BTreeMap<FixtureKey, FixtureGenerator>);

impl Generators {
    /// `registerFixtureGenerator`: baseline generators also register to
    /// restricted.
    fn register(&mut self, key: FixtureKey, gen: FixtureGenerator) {
        check_key(&key).unwrap();
        assert!(
            !self.0.contains_key(&key),
            "fixture generator already registered for key {key:?}"
        );
        if key.level == Level::Baseline {
            let restricted = FixtureKey {
                level: Level::Restricted,
                ..key
            };
            assert!(
                !self.0.contains_key(&restricted),
                "fixture generator already registered for restricted version of key {key:?}"
            );
            self.0.insert(
                restricted,
                FixtureGenerator {
                    generate_pass: gen.generate_pass,
                    generate_fail: gen.generate_fail,
                },
            );
        }
        self.0.insert(key, gen);
    }

    /// `getFixtures`: generators registered for v1.x are used for v1.x+ when
    /// no generator is registered for the higher version. `None` when the
    /// check has no ported generator.
    fn get_fixtures(
        &self,
        mins: &MinimalValidPods,
        key: FixtureKey,
    ) -> Result<Option<FixtureData>, String> {
        check_key(&key)?;
        let valid = get_minimal(&mins.neutral, key.level, key.minor)?;
        let mut k = key;
        loop {
            if let Some(g) = self.0.get(&k) {
                let data = FixtureData {
                    pass: (g.generate_pass)(&valid, key.level),
                    fail: (g.generate_fail)(&valid),
                };
                if data.fail.is_empty() {
                    return Err(format!(
                        "generateFail for {key:?} must return at least one pod"
                    ));
                }
                return Ok(Some(data));
            }
            if k.minor == 0 {
                return Ok(None);
            }
            k.minor -= 1;
        }
    }
}

// --------------------------------------------------------- fixtures_*.go

fn registry() -> Generators {
    let mut g = Generators(BTreeMap::new());
    let key = |level, minor, check| FixtureKey {
        level,
        minor,
        check,
    };

    // fixtures_privileged.go
    g.register(
        key(Level::Baseline, 0, "privileged"),
        FixtureGenerator {
            generate_pass: |p, _| {
                let p = ensure_security_context(p);
                vec![tweak(&p, |p| {
                    set(
                        p,
                        "/spec/containers/0/securityContext/privileged",
                        json!(false),
                    );
                    set(
                        p,
                        "/spec/initContainers/0/securityContext/privileged",
                        json!(false),
                    );
                })]
            },
            generate_fail: |p| {
                let p = ensure_security_context(p);
                vec![
                    tweak(&p, |p| {
                        set(
                            p,
                            "/spec/containers/0/securityContext/privileged",
                            json!(true),
                        );
                        unset(
                            p,
                            "/spec/containers/0/securityContext/allowPrivilegeEscalation",
                        );
                    }),
                    tweak(&p, |p| {
                        set(
                            p,
                            "/spec/initContainers/0/securityContext/privileged",
                            json!(true),
                        );
                        unset(
                            p,
                            "/spec/initContainers/0/securityContext/allowPrivilegeEscalation",
                        );
                    }),
                ]
            },
        },
    );

    // fixtures_allowPrivilegeEscalation.go
    g.register(
        key(Level::Restricted, 8, "allowPrivilegeEscalation"),
        FixtureGenerator {
            // minimal valid pod already captures all valid combinations
            generate_pass: |_, _| vec![],
            generate_fail: |p| {
                if p["spec"]["os"]["name"] == "windows" {
                    return vec![];
                }
                vec![
                    // explicit true
                    tweak(p, |p| {
                        set(
                            p,
                            "/spec/containers/0/securityContext/allowPrivilegeEscalation",
                            json!(true),
                        )
                    }),
                    // ensure initContainers are checked
                    tweak(p, |p| {
                        set(
                            p,
                            "/spec/initContainers/0/securityContext/allowPrivilegeEscalation",
                            json!(true),
                        )
                    }),
                    // nil AllowPrivilegeEscalation
                    tweak(p, |p| {
                        unset(
                            p,
                            "/spec/containers/0/securityContext/allowPrivilegeEscalation",
                        )
                    }),
                    // nil security context
                    tweak(p, |p| unset(p, "/spec/containers/0/securityContext")),
                ]
            },
        },
    );

    // fixtures_runAsNonRoot.go
    g.register(
        key(Level::Restricted, 0, "runAsNonRoot"),
        FixtureGenerator {
            generate_pass: |p, _| {
                let p = ensure_security_context(p);
                vec![
                    // set at pod level
                    tweak(&p, |p| {
                        set(p, "/spec/securityContext/runAsNonRoot", json!(true));
                        unset(p, "/spec/containers/0/securityContext/runAsNonRoot");
                        unset(p, "/spec/initContainers/0/securityContext/runAsNonRoot");
                    }),
                    // set on all containers
                    tweak(&p, |p| {
                        unset(p, "/spec/securityContext/runAsNonRoot");
                        set(
                            p,
                            "/spec/containers/0/securityContext/runAsNonRoot",
                            json!(true),
                        );
                        set(
                            p,
                            "/spec/initContainers/0/securityContext/runAsNonRoot",
                            json!(true),
                        );
                    }),
                ]
            },
            generate_fail: |p| {
                let p = ensure_security_context(p);
                vec![
                    // unset everywhere
                    tweak(&p, |p| {
                        unset(p, "/spec/securityContext/runAsNonRoot");
                        unset(p, "/spec/containers/0/securityContext/runAsNonRoot");
                        unset(p, "/spec/initContainers/0/securityContext/runAsNonRoot");
                    }),
                    // explicit false on pod
                    tweak(&p, |p| {
                        set(p, "/spec/securityContext/runAsNonRoot", json!(false))
                    }),
                    // explicit false on containers
                    tweak(&p, |p| {
                        set(
                            p,
                            "/spec/containers/0/securityContext/runAsNonRoot",
                            json!(false),
                        )
                    }),
                    tweak(&p, |p| {
                        set(
                            p,
                            "/spec/initContainers/0/securityContext/runAsNonRoot",
                            json!(false),
                        )
                    }),
                ]
            },
        },
    );

    // fixtures_runAsUser.go
    g.register(
        key(Level::Restricted, 23, "runAsUser"),
        FixtureGenerator {
            generate_pass: |p, _| {
                let p = ensure_security_context(p);
                vec![tweak(&p, |p| {
                    set(p, "/spec/securityContext/runAsUser", json!(1000));
                    set(
                        p,
                        "/spec/containers/0/securityContext/runAsUser",
                        json!(1000),
                    );
                    set(
                        p,
                        "/spec/initContainers/0/securityContext/runAsUser",
                        json!(1000),
                    );
                })]
            },
            generate_fail: |p| {
                let p = ensure_security_context(p);
                vec![
                    // explicit 0 on pod
                    tweak(&p, |p| set(p, "/spec/securityContext/runAsUser", json!(0))),
                    // explicit 0 on containers
                    tweak(&p, |p| {
                        set(p, "/spec/containers/0/securityContext/runAsUser", json!(0))
                    }),
                    tweak(&p, |p| {
                        set(
                            p,
                            "/spec/initContainers/0/securityContext/runAsUser",
                            json!(0),
                        )
                    }),
                ]
            },
        },
    );

    // fixtures_hostNamespaces.go
    g.register(
        key(Level::Baseline, 0, "hostNamespaces"),
        FixtureGenerator {
            // minimal valid pod already captures all valid combinations
            generate_pass: |_, _| vec![],
            generate_fail: |p| {
                vec![
                    tweak(p, |p| set(p, "/spec/hostIPC", json!(true))),
                    tweak(p, |p| set(p, "/spec/hostNetwork", json!(true))),
                    tweak(p, |p| set(p, "/spec/hostPID", json!(true))),
                ]
            },
        },
    );

    // fixtures_appArmorProfile.go
    const APPARMOR: &str = "container.apparmor.security.beta.kubernetes.io/";
    g.register(
        key(Level::Baseline, 0, "appArmorProfile"),
        FixtureGenerator {
            generate_pass: |p, _| {
                let p = ensure_annotation(p);
                vec![
                    // container with runtime/default annotation
                    // container with localhost/foo annotation
                    tweak(&p, |c| {
                        let name = c["spec"]["containers"][0]["name"]
                            .as_str()
                            .unwrap()
                            .to_string();
                        c["metadata"]["annotations"][format!("{APPARMOR}{name}")] =
                            json!("runtime/default");
                        // upstream reads Containers[0] for the init name too.
                        let init_name = c["spec"]["containers"][0]["name"]
                            .as_str()
                            .unwrap()
                            .to_string();
                        c["metadata"]["annotations"][format!("{APPARMOR}{init_name}")] =
                            json!("localhost/foo");
                    }),
                ]
            },
            generate_fail: |p| {
                let p = ensure_annotation(p);
                vec![
                    // container with unconfined annotation
                    tweak(&p, |c| {
                        let name = c["spec"]["containers"][0]["name"]
                            .as_str()
                            .unwrap()
                            .to_string();
                        c["metadata"]["annotations"][format!("{APPARMOR}{name}")] =
                            json!("unconfined");
                    }),
                    // initContainer with unconfined annotation
                    tweak(&p, |c| {
                        let name = c["spec"]["initContainers"][0]["name"]
                            .as_str()
                            .unwrap()
                            .to_string();
                        c["metadata"]["annotations"][format!("{APPARMOR}{name}")] =
                            json!("unconfined");
                    }),
                ]
            },
        },
    );
    g
}

/// Checks that have a `test/fixtures_*.go` generator not yet ported (#2904).
/// Their `<id><n>.yaml` files are not compared and not counted as extra.
const UNPORTED_CHECKS: &[&str] = &[
    "capabilities_baseline",
    "capabilities_restricted",
    "hostPathVolumes",
    "hostPorts",
    "hostProbesAndHostLifecycle",
    "procMount",
    "procMount_restricted",
    "restrictedVolumes",
    "seLinuxOptions",
    "seccompProfile_baseline",
    "seccompProfile_restricted",
    "sysctls",
    "windowsHostProcess",
];

// ------------------------------------------------------------------ run.go

/// run.go:85-93 `checksForLevelAndVersion`.
fn checks_for_level_and_version(checks: &[Check], level: Level, minor: u32) -> Vec<&'static str> {
    checks
        .iter()
        .filter(|c| {
            !Version::major_minor(1, minor).older(&c.versions[0].minimum_version)
                && (level == c.level || level == Level::Restricted)
        })
        .map(|c| c.id)
        .collect()
}

/// run.go:101-163 `computeVersionsToTest` (minors only; major is always 1).
fn compute_versions_to_test(checks: &[Check], mins: &MinimalValidPods) -> Vec<u32> {
    let mut seen = BTreeSet::new();
    for c in checks {
        for v in &c.versions {
            assert_eq!(v.minimum_version.major, 1);
            seen.insert(v.minimum_version.minor);
        }
    }
    assert!(!seen.is_empty(), "no versions defined for checks");
    for table in [&mins.neutral, &mins.linux, &mins.windows] {
        for versions in table.values() {
            seen.extend(versions.keys().copied());
        }
    }
    seen.insert(NEWEST_MINOR_VERSION_TO_TEST);
    seen.into_iter().collect()
}

// -------------------------------------------------------- fixtures_test.go

fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/pod-security-admission")
}

/// What `testFixtureFile` writes: the pod named `name`, as the YAML's value.
fn rendered(pod: &Pod, name: &str) -> Value {
    let mut p = pod.clone();
    p["apiVersion"] = json!("v1");
    p["kind"] = json!("Pod");
    set(&mut p, "/metadata/name", json!(name));
    p
}

/// `testFixtureFile`: compares one in-memory pod with its YAML file and
/// returns the expected file name.
fn test_fixture_file(dir: &Path, name: &str, pod: &Pod, errs: &mut Vec<String>) -> String {
    let file = format!("{name}.yaml");
    match std::fs::read_to_string(dir.join(&file)) {
        Err(e) => errs.push(format!("{}: unreadable: {e}", dir.join(&file).display())),
        Ok(text) => {
            let want: Value = serde_yaml::from_str(&text).unwrap();
            let got = rendered(pod, name);
            if got != want {
                errs.push(format!(
                    "fixture data does not match the test fixture in {}\n got: {got}\nwant: {want}",
                    dir.join(&file).display()
                ));
            }
        }
    }
    file
}

fn yaml_files(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn is_unported_file(file: &str) -> bool {
    let stem = file
        .trim_end_matches(".yaml")
        .trim_end_matches(|c: char| c.is_ascii_digit());
    UNPORTED_CHECKS.iter().any(|id| id.to_lowercase() == stem)
}

/// `TestFixtures`.
#[test]
fn fixtures_match_testdata() {
    let checks = default_checks();
    let mins = minimal_valid_pods();
    let gens = registry();

    let versions = compute_versions_to_test(&checks, &mins);
    let newest_with_policy_changes = *versions.last().unwrap();
    assert!(
        NEWEST_MINOR_VERSION_TO_TEST >= newest_with_policy_changes,
        "fixtures only tested up to {NEWEST_MINOR_VERSION_TO_TEST}, but policy changes exist up to {newest_with_policy_changes}"
    );

    let mut errs = Vec::new();
    let mut compared = 0;
    for level in [Level::Baseline, Level::Restricted] {
        for minor in 0..=NEWEST_MINOR_VERSION_TO_TEST {
            let base = testdata()
                .join(level.to_string())
                .join(format!("v1.{minor}"));
            let (pass_dir, fail_dir) = (base.join("pass"), base.join("fail"));
            let (mut want_pass, mut want_fail) = (BTreeSet::new(), BTreeSet::new());

            // render the minimal valid pod fixture
            let neutral = get_minimal(&mins.neutral, level, minor).unwrap();
            want_pass.insert(test_fixture_file(&pass_dir, "base", &neutral, &mut errs));
            // Don't generate OS specific pods when version < 1.29: the pod
            // os field based restriction is not enabled.
            if level == Level::Restricted && minor >= POD_OS_BASED_RESTRICTION_ENABLED_VERSION {
                let linux = get_minimal(&mins.linux, level, minor).unwrap();
                want_pass.insert(test_fixture_file(
                    &pass_dir,
                    "base_linux",
                    &linux,
                    &mut errs,
                ));
                let windows = get_minimal(&mins.windows, level, minor).unwrap();
                want_pass.insert(test_fixture_file(
                    &pass_dir,
                    "base_windows",
                    &windows,
                    &mut errs,
                ));
            }

            // render check-specific fixtures
            let ids = checks_for_level_and_version(&checks, level, minor);
            assert!(
                !ids.is_empty(),
                "no checks registered for {level}/1.{minor}"
            );
            for id in ids {
                let key = FixtureKey {
                    level,
                    minor,
                    check: id,
                };
                let Some(data) = gens.get_fixtures(&mins, key).unwrap() else {
                    assert!(
                        UNPORTED_CHECKS.contains(&id),
                        "{level}/1.{minor}: check {id} has no fixture generator"
                    );
                    continue;
                };
                for (i, pod) in data.pass.iter().enumerate() {
                    let n = format!("{}{i}", id.to_lowercase());
                    want_pass.insert(test_fixture_file(&pass_dir, &n, pod, &mut errs));
                    compared += 1;
                }
                for (i, pod) in data.fail.iter().enumerate() {
                    let n = format!("{}{i}", id.to_lowercase());
                    want_fail.insert(test_fixture_file(&fail_dir, &n, pod, &mut errs));
                    compared += 1;
                }
            }

            for (dir, want) in [(&pass_dir, &want_pass), (&fail_dir, &want_fail)] {
                let actual: BTreeSet<String> = yaml_files(dir)
                    .into_iter()
                    .filter(|f| !is_unported_file(f))
                    .collect();
                let extra: Vec<_> = actual.difference(want).collect();
                if !extra.is_empty() {
                    errs.push(format!(
                        "unexpected extra fixtures in {}: {extra:?}",
                        dir.display()
                    ));
                }
            }
        }
    }
    assert!(
        errs.is_empty(),
        "{} problems:\n{}",
        errs.len(),
        errs.join("\n")
    );
    assert!(compared > 100, "only {compared} fixtures compared");
}

/// fixtures.go `getFixtures` falls back to older generators, and
/// `GetMinimalValid*` to older ladder entries.
#[test]
fn version_fallback_matches_upstream() {
    let mins = minimal_valid_pods();
    assert_eq!(
        get_minimal(&mins.neutral, Level::Restricted, 24).unwrap(),
        get_minimal(&mins.neutral, Level::Restricted, 22).unwrap()
    );
    assert!(get_minimal(&mins.linux, Level::Baseline, 10).is_err());
    let g = registry();
    // runAsUser is registered at 1.23: absent below, used above.
    let at = |minor| {
        g.get_fixtures(
            &mins,
            FixtureKey {
                level: Level::Restricted,
                minor,
                check: "runAsUser",
            },
        )
        .unwrap()
        .is_some()
    };
    assert!(!at(22));
    assert!(at(23));
    assert!(at(35));
    // baseline generators are also registered for restricted.
    assert!(g
        .get_fixtures(
            &mins,
            FixtureKey {
                level: Level::Restricted,
                minor: 3,
                check: "privileged"
            }
        )
        .unwrap()
        .is_some());
    assert!(check_key(&FixtureKey {
        level: Level::Baseline,
        minor: 0,
        check: "nope"
    })
    .is_err());
}
