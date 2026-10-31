//! Unit tests moved from `src/admission/pod_security_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::admission::pod_security_api::{Level, LevelVersion, Version};
    pub use rusternetes_common::resources::pod::{
        ContainerPort, EphemeralContainer, Lifecycle, LifecycleHandler, PodSpec, Probe,
        SecurityContext, Volume,
    };
    pub use rusternetes_common::types::ObjectMeta;
    pub use std::collections::{BTreeMap, BTreeSet, HashMap};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_policy::*;
    use rusternetes_common::resources::pod::Pod;

    fn lv(level: Level, v: &str) -> LevelVersion {
        let (version, err) = rusternetes_api_server::admission::pod_security_api::parse_version(v);
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
                    let ver =
                        rusternetes_api_server::admission::pod_security_api::parse_version(v).0;
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

#[path = "pod_security_policy_cases.rs"]
mod cases {
    //! Verbatim case-by-case ports of upstream pod-security-admission per-check
    //! tables (staging/src/k8s.io/pod-security-admission/policy/):
    //! check_appArmorProfile_test.go, check_seccompProfile_restricted_test.go,
    //! check_seccompProfile_baseline_test.go, check_seLinuxOptions_test.go,
    //! check_runAsUser_test.go, check_runAsNonRoot_test.go,
    //! check_allowPrivilegeEscalation_test.go, check_hostNamespaces_test.go,
    //! check_privileged_test.go, check_hostPathVolumes_test.go,
    //! check_restrictedVolumes_test.go, check_windowsHostProcess_test.go,
    //! check_capabilities_baseline_test.go.
    //! Child module of `pod_security_policy` so it can reach the private checks.
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_policy::*;
    use rusternetes_common::resources::pod::Pod;
    use serde_json::{json, Value};

    fn mk(annotations: Value, spec: Value) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "annotations": annotations}, "spec": spec,
        }))
        .unwrap()
    }

    fn run(f: fn(&ObjectMeta, &PodSpec) -> CheckResult, p: &Pod) -> CheckResult {
        f(&p.metadata, p.spec.as_ref().unwrap())
    }

    #[track_caller]
    fn forbidden(r: CheckResult, reason: &str, detail: &str) {
        assert!(!r.allowed, "expected disallowed");
        assert_eq!(r.forbidden_reason, reason);
        assert_eq!(r.forbidden_detail, detail);
    }

    const AA: &str = "container.apparmor.security.beta.kubernetes.io/";

    /// TestCheckAppArmor_Allowed
    #[test]
    fn app_armor_allowed() {
        let cases = [
            // container with default AppArmor + extra annotations
            mk(
                json!({format!("{AA}test"): "runtime/default", "env": "prod"}),
                json!({}),
            ),
            // container with local AppArmor + extra annotations
            mk(
                json!({format!("{AA}test"): "localhost/sec-profile01", "env": "dev"}),
                json!({}),
            ),
            // container with no AppArmor annotations
            mk(json!({"env": "dev"}), json!({})),
            // container with no annotations
            mk(json!({}), json!({})),
            // pod with runtime default
            mk(
                json!({}),
                json!({"securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}}}),
            ),
            // container with localhost profile (sic: upstream sets RuntimeDefault)
            mk(
                json!({}),
                json!({"containers": [{"name": "foo", "securityContext":
                    {"appArmorProfile": {"type": "RuntimeDefault"}}}]}),
            ),
        ];
        for (i, p) in cases.iter().enumerate() {
            let r = run(app_armor_profile_1_0, p);
            assert!(r.allowed, "case {i} should be allowed: {r:?}");
        }
    }

    /// TestCheckAppArmor_Forbidden
    #[test]
    fn app_armor_forbidden() {
        // unconfined pod
        forbidden(
            run(
                app_armor_profile_1_0,
                &mk(
                    json!({}),
                    json!({"securityContext": {"appArmorProfile": {"type": "Unconfined"}}}),
                ),
            ),
            "forbidden AppArmor profile",
            r#"pod must not set AppArmor profile type to "Unconfined""#,
        );
        // unconfined container
        forbidden(
            run(
                app_armor_profile_1_0,
                &mk(
                    json!({}),
                    json!({
                        "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                        "containers": [{"name": "foo", "securityContext":
                            {"appArmorProfile": {"type": "Unconfined"}}}]}),
                ),
            ),
            "forbidden AppArmor profile",
            r#"container "foo" must not set AppArmor profile type to "Unconfined""#,
        );
        // unconfined init container
        forbidden(
            run(
                app_armor_profile_1_0,
                &mk(
                    json!({}),
                    json!({
                        "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                        "containers": [{"name": "foo"}],
                        "initContainers": [{"name": "bar", "securityContext":
                            {"appArmorProfile": {"type": "Unconfined"}}}]}),
                ),
            ),
            "forbidden AppArmor profile",
            r#"container "bar" must not set AppArmor profile type to "Unconfined""#,
        );
        // multiple containers
        forbidden(
            run(
                app_armor_profile_1_0,
                &mk(
                    json!({
                        AA: "bogus",
                        format!("{AA}a"): "",
                        format!("{AA}b"): "runtime/default",
                        format!("{AA}c"): "localhost/",
                        format!("{AA}d"): "localhost/foo",
                        format!("{AA}e"): "unconfined",
                        format!("{AA}f"): "unknown",
                    }),
                    json!({}),
                ),
            ),
            "forbidden AppArmor profiles",
            &format!(
                "annotations must not set AppArmor profile type to {}",
                [
                    format!(r#""{AA}="bogus"""#),
                    format!(r#""{AA}e="unconfined"""#),
                    format!(r#""{AA}f="unknown"""#),
                ]
                .join(", ")
            ),
        );
    }

    fn sc(profile_json: Value) -> Value {
        json!({"securityContext": {"seccompProfile": profile_json}})
    }

    fn containers_invalid_spec() -> Value {
        let c = |n: &str, t: &str| json!({"name": n, "securityContext": {"seccompProfile": {"type": t}}});
        json!({
            "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [
                {"name": "a"},
                {"name": "b", "securityContext": {}},
                c("c", "Unconfined"),
                c("d", "Unconfined"),
                c("e", "RuntimeDefault"),
                c("f", "RuntimeDefault"),
            ],
        })
    }

    fn fallthrough_spec() -> Value {
        json!({"containers": [
            {"name": "a"},
            {"name": "b", "securityContext": {}},
            {"name": "d", "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}},
            {"name": "e", "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}},
        ]})
    }

    const NO_EXPLICIT: &str = r#"pod or container "a" must set securityContext.seccompProfile.type to "RuntimeDefault" or "Localhost""#;
    const FALLTHROUGH: &str = r#"pod or containers "a", "b" must set securityContext.seccompProfile.type to "RuntimeDefault" or "Localhost""#;
    const POD_UNCONFINED: &str =
        r#"pod must not set securityContext.seccompProfile.type to "Unconfined""#;
    const CONTAINERS_UNCONFINED: &str =
        r#"containers "c", "d" must not set securityContext.seccompProfile.type to "Unconfined""#;

    /// TestSeccompProfileRestricted_1_25
    #[test]
    fn seccomp_restricted_1_25() {
        let run_spec = |spec: Value| run(seccomp_profile_restricted_1_25, &mk(json!({}), spec));
        // no explicit seccomp
        forbidden(
            run_spec(json!({"containers": [{"name": "a"}]})),
            "seccompProfile",
            NO_EXPLICIT,
        );
        // no explicit seccomp, windows Pod
        let r = run_spec(json!({"os": {"name": "windows"}, "containers": [{"name": "a"}]}));
        assert!(r.allowed, "{r:?}");
        // no explicit seccomp, linux pod
        forbidden(
            run_spec(json!({"os": {"name": "linux"}, "containers": [{"name": "a"}]})),
            "seccompProfile",
            NO_EXPLICIT,
        );
        // pod seccomp invalid
        let mut s = sc(json!({"type": "Unconfined"}));
        s["containers"] = json!([{"name": "a"}]);
        forbidden(run_spec(s), "seccompProfile", POD_UNCONFINED);
        // containers seccomp invalid
        forbidden(
            run_spec(containers_invalid_spec()),
            "seccompProfile",
            CONTAINERS_UNCONFINED,
        );
        // pod nil, container fallthrough
        forbidden(run_spec(fallthrough_spec()), "seccompProfile", FALLTHROUGH);
    }

    /// TestSeccompProfileRestricted_1_19
    #[test]
    fn seccomp_restricted_1_19() {
        let run_spec = |spec: Value| run(seccomp_profile_restricted_1_19, &mk(json!({}), spec));
        forbidden(
            run_spec(json!({"containers": [{"name": "a"}]})),
            "seccompProfile",
            NO_EXPLICIT,
        );
        let mut s = sc(json!({"type": "Unconfined"}));
        s["containers"] = json!([{"name": "a"}]);
        forbidden(run_spec(s), "seccompProfile", POD_UNCONFINED);
        forbidden(
            run_spec(containers_invalid_spec()),
            "seccompProfile",
            CONTAINERS_UNCONFINED,
        );
        forbidden(run_spec(fallthrough_spec()), "seccompProfile", FALLTHROUGH);
    }

    const SA: &str = "seccomp.security.alpha.kubernetes.io/pod";
    const SCP: &str = "container.seccomp.security.alpha.kubernetes.io/";

    /// TestSeccompProfileBaseline_1_0
    #[test]
    fn seccomp_baseline_1_0() {
        let f = seccomp_profile_baseline_1_0;
        // pod seccomp invalid
        forbidden(
            run(f, &mk(json!({SA: "unconfined"}), json!({}))),
            "seccompProfile",
            r#"forbidden annotation seccomp.security.alpha.kubernetes.io/pod="unconfined""#,
        );
        let abc = json!({"containers": [{"name": "a"}, {"name": "b"}, {"name": "c"}]});
        // containers seccomp invalid
        forbidden(
            run(
                f,
                &mk(
                    json!({format!("{SCP}a"): "unconfined", format!("{SCP}b"): "unknown"}),
                    abc.clone(),
                ),
            ),
            "seccompProfile",
            &format!(r#"forbidden annotations {SCP}a="unconfined", {SCP}b="unknown""#),
        );
        // pod and containers seccomp invalid
        forbidden(
            run(
                f,
                &mk(
                    json!({SA: "unconfined", format!("{SCP}a"): "unconfined", format!("{SCP}b"): "unknown"}),
                    abc,
                ),
            ),
            "seccompProfile",
            &format!(
                r#"forbidden annotations {SCP}a="unconfined", {SCP}b="unknown", {SA}="unconfined""#
            ),
        );
    }

    /// TestSeccompProfileBaseline_1_19
    #[test]
    fn seccomp_baseline_1_19() {
        let run_spec = |spec: Value| run(seccomp_profile_baseline_1_19, &mk(json!({}), spec));
        // pod seccomp invalid
        let mut s = sc(json!({"type": "Unconfined"}));
        s["containers"] = json!([{"name": "a"}]);
        forbidden(run_spec(s), "seccompProfile", POD_UNCONFINED);
        // containers seccomp invalid
        forbidden(
            run_spec(containers_invalid_spec()),
            "seccompProfile",
            CONTAINERS_UNCONFINED,
        );
        // pod and containers seccomp invalid
        let mut s = containers_invalid_spec();
        s["securityContext"] = json!({"seccompProfile": {"type": "Unconfined"}});
        forbidden(
            run_spec(s),
            "seccompProfile",
            r#"pod and containers "c", "d" must not set securityContext.seccompProfile.type to "Unconfined""#,
        );
    }

    fn selinux_c(name: &str, opts: Value) -> Value {
        json!({"name": name, "securityContext": {"seLinuxOptions": opts}})
    }

    fn selinux_ok_containers() -> Vec<Value> {
        [
            "container_t",
            "container_init_t",
            "container_kvm_t",
            "container_engine_t",
        ]
        .iter()
        .zip(["a", "b", "c", "d"])
        .map(|(t, n)| selinux_c(n, json!({"type": t})))
        .collect()
    }

    /// TestSELinuxOptions (checks seLinuxOptions1_31)
    #[test]
    fn se_linux_options() {
        let run_spec = |spec: Value| run(se_linux_options_1_31, &mk(json!({}), spec));
        let pod_bad = json!({"seLinuxOptions": {"type": "foo", "user": "bar", "role": "baz"}});
        let bad_efg = || {
            vec![
                selinux_c("e", json!({"type": "bar"})),
                selinux_c("f", json!({"user": "bar"})),
                selinux_c("g", json!({"role": "baz"})),
            ]
        };
        // invalid pod and containers
        let mut cs = selinux_ok_containers();
        cs.extend(bad_efg());
        forbidden(
            run_spec(json!({"securityContext": pod_bad, "containers": cs})),
            "seLinuxOptions",
            r#"pod and containers "e", "f", "g" set forbidden securityContext.seLinuxOptions: types "bar", "foo"; user may not be set; role may not be set"#,
        );
        // invalid pod
        forbidden(
            run_spec(json!({"securityContext": pod_bad, "containers": selinux_ok_containers()})),
            "seLinuxOptions",
            r#"pod set forbidden securityContext.seLinuxOptions: type "foo"; user may not be set; role may not be set"#,
        );
        // invalid containers
        let mut cs = selinux_ok_containers();
        cs.extend(bad_efg());
        forbidden(
            run_spec(json!({"securityContext": {"seLinuxOptions": {}}, "containers": cs})),
            "seLinuxOptions",
            r#"containers "e", "f", "g" set forbidden securityContext.seLinuxOptions: type "bar"; user may not be set; role may not be set"#,
        );
        // bad type / user / role
        for (opts, detail) in [
            (json!({"type": "bad"}), r#"type "bad""#),
            (json!({"user": "bad"}), "user may not be set"),
            (json!({"role": "bad"}), "role may not be set"),
        ] {
            forbidden(
                run_spec(json!({"securityContext": {"seLinuxOptions": opts}})),
                "seLinuxOptions",
                &format!("pod set forbidden securityContext.seLinuxOptions: {detail}"),
            );
        }
    }

    /// Run `f` against a pod built from `spec` and assert reason/detail, or
    /// allowed when `want` is `None` (the tables' expectAllowed).
    #[track_caller]
    fn table(f: fn(&ObjectMeta, &PodSpec) -> CheckResult, spec: Value, want: Option<(&str, &str)>) {
        let r = run(f, &mk(json!({}), spec));
        match want {
            None => assert!(r.allowed, "expected allowed: {r:?}"),
            Some((reason, detail)) => forbidden(r, reason, detail),
        }
    }

    fn named(n: &str, sc: Option<Value>) -> Value {
        match sc {
            Some(sc) => json!({"name": n, "securityContext": sc}),
            None => json!({"name": n}),
        }
    }

    /// check_runAsUser_test.go TestRunAsUser (runAsUser1_35).
    #[test]
    fn run_as_user_table() {
        let f = run_as_user_1_35;
        // pod runAsUser=0
        table(
            f,
            json!({"securityContext": {"runAsUser": 0}, "containers": [named("a", None)]}),
            Some(("runAsUser=0", "pod must not set runAsUser=0")),
        );
        // pod runAsUser=non-zero
        table(
            f,
            json!({"securityContext": {"runAsUser": 1000}, "containers": [named("a", None)]}),
            None,
        );
        // pod runAsUser=nil
        table(
            f,
            json!({"securityContext": {}, "containers": [named("a", None)]}),
            None,
        );
        // containers runAsUser=0
        table(
            f,
            json!({"securityContext": {"runAsUser": 1000}, "containers": [
                named("a", None),
                named("b", Some(json!({}))),
                named("c", Some(json!({"runAsUser": 0}))),
                named("d", Some(json!({"runAsUser": 0}))),
                named("e", Some(json!({"runAsUser": 1}))),
                named("f", Some(json!({"runAsUser": 1}))),
            ]}),
            Some((
                "runAsUser=0",
                r#"containers "c", "d" must not set runAsUser=0"#,
            )),
        );
        // containers runAsUser=non-zero
        table(
            f,
            json!({"containers": [
                named("c", Some(json!({"runAsUser": 1}))),
                named("d", Some(json!({"runAsUser": 2}))),
                named("e", Some(json!({"runAsUser": 3}))),
                named("f", Some(json!({"runAsUser": 4}))),
            ]}),
            None,
        );
        // host users false allowed
        table(f, json!({"hostUsers": false}), None);
    }

    /// check_runAsNonRoot_test.go TestRunAsNonRoot (runAsNonRoot1_35).
    #[test]
    fn run_as_non_root_table() {
        let f = run_as_non_root_1_35;
        let reason = "runAsNonRoot != true";
        // no explicit runAsNonRoot
        table(
            f,
            json!({"containers": [named("a", None)]}),
            Some((
                reason,
                r#"pod or container "a" must set securityContext.runAsNonRoot=true"#,
            )),
        );
        // pod runAsNonRoot=false
        table(
            f,
            json!({"securityContext": {"runAsNonRoot": false}, "containers": [named("a", None)]}),
            Some((
                reason,
                "pod must not set securityContext.runAsNonRoot=false",
            )),
        );
        // containers runAsNonRoot=false
        table(
            f,
            json!({"securityContext": {"runAsNonRoot": true}, "containers": [
                named("a", None),
                named("b", Some(json!({}))),
                named("c", Some(json!({"runAsNonRoot": false}))),
                named("d", Some(json!({"runAsNonRoot": false}))),
                named("e", Some(json!({"runAsNonRoot": true}))),
                named("f", Some(json!({"runAsNonRoot": true}))),
            ]}),
            Some((
                reason,
                r#"containers "c", "d" must not set securityContext.runAsNonRoot=false"#,
            )),
        );
        // pod nil, container fallthrough
        table(
            f,
            json!({"containers": [
                named("a", None),
                named("b", Some(json!({}))),
                named("d", Some(json!({"runAsNonRoot": true}))),
                named("e", Some(json!({"runAsNonRoot": true}))),
            ]}),
            Some((
                reason,
                r#"pod or containers "a", "b" must set securityContext.runAsNonRoot=true"#,
            )),
        );
        // host users false allowed
        table(f, json!({"hostUsers": false}), None);
    }

    /// check_allowPrivilegeEscalation_test.go TestAllowPrivilegeEscalation_1_25
    /// and _1_8.
    #[test]
    fn allow_privilege_escalation_table() {
        let reason = "allowPrivilegeEscalation != false";
        let multi = json!({"containers": [
            named("a", None),
            named("b", Some(json!({}))),
            named("c", Some(json!({"allowPrivilegeEscalation": true}))),
            named("d", Some(json!({"allowPrivilegeEscalation": false}))),
        ]});
        let detail =
            r#"containers "a", "b", "c" must set securityContext.allowPrivilegeEscalation=false"#;
        // 1_25: multiple containers
        table(
            allow_privilege_escalation_1_25,
            multi.clone(),
            Some((reason, detail)),
        );
        // 1_25: windows pod, admit without checking privilegeEscalation
        table(
            allow_privilege_escalation_1_25,
            json!({"os": {"name": "windows"}, "containers": [named("a", None)]}),
            None,
        );
        // 1_25: linux pod, reject if security context is not set
        table(
            allow_privilege_escalation_1_25,
            json!({"os": {"name": "linux"}, "containers": [named("a", None)]}),
            Some((
                reason,
                r#"container "a" must set securityContext.allowPrivilegeEscalation=false"#,
            )),
        );
        // 1_8: multiple containers
        table(
            allow_privilege_escalation_1_8,
            multi,
            Some((reason, detail)),
        );
    }

    fn caps_containers() -> Value {
        json!({"containers": [
            {"name": "a", "securityContext": {"capabilities": {"add": ["FOO", "BAR"]}}},
            {"name": "b", "securityContext": {"capabilities": {"add": ["BAR", "BAZ"]}}},
            {"name": "c", "securityContext": {"capabilities":
                {"add": ["NET_BIND_SERVICE", "CHOWN"], "drop": ["ALL", "FOO"]}}},
        ]})
    }

    const CAPS_DETAIL: &str = "containers \"a\", \"b\" must set securityContext.capabilities.drop=[\"ALL\"]; containers \"a\", \"b\", \"c\" must not include \"BAR\", \"BAZ\", \"CHOWN\", \"FOO\" in securityContext.capabilities.add";

    /// check_capabilities_restricted_test.go TestCapabilitiesRestricted_1_25.
    #[test]
    fn capabilities_restricted_1_25_table() {
        table(
            capabilities_restricted_1_25,
            caps_containers(),
            Some(("unrestricted capabilities", CAPS_DETAIL)),
        );
        // windows pod, admit without checking capabilities
        table(
            capabilities_restricted_1_25,
            json!({"os": {"name": "windows"}, "containers": [{"name": "a"}]}),
            None,
        );
        // linux pod, reject if security context is not set
        table(
            capabilities_restricted_1_25,
            json!({"os": {"name": "linux"}, "containers": [{"name": "a"}]}),
            Some((
                "unrestricted capabilities",
                "container \"a\" must set securityContext.capabilities.drop=[\"ALL\"]",
            )),
        );
    }

    /// check_capabilities_restricted_test.go TestCapabilitiesRestricted_1_22.
    #[test]
    fn capabilities_restricted_1_22_table() {
        table(
            capabilities_restricted_1_22,
            caps_containers(),
            Some(("unrestricted capabilities", CAPS_DETAIL)),
        );
    }

    /// check_hostPorts_test.go TestHostPort.
    #[test]
    fn host_ports_table() {
        table(
            host_ports_1_0,
            json!({"containers": [
                {"name": "a", "ports": [{"hostPort": 0}]},
                {"name": "b", "ports": [{"hostPort": 0}, {"hostPort": 20}]},
            ]}),
            Some(("hostPort", "container \"b\" uses hostPort 20")),
        );
        table(
            host_ports_1_0,
            json!({"containers": [
                {"name": "a", "ports": [{"hostPort": 0}]},
                {"name": "b", "ports": [{"hostPort": 0}, {"hostPort": 10}, {"hostPort": 20}]},
                {"name": "c", "ports": [{"hostPort": 0}, {"hostPort": 10}, {"hostPort": 30}]},
            ]}),
            Some((
                "hostPort",
                "containers \"b\", \"c\" use hostPorts 10, 20, 30",
            )),
        );
    }

    fn proc_mount_containers() -> Vec<Value> {
        vec![
            json!({"name": "a"}),
            json!({"name": "b", "securityContext": {}}),
            json!({"name": "c", "securityContext": {"procMount": "Default"}}),
            json!({"name": "d", "securityContext": {"procMount": "Unmasked"}}),
            json!({"name": "e", "securityContext": {"procMount": "other"}}),
        ]
    }

    const PROC_MOUNT_DETAIL: &str =
        "containers \"d\", \"e\" must not set securityContext.procMount to \"Unmasked\", \"other\"";

    /// check_procMount_baseline_test.go TestProcMountBaseline
    /// (procMount1_35baseline).
    #[test]
    fn proc_mount_baseline_table() {
        table(
            proc_mount_1_35_baseline,
            json!({"containers": proc_mount_containers(), "hostUsers": true}),
            Some(("procMount", PROC_MOUNT_DETAIL)),
        );
        // procMount with userns
        table(
            proc_mount_1_35_baseline,
            json!({"containers": proc_mount_containers(), "hostUsers": false}),
            None,
        );
    }

    /// check_procMount_restricted_test.go TestProcMountRestricted: forbidden for
    /// both hostUsers values.
    #[test]
    fn proc_mount_restricted_table() {
        for userns in [true, false] {
            table(
                proc_mount_1_0,
                json!({"containers": proc_mount_containers(), "hostUsers": userns}),
                Some(("procMount", PROC_MOUNT_DETAIL)),
            );
        }
    }

    /// check_hostNamespaces_test.go TestHostNamespaces (hostNamespaces_1_0).
    #[test]
    fn host_namespaces_table() {
        table(
            host_namespaces_1_0,
            json!({"hostNetwork": true, "hostIPC": true, "hostPID": true, "containers": []}),
            Some((
                "host namespaces",
                "hostNetwork=true, hostPID=true, hostIPC=true",
            )),
        );
    }

    /// check_privileged_test.go TestPrivileged (privileged_1_0).
    #[test]
    fn privileged_table() {
        table(
            privileged_1_0,
            json!({"containers": [
                named("a", None),
                named("b", Some(json!({}))),
                named("c", Some(json!({"privileged": false}))),
                named("d", Some(json!({"privileged": true}))),
                named("e", Some(json!({"privileged": true}))),
            ]}),
            Some((
                "privileged",
                r#"containers "d", "e" must not set securityContext.privileged=true"#,
            )),
        );
    }

    /// check_hostPathVolumes_test.go TestHostPathVolumes (hostPathVolumes_1_0).
    #[test]
    fn host_path_volumes_table() {
        table(
            host_path_volumes_1_0,
            json!({"containers": [], "volumes": [
                {"name": "a", "hostPath": {"path": ""}},
                {"name": "b", "hostPath": {"path": ""}},
                {"name": "c", "emptyDir": {}},
            ]}),
            Some(("hostPath volumes", r#"volumes "a", "b""#)),
        );
    }

    /// check_restrictedVolumes_test.go TestRestrictedVolumes
    /// (restrictedVolumes_1_0): every volume source of the table, in order.
    #[test]
    fn restricted_volumes_table() {
        let allowed: [(&str, Value); 9] = [
            ("emptyDir", json!({})),
            ("secret", json!({})),
            ("persistentVolumeClaim", json!({"claimName": ""})),
            ("downwardAPI", json!({})),
            ("configMap", json!({})),
            ("projected", json!({})),
            ("csi", json!({"driver": ""})),
            ("ephemeral", json!({})),
            ("image", json!({})),
        ];
        let restricted: [(&str, Value); 21] = [
            ("hostPath", json!({"path": ""})),
            ("gcePersistentDisk", json!({"pdName": ""})),
            ("awsElasticBlockStore", json!({"volumeID": ""})),
            ("gitRepo", json!({"repository": ""})),
            ("nfs", json!({"server": "", "path": ""})),
            ("iscsi", json!({"targetPortal": "", "iqn": "", "lun": 0})),
            ("glusterfs", json!({"endpoints": "", "path": ""})),
            ("rbd", json!({"monitors": [], "image": ""})),
            ("flexVolume", json!({"driver": ""})),
            ("cinder", json!({"volumeID": ""})),
            ("cephfs", json!({"monitors": []})),
            ("flocker", json!({})),
            ("fc", json!({})),
            ("azureFile", json!({"secretName": "", "shareName": ""})),
            ("vsphereVolume", json!({"volumePath": ""})),
            ("quobyte", json!({"registry": "", "volume": ""})),
            ("azureDisk", json!({"diskName": "", "diskURI": ""})),
            ("photonPersistentDisk", json!({"pdID": ""})),
            ("portworxVolume", json!({"volumeID": ""})),
            (
                "scaleIO",
                json!({"gateway": "", "system": "", "secretRef": {}}),
            ),
            ("storageos", json!({})),
        ];
        let mut volumes = Vec::new();
        for (i, (k, v)) in allowed.iter().enumerate() {
            volumes.push(json!({"name": format!("a{}", i + 1), *k: v}));
        }
        for (i, (k, v)) in restricted.iter().enumerate() {
            volumes.push(json!({"name": format!("b{}", i + 1), *k: v}));
        }
        volumes.push(json!({"name": "c1"}));
        table(
            restricted_volumes_1_0,
            json!({"containers": [], "volumes": volumes}),
            Some((
                "restricted volume types",
                concat!(
                    r#"volumes "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "b10", "b11", "b12", "b13", "b14", "b15", "b16", "b17", "b18", "b19", "b20", "b21", "c1""#,
                    " use restricted volume types ",
                    r#""awsElasticBlockStore", "azureDisk", "azureFile", "cephfs", "cinder", "fc", "flexVolume", "flocker", "gcePersistentDisk", "gitRepo", "glusterfs", "#,
                    r#""hostPath", "iscsi", "nfs", "photonPersistentDisk", "portworxVolume", "quobyte", "rbd", "scaleIO", "storageos", "unknown", "vsphereVolume""#
                ),
            )),
        );
    }

    /// check_windowsHostProcess_test.go TestWindowsHostProcess
    /// (windowsHostProcess_1_0).
    #[test]
    fn windows_host_process_table() {
        let wo = |hp: Option<bool>| match hp {
            Some(b) => json!({"windowsOptions": {"hostProcess": b}}),
            None => json!({"windowsOptions": {}}),
        };
        table(
            windows_host_process_1_0,
            json!({
            "securityContext": wo(Some(true)),
            "containers": [
                named("a", None),
                named("b", Some(json!({}))),
                named("c", Some(wo(None))),
                named("d", Some(wo(Some(false)))),
                named("e", Some(wo(Some(true)))),
                named("f", Some(wo(Some(true)))),
            ]}),
            Some((
                "hostProcess",
                r#"pod and containers "e", "f" must not set securityContext.windowsOptions.hostProcess=true"#,
            )),
        );
    }

    /// check_capabilities_baseline_test.go TestCapabilitiesBaseline
    /// (capabilitiesBaseline_1_0).
    #[test]
    fn capabilities_baseline_table() {
        let add = |names: [&str; 2]| Some(json!({"capabilities": {"add": names}}));
        table(
            capabilities_baseline_1_0,
            json!({"containers": [
                named("a", add(["FOO", "BAR"])),
                named("b", add(["BAR", "BAZ"])),
            ]}),
            Some((
                "non-default capabilities",
                r#"containers "a", "b" must not include "BAR", "BAZ", "FOO" in securityContext.capabilities.add"#,
            )),
        );
    }
}

#[path = "pod_security_fixtures.rs"]
mod fixtures {
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
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_api::{Level, LevelVersion, Version};
    use rusternetes_api_server::admission::pod_security_policy::*;
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
            rusternetes_api_server::handlers::defaults::apply_pod_spec_defaults(
                pod.spec.as_mut().unwrap(),
            );
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
    /// fixtures_procMount.go:93) means API validation rejects the pod outright
    /// (`ValidateSecurityContext`, validation.go:8387-8389), so those scenarios
    /// are replayed against `validate_deployment` instead of PodSecurity.
    #[tokio::test]
    async fn pod_security_fixtures_controller_scenarios() {
        use rusternetes_common::resources::Namespace;
        use rusternetes_storage::{build_key, MemoryStorage, Storage};
        use std::sync::Arc;

        let psa = rusternetes_api_server::admission::PodSecurityAdmission::new();
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
                        // failRequiresError: the Deployment itself is rejected.
                        let mut d = deployment(&pod);
                        d["metadata"]["namespace"] = ns.clone().into();
                        d["spec"]["template"]["metadata"]["labels"] =
                            serde_json::json!({"test": "true"});
                        let d: rusternetes_common::resources::Deployment =
                            serde_json::from_value(d).unwrap();
                        let errs = rusternetes_common::validation::apps::validate_deployment(&d);
                        assert!(
                            errs.iter()
                                .any(|e| e.field.ends_with("securityContext.procMount")),
                            "{ns} fail/{name}: expected a procMount validation error, got {errs:?}"
                        );
                        controllers += 1;
                        continue;
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
}

#[path = "pod_security_fixture_gen.rs"]
mod fixture_gen {
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
    //! Every `test/fixtures_*.go` generator is ported (#2904).
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_api::{Level, Version};
    use rusternetes_api_server::admission::pod_security_policy::*;
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

    /// fixtures_capabilities_baseline.go `ensureCapabilities`.
    fn ensure_capabilities(p: &Pod) -> Pod {
        let mut p = ensure_security_context(p);
        for list in ["containers", "initContainers"] {
            let n = p["spec"][list].as_array().map_or(0, |a| a.len());
            for i in 0..n {
                let ptr = format!("/spec/{list}/{i}/securityContext/capabilities");
                if p.pointer(&ptr).is_none() {
                    set(&mut p, &ptr, json!({}));
                }
            }
        }
        p
    }

    /// helpers.go `ensureSELinuxOptions`.
    fn ensure_se_linux_options(p: &Pod) -> Pod {
        let mut p = ensure_security_context(p);
        let mut ptrs = vec!["/spec/securityContext/seLinuxOptions".to_string()];
        for list in ["containers", "initContainers"] {
            let n = p["spec"][list].as_array().map_or(0, |a| a.len());
            for i in 0..n {
                ptrs.push(format!("/spec/{list}/{i}/securityContext/seLinuxOptions"));
            }
        }
        for ptr in ptrs {
            if p.pointer(&ptr).is_none() {
                set(&mut p, &ptr, json!({}));
            }
        }
        p
    }

    /// fixtures_sysctls.go: `if p.Spec.SecurityContext == nil { ... = &PodSecurityContext{} }`.
    fn ensure_pod_security_context(p: &Pod) -> Pod {
        tweak(p, |p| {
            if p.pointer("/spec/securityContext").is_none() {
                set(p, "/spec/securityContext", json!({}));
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

        // fixtures_capabilities_baseline.go
        const ALLOWED_CAPS: &[&str] = &[
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
        g.register(
            key(Level::Baseline, 0, "capabilities_baseline"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    // don't generate fixtures if minimal valid pod drops ALL
                    if let Some(drop) = p
                        .pointer("/spec/containers/0/securityContext/capabilities/drop")
                        .and_then(Value::as_array)
                    {
                        if drop.iter().any(|c| c == "ALL") {
                            return vec![];
                        }
                    }
                    let p = ensure_capabilities(p);
                    vec![
                        // all allowed capabilities
                        tweak(&p, |p| {
                            set(
                                p,
                                "/spec/containers/0/securityContext/capabilities/add",
                                json!(ALLOWED_CAPS),
                            );
                            set(
                                p,
                                "/spec/initContainers/0/securityContext/capabilities/add",
                                json!(ALLOWED_CAPS),
                            );
                        }),
                    ]
                },
                generate_fail: |p| {
                    let p = ensure_capabilities(p);
                    let add =
                        |ptr: &'static str, v: &'static str| tweak(&p, |p| set(p, ptr, json!([v])));
                    vec![
                        // NET_RAW
                        add(
                            "/spec/containers/0/securityContext/capabilities/add",
                            "NET_RAW",
                        ),
                        // ensure init container is enforced
                        add(
                            "/spec/initContainers/0/securityContext/capabilities/add",
                            "NET_RAW",
                        ),
                        // case-difference
                        add(
                            "/spec/containers/0/securityContext/capabilities/add",
                            "chown",
                        ),
                        // CAP_ prefix
                        add(
                            "/spec/containers/0/securityContext/capabilities/add",
                            "CAP_CHOWN",
                        ),
                    ]
                },
            },
        );

        // fixtures_capabilities_restricted.go
        const ALL_INDIVIDUAL_CAPS: &[&str] = &[
            "SYS_TIME",
            "SYS_MODULE",
            "SYS_RAWIO",
            "SYS_PACCT",
            "SYS_ADMIN",
            "SYS_NICE",
            "SYS_RESOURCE",
            "SYS_TIME",
            "SYS_TTY_CONFIG",
            "MKNOD",
            "AUDIT_WRITE",
            "AUDIT_CONTROL",
            "MAC_OVERRIDE",
            "MAC_ADMIN",
            "NET_ADMIN",
            "SYSLOG",
            "CHOWN",
            "NET_RAW",
            "DAC_OVERRIDE",
            "FOWNER",
            "DAC_READ_SEARCH",
            "FSETID",
            "KILL",
            "SETGID",
            "SETUID",
            "LINUX_IMMUTABLE",
            "NET_BIND_SERVICE",
            "NET_BROADCAST",
            "IPC_LOCK",
            "IPC_OWNER",
            "SYS_CHROOT",
            "SYS_PTRACE",
            "SYS_BOOT",
            "LEASE",
            "SETFCAP",
            "WAKE_ALARM",
            "BLOCK_SUSPEND",
        ];
        g.register(
            key(Level::Restricted, 22, "capabilities_restricted"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    let p = ensure_capabilities(p);
                    vec![tweak(&p, |p| {
                        for c in ["containers", "initContainers"] {
                            let caps = format!("/spec/{c}/0/securityContext/capabilities");
                            set(p, &format!("{caps}/drop"), json!(["ALL"]));
                            set(p, &format!("{caps}/add"), json!(["NET_BIND_SERVICE"]));
                        }
                    })]
                },
                generate_fail: |p| {
                    let p = ensure_capabilities(p);
                    vec![
                        // test container
                        tweak(&p, |p| {
                            unset(p, "/spec/containers/0/securityContext/capabilities/drop")
                        }),
                        // test initContainer
                        tweak(&p, |p| {
                            unset(
                                p,
                                "/spec/initContainers/0/securityContext/capabilities/drop",
                            )
                        }),
                        // dropping all individual capabilities is not sufficient
                        tweak(&p, |p| {
                            for c in ["containers", "initContainers"] {
                                set(
                                    p,
                                    &format!("/spec/{c}/0/securityContext/capabilities/drop"),
                                    json!(ALL_INDIVIDUAL_CAPS),
                                );
                            }
                        }),
                        tweak(&p, |p| {
                            for c in ["containers", "initContainers"] {
                                let caps = format!("/spec/{c}/0/securityContext/capabilities");
                                set(p, &format!("{caps}/drop"), json!(["ALL"]));
                                // try adding back capabilities other than
                                // NET_BIND_SERVICE, should be forbidden
                                set(p, &format!("{caps}/add"), json!(ALLOWED_CAPS));
                            }
                        }),
                    ]
                },
            },
        );

        // fixtures_hostPathVolumes.go
        g.register(
            key(Level::Baseline, 0, "hostPathVolumes"),
            FixtureGenerator {
                // minimal valid pod already captures all valid combinations
                generate_pass: |_, _| vec![],
                generate_fail: |p| {
                    vec![
                        // mix of hostPath and non-hostPath volumes
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/volumes",
                                json!([
                                    {"name": "volume-emptydir", "emptyDir": {}},
                                    {"name": "volume-hostpath", "hostPath": {"path": "/a"}},
                                ]),
                            )
                        }),
                        // just hostPath volumes
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/volumes",
                                json!([
                                    {"name": "volume-hostpath-a", "hostPath": {"path": "/a"}},
                                    {"name": "volume-hostpath-b", "hostPath": {"path": "/b"}},
                                ]),
                            )
                        }),
                    ]
                },
            },
        );

        // fixtures_hostPorts.go
        g.register(
            key(Level::Baseline, 0, "hostPorts"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    vec![
                        // no host ports
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/ports",
                                json!([{"containerPort": 12345}]),
                            );
                            set(
                                p,
                                "/spec/initContainers/0/ports",
                                json!([{"containerPort": 12346}]),
                            );
                        }),
                    ]
                },
                generate_fail: |p| {
                    vec![
                        // Host Port present
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/ports",
                                json!([{"containerPort": 12345, "hostPort": 12345}]),
                            )
                        }),
                        // check initContainer
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/initContainers/0/ports",
                                json!([{"containerPort": 12346, "hostPort": 12346}]),
                            )
                        }),
                        // mix of hostPorts and regular ports
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/ports",
                                json!([
                                    {"containerPort": 12345, "hostPort": 12345},
                                    {"containerPort": 12347},
                                ]),
                            );
                            set(
                                p,
                                "/spec/initContainers/0/ports",
                                json!([
                                    {"containerPort": 12346, "hostPort": 12346},
                                    {"containerPort": 12348},
                                ]),
                            );
                        }),
                    ]
                },
            },
        );

        // fixtures_hostProbesAndHostLifecycle.go
        g.register(
            key(Level::Baseline, 34, "hostProbesAndHostLifecycle"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    vec![
                        // A pod with no probes should pass.
                        p.clone(),
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/livenessProbe",
                                json!({"httpGet": {"port": 8080}}),
                            )
                        }),
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/readinessProbe",
                                json!({"tcpSocket": {"port": 8080}}),
                            )
                        }),
                    ]
                },
                generate_fail: |p| {
                    vec![
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/livenessProbe",
                                json!({"httpGet": {"host": "bad.host", "port": 8080}}),
                            )
                        }),
                        tweak(p, |p| {
                            set(p, "/spec/initContainers/0/restartPolicy", json!("Always"));
                            set(
                                p,
                                "/spec/initContainers/0/readinessProbe",
                                json!({"tcpSocket": {"host": "8.8.8.8", "port": 8080}}),
                            );
                        }),
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/lifecycle",
                                json!({"postStart": {"httpGet": {"host": "bad.host", "port": 8080}}}),
                            )
                        }),
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/livenessProbe",
                                json!({"httpGet": {"host": "127.0.0.1", "port": 8080}}),
                            )
                        }),
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/containers/0/readinessProbe",
                                json!({"tcpSocket": {"host": "::1", "port": 8080}}),
                            )
                        }),
                    ]
                },
            },
        );

        // fixtures_procMount.go
        fn proc_mount_pass(p: &Pod, _: Level) -> Vec<Pod> {
            let p = ensure_security_context(p);
            vec![
                // set proc mount of container and init container to a valid value
                tweak(&p, |c| {
                    set(
                        c,
                        "/spec/containers/0/securityContext/procMount",
                        json!("Default"),
                    );
                    set(
                        c,
                        "/spec/initContainers/0/securityContext/procMount",
                        json!("Default"),
                    );
                    set(c, "/spec/hostUsers", json!(false));
                }),
            ]
        }
        fn proc_mount_fail(p: &Pod, host_users: bool) -> Vec<Pod> {
            let p = ensure_security_context(p);
            vec![
                // set proc mount of container to a forbidden value
                tweak(&p, |c| {
                    set(
                        c,
                        "/spec/containers/0/securityContext/procMount",
                        json!("Unmasked"),
                    );
                    set(c, "/spec/hostUsers", json!(host_users));
                }),
                // set proc mount of init container to a forbidden value
                tweak(&p, |c| {
                    set(
                        c,
                        "/spec/initContainers/0/securityContext/procMount",
                        json!("Unmasked"),
                    );
                    set(c, "/spec/hostUsers", json!(host_users));
                }),
            ]
        }
        g.register(
            key(Level::Baseline, 0, "procMount"),
            FixtureGenerator {
                generate_pass: proc_mount_pass,
                generate_fail: |p| proc_mount_fail(p, false),
            },
        );
        g.register(
            key(Level::Baseline, 35, "procMount"),
            FixtureGenerator {
                generate_pass: |p, level| {
                    let mut retval = proc_mount_pass(p, level);
                    if level != Level::Restricted {
                        // don't expect the unmasked namespace user pod to pass in
                        // restricted mode
                        let p = ensure_security_context(p);
                        retval.push(tweak(&p, |c| {
                            set(
                                c,
                                "/spec/containers/0/securityContext/procMount",
                                json!("Unmasked"),
                            );
                            set(
                                c,
                                "/spec/initContainers/0/securityContext/procMount",
                                json!("Unmasked"),
                            );
                            set(c, "/spec/hostUsers", json!(false));
                        }));
                    }
                    retval
                },
                generate_fail: |p| proc_mount_fail(p, true),
            },
        );
        g.register(
            key(Level::Restricted, 35, "procMount_restricted"),
            FixtureGenerator {
                generate_pass: proc_mount_pass,
                generate_fail: |p| proc_mount_fail(p, false),
            },
        );

        // fixtures_restrictedVolumes.go
        g.register(
            key(Level::Restricted, 0, "restrictedVolumes"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    vec![
                        // pod that has all allowed volume types
                        tweak(p, |p| {
                            set(
                                p,
                                "/spec/volumes",
                                json!([
                                    // implicit empty dir
                                    {"name": "volume0"},
                                    {"name": "volume1", "emptyDir": {}},
                                    {"name": "volume2", "secret": {"secretName": "test"}},
                                    {"name": "volume3", "persistentVolumeClaim": {"claimName": "test"}},
                                    {"name": "volume4", "downwardAPI": {"items": [
                                        {"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}]}},
                                    {"name": "volume5", "configMap": {"name": "test"}},
                                    {"name": "volume6", "projected": {"sources": []}},
                                ]),
                            )
                        }),
                    ]
                },
                generate_fail: |p| {
                    [
                        json!({"gcePersistentDisk": {"pdName": "test"}}),
                        json!({"awsElasticBlockStore": {"volumeID": "test"}}),
                        json!({"gitRepo": {"repository": "github.com/kubernetes/kubernetes"}}),
                        json!({"nfs": {"server": "test", "path": "/test"}}),
                        json!({"iscsi": {"targetPortal": "test",
                            "iqn": "iqn.2001-04.com.example:storage.kube.sys1.xyz", "lun": 0}}),
                        json!({"glusterfs": {"path": "test", "endpoints": "test"}}),
                        json!({"rbd": {"monitors": ["test"], "image": "test"}}),
                        json!({"flexVolume": {"driver": "test"}}),
                        json!({"cinder": {"volumeID": "test"}}),
                        json!({"cephfs": {"monitors": ["test"]}}),
                        json!({"flocker": {"datasetName": "test"}}),
                        json!({"fc": {"wwids": ["test"]}}),
                        json!({"azureFile": {"secretName": "test", "shareName": "test"}}),
                        json!({"vsphereVolume": {"volumePath": "test"}}),
                        json!({"quobyte": {"registry": "localhost:1234", "volume": "test"}}),
                        json!({"azureDisk": {"diskName": "test",
                            "diskURI": "https://test.blob.core.windows.net/test/test.vhd"}}),
                        json!({"portworxVolume": {"volumeID": "test", "fsType": "ext4"}}),
                        json!({"scaleIO": {"gateway": "localhost", "system": "test",
                            "secretRef": null, "volumeName": "test"}}),
                        json!({"storageos": {"volumeName": "test"}}),
                        json!({"hostPath": {"path": "/dev/null"}}),
                    ]
                    .into_iter()
                    .map(|mut source| {
                        source["name"] = json!("volume1");
                        tweak(p, |p| set(p, "/spec/volumes", json!([source])))
                    })
                    .collect()
                },
            },
        );

        // fixtures_seLinuxOptions.go
        g.register(
            key(Level::Baseline, 0, "seLinuxOptions"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    let p = ensure_se_linux_options(p);
                    vec![
                        // security context with no seLinuxOptions
                        tweak(&p, |p| {
                            // no pod-level
                            unset(p, "/spec/securityContext/seLinuxOptions");
                            // no container-level
                            unset(p, "/spec/containers/0/securityContext/seLinuxOptions");
                            // empty container-level
                            set(
                                p,
                                "/spec/initContainers/0/securityContext/seLinuxOptions",
                                json!({}),
                            );
                        }),
                        tweak(&p, |p| {
                            // seLinuxOptions with all valid types
                            set(
                                p,
                                "/spec/securityContext/seLinuxOptions/type",
                                json!("container_t"),
                            );
                            set(
                                p,
                                "/spec/containers/0/securityContext/seLinuxOptions/type",
                                json!("container_init_t"),
                            );
                            set(
                                p,
                                "/spec/initContainers/0/securityContext/seLinuxOptions/type",
                                json!("container_kvm_t"),
                            );
                            // with empty level and arbitrary level (Go's empty
                            // string is omitted)
                            unset(p, "/spec/securityContext/seLinuxOptions/level");
                            set(
                                p,
                                "/spec/containers/0/securityContext/seLinuxOptions/level",
                                json!("somevalue"),
                            );
                        }),
                    ]
                },
                generate_fail: |p| {
                    let p = ensure_se_linux_options(p);
                    let bad = |ptr: &str| tweak(&p, |p| set(p, ptr, json!("somevalue")));
                    vec![
                        // seLinuxOptions with out of bounds type
                        // pod-level
                        bad("/spec/securityContext/seLinuxOptions/type"),
                        // container
                        bad("/spec/containers/0/securityContext/seLinuxOptions/type"),
                        // initContainer
                        bad("/spec/initContainers/0/securityContext/seLinuxOptions/type"),
                        // seLinuxOptions with out of bounds user
                        bad("/spec/securityContext/seLinuxOptions/user"),
                        // seLinuxOptions with out of bounds role
                        bad("/spec/securityContext/seLinuxOptions/role"),
                    ]
                },
            },
        );

        // fixtures_seccompProfile_baseline.go (helpers_seccomp.go)
        const ANNOTATION_KEY_POD: &str = "seccomp.security.alpha.kubernetes.io/pod";
        const ANNOTATION_KEY_CONTAINER_PREFIX: &str =
            "container.seccomp.security.alpha.kubernetes.io/";
        fn seccomp_annotation_key(p: &Pod, list: &str) -> String {
            format!(
                "{ANNOTATION_KEY_CONTAINER_PREFIX}{}",
                p["spec"][list][0]["name"].as_str().unwrap()
            )
        }
        fn annotate(p: &Pod, key: &str, v: &str) -> Pod {
            tweak(p, |p| p["metadata"]["annotations"][key] = json!(v))
        }
        g.register(
            key(Level::Baseline, 0, "seccompProfile_baseline"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    // don't generate fixtures if minimal valid pod already has
                    // seccomp config
                    if p["metadata"]["annotations"][ANNOTATION_KEY_POD] == "runtime/default" {
                        return vec![];
                    }
                    let p = ensure_annotation(p);
                    vec![tweak(&p, |p| {
                        // pod-level default
                        p["metadata"]["annotations"][ANNOTATION_KEY_POD] = json!("runtime/default");
                        // container-level localhost
                        let k = seccomp_annotation_key(p, "containers");
                        p["metadata"]["annotations"][k] = json!("localhost/testing");
                        // init-container unset
                    })]
                },
                generate_fail: |p| {
                    let p = ensure_annotation(p);
                    vec![
                        annotate(&p, ANNOTATION_KEY_POD, "unconfined"),
                        annotate(&p, &seccomp_annotation_key(&p, "containers"), "unconfined"),
                        annotate(
                            &p,
                            &seccomp_annotation_key(&p, "initContainers"),
                            "unconfined",
                        ),
                    ]
                },
            },
        );
        g.register(
            key(Level::Baseline, 19, "seccompProfile_baseline"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    // don't generate fixtures if minimal valid pod already has
                    // seccomp config
                    if p["spec"]["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault" {
                        return vec![];
                    }
                    let p = ensure_security_context(p);
                    vec![tweak(&p, |p| {
                        // pod-level default
                        set(
                            p,
                            "/spec/securityContext/seccompProfile",
                            json!({"type": "RuntimeDefault"}),
                        );
                        // container-level localhost
                        set(
                            p,
                            "/spec/containers/0/securityContext/seccompProfile",
                            json!({"type": "RuntimeDefault"}),
                        );
                        // init-container unset
                    })]
                },
                generate_fail: |p| {
                    let p = ensure_security_context(p);
                    [
                        "/spec/securityContext/seccompProfile",
                        "/spec/containers/0/securityContext/seccompProfile",
                        "/spec/initContainers/0/securityContext/seccompProfile",
                    ]
                    .into_iter()
                    .map(|ptr| tweak(&p, |p| set(p, ptr, json!({"type": "Unconfined"}))))
                    .collect()
                },
            },
        );

        // fixtures_seccompProfile_restricted.go
        const SECCOMP_POD: &str = "/spec/securityContext/seccompProfile";
        const SECCOMP_C: &str = "/spec/containers/0/securityContext/seccompProfile";
        const SECCOMP_I: &str = "/spec/initContainers/0/securityContext/seccompProfile";
        fn seccomp_default() -> Value {
            json!({"type": "RuntimeDefault"})
        }
        fn seccomp_unconfined() -> Value {
            json!({"type": "Unconfined"})
        }
        fn seccomp_localhost(profile: &str) -> Value {
            json!({"type": "Localhost", "localhostProfile": profile})
        }
        g.register(
            key(Level::Restricted, 19, "seccompProfile_restricted"),
            FixtureGenerator {
                generate_pass: |p, _| {
                    let p = ensure_security_context(p);
                    vec![
                        tweak(&p, |p| set(p, SECCOMP_POD, seccomp_default())),
                        tweak(&p, |p| set(p, SECCOMP_POD, seccomp_localhost("testing"))),
                        tweak(&p, |p| {
                            unset(p, SECCOMP_POD);
                            set(p, SECCOMP_C, seccomp_default());
                            set(p, SECCOMP_I, seccomp_localhost("testing"));
                        }),
                    ]
                },
                generate_fail: |p| {
                    let p = ensure_security_context(p);
                    vec![
                        // unset everywhere
                        tweak(&p, |p| unset(p, SECCOMP_POD)),
                        // unconfined, pod-level
                        tweak(&p, |p| set(p, SECCOMP_POD, seccomp_unconfined())),
                        // unset initContainer
                        tweak(&p, |p| {
                            unset(p, SECCOMP_POD);
                            set(p, SECCOMP_C, seccomp_default());
                        }),
                        // unset container
                        tweak(&p, |p| {
                            unset(p, SECCOMP_POD);
                            set(p, SECCOMP_I, seccomp_default());
                        }),
                        // unconfined, container-level
                        tweak(&p, |p| {
                            unset(p, SECCOMP_POD);
                            set(p, SECCOMP_C, seccomp_default());
                            set(p, SECCOMP_I, seccomp_unconfined());
                        }),
                    ]
                },
            },
        );

        // fixtures_sysctls.go: one generator per policy version; they differ
        // only in the allowed set the pass case exercises.
        macro_rules! sysctls_generator {
            ($allowed:expr) => {
                FixtureGenerator {
                    generate_pass: |p, _| {
                        let p = ensure_pod_security_context(p);
                        vec![
                            // security context with no sysctls
                            tweak(&p, |p| unset(p, "/spec/securityContext/sysctls")),
                            tweak(&p, |p| {
                                let allowed: Vec<(&str, &str)> = $allowed;
                                let list: Vec<Value> = allowed
                                    .into_iter()
                                    .map(|(n, v)| json!({"name": n, "value": v}))
                                    .collect();
                                set(p, "/spec/securityContext/sysctls", Value::Array(list));
                            }),
                        ]
                    },
                    generate_fail: |p| {
                        let p = ensure_pod_security_context(p);
                        vec![
                            // sysctls with out of allowed name
                            tweak(&p, |p| {
                                set(
                                    p,
                                    "/spec/securityContext/sysctls",
                                    json!([{"name": "othersysctl", "value": "other"}]),
                                )
                            }),
                        ]
                    },
                }
            };
        }
        g.register(
            key(Level::Baseline, 0, "sysctls"),
            sysctls_generator!(vec![
                ("kernel.shm_rmid_forced", "0"),
                ("net.ipv4.ip_local_port_range", "1024 65535"),
                ("net.ipv4.tcp_syncookies", "0"),
                ("net.ipv4.ping_group_range", "1 0"),
                ("net.ipv4.ip_unprivileged_port_start", "1024"),
            ]),
        );
        g.register(
            key(Level::Baseline, 27, "sysctls"),
            sysctls_generator!(vec![
                ("kernel.shm_rmid_forced", "0"),
                ("net.ipv4.ip_local_port_range", "1024 65535"),
                ("net.ipv4.tcp_syncookies", "0"),
                ("net.ipv4.ping_group_range", "1 0"),
                ("net.ipv4.ip_unprivileged_port_start", "1024"),
                ("net.ipv4.ip_local_reserved_ports", "1024-4999"),
            ]),
        );
        g.register(
            key(Level::Baseline, 29, "sysctls"),
            sysctls_generator!(vec![
                ("kernel.shm_rmid_forced", "0"),
                ("net.ipv4.ip_local_port_range", "1024 65535"),
                ("net.ipv4.tcp_syncookies", "0"),
                ("net.ipv4.ping_group_range", "1 0"),
                ("net.ipv4.ip_unprivileged_port_start", "1024"),
                ("net.ipv4.ip_local_reserved_ports", "1024-4999"),
                ("net.ipv4.tcp_keepalive_time", "7200"),
                ("net.ipv4.tcp_fin_timeout", "60"),
                ("net.ipv4.tcp_keepalive_intvl", "75"),
                ("net.ipv4.tcp_keepalive_probes", "9"),
            ]),
        );
        g.register(
            key(Level::Baseline, 32, "sysctls"),
            sysctls_generator!(vec![
                ("net.ipv4.tcp_rmem", "4096 87380 16777216"),
                ("net.ipv4.tcp_wmem", "4096 65536 16777216"),
            ]),
        );

        // fixtures_windowsHostProcess.go
        g.register(
            key(Level::Baseline, 0, "windowsHostProcess"),
            FixtureGenerator {
                // minimal valid pod already captures all valid combinations
                generate_pass: |_, _| vec![],
                generate_fail: |p| {
                    let mut p = ensure_security_context(p);
                    for ptr in [
                        "/spec/securityContext/windowsOptions",
                        "/spec/containers/0/securityContext/windowsOptions",
                        "/spec/initContainers/0/securityContext/windowsOptions",
                    ] {
                        if p.pointer(ptr).is_none() {
                            set(&mut p, ptr, json!({}));
                        }
                    }
                    vec![
                        // true for pod
                        tweak(&p, |p| {
                            // HostNetwork is required to be true for HostProcess
                            // pods. Set to true here so we pass API validation
                            // and get to admission checks.
                            set(p, "/spec/hostNetwork", json!(true));
                            set(
                                p,
                                "/spec/securityContext/windowsOptions/hostProcess",
                                json!(true),
                            );
                        }),
                        // true for containers
                        tweak(&p, |p| {
                            set(p, "/spec/hostNetwork", json!(true));
                            set(
                                p,
                                "/spec/containers/0/securityContext/windowsOptions/hostProcess",
                                json!(true),
                            );
                            set(
                                p,
                                "/spec/initContainers/0/securityContext/windowsOptions/hostProcess",
                                json!(true),
                            );
                        }),
                    ]
                },
            },
        );
        g
    }

    // ------------------------------------------------------------------ run.go

    /// run.go:85-93 `checksForLevelAndVersion`.
    fn checks_for_level_and_version(
        checks: &[Check],
        level: Level,
        minor: u32,
    ) -> Vec<&'static str> {
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
                        panic!("{level}/1.{minor}: check {id} has no fixture generator");
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
                    let actual = yaml_files(dir);
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
}
