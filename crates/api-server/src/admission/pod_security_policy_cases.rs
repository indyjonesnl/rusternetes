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
use super::*;
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
    let c =
        |n: &str, t: &str| json!({"name": n, "securityContext": {"seccompProfile": {"type": t}}});
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
