//! Verbatim case-by-case ports of upstream pod-security-admission per-check
//! tables (staging/src/k8s.io/pod-security-admission/policy/):
//! check_appArmorProfile_test.go, check_seccompProfile_restricted_test.go,
//! check_seccompProfile_baseline_test.go, check_seLinuxOptions_test.go.
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
