//! Wide serialization sweep: create EVERY creatable resource and assert the
//! shape of what comes back.
//!
//! Two whole classes of bug have now been found one resource at a time:
//!
//!   * an `IntOrString` modelled as a Rust `String`, so a request carrying the
//!     JSON number `1` read back as the JSON string `"1"`. The Go DaemonSet
//!     controller then wedged on it and nothing programmed the service
//!     iptables in the vanilla-swap leg (#1902, #1909).
//!   * a duplicated `ObjectMeta` that serialised timestamps with chrono's
//!     default *nanosecond* precision instead of RFC3339 seconds (#1895).
//!
//! Both were single-field defects invisible to every test except the one that
//! happened to touch that field. Finding them one at a time does not converge:
//! there are ~69 creatable resources and thousands of fields. So this file
//! asserts the two *invariants* over the whole creatable surface at once.
//!
//! # Invariant 1 — scalar type parity across the round trip
//!
//! For every path present in both the request and the response, the JSON
//! *type* must match, and a request integer must read back as an integer.
//! Upstream gets this for free: `encoding/json` marshals through the same
//! typed struct it unmarshalled into, so `intstr.IntOrString` with
//! `Type: Int` marshals as a number
//! (`staging/src/k8s.io/apimachinery/pkg/util/intstr/intstr.go`,
//! `MarshalJSON`). Rusternetes has one hand-written struct per resource, so a
//! mistyped field is a silent, per-field lie about the wire format.
//!
//! Note the fixtures deliberately spell every `Quantity` as a string
//! (`"2Gi"`, `"100m"`) — the shape a real manifest uses. Upstream's
//! `Quantity.UnmarshalJSON` accepts a bare number but `MarshalJSON` always
//! emits a string, so a number there is a *legitimate* type change and would
//! need an exception. Writing the fixtures the manifest way keeps this test
//! exception-free.
//!
//! # Invariant 2 — timestamp precision
//!
//! `metav1.Time` marshals with `time.RFC3339`
//! (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/time.go:167-170`:
//! `buf = t.UTC().AppendFormat(buf, time.RFC3339)`), whose layout
//! `"2006-01-02T15:04:05Z07:00"` carries **no fractional seconds**.
//! `metav1.MicroTime` marshals with `RFC3339Micro`
//! (`micro_time.go:26,181`: `"2006-01-02T15:04:05.000000Z07:00"`), which
//! carries **exactly six**. Both call `.UTC()`, so the zone is always `Z`.
//!
//! Every timestamp-shaped string anywhere in a response is checked against
//! that rule, keyed on the field name only to pick which of the two applies.
//! A newly added timestamp field is therefore covered the day it appears,
//! with nobody having to list it.
//!
//! # Completeness
//!
//! The resource list is not written down here — it is read from the server's
//! own discovery documents at run time, and a creatable resource with no
//! fixture FAILS the sweep. A new resource is in scope the moment it is
//! registered in discovery, and cannot be quietly skipped. There is no
//! allowlist, and no `continue` acting as one.
//!
//! Resources whose discovery verbs are exactly `["create"]` (the six
//! review-style kinds) are not persisted, so they get the type-parity check
//! but no read-back and no `creationTimestamp` — a distinction taken from the
//! server's own verb list, not from a hand-kept exception list.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const NS: &str = "wide-serialization-sweep";

/// The five `metav1.MicroTime` JSON field names in the whole API surface.
///
/// Derived by grepping the upstream types:
/// `grep -rn 'metav1.MicroTime' staging/src/k8s.io/api/ --include=types.go`
/// on the pinned checkout. Every other timestamp field is a `metav1.Time`.
const MICRO_TIME_FIELDS: &[&str] = &[
    "acquireTime",
    "eventTime",
    "lastObservedTime",
    "pingTime",
    "renewTime",
];

// ---------------------------------------------------------------------------
// timestamp shape
// ---------------------------------------------------------------------------

/// Does `s` look like an RFC3339 timestamp? Matches
/// `YYYY-MM-DDTHH:MM:SS[.frac][Z|±HH:MM]` structurally, without pulling in a
/// regex engine. Deliberately loose on the tail so that a *wrong* tail (a
/// numeric offset where upstream always emits `Z`) is reported by the caller
/// rather than silently failing to match and skipping the check.
fn timestamp_parts(s: &str) -> Option<(usize, &str)> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let digit = |i: usize| b[i].is_ascii_digit();
    let shape = (0..4).all(digit)
        && b[4] == b'-'
        && digit(5)
        && digit(6)
        && b[7] == b'-'
        && digit(8)
        && digit(9)
        && b[10] == b'T'
        && digit(11)
        && digit(12)
        && b[13] == b':'
        && digit(14)
        && digit(15)
        && b[16] == b':'
        && digit(17)
        && digit(18);
    if !shape {
        return None;
    }
    let rest = &s[19..];
    let frac_digits = if let Some(after_dot) = rest.strip_prefix('.') {
        after_dot.chars().take_while(|c| c.is_ascii_digit()).count()
    } else {
        0
    };
    let tail = if frac_digits > 0 {
        &rest[1 + frac_digits..]
    } else {
        rest
    };
    Some((frac_digits, tail))
}

/// Walk `v` and record every timestamp-shaped string that does not match the
/// precision upstream would have produced for its field.
fn check_timestamps(v: &Value, path: &str, field: &str, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                check_timestamps(child, &format!("{path}.{k}"), k, out);
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                // An array element keeps its parent's field name: the
                // timestamps in `series` / `conditions` are typed by the key
                // that holds the array, not by the index.
                check_timestamps(child, &format!("{path}[{i}]"), field, out);
            }
        }
        Value::String(s) => {
            let Some((frac_digits, tail)) = timestamp_parts(s) else {
                return;
            };
            if tail != "Z" {
                out.push(format!(
                    "{path}: {s:?} must be UTC-suffixed `Z` — upstream marshals \
                     `t.UTC().Format(...)` for both metav1.Time and metav1.MicroTime, \
                     so a numeric offset can never appear on the wire"
                ));
                return;
            }
            if MICRO_TIME_FIELDS.contains(&field) {
                if frac_digits != 6 {
                    out.push(format!(
                        "{path}: {s:?} has {frac_digits} fractional digit(s); \
                         `{field}` is a metav1.MicroTime and RFC3339Micro \
                         (\"2006-01-02T15:04:05.000000Z07:00\") pins it to exactly 6"
                    ));
                }
            } else if frac_digits != 0 {
                out.push(format!(
                    "{path}: {s:?} carries {frac_digits} fractional second digit(s); \
                     `{field}` is a metav1.Time and time.RFC3339 \
                     (\"2006-01-02T15:04:05Z07:00\") has no fractional part. \
                     A chrono default format leaks nanoseconds here (#1895)"
                ));
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// scalar type parity
// ---------------------------------------------------------------------------

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Compare the request against the response for every path the request set.
/// Paths the server adds are ignored (that is the point of defaulting); paths
/// the server drops are ignored too (a status the create path rebuilds). What
/// must not happen is a path coming back with a *different JSON type*.
fn check_type_parity(req: &Value, resp: &Value, path: &str, out: &mut Vec<String>) {
    if json_type(req) != json_type(resp) {
        out.push(format!(
            "{path}: sent {} {}, read back {} {} — the round trip must preserve the \
             JSON type. A number arriving as a string is the IntOrString-as-String \
             defect (#1902/#1909); upstream marshals through the same typed field it \
             unmarshalled into, so the type cannot drift",
            json_type(req),
            req,
            json_type(resp),
            resp,
        ));
        return;
    }
    match (req, resp) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, av) in a {
                if let Some(bv) = b.get(k) {
                    check_type_parity(av, bv, &format!("{path}.{k}"), out);
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            for (i, av) in a.iter().enumerate() {
                if let Some(bv) = b.get(i) {
                    check_type_parity(av, bv, &format!("{path}[{i}]"), out);
                }
            }
        }
        (Value::Number(a), Value::Number(b)) => {
            if a.is_i64() && !b.is_i64() {
                out.push(format!(
                    "{path}: sent the integer {a}, read back {b} — an integral field \
                     must not round-trip through a float"
                ));
            } else if a.as_f64() != b.as_f64() {
                out.push(format!("{path}: sent {a}, read back {b}"));
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// the fixtures
// ---------------------------------------------------------------------------

/// A resource body to create. Keyed by `(group, version, resource)` — the
/// same triple discovery reports, so the completeness check is an exact set
/// comparison rather than a fuzzy match.
struct Fixture {
    group: &'static str,
    version: &'static str,
    resource: &'static str,
    body: Value,
}

fn pod_template() -> Value {
    json!({
        "metadata": { "labels": { "app": "sweep" } },
        "spec": {
            "terminationGracePeriodSeconds": 30,
            "containers": [{
                "name": "app",
                "image": "registry.k8s.io/pause:3.10",
                "ports": [{ "containerPort": 8080, "hostPort": 8080, "protocol": "TCP" }],
                "resources": {
                    "requests": { "cpu": "100m", "memory": "64Mi" },
                    "limits": { "cpu": "200m", "memory": "128Mi" }
                },
                "livenessProbe": {
                    // IntOrString with Type=Int: must read back as the number 8080.
                    "httpGet": { "path": "/healthz", "port": 8080 },
                    "initialDelaySeconds": 5,
                    "periodSeconds": 10,
                    "timeoutSeconds": 1,
                    "successThreshold": 1,
                    "failureThreshold": 3
                },
                "readinessProbe": {
                    // IntOrString with Type=String: must stay a string.
                    "tcpSocket": { "port": "http" },
                    "periodSeconds": 10
                }
            }]
        }
    })
}

fn fixtures() -> Vec<Fixture> {
    let f =
        |group: &'static str, version: &'static str, resource: &'static str, body: Value| Fixture {
            group,
            version,
            resource,
            body,
        };
    vec![
        // ---- core/v1 -----------------------------------------------------
        f(
            "",
            "v1",
            "namespaces",
            json!({ "metadata": { "name": "sweep-ns" } }),
        ),
        f("", "v1", "pods", {
            let mut p = json!({ "metadata": { "name": "sweep-pod" } });
            p["spec"] = pod_template()["spec"].clone();
            // Only legal on a bare Pod: upstream's ValidatePodTemplateSpec
            // rejects activeDeadlineSeconds inside a controller's template
            // ("activeDeadlineSeconds in ReplicationController is not
            // Supported"), so it cannot live in `pod_template()`.
            p["spec"]["activeDeadlineSeconds"] = json!(300);
            p
        }),
        f(
            "",
            "v1",
            "services",
            json!({
                "metadata": { "name": "sweep-svc" },
                "spec": {
                    "selector": { "app": "sweep" },
                    "ports": [{
                        "name": "http", "port": 80, "protocol": "TCP",
                        // IntOrString, Type=Int.
                        "targetPort": 8080
                    }, {
                        "name": "named", "port": 81, "protocol": "TCP",
                        // IntOrString, Type=String.
                        "targetPort": "http"
                    }]
                }
            }),
        ),
        f(
            "",
            "v1",
            "nodes",
            json!({
                "metadata": { "name": "sweep-node" },
                "spec": { "podCIDR": "10.244.9.0/24", "podCIDRs": ["10.244.9.0/24"] },
                "status": { "capacity": { "cpu": "4", "memory": "8Gi", "pods": "110" } }
            }),
        ),
        f(
            "",
            "v1",
            "configmaps",
            json!({
                "metadata": { "name": "sweep-cm" },
                "data": { "key": "value", "number-shaped": "3" }
            }),
        ),
        f(
            "",
            "v1",
            "secrets",
            json!({
                "metadata": { "name": "sweep-secret" },
                "type": "Opaque",
                "stringData": { "password": "s3cr3t" }
            }),
        ),
        f(
            "",
            "v1",
            "serviceaccounts",
            json!({
                "metadata": { "name": "sweep-sa" },
                "automountServiceAccountToken": true
            }),
        ),
        f(
            "",
            "v1",
            "persistentvolumes",
            json!({
                "metadata": { "name": "sweep-pv" },
                "spec": {
                    "capacity": { "storage": "2Gi" },
                    "accessModes": ["ReadWriteOnce"],
                    "persistentVolumeReclaimPolicy": "Retain",
                    "storageClassName": "standard",
                    "hostPath": { "path": "/mnt/sweep" }
                }
            }),
        ),
        f(
            "",
            "v1",
            "persistentvolumeclaims",
            json!({
                "metadata": { "name": "sweep-pvc" },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "standard",
                    "resources": { "requests": { "storage": "2Gi" } }
                }
            }),
        ),
        f(
            "",
            "v1",
            "endpoints",
            json!({
                "metadata": { "name": "sweep-endpoints" },
                "subsets": [{
                    "addresses": [{ "ip": "10.244.9.5" }],
                    "ports": [{ "name": "http", "port": 8080, "protocol": "TCP" }]
                }]
            }),
        ),
        // core/v1 Event carries BOTH clock types: firstTimestamp /
        // lastTimestamp are metav1.Time, eventTime and
        // series.lastObservedTime are metav1.MicroTime. It is the single
        // best witness for invariant 2.
        f(
            "",
            "v1",
            "events",
            json!({
                "metadata": { "name": "sweep-event" },
                "involvedObject": { "kind": "Pod", "name": "sweep-pod", "namespace": NS },
                "action": "Sweep",
                "reason": "Sweep",
                "message": "wide serialization sweep",
                "type": "Normal",
                "count": 2,
                "firstTimestamp": "2026-09-08T10:00:00Z",
                "lastTimestamp": "2026-09-08T10:05:00Z",
                "eventTime": "2026-09-08T10:05:00.123456Z",
                "series": { "count": 3, "lastObservedTime": "2026-09-08T10:06:00.654321Z" },
                "reportingComponent": "sweep",
                "reportingInstance": "sweep-0"
            }),
        ),
        f(
            "",
            "v1",
            "resourcequotas",
            json!({
                "metadata": { "name": "sweep-quota" },
                "spec": { "hard": { "pods": "10", "cpu": "2", "configmaps": "10" } }
            }),
        ),
        f(
            "",
            "v1",
            "limitranges",
            json!({
                "metadata": { "name": "sweep-limits" },
                "spec": { "limits": [{
                    "type": "Container",
                    "default": { "cpu": "200m", "memory": "128Mi" },
                    "defaultRequest": { "cpu": "100m", "memory": "64Mi" },
                    "max": { "cpu": "1", "memory": "1Gi" },
                    "min": { "cpu": "50m", "memory": "32Mi" }
                }] }
            }),
        ),
        f(
            "",
            "v1",
            "replicationcontrollers",
            json!({
                "metadata": { "name": "sweep-rc" },
                "spec": {
                    "replicas": 2,
                    "minReadySeconds": 5,
                    "selector": { "app": "sweep" },
                    "template": pod_template()
                }
            }),
        ),
        f(
            "",
            "v1",
            "podtemplates",
            json!({
                "metadata": { "name": "sweep-podtemplate" },
                "template": pod_template()
            }),
        ),
        // ---- apps/v1 -----------------------------------------------------
        f(
            "apps",
            "v1",
            "deployments",
            json!({
                "metadata": { "name": "sweep-deploy" },
                "spec": {
                    "replicas": 2,
                    "revisionHistoryLimit": 5,
                    "progressDeadlineSeconds": 600,
                    "minReadySeconds": 5,
                    "selector": { "matchLabels": { "app": "sweep" } },
                    "strategy": {
                        "type": "RollingUpdate",
                        // The exact pair the vanilla-swap leg wedged on: sent as
                        // JSON numbers, they must not come back quoted (#1902).
                        "rollingUpdate": { "maxUnavailable": 1, "maxSurge": 1 }
                    },
                    "template": pod_template()
                }
            }),
        ),
        f(
            "apps",
            "v1",
            "replicasets",
            json!({
                "metadata": { "name": "sweep-rs" },
                "spec": {
                    "replicas": 2,
                    "minReadySeconds": 5,
                    "selector": { "matchLabels": { "app": "sweep" } },
                    "template": pod_template()
                }
            }),
        ),
        f(
            "apps",
            "v1",
            "statefulsets",
            json!({
                "metadata": { "name": "sweep-sts" },
                "spec": {
                    "replicas": 2,
                    "revisionHistoryLimit": 5,
                    "minReadySeconds": 5,
                    "serviceName": "sweep-svc",
                    "podManagementPolicy": "OrderedReady",
                    "selector": { "matchLabels": { "app": "sweep" } },
                    "updateStrategy": {
                        "type": "RollingUpdate",
                        "rollingUpdate": { "partition": 0, "maxUnavailable": 1 }
                    },
                    "template": pod_template()
                }
            }),
        ),
        // ValidateRollingUpdateDaemonSet forbids both maxUnavailable and
        // maxSurge being non-zero, so one of the pair must be 0 — which still
        // exercises the number path for both fields.
        f(
            "apps",
            "v1",
            "daemonsets",
            json!({
                "metadata": { "name": "sweep-ds" },
                "spec": {
                    "revisionHistoryLimit": 5,
                    "minReadySeconds": 5,
                    "selector": { "matchLabels": { "app": "sweep" } },
                    "updateStrategy": {
                        "type": "RollingUpdate",
                        "rollingUpdate": { "maxUnavailable": 1, "maxSurge": 0 }
                    },
                    "template": pod_template()
                }
            }),
        ),
        f(
            "apps",
            "v1",
            "controllerrevisions",
            json!({
                "metadata": { "name": "sweep-cr" },
                "revision": 1,
                "data": { "spec": { "replicas": 1 } }
            }),
        ),
        // ---- batch/v1 ----------------------------------------------------
        f(
            "batch",
            "v1",
            "jobs",
            json!({
                "metadata": { "name": "sweep-job" },
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "backoffLimit": 4,
                    "activeDeadlineSeconds": 300,
                    "ttlSecondsAfterFinished": 100,
                    "template": {
                        "metadata": { "labels": { "app": "sweep" } },
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [{
                                "name": "app",
                                "image": "registry.k8s.io/pause:3.10",
                                "ports": [{ "containerPort": 8080 }]
                            }]
                        }
                    }
                }
            }),
        ),
        f(
            "batch",
            "v1",
            "cronjobs",
            json!({
                "metadata": { "name": "sweep-cronjob" },
                "spec": {
                    "schedule": "*/5 * * * *",
                    "startingDeadlineSeconds": 30,
                    "successfulJobsHistoryLimit": 3,
                    "failedJobsHistoryLimit": 1,
                    "concurrencyPolicy": "Allow",
                    "suspend": false,
                    "jobTemplate": { "spec": {
                        "backoffLimit": 4,
                        "template": {
                            "metadata": { "labels": { "app": "sweep" } },
                            "spec": {
                                "restartPolicy": "OnFailure",
                                "containers": [{
                                    "name": "app",
                                    "image": "registry.k8s.io/pause:3.10",
                                    "ports": [{ "containerPort": 8080 }]
                                }]
                            }
                        }
                    } }
                }
            }),
        ),
        // ---- networking.k8s.io/v1 ---------------------------------------
        f(
            "networking.k8s.io",
            "v1",
            "ingresses",
            json!({
                "metadata": { "name": "sweep-ingress" },
                "spec": {
                    "ingressClassName": "sweep",
                    "rules": [{
                        "host": "sweep.example.com",
                        "http": { "paths": [{
                            "path": "/", "pathType": "Prefix",
                            "backend": { "service": {
                                "name": "sweep-svc",
                                // ServiceBackendPort.number is an int32, not an
                                // IntOrString — a string here is a decode error.
                                "port": { "number": 80 }
                            } }
                        }] }
                    }]
                }
            }),
        ),
        f(
            "networking.k8s.io",
            "v1",
            "ingressclasses",
            json!({
                "metadata": { "name": "sweep-ingressclass" },
                "spec": { "controller": "example.com/ingress-controller" }
            }),
        ),
        f(
            "networking.k8s.io",
            "v1",
            "networkpolicies",
            json!({
                "metadata": { "name": "sweep-netpol" },
                "spec": {
                    "podSelector": { "matchLabels": { "app": "sweep" } },
                    "policyTypes": ["Ingress", "Egress"],
                    "ingress": [{
                        "ports": [
                            // NetworkPolicyPort.port is an IntOrString; endPort an int32.
                            { "protocol": "TCP", "port": 6379, "endPort": 7000 },
                            { "protocol": "TCP", "port": "http" }
                        ],
                        "from": [{ "podSelector": { "matchLabels": { "role": "client" } } }]
                    }],
                    "egress": [{ "ports": [{ "protocol": "UDP", "port": 53 }] }]
                }
            }),
        ),
        f(
            "networking.k8s.io",
            "v1",
            "servicecidrs",
            json!({
                "metadata": { "name": "sweep-servicecidr" },
                "spec": { "cidrs": ["10.97.0.0/16"] }
            }),
        ),
        f(
            "networking.k8s.io",
            "v1",
            "ipaddresses",
            json!({
                "metadata": { "name": "10.97.0.7" },
                "spec": { "parentRef": {
                    "group": "", "resource": "services",
                    "namespace": NS, "name": "sweep-svc"
                } }
            }),
        ),
        // ---- rbac.authorization.k8s.io/v1 --------------------------------
        f(
            "rbac.authorization.k8s.io",
            "v1",
            "roles",
            json!({
                "metadata": { "name": "sweep-role" },
                "rules": [{
                    "apiGroups": [""], "resources": ["pods"],
                    "verbs": ["get", "list", "watch"]
                }]
            }),
        ),
        f(
            "rbac.authorization.k8s.io",
            "v1",
            "rolebindings",
            json!({
                "metadata": { "name": "sweep-rolebinding" },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role", "name": "sweep-role"
                },
                "subjects": [{ "kind": "ServiceAccount", "name": "sweep-sa", "namespace": NS }]
            }),
        ),
        f(
            "rbac.authorization.k8s.io",
            "v1",
            "clusterroles",
            json!({
                "metadata": { "name": "sweep-clusterrole" },
                "rules": [{
                    "apiGroups": [""], "resources": ["nodes"],
                    "verbs": ["get", "list"]
                }]
            }),
        ),
        f(
            "rbac.authorization.k8s.io",
            "v1",
            "clusterrolebindings",
            json!({
                "metadata": { "name": "sweep-clusterrolebinding" },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole", "name": "sweep-clusterrole"
                },
                "subjects": [{ "kind": "ServiceAccount", "name": "sweep-sa", "namespace": NS }]
            }),
        ),
        // ---- storage.k8s.io/v1 ------------------------------------------
        f(
            "storage.k8s.io",
            "v1",
            "storageclasses",
            json!({
                "metadata": { "name": "sweep-sc" },
                "provisioner": "example.com/csi",
                "reclaimPolicy": "Delete",
                "volumeBindingMode": "Immediate",
                "allowVolumeExpansion": true,
                "parameters": { "type": "ssd" }
            }),
        ),
        f(
            "storage.k8s.io",
            "v1",
            "csidrivers",
            json!({
                "metadata": { "name": "sweep.csi.example.com" },
                "spec": {
                    "attachRequired": true,
                    "podInfoOnMount": false,
                    "fsGroupPolicy": "File",
                    "volumeLifecycleModes": ["Persistent"]
                }
            }),
        ),
        f(
            "storage.k8s.io",
            "v1",
            "csinodes",
            json!({
                "metadata": { "name": "sweep-csinode" },
                "spec": { "drivers": [{
                    "name": "sweep.csi.example.com",
                    "nodeID": "sweep-node",
                    // CSINodeDriver.allocatable.count is an int32.
                    "allocatable": { "count": 10 },
                    "topologyKeys": ["topology.kubernetes.io/zone"]
                }] }
            }),
        ),
        f(
            "storage.k8s.io",
            "v1",
            "csistoragecapacities",
            json!({
                "metadata": { "name": "sweep-csicapacity" },
                "storageClassName": "sweep-sc",
                "capacity": "10Gi",
                "maximumVolumeSize": "5Gi"
            }),
        ),
        f(
            "storage.k8s.io",
            "v1",
            "volumeattachments",
            json!({
                "metadata": { "name": "sweep-volumeattachment" },
                "spec": {
                    "attacher": "sweep.csi.example.com",
                    "nodeName": "sweep-node",
                    "source": { "persistentVolumeName": "sweep-pv" }
                }
            }),
        ),
        f(
            "storage.k8s.io",
            "v1",
            "volumeattributesclasses",
            json!({
                "metadata": { "name": "sweep-vac" },
                "driverName": "sweep.csi.example.com",
                "parameters": { "iops": "3000" }
            }),
        ),
        // ---- snapshot.storage.k8s.io/v1 ---------------------------------
        f(
            "snapshot.storage.k8s.io",
            "v1",
            "volumesnapshotclasses",
            json!({
                "metadata": { "name": "sweep-vsclass" },
                "driver": "sweep.csi.example.com",
                "deletionPolicy": "Delete"
            }),
        ),
        f(
            "snapshot.storage.k8s.io",
            "v1",
            "volumesnapshots",
            json!({
                "metadata": { "name": "sweep-vs" },
                "spec": {
                    "volumeSnapshotClassName": "sweep-vsclass",
                    "source": { "persistentVolumeClaimName": "sweep-pvc" }
                }
            }),
        ),
        f(
            "snapshot.storage.k8s.io",
            "v1",
            "volumesnapshotcontents",
            json!({
                "metadata": { "name": "sweep-vscontent" },
                "spec": {
                    "deletionPolicy": "Delete",
                    "driver": "sweep.csi.example.com",
                    "volumeSnapshotClassName": "sweep-vsclass",
                    "source": { "snapshotHandle": "snap-1" },
                    "volumeSnapshotRef": {
                        "kind": "VolumeSnapshot", "name": "sweep-vs", "namespace": NS
                    }
                }
            }),
        ),
        // ---- scheduling.k8s.io/v1 ---------------------------------------
        f(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            json!({
                "metadata": { "name": "sweep-priorityclass" },
                // PriorityClass.value is an int32 and the whole point of the type.
                "value": 1000,
                "globalDefault": false,
                "preemptionPolicy": "PreemptLowerPriority",
                "description": "wide serialization sweep"
            }),
        ),
        // ---- apiextensions.k8s.io/v1 ------------------------------------
        // The OpenAPI schema is the densest numeric surface in the API:
        // maximum/minimum are float64, maxLength/minLength/maxItems are int64.
        f(
            "apiextensions.k8s.io",
            "v1",
            "customresourcedefinitions",
            json!({
                "metadata": { "name": "sweeps.example.com" },
                "spec": {
                    "group": "example.com",
                    "scope": "Namespaced",
                    "names": {
                        "plural": "sweeps", "singular": "sweep",
                        "kind": "Sweep", "listKind": "SweepList"
                    },
                    "versions": [{
                        "name": "v1", "served": true, "storage": true,
                        "schema": { "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "properties": {
                                        "size": {
                                            "type": "integer",
                                            "maximum": 10, "minimum": 1
                                        },
                                        "name": {
                                            "type": "string",
                                            "maxLength": 32, "minLength": 1
                                        },
                                        "tags": {
                                            "type": "array",
                                            "maxItems": 5,
                                            "items": { "type": "string" }
                                        }
                                    }
                                }
                            }
                        } }
                    }]
                }
            }),
        ),
        // ---- admissionregistration.k8s.io/v1 ----------------------------
        f(
            "admissionregistration.k8s.io",
            "v1",
            "validatingwebhookconfigurations",
            json!({
                "metadata": { "name": "sweep-validating" },
                "webhooks": [{
                    "name": "sweep.example.com",
                    "admissionReviewVersions": ["v1"],
                    "sideEffects": "None",
                    "failurePolicy": "Ignore",
                    "matchPolicy": "Equivalent",
                    // WebhookClientConfig.service.port is an int32.
                    "timeoutSeconds": 10,
                    "clientConfig": { "service": {
                        "name": "sweep-webhook", "namespace": NS,
                        "path": "/validate", "port": 8443
                    } },
                    "rules": [{
                        "apiGroups": [""], "apiVersions": ["v1"],
                        "operations": ["CREATE"], "resources": ["pods"],
                        "scope": "Namespaced"
                    }]
                }]
            }),
        ),
        f(
            "admissionregistration.k8s.io",
            "v1",
            "mutatingwebhookconfigurations",
            json!({
                "metadata": { "name": "sweep-mutating" },
                "webhooks": [{
                    "name": "sweep.example.com",
                    "admissionReviewVersions": ["v1"],
                    "sideEffects": "None",
                    "failurePolicy": "Ignore",
                    "reinvocationPolicy": "Never",
                    "timeoutSeconds": 10,
                    "clientConfig": { "service": {
                        "name": "sweep-webhook", "namespace": NS,
                        "path": "/mutate", "port": 8443
                    } },
                    "rules": [{
                        "apiGroups": [""], "apiVersions": ["v1"],
                        "operations": ["CREATE"], "resources": ["pods"],
                        "scope": "Namespaced"
                    }]
                }]
            }),
        ),
        f(
            "admissionregistration.k8s.io",
            "v1",
            "validatingadmissionpolicies",
            json!({
                "metadata": { "name": "sweep-vap" },
                "spec": {
                    "failurePolicy": "Fail",
                    "matchConstraints": { "resourceRules": [{
                        "apiGroups": ["apps"], "apiVersions": ["v1"],
                        "operations": ["CREATE", "UPDATE"], "resources": ["deployments"]
                    }] },
                    "variables": [{ "name": "replicas", "expression": "object.spec.replicas" }],
                    "validations": [{
                        "expression": "variables.replicas <= 5",
                        "message": "no more than 5 replicas",
                        "reason": "Invalid"
                    }]
                }
            }),
        ),
        f(
            "admissionregistration.k8s.io",
            "v1",
            "validatingadmissionpolicybindings",
            json!({
                "metadata": { "name": "sweep-vapbinding" },
                "spec": {
                    "policyName": "sweep-vap",
                    "validationActions": ["Deny"],
                    "matchResources": { "namespaceSelector": { "matchLabels": { "app": "sweep" } } }
                }
            }),
        ),
        // ---- coordination.k8s.io/v1 -------------------------------------
        // acquireTime and renewTime are both metav1.MicroTime: the only
        // resource other than Event that pins the 6-digit branch.
        f(
            "coordination.k8s.io",
            "v1",
            "leases",
            json!({
                "metadata": { "name": "sweep-lease" },
                "spec": {
                    "holderIdentity": "sweep-holder",
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 3,
                    "acquireTime": "2026-09-08T10:00:00.000000Z",
                    "renewTime": "2026-09-08T10:00:10.500000Z"
                }
            }),
        ),
        // ---- flowcontrol.apiserver.k8s.io/v1 ----------------------------
        f(
            "flowcontrol.apiserver.k8s.io",
            "v1",
            "flowschemas",
            json!({
                "metadata": { "name": "sweep-flowschema" },
                "spec": {
                    "matchingPrecedence": 1000,
                    "priorityLevelConfiguration": { "name": "sweep-plc" },
                    "distinguisherMethod": { "type": "ByUser" },
                    "rules": [{
                        "subjects": [{ "kind": "Group", "group": { "name": "system:authenticated" } }],
                        "resourceRules": [{
                            "apiGroups": ["*"], "resources": ["*"],
                            "verbs": ["*"], "clusterScope": true, "namespaces": ["*"]
                        }]
                    }]
                }
            }),
        ),
        f(
            "flowcontrol.apiserver.k8s.io",
            "v1",
            "prioritylevelconfigurations",
            json!({
                "metadata": { "name": "sweep-plc" },
                "spec": {
                    "type": "Limited",
                    "limited": {
                        "nominalConcurrencyShares": 10,
                        "lendablePercent": 20,
                        "borrowingLimitPercent": 50,
                        "limitResponse": {
                            "type": "Queue",
                            "queuing": { "queues": 64, "handSize": 6, "queueLengthLimit": 50 }
                        }
                    }
                }
            }),
        ),
        // ---- certificates.k8s.io/v1 -------------------------------------
        f(
            "certificates.k8s.io",
            "v1",
            "certificatesigningrequests",
            json!({
                "metadata": { "name": "sweep-csr" },
                "spec": {
                    "signerName": "kubernetes.io/kube-apiserver-client",
                    "expirationSeconds": 86400,
                    "usages": ["client auth"],
                    "request": SWEEP_CSR_PEM_B64
                }
            }),
        ),
        // ---- discovery.k8s.io/v1 ----------------------------------------
        f(
            "discovery.k8s.io",
            "v1",
            "endpointslices",
            json!({
                "metadata": { "name": "sweep-endpointslice", "labels": {
                    "kubernetes.io/service-name": "sweep-svc"
                } },
                "addressType": "IPv4",
                "endpoints": [{
                    "addresses": ["10.244.9.5"],
                    "conditions": { "ready": true, "serving": true, "terminating": false }
                }],
                "ports": [{ "name": "http", "port": 8080, "protocol": "TCP" }]
            }),
        ),
        // ---- node.k8s.io/v1 ---------------------------------------------
        f(
            "node.k8s.io",
            "v1",
            "runtimeclasses",
            json!({
                "metadata": { "name": "sweep-runtimeclass" },
                "handler": "runc",
                "overhead": { "podFixed": { "cpu": "50m", "memory": "64Mi" } },
                "scheduling": { "nodeSelector": { "kubernetes.io/os": "linux" } }
            }),
        ),
        // ---- autoscaling ------------------------------------------------
        f(
            "autoscaling",
            "v1",
            "horizontalpodautoscalers",
            json!({
                "metadata": { "name": "sweep-hpa-v1" },
                "spec": {
                    "scaleTargetRef": {
                        "apiVersion": "apps/v1", "kind": "Deployment", "name": "sweep-deploy"
                    },
                    "minReplicas": 1,
                    "maxReplicas": 5,
                    "targetCPUUtilizationPercentage": 80
                }
            }),
        ),
        f(
            "autoscaling",
            "v2",
            "horizontalpodautoscalers",
            json!({
                "metadata": { "name": "sweep-hpa-v2" },
                "spec": {
                    "scaleTargetRef": {
                        "apiVersion": "apps/v1", "kind": "Deployment", "name": "sweep-deploy"
                    },
                    "minReplicas": 1,
                    "maxReplicas": 5,
                    "metrics": [{
                        "type": "Resource",
                        "resource": {
                            "name": "cpu",
                            "target": { "type": "Utilization", "averageUtilization": 80 }
                        }
                    }],
                    "behavior": { "scaleDown": {
                        "stabilizationWindowSeconds": 300,
                        "selectPolicy": "Max",
                        "policies": [{ "type": "Percent", "value": 25, "periodSeconds": 60 }]
                    } }
                }
            }),
        ),
        // ---- policy/v1 --------------------------------------------------
        f(
            "policy",
            "v1",
            "poddisruptionbudgets",
            json!({
                "metadata": { "name": "sweep-pdb" },
                "spec": {
                    // PDB minAvailable is an IntOrString: this is the same field
                    // family that broke the DaemonSet rollout in the swap leg.
                    "minAvailable": 1,
                    "unhealthyPodEvictionPolicy": "IfHealthyBudget",
                    "selector": { "matchLabels": { "app": "sweep" } }
                }
            }),
        ),
        // ---- resource.k8s.io/v1 (DRA) -----------------------------------
        f(
            "resource.k8s.io",
            "v1",
            "resourceclaims",
            json!({
                "metadata": { "name": "sweep-resourceclaim" },
                "spec": { "devices": { "requests": [{
                    "name": "gpu",
                    "exactly": {
                        "deviceClassName": "sweep-deviceclass",
                        "allocationMode": "ExactCount",
                        "count": 1
                    }
                }] } }
            }),
        ),
        f(
            "resource.k8s.io",
            "v1",
            "resourceclaimtemplates",
            json!({
                "metadata": { "name": "sweep-resourceclaimtemplate" },
                "spec": { "spec": { "devices": { "requests": [{
                    "name": "gpu",
                    "exactly": {
                        "deviceClassName": "sweep-deviceclass",
                        "allocationMode": "ExactCount",
                        "count": 1
                    }
                }] } } }
            }),
        ),
        f(
            "resource.k8s.io",
            "v1",
            "deviceclasses",
            json!({
                "metadata": { "name": "sweep-deviceclass" },
                "spec": { "selectors": [{ "cel": {
                    "expression": "device.attributes[\"example.com\"].model == \"sweep\""
                } }] }
            }),
        ),
        f(
            "resource.k8s.io",
            "v1",
            "resourceslices",
            json!({
                "metadata": { "name": "sweep-resourceslice" },
                "spec": {
                    "driver": "sweep.example.com",
                    "nodeName": "sweep-node",
                    "pool": {
                        "name": "sweep-pool",
                        // Both int64 on the wire.
                        "generation": 1,
                        "resourceSliceCount": 1
                    },
                    "devices": [{ "name": "gpu-0" }]
                }
            }),
        ),
        // ---- events.k8s.io/v1 -------------------------------------------
        // The newer Event: eventTime is REQUIRED here, and deprecatedCount /
        // series.count are the int32 pair.
        f(
            "events.k8s.io",
            "v1",
            "events",
            json!({
                "metadata": { "name": "sweep-event-v1" },
                "eventTime": "2026-09-08T10:05:00.123456Z",
                "reportingController": "sweep",
                "reportingInstance": "sweep-0",
                "action": "Sweep",
                "reason": "Sweep",
                "type": "Normal",
                "note": "wide serialization sweep",
                "regarding": { "kind": "Pod", "name": "sweep-pod", "namespace": NS },
                // No deprecatedCount / deprecatedFirstTimestamp /
                // deprecatedLastTimestamp: they map onto core Event's Count /
                // FirstTimestamp / LastTimestamp, and the strict branch of
                // ValidateEventCreate (events.go:60-68) requires each to be
                // unset when creating through events.k8s.io/v1. `series.count`
                // still covers the int32 path here, and it must be >= 2
                // (validateV1EventSeries).
                "series": { "count": 3, "lastObservedTime": "2026-09-08T10:06:00.654321Z" }
            }),
        ),
        // ---- apiregistration.k8s.io/v1 ----------------------------------
        f(
            "apiregistration.k8s.io",
            "v1",
            "apiservices",
            json!({
                "metadata": { "name": "v1beta1.sweep.example.com" },
                "spec": {
                    "group": "sweep.example.com",
                    "version": "v1beta1",
                    "groupPriorityMinimum": 1000,
                    "versionPriority": 15,
                    "insecureSkipTLSVerify": true,
                    "service": { "name": "sweep-svc", "namespace": NS, "port": 443 }
                }
            }),
        ),
        // ---- the six create-only review kinds ---------------------------
        f(
            "authentication.k8s.io",
            "v1",
            "tokenreviews",
            json!({
                "spec": { "token": "not-a-real-token", "audiences": ["https://kubernetes.default.svc"] }
            }),
        ),
        f(
            "authentication.k8s.io",
            "v1",
            "selfsubjectreviews",
            json!({ "spec": {} }),
        ),
        f(
            "authorization.k8s.io",
            "v1",
            "subjectaccessreviews",
            json!({
                "spec": {
                    "user": "sweep-user",
                    "groups": ["system:authenticated"],
                    "resourceAttributes": {
                        "namespace": NS, "verb": "get", "group": "", "resource": "pods",
                        "name": "sweep-pod"
                    }
                }
            }),
        ),
        f(
            "authorization.k8s.io",
            "v1",
            "selfsubjectaccessreviews",
            json!({
                "spec": { "resourceAttributes": {
                    "namespace": NS, "verb": "get", "group": "", "resource": "pods"
                } }
            }),
        ),
        f(
            "authorization.k8s.io",
            "v1",
            "localsubjectaccessreviews",
            json!({
                "spec": {
                    "user": "sweep-user",
                    "resourceAttributes": {
                        "namespace": NS, "verb": "get", "group": "", "resource": "pods"
                    }
                }
            }),
        ),
        f(
            "authorization.k8s.io",
            "v1",
            "selfsubjectrulesreviews",
            json!({
                "spec": { "namespace": NS }
            }),
        ),
    ]
}

/// A syntactically valid PKCS#10 CSR, PEM then base64 — `spec.request` is
/// parsed by `ValidateCertificateSigningRequestCreate`, so a placeholder is
/// rejected. Generated once with `openssl req -new -newkey rsa:2048 -nodes`
/// and pinned here; it signs nothing and expires never.
const SWEEP_CSR_PEM_B64: &str = concat!(
    "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlJQ2REQ0NBVndDQVFBd0x6",
    "RU9NQXdHQTFVRUF3d0ZjM2RsWlhBeEhUQWJCZ05WQkFvTUZITjVjM1JsYlRwaApkWFJvWlc1",
    "MGFXTmhkR1ZrTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUFy",
    "NnBKCk5oa1VFbURIRGZZZjhHclg0NGxheGlEYUpMTy9QY3I2V2FYWU5SZ3Y0ZFpGRU5IMC8r",
    "ZExmOTMxNit5WWJqc1MKL0ZIU3V5VEJObXVYMEVEWVRGMFZTeU8vM2x4NytsdnJ4TmxZaWV1",
    "eVhUdzE5RzB3OW1xUzg4UmtVWFRvSUNkYword0NtWFFUS1BML3lLZzlnYnJHYnowTWxRSGVm",
    "MWhYVjBRSVRMdElxUE9Cejk5Sm9ZN21CcTlxK2kwUmdEczgzCmorbHZjL01kQXRzN2hpYzFP",
    "RlFMYXVJYXZQWE5TYU1NRzFFNmxJRU1pcm9FWFUvT3BmZE81a1FBcGdvVVF2Wk8KV1VhakFs",
    "aFVRTkNMdDJLa2NDbGxvU2tzWjhudXB5T3JWUllrTEVwY29Ld3VmOVkxa2lZTGNXWE1mQ3hJ",
    "NVQyZQpFSllaMHoxdlhSTkxCbkI5NlFJREFRQUJvQUF3RFFZSktvWklodmNOQVFFTEJRQURn",
    "Z0VCQUNNaVJWc2NpOEtXCkZBYTVDR3NRN2x2bSs0RnNFZEZ1V1FMVlhFZWVEemN4R2lPaTdJ",
    "MkVEblZhK1VLRDltcnNFTE5IZERoTEdvSjgKN0hkbm9JeXhUbmthOHg4ZDFzYVQydEl5bFR3",
    "TXZvc05jY3Y0OHRGbklmT0tBQ0d4K2cySU8zZG85RkJydzVYSwpMZ2VEMVlsWks1Y0NSVjFC",
    "UHRWRlM5ZFprRzBlM1F3aHNzVHNQdTBKNVZyaEJyL2s4ZmdHSHhCY0RrWFgrZ2pMCmhjQWlm",
    "UDNuQ2x1NWFvWFBWc2pZZHJSVGMzR01oNGNMb1I2K3lyVU5qQkRyTUhJM2hPMys4UzlqSzJH",
    "allLT3QKT0lKd1ViTm1ST1U1RXRKKzIxeHgwVW1MQW1LWmE2R1lTL0Fxd1l5WDYrT1oveXll",
    "eGJTeHljc2dKbS94V1MycApyNlc5bmNSQ3VsMD0KLS0tLS1FTkQgQ0VSVElGSUNBVEUgUkVR",
    "VUVTVC0tLS0tCg==",
);

// ---------------------------------------------------------------------------
// the sweep
// ---------------------------------------------------------------------------

/// `(group, version, resource)` as discovery reports it.
type Gvr = (String, String, String);

/// What the server itself says about a creatable resource.
struct Creatable {
    namespaced: bool,
    /// Discovery verbs are exactly `["create"]` for the review-style kinds:
    /// they are computed and returned, never stored, so there is nothing to
    /// read back and no `creationTimestamp` to check. Taken from the verb
    /// list rather than a hand-kept name list.
    persisted: bool,
}

async fn creatable_resources(s: &TestApiServer) -> BTreeMap<Gvr, Creatable> {
    let mut gvs: Vec<(String, String)> = vec![(String::new(), "v1".to_string())];
    let (st, apis) = s.get("/apis").await;
    assert!(st.is_success(), "GET /apis: {st} {apis}");
    for g in apis["groups"].as_array().cloned().unwrap_or_default() {
        let name = g["name"].as_str().unwrap_or_default().to_string();
        for v in g["versions"].as_array().cloned().unwrap_or_default() {
            gvs.push((
                name.clone(),
                v["version"].as_str().unwrap_or_default().to_string(),
            ));
        }
    }

    let mut out = BTreeMap::new();
    for (group, version) in gvs {
        let uri = if group.is_empty() {
            format!("/api/{version}")
        } else {
            format!("/apis/{group}/{version}")
        };
        let (st, list) = s.get(&uri).await;
        assert!(
            st.is_success(),
            "GET {uri} must serve an APIResourceList — it is advertised by /apis: {st} {list}"
        );
        for r in list["resources"].as_array().cloned().unwrap_or_default() {
            let name = r["name"].as_str().unwrap_or_default();
            // Subresources (`pods/status`, `deployments/scale`) are not
            // separately creatable objects.
            if name.contains('/') {
                continue;
            }
            let verbs: BTreeSet<&str> = r["verbs"]
                .as_array()
                .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                .unwrap_or_default();
            if !verbs.contains("create") {
                continue;
            }
            out.insert(
                (group.clone(), version.clone(), name.to_string()),
                Creatable {
                    namespaced: r["namespaced"].as_bool().unwrap_or(false),
                    persisted: verbs.contains("get"),
                },
            );
        }
    }
    out
}

fn collection_uri(group: &str, version: &str, resource: &str, ns: Option<&str>) -> String {
    let root = if group.is_empty() {
        format!("/api/{version}")
    } else {
        format!("/apis/{group}/{version}")
    };
    match ns {
        Some(ns) => format!("{root}/namespaces/{ns}/{resource}"),
        None => format!("{root}/{resource}"),
    }
}

/// Every creatable resource must have a fixture, and every fixture must name a
/// creatable resource. Neither direction is allowed to drift: a new resource
/// with no fixture is unswept, and a stale fixture is a test that silently
/// stopped covering anything.
#[tokio::test]
async fn every_creatable_resource_has_a_sweep_fixture() {
    let s = TestApiServer::new();
    let discovered = creatable_resources(&s).await;
    assert!(
        discovered.len() > 60,
        "discovery returned only {} creatable resources — the enumeration broke, \
         and an empty sweep would pass vacuously",
        discovered.len()
    );

    let have: BTreeSet<Gvr> = fixtures()
        .iter()
        .map(|f| {
            (
                f.group.to_string(),
                f.version.to_string(),
                f.resource.to_string(),
            )
        })
        .collect();
    let want: BTreeSet<Gvr> = discovered.keys().cloned().collect();

    let missing: Vec<String> = want
        .difference(&have)
        .map(|(g, v, r)| format!("{}/{v} {r}", if g.is_empty() { "core" } else { g }))
        .collect();
    let stale: Vec<String> = have
        .difference(&want)
        .map(|(g, v, r)| format!("{}/{v} {r}", if g.is_empty() { "core" } else { g }))
        .collect();

    assert!(
        missing.is_empty(),
        "these creatable resources have no fixture in `fixtures()`, so the wide \
         serialization sweep does not cover them. Add a body that spells out its \
         integer and IntOrString fields as JSON numbers:\n  {}",
        missing.join("\n  ")
    );
    assert!(
        stale.is_empty(),
        "these fixtures name a resource discovery no longer reports as creatable — \
         they cover nothing:\n  {}",
        stale.join("\n  ")
    );
}

/// The sweep proper: create each fixture, then assert both invariants on the
/// create response and, for persisted kinds, on the GET read-back too. The
/// create path and the storage decode path are different code, and either can
/// lose the type on its own.
#[tokio::test]
async fn creating_every_resource_preserves_json_types_and_timestamp_precision() {
    let s = TestApiServer::new();
    let discovered = creatable_resources(&s).await;

    let (st, body) = s
        .post("/api/v1/namespaces", &json!({ "metadata": { "name": NS } }))
        .await;
    assert!(
        st.is_success(),
        "the sweep namespace must be creatable: {st} {body}"
    );

    let mut failures: Vec<String> = Vec::new();
    let mut swept = 0usize;

    for fx in fixtures() {
        let key = (
            fx.group.to_string(),
            fx.version.to_string(),
            fx.resource.to_string(),
        );
        let Some(meta) = discovered.get(&key) else {
            // The completeness test above owns this; skipping here keeps the
            // two failures from being reported as one confusing blob.
            continue;
        };
        let ns = if meta.namespaced { Some(NS) } else { None };
        let uri = collection_uri(fx.group, fx.version, fx.resource, ns);
        let ctx = format!(
            "{}/{} {}",
            if fx.group.is_empty() {
                "core"
            } else {
                fx.group
            },
            fx.version,
            fx.resource
        );

        let (st, created) = s.post(&uri, &fx.body).await;
        if !st.is_success() {
            failures.push(format!(
                "{ctx}: POST {uri} returned {st}, so nothing about its serialized form \
                 could be checked: {created}"
            ));
            continue;
        }
        swept += 1;

        check_type_parity(
            &fx.body,
            &created,
            &format!("{ctx} (create response)"),
            &mut failures,
        );
        check_timestamps(
            &created,
            &format!("{ctx} (create response)"),
            "",
            &mut failures,
        );

        if !meta.persisted {
            continue;
        }

        // Upstream stamps creationTimestamp in the registry store for every
        // persisted object; a missing one would also make invariant 2 vacuous
        // for this resource.
        let created_ts = created
            .get("metadata")
            .and_then(|m| m.get("creationTimestamp"));
        match created_ts {
            Some(Value::String(_)) => {}
            other => failures.push(format!(
                "{ctx}: metadata.creationTimestamp is {other:?} — the server stamps it \
                 on create for every persisted object, and without it this resource's \
                 timestamp format is never exercised"
            )),
        }

        let name = fx.body["metadata"]["name"].as_str().unwrap_or_default();
        let (st, fetched) = s.get(&format!("{uri}/{name}")).await;
        if !st.is_success() {
            failures.push(format!("{ctx}: GET {uri}/{name} returned {st}: {fetched}"));
            continue;
        }
        check_type_parity(
            &fx.body,
            &fetched,
            &format!("{ctx} (read-back)"),
            &mut failures,
        );
        check_timestamps(&fetched, &format!("{ctx} (read-back)"), "", &mut failures);
    }

    // Report the defects before the vacuity check: a POST that 4xx'd lands in
    // `failures` AND lowers `swept`, and the count alone says nothing about
    // which resource broke.
    assert!(
        failures.is_empty(),
        "{} serialization defect(s) across {swept} created resources:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert!(
        swept > 60,
        "only {swept} resources were actually created — the sweep would pass vacuously"
    );
}

/// The detectors must fail on the shapes they exist to catch, or the sweep
/// above proves nothing. Breaking a real field to demonstrate that is not
/// possible here — the fixtures are data, not types — so pin the detectors
/// directly, including the cases they must NOT flag.
#[test]
fn the_detectors_flag_exactly_the_wrong_shapes() {
    // --- type parity ---
    let mut out = Vec::new();
    check_type_parity(
        &json!({ "port": 1 }),
        &json!({ "port": "1" }),
        "x",
        &mut out,
    );
    assert_eq!(out.len(), 1, "number -> string must be flagged: {out:?}");
    assert!(out[0].contains("IntOrString"), "{out:?}");

    let mut out = Vec::new();
    check_type_parity(
        &json!({ "port": "1" }),
        &json!({ "port": 1 }),
        "x",
        &mut out,
    );
    assert_eq!(
        out.len(),
        1,
        "string -> number must be flagged too: {out:?}"
    );

    let mut out = Vec::new();
    check_type_parity(&json!({ "n": 10 }), &json!({ "n": 10.0 }), "x", &mut out);
    assert_eq!(out.len(), 1, "integer -> float must be flagged: {out:?}");

    let mut out = Vec::new();
    check_type_parity(
        &json!({ "a": { "b": [ { "c": 1 } ] } }),
        &json!({ "a": { "b": [ { "c": "1" } ] } }),
        "x",
        &mut out,
    );
    assert_eq!(out.len(), 1, "nested/array paths must be walked: {out:?}");

    let mut out = Vec::new();
    check_type_parity(
        &json!({ "keep": 1, "dropped": 2 }),
        &json!({ "keep": 1, "added": "x", "status": {} }),
        "x",
        &mut out,
    );
    assert!(
        out.is_empty(),
        "server-added and server-dropped paths are not type changes: {out:?}"
    );

    // --- timestamps ---
    let mut out = Vec::new();
    check_timestamps(
        &json!({ "metadata": { "creationTimestamp": "2026-09-08T10:00:00.123456789Z" } }),
        "x",
        "",
        &mut out,
    );
    assert_eq!(out.len(), 1, "nanoseconds on a metav1.Time: {out:?}");
    assert!(out[0].contains("no fractional part"), "{out:?}");

    let mut out = Vec::new();
    check_timestamps(
        &json!({ "renewTime": "2026-09-08T10:00:00Z" }),
        "x",
        "",
        &mut out,
    );
    assert_eq!(out.len(), 1, "a MicroTime needs exactly 6 digits: {out:?}");

    let mut out = Vec::new();
    check_timestamps(
        &json!({ "renewTime": "2026-09-08T10:00:00.000000Z" }),
        "x",
        "",
        &mut out,
    );
    assert!(out.is_empty(), "6-digit MicroTime is correct: {out:?}");

    let mut out = Vec::new();
    check_timestamps(
        &json!({ "creationTimestamp": "2026-09-08T10:00:00+02:00" }),
        "x",
        "",
        &mut out,
    );
    assert_eq!(out.len(), 1, "a non-UTC offset must be flagged: {out:?}");

    // An array of timestamps inherits the key that holds it, not the index.
    let mut out = Vec::new();
    check_timestamps(
        &json!({ "renewTime": ["2026-09-08T10:00:00Z"] }),
        "x",
        "",
        &mut out,
    );
    assert_eq!(
        out.len(),
        1,
        "array elements must be typed by the parent key: {out:?}"
    );

    let mut out = Vec::new();
    check_timestamps(
        &json!({ "image": "registry.k8s.io/pause:3.10" }),
        "x",
        "",
        &mut out,
    );
    assert!(
        out.is_empty(),
        "non-timestamp strings must be ignored: {out:?}"
    );
}
