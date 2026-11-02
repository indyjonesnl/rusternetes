//! Regression guard for the stock-RBAC contract of an api-server-mode
//! scheduler (#3794 follow-up: a PriorityClass reflector was permanently
//! Forbidden under stock `system:kube-scheduler`, so the cache-sync wait never
//! completed and nothing was scheduled).
//!
//! Every `/api...` collection path the scheduler source mentions must name a
//! resource covered by upstream's `kubeSchedulerRules` plus
//! `system:volume-scheduler`
//! (plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go, release-1.35).
//! Notably absent upstream: `priorityclasses`.

use std::collections::BTreeSet;
use std::path::Path;

/// Resources readable by stock `system:kube-scheduler` / `system:volume-scheduler`.
const ALLOWED: &[&str] = &[
    "nodes",
    "pods",
    "bindings",
    "services",
    "replicationcontrollers",
    "replicasets",
    "statefulsets",
    "poddisruptionbudgets",
    "persistentvolumeclaims",
    "persistentvolumes",
    "csinodes",
    "volumeattachments",
    "namespaces",
    "csidrivers",
    "csistoragecapacities",
    "storageclasses",
    "leases",
    "events",
    "deviceclasses",
    "resourceclaims",
    "resourceslices",
    "devicetaintrules",
    "workloads",
];

fn collect(dir: &Path, out: &mut Vec<String>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(std::fs::read_to_string(&p).unwrap());
        }
    }
}

/// Resource segment of every `"/api/v1/..."` / `"/apis/<g>/<v>/..."` literal.
fn resources(src: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for lit in src.split('"') {
        let segs: Vec<&str> = lit.split('/').collect();
        let rest = match segs.as_slice() {
            ["", "api", "v1", rest @ ..] => rest,
            ["", "apis", _g, _v, rest @ ..] => rest,
            _ => continue,
        };
        let rest = match rest {
            ["namespaces", _ns, r, ..] => r,
            [r, ..] => r,
            [] => continue,
        };
        if rest.is_empty() || rest.contains('{') {
            continue;
        }
        found.insert(rest.to_string());
    }
    found
}

#[test]
fn scheduler_only_touches_resources_stock_rbac_permits() {
    let mut files = Vec::new();
    collect(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let mut all = BTreeSet::new();
    for f in &files {
        all.extend(resources(f));
    }
    assert!(all.contains("pods"), "scanner found nothing: {all:?}");
    let bad: Vec<_> = all
        .iter()
        .filter(|r| !ALLOWED.contains(&r.as_str()))
        .collect();
    assert!(
        bad.is_empty(),
        "scheduler references resources outside stock system:kube-scheduler RBAC: {bad:?}"
    );
    assert!(!all.contains("priorityclasses"));
}
