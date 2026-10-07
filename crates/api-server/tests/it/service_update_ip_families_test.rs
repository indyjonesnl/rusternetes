//! `TestUpdateIPsFromSingleStack` (pkg/registry/core/service/storage/
//! storage_test.go:7182-8893) and `TestUpdateIPsFromDualStack` (:8894-10489):
//! 182 create-then-update cases pinning which IP-family / ClusterIP
//! transitions an update may make (`update`, alloc.go `initIPFamilyFields` /
//! `updateIPFamilyFields` and `validateClusterIPFlags`), driven by
//! `helpTestCreateUpdateDeleteWithFamilies` (:789-863).
//!
//! The table is extracted mechanically from the checkout, not retyped:
//! `fixtures_update_ip_families.json` has one section per upstream
//! `t.Run`, each case being the create / update `svctest.MakeService`
//! tweaks plus the `svcTestCase` expectations (`expectError`,
//! `expectClusterIPs`, `expectHeadless`, `expectStackDowngrade`,
//! `proveNumFamilies`) and the optional `beforeUpdate` pre-allocation.
//!
//! Verification ports `verifyEquiv` (:893-945) and `proveClusterIPsAllocated`
//! / `proveHeadless` (:1148-1255). Allocation is probed through the API (a
//! second Service asking for the same IP) since the allocator is not
//! reachable from here.
//!
//! One deliberate deviation: upstream's allocator ranges are `10.0.0.0/16`
//! and `2000::/108` (storage_test.go:70-81); the table's `10.0.0.1` /
//! `2000::1` are remapped into `10.96.0.0/12` / `2001:db8:1::/112`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";

fn remap(ip: &str) -> String {
    match ip {
        "10.0.0.1" => "10.96.0.10".into(),
        "2000::1" => "2001:db8:1::10".into(),
        other => other.into(),
    }
}

fn family_of(ip: &str) -> &'static str {
    if ip.contains(':') {
        "IPv6"
    } else {
        "IPv4"
    }
}

fn range_for(families: &[Value]) -> String {
    families
        .iter()
        .map(|f| match f.as_str().unwrap() {
            "IPv4" => "10.96.0.0/12",
            _ => "2001:db8:1::/112",
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// `svctest.MakeService(name, SetTypeClusterIP, ...)` (make.go:34).
fn make_service(name: &str, phase: &Value) -> Value {
    let svc = &phase["svc"];
    let mut spec = json!({
        "type": "ClusterIP",
        "sessionAffinity": "None",
        "selector": {svc["selectorKey"].as_str().unwrap(): if svc["selectorKey"] == "k" {"v"} else {"v2"}},
        "ports": [{"port": 93, "targetPort": 76, "protocol": "TCP"}],
    });
    if let Some(ips) = svc.get("clusterIPs") {
        let ips: Vec<Value> = ips
            .as_array()
            .unwrap()
            .iter()
            .map(|ip| json!(remap(ip.as_str().unwrap())))
            .collect();
        spec["clusterIP"] = ips[0].clone();
        spec["clusterIPs"] = json!(ips);
    }
    if let Some(p) = svc.get("policy") {
        spec["ipFamilyPolicy"] = p.clone();
    }
    if let Some(f) = svc.get("families") {
        spec["ipFamilies"] = f.clone();
    }
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
    })
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| a.iter().map(|x| x.as_str().unwrap().to_string()).collect())
        .unwrap_or_default()
}

/// `verifyEquiv` + `proveClusterIPsAllocated` / `proveHeadless` + the
/// `proveNumFamilies` proofs. `before` is what upstream passes as `before`
/// to `verifyExpectations` (the input on create, the created object on
/// update). Returns the problems found.
fn verify(
    phase: &Value,
    input: &Value,
    before: &Value,
    got: &Value,
    cluster_families: usize,
) -> Vec<String> {
    let mut errs = Vec::new();
    let (inp, got) = (&input["spec"], &got["spec"]);
    let downgrade = phase["expectStackDowngrade"].as_bool().unwrap();
    let alloc = phase["expectClusterIPs"].as_bool().unwrap();
    let headless = phase["expectHeadless"].as_bool().unwrap();

    // verifyEquiv (:906-924).
    let got_ips = strs(&got["clusterIPs"]);
    let got_fams = strs(&got["ipFamilies"]);
    let mut want_ips = strs(&inp["clusterIPs"]);
    let mut want_fams = strs(&inp["ipFamilies"]);
    let mut want_cip = inp["clusterIP"].as_str().unwrap_or("").to_string();
    if alloc || headless {
        if want_cip.is_empty() {
            want_cip = got["clusterIP"].as_str().unwrap_or("").to_string();
        }
        if inp["ipFamilyPolicy"].is_null() {
            // want takes got's policy.
        } else if inp["ipFamilyPolicy"] != got["ipFamilyPolicy"] {
            errs.push(format!(
                "policy want {} got {}",
                inp["ipFamilyPolicy"], got["ipFamilyPolicy"]
            ));
        }
        if downgrade && want_ips.len() > got_ips.len() {
            want_ips.truncate(1);
        } else if got_ips.len() > want_ips.len() {
            want_ips.extend_from_slice(&got_ips[want_ips.len()..]);
        }
        if downgrade && want_fams.len() > got_ips.len() {
            want_fams.truncate(1);
        } else if got_fams.len() > want_fams.len() {
            want_fams.extend_from_slice(&got_fams[want_fams.len()..]);
        }
    } else if inp["ipFamilyPolicy"] != got["ipFamilyPolicy"] {
        errs.push(format!(
            "policy want {} got {}",
            inp["ipFamilyPolicy"], got["ipFamilyPolicy"]
        ));
    }
    if want_cip != got["clusterIP"].as_str().unwrap_or("") {
        errs.push(format!(
            "clusterIP want {want_cip:?} got {}",
            got["clusterIP"]
        ));
    }
    if want_ips != got_ips {
        errs.push(format!("clusterIPs want {want_ips:?} got {got_ips:?}"));
    }
    if want_fams != got_fams {
        errs.push(format!("ipFamilies want {want_fams:?} got {got_fams:?}"));
    }
    if inp["selector"] != got["selector"] {
        errs.push(format!(
            "selector want {} got {}",
            inp["selector"], got["selector"]
        ));
    }

    if headless {
        // proveHeadless (:1241).
        if got["clusterIP"] != "None" || got_ips != ["None"] {
            errs.push(format!("not headless: {} {got_ips:?}", got["clusterIP"]));
        }
    } else if alloc {
        // proveClusterIPsAllocated (:1148-1210).
        if got_ips.first().map(String::as_str) != got["clusterIP"].as_str() {
            errs.push(format!("clusterIP != clusterIPs[0]: {got_ips:?}"));
        }
        if got_ips.len() != got_fams.len() {
            errs.push(format!(
                "{} clusterIPs vs {} ipFamilies",
                got_ips.len(),
                got_fams.len()
            ));
        }
        for (ip, fam) in got_ips.iter().zip(&got_fams) {
            if family_of(ip) != fam {
                errs.push(format!("clusterIP {ip} is not {fam}"));
            }
        }
        let n = got_fams.len();
        match got["ipFamilyPolicy"].as_str() {
            Some("SingleStack") if n != 1 => errs.push(format!("SingleStack with {n} families")),
            Some("RequireDualStack") if n != 2 => {
                errs.push(format!("RequireDualStack with {n} families"))
            }
            Some("PreferDualStack") if n != cluster_families => {
                errs.push(format!("PreferDualStack: want {cluster_families} got {n}"))
            }
            None => errs.push("ipFamilyPolicy unset".into()),
            _ => {}
        }
        let bspec = &before["spec"];
        if let Some(b) = bspec["clusterIP"].as_str().filter(|b| !b.is_empty()) {
            if got["clusterIP"] != b {
                errs.push(format!("clusterIP changed {b} -> {}", got["clusterIP"]));
            }
        }
        for (i, (b, a)) in strs(&bspec["clusterIPs"]).iter().zip(&got_ips).enumerate() {
            if b != a {
                errs.push(format!("clusterIPs[{i}] changed {b} -> {a}"));
            }
        }
        for (i, (b, a)) in strs(&bspec["ipFamilies"]).iter().zip(&got_fams).enumerate() {
            if b != a {
                errs.push(format!("ipFamilies[{i}] changed {b} -> {a}"));
            }
        }
    }
    for n in phase["numFamilies"].as_array().unwrap() {
        if got_fams.len() as u64 != n.as_u64().unwrap() {
            errs.push(format!("want {n} ipFamilies, got {got_fams:?}"));
        }
    }
    errs
}

/// A second Service asking for `ip` explicitly: it can only be created if
/// the IP is free (`ipIsAllocated`, storage_test.go:1129).
async fn ip_is_allocated(api: &TestApiServer, ip: &str) -> bool {
    let probe = json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "probe", "namespace": "default"},
        "spec": {"type": "ClusterIP", "clusterIP": ip, "clusterIPs": [ip],
                 "ipFamilyPolicy": "SingleStack", "selector": {"p": "p"},
                 "ports": [{"port": 93, "protocol": "TCP"}]},
    });
    let (status, _) = api.post(SVCS, &probe).await;
    if status.is_success() {
        let _ = api.delete(&format!("{SVCS}/probe")).await;
        false
    } else {
        true
    }
}

#[tokio::test]
async fn update_ip_families_matches_upstream_tables() {
    let sections: Vec<Value> =
        serde_json::from_str(include_str!("fixtures_update_ip_families.json")).unwrap();
    let mut failures = Vec::new();
    let mut ran = 0;
    for sec in &sections {
        let fams = sec["clusterFamilies"].as_array().unwrap();
        // The harness builder polls with `now_or_never`; start from a fresh
        // tokio coop budget instead of the one the previous section spent.
        tokio::task::yield_now().await;
        let api = TestApiServer::builder()
            .service_cluster_ip_range(&range_for(fams))
            .build();
        for case in sec["cases"].as_array().unwrap() {
            ran += 1;
            let label = format!(
                "{}/{}/{}",
                sec["test"].as_str().unwrap(),
                sec["name"].as_str().unwrap(),
                case["name"].as_str().unwrap()
            );
            run_case(&api, &label, case, fams.len(), &mut failures).await;
            let _ = api.delete(&format!("{SVCS}/foo")).await;
            let _ = api.delete(&format!("{SVCS}/other")).await;
        }
    }
    assert_eq!(ran, 182);
    assert!(
        failures.is_empty(),
        "{} of {ran} cases diverge from upstream:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

async fn run_case(
    api: &TestApiServer,
    label: &str,
    case: &Value,
    cluster_families: usize,
    failures: &mut Vec<String>,
) {
    let create = &case["create"];
    let input = make_service("foo", create);
    let (status, created) = api.post(SVCS, &input).await;
    if create["expectError"].as_bool().unwrap() {
        if status.is_success() {
            failures.push(format!("{label}: create unexpectedly succeeded: {created}"));
        }
        return;
    }
    if status != StatusCode::CREATED {
        failures.push(format!("{label}: create failed {status}: {created}"));
        return;
    }
    let errs = verify(create, &input, &input, &created, cluster_families);
    failures.extend(errs.iter().map(|e| format!("{label}: create: {e}")));
    let mut last = created.clone();
    let mut last_ok = errs.is_empty();

    if let Some(pre) = case.get("preallocate") {
        // beforeUpdate: take the IP out of the family's allocator.
        let ip = remap(pre["ip"].as_str().unwrap());
        let other = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "other", "namespace": "default"},
            "spec": {"type": "ClusterIP", "clusterIP": ip, "clusterIPs": [ip],
                     "ipFamilyPolicy": "SingleStack", "selector": {"o": "o"},
                     "ports": [{"port": 93, "protocol": "TCP"}]},
        });
        let (s, b) = api.post(SVCS, &other).await;
        assert!(s.is_success(), "{label}: cannot preallocate {ip}: {b}");
    }

    if let Some(update) = case.get("update").filter(|u| !u.is_null()) {
        let uinput = make_service("foo", update);
        let (status, updated) = api.put(&format!("{SVCS}/foo"), &uinput).await;
        if update["expectError"].as_bool().unwrap() {
            if status.is_success() {
                failures.push(format!("{label}: update unexpectedly succeeded: {updated}"));
            }
            return;
        }
        if !status.is_success() {
            failures.push(format!("{label}: update failed {status}: {updated}"));
            return;
        }
        let errs = verify(update, &uinput, &created, &updated, cluster_families);
        failures.extend(errs.iter().map(|e| format!("{label}: update: {e}")));
        last_ok = errs.is_empty();
        last = updated;
        if update["expectClusterIPs"].as_bool().unwrap() {
            for ip in strs(&last["spec"]["clusterIPs"]) {
                if !ip_is_allocated(api, &ip).await {
                    failures.push(format!("{label}: update: {ip} not allocated"));
                }
            }
        }
    } else if create["expectClusterIPs"].as_bool().unwrap() {
        for ip in strs(&last["spec"]["clusterIPs"]) {
            if !ip_is_allocated(api, &ip).await {
                failures.push(format!("{label}: create: {ip} not allocated"));
            }
        }
    }
    let _ = last_ok;

    let (s, b) = api.delete(&format!("{SVCS}/foo")).await;
    if s != StatusCode::OK {
        failures.push(format!("{label}: delete failed {s}: {b}"));
        return;
    }
    // verifyExpectations(all false, lastSvc, nil): proveClusterIPsDeallocated.
    for ip in strs(&last["spec"]["clusterIPs"]) {
        if ip != "None" && ip_is_allocated(api, &ip).await {
            failures.push(format!("{label}: delete: {ip} still allocated"));
        }
    }
}
