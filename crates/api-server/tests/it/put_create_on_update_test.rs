//! A PUT to a name that does not exist is a 404, not a create.
//!
//! Upstream decides this per strategy, in exactly one place —
//! `Store.Update` (`registry/generic/registry/store.go:638`, `:646-650`):
//!
//! ```go
//! ignoreNotFound := e.UpdateStrategy.AllowCreateOnUpdate() || forceAllowCreate
//! ...
//! if existingResourceVersion == 0 {
//!     if !e.UpdateStrategy.AllowCreateOnUpdate() && !forceAllowCreate {
//!         return nil, nil, apierrors.NewNotFound(qualifiedResource, name)
//!     }
//! }
//! ```
//!
//! `AllowCreateOnUpdate()` returns `true` for nine resources in the whole
//! upstream tree. Rusternetes had it the other way round: thirty update
//! handlers carried an `Err(NotFound) => storage.create(...)` fallback, so any
//! PUT created the object with server-assigned metadata the client never asked
//! for (#1905).
//!
//! The sweep enumerates resources from the server's own discovery rather than a
//! fixed list, so a resource added later is covered without editing this file.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The nine upstream strategies whose `AllowCreateOnUpdate()` returns `true`,
/// as `(group, resource)`. Mirrors `handlers::lifecycle::allow_create_on_update`
/// — deliberately restated here rather than imported, so a change to the table
/// has to be made twice and cannot be waved through as "the test follows the
/// code".
const CREATE_ON_UPDATE: &[(&str, &str)] = &[
    ("coordination.k8s.io", "leases"),
    ("coordination.k8s.io", "leasecandidates"),
    ("rbac.authorization.k8s.io", "roles"),
    ("rbac.authorization.k8s.io", "rolebindings"),
    ("rbac.authorization.k8s.io", "clusterroles"),
    ("rbac.authorization.k8s.io", "clusterrolebindings"),
    ("", "limitranges"),
    ("", "events"),
    // The events.k8s.io endpoint reaches the same registry upstream, so it
    // inherits the core Event strategy's AllowCreateOnUpdate.
    ("events.k8s.io", "events"),
    ("", "endpoints"),
];

type Gvr = (String, String, String);

/// Every resource discovery reports an `update` verb for, with its scope and
/// kind.
async fn updatable_resources(s: &TestApiServer) -> BTreeMap<Gvr, (bool, String)> {
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
        assert!(st.is_success(), "GET {uri}: {st} {list}");
        for r in list["resources"].as_array().cloned().unwrap_or_default() {
            let name = r["name"].as_str().unwrap_or_default();
            if name.contains('/') {
                continue;
            }
            let verbs: BTreeSet<&str> = r["verbs"]
                .as_array()
                .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                .unwrap_or_default();
            if !verbs.contains("update") {
                continue;
            }
            out.insert(
                (group.clone(), version.clone(), name.to_string()),
                (
                    r["namespaced"].as_bool().unwrap_or(false),
                    r["kind"].as_str().unwrap_or_default().to_string(),
                ),
            );
        }
    }
    out
}

#[tokio::test]
async fn a_put_to_a_missing_object_is_not_a_create() {
    let api = TestApiServer::new();
    let resources = updatable_resources(&api).await;
    assert!(
        resources.len() > 50,
        "discovery returned only {} updatable resources -- the sweep would be \
         nearly vacuous",
        resources.len()
    );

    let mut created: Vec<String> = Vec::new();
    let mut missing_404 = 0usize;
    // A body that does not decode is answered before `Store.Update` is reached
    // (400/BadRequest, #1915), so those resources cannot be judged here. They
    // are counted, not skipped silently: if the decodable set shrinks, the
    // assertion below fails rather than the sweep quietly narrowing.
    let mut undecodable: Vec<String> = Vec::new();
    let mut other: Vec<String> = Vec::new();

    for ((group, version, resource), (namespaced, kind)) in &resources {
        let root = if group.is_empty() {
            format!("/api/{version}")
        } else {
            format!("/apis/{group}/{version}")
        };
        let base = if *namespaced {
            format!("{root}/namespaces/default/{resource}")
        } else {
            format!("{root}/{resource}")
        };
        // Per-group name: core `events` and `events.k8s.io` events share a
        // storage key, so a create-on-update resource would otherwise leave an
        // object behind for the next probe to find.
        let name = format!(
            "put-create-probe-{}",
            if group.is_empty() { "core" } else { group }
        );
        let name = name.as_str();
        let api_version = if group.is_empty() {
            version.clone()
        } else {
            format!("{group}/{version}")
        };
        let body = json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": { "name": name },
        });

        let (status, answer) = api
            .send(
                "PUT",
                &format!("{base}/{name}"),
                Some("application/json"),
                Some(&body),
            )
            .await;
        let code = status.as_u16();
        let allowed = CREATE_ON_UPDATE
            .iter()
            .any(|(g, r)| g == group && r == resource);
        let id = format!("{api_version} {resource}");

        if allowed {
            // A probe body the resource cannot decode never reaches the
            // existence check, in either direction.
            assert!(
                (200..300).contains(&code) || is_decode_failure(&answer),
                "{id} opts into create-on-update upstream but answered {code}: {answer}"
            );
            continue;
        }
        match code {
            404 => missing_404 += 1,
            200..=299 => created.push(format!("{id} -> {code}")),
            400 | 422 if is_decode_failure(&answer) => undecodable.push(id),
            _ => other.push(format!("{id} -> {code}: {}", answer["message"])),
        }
    }

    assert!(
        created.is_empty(),
        "{} resource(s) created an object on PUT to a name that does not \
         exist. Upstream answers NotFound unless the strategy's \
         AllowCreateOnUpdate() is true (store.go:646-650), which holds for \
         nine resources -- none of these.\n\nOffenders:\n  {}",
        created.len(),
        created.join("\n  ")
    );
    assert!(
        other.is_empty(),
        "{} resource(s) answered a PUT to a missing object with neither 404 \
         nor a decode failure. Upstream returns NotFound before any validator \
         runs, so a validation error here means the existence check is in the \
         wrong place.\n\n  {}",
        other.len(),
        other.join("\n  ")
    );
    assert!(
        missing_404 >= 20,
        "only {missing_404} resources reached the existence check ({} could not \
         decode the probe body) -- too few for this sweep to mean anything",
        undecodable.len()
    );
}

/// Distinguish "the body never decoded" from "the object was validated".
/// Decode failures are `transformDecodeError`'s wording (#1915).
fn is_decode_failure(answer: &Value) -> bool {
    let msg = answer["message"].as_str().unwrap_or_default();
    msg.contains("cannot be handled as a")
        || msg.contains("the object provided is unrecognized")
        || msg.contains("failed to decode")
}

/// The single-resource form of the sweep, spelled out so a regression names the
/// repro from #1905 directly.
#[tokio::test]
async fn put_to_a_missing_configmap_is_a_notfound_status() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "PUT",
            "/api/v1/namespaces/default/configmaps/nope",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": "nope" },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 404, "{answer}");
    assert_eq!(answer["reason"], json!("NotFound"));
    // `NewNotFound(qualifiedResource, name)` wording and details.
    assert_eq!(answer["message"], json!("configmaps \"nope\" not found"));
    assert_eq!(answer["details"]["kind"], json!("configmaps"));
    assert_eq!(answer["details"]["name"], json!("nope"));

    // And nothing was persisted on the way out.
    let (status, _) = api.get("/api/v1/namespaces/default/configmaps/nope").await;
    assert_eq!(status.as_u16(), 404, "the rejected PUT still created it");
}

/// Lease is one of the nine: `pkg/registry/coordination/lease/strategy.go:84`.
#[tokio::test]
async fn a_lease_is_still_created_on_update() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "PUT",
            "/apis/coordination.k8s.io/v1/namespaces/default/leases/holder",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": { "name": "holder" },
                "spec": { "holderIdentity": "node-1" },
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "Lease opts into create-on-update upstream: {status} {answer}"
    );
}

// ---------------------------------------------------------------------------
// Structural guard
// ---------------------------------------------------------------------------

/// The sweep above can only judge a resource whose probe body decodes, and
/// thirty-seven currently cannot (#1931). This guard covers the rest: every
/// update handler must consult the create-on-update table, either by calling
/// `reject_create_on_update` or -- for one of the nine that opt in -- by
/// keeping an explicit create fallback that names the rule.
///
/// Keyed on mechanism, not on a list of handler names: a name list is the
/// allowlist this guard exists to avoid.
#[tokio::test]
async fn every_update_handler_consults_the_create_on_update_table() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/handlers");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("read handlers dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();

    let mut offenders = Vec::new();
    let mut checked = 0usize;

    for path in files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        // The helper's own module.
        if name == "lifecycle.rs" {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read handler");
        let src = src.split("\n#[cfg(test)]").next().unwrap_or("").to_string();

        for (fname, body) in update_fn_bodies(&src) {
            // In scope: every update handler that writes. The bug is a write
            // path that reaches `create` for a name that does not exist, and
            // only a handler that writes can have one.
            let writes = body.contains("storage.update(")
                || body.contains("update_inheriting_server_owned_metadata");
            if !writes {
                continue;
            }
            // Subresource writes (`status`, `scale`, `approval`, …) are in
            // scope too: upstream reaches them through the same `Store.Update`,
            // so the gate applies to a status write exactly as to a spec write
            // (#1932).
            checked += 1;

            let consults_table = body.contains("reject_create_on_update")
                // The opt-in side of the same table, spelled as upstream's
                // method name in the comment that justifies the fallback.
                || body.contains("AllowCreateOnUpdate");
            if !consults_table {
                offenders.push(format!(
                    "{name}::{fname} (creates from an update path without \
                     consulting AllowCreateOnUpdate)"
                ));
            }
        }
    }

    assert!(
        checked >= 64,
        "guard scanned only {checked} update handlers that can create -- the \
         parser stopped matching, which would make this test vacuously green"
    );
    assert!(
        offenders.is_empty(),
        "{} update handler(s) can create an object on PUT without consulting \
         the create-on-update table. Upstream answers NotFound unless the \
         strategy's AllowCreateOnUpdate() is true \
         (registry/generic/registry/store.go:646-650). Call \
         `lifecycle::reject_create_on_update(...)` after authorization, or -- \
         for one of the nine that opt in -- keep the create fallback and say \
         so.\n\nOffenders:\n  {}",
        offenders.len(),
        offenders.join("\n  ")
    );
}

/// `(name, body)` for every `pub async fn *update*` in a handler file.
fn update_fn_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = src;
    while let Some(at) = rest.find("\npub async fn ") {
        let after = &rest[at + "\npub async fn ".len()..];
        let Some(paren) = after.find('(') else { break };
        let fname = after[..paren].trim().to_string();
        let Some(open) = after.find('{') else { break };
        let bytes = after.as_bytes();
        let mut depth = 0usize;
        let mut end = open;
        for (i, b) in bytes.iter().enumerate().skip(open) {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if fname.contains("update") {
            out.push((fname, after[open..=end].to_string()));
        }
        rest = &after[end..];
    }
    out
}

// ---------------------------------------------------------------------------
// Subresources (#1932)
// ---------------------------------------------------------------------------

/// Every `<resource>/<subresource>` discovery reports an `update` verb for,
/// with the parent's scope and kind.
///
/// Upstream serves `/status`, `/scale`, `/approval` and `/finalize` from a
/// second `*REST` built on the same `genericregistry.Store` as the parent, with
/// only the strategy swapped (`registry/apps/deployment/storage/storage.go`
/// wires `statusStore.UpdateStrategy = deployment.StatusStrategy`, and
/// `StatusStrategy` embeds `deploymentStrategy`). So `AllowCreateOnUpdate()`
/// answers the same for the subresource as for the parent, and the
/// `store.go:646-650` NotFound applies unchanged.
async fn updatable_subresources(s: &TestApiServer) -> BTreeMap<Gvr, (bool, String)> {
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
        assert!(st.is_success(), "GET {uri}: {st} {list}");
        for r in list["resources"].as_array().cloned().unwrap_or_default() {
            let name = r["name"].as_str().unwrap_or_default();
            if !name.contains('/') {
                continue;
            }
            let verbs: BTreeSet<&str> = r["verbs"]
                .as_array()
                .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                .unwrap_or_default();
            if !verbs.contains("update") {
                continue;
            }
            out.insert(
                (group.clone(), version.clone(), name.to_string()),
                (
                    r["namespaced"].as_bool().unwrap_or(false),
                    r["kind"].as_str().unwrap_or_default().to_string(),
                ),
            );
        }
    }
    out
}

/// A PUT to `<resource>/<subresource>` of a name that does not exist is a 404.
///
/// None of the nine `AllowCreateOnUpdate()` strategies has a writable
/// subresource, so this direction has no opt-in half: every entry must answer
/// NotFound.
#[tokio::test]
async fn a_put_to_a_missing_objects_subresource_is_not_a_create() {
    let api = TestApiServer::new();
    let subresources = updatable_subresources(&api).await;
    assert!(
        subresources.len() > 15,
        "discovery returned only {} updatable subresources -- the sweep would \
         be nearly vacuous",
        subresources.len()
    );

    let mut created: Vec<String> = Vec::new();
    let mut missing_404 = 0usize;
    let mut undecodable: Vec<String> = Vec::new();
    let mut other: Vec<String> = Vec::new();

    for ((group, version, path), (namespaced, kind)) in &subresources {
        let (resource, subresource) = path.split_once('/').expect("name contains '/'");
        let root = if group.is_empty() {
            format!("/api/{version}")
        } else {
            format!("/apis/{group}/{version}")
        };
        let base = if *namespaced {
            format!("{root}/namespaces/default/{resource}")
        } else {
            format!("{root}/{resource}")
        };
        let name = format!(
            "put-sub-probe-{}",
            if group.is_empty() { "core" } else { group }
        );
        let api_version = if group.is_empty() {
            version.clone()
        } else {
            format!("{group}/{version}")
        };
        // `scale` is its own kind on the wire; everything else takes the
        // parent's kind with only `.status` read off it.
        // `Scale` still decodes through a bespoke `ScaleMetadata` whose
        // `namespace`, and whose `status`, are required fields (#1931), so the
        // probe spells them out to reach the gate at all.
        let body = if subresource == "scale" {
            json!({
                "apiVersion": "autoscaling/v1",
                "kind": "Scale",
                "metadata": { "name": name, "namespace": "default" },
                "spec": { "replicas": 1 },
                "status": { "replicas": 0 },
            })
        } else {
            json!({
                "apiVersion": api_version,
                "kind": kind,
                "metadata": { "name": name },
                "status": {},
            })
        };

        let (status, answer) = api
            .send(
                "PUT",
                &format!("{base}/{name}/{subresource}"),
                Some("application/json"),
                Some(&body),
            )
            .await;
        let code = status.as_u16();
        let id = format!("{api_version} {path}");
        match code {
            404 => missing_404 += 1,
            200..=299 => created.push(format!("{id} -> {code}")),
            400 | 422 if is_decode_failure(&answer) => undecodable.push(id),
            _ => other.push(format!("{id} -> {code}: {}", answer["message"])),
        }
    }

    assert!(
        created.is_empty(),
        "{} subresource(s) accepted a PUT to a name that does not exist. \
         Upstream serves the subresource from the same Store, so the \
         AllowCreateOnUpdate() gate (store.go:646-650) answers NotFound \
         first.\n\nOffenders:\n  {}",
        created.len(),
        created.join("\n  ")
    );
    assert!(
        other.is_empty(),
        "{} subresource(s) answered a PUT to a missing object with neither 404 \
         nor a decode failure. The existence check runs before any validator \
         upstream, so a validation error here means the gate is in the wrong \
         place.\n\n  {}",
        other.len(),
        other.join("\n  ")
    );
    assert!(
        missing_404 >= 10,
        "only {missing_404} subresources reached the existence check ({} could \
         not decode the probe body) -- too few for this sweep to mean anything",
        undecodable.len()
    );
}

/// The gate runs before validation, so an invalid body against a missing object
/// is still a 404 -- the ordering `Store.Update` fixes by checking existence
/// before `BeforeUpdate` and the strategy's `ValidateUpdate`.
///
/// `update_scale` validated `spec.replicas >= 0` before reading the object, so
/// this exact request used to answer 422 Invalid for an object that was never
/// there.
#[tokio::test]
async fn an_invalid_scale_on_a_missing_deployment_is_a_notfound() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "PUT",
            "/apis/apps/v1/namespaces/default/deployments/nope/scale",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "autoscaling/v1",
                "kind": "Scale",
                "metadata": { "name": "nope", "namespace": "default" },
                "spec": { "replicas": -3 },
                "status": { "replicas": 0 },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 404, "{answer}");
    assert_eq!(answer["reason"], json!("NotFound"));
    assert_eq!(answer["message"], json!("deployments \"nope\" not found"));
}

/// The status subresource of a resource that does exist still writes, so the
/// gate above cannot be satisfied by rejecting everything.
#[tokio::test]
async fn a_status_put_on_an_existing_object_still_writes() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/pods",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "status-writes" },
                "spec": { "containers": [ { "name": "c", "image": "busybox" } ] },
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {answer}");

    let (status, answer) = api
        .send(
            "PUT",
            "/api/v1/namespaces/default/pods/status-writes/status",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "status-writes" },
                "status": { "phase": "Running" },
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {answer}");
    assert_eq!(answer["status"]["phase"], json!("Running"));
}
