//! Guard: every PUT handler must reinstate server-owned metadata (#1793).
//!
//! Upstream applies this once, for every resource, in the generic registry
//! store: `staging/src/k8s.io/apiserver/pkg/registry/rest/update.go`,
//! `BeforeUpdate` (lines 131-146) fills the UID from the stored object when the
//! request omits it, and likewise preserves `creationTimestamp`, a pending
//! `deletionTimestamp` and `deletionGracePeriodSeconds`. No resource can opt
//! out, because no resource wires it up individually.
//!
//! Rusternetes has one hand-written handler per resource, so the same
//! behaviour has to be called from ~60 places — and the failure mode is
//! silent: a handler that simply never calls it stores the client's blanks. A
//! blanked `uid` orphans every child, because `ownerReferences[].uid` then
//! matches no live owner and the garbage collector deletes the children. That
//! is exactly how `[sig-apps] Deployment should run the lifecycle of a
//! Deployment` failed (#1605): the Deployment's ReplicaSets and their pods were
//! collected while its status still described them.
//!
//! #1788 fixed five handlers. This test exists so the remaining ones are
//! visible, and so a NEW handler cannot regress the invariant silently — the
//! thing a per-handler behavioural test cannot do, since it can only cover
//! handlers someone remembered to add.
//!
//! This is a source-level check rather than a behavioural one on purpose:
//! completeness is the property under test.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// The DRA resources are the only handlers still persisting a PUT without
/// reinstating the stored object's server-owned metadata.
///
/// They model metadata with their OWN `resources::dra::ObjectMeta`, wrapped in
/// an `Option`, rather than the shared `types::ObjectMeta` every other resource
/// uses, so neither `inherit_server_owned_metadata` nor the shared
/// `HasMetadata` bound typechecks against them. That divergence is the actual
/// bug to fix — a second ObjectMeta type means every generic metadata rule has
/// to be written twice — so these are held here rather than given a duplicate
/// helper, which would entrench it. Tracked in #1895.
///
/// Note this is FOUR handlers, not the two originally recorded here:
/// `deviceclass` and `resourceclaimtemplate` were listed as merely needing a
/// read, but they share the same metadata type and so share the same blocker.
/// Empty, like `PENDING_READ`. The DRA resources used to define their own
/// `ObjectMeta`, so neither the shared `inherit_server_owned_metadata` nor the
/// `HasMetadata` bound typechecked against them and the generic rule could not
/// reach four handlers. They now use `types::ObjectMeta` like every other
/// resource, which removes the exception rather than recording it.
const PENDING_DRA_METADATA_TYPE: &[(&str, &str)] = &[];

/// Every other handler now reinstates the metadata, via
/// `lifecycle::update_inheriting_server_owned_metadata` (typed) or
/// `inherit_server_owned_metadata_json` (untyped documents). Kept as an empty
/// list rather than deleted: a new handler that forgets belongs in a fix, not
/// in an allowlist, and an empty constant says so louder than a missing one.
const PENDING_READ: &[(&str, &str)] = &[];

/// Blank out char literals, string literals and comments, replacing each byte
/// with a space so byte offsets are preserved. Brace matching over the raw
/// source is wrong: `update_crd` contains the byte literal `b'{'`, which a
/// naive counter reads as an opening brace and so truncates the function body
/// early — reporting a handler as unwired when the call sits just past the
/// truncation point. That produced a false positive the first time this test
/// ran, and the same flaw could equally hide a real one.
fn blank_literals_and_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = b.to_vec();
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'/' if i + 1 < b.len() && b[i + 1] == b'/' => {
                while i < b.len() && b[i] != b'\n' {
                    out[i] = b' ';
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                let mut depth = 1usize;
                out[i] = b' ';
                out[i + 1] = b' ';
                i += 2;
                while i < b.len() && depth > 0 {
                    if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
                        depth += 1;
                        out[i] = b' ';
                        i += 1;
                    } else if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                        depth -= 1;
                        out[i] = b' ';
                        i += 1;
                    }
                    if b[i] != b'\n' {
                        out[i] = b' ';
                    }
                    i += 1;
                }
            }
            b'"' => {
                out[i] = b' ';
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        out[i] = b' ';
                        if i + 1 < b.len() && b[i + 1] != b'\n' {
                            out[i + 1] = b' ';
                        }
                        i += 2;
                        continue;
                    }
                    let done = b[i] == b'"';
                    if b[i] != b'\n' {
                        out[i] = b' ';
                    }
                    i += 1;
                    if done {
                        break;
                    }
                }
            }
            // A char literal: `'x'`, `b'x'`, `'\n'`. A lone `'` also starts a
            // lifetime (`'a`), which has no closing quote — detect the literal
            // shape explicitly and leave lifetimes alone.
            b'\'' => {
                let is_literal = (i + 2 < b.len() && b[i + 2] == b'\'' && b[i + 1] != b'\\')
                    || (i + 3 < b.len() && b[i + 1] == b'\\' && b[i + 3] == b'\'');
                if is_literal {
                    let end = if b[i + 1] == b'\\' { i + 3 } else { i + 2 };
                    for slot in out.iter_mut().take(end + 1).skip(i) {
                        *slot = b' ';
                    }
                    i = end + 1;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Return the name and body span of every `pub async fn update*/replace*/put*`
/// in `src`, by brace matching over a literal-stripped copy.
fn update_fn_bodies(src: &str) -> Vec<(String, String)> {
    let scan = blank_literals_and_comments(src);
    let mut out = Vec::new();
    let mut search = 0usize;
    while let Some(rel) = scan[search..].find("\npub async fn ") {
        let sig_start = search + rel + 1;
        let after = sig_start + "pub async fn ".len();
        let name: String = scan[after..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        search = after;
        if !(name.starts_with("update") || name.starts_with("replace") || name.starts_with("put")) {
            continue;
        }
        let Some(open_rel) = scan[after..].find('{') else {
            continue;
        };
        let open = after + open_rel;
        let mut depth = 0usize;
        let mut end = open;
        for (i, c) in scan[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        // Report the ORIGINAL text for the span, so the `contains` check sees
        // real code rather than the blanked copy.
        out.push((name, src[open..end].to_string()));
    }
    out
}

#[test]
fn every_put_handler_reinstates_server_owned_metadata() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/handlers");
    let pending: BTreeSet<(String, String)> = PENDING_READ
        .iter()
        .chain(PENDING_DRA_METADATA_TYPE.iter())
        .map(|(f, n)| (f.to_string(), n.to_string()))
        .collect();

    let mut missing: Vec<String> = Vec::new();
    let mut seen_pending: BTreeSet<(String, String)> = BTreeSet::new();
    let mut checked = 0usize;

    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("handlers dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.sort();

    for path in files {
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        // lifecycle.rs defines the helper; status.rs and scale.rs serve
        // subresources, whose bodies carry no client metadata to reinstate.
        if matches!(
            file_name.as_str(),
            "lifecycle.rs" | "status.rs" | "scale.rs"
        ) {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read handler");
        for (name, body) in update_fn_bodies(&src) {
            // A `/status` or `/scale` write replaces only that subresource;
            // upstream uses a separate strategy that never takes the client's
            // ObjectMeta, so there is nothing to reinstate.
            if name.contains("status") || name.contains("scale") {
                continue;
            }
            checked += 1;
            // Either the rule applied inline, or the read-modify-write helper
            // that applies it (`update_inheriting_server_owned_metadata`),
            // which is what a handler with no other need for the stored object
            // should call. Both spellings satisfy the invariant; what matters
            // is that the stored metadata reaches the object being written.
            if body.contains("inherit_server_owned_metadata")
                || body.contains("update_inheriting_server_owned_metadata")
            {
                continue;
            }
            let key = (file_name.clone(), name.clone());
            if pending.contains(&key) {
                seen_pending.insert(key);
                continue;
            }
            missing.push(format!("{file_name}::{name}"));
        }
    }

    assert!(
        checked > 50,
        "the scanner found only {checked} update handlers — it has probably stopped \
         matching (a rename or a formatting change), which would make this guard \
         silently vacuous"
    );

    assert!(
        missing.is_empty(),
        "these PUT handlers do not call \
         `lifecycle::inherit_server_owned_metadata`, so a client that omits \
         metadata.uid (what the dynamic client's Update() sends) has it stored \
         blank — which orphans every child and lets the GC delete them (#1605, \
         #1793). Upstream does this for all resources at once in \
         registry/rest/update.go::BeforeUpdate.\n  {}",
        missing.join("\n  ")
    );

    let stale: Vec<String> = pending
        .difference(&seen_pending)
        .map(|(f, n)| format!("{f}::{n}"))
        .collect();
    assert!(
        stale.is_empty(),
        "PENDING_READ names handlers that are now fixed or gone — drop them from \
         the list so it keeps meaning what it says:\n  {}",
        stale.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// Behavioural counterpart
// ---------------------------------------------------------------------------

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

/// The source-level guard above proves every handler *calls* the helper; this
/// proves the call does what it claims end-to-end, through the real router.
///
/// Table-driven on purpose: the failure mode is "a resource nobody thought
/// about", so the table is the unit of review, not the individual case.
#[tokio::test]
async fn put_omitting_uid_does_not_blank_it() {
    // (label, collection path, body) — the body is PUT back verbatim, which is
    // what a client that builds the object locally sends: no uid, no
    // creationTimestamp. Upstream's dynamic client Update() does exactly this.
    let cases: Vec<(&str, &str, serde_json::Value)> = vec![
        (
            "storageclasses",
            "/apis/storage.k8s.io/v1/storageclasses",
            json!({"apiVersion":"storage.k8s.io/v1","kind":"StorageClass",
                   "metadata":{"name":"t"},"provisioner":"example.com/p"}),
        ),
        (
            "runtimeclasses",
            "/apis/node.k8s.io/v1/runtimeclasses",
            json!({"apiVersion":"node.k8s.io/v1","kind":"RuntimeClass",
                   "metadata":{"name":"t"},"handler":"runc"}),
        ),
        (
            "configmaps",
            "/api/v1/namespaces/default/configmaps",
            json!({"apiVersion":"v1","kind":"ConfigMap",
                   "metadata":{"name":"t"},"data":{"k":"v"}}),
        ),
        (
            "secrets",
            "/api/v1/namespaces/default/secrets",
            json!({"apiVersion":"v1","kind":"Secret",
                   "metadata":{"name":"t"},"stringData":{"k":"v"}}),
        ),
        (
            "priorityclasses",
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            json!({"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass",
                   "metadata":{"name":"t"},"value":100}),
        ),
        (
            "resourcequotas",
            "/api/v1/namespaces/default/resourcequotas",
            json!({"apiVersion":"v1","kind":"ResourceQuota",
                   "metadata":{"name":"t"},"spec":{"hard":{"pods":"5"}}}),
        ),
        // Below: the handlers drained from PENDING_READ. They persisted the
        // client's body blind — no read of the stored object at all — so every
        // one of these cases failed before the fix. Chosen to span both persist
        // shapes (plain update, and update-then-create upsert) and the untyped
        // document path.
        (
            "endpoints",
            "/api/v1/namespaces/default/endpoints",
            json!({"apiVersion":"v1","kind":"Endpoints",
                   "metadata":{"name":"t"},"subsets":[]}),
        ),
        (
            "podtemplates",
            "/api/v1/namespaces/default/podtemplates",
            json!({"apiVersion":"v1","kind":"PodTemplate","metadata":{"name":"t"},
                   "template":{"metadata":{"labels":{"a":"b"}},
                     "spec":{"containers":[{"name":"c","image":"busybox"}]}}}),
        ),
        (
            "roles",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles",
            json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role",
                   "metadata":{"name":"t"},"rules":[]}),
        ),
        (
            "serviceaccounts",
            "/api/v1/namespaces/default/serviceaccounts",
            json!({"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"t"}}),
        ),
        (
            "leases",
            "/apis/coordination.k8s.io/v1/namespaces/default/leases",
            json!({"apiVersion":"coordination.k8s.io/v1","kind":"Lease",
                   "metadata":{"name":"t"},"spec":{"holderIdentity":"h"}}),
        ),
        (
            "networkpolicies",
            "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies",
            json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy",
                   "metadata":{"name":"t"},"spec":{"podSelector":{}}}),
        ),
        (
            "validatingwebhookconfigurations",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            json!({"apiVersion":"admissionregistration.k8s.io/v1",
                   "kind":"ValidatingWebhookConfiguration",
                   "metadata":{"name":"t"},"webhooks":[]}),
        ),
        // The DRA resources, drained from PENDING_DRA_METADATA_TYPE. They were
        // unreachable by the generic rule until they shared one ObjectMeta.
        (
            "deviceclasses",
            "/apis/resource.k8s.io/v1/deviceclasses",
            json!({"apiVersion":"resource.k8s.io/v1","kind":"DeviceClass",
                   "metadata":{"name":"t"},"spec":{}}),
        ),
        (
            "resourceclaims",
            "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims",
            json!({"apiVersion":"resource.k8s.io/v1","kind":"ResourceClaim",
                   "metadata":{"name":"t"},"spec":{}}),
        ),
        (
            "resourceclaimtemplates",
            "/apis/resource.k8s.io/v1/namespaces/default/resourceclaimtemplates",
            json!({"apiVersion":"resource.k8s.io/v1","kind":"ResourceClaimTemplate",
                   "metadata":{"name":"t"},"spec":{"spec":{}}}),
        ),
        (
            "resourceslices",
            "/apis/resource.k8s.io/v1/resourceslices",
            json!({"apiVersion":"resource.k8s.io/v1","kind":"ResourceSlice",
                   "metadata":{"name":"t"},
                   // driver must be an RFC-1123 subdomain, and exactly one of
                   // nodeName/nodeSelector/allNodes/perDeviceNodeSelection is
                   // required — both enforced by validate_resource_slice.
                   "spec":{"driver":"example.com","allNodes":true,
                           "pool":{"name":"p","resourceSliceCount":1,
                                   "generation":1}}}),
        ),
        // The untyped-document path: APIService is stored as a raw
        // `serde_json::Value`, so it goes through
        // `inherit_server_owned_metadata_json` rather than the typed helper.
        (
            "apiservices",
            "/apis/apiregistration.k8s.io/v1/apiservices",
            json!({"apiVersion":"apiregistration.k8s.io/v1","kind":"APIService",
                   "metadata":{"name":"v1.example.com"},
                   "spec":{"group":"example.com","version":"v1",
                           "groupPriorityMinimum":100,"versionPriority":100}}),
        ),
    ];

    for (label, collection, body) in cases {
        // The object's own name, not a fixed "t": APIService names are
        // structurally constrained (`version.group`) and would fail validation.
        let name = body["metadata"]["name"]
            .as_str()
            .expect("case body names the object")
            .to_string();
        let state = TestApiServer::new();
        let (status, _, created) = state
            .send_raw("POST", collection, Some("application/json"), Some(&body))
            .await;
        assert!(
            status.is_success(),
            "{label}: create failed {status}: {created}"
        );
        let uid_before = created["metadata"]["uid"]
            .as_str()
            .unwrap_or("")
            .to_string();
        assert!(!uid_before.is_empty(), "{label}: create assigned no uid");

        let (status, _, updated) = state
            .send_raw(
                "PUT",
                &format!("{collection}/{name}"),
                Some("application/json"),
                Some(&body),
            )
            .await;
        assert!(
            status.is_success(),
            "{label}: PUT failed {status}: {updated}"
        );

        // Read it back: what matters is what was STORED, not what the update
        // response happened to echo.
        let (_, stored) = {
            let (s, _, v) = state
                .send_raw("GET", &format!("{collection}/{name}"), None, None)
                .await;
            (s, v)
        };
        assert_eq!(
            stored["metadata"]["uid"].as_str(),
            Some(uid_before.as_str()),
            "{label}: a PUT omitting metadata.uid blanked or changed it — every \
             child's ownerReferences[].uid then matches no live owner and the \
             garbage collector deletes them (#1605, #1793). Stored: {stored}"
        );
        assert!(
            stored["metadata"]["creationTimestamp"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "{label}: a PUT omitting creationTimestamp blanked it: {stored}"
        );
    }
}
