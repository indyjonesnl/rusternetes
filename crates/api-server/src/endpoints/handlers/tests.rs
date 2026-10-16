use std::collections::HashMap;

use rusternetes_common::resources::ConfigMap;
use rusternetes_common::types::{DeletionPropagation, ObjectMeta};
use rusternetes_common::Error;

use super::delete::decode_delete_options;
use super::rest::check_name;

fn cm(name: &str, namespace: Option<&str>) -> ConfigMap {
    ConfigMap {
        type_meta: Default::default(),
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: namespace.map(str::to_string),
            ..Default::default()
        },
        data: None,
        binary_data: None,
        immutable: None,
    }
}

fn bad_request(r: rusternetes_common::Result<()>) -> String {
    match r {
        Err(Error::BadRequest(msg)) => msg,
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

/// The three `checkName` outcomes (rest.go:272-290, namer.go:74-85).
#[test]
fn check_name_matches_upstream() {
    assert!(check_name(&cm("a", Some("ns")), "a", Some("ns")).is_ok());
    // An object without a namespace is fine: the handler fills it.
    assert!(check_name(&cm("a", None), "a", Some("ns")).is_ok());

    assert_eq!(
        bad_request(check_name(&cm("", None), "a", Some("ns"))),
        "the name of the object (a based on URL) was undeterminable: name must be provided"
    );
    assert_eq!(
        bad_request(check_name(&cm("b", None), "a", Some("ns"))),
        "the name of the object (b) does not match the name on the URL (a)"
    );
    assert_eq!(
        bad_request(check_name(&cm("a", Some("other")), "a", Some("ns"))),
        "the namespace of the object (other) does not match the namespace on the request (ns)"
    );
}

/// get.go:90-110: `export` is refused unless it converts to false
/// (runtime.Convert_Slice_string_To_bool, conversion.go:79-95); the
/// `resourceVersion` reaches the Store.
#[test]
fn decode_get_options_matches_get_resource() {
    use super::get::decode_get_options;

    assert_eq!(decode_get_options(&q(&[])).unwrap().resource_version, "");
    assert_eq!(
        decode_get_options(&q(&[("resourceVersion", "12")]))
            .unwrap()
            .resource_version,
        "12"
    );
    for ok in ["0", "false", "FALSE"] {
        assert!(decode_get_options(&q(&[("export", ok)])).is_ok(), "{ok}");
    }
    for refused in ["true", "1", "", "yes"] {
        assert_eq!(
            bad_request(decode_get_options(&q(&[("export", refused)])).map(|_| ())),
            "the export parameter, deprecated since v1.14, is no longer supported",
            "{refused:?}"
        );
    }
}

fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// delete.go:95-126: a body wins outright; the query applies only without one.
#[test]
fn delete_options_come_from_the_body_or_else_the_query() {
    let from_query = decode_delete_options(
        &q(&[
            ("propagationPolicy", "Foreground"),
            ("gracePeriodSeconds", "5"),
            ("dryRun", "All"),
        ]),
        b"",
    )
    .unwrap();
    assert_eq!(
        from_query.propagation_policy,
        Some(DeletionPropagation::Foreground)
    );
    assert_eq!(from_query.grace_period_seconds, Some(5));
    assert_eq!(from_query.dry_run, Some(vec!["All".to_string()]));

    let from_body = decode_delete_options(
        &q(&[("propagationPolicy", "Foreground")]),
        br#"{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"u"}}"#,
    )
    .unwrap();
    assert_eq!(from_body.propagation_policy, None);
    assert_eq!(
        from_body.preconditions.and_then(|p| p.uid),
        Some("u".to_string())
    );
}

/// Undecodable options are a BadRequest; decodable-but-invalid ones fail
/// `ValidateDeleteOptions` (delete.go:103-107, 130-134).
#[test]
fn delete_options_errors() {
    assert!(matches!(
        decode_delete_options(&q(&[]), b"not json"),
        Err(Error::BadRequest(_))
    ));
    assert!(matches!(
        decode_delete_options(&q(&[("gracePeriodSeconds", "soon")]), b""),
        Err(Error::BadRequest(_))
    ));
    assert!(matches!(
        decode_delete_options(&q(&[("dryRun", "Maybe")]), b""),
        Err(Error::Status(_))
    ));
}

fn list_option_errors(pairs: &[(&str, &str)]) -> Vec<String> {
    crate::handlers::list_options::validate_list_options(&q(pairs))
        .iter()
        .map(|e| e.to_string())
        .collect()
}

/// `TestValidateListOptions` (internalversion/validation/validation_test.go),
/// with `isWatchListFeatureEnabled` true (WatchList is on by default in 1.35,
/// kube_features.go:503-509).
#[test]
fn validate_list_options_matches_upstream() {
    assert!(list_option_errors(&[]).is_empty());
    assert!(
        list_option_errors(&[("resourceVersion", "1"), ("resourceVersionMatch", "Exact")])
            .is_empty()
    );
    assert!(list_option_errors(&[
        ("resourceVersion", "0"),
        ("resourceVersionMatch", "NotOlderThan")
    ])
    .is_empty());
    assert_eq!(
        list_option_errors(&[("resourceVersion", "0"), ("resourceVersionMatch", "Exact")]),
        vec![
            r#"resourceVersionMatch: Forbidden: resourceVersionMatch "exact" is forbidden for resourceVersion "0""#
        ]
    );
    assert_eq!(
        list_option_errors(&[("resourceVersion", "0"), ("resourceVersionMatch", "foo")]),
        vec![
            r#"resourceVersionMatch: Unsupported value: "foo": supported values: "Exact", "NotOlderThan", """#
        ]
    );
    assert_eq!(
        list_option_errors(&[("resourceVersionMatch", "Exact"), ("continue", "x")]),
        vec![
            "resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden unless resourceVersion is provided",
            "resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden when continue is provided",
        ]
    );
    assert_eq!(
        list_option_errors(&[("sendInitialEvents", "true")]),
        vec!["sendInitialEvents: Forbidden: sendInitialEvents is forbidden for list"]
    );
}

#[test]
fn validate_list_options_watch() {
    assert!(list_option_errors(&[("watch", "true")]).is_empty());
    assert!(list_option_errors(&[
        ("watch", "true"),
        ("sendInitialEvents", "true"),
        ("resourceVersionMatch", "NotOlderThan"),
    ])
    .is_empty());
    assert_eq!(
        list_option_errors(&[("watch", "true"), ("resourceVersionMatch", "NotOlderThan")]),
        vec!["resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided"]
    );
    assert_eq!(
        list_option_errors(&[("watch", "true"), ("sendInitialEvents", "true")]),
        vec!["resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan"]
    );
    assert_eq!(
        list_option_errors(&[
            ("watch", "true"),
            ("sendInitialEvents", "true"),
            ("resourceVersionMatch", "Exact"),
        ]),
        vec![
            "resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan",
            r#"resourceVersionMatch: Unsupported value: "Exact": supported values: "NotOlderThan""#,
        ]
    );
    assert_eq!(
        list_option_errors(&[
            ("watch", "true"),
            ("sendInitialEvents", "true"),
            ("resourceVersionMatch", "NotOlderThan"),
            ("continue", "123"),
        ]),
        vec!["resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden when continue is provided"]
    );
}

fn owner(uid: &str, controller: Option<bool>) -> rusternetes_common::types::OwnerReference {
    rusternetes_common::types::OwnerReference {
        api_version: "v1".into(),
        kind: "Kind".into(),
        name: "name".into(),
        uid: uid.into(),
        block_owner_deletion: controller,
        controller,
    }
}

/// rest_test.go `TestDedupOwnerReferences`: an entry is dropped only when it
/// is wholly equal to an earlier one; same UID with other fields differing
/// is kept.
#[test]
fn dedup_owner_references_matches_upstream() {
    use super::rest::dedup_owner_references;

    let refs = vec![owner("1", None), owner("2", None), owner("1", None)];
    let (deduped, dups) = dedup_owner_references(&refs);
    assert_eq!(deduped, vec![owner("1", None), owner("2", None)]);
    assert_eq!(dups, vec!["1".to_string()]);

    let refs = vec![owner("1", Some(false)), owner("1", None)];
    let (deduped, dups) = dedup_owner_references(&refs);
    assert_eq!(deduped, refs, "semantic-different entries are kept");
    assert!(dups.is_empty());
}

/// rest.go:332-353: duplicates are removed from the object and a warning is
/// recorded; the text differs after mutating admission.
#[test]
fn dedup_owner_references_and_add_warning_matches_upstream() {
    use super::rest::dedup_owner_references_and_add_warning;
    use crate::registry::rest::RequestContext;

    for (after, needle) in [
        (
            false,
            ".metadata.ownerReferences contains duplicate entries; API server dedups",
        ),
        (
            true,
            ".metadata.ownerReferences contains duplicate entries after mutating admission happens; API server dedups",
        ),
    ] {
        let mut c = cm("a", Some("ns"));
        c.metadata.owner_references = Some(vec![owner("u1", None), owner("u1", None)]);
        let ctx = RequestContext::new(Some("ns"));
        dedup_owner_references_and_add_warning(&mut c, &ctx, after);
        assert_eq!(c.metadata.owner_references, Some(vec![owner("u1", None)]));
        let w = ctx.warnings();
        assert_eq!(w.len(), 1);
        assert!(w[0].starts_with(needle), "{}", w[0]);
        assert!(w[0].ends_with("please fix your requests; duplicate UID(s) observed: u1"));
    }

    // No duplicates: nothing recorded, object untouched.
    let mut c = cm("a", Some("ns"));
    c.metadata.owner_references = Some(vec![owner("u1", None), owner("u2", None)]);
    let ctx = RequestContext::new(Some("ns"));
    dedup_owner_references_and_add_warning(&mut c, &ctx, false);
    assert!(ctx.warnings().is_empty());
}

/// create.go:211-218 / update.go:226-240 / patch.go:710-726: a too-large write
/// is retried exactly once, with managedFields stripped; any other error, and
/// a second too-large error, are returned as they are.
mod too_large_retry {
    use super::super::rest::retry_without_managed_fields_if_too_large;
    use rusternetes_common::Error;

    fn too_large() -> Error {
        Error::StorageTooLarge("etcdserver: request is too large".into())
    }

    #[tokio::test]
    async fn retries_once_without_managed_fields() {
        let mut calls = Vec::new();
        let out = retry_without_managed_fields_if_too_large(|strip| {
            calls.push(strip);
            let r = if strip {
                Ok("stored")
            } else {
                Err(too_large())
            };
            async move { r }
        })
        .await;
        assert_eq!(out.unwrap(), "stored");
        assert_eq!(calls, vec![false, true]);
    }

    #[tokio::test]
    async fn success_is_not_retried() {
        let mut calls = 0;
        let out = retry_without_managed_fields_if_too_large(|_| {
            calls += 1;
            async { Ok(()) }
        })
        .await;
        assert!(out.is_ok());
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn other_errors_are_not_retried() {
        let mut calls = 0;
        let out: rusternetes_common::Result<()> = retry_without_managed_fields_if_too_large(|_| {
            calls += 1;
            async { Err(Error::Storage("etcdserver: leader changed".into())) }
        })
        .await;
        assert!(matches!(out, Err(Error::Storage(_))));
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn a_second_too_large_error_is_returned() {
        let mut calls = 0;
        let out: rusternetes_common::Result<()> = retry_without_managed_fields_if_too_large(|_| {
            calls += 1;
            async { Err(too_large()) }
        })
        .await;
        assert!(out.unwrap_err().is_too_large_error());
        assert_eq!(calls, 2);
    }
}

/// `--enable-admission-plugins` / `--disable-admission-plugins` choose the
/// chain (apiserver/pkg/server/options/admission.go `enabledPluginNames`):
/// NodeRestriction is off by default (pkg/kubeapiserver/options/plugins.go
/// `DefaultOffAdmissionPlugins`), so a node's non-mirror pod is admitted, and
/// it is refused once the plugin is enabled.
mod admission_plugin_selection {
    use std::sync::Arc;

    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
    use rusternetes_common::auth::{TokenManager, UserInfo};
    use rusternetes_common::authz::{AlwaysAllowAuthorizer, Authorizer};
    use rusternetes_common::observability::MetricsRegistry;
    use rusternetes_common::resources::Pod;
    use rusternetes_storage::StorageBackend;

    use crate::admission::plugins::EnabledForTest;
    use crate::endpoints::handlers::admission::Admission;
    use crate::state::ApiServerState;

    fn state() -> ApiServerState {
        ApiServerState::new(
            Arc::new(StorageBackend::new_memory()),
            Arc::new(TokenManager::new(b"test-secret")),
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn Authorizer>,
            Arc::new(MetricsRegistry::new()),
            true,
        )
    }

    async fn user_creates_plain_pod(username: &str) -> rusternetes_common::Result<Pod> {
        let state = state();
        let kind = GroupVersionKind {
            group: String::new(),
            version: "v1".into(),
            kind: "Pod".into(),
        };
        let resource = GroupVersionResource {
            group: String::new(),
            version: "v1".into(),
            resource: "pods".into(),
        };
        let user = UserInfo {
            username: username.into(),
            uid: String::new(),
            groups: if username.starts_with("system:node:") {
                vec!["system:nodes".into()]
            } else {
                vec![]
            },
            extra: Default::default(),
        };
        let admission = Admission {
            state: &state,
            kind: &kind,
            resource: &resource,
            subresource: None,
            namespace: Some("default"),
            user: &user,
            dry_run: false,
            forbid_create: false,
        };
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"nodeName": "node-1", "containers": [{"name": "c", "image": "i"}]}
        }))
        .unwrap();
        admission.admit(Operation::Create, pod, None).await
    }

    async fn node_creates_plain_pod() -> rusternetes_common::Result<Pod> {
        user_creates_plain_pod("system:node:node-1").await
    }

    #[tokio::test]
    async fn node_restriction_is_off_by_default() {
        node_creates_plain_pod()
            .await
            .expect("NodeRestriction is default-off");
    }

    #[tokio::test]
    async fn node_restriction_runs_when_enabled() {
        let _g = EnabledForTest::new(&["NodeRestriction"]);
        let err = node_creates_plain_pod().await.unwrap_err();
        assert!(err.to_string().contains("mirror pods"), "{err}");
    }

    #[tokio::test]
    async fn priority_stops_when_disabled() {
        let _g = EnabledForTest::new(&[]);
        let pod = user_creates_plain_pod("alice").await.unwrap();
        assert_eq!(pod.spec.unwrap().priority, None, "Priority is off");
    }

    #[tokio::test]
    async fn priority_runs_by_default() {
        let pod = user_creates_plain_pod("alice").await.unwrap();
        assert_eq!(pod.spec.unwrap().priority, Some(0), "Priority is on");
    }
}

/// `errors.NewInvalid` for the request-options validators and the patch
/// decode failure (#2392). Upstream: update.go:93, create.go:110,
/// patch.go:125, 345-347, delete.go:132 — all go through
/// `NewInvalid(schema.GroupKind{...}, "", errs)`
/// (apimachinery errors.go:284-312), so the Status message carries the
/// `<Kind>.<group> "" is invalid: ` prefix and `details.kind/group`.
mod options_new_invalid {
    use super::*;
    use rusternetes_common::Error;

    fn status(err: Error) -> Box<rusternetes_common::types::Status> {
        match err {
            Error::Status(s) => s,
            other => panic!("expected a NewInvalid Status, got {other:?}"),
        }
    }

    fn assert_meta_invalid(err: Error, kind: &str, field: &str) {
        let st = status(err);
        let msg = st.message.clone().unwrap_or_default();
        let prefix = format!("{kind}.meta.k8s.io \"\" is invalid: {field}: ");
        assert!(msg.starts_with(&prefix), "message {msg:?}");
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
        assert_eq!(st.code, Some(422));
        let d = st.details.expect("details");
        assert_eq!(d.kind.as_deref(), Some(kind));
        assert_eq!(d.group.as_deref(), Some("meta.k8s.io"));
        let c = &d.causes.expect("causes")[0];
        assert_eq!(c.field.as_deref(), Some(field));
        assert!(
            !c.message.as_deref().unwrap_or("").starts_with(field),
            "cause message is ErrorBody(), without the field prefix"
        );
    }

    #[test]
    fn delete_options() {
        let err = decode_delete_options(&q(&[("dryRun", "Maybe")]), b"").unwrap_err();
        assert_meta_invalid(err, "DeleteOptions", "dryRun");
    }

    #[test]
    fn create_options() {
        let err =
            super::super::create::checked_create_options(&q(&[("dryRun", "Maybe")])).unwrap_err();
        assert_meta_invalid(err, "CreateOptions", "dryRun");
    }

    #[test]
    fn update_options() {
        let err =
            super::super::update::checked_update_options(&q(&[("dryRun", "Maybe")])).unwrap_err();
        assert_meta_invalid(err, "UpdateOptions", "dryRun");
    }

    #[test]
    fn patch_options() {
        let err = super::super::patch::checked_patch_options(
            &q(&[("dryRun", "Maybe")]),
            "application/merge-patch+json",
        )
        .unwrap_err();
        assert_meta_invalid(err, "PatchOptions", "dryRun");
    }

    /// patch.go:345-347 uses the zero GroupKind and an empty name, so the
    /// message has an empty qualified kind: ` "" is invalid: patch: ...`.
    #[test]
    fn patch_decode_failure() {
        let st = status(super::super::patch::patch_invalid(
            "{}".into(),
            "boom".into(),
        ));
        assert_eq!(
            st.message.as_deref(),
            Some(r#" "" is invalid: patch: Invalid value: "{}": boom"#)
        );
        let d = st.details.expect("details");
        assert_eq!((d.kind, d.group), (None, None));
        assert_eq!(d.causes.expect("causes")[0].field.as_deref(), Some("patch"));
    }
}
