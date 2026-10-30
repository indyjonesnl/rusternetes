//! Unit tests moved from `src/handlers/watch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        extract::{Path, Query, State},
        http::{header, StatusCode},
        response::{IntoResponse, Response},
        Extension,
    };
    pub use futures::future::BoxFuture;
    pub use futures::StreamExt;
    pub use rusternetes_api_server::registry::rest::HasMetadata;
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        types::{ObjectMeta, Status},
        Error, Result,
    };
    pub use rusternetes_storage::{build_prefix, Storage, WatchEvent};
    pub use serde::{de::DeserializeOwned, Deserialize, Serialize};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time::{interval, timeout};
    pub use tokio_stream::wrappers::ReceiverStream;
    pub use tracing::{debug, info, warn};
}

mod kind_table_tests {
    use rusternetes_api_server::handlers::watch::resource_type_to_kind_and_version;

    /// APIService must map to the correct Kind + apiVersion. The CamelCase
    /// fallback produced "Apiservice", which made client-go's APIService
    /// informer fail to decode watch bookmarks.
    #[test]
    fn apiservice_maps_to_correct_kind() {
        let (kind, api_version) =
            resource_type_to_kind_and_version("apiservices", "apiregistration.k8s.io");
        assert_eq!(kind, "APIService");
        assert_eq!(api_version, "apiregistration.k8s.io/v1");
    }

    /// A built-in still resolves correctly (regression guard for the match).
    #[test]
    fn builtin_pod_maps_to_pod() {
        let (kind, api_version) = resource_type_to_kind_and_version("pods", "");
        assert_eq!(kind, "Pod");
        assert_eq!(api_version, "v1");
    }
}

mod watch_bool_tests {
    use rusternetes_api_server::handlers::watch::{is_watch_request, parse_k8s_bool};
    use std::collections::HashMap;

    /// Kubernetes resolves `?watch=` via `runtime.Convert_Slice_string_To_bool`
    /// (conversion.go:83-95), where only "0" and "false" are false — NOT
    /// `strconv.ParseBool`.
    /// Rust's `str::parse::<bool>()` only accepts "true"/"false", so clients
    /// that send `?watch=1` (Lens and other non-client-go informers) were
    /// silently served a plain LIST instead of a watch stream — making their
    /// reflectors relist-loop (poll) instead of watching.
    #[test]
    fn parse_k8s_bool_matches_upstream_query_conversion() {
        for t in ["1", "t", "T", "true", "True", "TRUE"] {
            assert_eq!(parse_k8s_bool(t), Some(true), "{t} should be true");
        }
        // NOTE: "f"/"F" are TRUE here, not false. Go's strconv.ParseBool would
        // read them as false, but query params do not go through ParseBool —
        // only "0" and a case-insensitive "false" are false (see below), so a
        // bare "f" falls into the default true branch. This test previously
        // asserted the ParseBool spelling set and was wrong about these two.
        for f in ["0", "false", "False", "FALSE"] {
            assert_eq!(parse_k8s_bool(f), Some(false), "{f} should be false");
        }
        for t in ["f", "F"] {
            assert_eq!(
                parse_k8s_bool(t),
                Some(true),
                "{t:?} is true under Convert_Slice_string_To_bool, unlike ParseBool"
            );
        }
        // Upstream's rule is NOT strconv.ParseBool: for query parameters,
        // meta/v1 ListOptions conversion uses
        // runtime.Convert_Slice_string_To_bool
        // (staging/src/k8s.io/apimachinery/pkg/runtime/conversion.go:79-95,
        // wired at apis/meta/v1/zz_generated.conversion.go:388 for out.Watch):
        //
        //   "Only the absence of a value (i.e. zero-length slice), a value of
        //    "false", or a value of "0" resolve to false. Any other value
        //    (including empty string) resolves to true."
        //
        // So an unrecognised value is TRUE, not false, and never an error.
        for t in ["yes", "t", "T", "banana", "", "TRUE", "True", "1"] {
            assert_eq!(
                parse_k8s_bool(t),
                Some(true),
                "{t:?} resolves to true upstream (anything but 0/false does)"
            );
        }
        // Only these are false, case-insensitively for "false".
        for f in ["0", "false", "False", "FALSE", "fAlSe"] {
            assert_eq!(parse_k8s_bool(f), Some(false), "{f:?} must be false");
        }
    }

    /// The HashMap path (`watch_params_from_query`, used by ~50 handlers) had
    /// the same defect as the typed path: allowWatchBookmarks and
    /// sendInitialEvents went through a strict `parse::<bool>()`, so
    /// `?allowWatchBookmarks=1` was silently dropped rather than honoured.
    #[test]
    fn watch_params_from_query_accepts_k8s_query_bools() {
        use rusternetes_api_server::handlers::watch::watch_params_from_query;
        for v in ["1", "true", "t", "yes", ""] {
            let mut p = HashMap::new();
            p.insert("allowWatchBookmarks".to_string(), v.to_string());
            p.insert("sendInitialEvents".to_string(), v.to_string());
            let wp = watch_params_from_query(&p);
            assert_eq!(
                wp.allow_watch_bookmarks,
                Some(true),
                "allowWatchBookmarks={v:?}"
            );
            assert_eq!(
                wp.send_initial_events,
                Some(true),
                "sendInitialEvents={v:?}"
            );
        }
        for v in ["0", "false", "FALSE"] {
            let mut p = HashMap::new();
            p.insert("allowWatchBookmarks".to_string(), v.to_string());
            let wp = watch_params_from_query(&p);
            assert_eq!(
                wp.allow_watch_bookmarks,
                Some(false),
                "allowWatchBookmarks={v:?}"
            );
        }
        // Absent stays absent -- callers distinguish it from explicit false.
        let wp = watch_params_from_query(&HashMap::new());
        assert_eq!(wp.allow_watch_bookmarks, None);
        assert_eq!(wp.send_initial_events, None);
    }

    #[test]
    fn watch_params_accepts_k8s_query_bools() {
        use rusternetes_api_server::handlers::watch::WatchParams;
        // The typed Query<WatchParams> path (services, endpoints,
        // endpointslices -- 53 call sites) deserialized `watch` as a strict
        // Rust bool, so `?watch=1` was rejected outright with
        // "Failed to deserialize query string: provided string was not
        // `true` or `false`" while the HashMap-based pods path accepted it.
        // Measured against a live api-server: pods watch ok, services and
        // endpointslices 400.
        for (q, want) in [
            (r#"{"watch":"1"}"#, Some(true)),
            (r#"{"watch":"true"}"#, Some(true)),
            (r#"{"watch":"t"}"#, Some(true)),
            (r#"{"watch":"yes"}"#, Some(true)),
            (r#"{"watch":""}"#, Some(true)),
            (r#"{"watch":"0"}"#, Some(false)),
            (r#"{"watch":"false"}"#, Some(false)),
            (r#"{"watch":"FALSE"}"#, Some(false)),
            (r#"{}"#, None),
        ] {
            let p: WatchParams =
                serde_json::from_str(q).unwrap_or_else(|e| panic!("{q} must deserialize, got {e}"));
            assert_eq!(p.watch, want, "watch for {q}");
        }
        // A real JSON bool must still work (non-query deserialization paths).
        let p: WatchParams = serde_json::from_str(r#"{"watch":true}"#).unwrap();
        assert_eq!(p.watch, Some(true));

        // The other two query bools take the same path.
        let p: WatchParams =
            serde_json::from_str(r#"{"allowWatchBookmarks":"1","sendInitialEvents":"0"}"#).unwrap();
        assert_eq!(p.allow_watch_bookmarks, Some(true));
        assert_eq!(p.send_initial_events, Some(false));
    }

    #[test]
    fn is_watch_request_recognizes_watch_eq_1() {
        let mut p = HashMap::new();
        p.insert("watch".to_string(), "1".to_string());
        assert!(
            is_watch_request(&p),
            "?watch=1 must be a watch (Lens uses this)"
        );

        p.insert("watch".to_string(), "true".to_string());
        assert!(
            is_watch_request(&p),
            "?watch=true must be a watch (kubectl)"
        );

        p.insert("watch".to_string(), "0".to_string());
        assert!(!is_watch_request(&p), "?watch=0 is not a watch");

        assert!(!is_watch_request(&HashMap::new()), "no watch param = list");
    }
}

/// Deterministic unit tests for the watch field/label selector *transition*
/// logic that backs `[sig-api-machinery] CustomResourceFieldSelectors` (#234):
/// a watched object that is updated *out of* the selector must surface as a
/// synthetic `DELETED`, and one updated *into* it as a synthetic `ADDED`, so an
/// informer's local cache ends up holding exactly the matching objects. Mirrors
/// upstream `staging/src/k8s.io/apiserver/pkg/storage/cacher`'s
/// add/update/delete event derivation, exercised here on the pure helpers so
/// the assertions stay timing-free.
mod selector_transition_tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::watch::*;
    use rusternetes_common::resources::CustomResource;
    use rusternetes_common::types::ObjectMeta;
    use std::collections::HashSet;

    /// A custom resource with a top-level selectable field `color`, matching the
    /// e2e CRD's `x-kubernetes-selectable-fields: [{ jsonPath: .color }]`.
    fn cr(name: &str, color: &str) -> CustomResource {
        let mut extra = std::collections::HashMap::new();
        extra.insert("color".to_string(), serde_json::json!(color));
        CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta {
                name: name.to_string(),
                ..Default::default()
            },
            spec: None,
            status: None,
            extra,
        }
    }

    fn sel(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    #[test]
    fn added_matches_tracks_exclusions() {
        let fs = sel("color=blue");
        let mut excluded = HashSet::new();

        // A non-matching ADDED is dropped and remembered as excluded, so a later
        // transition INTO the selector can be promoted to ADDED.
        assert!(!watch_added_matches(
            &cr("c1", "red"),
            &None,
            &fs,
            &mut excluded
        ));
        assert!(excluded.contains("c1"));

        // A matching ADDED passes and is not excluded.
        assert!(watch_added_matches(
            &cr("c2", "blue"),
            &None,
            &fs,
            &mut excluded
        ));
        assert!(!excluded.contains("c2"));
    }

    #[test]
    fn modified_out_of_selector_emits_deleted_once() {
        let fs = sel("color=blue");
        let mut excluded = HashSet::new();

        // c1 was matching (not excluded); recolour red → falls out → DELETED.
        let et = watch_modified_event_type(&cr("c1", "red"), &None, &fs, &mut excluded)
            .expect("out-of-selector transition must emit an event");
        assert!(matches!(et, WatchEventType::Deleted));
        assert!(excluded.contains("c1"));

        // A second non-matching update is suppressed (already excluded).
        assert!(
            watch_modified_event_type(&cr("c1", "green"), &None, &fs, &mut excluded).is_none(),
            "repeat non-match must be suppressed, not a second DELETED"
        );
    }

    #[test]
    fn modified_into_selector_emits_added() {
        let fs = sel("color=blue");
        let mut excluded = HashSet::from(["c1".to_string()]);

        // c1 was excluded (red); recolour blue → enters selector → ADDED.
        let et = watch_modified_event_type(&cr("c1", "blue"), &None, &fs, &mut excluded)
            .expect("into-selector transition must emit an event");
        assert!(matches!(et, WatchEventType::Added));
        assert!(!excluded.contains("c1"));
    }

    #[test]
    fn modified_match_to_match_stays_modified() {
        let fs = sel("color=blue");
        let mut excluded = HashSet::new();
        let et = watch_modified_event_type(&cr("c1", "blue"), &None, &fs, &mut excluded)
            .expect("match→match must emit MODIFIED");
        assert!(matches!(et, WatchEventType::Modified));
    }

    #[test]
    fn modified_without_selector_always_modified() {
        let mut excluded = HashSet::new();
        let et = watch_modified_event_type(&cr("c1", "red"), &None, &None, &mut excluded)
            .expect("no selector must always emit MODIFIED");
        assert!(matches!(et, WatchEventType::Modified));
        assert!(excluded.is_empty(), "no selector ⇒ no exclusion tracking");
    }
}
