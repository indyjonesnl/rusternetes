//! Unit tests moved from `src/registry/core/event.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::{apply_legacy, ApplyError, ApplyOptions, ApplyOutcome};
    pub use rusternetes_common::resources::{Event, EventV1};
    pub use rusternetes_common::validation::events::{
        validate_event_create, validate_event_update, RequestVersion,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_storage::event_recorder::DEFAULT_EVENT_TTL_SECONDS;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::event::*;

    fn ctx(group: &str, version: &str) -> RequestContext {
        RequestContext::new(Some("default")).with_group_version(group, version)
    }

    /// storage.go:42-44: every write carries the one TTL, whatever the
    /// operation and the existing TTL.
    #[test]
    fn the_event_store_expires_every_write_after_the_event_ttl() {
        let store = new_store(
            Arc::new(StorageBackend::Memory(Arc::new(
                rusternetes_storage::MemoryStorage::new(),
            ))),
            DEFAULT_EVENT_TTL_SECONDS,
        );
        let ttl_func = store.ttl_func.as_ref().expect("events have a TTLFunc");
        let event: Event =
            serde_json::from_value(serde_json::json!({"metadata": {"name": "e"}})).unwrap();
        assert_eq!(ttl_func(&event, 0, false).unwrap(), 3600);
        assert_eq!(ttl_func(&event, 7, true).unwrap(), 3600);
    }

    /// `--event-ttl` is a Go duration.
    #[test]
    fn event_ttl_parses_as_a_go_duration() {
        assert_eq!(parse_event_ttl("1h"), Ok(3600));
        assert_eq!(parse_event_ttl("90m"), Ok(5400));
        assert_eq!(parse_event_ttl("1h30m15s"), Ok(5415));
        assert_eq!(parse_event_ttl("0"), Ok(0));
        assert_eq!(parse_event_ttl("0s"), Ok(0));
        for bad in [
            "",
            "h",
            "1",
            "1x",
            "-1h",
            "1.5h",
            "1h 1m",
            "99999999999999999999h",
        ] {
            assert!(parse_event_ttl(bad).is_err(), "{bad:?} must not parse");
        }
    }

    /// strategy.go:46-92.
    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert_eq!(
            Strategy.default_garbage_collection_policy(&ctx("", "v1")),
            Some(GarbageCollectionPolicy::Unsupported)
        );
    }

    /// strategy.go:134-139 and events.go:43: only core `v1` and
    /// `events.k8s.io/v1beta1` are legacy; no RequestInfo is strict.
    #[test]
    fn the_request_version_comes_from_the_group_version() {
        assert_eq!(request_version(&ctx("", "v1")), RequestVersion::CoreV1);
        assert_eq!(
            request_version(&ctx("events.k8s.io", "v1beta1")),
            RequestVersion::EventsV1Beta1
        );
        assert_eq!(
            request_version(&ctx("events.k8s.io", "v1")),
            RequestVersion::EventsV1
        );
        assert_eq!(
            request_version(&RequestContext::new(Some("default"))),
            RequestVersion::EventsV1
        );
    }

    /// The stored source.component stays as the client sent it
    /// (strategy.go:117-121 only affects selection).
    #[test]
    fn the_source_component_is_not_backfilled() {
        let mut e: Event = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "e"}, "reportingComponent": "ctl"
        }))
        .unwrap();
        RestCreateStrategy::canonicalize(&Strategy, &mut e);
        assert_eq!(e.source.component, "");
    }
}
