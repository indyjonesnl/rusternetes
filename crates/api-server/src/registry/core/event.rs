//! Event strategy and storage -- port of `pkg/registry/core/event/strategy.go`
//! and `pkg/registry/core/event/storage/storage.go`.
//!
//! One registry serves both `/api/v1` `events` and `/apis/events.k8s.io/v1`
//! `events`. The strategy's validation depends on which of them the request came
//! in on (`requestGroupVersion`, strategy.go:134-139), which is read here from
//! [`RequestContext::group_version`].

use std::sync::Arc;

use rusternetes_common::resources::{Event, EventV1};
use rusternetes_common::validation::events::{
    validate_event_create, validate_event_update, RequestVersion,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::ssa::{apply_legacy, ApplyError, ApplyOptions, ApplyOutcome};

/// The shape a decoded Event body takes before the Store sees it. The stored
/// object is the core one whichever API it arrived on, so an
/// `events.k8s.io/v1` body that has been converted onto it
/// (`Convert_v1_Event_To_core_Event`, pkg/apis/events/v1/conversion.go:29-43)
/// is stamped as core here.
pub fn convert_to_internal(event: &mut Event) {
    event.api_version = "v1".to_string();
    event.kind = "Event".to_string();
}

/// `requestGroupVersion` + the comparisons `ValidateEventCreate` /
/// `ValidateEventUpdate` make against it (pkg/apis/core/validation/events.go:
/// 43, 76): `v1` and `events.k8s.io/v1beta1` are legacy-only; every other
/// group-version, a missing one included, takes the strict rules.
fn request_version(ctx: &RequestContext) -> RequestVersion {
    match ctx
        .group_version
        .as_ref()
        .map(|(g, v)| (g.as_str(), v.as_str()))
    {
        Some(("", "v1")) => RequestVersion::CoreV1,
        Some(("events.k8s.io", "v1beta1")) => RequestVersion::EventsV1Beta1,
        _ => RequestVersion::EventsV1,
    }
}

/// `eventStrategy` (strategy.go:35-44).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Event> for Strategy {
    /// strategy.go:56-57: nothing to prepare.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut Event) {}

    /// `ValidateEventCreate` (strategy.go:59-64).
    fn validate(&self, ctx: &RequestContext, obj: &Event) -> ErrorList {
        validate_event_create(obj, request_version(ctx))
    }
}

impl RestUpdateStrategy<Event> for Strategy {
    /// strategy.go:74-76.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// strategy.go:59-60: nothing to prepare. The `source` fallback lives in
    /// field selection (`field_selector.rs`), not in the stored object.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut Event, _old: &Event) {}

    /// `ValidateEventUpdate` (strategy.go:78-83).
    fn validate_update(&self, ctx: &RequestContext, obj: &Event, old: &Event) -> ErrorList {
        validate_event_update(obj, old, request_version(ctx))
    }

    /// strategy.go:90-92.
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<Event> for Strategy {
    /// strategy.go:46-48: `rest.Unsupported`.
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::Unsupported)
    }
}

/// `--event-ttl`'s default, 1h (`pkg/controlplane/apiserver/options/options.go:129`),
/// in seconds. Defined in the storage crate so in-process recorders share it.
pub use rusternetes_storage::event_recorder::DEFAULT_EVENT_TTL_SECONDS;

/// `NewREST(optsGetter, ttl)` (storage/storage.go:40-60): events expire `ttl`
/// seconds after they are written. The `TTLFunc` ignores the existing TTL and
/// the operation (storage.go:42-44):
///
/// ```go
/// TTLFunc: func(runtime.Object, uint64, bool) (uint64, error) { return ttl, nil },
/// ```
pub fn new_store(storage: Arc<StorageBackend>, ttl: u64) -> Store<Event, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "events"),
        Arc::new(Strategy),
    )
    .with_ttl_func(Arc::new(move |_, _, _| Ok(ttl)))
}

/// `--event-ttl`: a Go `time.Duration` (`1h`, `90m`, `1h30m`, `45s`) as whole
/// seconds, which is what `uint64(c.EventTTL.Seconds())` hands `NewREST`
/// (pkg/registry/core/rest/storage_core_generic.go:90). `0` disables expiry.
/// Only the `h`, `m` and `s` units, with integer values, are accepted.
pub fn parse_event_ttl(text: &str) -> Result<u64, String> {
    let bad = || format!("invalid duration {text:?}: want e.g. 1h, 90m, 1h30m or 45s");
    if text == "0" {
        return Ok(0);
    }
    let mut rest = text;
    let mut total: u64 = 0;
    if rest.is_empty() {
        return Err(bad());
    }
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
        if digits == 0 {
            return Err(bad());
        }
        let value: u64 = rest[..digits].parse().map_err(|_| bad())?;
        let unit: u64 = match rest[digits..].chars().next() {
            Some('h') => 3600,
            Some('m') => 60,
            Some('s') => 1,
            _ => return Err(bad()),
        };
        total = value
            .checked_mul(unit)
            .and_then(|n| total.checked_add(n))
            .ok_or_else(bad)?;
        rest = &rest[digits + 1..];
    }
    Ok(total)
}

/// Stored (core) Event JSON in the `events.k8s.io/v1` shape
/// (`Convert_core_Event_To_v1_Event`). Best-effort, like every watch
/// converter: an object that will not decode is passed through unchanged.
pub fn core_event_json_to_events_v1(value: &serde_json::Value) -> serde_json::Value {
    match serde_json::from_value::<Event>(value.clone()) {
        Ok(event) => serde_json::to_value(event.to_events_v1()).unwrap_or_else(|_| value.clone()),
        Err(_) => value.clone(),
    }
}

/// A patched `events.k8s.io/v1` document back onto the stored core shape, via
/// [`EventV1::into_core`] -- the same conversion a create or update body goes
/// through, so PATCH cannot drift from them.
pub fn events_v1_json_onto_core_event(
    stored: &serde_json::Value,
    patched: serde_json::Value,
) -> serde_json::Value {
    let Ok(v1) = serde_json::from_value::<EventV1>(patched.clone()) else {
        // Hand back a document that will not decode as the v1 type so the
        // caller's own decode produces the error message rather than this
        // conversion swallowing it.
        return patched;
    };
    let Ok(mut core) = serde_json::to_value(v1.into_core()) else {
        return patched;
    };
    // Carry over whatever `Event::extra` was holding -- keys neither schema
    // names, kept only so a stored object survives a round trip unchanged. The
    // patch was written against the v1 shape, so it cannot have named them.
    if let Some(extra) = serde_json::from_value::<Event>(stored.clone())
        .ok()
        .and_then(|event| event.extra)
    {
        if let Some(obj) = core.as_object_mut() {
            for (key, value) in extra {
                obj.entry(key).or_insert(value);
            }
        }
    }
    core
}

/// The conversion a PATCH to the `events.k8s.io/v1` endpoint applies around
/// the patch (patch.go:323-338, :449-462).
pub const EVENTS_V1_PATCH_CONVERSION: crate::endpoints::handlers::RequestVersionConversion =
    crate::endpoints::handlers::RequestVersionConversion {
        to_request_version: core_event_json_to_events_v1,
        from_request_version: events_v1_json_onto_core_event,
    };

/// Server-side apply on `events.k8s.io/v1`: the apply configuration is written
/// against that version, so the live object is merged in it and the result
/// converted back (`structuredmerge.go:139`, `toVersioned`).
pub fn apply_events_v1(
    current: Option<&Event>,
    desired: &serde_json::Value,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome<Event>, ApplyError> {
    let current_json = current
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| ApplyError::Internal(e.to_string()))?;
    let versioned = current_json.as_ref().map(core_event_json_to_events_v1);
    match apply_legacy::<serde_json::Value>(versioned.as_ref(), desired, opts)? {
        ApplyOutcome::Applied { object } => {
            let core = events_v1_json_onto_core_event(
                current_json.as_ref().unwrap_or(&serde_json::Value::Null),
                *object,
            );
            let object = serde_json::from_value(core).map_err(|e| ApplyError::InvalidBody {
                kind: "Event",
                message: e.to_string(),
            })?;
            Ok(ApplyOutcome::Applied {
                object: Box::new(object),
            })
        }
        ApplyOutcome::Conflicts(c) => Ok(ApplyOutcome::Conflicts(c)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
