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

/// `ToSelectableFields` makes `source` fall back to the reporting controller
/// (strategy.go:117-121). Selection here reads the stored `source.component`,
/// so the fallback is applied to the object instead. A deliberate deviation
/// kept from the handlers this replaces; the update path runs it before
/// validation so `source` compares like with like against the stored object.
fn backfill_source_component(event: &mut Event) {
    if event.source.component.is_empty() {
        if let Some(rc) = event.reporting_component.as_ref() {
            event.source.component = rc.clone();
        }
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

    fn canonicalize(&self, obj: &mut Event) {
        backfill_source_component(obj);
    }
}

impl RestUpdateStrategy<Event> for Strategy {
    /// strategy.go:74-76.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// strategy.go:59-60: nothing to prepare beyond the source back-fill above.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Event, _old: &Event) {
        backfill_source_component(obj);
    }

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

/// `NewREST` (storage/storage.go:40-60).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Event, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "events"),
        Arc::new(Strategy),
    )
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

    #[test]
    fn the_source_component_falls_back_to_the_reporting_controller() {
        let mut e: Event = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "e"}, "reportingComponent": "ctl"
        }))
        .unwrap();
        RestCreateStrategy::canonicalize(&Strategy, &mut e);
        assert_eq!(e.source.component, "ctl");
    }
}
