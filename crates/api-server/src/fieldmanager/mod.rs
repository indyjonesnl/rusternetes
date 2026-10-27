//! The field manager's Update path: the `managedFields` entry that a create,
//! an update or a non-apply patch records for the requester (#2701).
//!
//! Port of `staging/src/k8s.io/apimachinery/pkg/util/managedfields/`:
//!
//! - `internal/fieldmanager.go` -- `FieldManager.Update` (:94-137),
//!   `UpdateNoErrors` (:140-160), `decodeLiveOrNew` (:63-92),
//!   `isResetManagedFields`, and the layering `NewDefaultFieldManager`
//!   (:46-61) builds: version check, last-applied, skip-non-applied (with
//!   `DefaultTrackOnCreateProbability` = 1), cap-managers
//!   (`DefaultMaxUpdateManagers` = 10), build-manager-info, the
//!   managed-fields updater, strip-meta, and the structured merge.
//! - `internal/managedfields.go` -- `DecodeManagedFields`,
//!   `encodeManagedFields`, `sortEncodedManagedFields`,
//!   `BuildManagerIdentifier`.
//! - `internal/managedfieldsupdater.go`, `capmanagers.go`,
//!   `buildmanagerinfo.go`, `stripmeta.go`, `skipnonapplied.go`.
//! - `sigs.k8s.io/structured-merge-diff/v6/merge/update.go` -- `Updater.Update`
//!   and `Updater.update`.
//!
//! and of the call sites in `apiserver/pkg/endpoints/handlers`:
//! `create.go:199`, `update.go:160-167`, `patch.go:372` and `:466`, each with
//! `managerOrUserAgent` (`create.go:259-282`).
//!
//! Server-side apply is [`apply`]; this is the other half, for every write
//! that is not an apply.
//!
//! The engine itself (`Managed`, the apply path, the field sets) lives in
//! `rusternetes_common::fieldmanager` so the direct storage backends run the
//! same `FieldManager.Apply` (#3292); this module keeps what needs the
//! registry `Object` bound and the request (the typed Update entry points and
//! the user agent).
//!
//! DEVIATIONS, deliberate: (1) the field sets come from the kind's schema
//! ([`schemas`]: the vendored OpenAPI for `apps/v1`, `batch/v1` and `Pod`, a
//! custom resource's structural schema) and from [`fieldset`]'s name-driven
//! table for every other built-in kind (#2734); (2) a kind served at one
//! version has no per-manager `versionConverter` (the identity); a custom
//! resource with several versions gets one (#2736).

pub mod schemas;

pub use rusternetes_common::fieldmanager::{
    apply, fieldset, is_reset_managed_fields, remove_managed_fields, Managed, ResetFields,
};

use std::ops::Deref;

use rusternetes_common::validation::metav1::FIELD_MANAGER_MAX_LENGTH;
use serde_json::Value;

use crate::registry::rest::Object;

tokio::task_local! {
    /// `req.UserAgent()` of the request being served, for [`manager_or_user_agent`].
    static USER_AGENT: String;
}

/// Serve the rest of the request with its `User-Agent` known to the field
/// manager (upstream hands `req.UserAgent()` to each handler).
pub async fn user_agent_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let agent = req
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    USER_AGENT.scope(agent, next.run(req)).await
}

/// `managerOrUserAgent` (create.go:259-264): the `fieldManager` parameter, or
/// the user agent's product name.
pub fn manager_or_user_agent(field_manager: Option<&str>) -> String {
    match field_manager {
        Some(m) if !m.is_empty() => m.to_string(),
        _ => prefix_from_user_agent(&USER_AGENT.try_with(Clone::clone).unwrap_or_default()),
    }
}

/// `prefixFromUserAgent` (create.go:266-282): the characters before the first
/// `/`, without unprintable ones, cut to `FieldManagerMaxLength` bytes.
pub fn prefix_from_user_agent(user_agent: &str) -> String {
    let product = user_agent.split('/').next().unwrap_or_default();
    let mut out = String::new();
    for c in product.chars() {
        if c.is_control() {
            continue;
        }
        if out.len() + c.len_utf8() > FIELD_MANAGER_MAX_LENGTH {
            break;
        }
        out.push(c);
    }
    out
}

/// `FieldManager` (fieldmanager.go:38-44) for one served resource: the
/// shared engine plus the typed Update entry points.
pub struct FieldManager(rusternetes_common::fieldmanager::FieldManager);

impl Deref for FieldManager {
    type Target = rusternetes_common::fieldmanager::FieldManager;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FieldManager {
    pub fn new(api_version: &str, subresource: Option<&str>, reset_fields: ResetFields) -> Self {
        Self(rusternetes_common::fieldmanager::FieldManager::new(
            api_version,
            subresource,
            reset_fields,
        ))
    }

    /// The kind's version `Converter` (#2736), see
    /// [`rusternetes_common::fieldmanager::FieldManager::with_converter`].
    pub fn with_converter(
        self,
        converter: Option<
            std::sync::Arc<dyn rusternetes_common::fieldmanager::versionconverter::Converter>,
        >,
    ) -> Self {
        Self(self.0.with_converter(converter))
    }

    /// The kind's `typeConverter`, see
    /// [`rusternetes_common::fieldmanager::FieldManager::with_schema`].
    pub fn with_schema(
        self,
        schema: Option<std::sync::Arc<rusternetes_common::fieldmanager::schema::Type>>,
    ) -> Self {
        Self(self.0.with_schema(schema))
    }

    /// `UpdateNoErrors` (fieldmanager.go:140-160): `live` is `None` for a
    /// create. On failure the object comes back with no `managedFields`.
    pub fn update_no_errors<T: Object>(&self, live: Option<&T>, new: T, manager: &str) -> T {
        match self.update(live, new.clone(), manager) {
            Ok(obj) => obj,
            Err(e) => {
                tracing::error!("[SHOULD NOT HAPPEN] failed to update managedFields: {e}");
                let mut new = new;
                new.metadata_mut().managed_fields = None;
                new
            }
        }
    }

    /// `FieldManager.Update` (fieldmanager.go:94-137).
    pub fn update<T: Object>(
        &self,
        live: Option<&T>,
        mut new: T,
        manager: &str,
    ) -> Result<T, String> {
        // decodeLiveOrNew (fieldmanager.go:63-92). A subresource ignores the
        // managedFields of the request object: "in case the request tries to
        // manually set managedFields via a subresource".
        let live_managed = || {
            live.and_then(|l| l.metadata().managed_fields.as_ref())
                .map(|e| Managed::decode(e).unwrap_or_default())
                .unwrap_or_default()
        };
        let managed = if !self.subresource().is_empty() {
            live_managed()
        } else if is_reset_managed_fields(&new.metadata().managed_fields) {
            Managed::default()
        } else {
            match new
                .metadata()
                .managed_fields
                .as_ref()
                .map(|e| Managed::decode(e))
            {
                Some(Ok(m)) if !m.fields.is_empty() => m,
                _ => live_managed(),
            }
        };

        // RemoveObjectManagedFields(newObj).
        new.metadata_mut().managed_fields = None;

        let mut new_json = serde_json::to_value(&new).map_err(|e| e.to_string())?;
        let mut live_json = match live {
            Some(l) => serde_json::to_value(l).map_err(|e| e.to_string())?,
            None => Value::Object(Default::default()),
        };
        remove_managed_fields(&mut new_json);
        remove_managed_fields(&mut live_json);
        let live_is_new = live.is_none_or(|l| l.metadata().uid.is_empty());

        let managed = self.update_managed(&live_json, &new_json, managed, manager, live_is_new)?;
        new.metadata_mut().managed_fields = managed.encode();
        Ok(new)
    }
}

#[cfg(test)]
mod tests;
