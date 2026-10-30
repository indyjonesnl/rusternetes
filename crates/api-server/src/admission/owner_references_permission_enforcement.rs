//! The `OwnerReferencesPermissionEnforcement` admission plugin.
//!
//! Port of `plugin/pkg/admission/gc/gc_admission.go` (`Validate`,
//! `isChangingOwnerReference`, `newBlockingOwnerDeletionRefs`,
//! `ownerRefToDeleteAttributeRecords`, `finalizeAnythingRecord`) and its
//! tests (`gc_admission_test.go`: `TestGCAdmission`,
//! `TestBlockOwnerDeletionAdmission`). Registered for CREATE and UPDATE; the
//! plugin is default-off (`DefaultOffAdmissionPlugins`,
//! `pkg/kubeapiserver/options/plugins.go`).
//!
//! - Changing an object's ownerReferences on UPDATE requires `delete` on the
//!   object itself.
//! - Setting `blockOwnerDeletion: true` (newly, on CREATE or UPDATE) requires
//!   `update` on `<owner resource>/finalizers`, the owner's resource being
//!   resolved from `apiVersion`/`kind` with the RESTMapper.
//!
//! `SetAuthorizer` / `SetRESTMapper` map to the constructor arguments: the
//! server's authorizer and the discovery-backed
//! [`ServedRestMapper`](crate::handlers::custom_metrics::ServedRestMapper).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Opinion, RequestAttributes};
use rusternetes_common::types::{ObjectMeta, OwnerReference};
use rusternetes_discovery::restmapper::{Gk, MapperError, RestMapping, Scope};

use crate::handlers::custom_metrics::ServedRestMapper;

/// The `meta.RESTMapper` surface the plugin uses: `RESTMappings`.
#[async_trait]
pub trait OwnerRestMapper: Send + Sync {
    async fn rest_mappings(
        &self,
        gk: &Gk,
        versions: &[&str],
    ) -> Result<Vec<RestMapping>, MapperError>;
}

#[async_trait]
impl OwnerRestMapper for ServedRestMapper {
    async fn rest_mappings(
        &self,
        gk: &Gk,
        versions: &[&str],
    ) -> Result<Vec<RestMapping>, MapperError> {
        ServedRestMapper::rest_mappings(self, gk, versions).await
    }
}

/// The parts of `admission.Attributes` the plugin reads.
pub struct Attributes<'a> {
    pub user: &'a UserInfo,
    /// `GetResource()`: group, version, resource.
    pub group: &'a str,
    pub version: &'a str,
    pub resource: &'a str,
    pub subresource: Option<&'a str>,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
    /// `GetOperation() == admission.Create`.
    pub create: bool,
}

/// `whiteListItem` (gc_admission.go:74-77).
struct WhiteListItem {
    group: &'static str,
    resource: &'static str,
    subresource: &'static str,
}

/// The whitelist `Register` builds (gc_admission.go:41-49): the pods/status
/// endpoint is ignored since old kubelets corrupt it; the pod status
/// strategy ensures status updates cannot mutate ownerRef.
const WHITE_LIST: &[WhiteListItem] = &[WhiteListItem {
    group: "",
    resource: "pods",
    subresource: "status",
}];

/// `gcPermissionsEnforcement`.
pub struct OwnerReferencesPermissionEnforcement {
    authorizer: Arc<dyn Authorizer>,
    rest_mapper: Arc<dyn OwnerRestMapper>,
}

impl OwnerReferencesPermissionEnforcement {
    pub fn new(authorizer: Arc<dyn Authorizer>, rest_mapper: Arc<dyn OwnerRestMapper>) -> Self {
        Self {
            authorizer,
            rest_mapper,
        }
    }

    /// `isWhiteListed` (gc_admission.go:80-87).
    fn is_white_listed(attributes: &Attributes<'_>) -> bool {
        WHITE_LIST.iter().any(|item| {
            item.group == attributes.group
                && item.resource == attributes.resource
                && item.subresource == attributes.subresource.unwrap_or_default()
        })
    }

    /// `Authorize`, as the `decision, reason, err` triple the error strings
    /// print: `Ok(())` on `DecisionAllow`, else `"<reason>, <err>"`.
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<(), String> {
        match self.authorizer.authorize_opinion(attrs).await {
            Ok(Opinion::Allow(_)) => Ok(()),
            Ok(Opinion::Deny(reason)) => Err(format!("{reason}, <nil>")),
            Ok(Opinion::NoOpinion { reason, error }) => Err(format!(
                "{reason}, {}",
                error.unwrap_or_else(|| "<nil>".to_string())
            )),
            Err(e) => Err(format!(", {e}")),
        }
    }

    /// `Validate` (gc_admission.go:89-156). `Err` carries the message
    /// `admission.NewForbidden` wraps.
    pub async fn validate(
        &self,
        attributes: &Attributes<'_>,
        new: &ObjectMeta,
        old: Option<&ObjectMeta>,
    ) -> Result<(), String> {
        // if the request is in the whitelist, we skip mutation checks for
        // this resource.
        if Self::is_white_listed(attributes) {
            return Ok(());
        }

        // if we aren't changing owner references, then the edit is always
        // allowed
        if !is_changing_owner_reference(new, old) {
            return Ok(());
        }

        // if you are creating a thing, you should always be allowed to set
        // an owner ref since you logically had the power to never create it.
        // We still need to check block owner deletion below, because the
        // power to delete does not imply the power to prevent deletion on
        // other resources.
        if !attributes.create {
            let mut delete =
                RequestAttributes::new(attributes.user.clone(), "delete", attributes.resource)
                    .with_api_group(attributes.group)
                    .with_api_version(attributes.version)
                    .with_name(attributes.name);
            if let Some(ns) = attributes.namespace {
                delete = delete.with_namespace(ns);
            }
            if let Some(sub) = attributes.subresource {
                delete = delete.with_subresource(sub);
            }
            if let Err(why) = self.authorize(&delete).await {
                return Err(format!(
                    "cannot set an ownerRef on a resource you can't delete: {why}"
                ));
            }
        }

        // Further check if the user is setting
        // ownerReference.blockOwnerDeletion to true. If so, only allows the
        // change if the user has delete permission of the _OWNER_.
        let new_blocking_refs = new_blocking_owner_deletion_refs(new, old);
        if new_blocking_refs.is_empty() {
            return Ok(());
        }

        // There can be a case where a restMapper tries to hit discovery
        // endpoints and times out if the network is inaccessible. This can
        // prevent creating the pod to run the network to be able to do
        // discovery and it appears as a timeout, not a rejection. Because
        // the timeout is wrapper on admission/request, we can run a single
        // check to see if the user can finalize any possible resource.
        if self
            .authorize(&finalize_anything_record(attributes.user))
            .await
            .is_ok()
        {
            return Ok(());
        }

        for r in new_blocking_refs {
            let records = self
                .owner_ref_to_delete_attribute_records(r, attributes)
                .await
                .map_err(|e| {
                    format!(
                        "cannot set blockOwnerDeletion in this case because cannot find RESTMapping for APIVersion {} Kind {}: {e}",
                        r.api_version, r.kind
                    )
                })?;
            // Multiple records are returned if ref.Kind could map to
            // multiple resources. User needs to have delete permission on
            // all the matched Resources.
            for record in records {
                if let Err(why) = self.authorize(&record).await {
                    return Err(format!(
                        "cannot set blockOwnerDeletion if an ownerReference refers to a resource you can't set finalizers on: {why}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// `ownerRefToDeleteAttributeRecords` (gc_admission.go:203-235):
    /// OwnerReference only records the object kind, which might map to
    /// multiple resources, so multiple records might be returned.
    async fn owner_ref_to_delete_attribute_records(
        &self,
        r: &OwnerReference,
        attributes: &Attributes<'_>,
    ) -> Result<Vec<RequestAttributes>, String> {
        let (group, version) = parse_group_version(&r.api_version)?;
        let mappings = self
            .rest_mapper
            .rest_mappings(
                &Gk {
                    group,
                    kind: r.kind.clone(),
                },
                &[version.as_str()],
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut ret = Vec::new();
        for mapping in mappings {
            let mut ar = RequestAttributes::new(
                attributes.user.clone(),
                "update",
                mapping.resource.resource.clone(),
            )
            .with_api_group(mapping.resource.group.clone())
            .with_api_version(mapping.resource.version.clone())
            .with_subresource("finalizers")
            .with_name(r.name.clone());
            if mapping.scope == Scope::Namespace {
                // if the owner is namespaced, it must be in the same
                // namespace as the dependent is.
                if let Some(ns) = attributes.namespace {
                    ar = ar.with_namespace(ns);
                }
            }
            ret.push(ar);
        }
        Ok(ret)
    }
}

/// `schema.ParseGroupVersion` (apimachinery/pkg/runtime/schema/group_version.go).
pub fn parse_group_version(gv: &str) -> Result<(String, String), String> {
    if gv.is_empty() || gv == "/" {
        return Ok((String::new(), String::new()));
    }
    match gv.matches('/').count() {
        0 => Ok((String::new(), gv.to_string())),
        1 => {
            let (g, v) = gv.split_once('/').unwrap_or_default();
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(format!("unexpected GroupVersion string: {gv}")),
    }
}

/// `isChangingOwnerReference` (gc_admission.go:158-187).
fn is_changing_owner_reference(new: &ObjectMeta, old: Option<&ObjectMeta>) -> bool {
    let new_owners = new.owner_references.as_deref().unwrap_or_default();
    let Some(old) = old else {
        return !new_owners.is_empty();
    };
    // compare the old and new. If they aren't the same, then we're trying
    // to change an ownerRef.
    old.owner_references.as_deref().unwrap_or_default() != new_owners
}

/// `finalizeAnythingRecord` (gc_admission.go:189-201).
fn finalize_anything_record(user: &UserInfo) -> RequestAttributes {
    RequestAttributes::new(user.clone(), "update", "*")
        .with_api_group("*")
        .with_api_version("*")
        .with_subresource("finalizers")
        .with_name("*")
}

fn is_blocking(r: &OwnerReference) -> bool {
    r.block_owner_deletion == Some(true)
}

/// `newBlockingOwnerDeletionRefs` (gc_admission.go:256-294): the new
/// blocking ownerReferences, and references whose blockOwnerDeletion field
/// changed from nil or false to true.
fn new_blocking_owner_deletion_refs<'a>(
    new: &'a ObjectMeta,
    old: Option<&ObjectMeta>,
) -> Vec<&'a OwnerReference> {
    let blocking_new: Vec<&OwnerReference> = new
        .owner_references
        .iter()
        .flatten()
        .filter(|r| is_blocking(r))
        .collect();
    if blocking_new.is_empty() {
        return Vec::new();
    }
    let Some(old) = old else {
        return blocking_new;
    };
    // `indexByUID`: a later reference with the same UID wins.
    let indexed_old: HashMap<&str, &OwnerReference> = old
        .owner_references
        .iter()
        .flatten()
        .map(|r| (r.uid.as_str(), r))
        .collect();
    blocking_new
        .into_iter()
        .filter(|r| match indexed_old.get(r.uid.as_str()) {
            // if ref is newly added, and it's blocking, then returns it.
            None => true,
            Some(old_ref) => !is_blocking(old_ref),
        })
        .collect()
}
