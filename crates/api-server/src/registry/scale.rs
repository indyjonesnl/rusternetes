//! The `/scale` subresource over a scalable resource's store — port of
//! `ScaleREST`, `scaleUpdatedObjectInfo` and `toScale{Create,Update}Validation`
//! (pkg/registry/apps/deployment/storage/storage.go:283-480).
//!
//! Upstream repeats this per resource (Deployment, ReplicaSet, StatefulSet,
//! ReplicationController) with only `scaleFrom<Resource>` and the replicas
//! field differing; here the resource supplies those two as a [`Scalable`].
//!
//! Not ported: `managedfields.NewScaleHandler`, which maps the parent's
//! `spec.replicas` managedFields entries to and from the Scale (#1996).

use async_trait::async_trait;
use rusternetes_common::deletion::Preconditions;
use rusternetes_common::resources::Scale;
use rusternetes_common::validation::hpa::validate_scale;
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::{Store, UpdateOptions};
use crate::registry::rest::{
    conflict, not_found, GroupResource, Object, RequestContext, RestStorage, UpdatedObjectInfo,
    ValidateObject, ValidateObjectUpdate,
};

/// What a resource provides to be served at `/scale`.
pub struct Scalable<P> {
    /// `scaleFrom<Resource>`: the Scale view of the object. An error is a
    /// selector that does not convert, which upstream answers as BadRequest.
    pub to_scale: fn(&P) -> std::result::Result<Scale, String>,
    /// Moves `Scale.spec.replicas` onto the object (`deployment.Spec.Replicas
    /// = scale.Spec.Replicas`).
    pub set_replicas: fn(&mut P, i32),
}

/// `ScaleREST`: `store` is the resource's main store, so a scale runs the
/// main update strategy — it is a spec change.
pub struct ScaleRest<P: Object> {
    store: Store<P, StorageBackend>,
    scalable: Scalable<P>,
    /// `<resource>/scale`, the resource `scaleUpdatedObjectInfo` names in its
    /// NotFound and Conflict errors.
    scale_resource: GroupResource,
}

impl<P: Object> ScaleRest<P> {
    pub fn new(store: Store<P, StorageBackend>, scalable: Scalable<P>) -> Self {
        let scale_resource = GroupResource::new(
            &store.qualified_resource.group,
            &format!("{}/scale", store.qualified_resource.resource),
        );
        Self {
            store,
            scalable,
            scale_resource,
        }
    }

    fn to_scale(&self, obj: &P) -> Result<Scale> {
        (self.scalable.to_scale)(obj).map_err(Error::BadRequest)
    }
}

/// `scaleUpdatedObjectInfo`: existing object → existing Scale → the request's
/// new Scale → new object (storage.go:396-480).
struct ScaleUpdatedObjectInfo<'a, P: Object> {
    name: &'a str,
    request: &'a dyn UpdatedObjectInfo<Scale>,
    rest: &'a ScaleRest<P>,
}

#[async_trait]
impl<P: Object> UpdatedObjectInfo<P> for ScaleUpdatedObjectInfo<'_, P> {
    fn preconditions(&self) -> Option<Preconditions> {
        self.request.preconditions()
    }

    async fn updated_object(&self, ctx: &RequestContext, old: Option<&P>) -> Result<P> {
        // "if zero-value, the existing object does not exist".
        let Some(old) = old else {
            return Err(not_found(&self.rest.scale_resource, self.name));
        };
        let mut obj = old.clone();
        let old_scale = self.rest.to_scale(&obj)?;
        let scale = self.request.updated_object(ctx, Some(&old_scale)).await?;

        let errs = validate_scale(&scale);
        if !errs.is_empty() {
            return Err(Error::Invalid(errs));
        }

        // "validate precondition if specified (resourceVersion matching is
        // handled by storage)".
        let uid = &scale.metadata.uid;
        if !uid.is_empty() && *uid != obj.metadata().uid {
            return Err(conflict(
                &self.rest.scale_resource,
                self.name,
                format!(
                    "Precondition failed: UID in precondition: {uid}, UID in object meta: {}",
                    obj.metadata().uid
                ),
            ));
        }

        (self.rest.scalable.set_replicas)(&mut obj, scale.spec.replicas);
        obj.metadata_mut().resource_version = scale.metadata.resource_version.clone();
        Ok(obj)
    }
}

/// `toScaleCreateValidation`: admission sees the Scale, not the parent.
struct ToScaleCreateValidation<'a, P: Object> {
    inner: &'a dyn ValidateObject<Scale>,
    rest: &'a ScaleRest<P>,
}

#[async_trait]
impl<P: Object> ValidateObject<P> for ToScaleCreateValidation<'_, P> {
    async fn validate(&self, ctx: &RequestContext, obj: &P) -> Result<()> {
        let scale = self.rest.to_scale(obj)?;
        self.inner.validate(ctx, &scale).await
    }
}

/// `toScaleUpdateValidation`.
struct ToScaleUpdateValidation<'a, P: Object> {
    inner: &'a dyn ValidateObjectUpdate<Scale>,
    rest: &'a ScaleRest<P>,
}

#[async_trait]
impl<P: Object> ValidateObjectUpdate<P> for ToScaleUpdateValidation<'_, P> {
    async fn validate(&self, ctx: &RequestContext, obj: &P, old: &P) -> Result<()> {
        let new_scale = self.rest.to_scale(obj)?;
        let old_scale = self.rest.to_scale(old)?;
        self.inner.validate(ctx, &new_scale, &old_scale).await
    }
}

#[async_trait]
impl<P: Object> RestStorage<Scale> for ScaleRest<P> {
    fn qualified_resource(&self) -> &GroupResource {
        &self.store.qualified_resource
    }

    fn namespace_scoped(&self) -> bool {
        self.store.create_strategy.namespace_scoped()
    }

    /// `ScaleREST.Get` (storage.go:314-326).
    async fn get(&self, ctx: &RequestContext, name: &str) -> Result<Scale> {
        let obj = self.store.get(ctx, name).await?;
        self.to_scale(&obj)
    }

    /// `ScaleREST.Update` (storage.go:328-352): never creates, and answers
    /// with the new Scale.
    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<Scale>,
        create_validation: Option<&dyn ValidateObject<Scale>>,
        update_validation: Option<&dyn ValidateObjectUpdate<Scale>>,
        _force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(Scale, bool)> {
        let info = ScaleUpdatedObjectInfo {
            name,
            request: obj_info,
            rest: self,
        };
        let create_validation =
            create_validation.map(|inner| ToScaleCreateValidation { inner, rest: self });
        let update_validation =
            update_validation.map(|inner| ToScaleUpdateValidation { inner, rest: self });
        let (obj, _) = self
            .store
            .update(
                ctx,
                name,
                &info,
                create_validation
                    .as_ref()
                    .map(|v| v as &dyn ValidateObject<P>),
                update_validation
                    .as_ref()
                    .map(|v| v as &dyn ValidateObjectUpdate<P>),
                false,
                options,
            )
            .await?;
        Ok((self.to_scale(&obj)?, false))
    }
}
