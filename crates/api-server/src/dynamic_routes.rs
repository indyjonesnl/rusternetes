//! Dynamic API route registration for Custom Resource Definitions
//!
//! This module enables hot-reload of API routes when CRDs are created or deleted,
//! allowing the API server to automatically serve new custom resource types without restart.

#![allow(dead_code)]

use crate::{handlers::custom_resource, state::ApiServerState};
use axum::{routing::get, Router};
use rusternetes_common::resources::{CustomResourceDefinition, ResourceScope};
use std::sync::Arc;
use tracing::{debug, info};

/// Manages dynamic route registration for custom resources
pub struct DynamicRouteManager {
    /// API server state
    state: Arc<ApiServerState>,
}

impl DynamicRouteManager {
    /// Create a new dynamic route manager
    pub fn new(state: Arc<ApiServerState>) -> Self {
        Self { state }
    }

    /// Build routes for a CRD
    /// This creates the router configuration that should be registered when a CRD is created
    pub fn build_crd_routes(&self, crd: &CustomResourceDefinition) -> Router<Arc<ApiServerState>> {
        info!("Building dynamic routes for CRD: {}", crd.metadata.name);

        let group = &crd.spec.group;
        let plural = &crd.spec.names.plural;
        let scope = &crd.spec.scope;

        // Get all served versions
        let served_versions: Vec<_> = crd
            .spec
            .versions
            .iter()
            .filter(|v| v.served)
            .map(|v| v.name.as_str())
            .collect();

        debug!(
            "Building routes for {} served version(s): {:?}",
            served_versions.len(),
            served_versions
        );

        // Create new routes for each served version
        let mut new_routes = Router::new();

        for version in served_versions {
            let routes = match scope {
                // `Unspecified` is the decoded Go zero value; a stored CRD never
                // carries it, because `validate_crd` rejects an unset
                // `spec.scope` the way upstream's `validateEnumStrings(...,
                // required=true)` does. Routing it as namespaced keeps this
                // unreachable arm from silently exposing a cluster-wide path.
                ResourceScope::Namespaced
                | ResourceScope::Unspecified
                | ResourceScope::Unknown(_) => {
                    // Namespaced resources have both namespaced and cluster-wide list endpoints
                    let ns_path = format!(
                        "/apis/{}/{}/namespaces/:namespace/{}",
                        group, version, plural
                    );
                    let ns_name_path = format!(
                        "/apis/{}/{}/namespaces/:namespace/{}/:name",
                        group, version, plural
                    );
                    let cluster_path = format!("/apis/{}/{}/{}", group, version, plural);

                    Router::new()
                        .route(
                            &ns_path,
                            get(custom_resource::list_custom_resources)
                                .post(custom_resource::create_custom_resource),
                        )
                        .route(
                            &ns_name_path,
                            get(custom_resource::get_custom_resource)
                                .put(custom_resource::update_custom_resource)
                                .delete(custom_resource::delete_custom_resource),
                        )
                        .route(&cluster_path, get(custom_resource::list_custom_resources))
                }
                ResourceScope::Cluster => {
                    // Cluster-scoped resources have only cluster-wide endpoints
                    let cluster_path = format!("/apis/{}/{}/{}", group, version, plural);
                    let cluster_name_path = format!("/apis/{}/{}/{}/:name", group, version, plural);

                    Router::new()
                        .route(
                            &cluster_path,
                            get(custom_resource::list_custom_resources)
                                .post(custom_resource::create_custom_resource),
                        )
                        .route(
                            &cluster_name_path,
                            get(custom_resource::get_custom_resource)
                                .put(custom_resource::update_custom_resource)
                                .delete(custom_resource::delete_custom_resource),
                        )
                }
            };

            new_routes = new_routes.merge(routes);

            // Register subresource routes if defined
            if let Some(version_spec) = crd.spec.versions.iter().find(|v| v.name == version) {
                if let Some(ref subresources) = version_spec.subresources {
                    // Status subresource
                    if subresources.status.is_some() {
                        let status_routes =
                            self.create_status_routes(group, version, plural, scope);
                        new_routes = new_routes.merge(status_routes);
                    }

                    // Scale subresource
                    if subresources.scale.is_some() {
                        let scale_routes = self.create_scale_routes(group, version, plural, scope);
                        new_routes = new_routes.merge(scale_routes);
                    }
                }
            }
        }

        info!(
            "Successfully built dynamic routes for CRD: {}",
            crd.metadata.name
        );

        new_routes
    }

    /// Log when a CRD is registered (routes are built on-demand)
    pub fn register_crd(&self, crd: &CustomResourceDefinition) {
        info!(
            "CRD registered: {} (routes available on-demand)",
            crd.metadata.name
        );
    }

    /// Log when a CRD is unregistered
    pub fn unregister_crd(&self, crd: &CustomResourceDefinition) {
        info!(
            "CRD unregistered: {} (routes will return 404)",
            crd.metadata.name
        );
    }

    /// Create status subresource routes
    fn create_status_routes(
        &self,
        group: &str,
        version: &str,
        plural: &str,
        scope: &ResourceScope,
    ) -> Router<Arc<ApiServerState>> {
        match scope {
            // See the note in `build_routes_for_crd`: unreachable for a stored
            // CRD, routed as namespaced rather than cluster-wide.
            ResourceScope::Namespaced | ResourceScope::Unspecified | ResourceScope::Unknown(_) => {
                let status_name_path = format!(
                    "/apis/{}/{}/namespaces/:namespace/{}/:name/status",
                    group, version, plural
                );

                Router::new().route(
                    &status_name_path,
                    get(custom_resource::get_custom_resource_status)
                        .put(custom_resource::update_custom_resource_status),
                )
            }
            ResourceScope::Cluster => {
                let status_name_path =
                    format!("/apis/{}/{}/{}/:name/status", group, version, plural);

                Router::new().route(
                    &status_name_path,
                    get(custom_resource::get_custom_resource_status)
                        .put(custom_resource::update_custom_resource_status),
                )
            }
        }
    }

    /// Create scale subresource routes
    fn create_scale_routes(
        &self,
        group: &str,
        version: &str,
        plural: &str,
        scope: &ResourceScope,
    ) -> Router<Arc<ApiServerState>> {
        match scope {
            // See the note in `build_routes_for_crd`: unreachable for a stored
            // CRD, routed as namespaced rather than cluster-wide.
            ResourceScope::Namespaced | ResourceScope::Unspecified | ResourceScope::Unknown(_) => {
                let scale_path = format!(
                    "/apis/{}/{}/namespaces/:namespace/{}/:name/scale",
                    group, version, plural
                );

                Router::new().route(
                    &scale_path,
                    get(custom_resource::get_custom_resource_scale)
                        .put(custom_resource::update_custom_resource_scale),
                )
            }
            ResourceScope::Cluster => {
                let scale_path = format!("/apis/{}/{}/{}/:name/scale", group, version, plural);

                Router::new().route(
                    &scale_path,
                    get(custom_resource::get_custom_resource_scale)
                        .put(custom_resource::update_custom_resource_scale),
                )
            }
        }
    }
}
