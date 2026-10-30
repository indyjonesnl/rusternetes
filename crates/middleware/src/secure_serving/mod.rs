//! Secure serving for the controller-manager and scheduler: TLS, delegated
//! authentication and authorization, and the handler chain that guards
//! `/metrics`, `/statusz` and `/flagz` (#3437).
//!
//! Port of `k8s.io/apiserver/pkg/server/options` (serving, authentication,
//! authorization) plus `server.BuildHandlerChain` as called from
//! `cmd/kube-controller-manager/app/controllermanager.go:236` and
//! `cmd/kube-scheduler/app/server.go:250-254`.

pub mod authn;
pub mod authz;
pub mod filters;
pub mod options;
pub mod serve;
pub mod webhook;

use axum::Router;
use options::{
    AuthenticationFlags, AuthorizationFlags, DelegatingAuthenticationOptions,
    DelegatingAuthorizationOptions, SecureServingFlags, SecureServingOptions,
};

/// `KubeControllerManagerPort` (`pkg/cluster/ports/ports.go`).
pub const KUBE_CONTROLLER_MANAGER_PORT: i32 = 10257;
/// `DefaultKubeSchedulerPort` (`pkg/scheduler/apis/config/types.go`).
pub const DEFAULT_KUBE_SCHEDULER_PORT: i32 = 10259;

/// The three option groups a component embeds: `SecureServing`,
/// `Authentication`, `Authorization` (kube-controller-manager
/// `KubeControllerManagerOptions`, kube-scheduler `Options`).
pub struct ComponentSecureServing {
    pub secure_serving: SecureServingOptions,
    pub authentication: DelegatingAuthenticationOptions,
    pub authorization: DelegatingAuthorizationOptions,
}

impl ComponentSecureServing {
    /// `NewKubeControllerManagerOptions` (cmd/kube-controller-manager/app/options/options.go:224-238).
    pub fn for_controller_manager() -> Self {
        let mut c = Self::base("kube-controller-manager", KUBE_CONTROLLER_MANAGER_PORT);
        c.authentication.remote_kubeconfig_file_optional = true;
        c.authorization.remote_kubeconfig_file_optional = true;
        c
    }

    /// `NewOptionsWithComponentGlobalsRegistry` (cmd/kube-scheduler/app/options/options.go:119-128).
    pub fn for_scheduler() -> Self {
        let mut c = Self::base("kube-scheduler", DEFAULT_KUBE_SCHEDULER_PORT);
        c.authentication.tolerate_in_cluster_lookup_failure = true;
        c.authentication.remote_kubeconfig_file_optional = true;
        c.authorization.remote_kubeconfig_file_optional = true;
        c
    }

    fn base(pair_name: &str, port: i32) -> Self {
        let mut secure_serving = SecureServingOptions::new();
        // Set the PairName but leave certificate directory blank to generate in-memory by default
        secure_serving.server_cert.cert_directory = String::new();
        secure_serving.server_cert.pair_name = pair_name.to_string();
        secure_serving.bind_port = port;
        Self {
            secure_serving,
            authentication: DelegatingAuthenticationOptions::new(),
            authorization: DelegatingAuthorizationOptions::new(),
        }
    }

    /// What `fs.Parse` does to the three option groups.
    pub fn apply_flags(
        &mut self,
        serving: &SecureServingFlags,
        authentication: &AuthenticationFlags,
        authorization: &AuthorizationFlags,
    ) {
        serving.apply_to_options(&mut self.secure_serving);
        authentication.apply_to_options(&mut self.authentication);
        authorization.apply_to_options(&mut self.authorization);
    }

    /// `Validate` (kcm options.go:474-476, scheduler options.go:290-292).
    pub fn validate(&self) -> Vec<String> {
        let mut errs = self.secure_serving.validate();
        errs.extend(self.authentication.validate());
        errs.extend(self.authorization.validate());
        errs
    }

    /// `Config` + `ApplyTo` + `Serve`: default a self-signed certificate
    /// (`MaybeDefaultWithSelfSignedCerts("localhost", nil, [127.0.0.1])`),
    /// bind, apply authentication and authorization (only when secure serving
    /// is on) and serve `handler` behind the `BuildHandlerChain` filters.
    /// `Ok(None)` when `--secure-port=0` turned serving off.
    pub async fn serve(mut self, handler: Router) -> Result<Option<serve::ServerHandle>, String> {
        self.secure_serving
            .maybe_default_with_self_signed_certs(
                "localhost",
                &[],
                &["127.0.0.1".parse().expect("literal address")],
            )
            .map_err(|e| format!("error creating self-signed certificates: {e}"))?;
        let Some(mut info) = self.secure_serving.apply_to()? else {
            return Ok(None);
        };
        let authentication = self.authentication.apply_to(Some(&mut info)).await?;
        let authorization = self.authorization.apply_to().await?;
        let handler = filters::build_handler_chain(
            handler,
            Some(authorization.authorizer),
            authentication.authenticator,
        );
        if let Ok(addr) = info.local_addr() {
            tracing::info!("Serving securely on {addr}");
        }
        serve::serve(handler, info).map(Some)
    }
}
