//! Unit tests moved from `src/secure_serving/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::Router;
    pub use rusternetes_middleware::secure_serving::options::{
        AuthenticationFlags, AuthorizationFlags, DelegatingAuthenticationOptions,
        DelegatingAuthorizationOptions, SecureServingFlags, SecureServingOptions,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::secure_serving::*;

    // kcm options.go:233-238 / scheduler options.go:119-128
    #[test]
    fn component_defaults_follow_upstream() {
        let k = ComponentSecureServing::for_controller_manager();
        assert_eq!(k.secure_serving.bind_port, 10257);
        assert_eq!(
            k.secure_serving.server_cert.pair_name,
            "kube-controller-manager"
        );
        assert_eq!(k.secure_serving.server_cert.cert_directory, "");
        assert!(k.authentication.remote_kubeconfig_file_optional);
        assert!(k.authorization.remote_kubeconfig_file_optional);
        assert!(!k.authentication.tolerate_in_cluster_lookup_failure);

        let s = ComponentSecureServing::for_scheduler();
        assert_eq!(s.secure_serving.bind_port, 10259);
        assert_eq!(s.secure_serving.server_cert.pair_name, "kube-scheduler");
        assert!(s.authentication.tolerate_in_cluster_lookup_failure);
        assert!(s.authentication.remote_kubeconfig_file_optional);
        assert!(s.authorization.remote_kubeconfig_file_optional);
    }

    #[test]
    fn defaults_validate_and_a_bad_port_does_not() {
        let mut c = ComponentSecureServing::for_controller_manager();
        assert!(c.validate().is_empty());
        c.secure_serving.bind_port = 70000;
        assert_eq!(c.validate().len(), 1);
    }

    // `--secure-port=0` serves nothing (and does not need authn/authz)
    #[tokio::test]
    async fn secure_port_zero_disables_serving() {
        let mut c = ComponentSecureServing::for_scheduler();
        c.secure_serving.bind_port = 0;
        assert!(c.serve(Router::new()).await.unwrap().is_none());
    }
}
