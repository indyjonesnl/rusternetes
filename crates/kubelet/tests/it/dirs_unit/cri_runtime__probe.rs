//! Unit tests moved from `src/cri_runtime/probe.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::Container;
    pub use rusternetes_common::resources::policy::IntOrString;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::ContainerPort;
    use rusternetes_kubelet::cri_runtime::probe::*;

    fn container_with_named_port() -> Container {
        Container {
            name: "web".to_string(),
            image: "nginx".to_string(),
            ports: Some(vec![ContainerPort {
                container_port: 8080,
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                host_port: None,
                host_ip: None,
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn int_port_passes_through() {
        let c = container_with_named_port();
        assert_eq!(resolve_port(&c, &IntOrString::Int(443)), Some(443));
    }

    #[test]
    fn named_port_resolves_from_spec() {
        let c = container_with_named_port();
        assert_eq!(
            resolve_port(&c, &IntOrString::String("http".to_string())),
            Some(8080)
        );
    }

    #[test]
    fn unknown_named_port_is_none() {
        let c = container_with_named_port();
        assert_eq!(
            resolve_port(&c, &IntOrString::String("grpc".to_string())),
            None
        );
    }

    #[test]
    fn redirect_to_same_hostname_is_local_even_on_new_port() {
        let original = reqwest::Url::parse("http://127.0.0.1:8080/redirect").unwrap();
        let next = reqwest::Url::parse("http://127.0.0.1:9090/success").unwrap();

        assert!(!redirect_is_non_local(&next, &[original]));
    }

    #[test]
    fn redirect_to_different_hostname_is_non_local() {
        let original = reqwest::Url::parse("http://10.244.0.12:8080/redirect").unwrap();
        let next = reqwest::Url::parse("http://0.0.0.0/fail").unwrap();

        assert!(redirect_is_non_local(&next, &[original]));
    }
}
