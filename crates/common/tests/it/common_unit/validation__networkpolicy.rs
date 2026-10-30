//! Unit tests moved from `src/validation/networkpolicy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::networking::{
        IPBlock, NetworkPolicy, NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
    };
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::ingress::parse_ip_sloppy;
    pub use rusternetes_common::validation::metav1::{
        validate_label_selector, LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use std::net::IpAddr;
}

mod ip_block_tests {
    use super::__parent::*;
    use rusternetes_common::validation::networkpolicy::*;

    fn ipb_errs(json: serde_json::Value) -> Vec<String> {
        let ipb: IPBlock = serde_json::from_value(json).unwrap();
        validate_ip_block(
            &ipb,
            &Path::new("ipBlock"),
            &NetworkPolicyValidationOptions::default(),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    #[test]
    fn valid_strict_subset_passes() {
        assert!(ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["10.1.0.0/16", "10.2.3.0/24"]
        }))
        .is_empty());
    }

    #[test]
    fn except_not_contained_rejected() {
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["192.168.0.0/16"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn except_equal_or_shorter_prefix_rejected() {
        // same prefix as cidr -> not strict (prefix must be longer)
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["10.0.0.0/8"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn ipv6_strict_subset_passes() {
        assert!(ipb_errs(serde_json::json!({
            "cidr": "2001:db8::/32", "except": ["2001:db8:1::/48"]
        }))
        .is_empty());
    }

    #[test]
    fn mixed_family_except_rejected() {
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["2001:db8::/64"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn invalid_cidr_still_rejected() {
        let e = ipb_errs(serde_json::json!({"cidr": "10.0.0.0/8", "except": ["notacidr"]}));
        assert!(e.iter().any(|m| m.contains("valid CIDR")), "{e:?}");
    }
}
