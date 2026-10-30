//! Unit tests moved from `src/commands/taint.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::{json, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::taint::*;

    #[test]
    fn test_parse_taint_with_value_and_effect() {
        let spec = parse_taint("dedicated=special-user:NoSchedule").unwrap();
        assert_eq!(spec.key, "dedicated");
        assert_eq!(spec.value, Some("special-user".to_string()));
        assert_eq!(spec.effect, Some("NoSchedule".to_string()));
        assert!(!spec.remove);
    }

    #[test]
    fn test_parse_taint_no_value() {
        let spec = parse_taint("bar:NoSchedule").unwrap();
        assert_eq!(spec.key, "bar");
        assert_eq!(spec.value, None);
        assert_eq!(spec.effect, Some("NoSchedule".to_string()));
        assert!(!spec.remove);
    }

    #[test]
    fn test_parse_taint_remove_with_effect() {
        let spec = parse_taint("dedicated:NoSchedule-").unwrap();
        assert_eq!(spec.key, "dedicated");
        assert_eq!(spec.value, None);
        assert_eq!(spec.effect, Some("NoSchedule".to_string()));
        assert!(spec.remove);
    }

    #[test]
    fn test_parse_taint_remove_key_only() {
        let spec = parse_taint("dedicated-").unwrap();
        assert_eq!(spec.key, "dedicated");
        assert_eq!(spec.value, None);
        assert_eq!(spec.effect, None);
        assert!(spec.remove);
    }

    #[test]
    fn test_parse_taint_invalid_effect() {
        assert!(parse_taint("key=value:BadEffect").is_err());
    }

    #[test]
    fn test_parse_taint_remove_with_value_and_effect() {
        let spec = parse_taint("key=value:NoExecute-").unwrap();
        assert_eq!(spec.key, "key");
        assert_eq!(spec.value, Some("value".to_string()));
        assert_eq!(spec.effect, Some("NoExecute".to_string()));
        assert!(spec.remove);
    }

    #[test]
    fn test_parse_taint_prefer_no_schedule() {
        let spec = parse_taint("key=foo:PreferNoSchedule").unwrap();
        assert_eq!(spec.effect, Some("PreferNoSchedule".to_string()));
    }

    #[test]
    fn test_parse_taint_no_effect_no_dash_fails() {
        // "key=value" without effect and without dash should fail
        let result = parse_taint("key=value");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_taint_remove_with_value_no_effect() {
        let spec = parse_taint("key=value-").unwrap();
        assert_eq!(spec.key, "key");
        assert_eq!(spec.value, Some("value".to_string()));
        assert_eq!(spec.effect, None);
        assert!(spec.remove);
    }

    #[test]
    fn test_taint_spec_equality() {
        let a = TaintSpec {
            key: "node-role".to_string(),
            value: Some("master".to_string()),
            effect: Some("NoSchedule".to_string()),
            remove: false,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
