//! Unit tests moved from `src/commands/kuberc.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_kubectl::types::KubercCommands;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::kuberc::*;

    #[test]
    fn test_kuberc_no_subcommand() {
        let result = execute(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kuberc_set() {
        let cmd = KubercCommands::Set {
            property: Some("default.namespace".to_string()),
            value: Some("kube-system".to_string()),
        };
        let result = execute(Some(&cmd));
        assert!(result.is_ok());
    }

    #[test]
    fn test_kuberc_view() {
        let cmd = KubercCommands::View {};
        let result = execute(Some(&cmd));
        assert!(result.is_ok());
    }

    #[test]
    fn test_kuberc_set_missing_value() {
        let cmd = KubercCommands::Set {
            property: Some("default.namespace".to_string()),
            value: None,
        };
        let result = execute(Some(&cmd));
        assert!(result.is_ok());
    }
}
