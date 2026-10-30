//! Unit tests moved from `src/admission/config.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::path::{Path, PathBuf};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::config::*;
    use rusternetes_api_server::admission::plugins::EnabledForTest;

    const LEGACY: &str = "podNodeSelectorPluginConfig:\n  clusterDefaultNodeSelector: a=b\n";

    // config.go:92-125: a file with the unversioned `podNodeSelectorPluginConfig`
    // key is the configuration of the enabled legacy plugins, whole.
    #[test]
    fn legacy_unversioned_config_goes_to_enabled_legacy_plugins() {
        let _g = EnabledForTest::new(&["PodNodeSelector"]);
        let f = Path::new("/etc/admission.yaml");
        assert_eq!(
            plugin_configuration_for(LEGACY, f, "PodNodeSelector").unwrap(),
            Some(LEGACY.to_string())
        );
        // ImagePolicyWebhook is legacy too but not enabled; other plugins never.
        assert_eq!(
            plugin_configuration_for(LEGACY, f, "ImagePolicyWebhook").unwrap(),
            None
        );
        assert_eq!(
            plugin_configuration_for(LEGACY, f, "PodSecurity").unwrap(),
            None
        );
    }

    #[test]
    fn versioned_config_is_not_legacy() {
        let _g = EnabledForTest::new(&["PodNodeSelector"]);
        let doc = "apiVersion: apiserver.config.k8s.io/v1\nkind: AdmissionConfiguration\nplugins:\n- name: PodNodeSelector\n  configuration:\n    podNodeSelectorPluginConfig:\n      x: y=z\n";
        let text = plugin_configuration_for(doc, Path::new("/f"), "PodNodeSelector")
            .unwrap()
            .unwrap();
        assert!(text.contains("podNodeSelectorPluginConfig"));
        assert!(!text.contains("AdmissionConfiguration"));
    }
}
