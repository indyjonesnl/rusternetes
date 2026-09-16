use crate::volume_plugins::plugin::{Spec, VolumePlugin};

/// Port of `ErrNoPluginMatched` (`pkg/volume/plugins.go:71-72`) plus the
/// multiple-match error (`plugins.go:662`). A typed error, not a string,
/// because `create_volume` must distinguish "nothing matched" (apply the
/// unsupported-kind fallback) from "several matched" (a malformed volume that
/// must fail).
#[derive(Debug, thiserror::Error)]
pub enum PluginLookupError {
    #[error("no volume plugin matched")]
    NoPluginMatched,
    #[error("multiple volume plugins matched: {0}")]
    MultipleMatched(String),
}

/// Port of `VolumePluginMgr` (`pkg/volume/plugins.go:425`).
///
/// Upstream also carries probed (dynamically discovered) plugins and a mutex.
/// Our set is fixed at construction and the registry is immutable afterwards,
/// so neither is ported.
pub struct VolumePluginMgr {
    plugins: Vec<Box<dyn VolumePlugin>>,
}

impl VolumePluginMgr {
    pub fn new(plugins: Vec<Box<dyn VolumePlugin>>) -> Self {
        Self { plugins }
    }

    /// Port of `FindPluginBySpec` (`pkg/volume/plugins.go:634-665`).
    ///
    /// Collects EVERY match rather than returning the first. Two plugins
    /// matching means the volume declares two sources, which upstream rejects
    /// outright — first-match-wins would silently provision one of them.
    pub fn find_plugin_by_spec(
        &self,
        spec: &Spec<'_>,
    ) -> Result<&dyn VolumePlugin, PluginLookupError> {
        let matched: Vec<&dyn VolumePlugin> = self
            .plugins
            .iter()
            .filter(|p| p.can_support(spec))
            .map(|p| p.as_ref())
            .collect();

        match matched.len() {
            0 => Err(PluginLookupError::NoPluginMatched),
            1 => Ok(matched[0]),
            _ => Err(PluginLookupError::MultipleMatched(
                matched
                    .iter()
                    .map(|p| p.name())
                    .collect::<Vec<_>>()
                    .join(","),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::plugin::{Mounter, Spec, VolumePlugin};
    use anyhow::Result;
    use async_trait::async_trait;
    use rusternetes_common::resources::{Pod, Volume};
    use serde_json::json;

    /// A plugin that matches when the volume's name starts with its prefix.
    /// Stands in for a real plugin so the registry can be tested without one.
    struct PrefixPlugin {
        name: &'static str,
        prefix: &'static str,
    }

    #[async_trait]
    impl VolumePlugin for PrefixPlugin {
        fn name(&self) -> &'static str {
            self.name
        }
        fn can_support(&self, spec: &Spec<'_>) -> bool {
            spec.volume.name.starts_with(self.prefix)
        }
        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            unimplemented!("registry tests never mount")
        }
    }

    fn volume(name: &str) -> Volume {
        serde_json::from_value(json!({ "name": name })).unwrap()
    }

    fn mgr() -> VolumePluginMgr {
        VolumePluginMgr::new(vec![
            Box::new(PrefixPlugin {
                name: "kubernetes.io/a",
                prefix: "a",
            }),
            Box::new(PrefixPlugin {
                name: "kubernetes.io/b",
                prefix: "b",
            }),
            Box::new(PrefixPlugin {
                name: "kubernetes.io/ab",
                prefix: "ab",
            }),
        ])
    }

    #[test]
    fn one_match_returns_that_plugin() {
        let v = volume("b-volume");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert_eq!(
            mgr().find_plugin_by_spec(&spec).unwrap().name(),
            "kubernetes.io/b"
        );
    }

    #[test]
    fn no_match_is_no_plugin_matched() {
        let v = volume("zzz");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(matches!(
            mgr().find_plugin_by_spec(&spec),
            Err(PluginLookupError::NoPluginMatched)
        ));
    }

    /// Upstream errors rather than letting first-match-wins pick one
    /// (`pkg/volume/plugins.go:659-662`). `pod_dirs::plugin_for_volume`
    /// silently picked; the registry must not.
    #[test]
    fn two_matches_is_an_error_naming_both() {
        let v = volume("ab-volume");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        let err = mgr().find_plugin_by_spec(&spec).err().unwrap();
        let msg = err.to_string();
        assert!(msg.contains("multiple volume plugins matched"), "{msg}");
        assert!(msg.contains("kubernetes.io/a"), "{msg}");
        assert!(msg.contains("kubernetes.io/ab"), "{msg}");
    }
}
