//! Port of `csi.RegistrationHandler` (`pkg/volume/csi/csi_plugin.go:95-290`) and
//! of `HighestSupportedVersion` (`staging/src/k8s.io/apimachinery/pkg/util/
//! version/version.go:93-130`): the plugin-manager handler for `CSIPlugin`
//! registrations, which populates [`DriversStore`].
//!
//! NOT PORTED YET (tracked in the follow-up issue): upstream's
//! `RegisterPlugin` goes on to call the driver's `NodeGetInfo` and
//! `nodeinfomanager.InstallCSIDriver` (the `CSINode` object and the
//! `csi.volume.kubernetes.io/nodeid` node annotation), unregistering the driver
//! again if either fails (`csi_plugin.go:134-175`), and `DeRegisterPlugin`
//! calls `UninstallCSIDriver`. Here registration ends once the driver is in
//! the store, which is what the CSI volume plugin reads
//! (`newCsiDriverClient`).

use super::cache::PluginHandler;
use crate::volume_plugins::csi_drivers_store::{csi_drivers, Driver, DriversStore};
use async_trait::async_trait;
use std::cmp::Ordering;
use std::time::Duration;

/// `registerapi.CSIPlugin` (`pluginregistration/v1/constants.go:21`): the
/// `PluginInfo.type` a CSI driver registrar reports.
pub const CSI_PLUGIN: &str = "CSIPlugin";

/// A "generic" version (`version.ParseGeneric`): two or more dot-separated
/// numeric fields, optional leading `v`, trailing data ignored. Only the
/// numeric components are kept, as upstream's `String()` does for a
/// non-semantic version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version(Vec<u64>);

impl Version {
    /// `ParseGeneric` (`version.go:45-89`, `versionMatchRE`
    /// `^\s*v?([0-9]+(?:\.[0-9]+)*)(.*)*$`).
    pub fn parse_generic(s: &str) -> Result<Self, String> {
        let t = s.trim_start();
        let t = t.strip_prefix('v').unwrap_or(t);
        let mut numbers = String::new();
        let mut chars = t.chars().peekable();
        // One or more digits, then (`.` digits)* where a `.` only counts when
        // followed by a digit.
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                numbers.push(c);
                chars.next();
            } else if c == '.' && !numbers.ends_with('.') && !numbers.is_empty() {
                let mut look = chars.clone();
                look.next();
                if look.peek().is_some_and(|d| d.is_ascii_digit()) {
                    numbers.push('.');
                    chars.next();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if numbers.is_empty() {
            return Err(format!("could not parse {s:?} as version"));
        }
        let comps: Vec<&str> = numbers.split('.').collect();
        if comps.len() < 2 {
            return Err(format!("illegal version string {s:?}"));
        }
        let mut out = Vec::with_capacity(comps.len());
        for (i, c) in comps.iter().enumerate() {
            if i == 0 && c.starts_with('0') && *c != "0" {
                return Err(format!(
                    "illegal zero-prefixed version component {c:?} in {s:?}"
                ));
            }
            out.push(c.parse::<u64>().map_err(|e| {
                format!("illegal non-numeric version component {c:?} in {s:?}: {e}")
            })?);
        }
        Ok(Version(out))
    }

    pub fn major(&self) -> u64 {
        self.0[0]
    }

    /// `compareInternal`: component-wise, a missing component counts as 0.
    fn compare(&self, other: &Self) -> Ordering {
        let n = self.0.len().max(other.0.len());
        for i in 0..n {
            let a = self.0.get(i).copied().unwrap_or(0);
            let b = other.0.get(i).copied().unwrap_or(0);
            match a.cmp(&b) {
                Ordering::Equal => {}
                o => return o,
            }
        }
        Ordering::Equal
    }

    pub fn less_than(&self, other: &Self) -> bool {
        self.compare(other) == Ordering::Less
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self.0.iter().map(|c| c.to_string()).collect();
        write!(f, "{}", parts.join("."))
    }
}

/// `HighestSupportedVersion` (`version.go:93-130`): the highest 0.x/1.x version
/// among `versions` (anything >= 2 is skipped, unparseable entries are
/// skipped), which must be a 1.x version.
pub fn highest_supported_version(versions: &[String]) -> Result<Version, String> {
    if versions.is_empty() {
        return Err("empty array for supported versions".to_string());
    }
    let mut highest: Option<Version> = None;
    let mut the_err = String::new();
    for v in versions.iter().rev() {
        let cur = match Version::parse_generic(v) {
            Ok(c) => c,
            Err(e) => {
                the_err = e;
                continue;
            }
        };
        if cur.major() > 1 {
            continue;
        }
        if highest.as_ref().is_none_or(|h| h.less_than(&cur)) {
            highest = Some(cur);
        }
    }
    let Some(highest) = highest else {
        return Err(format!(
            "could not find a highest supported version from versions ({versions:?}) reported: {the_err}"
        ));
    };
    if highest.major() != 1 {
        return Err(format!(
            "highest supported version reported is {highest}, must be v1.x"
        ));
    }
    Ok(highest)
}

/// `RegistrationHandler` (`csi_plugin.go:93`).
pub struct RegistrationHandler {
    drivers: &'static DriversStore,
}

impl Default for RegistrationHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistrationHandler {
    /// Backed by the process-wide `csiDrivers` store, as upstream's
    /// `PluginHandler` is.
    pub fn new() -> Self {
        Self {
            drivers: csi_drivers(),
        }
    }

    /// Back the handler with another store (tests).
    pub fn with_store(drivers: &'static DriversStore) -> Self {
        Self { drivers }
    }

    /// `validateVersions` (`csi_plugin.go:262-297`).
    fn validate_versions(
        &self,
        caller: &str,
        plugin_name: &str,
        versions: &[String],
    ) -> Result<Version, String> {
        if versions.is_empty() {
            return Err(format!(
                "kubernetes.io/csi: {caller} for CSI driver {plugin_name:?} failed. Plugin returned an empty list for supported versions"
            ));
        }
        let new_highest = highest_supported_version(versions).map_err(|e| {
            format!(
                "kubernetes.io/csi: {caller} for CSI driver {plugin_name:?} failed. None of the versions specified {versions:?} are supported. err={e}"
            )
        })?;
        if let Some(existing) = self.drivers.get(plugin_name) {
            // The stored string came from this function, so it parses; if it
            // somehow does not, treat the existing driver as superseded.
            if let Ok(existing_v) = Version::parse_generic(&existing.highest_supported_version) {
                if !existing_v.less_than(&new_highest) {
                    return Err(format!(
                        "kubernetes.io/csi: {caller} for CSI driver {plugin_name:?} failed. Another driver with the same name is already registered with a higher supported version: {:?}",
                        existing.highest_supported_version
                    ));
                }
            }
        }
        Ok(new_highest)
    }
}

#[async_trait]
impl PluginHandler for RegistrationHandler {
    /// `ValidatePlugin` (`csi_plugin.go:101-111`).
    fn validate_plugin(
        &self,
        plugin_name: &str,
        endpoint: &str,
        versions: &[String],
    ) -> Result<(), String> {
        self.validate_versions("ValidatePlugin", plugin_name, versions)
            .map(|_| ())
            .map_err(|e| {
                format!(
                    "validation failed for CSI Driver {plugin_name} at endpoint {endpoint}: {e}"
                )
            })
    }

    /// `RegisterPlugin` (`csi_plugin.go:114-177`), up to and including
    /// `csiDrivers.Set`; see the module doc for what is not ported.
    async fn register_plugin(
        &self,
        plugin_name: &str,
        endpoint: &str,
        versions: &[String],
        _plugin_client_timeout: Option<Duration>,
    ) -> Result<(), String> {
        tracing::info!(
            "kubernetes.io/csi: Register new plugin with name: {plugin_name} at endpoint: {endpoint}"
        );
        let highest = self.validate_versions("RegisterPlugin", plugin_name, versions)?;
        // Other CSI components find the driver's socket by name in this store.
        self.drivers.set(
            plugin_name,
            Driver {
                endpoint: endpoint.to_string(),
                highest_supported_version: highest.to_string(),
            },
        );
        Ok(())
    }

    /// `DeRegisterPlugin` (`csi_plugin.go:242-252`): `unregisterDriver` ->
    /// `csiDrivers.Delete`.
    fn deregister_plugin(&self, plugin_name: &str, endpoint: &str) {
        tracing::info!(
            "kubernetes.io/csi: registrationHandler.DeRegisterPlugin request for plugin {plugin_name}, endpoint {endpoint}"
        );
        self.drivers.delete(plugin_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn store() -> &'static DriversStore {
        Box::leak(Box::new(DriversStore::new()))
    }

    /// `TestHighestSupportedVersion` (`version_test.go:350-440`), every row.
    #[test]
    fn highest_supported_version_table() {
        let cases: &[(&[&str], Option<&str>)] = &[
            (&["v1.0.0"], Some("1.0.0")),
            (&["0.3.0"], None),
            (&["0.2.0"], None),
            (&["1.0.0"], Some("1.0.0")),
            (&["v0.3.0"], None),
            (&["v0.2.0"], None),
            (&["0.2.0", "v0.3.0"], None),
            (&["0.2.0", "v1.0.0"], Some("1.0.0")),
            (&["0.2.0", "v1.2.3"], Some("1.2.3")),
            (&["v1.2.3", "v0.3.0"], Some("1.2.3")),
            (&["v1.2.3", "v0.3.0", "2.0.1"], Some("1.2.3")),
            (&["v1.2.3", "4.9.12", "v0.3.0", "2.0.1"], Some("1.2.3")),
            (&["4.9.12", "2.0.1"], None),
            (&["v1.2.3", "boo", "v0.3.0", "2.0.1"], Some("1.2.3")),
            (&[], None),
            (&["var", "boo", "foo"], None),
        ];
        for (versions, want) in cases {
            let got = highest_supported_version(&s(versions));
            match want {
                Some(w) => assert_eq!(got.unwrap().to_string(), *w, "{versions:?}"),
                None => assert!(got.is_err(), "{versions:?} should fail"),
            }
        }
    }

    /// `TestValidatePlugin` (`csi_plugin_test.go:1268`): same table through the
    /// handler (fresh store, so no existing driver).
    #[test]
    fn validate_plugin_table() {
        let h = RegistrationHandler::with_store(store());
        let ok = |v: &[&str]| h.validate_plugin("test.plugin", "/csi.sock", &s(v)).is_ok();
        assert!(ok(&["v1.0.0"]));
        assert!(!ok(&["0.3.0"]));
        assert!(!ok(&["0.2.0", "v0.3.0"]));
        assert!(ok(&["0.2.0", "v1.0.0"]));
        assert!(ok(&["v1.2.3", "boo", "v0.3.0", "2.0.1"]));
        assert!(!ok(&["4.9.12", "2.0.1"]));
        assert!(!ok(&[]));
        assert!(!ok(&["var", "boo", "foo"]));
    }

    /// `TestValidatePluginExistingDriver` (`csi_plugin_test.go:1369`).
    #[test]
    fn validate_plugin_existing_driver() {
        let seed = |name: &str, versions: &[&str]| {
            let st = store();
            st.set(
                name,
                Driver {
                    endpoint: "/old.sock".into(),
                    highest_supported_version: highest_supported_version(&s(versions))
                        .unwrap()
                        .to_string(),
                },
            );
            RegistrationHandler::with_store(st)
        };
        // Different name: fine.
        let h = seed("test.plugin", &["v1.0.0"]);
        assert!(h
            .validate_plugin("test.plugin2", "/c.sock", &s(&["v1.0.0"]))
            .is_ok());
        // Same name, same version: refused.
        let h = seed("test.plugin", &["v1.0.0"]);
        assert!(h
            .validate_plugin("test.plugin", "/c.sock", &s(&["v1.0.0"]))
            .is_err());
        // Same name, higher version: allowed.
        let h = seed("test.plugin", &["v0.3.0", "v0.2.0", "v1.0.0"]);
        assert!(h
            .validate_plugin("test.plugin", "/c.sock", &s(&["v1.0.1"]))
            .is_ok());
    }

    /// Register puts the driver in the store the CSI volume plugin reads;
    /// DeRegister removes it.
    #[tokio::test]
    async fn register_populates_store_and_deregister_clears() {
        let st = store();
        let h = RegistrationHandler::with_store(st);
        h.register_plugin("csi.example.com", "/p/csi.sock", &s(&["v1.2.3"]), None)
            .await
            .unwrap();
        let d = st.get("csi.example.com").expect("driver registered");
        assert_eq!(d.endpoint, "/p/csi.sock");
        assert_eq!(d.highest_supported_version, "1.2.3");
        h.deregister_plugin("csi.example.com", "/p/csi.sock");
        assert!(st.get("csi.example.com").is_none());
    }
}
