//! Port of `csi.RegistrationHandler` (`pkg/volume/csi/csi_plugin.go:95-290`) and
//! of `HighestSupportedVersion` (`staging/src/k8s.io/apimachinery/pkg/util/
//! version/version.go:93-130`): the plugin-manager handler for `CSIPlugin`
//! registrations, which populates [`DriversStore`].
//!
//! `RegisterPlugin` also runs the driver's `NodeGetInfo` and hands the result to
//! `nodeinfomanager.InstallCSIDriver` (the `CSINode` object and the
//! `csi.volume.kubernetes.io/nodeid` node annotation), unregistering the driver
//! again if either fails (`csi_plugin.go:134-175`); `DeRegisterPlugin` calls
//! `UninstallCSIDriver` (`unregisterDriver`, `csi_plugin.go:962-970`).
//!
//! NOT PORTED (tracked in follow-up issues): `csiNodeUpdaterVar.syncDriverUpdater`
//! (the periodic `NodeGetInfo` refresh driven by
//! `CSIDriver.NodeAllocatableUpdatePeriodSeconds`).

use super::cache::PluginHandler;
use crate::volume_plugins::csi_client::{CsiDriverClient, CSI_TIMEOUT};
use crate::volume_plugins::csi_drivers_store::{csi_drivers, Driver, DriversStore};
use crate::volume_plugins::nodeinfomanager::NodeInfoInstaller;
use async_trait::async_trait;
use std::cmp::Ordering;
use std::sync::Arc;
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
    /// Upstream's package-level `nim` (`csi_plugin.go:73`).
    nim: Arc<dyn NodeInfoInstaller>,
}

impl RegistrationHandler {
    /// Backed by the process-wide `csiDrivers` store, as upstream's
    /// `PluginHandler` is.
    pub fn new(nim: Arc<dyn NodeInfoInstaller>) -> Self {
        Self {
            drivers: csi_drivers(),
            nim,
        }
    }

    /// Back the handler with another store (tests).
    pub fn with_store(drivers: &'static DriversStore, nim: Arc<dyn NodeInfoInstaller>) -> Self {
        Self { drivers, nim }
    }

    /// `unregisterDriver` (`csi_plugin.go:962-970`): delete from the store
    /// first, then uninstall the node info.
    async fn unregister_driver(&self, driver_name: &str) -> Result<(), String> {
        self.drivers.delete(driver_name);
        self.nim
            .uninstall_csi_driver(driver_name)
            .await
            .map_err(|e| format!("kubernetes.io/csi: Error uninstalling CSI driver: {e}"))
    }

    /// The shared failure arm of `RegisterPlugin` (`csi_plugin.go:150-164`):
    /// unregister the driver, log a failure to do so, return the original error.
    async fn fail_registration(&self, plugin_name: &str, err: String) -> Result<(), String> {
        if let Err(unreg) = self.unregister_driver(plugin_name).await {
            tracing::error!(
                "kubernetes.io/csi: registrationHandler.RegisterPlugin failed to unregister plugin due to previous error: {unreg}"
            );
        }
        Err(err)
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

    /// `RegisterPlugin` (`csi_plugin.go:114-177`).
    async fn register_plugin(
        &self,
        plugin_name: &str,
        endpoint: &str,
        versions: &[String],
        plugin_client_timeout: Option<Duration>,
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

        // Get node info from the driver (`newCsiDriverClient` resolves the
        // endpoint from the store just written).
        let driver = self.drivers.get(plugin_name).ok_or_else(|| {
            format!("driver name {plugin_name} not found in the list of registered CSI drivers")
        })?;
        let csi = CsiDriverClient::with_endpoint(plugin_name, driver.endpoint);
        let timeout = plugin_client_timeout.unwrap_or(CSI_TIMEOUT);
        let info = match tokio::time::timeout(timeout, csi.node_get_info()).await {
            Ok(Ok(info)) => info,
            Ok(Err(e)) => return self.fail_registration(plugin_name, e.to_string()).await,
            Err(_) => {
                return self
                    .fail_registration(plugin_name, "context deadline exceeded".to_string())
                    .await
            }
        };

        if let Err(e) = self
            .nim
            .install_csi_driver(
                plugin_name,
                &info.node_id,
                info.max_volumes_per_node,
                &info.accessible_topology,
            )
            .await
        {
            return self.fail_registration(plugin_name, e).await;
        }
        Ok(())
    }

    /// `DeRegisterPlugin` (`csi_plugin.go:270-279`): `unregisterDriver`; its
    /// error is logged, not returned.
    async fn deregister_plugin(&self, plugin_name: &str, endpoint: &str) {
        tracing::info!(
            "kubernetes.io/csi: registrationHandler.DeRegisterPlugin request for plugin {plugin_name}, endpoint {endpoint}"
        );
        if let Err(e) = self.unregister_driver(plugin_name).await {
            tracing::error!("kubernetes.io/csi: registrationHandler.DeRegisterPlugin failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::csi_client::fake as csi_fake;
    use crate::volume_plugins::csi_client::proto::{NodeGetInfoResponse, Topology};
    use csi_fake::FakeDriver;
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Install = (String, String, i64, HashMap<String, String>);

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn store() -> &'static DriversStore {
        Box::leak(Box::new(DriversStore::new()))
    }

    /// Records what the handler asks the NodeInfoManager to do.
    #[derive(Default)]
    struct RecordingNim {
        installs: Mutex<Vec<Install>>,
        uninstalls: Mutex<Vec<String>>,
        fail_install: bool,
        fail_uninstall: bool,
    }

    #[async_trait]
    impl NodeInfoInstaller for RecordingNim {
        async fn install_csi_driver(
            &self,
            driver_name: &str,
            driver_node_id: &str,
            max_attach_limit: i64,
            topology: &HashMap<String, String>,
        ) -> Result<(), String> {
            self.installs.lock().unwrap().push((
                driver_name.into(),
                driver_node_id.into(),
                max_attach_limit,
                topology.clone(),
            ));
            if self.fail_install {
                return Err("install failed".into());
            }
            Ok(())
        }
        async fn uninstall_csi_driver(&self, driver_name: &str) -> Result<(), String> {
            self.uninstalls.lock().unwrap().push(driver_name.into());
            if self.fail_uninstall {
                return Err("uninstall failed".into());
            }
            Ok(())
        }
    }

    /// A fake CSI driver answering `NodeGetInfo` on a unix socket; returns the
    /// socket path (the temp dir is leaked for the test's lifetime).
    fn driver_socket(info: Option<Result<NodeGetInfoResponse, tonic::Code>>) -> String {
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let sock = dir.path().join("csi.sock");
        let d = FakeDriver::default();
        *d.node_info.lock().unwrap() = info;
        std::mem::forget(csi_fake::serve(d, &sock));
        sock.to_string_lossy().into_owned()
    }

    fn node_info(id: &str) -> NodeGetInfoResponse {
        NodeGetInfoResponse {
            node_id: id.into(),
            max_volumes_per_node: 7,
            accessible_topology: Some(Topology {
                segments: HashMap::from([("topology.example.com/zone".into(), "z1".into())]),
            }),
        }
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
        let h = RegistrationHandler::with_store(store(), Arc::new(RecordingNim::default()));
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
            RegistrationHandler::with_store(st, Arc::new(RecordingNim::default()))
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

    /// `RegisterPlugin` (`csi_plugin.go:134-177`): after `csiDrivers.Set` it
    /// calls `NodeGetInfo` and hands the node id, max volumes and topology
    /// segments to `InstallCSIDriver`; DeRegister clears the store and calls
    /// `UninstallCSIDriver` (`unregisterDriver`, `csi_plugin.go:962-970`).
    #[tokio::test]
    async fn register_installs_node_info_and_deregister_uninstalls() {
        let st = store();
        let nim = Arc::new(RecordingNim::default());
        let h = RegistrationHandler::with_store(st, nim.clone());
        let ep = driver_socket(Some(Ok(node_info("csi-node-1"))));
        h.register_plugin("csi.example.com", &ep, &s(&["v1.2.3"]), None)
            .await
            .unwrap();
        let d = st.get("csi.example.com").expect("driver registered");
        assert_eq!(d.endpoint, ep);
        assert_eq!(d.highest_supported_version, "1.2.3");
        {
            let installs = nim.installs.lock().unwrap();
            assert_eq!(installs.len(), 1);
            assert_eq!(installs[0].0, "csi.example.com");
            assert_eq!(installs[0].1, "csi-node-1");
            assert_eq!(installs[0].2, 7);
            assert_eq!(installs[0].3["topology.example.com/zone"], "z1");
        }

        h.deregister_plugin("csi.example.com", &ep).await;
        assert!(st.get("csi.example.com").is_none());
        assert_eq!(*nim.uninstalls.lock().unwrap(), vec!["csi.example.com"]);
    }

    /// `NodeGetInfo` failing unregisters the driver again and surfaces the
    /// error (`csi_plugin.go:150-156`); nothing is installed.
    #[tokio::test]
    async fn node_get_info_failure_unregisters_the_driver() {
        let st = store();
        let nim = Arc::new(RecordingNim::default());
        let h = RegistrationHandler::with_store(st, nim.clone());
        let ep = driver_socket(Some(Err(tonic::Code::Unavailable)));
        let err = h
            .register_plugin(
                "csi.example.com",
                &ep,
                &s(&["v1.0.0"]),
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap_err();
        assert!(!err.is_empty());
        assert!(
            st.get("csi.example.com").is_none(),
            "driver must be unregistered"
        );
        assert!(nim.installs.lock().unwrap().is_empty());
        // unregisterDriver also uninstalls.
        assert_eq!(*nim.uninstalls.lock().unwrap(), vec!["csi.example.com"]);
    }

    /// `InstallCSIDriver` failing unregisters the driver again
    /// (`csi_plugin.go:158-164`).
    #[tokio::test]
    async fn install_failure_unregisters_the_driver() {
        let st = store();
        let nim = Arc::new(RecordingNim {
            fail_install: true,
            ..Default::default()
        });
        let h = RegistrationHandler::with_store(st, nim.clone());
        let ep = driver_socket(Some(Ok(node_info("csi-node-1"))));
        let err = h
            .register_plugin("csi.example.com", &ep, &s(&["v1.0.0"]), None)
            .await
            .unwrap_err();
        assert!(err.contains("install failed"), "{err}");
        assert!(st.get("csi.example.com").is_none());
    }

    /// `unregisterDriver` deletes from the store BEFORE uninstalling, so a
    /// failing uninstall still leaves the store clean (`csi_plugin.go:963-967`).
    #[tokio::test]
    async fn deregister_clears_the_store_even_if_uninstall_fails() {
        let st = store();
        let nim = Arc::new(RecordingNim {
            fail_uninstall: true,
            ..Default::default()
        });
        let h = RegistrationHandler::with_store(st, nim);
        let ep = driver_socket(Some(Ok(node_info("n"))));
        h.register_plugin("csi.example.com", &ep, &s(&["v1.0.0"]), None)
            .await
            .unwrap();
        h.deregister_plugin("csi.example.com", &ep).await;
        assert!(st.get("csi.example.com").is_none());
    }
}
