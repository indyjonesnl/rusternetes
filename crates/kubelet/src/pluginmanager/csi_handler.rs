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
//! After a successful `RegisterPlugin`, and in `DeRegisterPlugin`, the handler
//! tells the `csiNodeUpdater` to re-evaluate the driver's periodic
//! `NodeGetInfo` refresh (`csiNodeUpdaterVar.syncDriverUpdater`,
//! `csi_plugin.go:165-167`, `:276-278`); see
//! `volume_plugins::csi_node_updater`.

use super::cache::PluginHandler;
use crate::volume_plugins::csi_client::{CsiDriverClient, CSI_TIMEOUT};
use crate::volume_plugins::csi_drivers_store::{csi_drivers, Driver, DriversStore};
use crate::volume_plugins::nodeinfomanager::NodeInfoInstaller;
use async_trait::async_trait;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// `newCsiDriverClient` (`csi_client.go:153-176`): a client for the endpoint a
/// registered driver reported, looked up by name in the store.
pub fn csi_driver_client_for(
    drivers: &DriversStore,
    driver_name: &str,
) -> Result<CsiDriverClient, String> {
    let driver = drivers.get(driver_name).ok_or_else(|| {
        format!("driver name {driver_name} not found in the list of registered CSI drivers")
    })?;
    Ok(CsiDriverClient::with_endpoint(driver_name, driver.endpoint))
}

/// The one method of `csiNodeUpdater` the handler calls
/// (`csiNodeUpdaterVar.syncDriverUpdater`).
#[async_trait]
pub trait DriverUpdaterSync: Send + Sync {
    async fn sync_driver_updater(&self, driver_name: &str);
}

#[async_trait]
impl<S: rusternetes_storage::Storage + 'static> DriverUpdaterSync
    for crate::volume_plugins::csi_node_updater::CsiNodeUpdater<S>
{
    async fn sync_driver_updater(&self, driver_name: &str) {
        crate::volume_plugins::csi_node_updater::CsiNodeUpdater::sync_driver_updater(
            self,
            driver_name,
        )
        .await
    }
}

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
    /// Upstream's package-level `csiNodeUpdaterVar` (nil unless the
    /// `MutableCSINodeAllocatableCount` gate is on, `csi_plugin.go:417`).
    node_updater: Option<Arc<dyn DriverUpdaterSync>>,
}

impl RegistrationHandler {
    /// Backed by the process-wide `csiDrivers` store, as upstream's
    /// `PluginHandler` is.
    pub fn new(nim: Arc<dyn NodeInfoInstaller>) -> Self {
        Self {
            drivers: csi_drivers(),
            nim,
            node_updater: None,
        }
    }

    /// Set `csiNodeUpdaterVar`.
    pub fn with_node_updater(mut self, node_updater: Arc<dyn DriverUpdaterSync>) -> Self {
        self.node_updater = Some(node_updater);
        self
    }

    /// Back the handler with another store (tests).
    pub fn with_store(drivers: &'static DriversStore, nim: Arc<dyn NodeInfoInstaller>) -> Self {
        Self {
            drivers,
            nim,
            node_updater: None,
        }
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

        // `csi_plugin.go:165-167`
        if let Some(u) = &self.node_updater {
            u.sync_driver_updater(plugin_name).await;
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
        // `csi_plugin.go:276-278`
        if let Some(u) = &self.node_updater {
            u.sync_driver_updater(plugin_name).await;
        }
    }
}
