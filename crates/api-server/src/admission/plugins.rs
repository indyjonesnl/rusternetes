//! `--enable-admission-plugins`, `--disable-admission-plugins` and the
//! deprecated `--admission-control`: which admission plugins run.
//!
//! Ported from
//! - `staging/src/k8s.io/apiserver/pkg/server/options/admission.go`
//!   (`AdmissionOptions`, `enabledPluginNames`, `Validate`),
//! - `pkg/kubeapiserver/options/admission.go` (the `--admission-control`
//!   wrapper: `Validate`, `computePluginNames`),
//! - `pkg/kubeapiserver/options/plugins.go` (`AllOrderedPlugins`,
//!   `DefaultOffAdmissionPlugins`).

use std::collections::HashSet;

/// `AllOrderedPlugins` (`pkg/kubeapiserver/options/plugins.go:69-111`): every
/// plugin kube-apiserver registers, in execution order.
pub const ALL_ORDERED_PLUGINS: &[&str] = &[
    "AlwaysAdmit",
    "NamespaceAutoProvision",
    "NamespaceLifecycle",
    "NamespaceExists",
    "LimitPodHardAntiAffinityTopology",
    "LimitRanger",
    "ServiceAccount",
    "NodeRestriction",
    "TaintNodesByCondition",
    "AlwaysPullImages",
    "ImagePolicyWebhook",
    "PodSecurity",
    "PodNodeSelector",
    "Priority",
    "DefaultTolerationSeconds",
    "PodTolerationRestriction",
    "EventRateLimit",
    "ExtendedResourceToleration",
    "DefaultStorageClass",
    "StorageObjectInUseProtection",
    "OwnerReferencesPermissionEnforcement",
    "PersistentVolumeClaimResize",
    "RuntimeClass",
    "CertificateApproval",
    "CertificateSigning",
    "ClusterTrustBundleAttest",
    "CertificateSubjectRestriction",
    "DefaultIngressClass",
    "DenyServiceExternalIPs",
    "PodTopologyLabels",
    "NodeDeclaredFeatureValidator",
    "MutatingAdmissionPolicy",
    "MutatingAdmissionWebhook",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionWebhook",
    "ResourceQuota",
    "AlwaysDeny",
];

/// The plugins `DefaultOffAdmissionPlugins` leaves on
/// (`pkg/kubeapiserver/options/plugins.go:167-190`); the rest of
/// [`ALL_ORDERED_PLUGINS`] is default-off.
const DEFAULT_ON_PLUGINS: &[&str] = &[
    "NamespaceLifecycle",
    "LimitRanger",
    "ServiceAccount",
    "DefaultStorageClass",
    "PersistentVolumeClaimResize",
    "DefaultTolerationSeconds",
    "MutatingAdmissionWebhook",
    "ValidatingAdmissionWebhook",
    "ResourceQuota",
    "StorageObjectInUseProtection",
    "Priority",
    "TaintNodesByCondition",
    "RuntimeClass",
    "CertificateApproval",
    "CertificateSigning",
    "ClusterTrustBundleAttest",
    "CertificateSubjectRestriction",
    "DefaultIngressClass",
    "PodSecurity",
    "PodTopologyLabels",
    "MutatingAdmissionPolicy",
    "ValidatingAdmissionPolicy",
    "NodeDeclaredFeatureValidator",
];

/// `DefaultOffAdmissionPlugins` (`pkg/kubeapiserver/options/plugins.go:166-199`).
pub fn default_off_plugins() -> HashSet<String> {
    ALL_ORDERED_PLUGINS
        .iter()
        .filter(|p| !DEFAULT_ON_PLUGINS.contains(p))
        .map(|p| p.to_string())
        .collect()
}

/// Go's `%v` of a `[]string`: `[a b]`.
fn go_list(names: &[String]) -> String {
    format!("[{}]", names.join(" "))
}

/// `AdmissionOptions` (apiserver/pkg/server/options/admission.go:60-74) with
/// the `--admission-control` field of kube-apiserver's wrapper
/// (pkg/kubeapiserver/options/admission.go:37-42). `None` is Go's nil slice,
/// which `Validate` tells apart from an empty one.
#[derive(Debug, Clone)]
pub struct AdmissionOptions {
    pub recommended_plugin_order: Vec<String>,
    pub default_off_plugins: HashSet<String>,
    pub enable_plugins: Option<Vec<String>>,
    pub disable_plugins: Option<Vec<String>>,
    /// `--admission-control` (deprecated).
    pub plugin_names: Option<Vec<String>>,
    /// `Plugins.Registered()`.
    pub registered: Vec<String>,
}

impl AdmissionOptions {
    /// `genericoptions.NewAdmissionOptions` (admission.go:88-104): the
    /// generic server registers `lifecycle`, the two policy plugins and the
    /// two webhooks (`server.RegisterAllAdmissionPlugins`, server/plugins.go:30-35).
    pub fn generic() -> Self {
        let order: Vec<String> = [
            "NamespaceLifecycle",
            "MutatingAdmissionPolicy",
            "MutatingAdmissionWebhook",
            "ValidatingAdmissionPolicy",
            "ValidatingAdmissionWebhook",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut registered = order.clone();
        registered.sort();
        Self {
            recommended_plugin_order: order,
            default_off_plugins: HashSet::new(),
            enable_plugins: None,
            disable_plugins: None,
            plugin_names: None,
            registered,
        }
    }

    /// `kubeoptions.NewAdmissionOptions` (pkg/kubeapiserver/options/admission.go:51-66):
    /// every kube-apiserver plugin registered, `AllOrderedPlugins` as the
    /// order, `DefaultOffAdmissionPlugins()` as the default-off set.
    pub fn kube_apiserver() -> Self {
        let mut registered: Vec<String> =
            ALL_ORDERED_PLUGINS.iter().map(|s| s.to_string()).collect();
        registered.sort();
        Self {
            recommended_plugin_order: ALL_ORDERED_PLUGINS.iter().map(|s| s.to_string()).collect(),
            default_off_plugins: default_off_plugins(),
            registered,
            ..Self::generic()
        }
    }

    /// `ApplyTo`'s pass-through (pkg/kubeapiserver/options/admission.go:128-135)
    /// and `computePluginNames` (:140-143): `--admission-control` enables
    /// exactly the plugins it names and disables every other one.
    pub fn apply_admission_control(&mut self) {
        let Some(names) = self.plugin_names.clone() else {
            return;
        };
        let disabled: std::collections::BTreeSet<String> = self
            .recommended_plugin_order
            .iter()
            .filter(|p| !names.contains(p))
            .cloned()
            .collect();
        self.enable_plugins = Some(names);
        self.disable_plugins = Some(disabled.into_iter().collect());
    }

    /// `enabledPluginNames` (admission.go:226-241): the plugins of
    /// `RecommendedPluginOrder` that are not default-off or disabled, unless
    /// enabled explicitly.
    pub fn enabled_plugin_names(&self) -> Vec<String> {
        let enabled: HashSet<&String> = self.enable_plugins.iter().flatten().collect();
        let disabled: HashSet<&String> = self
            .default_off_plugins
            .iter()
            .chain(self.disable_plugins.iter().flatten())
            .filter(|p| !enabled.contains(p))
            .collect();
        self.recommended_plugin_order
            .iter()
            .filter(|p| !disabled.contains(p))
            .cloned()
            .collect()
    }

    /// `Validate`: kube-apiserver's wrapper (pkg/kubeapiserver/options/admission.go:95-115)
    /// then the generic one (apiserver/pkg/server/options/admission.go:184-222).
    pub fn validate(&self) -> Vec<String> {
        use std::collections::BTreeSet;
        let mut errs = Vec::new();
        if self.plugin_names.is_some()
            && (self.enable_plugins.is_some() || self.disable_plugins.is_some())
        {
            errs.push(
                "admission-control and enable-admission-plugins/disable-admission-plugins flags are mutually exclusive"
                    .to_string(),
            );
        }
        let registered: BTreeSet<&String> = self.registered.iter().collect();
        for name in self.plugin_names.iter().flatten() {
            if !registered.contains(name) {
                errs.push(format!("admission-control plugin {name:?} is unknown"));
            }
        }

        for name in self.enable_plugins.iter().flatten() {
            if !registered.contains(name) {
                errs.push(format!(
                    "enable-admission-plugins plugin {name:?} is unknown"
                ));
            }
        }
        for name in self.disable_plugins.iter().flatten() {
            if !registered.contains(name) {
                errs.push(format!(
                    "disable-admission-plugins plugin {name:?} is unknown"
                ));
            }
        }
        let enable: BTreeSet<&String> = self.enable_plugins.iter().flatten().collect();
        let disable: BTreeSet<&String> = self.disable_plugins.iter().flatten().collect();
        let overlap: Vec<String> = enable
            .intersection(&disable)
            .map(|s| s.to_string())
            .collect();
        if !overlap.is_empty() {
            errs.push(format!(
                "{} in enable-admission-plugins and disable-admission-plugins overlapped",
                go_list(&overlap)
            ));
        }

        // Verify RecommendedPluginOrder (developer errors).
        let recommended: BTreeSet<&String> = self.recommended_plugin_order.iter().collect();
        let intersection: BTreeSet<&String> =
            registered.intersection(&recommended).copied().collect();
        if intersection != recommended {
            let missing: Vec<String> = recommended
                .difference(&intersection)
                .map(|s| s.to_string())
                .collect();
            errs.push(format!(
                "plugins {} in RecommendedPluginOrder are not registered",
                go_list(&missing)
            ));
        }
        if intersection != registered {
            let missing: Vec<String> = registered
                .difference(&intersection)
                .map(|s| s.to_string())
                .collect();
            errs.push(format!(
                "plugins {} registered are not in RecommendedPluginOrder",
                go_list(&missing)
            ));
        }
        errs
    }
}

impl Default for AdmissionOptions {
    fn default() -> Self {
        Self::kube_apiserver()
    }
}

/// `utilerrors.NewAggregate(errs).Error()`: one error is its message, several
/// are `[a, b]`.
pub fn aggregate_message(errs: &[String]) -> String {
    match errs {
        [one] => one.clone(),
        many => format!("[{}]", many.join(", ")),
    }
}

/// The enabled plugin names `ApplyTo` builds the chain from; `None` until
/// [`install`] runs, when the default set applies.
static ENABLED: std::sync::RwLock<Option<HashSet<String>>> = std::sync::RwLock::new(None);

/// `ApplyTo` (admission.go:136-182): settle `--admission-control`, then
/// install `enabledPluginNames()` as the chain.
pub fn install(options: &AdmissionOptions) {
    let mut options = options.clone();
    options.apply_admission_control();
    *ENABLED.write().unwrap_or_else(|e| e.into_inner()) =
        Some(options.enabled_plugin_names().into_iter().collect());
}

/// Whether `name` is in the chain this server runs.
pub fn enabled(name: &str) -> bool {
    #[cfg(test)]
    if let Some(r) = TEST_ENABLED.with(|t| t.borrow().as_ref().map(|s| s.contains(name))) {
        return r;
    }
    if let Some(set) = ENABLED.read().unwrap_or_else(|e| e.into_inner()).as_ref() {
        return set.contains(name);
    }
    !default_off_plugins().contains(name)
}

#[cfg(test)]
thread_local! {
    static TEST_ENABLED: std::cell::RefCell<Option<HashSet<String>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run the current thread with exactly `names` enabled (tests only).
#[cfg(test)]
pub struct EnabledForTest;

#[cfg(test)]
impl EnabledForTest {
    pub fn new(names: &[&str]) -> Self {
        TEST_ENABLED.with(|t| {
            *t.borrow_mut() = Some(names.iter().map(|s| s.to_string()).collect());
        });
        Self
    }
}

#[cfg(test)]
impl Drop for EnabledForTest {
    fn drop(&mut self) {
        TEST_ENABLED.with(|t| *t.borrow_mut() = None);
    }
}

/// The admission flags, shared by the `api-server` and all-in-one binaries
/// (`AddFlags`: pkg/kubeapiserver/options/admission.go:69-92 and
/// apiserver/pkg/server/options/admission.go:107-124).
#[derive(clap::Args, Debug, Clone, Default)]
pub struct AdmissionArgs {
    /// Admission plugins that should be enabled in addition to default
    /// enabled ones. Comma-delimited. The order does not matter.
    #[arg(long = "enable-admission-plugins", value_delimiter = ',')]
    pub enable_admission_plugins: Option<Vec<String>>,

    /// Admission plugins that should be disabled although they are in the
    /// default enabled plugins list. Comma-delimited. The order does not
    /// matter.
    #[arg(long = "disable-admission-plugins", value_delimiter = ',')]
    pub disable_admission_plugins: Option<Vec<String>>,

    /// DEPRECATED: use --enable-admission-plugins or
    /// --disable-admission-plugins instead. Comma-delimited list of the
    /// plugins to run; the order does not matter.
    #[arg(long = "admission-control", value_delimiter = ',')]
    pub admission_control: Option<Vec<String>>,
}

impl AdmissionArgs {
    pub fn to_options(&self) -> AdmissionOptions {
        AdmissionOptions {
            enable_plugins: self.enable_admission_plugins.clone(),
            disable_plugins: self.disable_admission_plugins.clone(),
            plugin_names: self.admission_control.clone(),
            ..AdmissionOptions::kube_apiserver()
        }
    }

    /// `s.Admission.Validate()` then `ApplyTo`: refuse to start on a bad
    /// flag, else install the chain.
    pub fn validate_and_install(&self) -> Result<(), String> {
        let options = self.to_options();
        let errs = options.validate();
        if !errs.is_empty() {
            return Err(aggregate_message(&errs));
        }
        install(&options);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn strs(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// `TestAdmissionPluginOrder` (pkg/kubeapiserver/options/plugins_test.go:24-31).
    #[test]
    fn admission_plugin_order_ends_with_webhooks_quota_deny() {
        let all = ALL_ORDERED_PLUGINS.join(",");
        assert!(all.ends_with(
            ",MutatingAdmissionPolicy,MutatingAdmissionWebhook,ValidatingAdmissionPolicy,ValidatingAdmissionWebhook,ResourceQuota,AlwaysDeny"
        ));
    }

    /// The default-on set of `DefaultOffAdmissionPlugins`
    /// (plugins.go:166-199), in `AllOrderedPlugins` order. Written out from
    /// the upstream list, not derived from it.
    #[test]
    fn default_enabled_set_matches_upstream() {
        let opts = AdmissionOptions::kube_apiserver();
        assert_eq!(
            opts.enabled_plugin_names(),
            strs(&[
                "NamespaceLifecycle",
                "LimitRanger",
                "ServiceAccount",
                "TaintNodesByCondition",
                "PodSecurity",
                "Priority",
                "DefaultTolerationSeconds",
                "DefaultStorageClass",
                "StorageObjectInUseProtection",
                "PersistentVolumeClaimResize",
                "RuntimeClass",
                "CertificateApproval",
                "CertificateSigning",
                "ClusterTrustBundleAttest",
                "CertificateSubjectRestriction",
                "DefaultIngressClass",
                "PodTopologyLabels",
                "NodeDeclaredFeatureValidator",
                "MutatingAdmissionPolicy",
                "MutatingAdmissionWebhook",
                "ValidatingAdmissionPolicy",
                "ValidatingAdmissionWebhook",
                "ResourceQuota",
            ])
        );
        assert!(opts.validate().is_empty());
    }

    /// `TestEnabledPluginNames` (options/admission_test.go:28-131).
    #[test]
    fn enabled_plugin_names_scenarios() {
        struct S {
            expected: Vec<&'static str>,
            default_off: Option<HashSet<String>>,
            order: Option<Vec<String>>,
            enable: Option<Vec<String>>,
            disable: Option<Vec<String>>,
            admission_control: Option<Vec<String>>,
        }
        let abcd = || Some(strs(&["pluginA", "pluginB", "pluginC", "pluginD"]));
        let scenarios = vec![
            // 0: the generic NewAdmissionOptions defaults.
            S {
                expected: vec![
                    "NamespaceLifecycle",
                    "MutatingAdmissionPolicy",
                    "MutatingAdmissionWebhook",
                    "ValidatingAdmissionPolicy",
                    "ValidatingAdmissionWebhook",
                ],
                default_off: None,
                order: None,
                enable: None,
                disable: None,
                admission_control: None,
            },
            // 1
            S {
                expected: vec!["pluginB"],
                default_off: Some(set(&["pluginA", "pluginC", "pluginD"])),
                order: abcd(),
                enable: None,
                disable: None,
                admission_control: None,
            },
            // 2
            S {
                expected: vec!["pluginA", "pluginB", "pluginC", "pluginD"],
                default_off: Some(set(&[])),
                order: abcd(),
                enable: None,
                disable: None,
                admission_control: None,
            },
            // 3
            S {
                expected: vec!["pluginA", "pluginB", "pluginC", "pluginD"],
                default_off: Some(set(&["pluginC", "pluginD"])),
                order: abcd(),
                enable: Some(strs(&["pluginD", "pluginC"])),
                disable: None,
                admission_control: None,
            },
            // 4
            S {
                expected: vec!["pluginB"],
                default_off: Some(set(&["pluginC", "pluginD"])),
                order: abcd(),
                enable: None,
                disable: Some(strs(&["pluginA"])),
                admission_control: None,
            },
            // 5
            S {
                expected: vec!["pluginA", "pluginC"],
                default_off: Some(set(&["pluginC", "pluginD"])),
                order: abcd(),
                enable: Some(strs(&["pluginC"])),
                disable: Some(strs(&["pluginB"])),
                admission_control: None,
            },
            // 6 (`setAdmissionControl` feeds EnablePlugins in the upstream test)
            S {
                expected: vec!["pluginA", "pluginB", "pluginC"],
                default_off: Some(set(&["pluginD"])),
                order: abcd(),
                enable: None,
                disable: None,
                admission_control: Some(strs(&["pluginA", "pluginB"])),
            },
            // 7
            S {
                expected: vec!["pluginA", "pluginB", "pluginC"],
                default_off: Some(set(&["pluginC", "pluginD"])),
                order: abcd(),
                enable: None,
                disable: None,
                admission_control: Some(strs(&["pluginA", "pluginB", "pluginC"])),
            },
        ];
        for (i, s) in scenarios.into_iter().enumerate() {
            let mut target = AdmissionOptions::generic();
            if let Some(d) = s.default_off {
                target.default_off_plugins = d;
            }
            if let Some(o) = s.order {
                target.recommended_plugin_order = o;
            }
            if let Some(e) = s.enable {
                target.enable_plugins = Some(e);
            }
            if let Some(d) = s.disable {
                target.disable_plugins = Some(d);
            }
            if let Some(a) = s.admission_control {
                target.enable_plugins = Some(a);
            }
            assert_eq!(
                target.enabled_plugin_names(),
                strs(&s.expected),
                "scenario {i}"
            );
        }
    }

    /// `TestValidate` (options/admission_test.go:133-232).
    #[test]
    fn validate_scenarios() {
        struct S {
            enable: Option<Vec<String>>,
            disable: Option<Vec<String>>,
            order: Option<Vec<String>>,
            ok: bool,
        }
        let none = || S {
            enable: None,
            disable: None,
            order: None,
            ok: true,
        };
        let scenarios = vec![
            none(),
            S {
                enable: Some(strs(&["pluginA", "pluginB"])),
                disable: Some(strs(&["pluginC"])),
                ..none()
            },
            S {
                enable: Some(strs(&["pluginA", "pluginB"])),
                disable: Some(strs(&["pluginB"])),
                ok: false,
                ..none()
            },
            S {
                enable: Some(strs(&["pluginA", "pluginE"])),
                ok: false,
                ..none()
            },
            S {
                disable: Some(strs(&["pluginA", "pluginE"])),
                ok: false,
                ..none()
            },
            S {
                enable: Some(strs(&["pluginA", "pluginB"])),
                ..none()
            },
            S {
                disable: Some(strs(&["pluginA"])),
                ..none()
            },
            S {
                order: Some(strs(&["pluginA", "pluginB", "pluginB", "pluginC"])),
                ok: false,
                ..none()
            },
            S {
                order: Some(strs(&["pluginA", "pluginB", "pluginC"])),
                ok: false,
                ..none()
            },
            S {
                order: Some(strs(&["pluginA", "pluginB", "pluginC", "pluginD"])),
                ..none()
            },
            S {
                order: Some(strs(&["pluginA", "pluginB", "pluginC", "pluginE"])),
                ok: false,
                ..none()
            },
        ];
        for (i, s) in scenarios.into_iter().enumerate() {
            let mut o = AdmissionOptions::generic();
            o.default_off_plugins = set(&["pluginC", "pluginD"]);
            o.recommended_plugin_order = strs(&["pluginA", "pluginB", "pluginC", "pluginD"]);
            o.registered = strs(&["pluginA", "pluginB", "pluginC", "pluginD"]);
            o.enable_plugins = s.enable;
            o.disable_plugins = s.disable;
            if let Some(order) = s.order {
                o.recommended_plugin_order = order;
            }
            let errs = o.validate();
            assert_eq!(errs.is_empty(), s.ok, "scenario {i}: {errs:?}");
        }
    }

    /// The exact wording of the unknown / overlapped errors
    /// (generic admission.go:190-203).
    #[test]
    fn validate_error_messages() {
        let mut o = AdmissionOptions::kube_apiserver();
        o.enable_plugins = Some(strs(&["Bogus", "NodeRestriction"]));
        o.disable_plugins = Some(strs(&["Nope", "NodeRestriction"]));
        assert_eq!(
            o.validate(),
            vec![
                "enable-admission-plugins plugin \"Bogus\" is unknown".to_string(),
                "disable-admission-plugins plugin \"Nope\" is unknown".to_string(),
                "[NodeRestriction] in enable-admission-plugins and disable-admission-plugins overlapped"
                    .to_string(),
            ]
        );
    }

    /// kube wrapper `Validate` (pkg/kubeapiserver/options/admission.go:95-115)
    /// and `computePluginNames` (:140-143).
    #[test]
    fn admission_control_flag() {
        let mut o = AdmissionOptions::kube_apiserver();
        o.plugin_names = Some(strs(&["NamespaceLifecycle"]));
        o.enable_plugins = Some(strs(&["NodeRestriction"]));
        assert!(o
            .validate()
            .contains(&"admission-control and enable-admission-plugins/disable-admission-plugins flags are mutually exclusive".to_string()));

        let mut o = AdmissionOptions::kube_apiserver();
        o.plugin_names = Some(strs(&["Bogus"]));
        assert_eq!(
            o.validate(),
            vec!["admission-control plugin \"Bogus\" is unknown".to_string()]
        );

        let mut o = AdmissionOptions::kube_apiserver();
        o.plugin_names = Some(strs(&["NodeRestriction", "ServiceAccount"]));
        o.apply_admission_control();
        assert_eq!(
            o.enabled_plugin_names(),
            strs(&["ServiceAccount", "NodeRestriction"])
        );
    }

    /// The flags parse into the options.
    #[test]
    fn flags_parse() {
        use clap::Parser;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            admission: AdmissionArgs,
        }
        let cli = Cli::parse_from([
            "x",
            "--enable-admission-plugins=NodeRestriction,AlwaysPullImages",
            "--disable-admission-plugins",
            "LimitRanger",
        ]);
        let o = cli.admission.to_options();
        assert_eq!(
            o.enable_plugins,
            Some(strs(&["NodeRestriction", "AlwaysPullImages"]))
        );
        assert_eq!(o.disable_plugins, Some(strs(&["LimitRanger"])));
        assert!(o.validate().is_empty());
        assert!(!o
            .enabled_plugin_names()
            .contains(&"LimitRanger".to_string()));
        assert!(o
            .enabled_plugin_names()
            .contains(&"NodeRestriction".to_string()));
    }
}
