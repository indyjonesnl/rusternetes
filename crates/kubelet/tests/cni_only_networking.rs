//! Regression guard for the netstack removal (spec 001-remove-netlink).
//!
//! Pod networking is provided **only** by the CNI plugin via the kubelet's
//! CRI backend. There is no embedded in-process network stack, no
//! `PodNetworkMode` selector, and no `netstack` handle on `KubeletConfig`.
//!
//! This test is intentionally compile-time-heavy: it constructs a
//! `KubeletConfig` positionally-free (struct-literal via `..Default::default()`
//! is NOT used so that adding a networking field back would break this file),
//! proving the config surface carries no netstack/pod-network-mode knob. If a
//! future change reintroduces an in-process networking field, this test stops
//! compiling — the intended tripwire.

use rusternetes_kubelet::KubeletConfig;

#[test]
fn kubelet_config_is_cni_only_no_netstack_surface() {
    // Full struct literal: every field of KubeletConfig must be listed here.
    // If a `netstack` / `pod_network_mode` (or any other networking-mode)
    // field is added back, this literal fails to compile.
    let config = KubeletConfig {
        node_name: "node-1".to_string(),
        volume_dir: "./volumes".to_string(),
        cluster_dns: "10.96.0.10".to_string(),
        cluster_domain: "cluster.local".to_string(),
        network: "rusternetes-network".to_string(),
        sync_interval: 3,
        metrics_port: 10250,
        kubernetes_service_host: "10.96.0.1".to_string(),
    };

    // Sanity: the default config is equivalent on the fields we set.
    let default = KubeletConfig::default();
    assert_eq!(config.node_name, default.node_name);
    assert_eq!(config.cluster_dns, default.cluster_dns);
    assert_eq!(config.metrics_port, default.metrics_port);
}
