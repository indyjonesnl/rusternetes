//! Translate a rusternetes [`Pod`] into CRI v1 sandbox and container configs.
//!
//! These are pure functions — they take a `Pod` (and, for containers, a
//! resolved image ref and a volume-name → host-path map) and produce the
//! `runtime.v1` config messages the CRI runtime expects. Keeping them pure makes
//! the Pod→CRI mapping unit-testable without a running runtime, and isolates the
//! single place where rusternetes' resource model meets the CRI wire model.
//!
//! Scope: the fields needed to launch a pod — metadata, labels, namespaces,
//! command/args/env, ports, resources, mounts, and the linux security context.
//! `valueFrom` env is resolved here from inputs the kubelet fetches first:
//! `fieldRef`/`resourceFieldRef` from the pod itself, `configMapKeyRef`/
//! `secretKeyRef` from caller-supplied ConfigMap/Secret maps (the runtime reads
//! these from storage before translation — see `container_config`). `envFrom`
//! bulk-imports a ConfigMap/Secret's keys (optionally prefixed). Deferred
//! (tracked separately): probes (driven kubelet-side via ExecSync) and windows
//! configs.

use std::collections::HashMap;
use std::path::Path;

use rusternetes_common::quantity::parse_resource_value;
use rusternetes_common::resources::pod::{Container, Pod};
use rusternetes_common::resources::{ConfigMap, Secret, Service};
use rusternetes_cri::v1;

/// Well-known CRI metadata label keys the runtime indexes sandboxes/containers
/// by. They mirror the keys the upstream kubelet sets so `crictl`/tools work.
///
/// Re-exported from [`crate::labels`] — the port of
/// `staging/src/k8s.io/kubelet/pkg/types/labels.go` — under the shorter names
/// this module's call sites use. These were a second, independent set of string
/// literals with the same values; a typo or a rename in one would have silently
/// orphaned every sandbox the other had labelled.
pub mod labels {
    pub use crate::labels::{
        CONTAINER_NAME_LABEL as CONTAINER_NAME, POD_NAMESPACE_LABEL as POD_NAMESPACE,
        POD_NAME_LABEL as POD_NAME, POD_UID_LABEL as POD_UID,
    };
}

fn namespace(pod: &Pod) -> &str {
    pod.metadata.namespace.as_deref().unwrap_or("default")
}

/// True when the pod requested the host network namespace.
fn host_network(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.host_network)
        .unwrap_or(false)
}

/// Build the sandbox-level namespace options (host vs pod network/pid/ipc).
fn namespace_options(pod: &Pod) -> v1::NamespaceOption {
    let spec = pod.spec.as_ref();
    let host = |f: fn(&rusternetes_common::resources::pod::PodSpec) -> Option<bool>| {
        spec.and_then(f).unwrap_or(false)
    };
    let mode = |on: bool| {
        if on {
            v1::NamespaceMode::Node as i32
        } else {
            v1::NamespaceMode::Pod as i32
        }
    };
    v1::NamespaceOption {
        network: mode(host_network(pod)),
        pid: mode(host(|s| s.host_pid)),
        ipc: mode(host(|s| s.host_ipc)),
        ..Default::default()
    }
}

/// Labels CRI attaches so the sandbox/container can be looked up by pod.
fn pod_labels(pod: &Pod) -> HashMap<String, String> {
    let mut l = HashMap::new();
    l.insert(labels::POD_NAME.to_string(), pod.metadata.name.clone());
    l.insert(
        labels::POD_NAMESPACE.to_string(),
        namespace(pod).to_string(),
    );
    l.insert(labels::POD_UID.to_string(), pod.metadata.uid.clone());
    l
}

fn truncate_hostname_for_kernel(hostname: &str) -> String {
    const HOSTNAME_MAX_LEN: usize = 63;
    if hostname.len() <= HOSTNAME_MAX_LEN {
        return hostname.to_string();
    }
    hostname[..HOSTNAME_MAX_LEN]
        .trim_end_matches(['-', '.'])
        .to_string()
}

/// Aggregate every container port into CRI sandbox port mappings.
fn port_mappings(pod: &Pod) -> Vec<v1::PortMapping> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for c in &spec.containers {
        let Some(ports) = c.ports.as_ref() else {
            continue;
        };
        for p in ports {
            out.push(v1::PortMapping {
                protocol: protocol_to_cri(Some(p.protocol.as_str())),
                container_port: i32::from(p.container_port),
                host_port: p.host_port.map(i32::from).unwrap_or(0),
                host_ip: p.host_ip.clone().unwrap_or_default(),
            });
        }
    }
    out
}

fn protocol_to_cri(proto: Option<&str>) -> i32 {
    match proto.unwrap_or("TCP").to_ascii_uppercase().as_str() {
        "UDP" => v1::Protocol::Udp as i32,
        "SCTP" => v1::Protocol::Sctp as i32,
        _ => v1::Protocol::Tcp as i32,
    }
}

/// Normalize a sysctl name before handing it to the CRI runtime.
///
/// Kubernetes accepts `/` as an alternate separator for `.`. Runtimes such as
/// runc still validate namespace ownership against dot-form names, so kubelet
/// normalizes slash-first names before sandbox creation. The swap preserves
/// interface names such as `eno2/100` in already-dot-form names.
pub fn normalize_sysctl_name(name: &str) -> String {
    match name.find(['.', '/']) {
        Some(index) if name.as_bytes()[index] == b'/' => name
            .chars()
            .map(|c| match c {
                '.' => '/',
                '/' => '.',
                _ => c,
            })
            .collect(),
        _ => name.to_string(),
    }
}

/// Sysctls to apply to the pod sandbox, from `spec.securityContext.sysctls`.
///
/// CRI carries these as `LinuxPodSandboxConfig.sysctls` (a `name -> value`
/// map); the runtime applies them to the sandbox's network/IPC namespaces. We
/// pass through what the pod declares — admission (allowed/unsafe-sysctl
/// gating) is a separate concern upstream, not the translation layer's.
fn sysctls(pod: &Pod) -> HashMap<String, String> {
    pod.spec
        .as_ref()
        .and_then(|s| s.security_context.as_ref())
        .and_then(|sc| sc.sysctls.as_ref())
        .map(|list| {
            list.iter()
                .map(|s| (normalize_sysctl_name(&s.name), s.value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// De-duplicate a sequence of strings, preserving first-seen order
/// (upstream `omitDuplicates`, `pkg/kubelet/network/dns/dns.go:89`).
fn dedup<I: IntoIterator<Item = String>>(iter: I) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    iter.into_iter()
        .filter(|x| seen.insert(x.clone()))
        .collect()
}

/// Upstream `validation.MaxDNSNameservers`
/// (`pkg/apis/core/validation/validation.go:4127`).
const MAX_DNS_NAMESERVERS: usize = 3;
/// Upstream `validation.MaxDNSSearchPaths` (`validation.go:4129`).
const MAX_DNS_SEARCH_PATHS: usize = 32;
/// Upstream `validation.MaxDNSSearchListChars` (`validation.go:4131`).
const MAX_DNS_SEARCH_LIST_CHARS: usize = 2048;
/// Upstream `utilvalidation.DNS1123SubdomainMaxLength`.
const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;
/// Upstream `maxResolvConfLength` (`dns.go:56`).
pub const MAX_RESOLV_CONF_LENGTH: u64 = 10 << 20;

/// Upstream `appendOptions` (`dns.go:357`): later options override earlier ones
/// of the same name (the part before `:`). Order is first-seen (Go's map
/// iteration order is random; we keep it deterministic).
fn append_options(options: Vec<String>, new: impl IntoIterator<Item = String>) -> Vec<String> {
    let name = |s: &str| s.split(':').next().unwrap_or(s).to_string();
    let mut out = options;
    for o in new {
        match out.iter().position(|e| name(e) == name(&o)) {
            Some(pos) => out[pos] = o,
            None => out.push(o),
        }
    }
    out
}

/// Port of upstream `parseResolvConf` (`dns.go:226`): nameservers accumulate,
/// the last `search` line wins (trailing dots trimmed, `.` dropped), `options`
/// lines are merged name-keyed. A bare `nameserver` line is an error.
pub fn parse_resolv_conf(content: &str) -> Result<v1::DnsConfig, String> {
    let mut cfg = v1::DnsConfig::default();
    let mut errors: Vec<&str> = Vec::new();
    for line in content.split('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = trimmed.split_whitespace().collect();
        let Some(first) = fields.first() else {
            continue;
        };
        match *first {
            "nameserver" => {
                if fields.len() >= 2 {
                    cfg.servers.push(fields[1].to_string());
                } else {
                    errors.push("nameserver list is empty ");
                }
            }
            "search" => {
                cfg.searches = fields[1..]
                    .iter()
                    .filter(|s| **s != ".")
                    .map(|s| s.strip_suffix('.').unwrap_or(s).to_string())
                    .collect();
            }
            "options" => {
                let opts = std::mem::take(&mut cfg.options);
                cfg.options = append_options(opts, fields[1..].iter().map(|s| s.to_string()));
            }
            _ => {}
        }
    }
    if errors.is_empty() {
        Ok(cfg)
    } else {
        Err(errors.join("\n"))
    }
}

/// Read and parse the host resolver file (upstream `getDNSConfig`,
/// `dns.go:279`). An empty path yields an empty config.
pub fn read_host_dns_config(path: &str) -> Result<v1::DnsConfig, String> {
    use std::io::Read;
    if path.is_empty() {
        return Ok(v1::DnsConfig::default());
    }
    let f = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    f.take(MAX_RESOLV_CONF_LENGTH)
        .read_to_end(&mut buf)
        .map_err(|e| e.to_string())?;
    parse_resolv_conf(&String::from_utf8_lossy(&buf))
        .map_err(|e| format!("Encountered error while parsing resolv conf file. Error: {e}"))
}

/// Port of upstream `mergeDNSOptions` (`dns.go:327`): an option from the pod's
/// `dnsConfig` overrides an existing option of the same name; otherwise it is
/// appended. Each option renders as `name` or `name:value`.
fn merge_dns_options(
    base: Vec<String>,
    extra: &[rusternetes_common::resources::pod::PodDNSConfigOption],
) -> Vec<String> {
    let opt_name = |s: &str| s.split(':').next().unwrap_or(s).to_string();
    let mut out = base;
    for o in extra {
        let rendered = match o.value.as_deref() {
            Some(v) if !v.is_empty() => format!("{}:{}", o.name, v),
            _ => o.name.clone(),
        };
        match out.iter().position(|e| opt_name(e) == o.name) {
            Some(pos) => out[pos] = rendered,
            None => out.push(rendered),
        }
    }
    out
}

/// Which object a [`DnsEvent`] is recorded against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsEventTarget {
    /// The pod being configured.
    Pod,
    /// The node (upstream `Configurer.nodeRef`).
    Node,
}

/// A warning event the upstream `Configurer` records while forming a pod's DNS
/// config (`c.recorder.Event` / `Eventf`). Always `Warning` type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsEvent {
    pub target: DnsEventTarget,
    pub reason: &'static str,
    pub message: String,
}

/// Port of upstream `formDNSSearchFitsLimits` (`dns.go:102`) and
/// `formDNSNameserversFitsLimits` (`dns.go:149`). Each truncation records a
/// `DNSConfigForming` warning on the pod, returned in `events`.
fn form_dns_config_fits_limits(
    mut cfg: v1::DnsConfig,
    pod: &Pod,
    events: &mut Vec<DnsEvent>,
) -> v1::DnsConfig {
    let pod_name = pod.metadata.name.as_str();
    if cfg.servers.len() > MAX_DNS_NAMESERVERS {
        cfg.servers.truncate(MAX_DNS_NAMESERVERS);
        let message = format!(
            "Nameserver limits were exceeded, some nameservers have been omitted, the applied nameserver line is: {}",
            cfg.servers.join(" ")
        );
        tracing::warn!(pod = pod_name, "DNSConfigForming: {message}");
        events.push(DnsEvent {
            target: DnsEventTarget::Pod,
            reason: "DNSConfigForming",
            message,
        });
    }

    let mut exceeded = false;
    let mut search = cfg.searches;
    if search.len() > MAX_DNS_SEARCH_PATHS {
        search.truncate(MAX_DNS_SEARCH_PATHS);
        exceeded = true;
    }
    // glibc 2.28 aborts on a search path over 255 chars; filter them out.
    let before = search.len();
    search.retain(|s| s.len() <= DNS1123_SUBDOMAIN_MAX_LENGTH);
    if search.len() != before {
        exceeded = true;
    }
    let line_len = search.join(" ").len();
    if line_len > MAX_DNS_SEARCH_LIST_CHARS {
        let mut cut_num = 0usize;
        let mut cut_len = 0usize;
        for s in search.iter().rev() {
            cut_len += s.len() + 1;
            cut_num += 1;
            if line_len.saturating_sub(cut_len) <= MAX_DNS_SEARCH_LIST_CHARS {
                break;
            }
        }
        search.truncate(search.len() - cut_num);
        exceeded = true;
    }
    if exceeded {
        let message = format!(
            "Search Line limits were exceeded, some search paths have been omitted, the applied search line is: {}",
            search.join(" ")
        );
        tracing::warn!(pod = pod_name, "DNSConfigForming: {message}");
        events.push(DnsEvent {
            target: DnsEventTarget::Pod,
            reason: "DNSConfigForming",
            message,
        });
    }
    cfg.searches = search;
    cfg
}

/// Port of upstream `Configurer.CheckLimitsForResolvConf` (`dns.go:178`).
/// Returns the message of the `CheckLimitsForResolvConf` node warning, or
/// `None` when the host resolv.conf is within limits. Upstream calls it once
/// from `syncLoop` (`kubelet.go:2525`) when `ResolverConfig != ""`.
pub fn check_limits_for_resolv_conf(resolv_conf: &str, cluster_domain: &str) -> Option<String> {
    let content = match std::fs::read(resolv_conf) {
        Ok(c) => c,
        Err(e) => return Some(format!("open {resolv_conf}: {e}")),
    };
    let host = match parse_resolv_conf(&String::from_utf8_lossy(&content)) {
        Ok(h) => h,
        Err(e) => return Some(e),
    };
    let mut domain_count_limit = MAX_DNS_SEARCH_PATHS;
    if !cluster_domain.is_empty() {
        domain_count_limit -= 3;
    }
    if host.searches.len() > domain_count_limit {
        return Some(format!(
            "Resolv.conf file '{resolv_conf}' contains search line consisting of more than {domain_count_limit} domains!"
        ));
    }
    if host
        .searches
        .iter()
        .any(|s| s.len() > DNS1123_SUBDOMAIN_MAX_LENGTH)
    {
        return Some(format!(
            "Resolv.conf file \"{resolv_conf}\" contains a search path which length is more than allowed {DNS1123_SUBDOMAIN_MAX_LENGTH} chars!"
        ));
    }
    if host.searches.join(" ").len() > MAX_DNS_SEARCH_LIST_CHARS {
        return Some(format!(
            "Resolv.conf file '{resolv_conf}' contains search line which length is more than allowed {MAX_DNS_SEARCH_LIST_CHARS} chars!"
        ));
    }
    None
}

/// Port of upstream `Configurer.GetPodDNS` (`pkg/kubelet/network/dns/dns.go:386`)
/// including `getPodDNSType` (`:304`), `generateSearchesForDNSClusterFirst`
/// (`:165`), `appendDNSConfig` (`:378`) and the limit trimming (`:159`).
///
/// `host` is the parsed host resolver file ([`read_host_dns_config`]): the base
/// for `Default` and the source of host searches for `ClusterFirst*`; policy
/// `None` starts empty. The `ResolverConfig == ""` localhost fallback (`:431`)
/// is ported in [`get_pod_dns_with_events`], which this wraps with a non-empty
/// resolv-conf path and drops the events.
pub fn get_pod_dns(
    pod: &Pod,
    cluster_dns: &[String],
    cluster_domain: &str,
    host: &v1::DnsConfig,
) -> v1::DnsConfig {
    get_pod_dns_with_events(
        pod,
        cluster_dns,
        cluster_domain,
        host,
        "/etc/resolv.conf",
        &[],
    )
    .0
}

/// [`get_pod_dns`] plus the `resolv_conf` path (`Configurer.ResolverConfig`) and
/// `node_ips` (`Configurer.nodeIPs`) it depends on, returning the warning
/// events upstream records via `c.recorder` (`MissingClusterDNS` on node + pod,
/// `DNSConfigForming` on the pod). An empty `resolv_conf` triggers the
/// localhost fallback (`dns.go:431-447`).
pub fn get_pod_dns_with_events(
    pod: &Pod,
    cluster_dns: &[String],
    cluster_domain: &str,
    host: &v1::DnsConfig,
    resolv_conf: &str,
    node_ips: &[String],
) -> (v1::DnsConfig, Vec<DnsEvent>) {
    let mut events = Vec::new();
    let policy = pod
        .spec
        .as_ref()
        .and_then(|s| s.dns_policy.as_deref())
        // The api-server defaults an unset dnsPolicy to ClusterFirst.
        .unwrap_or("ClusterFirst");

    enum Kind {
        Cluster,
        Host,
        None,
    }
    let kind = match policy {
        "None" => Kind::None,
        "ClusterFirstWithHostNet" => Kind::Cluster,
        "ClusterFirst" if !host_network(pod) => Kind::Cluster,
        "ClusterFirst" | "Default" => Kind::Host,
        // Invalid policy: upstream logs and falls back to ClusterFirst.
        _ => Kind::Cluster,
    };

    let mut cfg = match kind {
        Kind::None => v1::DnsConfig::default(),
        Kind::Cluster if !cluster_dns.is_empty() => {
            // generateSearchesForDNSClusterFirst: with no cluster domain the
            // host searches are kept as-is.
            let searches = if cluster_domain.is_empty() {
                host.searches.clone()
            } else {
                let ns = namespace(pod);
                dedup(
                    [
                        format!("{ns}.svc.{cluster_domain}"),
                        format!("svc.{cluster_domain}"),
                        cluster_domain.to_string(),
                    ]
                    .into_iter()
                    .chain(host.searches.iter().cloned()),
                )
            };
            v1::DnsConfig {
                servers: cluster_dns.to_vec(),
                searches,
                options: vec!["ndots:5".to_string()],
            }
        }
        // Cluster without ClusterDNS falls through to Default (MissingClusterDNS).
        Kind::Cluster | Kind::Host => {
            if matches!(kind, Kind::Cluster) {
                let node_msg = "kubelet does not have ClusterDNS IP configured and cannot create Pod using \"ClusterFirst\" policy. Falling back to \"Default\" policy.".to_string();
                // format.Pod: name_namespace(uid).
                let pod_msg = format!(
                    "pod: \"{}_{}({})\". {}",
                    pod.metadata.name,
                    namespace(pod),
                    pod.metadata.uid,
                    node_msg
                );
                tracing::warn!(pod = %pod.metadata.name, "MissingClusterDNS: {node_msg}");
                events.push(DnsEvent {
                    target: DnsEventTarget::Node,
                    reason: "MissingClusterDNS",
                    message: node_msg,
                });
                events.push(DnsEvent {
                    target: DnsEventTarget::Pod,
                    reason: "MissingClusterDNS",
                    message: pod_msg,
                });
            }
            let mut cfg = host.clone();
            // dns.go:431: `--resolv-conf=""` => localhost nameserver (bind's
            // documented default) and a "." search, disabling lookups.
            if resolv_conf.is_empty() {
                for ip in node_ips {
                    cfg.servers.push(if ip.contains(':') {
                        "::1".to_string()
                    } else {
                        "127.0.0.1".to_string()
                    });
                }
                if cfg.servers.is_empty() {
                    cfg.servers.push("127.0.0.1".to_string());
                }
                cfg.searches = vec![".".to_string()];
            }
            cfg
        }
    };

    // appendDNSConfig.
    if let Some(dns) = pod.spec.as_ref().and_then(|s| s.dns_config.as_ref()) {
        cfg.servers = dedup(
            cfg.servers
                .into_iter()
                .chain(dns.nameservers.iter().flatten().cloned()),
        );
        cfg.searches = dedup(
            cfg.searches
                .into_iter()
                .chain(dns.searches.iter().flatten().cloned()),
        );
        cfg.options = merge_dns_options(cfg.options, dns.options.as_deref().unwrap_or(&[]));
    }
    let cfg = form_dns_config_fits_limits(cfg, pod, &mut events);
    (cfg, events)
}

/// Whether any container in the pod requests `privileged` (upstream
/// `kubecontainer.HasPrivilegedContainer`). Covers regular + init containers.
fn pod_has_privileged_container(pod: &Pod) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    let any_priv = |cs: &[Container]| {
        cs.iter().any(|c| {
            c.security_context
                .as_ref()
                .and_then(|s| s.privileged)
                .unwrap_or(false)
        })
    };
    any_priv(&spec.containers)
        || spec
            .init_containers
            .as_deref()
            .map(any_priv)
            .unwrap_or(false)
}

/// Build the pod sandbox's [`LinuxSandboxSecurityContext`], porting upstream
/// `generatePodSandboxLinuxConfig` (`pkg/kubelet/kuberuntime/kuberuntime_sandbox.go`).
///
/// Beyond the namespace options we already set, this carries the pod-level
/// security context onto the sandbox: `privileged` (if any container is),
/// `runAsUser`/`runAsGroup`, `supplementalGroups` (`fsGroup` first, then the
/// pod's `supplementalGroups`) + its policy, and `seLinuxOptions`. The sandbox
/// seccomp is forced to `RuntimeDefault` so pods can still pick least-privileged
/// seccomp at the container level (upstream issue #84623).
fn sandbox_security_context(pod: &Pod) -> v1::LinuxSandboxSecurityContext {
    use v1::security_profile::ProfileType;
    let mut sc = v1::LinuxSandboxSecurityContext {
        namespace_options: Some(namespace_options(pod)),
        privileged: pod_has_privileged_container(pod),
        seccomp: Some(v1::SecurityProfile {
            profile_type: ProfileType::RuntimeDefault as i32,
            localhost_ref: String::new(),
        }),
        ..Default::default()
    };
    if let Some(psc) = pod.spec.as_ref().and_then(|s| s.security_context.as_ref()) {
        sc.run_as_user = psc.run_as_user.map(|v| v1::Int64Value { value: v });
        sc.run_as_group = psc.run_as_group.map(|v| v1::Int64Value { value: v });
        let mut groups: Vec<i64> = Vec::new();
        if let Some(fsg) = psc.fs_group {
            groups.push(fsg);
        }
        if let Some(sg) = psc.supplemental_groups.as_ref() {
            groups.extend(sg.iter().copied());
        }
        sc.supplemental_groups = groups;
        if let Some(policy) = psc.supplemental_groups_policy.as_deref() {
            sc.supplemental_groups_policy = match policy {
                "Strict" => v1::SupplementalGroupsPolicy::Strict as i32,
                // "Merge" (default) and any unknown value.
                _ => v1::SupplementalGroupsPolicy::Merge as i32,
            };
        }
        if let Some(opts) = psc.se_linux_options.as_ref() {
            sc.selinux_options = Some(v1::SeLinuxOption {
                user: opts.user.clone().unwrap_or_default(),
                role: opts.role.clone().unwrap_or_default(),
                r#type: opts.type_.clone().unwrap_or_default(),
                level: opts.level.clone().unwrap_or_default(),
            });
        }
    }
    sc
}

/// Translate the pod into a CRI [`PodSandboxConfig`](v1::PodSandboxConfig).
///
/// `log_directory` is the kubelet-owned dir the runtime writes container logs
/// under; it must exist before `RunPodSandbox`.
pub fn sandbox_config(pod: &Pod, log_directory: &str) -> v1::PodSandboxConfig {
    // Upstream kubelet does not set a sandbox hostname for hostNetwork pods.
    // With crun, setting Hostname while the sandbox omits a private UTS
    // namespace fails OCI create with "hostname requires the UTS namespace".
    let hostname = if host_network(pod) {
        String::new()
    } else {
        truncate_hostname_for_kernel(
            pod.spec
                .as_ref()
                .and_then(|s| s.hostname.as_deref())
                .unwrap_or(&pod.metadata.name),
        )
    };

    v1::PodSandboxConfig {
        metadata: Some(v1::PodSandboxMetadata {
            name: pod.metadata.name.clone(),
            uid: pod.metadata.uid.clone(),
            namespace: namespace(pod).to_string(),
            attempt: 0,
        }),
        hostname,
        log_directory: log_directory.to_string(),
        labels: pod_labels(pod),
        port_mappings: port_mappings(pod),
        linux: Some(v1::LinuxPodSandboxConfig {
            security_context: Some(sandbox_security_context(pod)),
            sysctls: sysctls(pod),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Translate env vars, resolving each `valueFrom` source in declared order.
/// Literal values pass through; `fieldRef`/`resourceFieldRef` resolve from the
/// pod; `configMapKeyRef`/`secretKeyRef` resolve from `config_maps`/`secrets`
/// (fetched from storage by the caller). A ref whose source/key is absent
/// yields no var (matching the optional case); the caller is responsible for
/// pre-validating non-optional refs.
/// Insert or override `key` in an ordered env list (env vars are name-unique;
/// a later write replaces an earlier one in place, keeping its position).
fn upsert_env(out: &mut Vec<v1::KeyValue>, key: String, value: String) {
    match out.iter_mut().find(|kv| kv.key == key) {
        Some(kv) => kv.value = value,
        None => out.push(v1::KeyValue { key, value }),
    }
}

/// Expand `$(VAR)` references in `input` against `mapping`, a faithful port of
/// upstream `third_party/forked/golang/expansion.Expand` +
/// `MappingFuncFor`:
/// - `$(VAR)` → `mapping[VAR]`, or the verbatim `$(VAR)` if VAR is unknown;
/// - `$$` → a literal `$`;
/// - an incomplete `$(` and a `$` not starting an expression are literal.
///
/// The kubelet runs this over both env values (against the vars defined so far)
/// and container command/args (against the full container env) — see
/// `pkg/kubelet/kubelet_pods.go::makeEnvironmentVariables` and
/// `kuberuntime_container.go::expandContainerCommandAndArgs`.
pub(crate) fn expand(input: &str, mapping: &HashMap<String, String>) -> String {
    // `MappingFuncFor` semantics: an unknown var expands to the verbatim
    // `$(VAR)`. `None` from the lookup means "leave it alone".
    expand_with(input, |name| mapping.get(name).cloned())
}

/// The shared `$(VAR)` parser behind [`expand`] and [`expand_subpath_expr`].
///
/// Upstream uses one `expansion.Expand` with two different mapping funcs: the
/// command/args path leaves an unknown reference verbatim
/// (`MappingFuncFor`), while the subPath path substitutes the empty string and
/// records the key as missing (`ExpandContainerVolumeMounts`,
/// `pkg/kubelet/container/helpers.go:242-256`). Keeping one parser here means
/// the two cannot drift on `$$` escaping or incomplete-reference handling.
///
/// `lookup` returning `None` reproduces the verbatim behaviour.
fn expand_with<F>(input: &str, mut lookup: F) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    let b = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0;
    while cursor < b.len() {
        if b[cursor] == b'$' && cursor + 1 < b.len() {
            match b[cursor + 1] {
                b'$' => {
                    out.push('$');
                    cursor += 2;
                    continue;
                }
                b'(' => {
                    if let Some(rel) = b[cursor + 2..].iter().position(|&c| c == b')') {
                        let name = &input[cursor + 2..cursor + 2 + rel];
                        match lookup(name) {
                            Some(v) => out.push_str(&v),
                            // Unknown var: leave the whole `$(name)` verbatim.
                            None => out.push_str(&input[cursor..cursor + 2 + rel + 1]),
                        }
                        cursor = cursor + 2 + rel + 1;
                        continue;
                    }
                    // Incomplete reference `$(` → literal.
                    out.push_str("$(");
                    cursor += 2;
                    continue;
                }
                _ => {
                    // `$` not starting an expression: emit `$` + the next char.
                    out.push('$');
                    let ch = input[cursor + 1..].chars().next().unwrap();
                    out.push(ch);
                    cursor += 1 + ch.len_utf8();
                    continue;
                }
            }
        }
        let ch = input[cursor..].chars().next().unwrap();
        out.push(ch);
        cursor += ch.len_utf8();
    }
    out
}

/// Expand `$(VAR)` in every element of a command/args vector against `env`.
fn expand_all(items: Vec<String>, env: &HashMap<String, String>) -> Vec<String> {
    items.iter().map(|s| expand(s, env)).collect()
}

/// Expand a `volumeMounts[].subPathExpr` against the container env.
///
/// Port of upstream `kubecontainer.ExpandContainerVolumeMounts`
/// (`pkg/kubelet/container/helpers.go:242-256`). Unlike command/args
/// expansion, a reference whose key is **missing or empty** is an error, not a
/// verbatim passthrough: the expanded value would otherwise silently become a
/// different directory. Missing keys are reported sorted, matching upstream's
/// `sets.List` ordering, so the message is deterministic.
pub(crate) fn expand_subpath_expr(
    expr: &str,
    env: &HashMap<String, String>,
) -> Result<String, String> {
    let mut missing: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let expanded = expand_with(expr, |name| {
        match env.get(name) {
            Some(v) if !v.is_empty() => Some(v.clone()),
            // Missing *or* empty: upstream records the key and substitutes "".
            _ => {
                missing.insert(name.to_string());
                Some(String::new())
            }
        }
    });
    if !missing.is_empty() {
        let keys: Vec<&str> = missing.iter().map(String::as_str).collect();
        return Err(format!("missing value for {}", keys.join(", ")));
    }
    Ok(expanded)
}

/// Guard an already-resolved subPath, porting the checks upstream runs in
/// `makeMounts` (`pkg/kubelet/kubelet_pods.go:316-325`) in this order:
///
/// 1. absolute → `error SubPath `<p>` must not be an absolute path`
/// 2. contains `..` → `unable to provision SubPath `<p>`: must not contain '..'`
///    (the inner wording is `volumevalidation.ValidatePathNoBacksteps`,
///    `pkg/volume/validation/pv_validation.go:62-71`)
///
/// The strings are part of the contract: the conformance specs assert the
/// container fails, and the message is what an operator sees.
fn validate_resolved_sub_path(sub_path: &str) -> Result<(), String> {
    if Path::new(sub_path).is_absolute() {
        return Err(format!(
            "error SubPath `{sub_path}` must not be an absolute path"
        ));
    }
    if sub_path.split('/').any(|part| part == "..") {
        return Err(format!(
            "unable to provision SubPath `{sub_path}`: must not contain '..'"
        ));
    }
    Ok(())
}

/// Resolve the effective subPath for one volume mount: `subPath` verbatim, or
/// `subPathExpr` expanded against the container env, then guarded.
///
/// Mirrors the head of upstream's `makeMounts` loop
/// (`pkg/kubelet/kubelet_pods.go:308-325`). Returns `None` when the mount has
/// no subPath at all.
pub(crate) fn resolve_sub_path(
    vm: &rusternetes_common::resources::VolumeMount,
    env: &HashMap<String, String>,
) -> Result<Option<String>, String> {
    let mut sub_path = vm
        .sub_path
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    if let Some(expr) = vm.sub_path_expr.as_deref().filter(|s| !s.is_empty()) {
        sub_path = Some(expand_subpath_expr(expr, env)?);
    }

    match sub_path.as_deref().filter(|s| !s.is_empty()) {
        Some(p) => {
            validate_resolved_sub_path(p)?;
            Ok(Some(p.to_string()))
        }
        None => Ok(None),
    }
}

fn env_vars(
    pod: &Pod,
    container: &Container,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
    node_allocatable: Option<&HashMap<String, String>>,
) -> Vec<v1::KeyValue> {
    let mut out: Vec<v1::KeyValue> = Vec::new();
    // Mirror of `out` for `$(VAR)` lookups: upstream expands each literal env
    // value against the vars defined so far (`makeEnvironmentVariables`).
    let mut seen: HashMap<String, String> = HashMap::new();

    // 1. envFrom: bulk-expand referenced ConfigMaps/Secrets (declared order),
    //    each key optionally prefixed. A later source overrides an earlier one.
    if let Some(sources) = container.env_from.as_ref() {
        for src in sources {
            let prefix = src.prefix.as_deref().unwrap_or("");
            if let Some(cmr) = src.config_map_ref.as_ref() {
                if let Some(data) = config_maps.get(&cmr.name).and_then(|cm| cm.data.as_ref()) {
                    for (k, v) in data {
                        let key = format!("{prefix}{k}");
                        seen.insert(key.clone(), v.clone());
                        upsert_env(&mut out, key, v.clone());
                    }
                }
            }
            if let Some(skr) = src.secret_ref.as_ref() {
                if let Some(data) = secrets.get(&skr.name).and_then(|s| s.data.as_ref()) {
                    for (k, v) in data {
                        // Secret data is decoded bytes; env values are the UTF-8
                        // interpretation (upstream casts []byte→string).
                        let key = format!("{prefix}{k}");
                        let val = String::from_utf8_lossy(v).into_owned();
                        seen.insert(key.clone(), val.clone());
                        upsert_env(&mut out, key, val);
                    }
                }
            }
        }
    }

    // 2. Individual env entries (declared order); each overrides any
    //    envFrom-supplied var of the same name.
    if let Some(env) = container.env.as_ref() {
        for e in env {
            let value = if let Some(v) = e.value.as_ref() {
                // Literal values get `$(VAR)` expanded against vars defined so
                // far (upstream `makeEnvironmentVariables`); `valueFrom` does not.
                Some(expand(v, &seen))
            } else if let Some(src) = e.value_from.as_ref() {
                if let Some(fr) = src.field_ref.as_ref() {
                    pod_field_value(pod, &fr.field_path)
                } else if let Some(rfr) = src.resource_field_ref.as_ref() {
                    container_resource_value(container, rfr, node_allocatable)
                } else if let Some(cmr) = src.config_map_key_ref.as_ref() {
                    config_maps
                        .get(&cmr.name)
                        .and_then(|cm| cm.data.as_ref())
                        .and_then(|d| d.get(&cmr.key).cloned())
                } else if let Some(skr) = src.secret_key_ref.as_ref() {
                    // Secret data is the already-decoded bytes; env values are
                    // the UTF-8 interpretation (upstream casts []byte→string).
                    secrets
                        .get(&skr.name)
                        .and_then(|s| s.data.as_ref())
                        .and_then(|d| d.get(&skr.key))
                        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(v) = value {
                seen.insert(e.name.clone(), v.clone());
                upsert_env(&mut out, e.name.clone(), v);
            }
        }
    }

    out
}

/// Resolve a downward-API pod field path (`fieldRef`) to a string value. Only
/// the fields known at container-create time are returned; `None` otherwise.
pub fn pod_field_value(pod: &Pod, field_path: &str) -> Option<String> {
    match field_path {
        "metadata.name" => Some(pod.metadata.name.clone()),
        "metadata.namespace" => Some(namespace(pod).to_string()),
        "metadata.uid" => Some(pod.metadata.uid.clone()),
        "spec.nodeName" => pod.spec.as_ref().and_then(|s| s.node_name.clone()),
        "spec.serviceAccountName" => pod
            .spec
            .as_ref()
            .and_then(|s| s.service_account_name.clone()),
        // podIP is the pod's own address; hostIP is the NODE's address. These
        // are distinct — upstream returns `podIP` vs `hostIPs[0]` respectively
        // (pkg/kubelet/kubelet_pods.go podFieldSelectorRuntimeValue). Returning
        // one for the other broke pods that read status.hostIP via fieldRef.
        "status.podIP" => pod.status.as_ref().and_then(|st| st.pod_ip.clone()),
        "status.hostIP" => pod.status.as_ref().and_then(|st| st.host_ip.clone()),
        // Dual-stack plural forms: comma-joined list (matches upstream).
        "status.podIPs" => pod.status.as_ref().and_then(|st| {
            st.pod_i_ps.as_ref().map(|ips| {
                ips.iter()
                    .map(|p| p.ip.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }),
        "status.hostIPs" => pod.status.as_ref().and_then(|st| {
            st.host_i_ps.as_ref().map(|ips| {
                ips.iter()
                    .map(|h| h.ip.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }),
        "spec.restartPolicy" => pod.spec.as_ref().and_then(|s| s.restart_policy.clone()),
        "spec.schedulerName" => pod.spec.as_ref().and_then(|s| s.scheduler_name.clone()),
        "status.phase" => pod
            .status
            .as_ref()
            .and_then(|st| st.phase.as_ref())
            .map(phase_str),
        // Subscript forms `metadata.labels['key']` / `metadata.annotations['key']`.
        _ => field_path
            .strip_prefix("metadata.labels[")
            .map(|k| (pod.metadata.labels.as_ref(), k))
            .or_else(|| {
                field_path
                    .strip_prefix("metadata.annotations[")
                    .map(|k| (pod.metadata.annotations.as_ref(), k))
            })
            .and_then(|(map, rest)| {
                let key = rest.strip_suffix(']')?.trim_matches(['\'', '"']);
                map?.get(key).cloned()
            }),
    }
}

/// Render a [`Phase`] to its Kubernetes string form (`status.phase` fieldRef).
fn phase_str(phase: &rusternetes_common::types::Phase) -> String {
    use rusternetes_common::types::Phase;
    match phase {
        Phase::Pending => "Pending",
        Phase::Running => "Running",
        Phase::Succeeded => "Succeeded",
        Phase::Failed => "Failed",
        Phase::Unknown => "Unknown",
        Phase::Active => "Active",
        Phase::Terminating => "Terminating",
    }
    .to_string()
}

/// Resolve an env var's `resourceFieldRef` (`limits.cpu` / `requests.memory`,
/// etc.) to its numeric value as a decimal string.
///
/// The arithmetic lives in [`crate::downward_api`] — the port of upstream's
/// `ExtractContainerResourceValue` chain — because the downwardAPI *volume*
/// path resolves the very same selectors and the two must not drift. This copy
/// used to floor byte quantities where upstream ceils, so the volume file and
/// the env var could disagree on the same `resourceFieldRef`.
///
/// `None` means "omit the variable": upstream instead fails container creation
/// with `CreateContainerConfigError` on an unsupported resource
/// (`pkg/api/v1/resource/helpers.go:141`). Dropping the var is the pre-existing
/// (more forgiving) behaviour and is kept here; the api-server's
/// `validateContainerResourceFieldSelector` is what actually rejects these.
pub fn container_resource_value(
    container: &Container,
    sel: &rusternetes_common::resources::ResourceFieldSelector,
    node_allocatable: Option<&HashMap<String, String>>,
) -> Option<String> {
    crate::downward_api::resolve_container_resource_in(container, sel, node_allocatable).ok()
}

/// Verify every *non-optional* `configMapKeyRef`/`secretKeyRef` env var in the
/// container resolves against the pre-fetched `config_maps`/`secrets`. Returns
/// the upstream error wording (`makeEnvironmentVariables`) on the first
/// unresolved ref; the caller fails container creation, matching upstream's
/// `CreateContainerConfigError` rather than silently dropping the var. Optional
/// refs are skipped (and may be legitimately absent).
pub fn validate_env_key_refs(
    pod: &Pod,
    container: &Container,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
) -> Result<(), String> {
    let ns = namespace(pod);
    for e in container.env.iter().flatten() {
        let Some(src) = e.value_from.as_ref() else {
            continue;
        };
        if let Some(cmr) = src.config_map_key_ref.as_ref() {
            if cmr.optional.unwrap_or(false) {
                continue;
            }
            match config_maps.get(&cmr.name) {
                None => return Err(format!("couldn't get ConfigMap {ns}/{}", cmr.name)),
                Some(cm) => {
                    if !cm.data.as_ref().is_some_and(|d| d.contains_key(&cmr.key)) {
                        return Err(format!(
                            "couldn't find key {} in ConfigMap {ns}/{}",
                            cmr.key, cmr.name
                        ));
                    }
                }
            }
        }
        if let Some(skr) = src.secret_key_ref.as_ref() {
            if skr.optional.unwrap_or(false) {
                continue;
            }
            match secrets.get(&skr.name) {
                None => return Err(format!("couldn't get Secret {ns}/{}", skr.name)),
                Some(s) => {
                    if !s.data.as_ref().is_some_and(|d| d.contains_key(&skr.key)) {
                        return Err(format!(
                            "couldn't find key {} in Secret {ns}/{}",
                            skr.key, skr.name
                        ));
                    }
                }
            }
        }
    }

    // `envFrom`: a non-optional configMapRef/secretRef that didn't resolve fails
    // container creation too, mirroring upstream `makeEnvironmentVariables`
    // (a missing required bulk source is `CreateContainerConfigError`, not a
    // silent skip). Optional sources are tolerated.
    for ef in container.env_from.iter().flatten() {
        if let Some(cmr) = ef.config_map_ref.as_ref() {
            if !cmr.optional.unwrap_or(false) && !config_maps.contains_key(&cmr.name) {
                return Err(format!("couldn't get ConfigMap {ns}/{}", cmr.name));
            }
        }
        if let Some(skr) = ef.secret_ref.as_ref() {
            if !skr.optional.unwrap_or(false) && !secrets.contains_key(&skr.name) {
                return Err(format!("couldn't get Secret {ns}/{}", skr.name));
            }
        }
    }

    Ok(())
}

/// Translate volume mounts into CRI mounts using a resolved volume-name →
/// host-path map (volume provisioning is runtime-agnostic and done earlier).
/// Mounts whose volume is absent from the map are skipped.
/// Map `volumeMount.mountPropagation` to the CRI propagation mode, porting
/// upstream `translateMountPropagation` (`pkg/kubelet/kubelet_pods.go`): unset,
/// `None`, and any unknown value → `Private` (the default);
/// `HostToContainer`/`Bidirectional` map straight through.
fn translate_mount_propagation(mode: Option<&str>) -> i32 {
    use v1::MountPropagation;
    match mode {
        Some("HostToContainer") => MountPropagation::PropagationHostToContainer as i32,
        Some("Bidirectional") => MountPropagation::PropagationBidirectional as i32,
        _ => MountPropagation::PropagationPrivate as i32,
    }
}

/// Port of upstream `resolveRecursiveReadOnly`
/// (`pkg/kubelet/kubelet_pods.go:2831-2853`): the quaternary core-API
/// `recursiveReadOnly` becomes the CRI `Mount.recursive_read_only` boolean.
/// Error strings are upstream's verbatim.
pub fn resolve_recursive_read_only(
    vm: &rusternetes_common::resources::pod::VolumeMount,
    runtime_supports_rro: bool,
) -> Result<bool, String> {
    let mode = match vm.recursive_read_only.as_deref() {
        None | Some("Disabled") => return Ok(false),
        Some(m) => m,
    };
    if !vm.read_only.unwrap_or(false) {
        return Err(format!(
            "volume {:?} requested recursive read-only mode, but it is not read-only",
            vm.name
        ));
    }
    if let Some(p) = vm.mount_propagation.as_deref() {
        if p != "None" {
            return Err(format!(
                "volume {:?} requested recursive read-only mode, but it is not compatible with propagation {:?}",
                vm.name, p
            ));
        }
    }
    match mode {
        "IfPossible" => Ok(runtime_supports_rro),
        "Enabled" if runtime_supports_rro => Ok(true),
        "Enabled" => Err(format!(
            "volume {:?} requested recursive read-only mode, but it is not supported by the runtime",
            vm.name
        )),
        other => Err(format!("unknown recursive read-only mode {other:?}")),
    }
}

/// Port of `runtimeHandlerSupportsRecursiveReadOnlyMounts`
/// (`pkg/kubelet/kubelet_pods.go:2815-2828`), fed by the CRI `Status`
/// `runtime_handlers` (`pkg/kubelet/kuberuntime/helpers.go:240-252`).
pub fn runtime_handler_supports_rro(name: &str, handlers: &[v1::RuntimeHandler]) -> bool {
    handlers
        .iter()
        .find(|h| h.name == name)
        .and_then(|h| h.features.as_ref())
        .map(|f| f.recursive_read_only_mounts)
        .unwrap_or(false)
}

/// Build the CRI mounts for a container.
///
/// `env` is the container's fully-resolved environment: upstream expands
/// `subPathExpr` against exactly this map inside `makeMounts`
/// (`pkg/kubelet/kubelet_pods.go:308-325`), so the two must agree. Fallible for
/// the same reason upstream's `makeMounts` is: an unresolvable or unsafe
/// subPath must stop container creation, not silently mount the volume root.
pub fn mounts(
    container: &Container,
    host_paths: &HashMap<String, String>,
    env: &HashMap<String, String>,
    attrs: &HashMap<String, MountAttrs>,
    supports_rro: bool,
    image_volumes: &HashMap<String, v1::ImageSpec>,
) -> Result<Vec<v1::Mount>, String> {
    mounts_with(
        container,
        host_paths,
        env,
        attrs,
        crate::go_selinux::get_enabled(),
        supports_rro,
        image_volumes,
    )
}

/// The per-volume result of upstream `makeMounts`' read of
/// `vol.Mounter.GetAttributes()` (`pkg/kubelet/kubelet_pods.go:296`, `:392`):
/// `read_only` is `mustMountRO`, `selinux_relabel` is the one-time
/// `relabelVolume` decision. Built by `VolumeManager::mount_attributes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MountAttrs {
    pub read_only: bool,
    pub selinux_relabel: bool,
}

/// [`mounts`] with the SELinux gate (`selinux.GetEnabled()`) injected so tests
/// can exercise both sides of it.
///
/// `image_volumes` is `makeMounts`' `imageVolumes` argument
/// (`kubelet_pods.go:277`): the image volumes the kubelet pulled for this
/// container, by volume name.
pub fn mounts_with(
    container: &Container,
    host_paths: &HashMap<String, String>,
    env: &HashMap<String, String>,
    attrs: &HashMap<String, MountAttrs>,
    selinux_enabled: bool,
    supports_rro: bool,
    image_volumes: &HashMap<String, v1::ImageSpec>,
) -> Result<Vec<v1::Mount>, String> {
    let Some(vms) = container.volume_mounts.as_ref() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    // Volumes already relabelled by an earlier mount in this container:
    // upstream sets `vol.SELinuxLabeled = true` on the first use so only that
    // mount carries `SELinuxRelabel` (`kubelet_pods.go:296-300`).
    let mut relabelled: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for vm in vms.iter() {
        // `if imageVolumes != nil && ImageVolume enabled { image =
        // imageVolumes[mount.Name] }` (`kubelet_pods.go:327-330`).
        let image = if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::ImageVolume,
        ) {
            image_volumes.get(&vm.name)
        } else {
            None
        };
        // A mount whose volume did not resolve to a host path is skipped, as
        // before — the volume manager reports that separately. An image
        // mount has no host path (`image == nil` guards `GetPath`,
        // `kubelet_pods.go:332-335`).
        let Some(host) = host_paths.get(&vm.name).filter(|_| image.is_none()) else {
            if let Some(image) = image {
                // The subPath guards (`kubelet_pods.go:308-325`) run before
                // the image arm; `imageSubPath = subPath` (`:329`).
                let sub_path = resolve_sub_path(vm, env)?;
                let rro = resolve_recursive_read_only(vm, supports_rro)?;
                let a = attrs.get(&vm.name).copied().unwrap_or_default();
                let relabel_volume = a.selinux_relabel && relabelled.insert(vm.name.as_str());
                out.push(v1::Mount {
                    container_path: vm.mount_path.clone(),
                    host_path: String::new(),
                    readonly: vm.read_only.unwrap_or(false) || a.read_only,
                    selinux_relabel: crate::go_selinux::relabel_if_enabled(
                        relabel_volume,
                        selinux_enabled,
                    ),
                    propagation: translate_mount_propagation(vm.mount_propagation.as_deref()),
                    recursive_read_only: rro,
                    image: Some(image.clone()),
                    image_sub_path: sub_path.unwrap_or_default(),
                    ..Default::default()
                });
            }
            continue;
        };
        let sub_path = resolve_sub_path(vm, env)?;
        // `resolveRecursiveReadOnly` (`kubelet_pods.go:394`); its error stops
        // container creation, as in `makeMounts`.
        let rro = resolve_recursive_read_only(vm, supports_rro)?;
        let a = attrs.get(&vm.name).copied().unwrap_or_default();
        let relabel_volume = a.selinux_relabel && relabelled.insert(vm.name.as_str());
        out.push(v1::Mount {
            container_path: vm.mount_path.clone(),
            host_path: match sub_path.as_deref() {
                Some(sub_path) => Path::new(host)
                    .join(sub_path)
                    .to_string_lossy()
                    .into_owned(),
                None => host.clone(),
            },
            // `ReadOnly: mount.ReadOnly || mustMountRO` (`kubelet_pods.go:417`).
            readonly: vm.read_only.unwrap_or(false) || a.read_only,
            // `v.SELinuxRelabel && selinux.GetEnabled()`
            // (`kuberuntime_container.go:484`).
            selinux_relabel: crate::go_selinux::relabel_if_enabled(relabel_volume, selinux_enabled),
            propagation: translate_mount_propagation(vm.mount_propagation.as_deref()),
            // `RecursiveReadOnly: rro` (`kubelet_pods.go:409`).
            recursive_read_only: rro,
            ..Default::default()
        });
    }
    Ok(out)
}

/// Parse a Kubernetes CPU quantity into millicores (`"500m"` → 500, `"2"` →
/// 2000). Returns `None` for input upstream `ParseQuantity` rejects.
pub fn parse_cpu_millicores(q: &str) -> Option<i64> {
    parse_resource_value(q, "cpu").ok()
}

/// Parse a Kubernetes memory quantity into bytes (`"128Mi"`, `"1Gi"`,
/// `"1000000"`). The local suffix table this replaced was missing `Pi`/`Ei` and
/// the `P`/`E` decimal-SI pair, so those fell through to a bare `i64` parse and
/// yielded `None` — dropping the downward-API env var entirely.
pub fn parse_memory_bytes(q: &str) -> Option<i64> {
    parse_resource_value(q, "memory").ok()
}

/// Build CRI linux resources from a container's limits/requests. CPU limit →
/// cfs quota (100ms period); CPU request → shares; memory limit → byte cap.
// CFS scheduling constants (upstream `pkg/kubelet/cm/helpers_linux.go:41-59`).
// `super::status` reads these back the other way — milliCPU out of the shares /
// quota the runtime reports — so both directions must agree on one definition.
pub const MIN_SHARES: i64 = 2;
const MAX_SHARES: i64 = 262_144;
pub const SHARES_PER_CPU: i64 = 1024;
pub const MILLI_CPU_TO_CPU: i64 = 1000;
const QUOTA_PERIOD: i64 = 100_000;
pub const MIN_QUOTA_PERIOD: i64 = 1000;
/// From the inverse of the conversion in `MilliCPUToQuota`:
/// `MinQuotaPeriod * MilliCPUToCPU / QuotaPeriod`.
pub const MIN_MILLI_CPU_LIMIT: i64 = MIN_QUOTA_PERIOD * MILLI_CPU_TO_CPU / QUOTA_PERIOD;

/// Port of upstream `cm.MilliCPUToShares`: convert milliCPU to CFS shares,
/// clamped to `[MinShares, MaxShares]`. 0 milliCPU → `MinShares` (the kernel
/// default for "unset").
fn milli_cpu_to_shares(milli_cpu: i64) -> i64 {
    if milli_cpu == 0 {
        return MIN_SHARES;
    }
    ((milli_cpu * SHARES_PER_CPU) / MILLI_CPU_TO_CPU).clamp(MIN_SHARES, MAX_SHARES)
}

/// Port of upstream `cm.MilliCPUToQuota`: convert a milliCPU limit to a CFS
/// quota over `period` microseconds, with a 1ms (`MinQuotaPeriod`) floor.
fn milli_cpu_to_quota(milli_cpu: i64, period: i64) -> i64 {
    if milli_cpu == 0 {
        return 0;
    }
    ((milli_cpu * period) / MILLI_CPU_TO_CPU).max(MIN_QUOTA_PERIOD)
}

/// Port of upstream `calculateLinuxResources`
/// (`pkg/kubelet/kuberuntime/kuberuntime_container_linux.go`).
///
/// `cpu_shares` is **always** set — from the CPU request, else the CPU limit
/// (the api-server defaults request→limit, but controller-created pods written
/// straight to storage may lack it), else `MinShares`. A CPU limit additionally
/// sets the CFS quota/period; a memory limit sets `memory_limit_in_bytes`.
/// Also used by the in-place-resize path (`super::runtime`), so a resize sends
/// byte-identical cgroup values to what the container was created with. A
/// hand-rolled second copy of this math skipped the `MinQuotaPeriod` floor and
/// had the kernel reject sub-10m CPU limits.
pub fn linux_resources(container: &Container) -> Option<v1::LinuxContainerResources> {
    let req = container.resources.as_ref();
    let cpu_of = |which: fn(
        &rusternetes_common::types::ResourceRequirements,
    ) -> &Option<HashMap<String, String>>| {
        req.and_then(|r| which(r).as_ref())
            .and_then(|m| m.get("cpu"))
            .and_then(|q| parse_cpu_millicores(q))
    };
    let cpu_request = cpu_of(|r| &r.requests);
    let cpu_limit = cpu_of(|r| &r.limits);
    let mem_limit = req
        .and_then(|r| r.limits.as_ref())
        .and_then(|m| m.get("memory"))
        .and_then(|q| parse_memory_bytes(q));

    let mut r = v1::LinuxContainerResources {
        // request, else limit, else 0 → MinShares.
        cpu_shares: milli_cpu_to_shares(cpu_request.or(cpu_limit).unwrap_or(0)),
        ..Default::default()
    };
    if let Some(limit) = cpu_limit {
        r.cpu_period = QUOTA_PERIOD;
        r.cpu_quota = milli_cpu_to_quota(limit, QUOTA_PERIOD);
    }
    if let Some(mem) = mem_limit {
        r.memory_limit_in_bytes = mem;
    }
    Some(r)
}

/// Resolve the effective container seccomp profile to a CRI [`SecurityProfile`].
///
/// Ports upstream `getSeccompProfile`/`fieldSeccompProfile`
/// (`pkg/kubelet/kuberuntime/helpers.go`): the container's
/// `securityContext.seccompProfile` wins, falling back to the pod's. `None` when
/// neither is set (the runtime then applies its own default; upstream's
/// `fallbackToRuntimeDefault` is off by default, so we don't force RuntimeDefault).
///
/// Note: for `Localhost`, upstream sets `localhost_ref` to
/// `join(seccompProfileRoot, localhostProfile)`. We pass the `localhostProfile`
/// through as-is (the translate layer is path-pure and has no kubelet root);
/// RuntimeDefault/Unconfined — the common cases — need no path.
fn seccomp_security_profile(pod: &Pod, container: &Container) -> Option<v1::SecurityProfile> {
    use rusternetes_common::resources::pod::SeccompProfile;
    let scmp: &SeccompProfile = container
        .security_context
        .as_ref()
        .and_then(|c| c.seccomp_profile.as_ref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|p| p.seccomp_profile.as_ref())
        })?;
    use v1::security_profile::ProfileType;
    let profile = match scmp.r#type.as_str() {
        "RuntimeDefault" => v1::SecurityProfile {
            profile_type: ProfileType::RuntimeDefault as i32,
            localhost_ref: String::new(),
        },
        "Localhost" => v1::SecurityProfile {
            profile_type: ProfileType::Localhost as i32,
            localhost_ref: scmp.localhost_profile.clone().unwrap_or_default(),
        },
        // "Unconfined" and any unknown value → Unconfined (upstream default arm).
        _ => v1::SecurityProfile {
            profile_type: ProfileType::Unconfined as i32,
            localhost_ref: String::new(),
        },
    };
    Some(profile)
}

/// Resolve the effective AppArmor profile to a CRI [`SecurityProfile`].
///
/// Ports upstream `getAppArmorProfile`/`apparmor.GetProfile`
/// (`pkg/kubelet/kuberuntime/helpers.go`): container `appArmorProfile` wins over
/// the pod's; `RuntimeDefault`/`Unconfined`/`Localhost` map to the CRI profile
/// (Localhost carries `localhost_ref`). We set only the modern `apparmor`
/// `SecurityProfile`, not the deprecated `apparmor_profile` string (that exists
/// solely for runtimes older than the CRI-v1 containerd we target).
fn apparmor_security_profile(pod: &Pod, container: &Container) -> Option<v1::SecurityProfile> {
    use rusternetes_common::resources::pod::AppArmorProfile;
    let profile: &AppArmorProfile = container
        .security_context
        .as_ref()
        .and_then(|c| c.app_armor_profile.as_ref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|p| p.app_armor_profile.as_ref())
        })?;
    use v1::security_profile::ProfileType;
    let profile = match profile.type_.as_str() {
        "RuntimeDefault" => v1::SecurityProfile {
            profile_type: ProfileType::RuntimeDefault as i32,
            localhost_ref: String::new(),
        },
        "Localhost" => v1::SecurityProfile {
            profile_type: ProfileType::Localhost as i32,
            localhost_ref: profile.localhost_profile.clone().unwrap_or_default(),
        },
        // "Unconfined" and any unknown value → Unconfined.
        _ => v1::SecurityProfile {
            profile_type: ProfileType::Unconfined as i32,
            localhost_ref: String::new(),
        },
    };
    Some(profile)
}

/// Effective SELinux options for the container (container `seLinuxOptions` wins
/// over the pod's), mapped to the CRI [`SeLinuxOption`] (upstream
/// `convertToRuntimeSELinuxOption`).
fn selinux_options(pod: &Pod, container: &Container) -> Option<v1::SeLinuxOption> {
    let opts = container
        .security_context
        .as_ref()
        .and_then(|c| c.se_linux_options.as_ref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|p| p.se_linux_options.as_ref())
        })?;
    Some(v1::SeLinuxOption {
        user: opts.user.clone().unwrap_or_default(),
        role: opts.role.clone().unwrap_or_default(),
        r#type: opts.type_.clone().unwrap_or_default(),
        level: opts.level.clone().unwrap_or_default(),
    })
}

/// Upstream `securitycontext.defaultMaskedPaths` (pkg/securitycontext/util.go).
/// Static base only; the per-CPU `thermal_throttle` paths upstream appends
/// after a host `os.Stat` are added by `host_policy::default_masked_paths`.
pub(crate) const DEFAULT_MASKED_PATHS: &[&str] = &[
    "/proc/asound",
    "/proc/acpi",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/sys/firmware",
    "/sys/devices/virtual/powercap",
];

/// Upstream `securitycontext.defaultReadonlyPaths`.
pub const DEFAULT_READONLY_PATHS: &[&str] = &[
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
];

/// Port of upstream `securitycontext.ConvertToRuntimeMaskedPaths`: the default
/// masked paths unless `procMount: Unmasked`, in which case nothing is masked.
///
/// The kubelet must send these explicitly: containerd's CRI resets masked paths
/// to empty and only re-applies what the security context carries, so an unset
/// field leaves `/proc` unmasked.
fn convert_masked_paths(proc_mount: Option<&str>) -> Vec<String> {
    if proc_mount == Some("Unmasked") {
        return Vec::new();
    }
    super::host_policy::default_masked_paths().to_vec()
}

/// Port of upstream `securitycontext.ConvertToRuntimeReadonlyPaths`.
fn convert_readonly_paths(proc_mount: Option<&str>) -> Vec<String> {
    if proc_mount == Some("Unmasked") {
        return Vec::new();
    }
    DEFAULT_READONLY_PATHS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

pub fn linux_security_context(
    pod: &Pod,
    container: &Container,
) -> Option<v1::LinuxContainerSecurityContext> {
    // seccomp/apparmor/selinux may come from the pod even when the container has
    // no security context of its own.
    let seccomp = seccomp_security_profile(pod, container);
    let apparmor = apparmor_security_profile(pod, container);
    let selinux_options = selinux_options(pod, container);
    let sc = container.security_context.as_ref();
    // runAsUser/runAsGroup follow the effective-security-context precedence from
    // upstream `securitycontext.DetermineEffectiveSecurityContext`: the pod-level
    // `securityContext.runAsUser`/`runAsGroup` are inherited by each container
    // that does not set its own, and a container-level value overrides the pod's.
    // Without the pod fallback, a pod that only sets these at pod level (e.g. the
    // "RunAsUser And RunAsGroup" conformance spec) would run every container as
    // the image default UID/GID instead of the requested one.
    let pod_sc = pod.spec.as_ref().and_then(|s| s.security_context.as_ref());
    let run_as_user = sc
        .and_then(|s| s.run_as_user)
        .or_else(|| pod_sc.and_then(|p| p.run_as_user));
    let run_as_group = sc
        .and_then(|s| s.run_as_group)
        .or_else(|| pod_sc.and_then(|p| p.run_as_group));
    // Always build a security context: the masked/readonly proc paths must be
    // sent for every container (upstream sets them unconditionally), so there is
    // no early-out.
    // Translate requested Linux capabilities. Names are passed through verbatim
    // (e.g. "NET_ADMIN"), matching upstream — the kubelet does not add the
    // "CAP_" prefix; the runtime (containerd) does when building the OCI spec.
    // Without this, NET_ADMIN/NET_RAW never reach the container and capability-
    // dependent workloads fail, e.g. flannel's vxlan link creation returns
    // netlink EPERM ("Operation not permitted") and never writes subnet.env.
    let capabilities = sc
        .and_then(|sc| sc.capabilities.as_ref())
        .map(|caps| v1::Capability {
            add_capabilities: caps.add.clone().unwrap_or_default(),
            drop_capabilities: caps.drop.clone().unwrap_or_default(),
            add_ambient_capabilities: Vec::new(),
        });
    // Supplemental groups for the workload process, from the POD security
    // context. In containerd the workload container is a separate OCI container
    // from the sandbox pause container, so the process's supplementary groups
    // come from THIS container SC, not the sandbox SC. Upstream
    // `determineEffectiveSecurityContext` (pkg/kubelet/kuberuntime/
    // security_context.go): append `fsGroup` first, then the pod's
    // `supplementalGroups`, then set the policy. Without fsGroup here a non-root
    // pod cannot read a mode-0440 secret/projected volume chowned to `:fsGroup`.
    let mut supplemental_groups: Vec<i64> = Vec::new();
    let mut supplemental_groups_policy = 0;
    if let Some(psc) = pod_sc {
        if let Some(fsg) = psc.fs_group {
            supplemental_groups.push(fsg);
        }
        if let Some(sg) = psc.supplemental_groups.as_ref() {
            supplemental_groups.extend(sg.iter().copied());
        }
        if let Some(policy) = psc.supplemental_groups_policy.as_deref() {
            supplemental_groups_policy = match policy {
                "Strict" => v1::SupplementalGroupsPolicy::Strict as i32,
                _ => v1::SupplementalGroupsPolicy::Merge as i32,
            };
        }
    }
    Some(v1::LinuxContainerSecurityContext {
        privileged: sc.and_then(|s| s.privileged).unwrap_or(false),
        capabilities,
        run_as_user: run_as_user.map(|v| v1::Int64Value { value: v }),
        run_as_group: run_as_group.map(|v| v1::Int64Value { value: v }),
        readonly_rootfs: sc
            .and_then(|s| s.read_only_root_filesystem)
            .unwrap_or(false),
        // `allowPrivilegeEscalation: false` → `no_new_privs`, blocking setuid/
        // file-capability escalation in the container. Upstream
        // `securitycontext.AddNoNewPrivileges`: true *only* when the field is
        // explicitly false (unset/true → false). Without this a pod that set
        // `allowPrivilegeEscalation: false` could still escalate.
        no_new_privs: sc.and_then(|s| s.allow_privilege_escalation) == Some(false),
        seccomp,
        apparmor,
        selinux_options,
        // procMount → masked/readonly proc paths (upstream ConvertToRuntime*Paths).
        masked_paths: convert_masked_paths(sc.and_then(|s| s.proc_mount.as_deref())),
        readonly_paths: convert_readonly_paths(sc.and_then(|s| s.proc_mount.as_deref())),
        supplemental_groups,
        supplemental_groups_policy,
        ..Default::default()
    })
}

/// Reduce volume-derived GIDs (read from the `pv.beta.kubernetes.io/gid`
/// annotation on the PVs a pod mounts) to the "extra" supplemental groups to add
/// to the pod's security contexts. Mirrors upstream `getExtraSupplementalGID`
/// (pkg/kubelet/volumemanager/volume_manager.go): de-duplicate, and drop any GID
/// already present in the pod's `securityContext.supplementalGroups` (fsGroup is
/// intentionally NOT consulted, matching upstream). Order of the input is
/// preserved for the survivors.
pub fn extra_supplemental_gids(volume_gids: &[i64], pod: &Pod) -> Vec<i64> {
    let existing: std::collections::HashSet<i64> = pod
        .spec
        .as_ref()
        .and_then(|s| s.security_context.as_ref())
        .and_then(|sc| sc.supplemental_groups.as_ref())
        .map(|g| g.iter().copied().collect())
        .unwrap_or_default();
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for &gid in volume_gids {
        if existing.contains(&gid) {
            continue;
        }
        if seen.insert(gid) {
            out.push(gid);
        }
    }
    out
}

/// Translate a single container into a CRI [`ContainerConfig`](v1::ContainerConfig).
///
/// `image_ref` is the canonical reference returned by `PullImage`. `host_paths`
/// maps volume names to their resolved host paths for mount translation.
/// `config_maps`/`secrets` are the ConfigMap/Secret objects (by name) the
/// kubelet pre-fetched from storage so `configMapKeyRef`/`secretKeyRef` env can
/// be resolved here without storage access.
pub fn container_config(
    pod: &Pod,
    container: &Container,
    image_ref: &str,
    host_paths: &HashMap<String, String>,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
) -> Result<v1::ContainerConfig, String> {
    container_config_with_allocatable(
        pod,
        container,
        image_ref,
        host_paths,
        config_maps,
        secrets,
        None,
    )
}

/// As [`container_config`], but `node_allocatable` lets unset cpu/memory/
/// ephemeral-storage resourceFieldRef LIMITS default to the node's allocatable
/// (upstream `MergeContainerResourceLimits`). The kubelet passes its node
/// allocatable here; the no-allocatable wrapper above keeps `None`.
#[allow(clippy::too_many_arguments)]
pub fn container_config_with_allocatable(
    pod: &Pod,
    container: &Container,
    image_ref: &str,
    host_paths: &HashMap<String, String>,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
    node_allocatable: Option<&HashMap<String, String>>,
) -> Result<v1::ContainerConfig, String> {
    container_config_with_mounts(
        pod,
        container,
        image_ref,
        host_paths,
        config_maps,
        secrets,
        node_allocatable,
        &HashMap::new(),
        false,
        &HashMap::new(),
    )
}

/// As [`container_config_with_allocatable`], plus the per-volume mounter
/// attributes (`mount_attrs`, keyed by volume name) `makeMounts` consumes.
#[allow(clippy::too_many_arguments)]
pub fn container_config_with_mounts(
    pod: &Pod,
    container: &Container,
    image_ref: &str,
    host_paths: &HashMap<String, String>,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
    node_allocatable: Option<&HashMap<String, String>>,
    mount_attrs: &HashMap<String, MountAttrs>,
    supports_rro: bool,
    image_volumes: &HashMap<String, v1::ImageSpec>,
) -> Result<v1::ContainerConfig, String> {
    let mut labels = pod_labels(pod);
    labels.insert(labels::CONTAINER_NAME.to_string(), container.name.clone());

    let linux = {
        let resources = linux_resources(container);
        let security_context = linux_security_context(pod, container);
        if resources.is_some() || security_context.is_some() {
            Some(v1::LinuxContainerConfig {
                resources,
                security_context,
            })
        } else {
            None
        }
    };

    let envs = env_vars(pod, container, config_maps, secrets, node_allocatable);
    // command/args expand `$(VAR)` against the full container env (upstream
    // `expandContainerCommandAndArgs`).
    let env_map: HashMap<String, String> = envs
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    Ok(v1::ContainerConfig {
        metadata: Some(v1::ContainerMetadata {
            name: container.name.clone(),
            attempt: 0,
        }),
        image: Some(v1::ImageSpec {
            image: image_ref.to_string(),
            ..Default::default()
        }),
        command: expand_all(container.command.clone().unwrap_or_default(), &env_map),
        args: expand_all(container.args.clone().unwrap_or_default(), &env_map),
        working_dir: container.working_dir.clone().unwrap_or_default(),
        envs,
        mounts: mounts(
            container,
            host_paths,
            &env_map,
            mount_attrs,
            supports_rro,
            image_volumes,
        )?,
        labels,
        log_path: format!("{}.log", container.name),
        linux,
        ..Default::default()
    })
}

/// Build the Docker-link-style service env a container sees, per upstream
/// kubelet `getServiceEnvVarMap` + `makeLinkVariables`. Each service with a
/// usable ClusterIP contributes vars keyed by the uppercased ('-'→'_') service
/// name: `{N}_SERVICE_HOST`, `{N}_SERVICE_PORT`, optional named
/// `{N}_SERVICE_PORT_{PORTNAME}`, and for each declared port the docker-link
/// quartet `{N}_PORT_{port}_{PROTO}[ |_PROTO|_PORT|_ADDR]` plus a single
/// `{N}_PORT` from the first port. Headless ("None") and IP-less services are
/// skipped. Services are processed in name order for deterministic output.
pub fn service_link_env_vars(services: &[Service]) -> Vec<(String, String)> {
    let mut sorted: Vec<&Service> = services.iter().collect();
    sorted.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    let mut out: Vec<(String, String)> = Vec::new();
    for svc in sorted {
        let Some(ip) = svc.spec.cluster_ip.as_deref() else {
            continue;
        };
        if ip.is_empty() || ip == "None" {
            continue;
        }
        let Some(first) = svc.spec.ports.first() else {
            continue;
        };
        let n = svc.metadata.name.to_uppercase().replace('-', "_");
        out.push((format!("{n}_SERVICE_HOST"), ip.to_string()));
        out.push((format!("{n}_SERVICE_PORT"), first.port.to_string()));
        for p in &svc.spec.ports {
            if let Some(name) = p.name.as_deref().filter(|s| !s.is_empty()) {
                let pn = name.to_uppercase().replace('-', "_");
                out.push((format!("{n}_SERVICE_PORT_{pn}"), p.port.to_string()));
            }
        }
        let proto0 = first.protocol.to_lowercase();
        out.push((
            format!("{n}_PORT"),
            format!("{proto0}://{ip}:{}", first.port),
        ));
        for p in &svc.spec.ports {
            let proto = p.protocol.to_lowercase();
            let prefix = format!("{n}_PORT_{}_{}", p.port, p.protocol.to_uppercase());
            out.push((prefix.clone(), format!("{proto}://{ip}:{}", p.port)));
            out.push((format!("{prefix}_PROTO"), proto.clone()));
            out.push((format!("{prefix}_PORT"), p.port.to_string()));
            out.push((format!("{prefix}_ADDR"), ip.to_string()));
        }
    }
    out
}
