//! `podutil.GetWarningsForPod` — port of `pkg/api/pod/warnings.go`
//! (release-1.35, `GetWarningsForPod` :38, `warningsForPodSpecAndMeta` :80).
//!
//! Called with a `nil` field path, as `podStrategy.WarningsOnCreate` does
//! (strategy.go:127): a Pod is not a template, so the container AppArmor
//! annotation warning is left to `applyAppArmorVersionSkew`
//! (`isPodTemplate := fieldPath != nil`, warnings.go:206).
//!
//! Not ported, because this tree has no field for them: the deprecated and
//! removed volume plugins (`photonPersistentDisk`, `gitRepo`, `scaleIO`,
//! `flocker`, `storageos`, `quobyte`, `glusterfs`, `cephfs`, `rbd`;
//! warnings.go:148-176) and the ephemeral volume's PVC warnings, which live
//! with the PersistentVolumeClaim strategy.

use std::collections::{HashMap, HashSet};

use crate::quantity::Quantity;
use crate::resources::pod::{Container, ContainerPort, EnvVar, VolumeProjection};
use crate::resources::{Pod, PodSpec};
use crate::validation::field::Path;
use crate::validation::metav1::{get_warnings_for_ip, is_valid_label_value};
use crate::validation::runtimeclass::get_node_label_deprecated_message;

/// `api.SeccompPodAnnotationKey`.
pub const SECCOMP_POD_ANNOTATION_KEY: &str = "seccomp.security.alpha.kubernetes.io/pod";
/// `api.SeccompContainerAnnotationKeyPrefix`.
pub const SECCOMP_CONTAINER_ANNOTATION_KEY_PREFIX: &str =
    "container.seccomp.security.alpha.kubernetes.io/";

/// `deprecatedAnnotations` (warnings.go:61-77).
const DEPRECATED_ANNOTATIONS: &[(&str, &str)] = &[
    (
        "scheduler.alpha.kubernetes.io/critical-pod",
        r#"non-functional in v1.16+; use the "priorityClassName" field instead"#,
    ),
    (
        "security.alpha.kubernetes.io/sysctls",
        r#"non-functional in v1.11+; use the "sysctls" field instead"#,
    ),
    (
        "security.alpha.kubernetes.io/unsafe-sysctls",
        r#"non-functional in v1.11+; use the "sysctls" field instead"#,
    ),
];

/// `GetWarningsForPod(ctx, pod, oldPod)` (warnings.go:38-52).
pub fn get_warnings_for_pod(pod: &Pod, _old: Option<&Pod>) -> Vec<String> {
    let default_spec = PodSpec::default();
    let spec = pod.spec.as_ref().unwrap_or(&default_spec);
    warnings_for_pod_spec_and_meta(spec, pod.metadata.annotations.as_ref())
}

/// A container of any kind, as `VisitContainersWithPath` hands it over: an
/// ephemeral container is visited through its `EphemeralContainerCommon`.
struct Visited<'a> {
    path: Path,
    name: &'a str,
    security_context: Option<&'a crate::resources::pod::SecurityContext>,
    resources: Option<&'a crate::types::ResourceRequirements>,
    env: &'a [EnvVar],
    ports: &'a [ContainerPort],
}

fn visit_containers<'a>(spec: &'a PodSpec) -> Vec<Visited<'a>> {
    let spec_path = Path::new("spec");
    let from = |path: Path, c: &'a Container| Visited {
        path,
        name: &c.name,
        security_context: c.security_context.as_ref(),
        resources: c.resources.as_ref(),
        env: c.env.as_deref().unwrap_or(&[]),
        ports: c.ports.as_deref().unwrap_or(&[]),
    };
    let mut out = Vec::new();
    for (i, c) in spec.containers.iter().enumerate() {
        out.push(from(spec_path.child("containers").index(i), c));
    }
    for (i, c) in spec.init_containers.iter().flatten().enumerate() {
        out.push(from(spec_path.child("initContainers").index(i), c));
    }
    for (i, c) in spec.ephemeral_containers.iter().flatten().enumerate() {
        out.push(Visited {
            path: spec_path.child("ephemeralContainers").index(i),
            name: &c.name,
            security_context: c.security_context.as_ref(),
            resources: c.resources.as_ref(),
            env: c.env.as_deref().unwrap_or(&[]),
            ports: &[],
        });
    }
    out
}

/// Whether the quantity has a fractional byte value
/// (`value.MilliValue()%int64(1000) != int64(0)`).
fn fractional(value: &str) -> Option<String> {
    let q = Quantity::parse(value).ok()?;
    (q.milli_value() % 1000 != 0).then(|| q.canonical_string())
}

fn fractional_warnings(list: Option<&HashMap<String, String>>, path: &Path, out: &mut Vec<String>) {
    let Some(list) = list else { return };
    for name in ["memory", "ephemeral-storage"] {
        if let Some(rendered) = list.get(name).and_then(|v| fractional(v)) {
            out.push(format!(
                "{}: fractional byte value {rendered:?} is invalid, must be an integer",
                path.key(name)
            ));
        }
    }
}

/// `warningsForPodSpecAndMeta` (warnings.go:80-324) with a `nil` field path.
pub fn warnings_for_pod_spec_and_meta(
    spec: &PodSpec,
    annotations: Option<&HashMap<String, String>>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    let empty = HashMap::new();
    let annotations = annotations.unwrap_or(&empty);
    let spec_path = Path::new("spec");
    let meta_annotations = Path::new("metadata").child("annotations");

    // use of deprecated node labels in selectors/affinity/topology
    for key in spec.node_selector.iter().flat_map(|m| m.keys()) {
        if let Some(msg) = get_node_label_deprecated_message(key) {
            warnings.push(format!(
                "{}: {msg}",
                spec_path.child("nodeSelector").key(key)
            ));
        }
    }
    if let Some(node_affinity) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
    {
        if let Some(required) = node_affinity
            .required_during_scheduling_ignored_during_execution
            .as_ref()
        {
            let term_path = spec_path
                .child("affinity")
                .child("nodeAffinity")
                .child("requiredDuringSchedulingIgnoredDuringExecution")
                .child("nodeSelectorTerms");
            for (i, term) in required.node_selector_terms.iter().enumerate() {
                warnings.extend(warnings_for_node_selector_term(
                    term,
                    false,
                    &term_path.index(i),
                ));
            }
        }
        let preferred_path = spec_path
            .child("affinity")
            .child("nodeAffinity")
            .child("preferredDuringSchedulingIgnoredDuringExecution");
        for (i, term) in node_affinity
            .preferred_during_scheduling_ignored_during_execution
            .iter()
            .flatten()
            .enumerate()
        {
            warnings.extend(warnings_for_node_selector_term(
                &term.preference,
                true,
                &preferred_path.index(i).child("preference"),
            ));
        }
    }
    for (i, t) in spec
        .topology_spread_constraints
        .iter()
        .flatten()
        .enumerate()
    {
        let path = spec_path.child("topologySpreadConstraints").index(i);
        if let Some(msg) = get_node_label_deprecated_message(&t.topology_key) {
            warnings.push(format!(
                "{}: {} is {msg}",
                path.child("topologyKey"),
                t.topology_key
            ));
        }
        // warn if labelSelector is empty which is no-match.
        if t.label_selector.is_none() {
            warnings.push(format!(
                "{}: a null labelSelector results in matching no pod",
                path.child("labelSelector")
            ));
        }
    }

    // use of deprecated annotations
    for (key, message) in DEPRECATED_ANNOTATIONS {
        if annotations.contains_key(*key) {
            warnings.push(format!("{}: {message}", meta_annotations.key(*key)));
        }
    }

    warnings.extend(warnings_for_overlapping_virtual_paths(spec));

    // duplicate hostAliases (#91670, #58477)
    let host_aliases = spec.host_aliases.as_deref().unwrap_or(&[]);
    if host_aliases.len() > 1 {
        let mut items = HashSet::new();
        for (i, item) in host_aliases.iter().enumerate() {
            if !items.insert(item.ip.as_str()) {
                warnings.push(format!(
                    "{}: duplicate ip {:?}",
                    spec_path.child("hostAliases").index(i).child("ip"),
                    item.ip
                ));
            }
        }
    }

    // duplicate imagePullSecrets (#91629, #58477)
    let pull_secrets = spec.image_pull_secrets.as_deref().unwrap_or(&[]);
    if pull_secrets.len() > 1 {
        let mut items = HashSet::new();
        for (i, item) in pull_secrets.iter().enumerate() {
            if !items.insert(item.name.as_str()) {
                warnings.push(format!(
                    "{}: duplicate name {:?}",
                    spec_path.child("imagePullSecrets").index(i).child("name"),
                    item.name
                ));
            }
        }
    }
    // imagePullSecrets with empty name (#99454#issuecomment-787838112)
    for (i, item) in pull_secrets.iter().enumerate() {
        if item.name.is_empty() {
            warnings.push(format!(
                "{}: invalid empty name {:?}",
                spec_path.child("imagePullSecrets").index(i).child("name"),
                item.name
            ));
        }
    }

    // fractional memory/ephemeral-storage requests/limits (#79950, #49442, #18538)
    fractional_warnings(
        spec.overhead.as_ref(),
        &spec_path.child("overhead"),
        &mut warnings,
    );

    // use of pod seccomp annotation without accompanying field
    let pod_has_seccomp = spec
        .security_context
        .as_ref()
        .is_some_and(|sc| sc.seccomp_profile.is_some());
    if !pod_has_seccomp && annotations.contains_key(SECCOMP_POD_ANNOTATION_KEY) {
        warnings.push(format!(
            r#"{}: non-functional in v1.27+; use the "seccompProfile" field instead"#,
            meta_annotations.key(SECCOMP_POD_ANNOTATION_KEY)
        ));
    }

    let visited = visit_containers(spec);
    for c in &visited {
        // use of container seccomp annotation without accompanying field
        let has_seccomp = c
            .security_context
            .is_some_and(|sc| sc.seccomp_profile.is_some());
        let key = format!("{SECCOMP_CONTAINER_ANNOTATION_KEY_PREFIX}{}", c.name);
        if !has_seccomp && annotations.contains_key(&key) {
            warnings.push(format!(
                r#"{}: non-functional in v1.27+; use the "seccompProfile" field instead"#,
                meta_annotations.key(key)
            ));
        }

        // fractional memory/ephemeral-storage requests/limits
        if let Some(resources) = c.resources {
            fractional_warnings(
                resources.limits.as_ref(),
                &c.path.child("resources").child("limits"),
                &mut warnings,
            );
            fractional_warnings(
                resources.requests.as_ref(),
                &c.path.child("resources").child("requests"),
                &mut warnings,
            );
        }

        // duplicate containers[*].env (#86163, #93266, #58477)
        if c.env.len() > 1 {
            let mut items: HashSet<&str> = HashSet::new();
            for (i, item) in c.env.iter().enumerate() {
                if items.contains(item.name.as_str()) {
                    // a previous value exists, but it might be OK
                    let reference = format!("$({})", item.name);
                    let value = item.value.as_deref().unwrap_or("");
                    // if we are replacing it with a valueFrom, warn; if this
                    // is X="$(X)", warn; if the new value does not contain a
                    // reference to the old value (e.g. X="abc"; X="$(X)123"),
                    // warn
                    let bad = item.value_from.is_some()
                        || value == reference
                        || !value.contains(&reference);
                    if bad {
                        warnings.push(format!(
                            "{}: hides previous definition of {:?}, which may be dropped when using apply",
                            c.path.child("env").index(i),
                            item.name
                        ));
                    }
                } else {
                    items.insert(item.name.as_str());
                }
            }
        }
    }

    // Accumulate ports across all containers
    let mut all_ports: HashMap<String, Vec<(Path, &ContainerPort)>> = HashMap::new();
    for c in &visited {
        for (i, port) in c.ports.iter().enumerate() {
            let path = c.path.child("ports").index(i);
            let host_ip = port.host_ip.as_deref().unwrap_or("");
            let host_port = port.host_port.unwrap_or(0);
            if !host_ip.is_empty() && host_port == 0 {
                warnings.push(format!(
                    "{path}: hostIP set without hostPort: {}",
                    format_container_port(port)
                ));
            }
            let k = format!("{}/{}", port.container_port, port.protocol);
            if let Some(others) = all_ports.get(&k) {
                // Someone else has this protocol+port, but it still might not
                // be a conflict.
                for (other_path, other) in others {
                    let other_ip = other.host_ip.as_deref().unwrap_or("");
                    let other_port = other.host_port.unwrap_or(0);
                    if host_ip == other_ip && host_port == other_port {
                        // Exactly-equal is obvious. Validation should already
                        // filter for this except when these are unspecified.
                        warnings.push(format!(
                            "{path}: duplicate port definition with {other_path}"
                        ));
                    } else if host_port == 0 || other_port == 0 {
                        // HostPort = 0 is redundant with any other value,
                        // which is odd but not really dangerous.
                        warnings.push(format!(
                            "{path}: overlapping port definition with {other_path}"
                        ));
                    } else if host_port == other_port && (host_ip.is_empty() != other_ip.is_empty())
                    {
                        // If the HostPorts are the same and either HostIP is
                        // not specified while the other is not, the behavior
                        // is undefined.
                        warnings.push(format!(
                            "{path}: dangerously ambiguous port definition with {other_path}"
                        ));
                    }
                }
            }
            all_ports.entry(k).or_default().push((path, port));
        }
    }

    // Accumulate port names of containers and sidecar containers
    let mut all_port_names: HashMap<&str, Path> = HashMap::new();
    for c in &visited {
        for (i, port) in c.ports.iter().enumerate() {
            let Some(name) = port.name.as_deref().filter(|n| !n.is_empty()) else {
                continue;
            };
            let path = c.path.child("ports").index(i);
            if let Some(other) = all_port_names.get(name) {
                warnings.push(format!(
                    "{path}: duplicate port name {name:?} with {other}, services and probes that select ports by name will use {other}"
                ));
            } else {
                all_port_names.insert(name, path);
            }
        }
    }

    // warn if the terminationGracePeriodSeconds is negative.
    if spec.termination_grace_period_seconds.is_some_and(|s| s < 0) {
        warnings.push(format!(
            "{}: must be >= 0; negative values are invalid and will be treated as 1",
            spec_path.child("terminationGracePeriodSeconds")
        ));
    }

    if let Some(affinity) = spec.affinity.as_ref() {
        let affinity_path = spec_path.child("affinity");
        if let Some(a) = affinity.pod_affinity.as_ref() {
            let path = affinity_path.child("podAffinity");
            warnings.extend(warnings_for_pod_affinity_terms(
                a.required_during_scheduling_ignored_during_execution
                    .as_deref()
                    .unwrap_or(&[]),
                &path.child("requiredDuringSchedulingIgnoredDuringExecution"),
            ));
            warnings.extend(warnings_for_weighted_pod_affinity_terms(
                a.preferred_during_scheduling_ignored_during_execution
                    .as_deref()
                    .unwrap_or(&[]),
                &path.child("preferredDuringSchedulingIgnoredDuringExecution"),
            ));
        }
        if let Some(a) = affinity.pod_anti_affinity.as_ref() {
            let path = affinity_path.child("podAntiAffinity");
            warnings.extend(warnings_for_pod_affinity_terms(
                a.required_during_scheduling_ignored_during_execution
                    .as_deref()
                    .unwrap_or(&[]),
                &path.child("requiredDuringSchedulingIgnoredDuringExecution"),
            ));
            warnings.extend(warnings_for_weighted_pod_affinity_terms(
                a.preferred_during_scheduling_ignored_during_execution
                    .as_deref()
                    .unwrap_or(&[]),
                &path.child("preferredDuringSchedulingIgnoredDuringExecution"),
            ));
        }
    }

    // Deprecated IP address formats
    if let Some(dns_config) = spec.dns_config.as_ref() {
        for (i, ns) in dns_config.nameservers.iter().flatten().enumerate() {
            warnings.extend(get_warnings_for_ip(
                &spec_path
                    .child("dnsConfig")
                    .child("nameservers")
                    .index(i)
                    .to_string(),
                ns,
            ));
        }
    }
    for (i, host_alias) in host_aliases.iter().enumerate() {
        warnings.extend(get_warnings_for_ip(
            &spec_path
                .child("hostAliases")
                .index(i)
                .child("ip")
                .to_string(),
            &host_alias.ip,
        ));
    }

    warnings
}

/// `%+v` of an `api.ContainerPort`.
fn format_container_port(port: &ContainerPort) -> String {
    format!(
        "{{Name:{} HostPort:{} ContainerPort:{} Protocol:{} HostIP:{}}}",
        port.name.as_deref().unwrap_or(""),
        port.host_port.unwrap_or(0),
        port.container_port,
        port.protocol,
        port.host_ip.as_deref().unwrap_or(""),
    )
}

/// `GetWarningsForNodeSelectorTerm` (pkg/api/node/util.go:95-125).
fn warnings_for_node_selector_term(
    term: &crate::resources::pod::NodeSelectorTerm,
    check_label_value: bool,
    path: &Path,
) -> Vec<String> {
    let mut warnings = Vec::new();
    for (i, expression) in term.match_expressions.iter().flatten().enumerate() {
        if let Some(msg) = get_node_label_deprecated_message(&expression.key) {
            warnings.push(format!(
                "{}: {} is {msg}",
                path.child("matchExpressions").index(i).child("key"),
                expression.key
            ));
        }
        if check_label_value {
            for (index, value) in expression.values.iter().flatten().enumerate() {
                for msg in is_valid_label_value(value) {
                    warnings.push(format!(
                        "{}: {value} is invalid, {msg}",
                        path.child("matchExpressions")
                            .index(i)
                            .child("values")
                            .index(index)
                    ));
                }
            }
        }
    }
    warnings
}

fn warnings_for_pod_affinity_terms(
    terms: &[crate::resources::pod::PodAffinityTerm],
    path: &Path,
) -> Vec<String> {
    terms
        .iter()
        .enumerate()
        .filter(|(_, t)| t.label_selector.is_none())
        .map(|(i, _)| {
            format!(
                "{}: a null labelSelector results in matching no pod",
                path.index(i).child("labelSelector")
            )
        })
        .collect()
}

fn warnings_for_weighted_pod_affinity_terms(
    terms: &[crate::resources::pod::WeightedPodAffinityTerm],
    path: &Path,
) -> Vec<String> {
    terms
        .iter()
        .enumerate()
        .filter(|(_, t)| t.pod_affinity_term.label_selector.is_none())
        .map(|(i, _)| {
            format!(
                "{}: a null labelSelector results in matching no pod",
                path.index(i)
                    .child("podAffinityTerm")
                    .child("labelSelector")
            )
        })
        .collect()
}

/// A path and where it came from, for better messages (`pathAndSource`,
/// warnings.go:495-505).
#[derive(Clone, PartialEq, Eq)]
struct PathAndSource {
    path: String,
    source: String,
}

impl std::fmt::Display for PathAndSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.source.is_empty() {
            write!(f, "{:?}", self.path)
        } else {
            write!(f, "{:?} ({})", self.path, self.source)
        }
    }
}

fn extract_paths<'a>(paths: impl Iterator<Item = &'a str>, source: &str) -> Vec<PathAndSource> {
    paths
        .map(|p| PathAndSource {
            path: p.to_string(),
            source: source.to_string(),
        })
        .collect()
}

/// `checkForOverlap` (warnings.go:547-571).
fn check_for_overlap<'a>(
    haystack: &'a [PathAndSource],
    needle: &PathAndSource,
) -> Vec<&'a PathAndSource> {
    if needle.path.is_empty() {
        return Vec::new();
    }
    haystack
        .iter()
        .filter(|item| {
            if item.path.is_empty() {
                return false;
            }
            *item == needle
                || format!("{}/", item.path).starts_with(&format!("{}/", needle.path))
                || format!("{}/", needle.path).starts_with(&format!("{}/", item.path))
        })
        .collect()
}

/// `checkVolumeMappingForOverlap` (warnings.go:528-545).
fn check_volume_mapping_for_overlap(paths: Vec<PathAndSource>) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut all_paths: Vec<PathAndSource> = Vec::new();
    for mut ps in paths {
        ps.path = ps.path.trim_end_matches('/').to_string();
        for c in check_for_overlap(&all_paths, &ps) {
            warnings.push(format!("{ps} with {c}"));
        }
        all_paths.push(ps);
    }
    warnings
}

/// The paths a projected source contributes (warnings.go:451-483).
fn projected_source_paths(source: &VolumeProjection) -> Vec<PathAndSource> {
    let ps = |path: &str, source: String| PathAndSource {
        path: path.to_string(),
        source,
    };
    if let Some(cm) = source.config_map.as_ref().filter(|c| c.items.is_some()) {
        return extract_paths(
            cm.items.iter().flatten().map(|k| k.path.as_str()),
            &format!("ConfigMap {:?}", cm.name.as_deref().unwrap_or("")),
        );
    }
    if let Some(secret) = source.secret.as_ref().filter(|s| s.items.is_some()) {
        return extract_paths(
            secret.items.iter().flatten().map(|k| k.path.as_str()),
            &format!("Secret {:?}", secret.name.as_deref().unwrap_or("")),
        );
    }
    if let Some(d) = source.downward_api.as_ref().filter(|d| d.items.is_some()) {
        return extract_paths(
            d.items.iter().flatten().map(|f| f.path.as_str()),
            "DownwardAPI",
        );
    }
    if let Some(token) = source.service_account_token.as_ref() {
        return vec![ps(&token.path, "ServiceAccountToken".to_string())];
    }
    if let Some(ctb) = source.cluster_trust_bundle.as_ref() {
        let name = ctb
            .name
            .as_deref()
            .or(ctb.signer_name.as_deref())
            .unwrap_or("");
        return vec![ps(&ctb.path, format!("ClusterTrustBundle {name:?}"))];
    }
    if let Some(cert) = source.pod_certificate.as_ref() {
        let mut out = Vec::new();
        for (path, what) in [
            (&cert.certificate_chain_path, "PodCertificate chain"),
            (&cert.key_path, "PodCertificate key"),
            (
                &cert.credential_bundle_path,
                "PodCertificate credential bundle",
            ),
        ] {
            if let Some(path) = path.as_deref().filter(|p| !p.is_empty()) {
                out.push(ps(path, what.to_string()));
            }
        }
        return out;
    }
    Vec::new()
}

/// `warningsForOverlappingVirtualPaths` (warnings.go:407-493).
fn warnings_for_overlapping_virtual_paths(spec: &PodSpec) -> Vec<String> {
    let mut warnings = Vec::new();
    let mk_warn = |vol: &str, desc: &str, body: &str| {
        format!("volume {vol:?} ({desc}): overlapping paths: {body}")
    };
    for v in spec.volumes.iter().flatten() {
        if let Some(cm) = v.config_map.as_ref().filter(|c| c.items.is_some()) {
            let paths = extract_paths(cm.items.iter().flatten().map(|k| k.path.as_str()), "");
            for ol in check_volume_mapping_for_overlap(paths) {
                warnings.push(mk_warn(
                    &v.name,
                    &format!("ConfigMap {:?}", cm.name.as_deref().unwrap_or("")),
                    &ol,
                ));
            }
        }
        if let Some(secret) = v.secret.as_ref().filter(|s| s.items.is_some()) {
            let paths = extract_paths(secret.items.iter().flatten().map(|k| k.path.as_str()), "");
            for ol in check_volume_mapping_for_overlap(paths) {
                warnings.push(mk_warn(
                    &v.name,
                    &format!("Secret {:?}", secret.secret_name.as_deref().unwrap_or("")),
                    &ol,
                ));
            }
        }
        if let Some(d) = v.downward_api.as_ref().filter(|d| d.items.is_some()) {
            let paths = extract_paths(d.items.iter().flatten().map(|f| f.path.as_str()), "");
            for ol in check_volume_mapping_for_overlap(paths) {
                warnings.push(mk_warn(&v.name, "DownwardAPI", &ol));
            }
        }
        if let Some(projected) = v.projected.as_ref() {
            let mut all_paths: Vec<PathAndSource> = Vec::new();
            for source in projected.sources.iter().flatten() {
                if source.secret.is_none()
                    && source.config_map.is_none()
                    && source.downward_api.is_none()
                    && source.service_account_token.is_none()
                    && source.cluster_trust_bundle.is_none()
                    && source.pod_certificate.is_none()
                {
                    warnings.push(format!(
                        "volume {:?} (Projected) has no sources provided",
                        v.name
                    ));
                    continue;
                }
                for mut ps in projected_source_paths(source) {
                    ps.path = ps.path.trim_end_matches('/').to_string();
                    for c in check_for_overlap(&all_paths, &ps) {
                        warnings.push(mk_warn(&v.name, "Projected", &format!("{ps} with {c}")));
                    }
                    all_paths.push(ps);
                }
            }
        }
    }
    warnings
}
