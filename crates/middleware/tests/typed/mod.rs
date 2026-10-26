//! Typed dispatch for the `typed` / `typed-pb` checks: which of our serde
//! resource structs models each GA (`v1`/`v2`) fixture. Pre-GA versions are not
//! dispatched: the repo models one shape per kind (the GA one), so a v1beta1
//! fixture cannot round-trip through it by construction.
//!
//! Stems with no typed model (no struct in `rusternetes_common::resources`)
//! are listed in `NO_TYPED_MODEL`; the test asserts every GA stem is either
//! dispatched or listed there, so a newly added struct must be wired in.
use rusternetes_common::resources::csi::{
    CSIDriver, CSINode, CSIStorageCapacity, VolumeAttachment, VolumeAttributesClass,
};
use rusternetes_common::resources::rangeallocation::RangeAllocation;
use rusternetes_common::resources::{
    Binding, CertificateSigningRequest, ClusterRole, ClusterRoleBinding, ComponentStatus,
    ConfigMap, ControllerRevision, CronJob, CustomResourceDefinition, DaemonSet, Deployment,
    DeviceClass, EndpointSlice, Endpoints, Event, EventV1, Eviction, FlowSchema,
    HorizontalPodAutoscaler, IPAddress, Ingress, IngressClass, Job, Lease, LimitRange,
    LocalSubjectAccessReview, MutatingWebhookConfiguration, Namespace, NetworkPolicy, Node,
    PersistentVolume, PersistentVolumeClaim, Pod, PodDisruptionBudget, PodTemplate, PriorityClass,
    PriorityLevelConfiguration, ReplicaSet, ReplicationController, ResourceClaim,
    ResourceClaimTemplate, ResourceQuota, ResourceSlice, Role, RoleBinding, RuntimeClass, Scale,
    Secret, SelfSubjectAccessReview, SelfSubjectReview, SelfSubjectRulesReview, Service,
    ServiceAccount, ServiceCIDR, StatefulSet, StorageClass, SubjectAccessReview, TokenRequest,
    TokenReview, ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding,
    ValidatingWebhookConfiguration,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// `(expected, got)`: `expected` is the input after the two documented
/// adjustments below, `got` is the input after a typed decode + encode.
pub type Roundtrip = fn(&Value) -> Result<(Value, Value), String>;

/// Our models keep several string enums as strict Rust enums, while the upstream
/// fixtures fill every string with `<field>Value` (e.g. `phaseValue`). On a
/// serde "unknown variant" error the offending literal is replaced everywhere
/// with the first valid variant and the decode retried (same idea as
/// `enum_token` in `crates/common/tests/it/k8s_openapi_parity.rs`); `expected`
/// carries the same replacement so the comparison stays exact. Open-string
/// upstream fields modelled as closed enums are a real strictness gap, tracked
/// via the `typed-strict-enum` allowlist entries.
fn replace_literal(v: &mut Value, from: &str, to: &str) {
    match v {
        Value::String(s) if s == from => *s = to.to_string(),
        Value::Array(a) => a.iter_mut().for_each(|x| replace_literal(x, from, to)),
        Value::Object(m) => m.values_mut().for_each(|x| replace_literal(x, from, to)),
        _ => {}
    }
}

/// `metadata.selfLink` is deprecated and intentionally not modelled by our typed
/// `ObjectMeta` (see the wire-level carry in `crates/protobuf`); strip it from
/// the expectation.
fn strip_self_link(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.remove("selfLink");
            m.values_mut().for_each(strip_self_link);
        }
        Value::Array(a) => a.iter_mut().for_each(strip_self_link),
        _ => {}
    }
}

fn rt<T: Serialize + DeserializeOwned>(v: &Value) -> Result<(Value, Value), String> {
    let mut expected = v.clone();
    strip_self_link(&mut expected);
    for _ in 0..64 {
        match serde_json::from_value::<T>(expected.clone()) {
            Ok(t) => {
                let got = serde_json::to_value(&t).map_err(|e| format!("typed encode: {e}"))?;
                return Ok((expected, got));
            }
            Err(e) => {
                let msg = e.to_string();
                let bad = msg
                    .strip_prefix("unknown variant `")
                    .and_then(|r| r.split_once('`'));
                let good = msg
                    .split("expected")
                    .nth(1)
                    .into_iter()
                    .flat_map(|r| r.split('`').skip(1).step_by(2))
                    .find(|t| !t.is_empty());
                match (bad, good) {
                    (Some((bad, _)), Some(good)) => replace_literal(&mut expected, bad, good),
                    _ => return Err(format!("typed decode: {msg}")),
                }
            }
        }
    }
    Err("typed decode: too many enum substitutions".into())
}

macro_rules! table {
    ($($stem:literal => $t:ty),* $(,)?) => {
        pub fn dispatch(stem: &str) -> Option<Roundtrip> {
            match stem {
                $($stem => Some(rt::<$t>),)*
                _ => None,
            }
        }
        pub const DISPATCHED: &[&str] = &[$($stem),*];
    };
}

table! {
    "admissionregistration.k8s.io.v1.MutatingWebhookConfiguration" => MutatingWebhookConfiguration,
    "admissionregistration.k8s.io.v1.ValidatingAdmissionPolicy" => ValidatingAdmissionPolicy,
    "admissionregistration.k8s.io.v1.ValidatingAdmissionPolicyBinding" => ValidatingAdmissionPolicyBinding,
    "admissionregistration.k8s.io.v1.ValidatingWebhookConfiguration" => ValidatingWebhookConfiguration,
    "apps.v1.ControllerRevision" => ControllerRevision,
    "apps.v1.DaemonSet" => DaemonSet,
    "apps.v1.Deployment" => Deployment,
    "apps.v1.ReplicaSet" => ReplicaSet,
    "apps.v1.StatefulSet" => StatefulSet,
    "authentication.k8s.io.v1.SelfSubjectReview" => SelfSubjectReview,
    "authentication.k8s.io.v1.TokenRequest" => TokenRequest,
    "authentication.k8s.io.v1.TokenReview" => TokenReview,
    "authorization.k8s.io.v1.LocalSubjectAccessReview" => LocalSubjectAccessReview,
    "authorization.k8s.io.v1.SelfSubjectAccessReview" => SelfSubjectAccessReview,
    "authorization.k8s.io.v1.SelfSubjectRulesReview" => SelfSubjectRulesReview,
    "authorization.k8s.io.v1.SubjectAccessReview" => SubjectAccessReview,
    "autoscaling.v1.Scale" => Scale,
    "autoscaling.v2.HorizontalPodAutoscaler" => HorizontalPodAutoscaler,
    "batch.v1.CronJob" => CronJob,
    "batch.v1.Job" => Job,
    "certificates.k8s.io.v1.CertificateSigningRequest" => CertificateSigningRequest,
    "coordination.k8s.io.v1.Lease" => Lease,
    "core.v1.Binding" => Binding,
    "core.v1.ComponentStatus" => ComponentStatus,
    "core.v1.ConfigMap" => ConfigMap,
    "core.v1.Endpoints" => Endpoints,
    "core.v1.Event" => Event,
    "core.v1.LimitRange" => LimitRange,
    "core.v1.Namespace" => Namespace,
    "core.v1.Node" => Node,
    "core.v1.PersistentVolume" => PersistentVolume,
    "core.v1.PersistentVolumeClaim" => PersistentVolumeClaim,
    "core.v1.Pod" => Pod,
    "core.v1.PodTemplate" => PodTemplate,
    "core.v1.RangeAllocation" => RangeAllocation,
    "core.v1.ReplicationController" => ReplicationController,
    "core.v1.ResourceQuota" => ResourceQuota,
    "core.v1.Secret" => Secret,
    "core.v1.Service" => Service,
    "core.v1.ServiceAccount" => ServiceAccount,
    "discovery.k8s.io.v1.EndpointSlice" => EndpointSlice,
    "events.k8s.io.v1.Event" => EventV1,
    "flowcontrol.apiserver.k8s.io.v1.FlowSchema" => FlowSchema,
    "flowcontrol.apiserver.k8s.io.v1.PriorityLevelConfiguration" => PriorityLevelConfiguration,
    "networking.k8s.io.v1.IPAddress" => IPAddress,
    "networking.k8s.io.v1.Ingress" => Ingress,
    "networking.k8s.io.v1.IngressClass" => IngressClass,
    "networking.k8s.io.v1.NetworkPolicy" => NetworkPolicy,
    "networking.k8s.io.v1.ServiceCIDR" => ServiceCIDR,
    "node.k8s.io.v1.RuntimeClass" => RuntimeClass,
    "policy.v1.Eviction" => Eviction,
    "policy.v1.PodDisruptionBudget" => PodDisruptionBudget,
    "rbac.authorization.k8s.io.v1.ClusterRole" => ClusterRole,
    "rbac.authorization.k8s.io.v1.ClusterRoleBinding" => ClusterRoleBinding,
    "rbac.authorization.k8s.io.v1.Role" => Role,
    "rbac.authorization.k8s.io.v1.RoleBinding" => RoleBinding,
    "resource.k8s.io.v1.DeviceClass" => DeviceClass,
    "resource.k8s.io.v1.ResourceClaim" => ResourceClaim,
    "resource.k8s.io.v1.ResourceClaimTemplate" => ResourceClaimTemplate,
    "resource.k8s.io.v1.ResourceSlice" => ResourceSlice,
    "scheduling.k8s.io.v1.PriorityClass" => PriorityClass,
    "storage.k8s.io.v1.StorageClass" => StorageClass,
    "storage.k8s.io.v1.CSIDriver" => CSIDriver,
    "storage.k8s.io.v1.CSINode" => CSINode,
    "storage.k8s.io.v1.CSIStorageCapacity" => CSIStorageCapacity,
    "storage.k8s.io.v1.VolumeAttachment" => VolumeAttachment,
    "storage.k8s.io.v1.VolumeAttributesClass" => VolumeAttributesClass,
    "apiextensions.k8s.io.v1.CustomResourceDefinition" => CustomResourceDefinition,
}

/// GA fixtures with no typed model, each with the reason. Shrinks as models land.
pub const NO_TYPED_MODEL: &[(&str, &str)] = &[
    (
        "admission.k8s.io.v1.AdmissionReview",
        "webhook wire type, no resource struct",
    ),
    (
        "apidiscovery.k8s.io.v2.APIGroupDiscovery",
        "discovery document, served as raw JSON",
    ),
    (
        "apiextensions.k8s.io.v1.ConversionReview",
        "webhook wire type, no resource struct",
    ),
    ("core.v1.APIGroup", "meta type, no resource struct"),
    ("core.v1.APIVersions", "meta type, no resource struct"),
    ("core.v1.CreateOptions", "options type"),
    ("core.v1.DeleteOptions", "options type"),
    ("core.v1.GetOptions", "options type"),
    ("core.v1.ListOptions", "options type"),
    ("core.v1.NodeProxyOptions", "options type"),
    ("core.v1.PatchOptions", "options type"),
    ("core.v1.PodAttachOptions", "options type"),
    ("core.v1.PodExecOptions", "options type"),
    ("core.v1.PodLogOptions", "options type"),
    ("core.v1.PodPortForwardOptions", "options type"),
    ("core.v1.PodProxyOptions", "options type"),
    ("core.v1.SerializedReference", "legacy reference type"),
    ("core.v1.ServiceProxyOptions", "options type"),
    (
        "core.v1.Status",
        "meta type, rusternetes_common::types::Status",
    ),
    ("core.v1.UpdateOptions", "options type"),
    ("core.v1.WatchEvent", "watch envelope"),
    ("core.v1.PodStatusResult", "no struct"),
    (
        "autoscaling.v1.HorizontalPodAutoscaler",
        "only the v2 HPA shape is modelled",
    ),
];
