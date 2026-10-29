//! #2931: a Go-protobuf object whose plain `omitempty` scalars were written as
//! zero must still deserialize into our models once the decoder drops them
//! (the JSON form Go clients send). A plain `i32`/`bool` field without
//! `#[serde(default)]` would fail here.
//!
//! 1. Every table entry names a field the registry knows (guards typos and
//!    upstream renames).
//! 2. For the messages we model in `rusternetes-common`, the all-zero wire
//!    message decodes to `{}` and then deserializes into the Rust type.
use rusternetes_common::resources::*;
use rusternetes_common::types::{ObjectMeta, Status, StatusDetails};
use rusternetes_protobuf::plain_zero_scalars::PLAIN_ZERO_SCALARS;
use rusternetes_protobuf::PROTO_REGISTRY;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

fn varint(mut n: u64, out: &mut Vec<u8>) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// Wire message with every table field of `msg` written as varint zero.
fn zero_wire(msg: &str) -> Vec<u8> {
    let i = PLAIN_ZERO_SCALARS
        .binary_search_by(|(m, _)| (*m).cmp(msg))
        .unwrap_or_else(|_| panic!("{msg} not in table"));
    let (_, fields) = PLAIN_ZERO_SCALARS[i];
    let (_, schema) = PROTO_REGISTRY
        .iter_schemas()
        .find(|(m, _)| *m == msg)
        .unwrap_or_else(|| panic!("{msg} not registered"));
    let mut out = Vec::new();
    for f in fields {
        // The registry is a subset of upstream's fields; a field it does not
        // decode cannot reach the models, so there is nothing to write.
        let Some((num, _)) = schema.fields.iter().find(|(_, (name, _))| name == f) else {
            continue;
        };
        varint(u64::from(*num) << 3, &mut out);
        out.push(0);
    }
    out
}

#[test]
fn every_registered_table_entry_decodes_to_empty() {
    for (msg, _) in PLAIN_ZERO_SCALARS {
        if PROTO_REGISTRY.iter_schemas().all(|(m, _)| m != *msg) {
            continue; // message we do not decode (e.g. DRA, VAP)
        }
        let v = PROTO_REGISTRY
            .decode_message(msg, &zero_wire(msg))
            .unwrap_or_else(|| panic!("{msg} decodes"));
        assert_eq!(v, json!({}), "{msg}");
    }
}

/// Decode the all-zero wire of `msg`, overlay `required` (fields unrelated to
/// the dropped scalars that the model insists on), and deserialize into `T`.
fn check<T: DeserializeOwned>(msg: &str, required: Value) {
    let mut v = PROTO_REGISTRY
        .decode_message(msg, &zero_wire(msg))
        .unwrap_or_else(|| panic!("{msg} decodes"));
    if let (Some(o), Some(r)) = (v.as_object_mut(), required.as_object()) {
        o.extend(r.clone());
    }
    serde_json::from_value::<T>(v.clone())
        .unwrap_or_else(|e| panic!("{msg}: {v} does not deserialize: {e}"));
}

#[test]
fn zero_dropped_scalars_deserialize_into_models() {
    check::<ObjectMeta>("ObjectMeta", json!({}));
    check::<Status>("Status", json!({}));
    check::<StatusDetails>("StatusDetails", json!({}));
    check::<DeploymentSpec>(
        "DeploymentSpec",
        json!({"selector": {}, "template": {"spec": {"containers": []}}}),
    );
    check::<DeploymentStatus>("DeploymentStatus", json!({}));
    check::<ReplicaSetSpec>(
        "ReplicaSetSpec",
        json!({"selector": {}, "template": {"spec": {"containers": []}}}),
    );
    check::<ReplicaSetStatus>("ReplicaSetStatus", json!({"replicas": 0}));
    check::<ReplicationControllerSpec>("ReplicationControllerSpec", json!({}));
    check::<ReplicationControllerStatus>("ReplicationControllerStatus", json!({"replicas": 0}));
    check::<StatefulSetSpec>(
        "StatefulSetSpec",
        json!({"selector": {}, "template": {"spec": {"containers": []}}, "serviceName": ""}),
    );
    check::<StatefulSetStatus>("StatefulSetStatus", json!({"replicas": 0}));
    check::<DaemonSetSpec>(
        "DaemonSetSpec",
        json!({"selector": {}, "template": {"spec": {"containers": []}}}),
    );
    check::<DaemonSetStatus>(
        "DaemonSetStatus",
        json!({"currentNumberScheduled": 0, "numberMisscheduled": 0,
               "desiredNumberScheduled": 0, "numberReady": 0}),
    );
    check::<JobStatus>("JobStatus", json!({}));
    check::<ServiceSpec>("ServiceSpec", json!({}));
    check::<ServicePort>("ServicePort", json!({"port": 80}));
    check::<NodeSpec>("NodeSpec", json!({}));
    check::<PodSpec>("PodSpec", json!({"containers": []}));
    check::<Container>("Container", json!({"name": "c"}));
    check::<ContainerPort>("ContainerPort", json!({"containerPort": 80}));
    check::<VolumeMount>("VolumeMount", json!({"name": "v", "mountPath": "/"}));
    check::<Probe>("Probe", json!({}));
    check::<PodCondition>("PodCondition", json!({"type": "Ready", "status": "True"}));
    check::<PodStatus>("PodStatus", json!({}));
    check::<PriorityClass>("PriorityClass", json!({"value": 0}));
    check::<ScaleSpec>("ScaleSpec", json!({}));
    check::<PodDisruptionBudgetStatus>(
        "PodDisruptionBudgetStatus",
        json!({"currentHealthy": 0, "desiredHealthy": 0, "disruptionsAllowed": 0,
               "expectedPods": 0}),
    );
}
