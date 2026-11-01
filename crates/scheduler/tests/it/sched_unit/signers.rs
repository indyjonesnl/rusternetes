//! `signers_test.go` (`kube-scheduler/framework`), `SignPod`
//! (`runtime/framework.go:805-854`) and the plugins' `SignPod`.
use rusternetes_common::resources::Pod;
use rusternetes_scheduler::framework_runtime::{Plugin, PluginSet, Plugins};
use rusternetes_scheduler::fwk::{Code, SignPlugin};
use rusternetes_scheduler::in_tree::{
    DynamicResources, InterPodAffinity, NodeName, NodePorts, PodTopologySpread,
};
use rusternetes_scheduler::profile::{Map, Profile};
use rusternetes_scheduler::signers::*;
use serde_json::{json, Value};

fn pod(spec: Value) -> Pod {
    serde_json::from_value(json!({"metadata": {"name": "p", "labels": {"a": "b"}}, "spec": spec}))
        .unwrap()
}

#[test]
fn host_ports_are_sorted_and_deduplicated() {
    assert_eq!(
        host_ports_signer(&pod(json!({"containers": []}))),
        json!([])
    );
    let p = pod(json!({
        "containers": [
            {"name": "a", "ports": [{"containerPort": 1, "hostPort": 8080}, {"containerPort": 2}]},
            {"name": "b", "ports": [{"containerPort": 3, "hostPort": 80}, {"containerPort": 4, "hostPort": 8080}]}
        ],
        "initContainers": [{"name": "i", "ports": [{"containerPort": 5, "hostPort": 443}]}]
    }));
    assert_eq!(host_ports_signer(&p), json!([80, 443, 8080]));
}

#[test]
fn tolerations_are_sorted_by_key_then_value() {
    assert_eq!(
        tolerations_signer(&pod(json!({"containers": []}))),
        json!([])
    );
    let p = pod(json!({"containers": [], "tolerations": [
        {"key": "b", "operator": "Equal", "value": "2"},
        {"key": "a", "operator": "Equal", "value": "9"},
        {"key": "a", "operator": "Equal", "value": "1"}
    ]}));
    let keys: Vec<String> = tolerations_signer(&p)
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            format!(
                "{}{}",
                t["key"].as_str().unwrap(),
                t["value"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(keys, ["a1", "a9", "b2"]);
}

#[test]
fn volumes_signer_ignores_config_maps_and_secrets() {
    let none = pod(json!({"containers": []}));
    assert_eq!(volumes_signer(&none), json!([]));
    let only_ignored = pod(json!({"containers": [], "volumes": [
        {"name": "c", "configMap": {"name": "cm"}}, {"name": "s", "secret": {"secretName": "x"}}
    ]}));
    assert_eq!(volumes_signer(&only_ignored), json!([]));
    let mixed = pod(json!({"containers": [], "volumes": [
        {"name": "z", "hostPath": {"path": "/z"}},
        {"name": "c", "configMap": {"name": "cm"}},
        {"name": "a", "emptyDir": {}}
    ]}));
    let v = volumes_signer(&mixed);
    let v = v.as_array().unwrap();
    assert_eq!(v.len(), 2);
    assert!(v[0].as_str().unwrap() < v[1].as_str().unwrap());
    // A VolumeSource has no name: pods differing only in volume names match.
    let renamed = pod(json!({"containers": [], "volumes": [
        {"name": "q", "hostPath": {"path": "/z"}}, {"name": "r", "emptyDir": {}}
    ]}));
    assert_eq!(volumes_signer(&renamed), volumes_signer(&mixed));
}

#[test]
fn node_affinity_signer_cases() {
    assert_eq!(
        node_affinity_signer(&pod(json!({"containers": []}))),
        Value::Null
    );
    let empty = pod(json!({"containers": [], "affinity": {"nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": []}}}}));
    assert_eq!(
        node_affinity_signer(&empty),
        json!({"Required": [], "Preferred": []})
    );
    let p = pod(json!({"containers": [], "affinity": {"nodeAffinity": {
    "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
        {"matchExpressions": [
            {"key": "kk3", "operator": "In", "values": ["v3", "kv4"]},
            {"key": "kk2", "operator": "In", "values": ["kv1", "v2"]}],
         "matchFields": [{"key": "kk1", "operator": "In", "values": ["kv3", "v4"]}]},
        {"matchExpressions": [{"key": "k2", "operator": "In", "values": ["v1", "v2"]}],
         "matchFields": [{"key": "k1", "operator": "In", "values": ["v3", "v4"]}]}
    ]},
    "preferredDuringSchedulingIgnoredDuringExecution": [
        {"weight": 3, "preference": {
            "matchExpressions": [{"key": "ppk2", "operator": "In", "values": ["ppv1", "v2"]}],
            "matchFields": [{"key": "ppk1", "operator": "In", "values": ["ppv3", "v4"]}]}},
        {"weight": 1, "preference": {
            "matchExpressions": [{"key": "pk2", "operator": "In", "values": ["pv1", "v2"]}],
            "matchFields": [{"key": "pk1", "operator": "In", "values": ["pv3", "v4"]}]}}
    ]}}}));
    let want = json!({
        "Required": [
            r#"{"MatchExpressions":["{\"key\":\"k2\",\"operator\":\"In\",\"values\":[\"v1\",\"v2\"]}"],"MatchFields":["{\"key\":\"k1\",\"operator\":\"In\",\"values\":[\"v3\",\"v4\"]}"]}"#,
            r#"{"MatchExpressions":["{\"key\":\"kk2\",\"operator\":\"In\",\"values\":[\"kv1\",\"v2\"]}","{\"key\":\"kk3\",\"operator\":\"In\",\"values\":[\"kv4\",\"v3\"]}"],"MatchFields":["{\"key\":\"kk1\",\"operator\":\"In\",\"values\":[\"kv3\",\"v4\"]}"]}"#
        ],
        "Preferred": [
            r#"{"Weight":1,"Preference":{"MatchExpressions":["{\"key\":\"pk2\",\"operator\":\"In\",\"values\":[\"pv1\",\"v2\"]}"],"MatchFields":["{\"key\":\"pk1\",\"operator\":\"In\",\"values\":[\"pv3\",\"v4\"]}"]}}"#,
            r#"{"Weight":3,"Preference":{"MatchExpressions":["{\"key\":\"ppk2\",\"operator\":\"In\",\"values\":[\"ppv1\",\"v2\"]}"],"MatchFields":["{\"key\":\"ppk1\",\"operator\":\"In\",\"values\":[\"ppv3\",\"v4\"]}"]}}"#
        ]
    });
    assert_eq!(node_affinity_signer(&p), want);
}

#[test]
fn plugins_refuse_pods_they_cannot_sign() {
    let dra =
        pod(json!({"containers": [], "resourceClaims": [{"name": "r", "resourceClaimName": "c"}]}));
    assert_eq!(
        DynamicResources.sign_pod(&dra).1.code(),
        Code::Unschedulable
    );
    let tsc = pod(json!({"containers": [], "topologySpreadConstraints": [
        {"maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule"}]}));
    assert_eq!(
        PodTopologySpread.sign_pod(&tsc).1.code(),
        Code::Unschedulable
    );
    let ipa = pod(json!({"containers": [], "affinity": {"podAffinity": {}}}));
    assert_eq!(
        InterPodAffinity.sign_pod(&ipa).1.code(),
        Code::Unschedulable
    );
    // Signable pods yield fragments.
    let plain = pod(json!({"containers": [], "nodeName": "n"}));
    assert!(InterPodAffinity.sign_pod(&plain).1.is_success());
    assert_eq!(NodeName.sign_pod(&plain).0[0].value, json!("n"));
    assert_eq!(NodePorts.sign_pod(&plain).0[0].value, json!([]));
}

fn objects_only_profile() -> Profile {
    let one = |n: &[&str]| PluginSet {
        enabled: n.iter().map(|n| Plugin::new(n)).collect(),
        disabled: Vec::new(),
    };
    let mut p = Profile::named("objects");
    p.plugins = Some(Plugins {
        queue_sort: one(&["PrioritySort"]),
        bind: one(&["DefaultBinder"]),
        pre_filter: one(&["NodePorts"]),
        filter: one(&[
            "NodeName",
            "NodePorts",
            "InterPodAffinity",
            "NodeResourcesFit",
        ]),
        ..Default::default()
    });
    Map::new_map(vec![p])
        .unwrap()
        .get("objects")
        .unwrap()
        .clone()
}

#[test]
fn sign_pod_combines_the_plugins_fragments() {
    let fw = objects_only_profile().framework;
    assert!(fw.signatures_enabled());
    let a = pod(json!({"containers": [{"name": "c"}], "schedulerName": "objects"}));
    let b = pod(json!({"containers": [{"name": "other"}], "schedulerName": "objects"}));
    let sig = fw.sign_pod(&a).expect("signable");
    assert_eq!(Some(&sig), fw.sign_pod(&b).as_ref(), "same signature");
    let parsed: Value = serde_json::from_slice(&sig).unwrap();
    assert_eq!(parsed[SCHEDULER_NAME_SIGNER_NAME], json!("objects"));
    assert_eq!(parsed[HOST_PORTS_SIGNER_NAME], json!([]));
    assert_eq!(parsed[LABELS_SIGNER_NAME], json!({"a": "b"}));

    // A different host port changes the signature.
    let c = pod(
        json!({"containers": [{"name": "c", "ports": [{"containerPort": 1, "hostPort": 80}]}],
        "schedulerName": "objects"}),
    );
    assert_ne!(Some(&sig), fw.sign_pod(&c).as_ref());

    // A plugin that refuses makes the pod unsignable.
    let ipa = pod(json!({"containers": [], "affinity": {"podAntiAffinity": {}}}));
    assert_eq!(fw.sign_pod(&ipa), None);
}

#[test]
fn signatures_are_disabled_when_a_plugin_cannot_sign() {
    // The default profile still has plugins that are not plugin objects (the
    // Score plugins), so upstream's `computeBatchablePlugins` disables
    // signatures.
    let fw = Profile::named("default-scheduler").framework;
    assert!(!fw.signatures_enabled());
    assert_eq!(fw.sign_pod(&pod(json!({"containers": []}))), None);
}
