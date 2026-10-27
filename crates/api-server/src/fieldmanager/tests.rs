//! Unit tests of the Update path; the HTTP behaviour is covered by
//! `tests/it/managedfields_update_test.rs`.

use rusternetes_common::resources::ConfigMap;
use serde_json::{json, Value};

use super::fieldset::{Fields, Set};
use super::*;

fn cm(data: Value) -> ConfigMap {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "c", "uid": "u"}, "data": data
    }))
    .unwrap()
}

fn fm(subresource: Option<&str>) -> FieldManager {
    FieldManager::new("v1", subresource, ResetFields::new())
}

/// `SetToFields` / `FieldsToSet` round trip (fields_test.go).
#[test]
fn fields_v1_round_trips() {
    let v = json!({"f:data": {".": {}, "f:k": {}}, "f:spec": {"f:containers": {"k:{\"name\":\"c\"}": {".": {}, "f:image": {}}}}});
    let set = Set::from_root_fields_v1(&v).unwrap();
    assert_eq!(set.to_fields_v1(), v);
    assert!(Set::from_root_fields_v1(&json!({"nope": {}})).is_none());
}

/// A keyed list is walked per item, with the key sorted into the path element
/// (`k:{"name":"c"}`), and `finalizers` is a set of `v:` elements.
#[test]
fn keyed_lists_and_sets_are_walked_per_item() {
    let obj = json!({
        "metadata": {"finalizers": ["a"], "labels": {"x": "y"}},
        "spec": {"containers": [{"name": "c", "image": "i"}], "tolerations": [{"key": "k"}]}
    });
    assert_eq!(
        Fields::of(&obj).set.to_fields_v1(),
        json!({
            "f:metadata": {
                "f:finalizers": {"v:\"a\"": {}},
                "f:labels": {".": {}, "f:x": {}}
            },
            "f:spec": {
                "f:containers": {"k:{\"name\":\"c\"}": {".": {}, "f:image": {}, "f:name": {}}},
                "f:tolerations": {}
            }
        })
    );
}

/// A subresource ignores the managedFields of the request object and records
/// its own name (fieldmanager.go:99-103; buildmanagerinfo.go).
#[test]
fn a_subresource_entry_carries_the_subresource_and_ignores_the_request_entries() {
    let live = fm(None)
        .update(None::<&ConfigMap>, cm(json!({"k": "v"})), "creator")
        .unwrap();
    let mut new = cm(json!({"k": "v", "k2": "v2"}));
    new.metadata.managed_fields = Some(vec![]);
    new.metadata.uid = "u".into();
    let mut live = live;
    live.metadata.uid = "u".into();
    let out = fm(Some("status"))
        .update(Some(&live), new, "kubelet")
        .unwrap();
    let entries = out.metadata.managed_fields.unwrap();
    let status = entries
        .iter()
        .find(|e| e.manager.as_deref() == Some("kubelet"))
        .unwrap();
    assert_eq!(status.subresource.as_deref(), Some("status"));
    assert!(entries
        .iter()
        .any(|e| e.manager.as_deref() == Some("creator")));
}

/// `ResetFieldsStrategy`: a reset path is never owned by an Update manager
/// (update.go:180-198 `ignoreFilter`).
#[test]
fn reset_fields_are_never_owned() {
    let fm = FieldManager::new("v1", None, ResetFields::new().with("v1", &[&["data"]]));
    let out = fm
        .update(None::<&ConfigMap>, cm(json!({"k": "v"})), "creator")
        .unwrap();
    assert!(out.metadata.managed_fields.is_none());
}

/// `prefixFromUserAgent` (create.go:266-282).
#[test]
fn the_user_agent_prefix_is_printable_and_bounded() {
    assert_eq!(prefix_from_user_agent("kubectl/v1.35 (linux)"), "kubectl");
    assert_eq!(prefix_from_user_agent("a\u{7}b/1"), "ab");
    assert_eq!(prefix_from_user_agent(""), "");
    assert_eq!(prefix_from_user_agent(&"x".repeat(300)).len(), 128);
}

fn pod_fields(obj: &Value) -> Value {
    let t = super::schemas::builtin("v1", "Pod").expect("Pod schema");
    Fields::of_typed(obj, &t).set.to_fields_v1()
}

/// Pod `ports` are keyed by `[containerPort, protocol]`, the key `protocol`
/// defaulting to `TCP` from the schema (`x-kubernetes-list-map-keys`); the
/// name table keyed no such list (#2734).
#[test]
fn pod_ports_are_keyed_by_port_and_protocol_with_the_schema_default() {
    let obj = json!({"spec": {"containers": [{"name": "c", "ports": [
        {"containerPort": 53, "protocol": "UDP"},
        {"containerPort": 53}]}]}});
    let ports = &pod_fields(&obj)["f:spec"]["f:containers"]["k:{\"name\":\"c\"}"]["f:ports"];
    assert!(ports
        .get("k:{\"containerPort\":53,\"protocol\":\"UDP\"}")
        .is_some());
    assert!(ports
        .get("k:{\"containerPort\":53,\"protocol\":\"TCP\"}")
        .is_some());
}

/// `nodeSelector` is `x-kubernetes-map-type: atomic` and `tolerations` an
/// atomic list: one leaf each, where the name table made `nodeSelector` a
/// granular map.
#[test]
fn atomic_maps_and_lists_are_one_leaf() {
    let obj = json!({"spec": {"nodeSelector": {"a": "b"}, "tolerations": [{"key": "k"}]}});
    let f = pod_fields(&obj);
    assert_eq!(f["f:spec"]["f:nodeSelector"], json!({}));
    assert_eq!(f["f:spec"]["f:tolerations"], json!({}));
}

/// A Deployment's root comes from the vendored apps/v1 definition, so
/// `spec.selector` (a `LabelSelector`, atomic) is a leaf.
#[test]
fn deployment_selector_is_atomic() {
    let t = super::schemas::builtin("apps/v1", "Deployment").expect("Deployment schema");
    let obj = json!({"spec": {"selector": {"matchLabels": {"a": "b"}}, "replicas": 1}});
    assert_eq!(
        Fields::of_typed(&obj, &t).set.to_fields_v1(),
        json!({"f:spec": {"f:replicas": {}, "f:selector": {}}})
    );
}

/// A custom resource's `x-kubernetes-list-type` / `x-kubernetes-map-type`
/// from its structural schema, including the multi-key list the name table
/// could not express.
#[test]
fn a_custom_resource_schema_drives_the_field_sets() {
    use rusternetes_common::fieldmanager::schema::Type;
    use rusternetes_common::resources::crd::JSONSchemaProps;
    use rusternetes_common::validation::structural::new_structural;
    let props: JSONSchemaProps = serde_json::from_value(json!({
        "type": "object", "properties": {"spec": {"type": "object", "properties": {
            "tags": {"type": "array", "items": {"type": "string"}, "x-kubernetes-list-type": "set"},
            "items": {"type": "array", "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["a", "b"],
                "items": {"type": "object", "properties": {
                    "a": {"type": "string"}, "b": {"type": "string"}, "v": {"type": "string"}}}},
            "pinned": {"type": "object", "x-kubernetes-map-type": "atomic",
                "properties": {"x": {"type": "string"}}},
            "free": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}}
    }))
    .unwrap();
    let t = Type::from_structural(&new_structural(&props).unwrap());
    let obj = json!({"spec": {
        "tags": ["x"],
        "items": [{"a": "1", "b": "2", "v": "3"}],
        "pinned": {"x": "y"},
        "free": {"k": [1, 2]}}});
    assert_eq!(
        Fields::of_typed(&obj, &t).set.to_fields_v1(),
        json!({"f:spec": {
            "f:tags": {"v:\"x\"": {}},
            "f:items": {"k:{\"a\":\"1\",\"b\":\"2\"}": {".": {}, "f:a": {}, "f:b": {}, "f:v": {}}},
            "f:pinned": {},
            "f:free": {"f:k": {}}}})
    );
}

/// An Update takes only the changed port: with the keyed list it is the one
/// `[containerPort, protocol]` item, not the whole list.
#[test]
fn update_with_a_schema_changes_one_keyed_item() {
    let schema = super::schemas::builtin("v1", "Pod");
    let fm = FieldManager::new("v1", None, ResetFields::new()).with_schema(schema);
    let live = json!({"spec": {"containers": [{"name": "c", "ports": [
        {"containerPort": 80, "protocol": "TCP"}, {"containerPort": 80, "protocol": "UDP"}]}]}});
    let new = json!({"spec": {"containers": [{"name": "c", "ports": [
        {"containerPort": 80, "protocol": "TCP"},
        {"containerPort": 80, "protocol": "UDP", "name": "dns"}]}]}});
    let managed = fm
        .update_managed(&live, &new, Managed::default(), "m", true)
        .unwrap();
    let entries = managed.encode().unwrap();
    let fields = entries[0].fields_v1.clone().unwrap();
    let ports = &fields["f:spec"]["f:containers"]["k:{\"name\":\"c\"}"]["f:ports"];
    let udp = &ports["k:{\"containerPort\":80,\"protocol\":\"UDP\"}"];
    assert!(udp.get("f:name").is_some());
    assert!(ports
        .get("k:{\"containerPort\":80,\"protocol\":\"TCP\"}")
        .is_none());
}
