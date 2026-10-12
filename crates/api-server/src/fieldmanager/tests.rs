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
