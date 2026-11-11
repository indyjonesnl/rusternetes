//! `TestFilterByPattern` of structured-merge-diff `fieldpath/set_test.go`
//! (v6.3.0), over `Set::filter_include_matches` / `Filter`.

use k3rs_common::fieldmanager::fieldset::{Filter, PathElementMatcher, Set, SetMatcher};
use serde_json::json;

/// A path element as a test spells it.
enum E<'a> {
    F(&'a str),
    I(usize),
    V(&'a str),
    K(&'a [(&'a str, &'a str)]),
}

fn path(parts: &[E]) -> Vec<String> {
    parts
        .iter()
        .map(|p| match p {
            E::F(n) => format!("f:{n}"),
            E::I(i) => format!("i:{i}"),
            E::V(v) => format!("v:{}", json!(v)),
            E::K(keys) => {
                let m: std::collections::BTreeMap<_, _> = keys.iter().copied().collect();
                format!("k:{}", serde_json::to_string(&m).unwrap())
            }
        })
        .collect()
}

fn set(paths: &[&[E]]) -> Set {
    let mut s = Set::new();
    for p in paths {
        s.insert(&path(p));
    }
    s
}

fn m_field(n: &str) -> PathElementMatcher {
    PathElementMatcher::field(n)
}

fn prefix(parts: Vec<PathElementMatcher>) -> SetMatcher {
    SetMatcher::prefix(&parts)
}

use E::{F, I, K, V};

fn containers_resources() -> Filter {
    Filter::include_matcher(&[prefix(vec![
        m_field("spec"),
        m_field("containers"),
        PathElementMatcher::Any,
        m_field("resources"),
    ])])
}

#[test]
fn container_resize_fields_exact_match() {
    let input = set(&[
        &[F("spec")],
        &[F("spec"), F("containers")],
        &[F("spec"), F("containers"), I(0)],
        &[F("spec"), F("containers"), I(0), F("resources")],
        &[
            F("spec"),
            F("containers"),
            I(0),
            F("resources"),
            F("limits"),
        ],
        &[
            F("spec"),
            F("containers"),
            I(0),
            F("resources"),
            F("limits"),
            F("cpu"),
        ],
        &[
            F("spec"),
            F("containers"),
            I(0),
            F("resources"),
            F("claims"),
            I(0),
            F("name"),
        ],
        &[F("spec"), F("containers"), I(1)],
        &[F("spec"), F("containers"), I(1), F("resources")],
    ]);
    assert_eq!(containers_resources().filter(&input), input);
}

#[test]
fn filter_status_and_metadata() {
    let input = set(&[&[F("spec")], &[F("status")], &[F("metadata")]]);
    assert_eq!(containers_resources().filter(&input), set(&[&[F("spec")]]));
}

#[test]
fn filter_non_container_spec_fields() {
    let input = set(&[
        &[F("spec")],
        &[F("spec"), F("volumes")],
        &[F("spec"), F("hostNetwork")],
    ]);
    assert_eq!(containers_resources().filter(&input), set(&[&[F("spec")]]));
}

#[test]
fn filter_non_resource_container_fields() {
    let input = set(&[
        &[F("spec")],
        &[F("spec"), F("containers")],
        &[F("spec"), F("containers"), I(0)],
        &[F("spec"), F("containers"), I(0), F("image")],
        &[F("spec"), F("containers"), I(0), F("workingDir")],
        &[F("spec"), F("containers"), I(0), F("resources")],
    ]);
    let expect = set(&[
        &[F("spec")],
        &[F("spec"), F("containers")],
        &[F("spec"), F("containers"), I(0)],
        &[F("spec"), F("containers"), I(0), F("resources")],
    ]);
    assert_eq!(containers_resources().filter(&input), expect);
}

#[test]
fn filter_list_map_key() {
    let k1: &[(&str, &str)] = &[("key1", "value1"), ("key2", "value2")];
    let k2: &[(&str, &str)] = &[("key1", "valueX"), ("key2", "valueY")];
    let input = set(&[
        &[F("spec")],
        &[F("spec"), F("listMap"), K(k1)],
        &[F("spec"), F("listMap"), K(k1), F("field")],
        &[F("spec"), F("listMap"), K(k2), F("field")],
    ]);
    let key = PathElementMatcher::key(&[("key1", json!("value1")), ("key2", json!("value2"))])
        .expect("a key");
    let filter = Filter::include_matcher(&[prefix(vec![m_field("spec"), m_field("listMap"), key])]);
    let expect = set(&[
        &[F("spec")],
        &[F("spec"), F("listMap"), K(k1)],
        &[F("spec"), F("listMap"), K(k1), F("field")],
    ]);
    assert_eq!(filter.filter(&input), expect);
}

#[test]
fn a_list_key_needs_at_least_one_key() {
    assert!(PathElementMatcher::key(&[]).is_none());
}

#[test]
fn filter_value() {
    let input = set(&[
        &[F("spec")],
        &[F("spec"), F("set"), V("v1")],
        &[F("spec"), F("set"), V("v2")],
    ]);
    let filter = Filter::include_matcher(&[prefix(vec![
        m_field("spec"),
        m_field("set"),
        PathElementMatcher::value(&json!("v1")),
    ])]);
    let expect = set(&[&[F("spec")], &[F("spec"), F("set"), V("v1")]]);
    assert_eq!(filter.filter(&input), expect);
}

fn list_input(n: usize, leaf: &[&str]) -> Set {
    let mut s = set(&[&[F("spec")], &[F("spec"), F("list")]]);
    for i in 0..n {
        let mut p = vec![F("spec"), F("list"), I(i)];
        s.insert(&path(&p));
        for l in leaf {
            p.push(F(l));
            s.insert(&path(&p));
            p.pop();
        }
    }
    s
}

#[test]
fn filter_by_index() {
    let filter = Filter::include_matcher(&[prefix(vec![
        m_field("spec"),
        m_field("list"),
        PathElementMatcher::index(1),
        m_field("value"),
    ])]);
    let expect = set(&[
        &[F("spec")],
        &[F("spec"), F("list")],
        &[F("spec"), F("list"), I(1)],
        &[F("spec"), F("list"), I(1), F("value")],
    ]);
    assert_eq!(filter.filter(&list_input(2, &["value"])), expect);
}

#[test]
fn multiple_index_matchers() {
    let one = |i| {
        prefix(vec![
            m_field("spec"),
            m_field("list"),
            PathElementMatcher::index(i),
            m_field("value"),
        ])
    };
    let filter = Filter::include_matcher(&[one(0), one(1)]);
    let expect = set(&[
        &[F("spec")],
        &[F("spec"), F("list")],
        &[F("spec"), F("list"), I(0)],
        &[F("spec"), F("list"), I(0), F("value")],
        &[F("spec"), F("list"), I(1)],
        &[F("spec"), F("list"), I(1), F("value")],
    ]);
    assert_eq!(filter.filter(&list_input(3, &["value"])), expect);
}

#[test]
fn multiple_field_matchers() {
    let input = set(&[
        &[F("spec")],
        &[F("spec"), F("f1")],
        &[F("spec"), F("f1"), F("f11")],
        &[F("spec"), F("f2")],
        &[F("spec"), F("f2"), F("f21")],
        &[F("spec"), F("f3")],
        &[F("spec"), F("f3"), F("f31")],
    ]);
    let filter = Filter::include_matcher(&[
        prefix(vec![m_field("spec"), m_field("f1")]),
        prefix(vec![m_field("spec"), m_field("f3")]),
    ]);
    let expect = set(&[
        &[F("spec")],
        &[F("spec"), F("f1")],
        &[F("spec"), F("f1"), F("f11")],
        &[F("spec"), F("f3")],
        &[F("spec"), F("f3"), F("f31")],
    ]);
    assert_eq!(filter.filter(&input), expect);
}

#[test]
fn wildcard_takes_precedence() {
    let filter = Filter::include_matcher(&[
        prefix(vec![
            m_field("spec"),
            m_field("list"),
            PathElementMatcher::Any,
            m_field("f1"),
        ]),
        prefix(vec![
            m_field("spec"),
            m_field("list"),
            PathElementMatcher::index(1),
            m_field("f2"),
        ]),
    ]);
    let expect = set(&[
        &[F("spec")],
        &[F("spec"), F("list")],
        &[F("spec"), F("list"), I(0)],
        &[F("spec"), F("list"), I(0), F("f1")],
        &[F("spec"), F("list"), I(1)],
        &[F("spec"), F("list"), I(1), F("f1")],
        &[F("spec"), F("list"), I(2)],
        &[F("spec"), F("list"), I(2), F("f1")],
    ]);
    assert_eq!(filter.filter(&list_input(3, &["f1", "f2"])), expect);
}

#[test]
fn no_matcher_includes_everything() {
    let input = set(&[&[F("spec"), F("a")], &[F("status")]]);
    assert_eq!(Filter::include_matcher(&[]).filter(&input), input);
}

#[test]
fn an_exclude_filter_removes_a_path_and_what_is_under_it() {
    let input = set(&[&[F("spec"), F("a")], &[F("status"), F("b")], &[F("status")]]);
    let filter = Filter::Exclude(set(&[&[F("status")]]));
    assert_eq!(filter.filter(&input), set(&[&[F("spec"), F("a")]]));
}

/// `installer.go:697-711`: a storage may provide a `ResetFieldsStrategy` or a
/// `ResetFieldsFilterStrategy`, not both.
#[test]
fn a_storage_may_not_provide_both_a_set_and_a_filter() {
    use k3rs_common::fieldmanager::ResetFields;
    let sets = ResetFields::new().with("v1", &[&["status"]]);
    let filters = ResetFields::new().with_filter("v1", Filter::include_matcher(&[]));
    let err = ResetFields::combine(sets.clone(), filters.clone()).unwrap_err();
    assert_eq!(
        err,
        "may not implement both ResetFieldsStrategy and ResetFieldsFilterStrategy"
    );
    assert_eq!(
        ResetFields::combine(sets, ResetFields::new()).unwrap(),
        ResetFields::new().with("v1", &[&["status"]])
    );
    let combined = ResetFields::combine(ResetFields::new(), filters).unwrap();
    assert!(combined.filter_for("v1").is_some());
    assert!(combined.filter_for("v2").is_none());
}
