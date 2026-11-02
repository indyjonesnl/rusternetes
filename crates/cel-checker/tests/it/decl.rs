//! `escaping_test.go` (`TestEscaping`) and the `DeclType` / `DeclTypeProvider`
//! behavior of `types.go`, through the checker.

use cel::parser::Parser;
use rusternetes_cel_checker::decl::{escape, DeclField, DeclType, DeclTypeProvider};
use rusternetes_cel_checker::{
    check, Container, DType, EnvSet, EnvType, Features, MapProvider, NoLocations, VariableDecl,
    Version,
};
use std::collections::BTreeMap;
use std::sync::Arc;

#[test]
fn escaping() {
    let cases = [
        ("a.a", "a__dot__a"),
        ("a-a", "a__dash__a"),
        ("a__a", "a__underscores__a"),
        ("a.-/__a", "a__dot____dash____slash____underscores__a"),
        ("a._a", "a__dot___a"),
        ("a__.__a", "a__underscores____dot____underscores__a"),
        ("a___a", "a__underscores___a"),
        ("a____a", "a__underscores____underscores__a"),
        ("a__dot__a", "a__underscores__dot__underscores__a"),
        (
            "a__underscores__a",
            "a__underscores__underscores__underscores__a",
        ),
        ("true", "__true__"),
        ("in", "__in__"),
        ("namespace", "__namespace__"),
        ("while", "__while__"),
        ("has", "has"),
        ("exists_one", "exists_one"),
        ("getSeconds", "getSeconds"),
        ("_if", "_if"),
        ("_anything", "_anything"),
    ];
    for (unescaped, escaped) in cases {
        assert_eq!(escape(unescaped).as_deref(), Some(escaped), "{unescaped}");
    }
    for unescapable in ["@", "1up", "\u{1F451}", ""] {
        assert_eq!(escape(unescapable), None, "{unescapable}");
    }
}

fn field(name: &str, ty: DeclType) -> (String, DeclField) {
    (
        name.to_string(),
        DeclField {
            name: name.to_string(),
            ty,
            required: false,
        },
    )
}

/// `{spec: {replicas: int, tags: list(string), labels: map(string),
/// items: list({name: string}), in: bool}}`
fn root() -> DeclType {
    let item = DeclType::object(
        "object",
        BTreeMap::from([field("name", DeclType::simple(DType::Str))]),
    );
    let spec = DeclType::object(
        "object",
        BTreeMap::from([
            field("replicas", DeclType::simple(DType::Int)),
            field("tags", DeclType::list(DeclType::simple(DType::Str), -1)),
            field("labels", DeclType::map(DeclType::simple(DType::Str), -1)),
            field("items", DeclType::list(item, -1)),
            field("__in__", DeclType::simple(DType::Bool)),
        ]),
    );
    DeclType::object("object", BTreeMap::from([field("spec", spec)]))
}

fn errors(src: &str, minor: u32) -> Vec<String> {
    let scoped = root().maybe_assign_type_name("selfType0");
    let provider = DeclTypeProvider::new(
        std::slice::from_ref(&scoped),
        Arc::new(MapProvider::new()),
        minor >= 31,
    );
    let set = EnvSet::must_base_env_set(Version::major_minor(1, minor), &Features::default());
    let mut env = set.env(
        EnvType::NewExpressions,
        Container::default(),
        Arc::new(provider),
    );
    env.add_ident(VariableDecl::new("self", scoped.dtype()))
        .unwrap();
    let expr = Parser::new().parse(src).unwrap();
    let (_, errs) = check(&expr, &mut env, &NoLocations);
    errs.errors().iter().map(|e| e.message.clone()).collect()
}

#[test]
fn unique_type_names_follow_the_path() {
    let scoped = root().maybe_assign_type_name("selfType0");
    assert_eq!(scoped.dtype(), DType::Struct("selfType0".into()));
    let fields = scoped.fields().unwrap();
    assert_eq!(
        fields["spec"].ty.dtype(),
        DType::Struct("selfType0.spec".into())
    );
    let fields = fields["spec"].ty.fields().unwrap();
    assert_eq!(
        fields["items"].ty.dtype(),
        DType::List(Box::new(DType::Struct("selfType0.spec.items.@idx".into())))
    );
}

#[test]
fn field_selection_is_typed_by_the_schema() {
    assert!(errors("self.spec.replicas > 1", 35).is_empty());
    assert!(errors("self.spec.tags.all(t, t.startsWith('a'))", 35).is_empty());
    assert!(errors("self.spec.items.exists(i, i.name == 'x')", 35).is_empty());
    assert!(errors("self.spec.labels['a'] == 'b'", 35).is_empty());
    assert!(errors("self.spec.labels.anything == 'b'", 35).is_empty());
    assert_eq!(
        errors("self.spec.missing", 35),
        ["undefined field 'missing'"]
    );
    assert_eq!(
        errors("self.spec.replicas == 'a'", 35),
        ["found no matching overload for '_==_' applied to '(int, string)'"]
    );
    assert_eq!(errors("self.nope.x", 35), ["undefined field 'nope'"]);
}

#[test]
fn escaped_names_are_the_field_names() {
    // A property called `in` is exposed as `__in__`.
    assert!(errors("has(self.spec.__in__)", 35).is_empty());
    assert!(errors("self.spec.__in__ == true", 35).is_empty());
}
