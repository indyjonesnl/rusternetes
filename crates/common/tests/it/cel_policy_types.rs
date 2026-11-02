//! The hand-built `request` / namespace types and the `variables`
//! composition of the policy environment (`plugin/cel/compile.go`,
//! `composition.go`).

use rusternetes_cel_checker::DType;
use rusternetes_common::cel_policy_types::{
    check_policy_expression, convert_output_type, namespace_type, request_type, DeclType,
    ParamsType, PolicyTypeContext,
};
use rusternetes_common::cel_versions::{self, EnvSet, EnvType};

fn set() -> EnvSet {
    EnvSet::must_base_env_set(cel_versions::default_compatibility_version())
}

fn ctx(variables: &[(&str, &str)]) -> PolicyTypeContext {
    PolicyTypeContext {
        object: DeclType::Simple(DType::Dyn),
        params: ParamsType::None,
        variables: variables
            .iter()
            .map(|(n, e)| (n.to_string(), e.to_string()))
            .collect(),
    }
}

fn errors(c: &PolicyTypeContext, src: &str) -> Vec<String> {
    check_policy_expression(&set(), EnvType::StoredExpressions, c, true, src).unwrap()
}

#[test]
fn request_and_namespace_have_the_upstream_fields() {
    let c = ctx(&[]);
    assert_eq!(
        request_type().dtype(),
        DType::Struct("kubernetes.AdmissionRequest".into())
    );
    assert_eq!(
        namespace_type().dtype(),
        DType::Struct("kubernetes.Namespace".into())
    );
    assert!(errors(&c, "request.userInfo.groups.exists(g, g == 'x')").is_empty());
    assert!(errors(&c, "request.userInfo.extra['a'].size() > 0").is_empty());
    assert!(errors(&c, "request.requestKind.version == 'v1' && request.dryRun").is_empty());
    assert!(errors(&c, "namespaceObject.spec.finalizers.size() == 0").is_empty());
    assert!(errors(
        &c,
        "namespaceObject.status.conditions.all(c, c.status == 'True')"
    )
    .is_empty());
    assert_eq!(errors(&c, "request.uid"), ["undefined field 'uid'"]);
    assert_eq!(
        errors(&c, "namespaceObject.metadata.uid"),
        ["undefined field 'uid'"]
    );
    assert!(errors(&c, "namespaceObject.metadata.UID == ''").is_empty());
}

#[test]
fn variable_types_follow_convert_cel_type_to_decl_type() {
    assert_eq!(convert_output_type(None).dtype(), DType::Dyn);
    assert_eq!(convert_output_type(Some(&DType::Int)).dtype(), DType::Int);
    assert_eq!(
        convert_output_type(Some(&DType::List(Box::new(DType::Str)))).dtype(),
        DType::List(Box::new(DType::Str))
    );
    assert_eq!(
        convert_output_type(Some(&DType::Map(
            Box::new(DType::Str),
            Box::new(DType::Bool)
        )))
        .dtype(),
        DType::Map(Box::new(DType::Str), Box::new(DType::Bool))
    );
    // Anything else (a struct, an optional) is `dyn`.
    assert_eq!(
        convert_output_type(Some(&DType::Struct("x".into()))).dtype(),
        DType::Dyn
    );
}

#[test]
fn variables_see_the_ones_before_them() {
    let c = ctx(&[("a", "1"), ("b", "variables.a + 1"), ("c", "variables.d")]);
    assert!(errors(&c, "variables.b == 2").is_empty());
    assert_eq!(errors(&c, "variables.b == 'x'").len(), 1);
    // `d` is not declared, so `c` does not compile and is `dyn`.
    assert!(errors(&c, "variables.c == 'x'").is_empty());
    assert_eq!(errors(&c, "variables.zzz"), ["undefined field 'zzz'"]);
}

#[test]
fn unparsable_expressions_are_not_checked() {
    assert_eq!(
        check_policy_expression(&set(), EnvType::StoredExpressions, &ctx(&[]), true, "1 +"),
        None
    );
}
