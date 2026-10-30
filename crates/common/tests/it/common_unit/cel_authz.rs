//! Unit tests moved from `src/cel_authz.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use cel::common::ast::{EntryExpr, Expr, IdedExpr, LiteralValue};
    pub use rusternetes_common::feature_gates::{self, Feature};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cel_authz::*;
    use rusternetes_common::feature_gates::with_feature;
    use serial_test::serial;

    /// compile_test.go `TestCompileCELExpression`.
    #[test]
    fn unknown_field_and_reference() {
        let e = |x: &str| compile_authorization_expression(x).unwrap_err();
        assert!(e("request.time == 'now'").contains("undefined field 'time'"));
        assert!(e("x.user").contains("undeclared reference"));
        assert!(e("request.user").contains("must evaluate to bool but got string"));
        assert!(compile_authorization_expression("request.user == 'bob'").is_ok());
    }

    /// compile_test.go "fieldSelector not enabled" / "labelSelector not enabled".
    #[test]
    #[serial]
    fn selectors_are_undefined_when_the_gate_is_off() {
        let _g = with_feature(Feature::AuthorizeWithSelectors, false);
        for f in ["fieldSelector", "labelSelector"] {
            let e = compile_authorization_expression(&format!(
                "request.resourceAttributes.{f}.rawSelector == 'foo'"
            ))
            .unwrap_err();
            assert!(e.contains(&format!("undefined field '{f}'")), "{e}");
        }
    }
}
