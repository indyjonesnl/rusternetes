//! Unit tests moved from `src/error.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::field::ErrorList;
    pub use thiserror::Error;
}

#[cfg(feature = "axum-support")]
mod tests {
    /// `NewGenericServerResponse(422, "", GroupResource{}, "", msg, 0, false)`
    /// (errors.go:476-478, :511-523): reason Invalid, canned message (the
    /// server message is dropped), empty details.
    #[test]
    fn generic_server_response_422_is_canned_with_empty_details() {
        let Error::Status(s) =
            Error::generic_server_response(422, "", "", "", "", "boom: not found", 0, false)
        else {
            panic!("expected Status");
        };
        assert_eq!(s.code, Some(422));
        assert_eq!(s.reason.as_deref(), Some("Invalid"));
        assert_eq!(
            s.message.as_deref(),
            Some("the server rejected our request due to an error in our request")
        );
        assert_eq!(
            serde_json::to_value(s.details.as_ref().unwrap()).unwrap(),
            serde_json::json!({})
        );
    }

    /// errors.go:437-524: verb-dependent 409, the qualified-resource suffix
    /// (with and without a name), the >=500 wording, unexpected-response
    /// causes and retry-after.
    #[test]
    fn generic_server_response_follows_the_upstream_switch() {
        let st = |e: Error| match e {
            Error::Status(s) => *s,
            other => panic!("expected Status, got {other:?}"),
        };
        let s = st(Error::generic_server_response(
            409,
            "POST",
            "apps",
            "deployments",
            "d",
            "x",
            0,
            false,
        ));
        assert_eq!(s.reason.as_deref(), Some("AlreadyExists"));
        assert_eq!(
            s.message.as_deref(),
            Some("the server reported a conflict (post deployments.apps d)")
        );
        let s = st(Error::generic_server_response(
            409, "GET", "", "pods", "", "x", 0, false,
        ));
        assert_eq!(s.reason.as_deref(), Some("Conflict"));
        assert_eq!(
            s.message.as_deref(),
            Some("the server reported a conflict (get pods)")
        );
        let s = st(Error::generic_server_response(
            500, "", "", "", "", "oops", 10, true,
        ));
        assert_eq!(s.reason.as_deref(), Some("InternalError"));
        assert_eq!(
            s.message.as_deref(),
            Some("an error on the server (\"oops\") has prevented the request from succeeding")
        );
        let d = s.details.unwrap();
        assert_eq!(d.retry_after_seconds, Some(10));
        let c = &d.causes.unwrap()[0];
        assert_eq!(c.reason.as_deref(), Some("UnexpectedServerResponse"));
        assert_eq!(c.message.as_deref(), Some("oops"));
        let s = st(Error::generic_server_response(
            418, "", "", "", "", "", 0, false,
        ));
        assert_eq!(s.reason.as_deref(), Some("Unknown"));
        assert_eq!(
            s.message.as_deref(),
            Some(
                "the server responded with the status code 418 but did not return more information"
            )
        );
        let s = st(Error::generic_server_response(
            406, "", "", "", "", "unknown", 0, false,
        ));
        assert_eq!(
            s.message.as_deref(),
            Some("the server was unable to respond with a content type that the client supports")
        );
    }

    // endpoints/filters/authorization.go:87-91: an authorizer error that did not
    // come with an Allow is `responsewriters.InternalError` (500), not Forbidden.
    #[test]
    fn authorizer_error_is_a_500_internal_error() {
        use axum::response::IntoResponse;
        let e = Error::Authorization("webhook failed".into());
        assert_eq!(e.reason(), "InternalError");
        assert_eq!(
            e.into_response().status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn too_large_storage_error_is_recognised_and_is_a_500() {
        use axum::response::IntoResponse;
        let e = Error::StorageTooLarge("etcdserver: request is too large".into());
        assert!(e.is_too_large_error());
        assert!(!Error::Storage("etcdserver: request is too large".into()).is_too_large_error());
        assert_eq!(e.reason(), "InternalError");
        assert_eq!(
            e.into_response().status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    use super::__parent::*;
    use rusternetes_common::error::*;
    use rusternetes_common::validation::field::{ErrorType, Path};

    /// `TestNewInvalid` (apimachinery pkg/api/errors/errors_test.go:124-220).
    #[test]
    fn new_invalid_matches_upstream_test_new_invalid() {
        let path = Path::new("field[0].name");
        let cases: Vec<(rusternetes_common::validation::field::Error, &str, &str)> = vec![
            (
                rusternetes_common::validation::field::Error::duplicate(&path, "bar"),
                "FieldValueDuplicate",
                r#"Kind "name" is invalid: field[0].name: Duplicate value: "bar""#,
            ),
            (
                rusternetes_common::validation::field::Error::invalid(&path, "bar", "detail"),
                "FieldValueInvalid",
                r#"Kind "name" is invalid: field[0].name: Invalid value: "bar": detail"#,
            ),
            (
                rusternetes_common::validation::field::Error::required(&path, ""),
                "FieldValueRequired",
                r#"Kind "name" is invalid: field[0].name: Required value"#,
            ),
        ];
        for (err, reason, msg) in cases {
            let body = err.error_body();
            let Error::Status(st) = Error::new_invalid("", "Kind", "name", vec![err]) else {
                panic!("not a Status")
            };
            assert_eq!(st.code, Some(422));
            assert_eq!(st.reason.as_deref(), Some("Invalid"));
            assert_eq!(st.message.as_deref(), Some(msg));
            let d = st.details.unwrap();
            assert_eq!(d.kind.as_deref(), Some("Kind"));
            assert_eq!(d.name.as_deref(), Some("name"));
            let c = &d.causes.unwrap()[0];
            assert_eq!(c.reason.as_deref(), Some(reason));
            assert_eq!(c.field.as_deref(), Some("field[0].name"));
            assert_eq!(c.message.as_deref(), Some(body.as_str()));
        }
    }

    /// `TestNewInvalid` cases beyond the three above (errors_test.go:124-220):
    /// NotFound, NotSupported, Required and the nil error list, plus
    /// `Cause.Type = CauseType(err.Type)` (errors.go:289), which makes an
    /// `Internal` error `InternalError` and a `TypeInvalid` one
    /// `FieldValueTypeInvalid` rather than collapsing them to
    /// `FieldValueInvalid`.
    #[test]
    fn new_invalid_matches_upstream_remaining_cases() {
        use rusternetes_common::validation::field::{Error as FE, ErrorType};
        let p = Path::new("field[0].name");
        let cases: Vec<(FE, &str, &str)> = vec![
            (
                FE::not_found(&p, "bar"),
                "FieldValueNotFound",
                r#"Kind "name" is invalid: field[0].name: Not found: "bar""#,
            ),
            (
                FE::not_supported(&p, "bar", &[] as &[&str]),
                "FieldValueNotSupported",
                r#"Kind "name" is invalid: field[0].name: Unsupported value: "bar""#,
            ),
            (
                FE::internal(&p, "boom"),
                "InternalError",
                r#"Kind "name" is invalid: field[0].name: Internal error: boom"#,
            ),
        ];
        for (err, reason, msg) in cases {
            let Error::Status(st) = Error::new_invalid("", "Kind", "name", vec![err]) else {
                panic!("not a Status")
            };
            assert_eq!(st.message.as_deref(), Some(msg));
            let c = &st.details.unwrap().causes.unwrap()[0];
            assert_eq!(c.reason.as_deref(), Some(reason), "{msg}");
        }
        assert_eq!(
            ErrorType::TypeInvalid.cause_reason(),
            "FieldValueTypeInvalid"
        );
        assert_eq!(ErrorType::Internal.cause_reason(), "InternalError");

        // `IsInvalid(NewInvalid(kind("Test"), "2", nil))` (errors_test.go:72).
        let Error::Status(st) = Error::new_invalid("", "Test", "2", vec![]) else {
            panic!()
        };
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
        assert!(st.details.unwrap().causes.is_none());
    }

    /// `StatusDetails.Name`/`Kind` are `omitempty` (meta/v1 types.go), so an
    /// empty name or kind must not serialize (delete.go:81 passes name "").
    #[test]
    fn new_invalid_omits_empty_name_and_kind() {
        let e = rusternetes_common::validation::field::Error::invalid(&Path::new("f"), "x", "d");
        let Error::Status(st) = Error::new_invalid("", "", "", vec![e]) else {
            panic!("expected Status");
        };
        let v = serde_json::to_value(st.details.unwrap()).unwrap();
        assert!(v.get("name").is_none(), "{v}");
        assert!(v.get("kind").is_none(), "{v}");
    }

    #[test]
    fn new_invalid_with_no_errors_and_with_several() {
        let Error::Status(st) = Error::new_invalid("", "Kind", "name", vec![]) else {
            panic!()
        };
        assert_eq!(st.message.as_deref(), Some(r#"Kind "name" is invalid"#));
        let p = Path::new("a");
        let e1 = rusternetes_common::validation::field::Error::required(&p, "");
        let e2 = rusternetes_common::validation::field::Error::required(&Path::new("b"), "");
        // Duplicate messages are dropped by `ToAggregate`; several render in
        // brackets, `, `-joined (util/errors/errors.go:70-94).
        let Error::Status(st) =
            Error::new_invalid("apps", "Deployment", "d", vec![e1.clone(), e1, e2])
        else {
            panic!()
        };
        assert_eq!(
            st.message.as_deref(),
            Some(r#"Deployment.apps "d" is invalid: [a: Required value, b: Required value]"#)
        );
        assert_eq!(st.details.unwrap().group.as_deref(), Some("apps"));
    }

    #[test]
    fn parse_legacy_required() {
        let (ty, field) = parse_legacy_field_error("spec.containers: Required value").unwrap();
        assert_eq!(ty, ErrorType::Required);
        assert_eq!(field, "spec.containers");
    }

    #[test]
    fn parse_legacy_duplicate_with_value() {
        let (ty, field) =
            parse_legacy_field_error("spec.containers[1].name: Duplicate value: \"ctr-a\"")
                .unwrap();
        assert_eq!(ty, ErrorType::Duplicate);
        assert_eq!(field, "spec.containers[1].name");
    }

    #[test]
    fn parse_legacy_forbidden() {
        let (ty, field) = parse_legacy_field_error(
            "spec.ephemeralContainers: Forbidden: cannot be set on create",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::Forbidden);
        assert_eq!(field, "spec.ephemeralContainers");
    }

    #[test]
    fn parse_legacy_invalid_with_detail() {
        let (ty, field) = parse_legacy_field_error(
            "spec.nodeName: Invalid value: \"node-2\": field is immutable",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::Invalid);
        assert_eq!(field, "spec.nodeName");
    }

    #[test]
    fn parse_legacy_unsupported() {
        let (ty, field) = parse_legacy_field_error(
            "spec.restartPolicy: Unsupported value: \"InvalidPolicy\": supported values: \"Always\", \"OnFailure\", \"Never\"",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::NotSupported);
        assert_eq!(field, "spec.restartPolicy");
    }

    #[test]
    fn build_details_from_error_list_one_cause_per_error() {
        let p = Path::new("spec");
        let errs = vec![
            rusternetes_common::validation::field::Error::required(&p.child("containers"), ""),
            rusternetes_common::validation::field::Error::not_supported(
                &p.child("restartPolicy"),
                "InvalidPolicy",
                &["Always", "OnFailure", "Never"],
            ),
        ];
        let details = build_invalid_details_from_errors(&errs);
        let causes = details.causes.unwrap();
        assert_eq!(causes.len(), 2);
        assert_eq!(causes[0].reason.as_deref(), Some("FieldValueRequired"));
        assert_eq!(causes[0].field.as_deref(), Some("spec.containers"));
        assert_eq!(causes[1].reason.as_deref(), Some("FieldValueNotSupported"));
        assert_eq!(causes[1].field.as_deref(), Some("spec.restartPolicy"));
    }
}
