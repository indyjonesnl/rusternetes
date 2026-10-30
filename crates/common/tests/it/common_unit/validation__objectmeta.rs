//! Unit tests moved from `src/validation/objectmeta.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{ObjectMeta, OwnerReference};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_dns1123_subdomain, is_qualified_name, validate_labels,
        validate_managed_fields,
    };
    pub use std::collections::HashMap;
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::objectmeta::*;

    #[test]
    fn mask_trailing_dash_replaces_dash_with_a() {
        // Upstream Go: `name[:len-2] + "a"`. "foo-" → "fo" + "a" = "foa".
        assert_eq!(mask_trailing_dash("foo-"), "foa");
        assert_eq!(mask_trailing_dash("foo"), "foo");
        // Single-char `-` is left alone (upstream's `len > 1` guard).
        assert_eq!(mask_trailing_dash("-"), "-");
    }

    #[test]
    fn render_timestamp_matches_upstream() {
        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        assert_eq!(render_timestamp(ts), "1970-01-01T00:16:40Z");
    }

    /// Port of upstream `TestValidatePathSegmentName`
    /// (`apimachinery/pkg/api/validation/path/name_test.go:24-131`).
    #[test]
    fn validate_path_segment_name_upstream_cases() {
        let cases: &[(&str, &str, bool, &str)] = &[
            ("empty", "", false, ""),
            ("empty,prefix", "", true, ""),
            ("valid", "foo.bar.baz", false, ""),
            ("valid,prefix", "foo.bar.baz", true, ""),
            (
                "valid complex",
                "sha256:ABCDEF012345@ABCDEF012345",
                false,
                "",
            ),
            ("valid extended charset", "Iñtërnâtiônàlizætiøn", false, ""),
            ("dot", ".", false, "."),
            ("dot leading", ".test", false, ""),
            ("dot,prefix", ".", true, ""),
            ("dot dot", "..", false, ".."),
            ("dot dot leading", "..test", false, ""),
            ("dot dot,prefix", "..", true, ""),
            ("slash", "foo/bar", false, "/"),
            ("slash,prefix", "foo/bar", true, "/"),
            ("percent", "foo%bar", false, "%"),
            ("percent,prefix", "foo%bar", true, "%"),
        ];
        for (k, name, prefix, expected) in cases {
            let msgs = validate_path_segment_name(name, *prefix);
            if expected.is_empty() {
                assert!(msgs.is_empty(), "{k}: expected no message, got {msgs:?}");
            } else {
                assert!(!msgs.is_empty(), "{k}: expected error message, got none");
                assert!(
                    msgs[0].contains(expected),
                    "{k}: expected message containing {expected:?}, got {:?}",
                    msgs[0]
                );
            }
        }
    }

    /// Port of upstream `TestValidateWithMultiErrors` (`name_test.go:133-168`).
    #[test]
    fn validate_path_segment_name_multi_errors() {
        for prefix in [false, true] {
            assert_eq!(
                validate_path_segment_name("foo//bar%", prefix),
                vec!["may not contain '/'", "may not contain '%'"],
                "prefix={prefix}"
            );
        }
    }

    /// `ValidateObjectMeta` calls the name func with `prefix = true` for
    /// `generateName` (`apimachinery/pkg/api/validation/objectmeta.go`), so a
    /// generateName of `.`/`..` passes `path.ValidatePathSegmentName` (#3734).
    #[test]
    fn generate_name_dot_passes_path_segment_validation() {
        for gn in [".", ".."] {
            let meta = ObjectMeta {
                name: "ok".to_string(),
                generate_name: Some(gn.to_string()),
                ..Default::default()
            };
            let errs = validate_object_meta(
                &meta,
                false,
                validate_path_segment_name,
                &Path::new("metadata"),
            );
            assert!(errs.is_empty(), "generateName {gn:?}: {errs:?}");
        }
    }

    /// `ValidateRBACName` ignores `prefix` and always applies
    /// `IsValidPathSegmentName` (`pkg/apis/rbac/validation/validation.go:32-34`).
    #[test]
    fn name_is_path_segment_ignores_prefix_like_validate_rbac_name() {
        assert_eq!(name_is_path_segment(".", true), vec!["may not be '.'"]);
        assert_eq!(name_is_path_segment("..", true), vec!["may not be '..'"]);
    }
}
