//! Query-parameter decoding shared by every server crate.
//!
//! Kubernetes decodes query-parameter booleans with
//! `runtime.Convert_Slice_string_To_bool`
//! (`staging/src/k8s.io/apimachinery/pkg/runtime/conversion.go:79-95`), whose
//! contract is deliberately permissive:
//!
//! > Only the absence of a value (i.e. zero-length slice), a value of "false",
//! > or a value of "0" resolve to false. Any other value (including empty
//! > string) resolves to true.
//!
//! It is wired to every boolean query parameter by the generated conversions —
//! `watch` and `allowWatchBookmarks` at
//! `apis/meta/v1/zz_generated.conversion.go`, the `*bool` ones (`force`,
//! `orphanDependents`, `sendInitialEvents`,
//! `ignoreStoreReadErrorWithClusterBreakingPotential`) via
//! `Convert_Slice_string_To_Pointer_bool`, and the pod subresource ones
//! (`stdin`, `stdout`, `stderr`, `tty`, `follow`, `previous`, `timestamps`,
//! `insecureSkipTLSVerifyBackend`) in the core conversions.
//!
//! It is NOT Go's `strconv.ParseBool`, and it is not Rust's
//! `str::parse::<bool>()` either. Both of those accept a narrow spelling set
//! and reject everything else, which silently turns `?watch=1` into a plain
//! list, `?force=1` into a non-forced apply, and `?stdin=t` into no stdin.
//! Use [`k8s_query_bool`] for EVERY boolean query parameter so there is one
//! implementation to be right, rather than a hand-rolled comparison per
//! call site.

/// Decode a boolean query parameter value the way Kubernetes does.
///
/// Only `"0"` and a case-insensitive `"false"` are false; every other value —
/// including `"1"`, `"t"`, `"yes"` and the empty string — is true. An ABSENT
/// parameter is the caller's concern (`Option::unwrap_or(false)`), matching
/// upstream's zero-length-slice case.
pub fn k8s_query_bool(v: &str) -> bool {
    !(v == "0" || v.eq_ignore_ascii_case("false"))
}

/// [`k8s_query_bool`] lifted over an optional raw value, for the common
/// `params.get("key").map(...)` shape. `None` stays `None` so callers can
/// distinguish "absent" from "explicitly false" (`*bool` parameters such as
/// `orphanDependents` and `force` need that distinction).
pub fn k8s_query_bool_opt(v: Option<&str>) -> Option<bool> {
    v.map(k8s_query_bool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_zero_and_false_are_false() {
        for f in ["0", "false", "False", "FALSE", "fAlSe"] {
            assert!(!k8s_query_bool(f), "{f:?} must be false");
        }
    }

    #[test]
    fn everything_else_is_true_including_empty() {
        // Upstream: "Any other value (including empty string) resolves to true."
        for t in [
            "1", "true", "True", "TRUE", "t", "T", "yes", "on", "banana", "",
        ] {
            assert!(k8s_query_bool(t), "{t:?} must be true");
        }
    }

    #[test]
    fn diverges_from_parse_bool_on_single_letters() {
        // Go's strconv.ParseBool reads "f"/"F" as false; the query conversion
        // does not, because it only special-cases "0" and "false".
        assert!(k8s_query_bool("f"));
        assert!(k8s_query_bool("F"));
    }

    #[test]
    fn absence_is_preserved_for_pointer_bool_params() {
        assert_eq!(k8s_query_bool_opt(None), None);
        assert_eq!(k8s_query_bool_opt(Some("1")), Some(true));
        assert_eq!(k8s_query_bool_opt(Some("0")), Some(false));
    }
}
