//! Port of upstream `validateWindowsSecurityContextOptions`
//! (`pkg/apis/core/validation/validation.go:8449-8526`, constants at
//! `:8413-8418`, regexes at `:8420-8446`). Called from upstream
//! `ValidateSecurityContext` (`:8407`) and `validatePodSecurityContext`
//! (`:5564`).

use std::sync::LazyLock;

use regex::Regex;

use crate::resources::pod::WindowsSecurityContextOptions;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;

const MAX_GMSA_CREDENTIAL_SPEC_LENGTH_IN_KIB: usize = 64;
const MAX_GMSA_CREDENTIAL_SPEC_LENGTH: usize = MAX_GMSA_CREDENTIAL_SPEC_LENGTH_IN_KIB * 1024;
const MAX_RUN_AS_USER_NAME_DOMAIN_LENGTH: usize = 256;
const MAX_RUN_AS_USER_NAME_USER_LENGTH: usize = 104;

// `ctrlRegex`
static CTRL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[[:cntrl:]]+").unwrap());
// `validNetBiosRegex`
static VALID_NETBIOS_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^[^\\/:\*\?"<>|\.][^\\/:\*\?"<>|]{0,14}$"#).unwrap());
// `validWindowsUserDomainDNSRegex` (dnsSubdomainFormat)
static VALID_DNS_DOMAIN_RE: LazyLock<Regex> = LazyLock::new(|| {
    let label = r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?";
    Regex::new(&format!(r"^{label}(?:\.{label})*$")).unwrap()
});
// `invalidUserNameCharsRegex`
static INVALID_USER_CHARS_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"["/\\:;|=,\+\*\?<>@\[\]]"#).unwrap());
// `invalidUserNameDotsSpacesRegex`
static INVALID_USER_DOTS_SPACES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[\. ]+$").unwrap());

/// Upstream `validateWindowsSecurityContextOptions`.
pub fn validate_windows_security_context_options(
    windows_options: Option<&WindowsSecurityContextOptions>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(opts) = windows_options else {
        return errs;
    };

    if let Some(name) = opts.gmsa_credential_spec_name.as_deref() {
        // gmsaCredentialSpecName must be the name of a custom resource
        for msg in is_dns1123_subdomain(name) {
            errs.push(Error::invalid(
                &fld_path.child("gmsaCredentialSpecName"),
                name.to_string(),
                msg,
            ));
        }
    }

    if let Some(spec) = opts.gmsa_credential_spec.as_deref() {
        let l = spec.len();
        if l == 0 {
            errs.push(Error::invalid(
                &fld_path.child("gmsaCredentialSpec"),
                spec.to_string(),
                "gmsaCredentialSpec cannot be an empty string",
            ));
        } else if l > MAX_GMSA_CREDENTIAL_SPEC_LENGTH {
            errs.push(Error::invalid(
                &fld_path.child("gmsaCredentialSpec"),
                spec.to_string(),
                format!(
                    "gmsaCredentialSpec size must be under {MAX_GMSA_CREDENTIAL_SPEC_LENGTH_IN_KIB} KiB"
                ),
            ));
        }
    }

    if let Some(user_name) = opts.run_as_user_name.as_deref() {
        let path = fld_path.child("runAsUserName");
        let invalid = |msg: String| Error::invalid(&path, user_name.to_string(), msg);
        let parts: Vec<&str> = user_name.split('\\').collect();
        if user_name.is_empty() {
            errs.push(invalid("runAsUserName cannot be an empty string".into()));
        } else if CTRL_RE.is_match(user_name) {
            errs.push(invalid(
                "runAsUserName cannot contain control characters".into(),
            ));
        } else if parts.len() > 2 {
            errs.push(invalid(
                "runAsUserName cannot contain more than one backslash".into(),
            ));
        } else {
            let (has_domain, domain, user) = if parts.len() == 1 {
                (false, "", parts[0])
            } else {
                (true, parts[0], parts[1])
            };

            if domain.len() >= MAX_RUN_AS_USER_NAME_DOMAIN_LENGTH {
                errs.push(invalid(format!(
                    "runAsUserName's Domain length must be under {MAX_RUN_AS_USER_NAME_DOMAIN_LENGTH} characters"
                )));
            }

            if has_domain
                && !(VALID_NETBIOS_RE.is_match(domain) || VALID_DNS_DOMAIN_RE.is_match(domain))
            {
                errs.push(invalid(
                    "runAsUserName's Domain doesn't match the NetBios nor the DNS format".into(),
                ));
            }

            if user.is_empty() {
                errs.push(invalid("runAsUserName's User cannot be empty".into()));
            } else if user.len() > MAX_RUN_AS_USER_NAME_USER_LENGTH {
                errs.push(invalid(format!(
                    "runAsUserName's User length must not be longer than {MAX_RUN_AS_USER_NAME_USER_LENGTH} characters"
                )));
            }

            if INVALID_USER_DOTS_SPACES_RE.is_match(user) {
                errs.push(invalid(
                    "runAsUserName's User cannot contain only periods or spaces".into(),
                ));
            }

            if INVALID_USER_CHARS_RE.is_match(user) {
                errs.push(invalid(
                    r#"runAsUserName's User cannot contain the following characters: "/\:;|=,+*?<>@[]"#
                        .into(),
                ));
            }
        }
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(
        name: Option<&str>,
        spec: Option<&str>,
        user: Option<&str>,
    ) -> WindowsSecurityContextOptions {
        WindowsSecurityContextOptions {
            gmsa_credential_spec_name: name.map(String::from),
            gmsa_credential_spec: spec.map(String::from),
            run_as_user_name: user.map(String::from),
            host_process: None,
        }
    }

    fn user(u: &str) -> Option<WindowsSecurityContextOptions> {
        Some(opts(None, None, Some(u)))
    }

    /// Port of upstream `TestValidateWindowsSecurityContextOptions`
    /// (`validation_test.go:23997`): exactly one error containing the
    /// substring, or none.
    #[test]
    fn upstream_cases() {
        let dns_too_long = {
            let s = format!("{}.", "a".repeat(63)).repeat(4);
            format!("{}.com\\user", &s[..253])
        };
        let cases: Vec<(&str, Option<WindowsSecurityContextOptions>, &str)> = vec![
            ("nil", None, ""),
            ("empty struct", Some(opts(None, None, None)), ""),
            (
                "valid input",
                Some(opts(
                    Some("dummy-gmsa-crep-spec-name"),
                    Some("dummy-gmsa-crep-spec-contents"),
                    None,
                )),
                "",
            ),
            (
                "bad gmsa name",
                Some(opts(Some("not_a-valid-gmsa-crep-spec-name"), None, None)),
                "a lowercase RFC 1123 subdomain must consist of",
            ),
            (
                "empty gmsa spec",
                Some(opts(None, Some(""), None)),
                "gmsaCredentialSpec cannot be an empty string",
            ),
            (
                "long gmsa spec",
                Some(opts(None, Some(&"a".repeat(64 * 1024 + 1)), None)),
                "gmsaCredentialSpec size must be under",
            ),
            ("valid user", user("Container. User"), ""),
            (
                "valid netbios",
                user("Network Service\\Container. User"),
                "",
            ),
            (
                "valid dns",
                user(&format!("{}.liSH\\Container. User", "fOo".repeat(20))),
                "",
            ),
            (
                "valid dns single char segment",
                user(&format!("{}.l\\Container. User", "fOo".repeat(20))),
                "",
            ),
            (
                "valid long single segment dns",
                user(&format!("{}\\Container. User", "a".repeat(42))),
                "",
            ),
            (
                "empty user",
                user(""),
                "runAsUserName cannot be an empty string",
            ),
            (
                "control char",
                user("Container\tUser"),
                "runAsUserName cannot contain control characters",
            ),
            (
                "too many backslashes",
                user("Container\\Foo\\Lish"),
                "runAsUserName cannot contain more than one backslash",
            ),
            (
                "empty domain",
                user("\\User"),
                "runAsUserName's Domain doesn't match the NetBios nor the DNS format",
            ),
            (
                "empty user after backslash",
                user("Container\\"),
                "runAsUserName's User cannot be empty",
            ),
            (
                "netbios too long",
                user(&format!("NetBios {}\\user", "a".repeat(8))),
                "runAsUserName's Domain doesn't match the NetBios",
            ),
            (
                "dns domain too long",
                user(&dns_too_long),
                "runAsUserName's Domain length must be under",
            ),
            (
                "user too long",
                user(&"a".repeat(105)),
                "runAsUserName's User length must not be longer than",
            ),
            (
                "only spaces or periods",
                user("... ..."),
                "runAsUserName's User cannot contain only periods or spaces",
            ),
            (
                "netbios starts with dot",
                user(".FooLish\\User"),
                "runAsUserName's Domain doesn't match the NetBios",
            ),
            (
                "netbios invalid chars",
                user("Foo? Lish?\\User"),
                "runAsUserName's Domain doesn't match the NetBios",
            ),
            (
                "dns invalid chars",
                user(&format!("{}.com-\\user", "a".repeat(32))),
                "runAsUserName's Domain doesn't match the NetBios nor the DNS format",
            ),
            (
                "user invalid chars",
                user("Container/User"),
                "runAsUserName's User cannot contain the following characters",
            ),
        ];
        for (name, o, want) in cases {
            let errs = validate_windows_security_context_options(o.as_ref(), &Path::new("field"));
            match errs.len() {
                0 => assert!(
                    want.is_empty(),
                    "{name}: expected failure containing {want:?}"
                ),
                1 => {
                    assert!(!want.is_empty(), "{name}: unexpected failure {:?}", errs[0]);
                    let s = errs[0].to_string();
                    assert!(s.contains(want), "{name}: want {want:?} got {s:?}");
                }
                n => panic!("{name}: got {n} failures: {errs:?}"),
            }
        }
    }

    /// The pod-level and container-level entry points must reach the
    /// validator (upstream `validation.go:5564` and `:8407`).
    #[test]
    fn wired_into_pod_and_container_security_context() {
        use crate::resources::pod::{Pod, SecurityContext};
        let bad = opts(None, Some(""), None);

        let mut pod: Pod = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p", "namespace": "d"},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        }))
        .unwrap();
        pod.spec.as_mut().unwrap().containers[0].security_context = Some(SecurityContext {
            windows_options: Some(bad.clone()),
            ..Default::default()
        });
        let errs = crate::validation::pod::validate_pod_create(&pod, false);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("containers[0].securityContext.windowsOptions.gmsaCredentialSpec")),
            "container: {errs:?}"
        );

        let mut pod2: Pod = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p", "namespace": "d"},
            "spec": {"containers": [{"name": "c", "image": "i"}], "securityContext": {}}
        }))
        .unwrap();
        pod2.spec
            .as_mut()
            .unwrap()
            .security_context
            .as_mut()
            .unwrap()
            .windows_options = Some(bad);
        let errs = crate::validation::pod::validate_pod_create(&pod2, false);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("spec.securityContext.windowsOptions.gmsaCredentialSpec")),
            "pod: {errs:?}"
        );
    }
}
