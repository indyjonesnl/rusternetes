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
