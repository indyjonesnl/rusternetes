//! The `--requestheader-*` front-proxy authentication flags and the runtime
//! trust they configure.
//!
//! Ported from upstream
//! `staging/src/k8s.io/apiserver/pkg/server/options/authentication.go`:
//! `RequestHeaderAuthenticationOptions` (:55-63), `Validate` (:67-108),
//! `AddFlags` (:129-155) and `ToAuthenticationRequestHeaderConfig` (:159-177),
//! wired the way `pkg/kubeapiserver/options/authentication.go` does
//! (`WithRequestHeader`, :215-218 — no defaults — and the overlap check in
//! `Validate`, :304-324).
//!
//! Where the authenticator sits in the chain is
//! `pkg/kubeapiserver/authenticator/config.go:113-131`: request-header first,
//! then x509, then bearer tokens (see `rusternetes_middleware::auth_middleware`).

use std::sync::Arc;

use rusternetes_middleware::request_header::RequestHeaderConfig;

/// `RequestHeaderAuthenticationOptions` as command-line flags. Shared by the
/// `api-server` and all-in-one binaries (#3414).
#[derive(clap::Args, Debug, Clone, Default)]
pub struct RequestHeaderAuthenticationArgs {
    /// List of request headers to inspect for usernames. X-Remote-User is
    /// common (`AddFlags`, authentication.go:134-135).
    #[arg(
        long = "requestheader-username-headers",
        id = "requestheader_username_headers",
        value_delimiter = ','
    )]
    pub username_headers: Vec<String>,

    /// List of request headers to inspect for UIDs. X-Remote-Uid is suggested
    /// (`AddFlags`, authentication.go:137-138).
    #[arg(
        long = "requestheader-uid-headers",
        id = "requestheader_uid_headers",
        value_delimiter = ','
    )]
    pub uid_headers: Vec<String>,

    /// List of request headers to inspect for groups. X-Remote-Group is
    /// suggested (`AddFlags`, authentication.go:140-141).
    #[arg(
        long = "requestheader-group-headers",
        id = "requestheader_group_headers",
        value_delimiter = ','
    )]
    pub group_headers: Vec<String>,

    /// List of request header prefixes to inspect. X-Remote-Extra- is
    /// suggested (`AddFlags`, authentication.go:143-144).
    #[arg(
        long = "requestheader-extra-headers-prefix",
        id = "requestheader_extra_headers_prefix",
        value_delimiter = ','
    )]
    pub extra_headers_prefix: Vec<String>,

    /// Root certificate bundle to use to verify client certificates on
    /// incoming requests before trusting usernames in headers specified by
    /// --requestheader-username-headers (`AddFlags`, authentication.go:146-150).
    #[arg(
        long = "requestheader-client-ca-file",
        id = "requestheader_client_ca_file",
        default_value = ""
    )]
    pub client_ca_file: String,

    /// List of client certificate common names to allow to provide usernames
    /// in headers. If empty, any client certificate validated by the
    /// authorities in --requestheader-client-ca-file is allowed
    /// (`AddFlags`, authentication.go:152-155).
    #[arg(
        long = "requestheader-allowed-names",
        id = "requestheader_allowed_names",
        value_delimiter = ','
    )]
    pub allowed_names: Vec<String>,
}

fn check_for_white_space_only(flag: &str, names: &[String]) -> Option<String> {
    names
        .iter()
        .any(|n| n.trim().is_empty())
        .then(|| format!("empty value in {flag:?}"))
}

fn case_insensitive_has(headers: &[String], header: &str) -> bool {
    headers.iter().any(|h| h.eq_ignore_ascii_case(header))
}

impl RequestHeaderAuthenticationArgs {
    /// `RequestHeaderAuthenticationOptions.Validate` (authentication.go:67-108).
    /// `RemoteRequestHeaderUID` is beta and on by default
    /// (kube_features.go:2059-2062), so its enabled branch applies.
    pub fn validate(&self) -> Vec<String> {
        let mut errs: Vec<String> = [
            check_for_white_space_only("requestheader-username-headers", &self.username_headers),
            check_for_white_space_only("requestheader-group-headers", &self.group_headers),
            check_for_white_space_only(
                "requestheader-extra-headers-prefix",
                &self.extra_headers_prefix,
            ),
            check_for_white_space_only("requestheader-allowed-names", &self.allowed_names),
        ]
        .into_iter()
        .flatten()
        .collect();

        if !self.username_headers.is_empty()
            && !case_insensitive_has(&self.username_headers, "X-Remote-User")
        {
            tracing::warn!("--requestheader-username-headers is set without specifying the standard X-Remote-User header - API aggregation will not work");
        }
        if !self.group_headers.is_empty()
            && !case_insensitive_has(&self.group_headers, "X-Remote-Group")
        {
            tracing::warn!("--requestheader-group-headers is set without specifying the standard X-Remote-Group header - API aggregation will not work");
        }
        if !self.extra_headers_prefix.is_empty()
            && !case_insensitive_has(&self.extra_headers_prefix, "X-Remote-Extra-")
        {
            tracing::warn!("--requestheader-extra-headers-prefix is set without specifying the standard X-Remote-Extra- header prefix - API aggregation will not work");
        }

        if let Some(e) = check_for_white_space_only("requestheader-uid-headers", &self.uid_headers)
        {
            errs.push(e);
        }
        if !self.uid_headers.is_empty() && !case_insensitive_has(&self.uid_headers, "X-Remote-Uid")
        {
            // this was added later and so we are able to error out
            errs.push("--requestheader-uid-headers is set without specifying the standard X-Remote-Uid header - API aggregation will not work".to_string());
        }
        errs
    }

    /// The overlap rule of `BuiltInAuthenticationOptions.Validate`
    /// (pkg/kubeapiserver/options/authentication.go:309-324): a front-proxy CA
    /// that shares a certificate with `--client-ca-file` requires
    /// `--requestheader-allowed-names`, so regular client certs cannot set
    /// authenticating-proxy headers for arbitrary users.
    pub fn validate_with_client_ca(&self, client_ca_file: Option<&str>) -> Vec<String> {
        let mut errs = self.validate();
        if let Some(client_ca) = client_ca_file.filter(|c| !c.is_empty()) {
            if !self.client_ca_file.is_empty() && self.allowed_names.is_empty() {
                if let Ok(true) =
                    rusternetes_common::tls::ca_files_overlap(client_ca, &self.client_ca_file)
                {
                    errs.push("--requestheader-client-ca-file and --client-ca-file contain overlapping certificates; --requestheader-allowed-names must be specified to ensure regular client certificates cannot set authenticating proxy headers for arbitrary users".to_string());
                }
            }
        }
        errs
    }

    /// `ToAuthenticationRequestHeaderConfig` (authentication.go:159-177):
    /// `None` unless `--requestheader-client-ca-file` is set.
    pub fn to_trust(&self) -> anyhow::Result<Option<RequestHeaderTrust>> {
        if self.client_ca_file.is_empty() {
            return Ok(None);
        }
        let config = RequestHeaderConfig::new(
            &self.username_headers,
            &self.uid_headers,
            &self.group_headers,
            &self.extra_headers_prefix,
            &self.allowed_names,
        )
        .map_err(|e| anyhow::anyhow!(e))?;
        Ok(Some(RequestHeaderTrust {
            config: Arc::new(config),
            client_ca_file: self.client_ca_file.clone(),
        }))
    }
}

/// The resolved front-proxy configuration (upstream
/// `authenticatorfactory.RequestHeaderConfig`).
#[derive(Clone, Debug)]
pub struct RequestHeaderTrust {
    pub config: Arc<RequestHeaderConfig>,
    /// `--requestheader-client-ca-file`.
    pub client_ca_file: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct Cli {
        #[command(flatten)]
        rh: RequestHeaderAuthenticationArgs,
    }

    fn parse(args: &[&str]) -> RequestHeaderAuthenticationArgs {
        Cli::parse_from(std::iter::once("t").chain(args.iter().copied())).rh
    }

    #[test]
    fn flags_parse_comma_separated_lists() {
        let a = parse(&[
            "--requestheader-username-headers=X-Remote-User,X-Alt-User",
            "--requestheader-uid-headers=X-Remote-Uid",
            "--requestheader-group-headers=X-Remote-Group",
            "--requestheader-extra-headers-prefix=X-Remote-Extra-",
            "--requestheader-client-ca-file=/pki/front-proxy-ca.crt",
            "--requestheader-allowed-names=front-proxy-client,other",
        ]);
        assert_eq!(a.username_headers, ["X-Remote-User", "X-Alt-User"]);
        assert_eq!(a.uid_headers, ["X-Remote-Uid"]);
        assert_eq!(a.group_headers, ["X-Remote-Group"]);
        assert_eq!(a.extra_headers_prefix, ["X-Remote-Extra-"]);
        assert_eq!(a.client_ca_file, "/pki/front-proxy-ca.crt");
        assert_eq!(a.allowed_names, ["front-proxy-client", "other"]);
        assert!(a.validate().is_empty());
    }

    #[test]
    fn unset_flags_yield_no_front_proxy() {
        // WithRequestHeader (pkg/kubeapiserver/options/authentication.go:215-218)
        // sets no defaults; ToAuthenticationRequestHeaderConfig returns nil
        // without a CA file (authentication.go:160-162).
        let a = parse(&[]);
        assert!(a.username_headers.is_empty());
        assert!(a.to_trust().unwrap().is_none());
        assert!(a.validate().is_empty());
    }

    #[test]
    fn validate_rejects_blank_entries_and_nonstandard_uid_header() {
        let a = parse(&["--requestheader-username-headers=X-Remote-User, "]);
        assert_eq!(
            a.validate(),
            ["empty value in \"requestheader-username-headers\""]
        );
        let a = parse(&["--requestheader-uid-headers=X-Other-Uid"]);
        assert_eq!(a.validate().len(), 1);
        assert!(a.validate()[0].contains("X-Remote-Uid"));
        // The username/group/extra non-standard cases are warnings, not errors.
        let a = parse(&["--requestheader-username-headers=X-Other-User"]);
        assert!(a.validate().is_empty());
    }

    #[test]
    fn blank_header_in_ca_enabled_config_is_rejected() {
        let a = parse(&[
            "--requestheader-client-ca-file=/x",
            "--requestheader-group-headers= ",
        ]);
        assert!(a.to_trust().is_err());
    }
}
