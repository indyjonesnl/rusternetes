//! Unit tests moved from `src/request_header_authentication.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_middleware::request_header::RequestHeaderConfig;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use clap::Parser;
    use rusternetes_api_server::request_header_authentication::*;

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
