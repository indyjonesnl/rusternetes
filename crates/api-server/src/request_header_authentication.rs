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

impl RequestHeaderAuthenticationArgs {
    /// Startup path: validate (including the `--client-ca-file` overlap rule),
    /// then resolve. An invalid flag set refuses to start, like upstream's
    /// aggregated `Validate()` errors.
    pub fn resolve(
        &self,
        client_ca_file: Option<&str>,
    ) -> anyhow::Result<Option<RequestHeaderTrust>> {
        let errs = self.validate_with_client_ca(client_ca_file);
        if !errs.is_empty() {
            anyhow::bail!(
                "invalid front-proxy authentication options: {}",
                errs.join("; ")
            );
        }
        self.to_trust()
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

/// The CA bundles the TLS handshake must trust: `--client-ca-file` plus
/// `--requestheader-client-ca-file` (`ApplyClientCert`,
/// staging/src/k8s.io/apiserver/pkg/server/config.go:535-548). Empty means no
/// client-cert authentication at all.
pub fn client_ca_files<'a>(
    client_ca_file: Option<&'a str>,
    trust: Option<&'a RequestHeaderTrust>,
) -> Vec<&'a str> {
    client_ca_file
        .filter(|c| !c.is_empty())
        .into_iter()
        .chain(trust.map(|t| t.client_ca_file.as_str()))
        .collect()
}

/// The reload machinery behind a mTLS listener; the CA files are re-read
/// while this is alive (`DynamicFileCAContent.Run` plus
/// `DynamicServingCertificateController.Run`), and abandoned when it drops.
pub struct CaReload {
    _tasks: crate::dynamic_certificates::ReloadTasks,
}

/// The acceptor for a listener that authenticates client certificates:
///   * `--client-ca-file` and `--requestheader-client-ca-file` are each a
///     `NewDynamicCAContentFromFile` provider (authentication.go:165, :238);
///   * the TLS handshake trusts their union (`ApplyClientCert`, config.go:535-548),
///     rebuilt by the serving controller whenever either file changes;
///   * which authenticator owns a presented cert is decided against each
///     provider's CURRENT bundle.
///
/// Must be called inside a tokio runtime (the watch tasks are spawned here).
pub fn mtls_acceptor(
    tls_config: rusternetes_common::tls::TlsConfig,
    client_ca_file: Option<&str>,
    trust: Option<&RequestHeaderTrust>,
) -> anyhow::Result<(crate::peer_cert_acceptor::PeerCertAcceptor, CaReload)> {
    use crate::dynamic_certificates::{
        CaContentProvider, DynamicFileCaContent, DynamicServingCertificateController, ReloadTasks,
        UnionCaContent,
    };

    let client_ca: Option<Arc<DynamicFileCaContent>> = client_ca_file
        .filter(|c| !c.is_empty())
        .map(|f| DynamicFileCaContent::new("client-ca-bundle", f))
        .transpose()?;
    let request_header_ca: Option<Arc<DynamicFileCaContent>> = trust
        .map(|t| DynamicFileCaContent::new("request-header", &t.client_ca_file))
        .transpose()?;

    let providers: Vec<Arc<dyn CaContentProvider>> = client_ca
        .iter()
        .map(|c| c.clone() as Arc<dyn CaContentProvider>)
        .chain(
            request_header_ca
                .iter()
                .map(|c| c.clone() as Arc<dyn CaContentProvider>),
        )
        .collect();
    anyhow::ensure!(
        !providers.is_empty(),
        "mtls_acceptor needs at least one client CA file"
    );
    let union: Arc<dyn CaContentProvider> = UnionCaContent::new(providers);

    let controller = DynamicServingCertificateController::new(tls_config, union.clone())?;
    let mut tasks = ReloadTasks::default();
    tasks.extend(union.clone().run());
    tasks.extend(controller.clone().run());

    let acceptor = crate::peer_cert_acceptor::PeerCertAcceptor::new(controller.rustls_config());
    let acceptor = match (trust, request_header_ca) {
        (Some(t), Some(rh_ca)) => acceptor.with_request_header(
            t.config.clone(),
            rh_ca,
            client_ca.map(|c| c as Arc<dyn CaContentProvider>),
        ),
        _ => acceptor,
    };
    Ok((acceptor, CaReload { _tasks: tasks }))
}
