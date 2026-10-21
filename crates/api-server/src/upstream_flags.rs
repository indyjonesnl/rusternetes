//! kube-apiserver flags that kubeadm and Talos pass, accepted for argv
//! compatibility (#3908 / #1882).
//!
//! Policy for flags whose feature is not implemented:
//! - security-relevant and would change what the operator thinks is enforced
//!   (`--encryption-provider-config`): refuse to start, never silently store
//!   plaintext;
//! - everything else: accept and log a WARN naming the flag
//!   (`--anonymous-auth=false`, `--tls-min-version` and `--tls-cipher-suites`
//!   are called out as SECURITY because ignoring them is fail-open).
//!
//! Flag names/types: apiserver/pkg/server/options/{serving,etcd,feature,
//! api_enablement}.go, cmd/kube-apiserver/app/options/options.go,
//! pkg/kubeapiserver/options/authentication.go,
//! pkg/controlplane/apiserver/options/options.go.

use anyhow::{bail, Result};
use clap::Args as ClapArgs;
use std::net::IpAddr;

/// `SecureServingOptions.BindPort` default for kube-apiserver
/// (cmd/kube-apiserver/app/options/options.go `SecurePort`: 6443).
pub const DEFAULT_SECURE_PORT: u16 = 6443;

#[derive(ClapArgs, Debug, Default)]
pub struct UpstreamCompatArgs {
    /// The port on which to serve HTTPS (`--secure-port`, serving.go:166,
    /// default 6443; 0 is invalid because serving is required). Overrides the
    /// port of `--bind-address`.
    #[arg(long)]
    pub secure_port: Option<u16>,

    /// `--encryption-provider-config` (etcd.go:186). Encryption at rest is
    /// not implemented, so a value is a startup error.
    #[arg(long)]
    pub encryption_provider_config: Option<String>,

    /// `--anonymous-auth` (authentication.go:359, default true). Accepted;
    /// `false` is not enforced.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub anonymous_auth: Option<bool>,

    /// `--tls-min-version` (serving.go:193). Accepted, not enforced.
    #[arg(long)]
    pub tls_min_version: Option<String>,

    /// `--tls-cipher-suites` (serving.go:186). Accepted, not enforced.
    #[arg(long, value_delimiter = ',')]
    pub tls_cipher_suites: Vec<String>,

    /// `--enable-bootstrap-token-auth` (authentication.go:367). Accepted, ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub enable_bootstrap_token_auth: Option<bool>,

    /// `--allow-privileged` (options.go:107). Accepted, ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub allow_privileged: Option<bool>,

    /// `--kubelet-client-certificate` (options.go:143). Accepted, ignored.
    #[arg(long)]
    pub kubelet_client_certificate: Option<String>,

    /// `--kubelet-client-key` (options.go). Accepted, ignored.
    #[arg(long)]
    pub kubelet_client_key: Option<String>,

    /// `--kubelet-certificate-authority` (options.go). Accepted, ignored.
    #[arg(long)]
    pub kubelet_certificate_authority: Option<String>,

    /// `--kubelet-preferred-address-types` (options.go:129). Accepted, ignored.
    #[arg(long, value_delimiter = ',')]
    pub kubelet_preferred_address_types: Vec<String>,

    /// `--proxy-client-cert-file` (controlplane options.go:174). Accepted, ignored.
    #[arg(long)]
    pub proxy_client_cert_file: Option<String>,

    /// `--proxy-client-key-file` (controlplane options.go). Accepted, ignored.
    #[arg(long)]
    pub proxy_client_key_file: Option<String>,

    /// `--etcd-cafile` (etcd.go:183). Accepted, ignored (etcd TLS is not wired).
    #[arg(long)]
    pub etcd_cafile: Option<String>,

    /// `--etcd-certfile` (etcd.go). Accepted, ignored.
    #[arg(long)]
    pub etcd_certfile: Option<String>,

    /// `--etcd-keyfile` (etcd.go). Accepted, ignored.
    #[arg(long)]
    pub etcd_keyfile: Option<String>,

    /// `--profiling` (feature.go:54). No pprof handlers are served.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub profiling: Option<bool>,

    /// `--contention-profiling` (feature.go). Accepted, ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub contention_profiling: Option<bool>,

    /// `--runtime-config` (api_enablement.go:49). Accepted, ignored.
    #[arg(long)]
    pub runtime_config: Option<String>,

    /// `--cloud-provider` (options.go). Accepted, ignored.
    #[arg(long)]
    pub cloud_provider: Option<String>,
}

impl UpstreamCompatArgs {
    /// Startup errors for flags that must not be silently ignored.
    pub fn validate(&self) -> Result<()> {
        if self.secure_port == Some(0) {
            bail!("--secure-port 0 is invalid: HTTPS serving cannot be switched off");
        }
        if let Some(p) = &self.encryption_provider_config {
            bail!(
                "--encryption-provider-config={p} is not supported: encryption at rest is not \
                 implemented, and starting would store Secrets unencrypted"
            );
        }
        Ok(())
    }

    /// Flags that were given, accepted, and have no effect (message per flag).
    pub fn ignored_flags(&self) -> Vec<String> {
        let given = [
            (
                self.enable_bootstrap_token_auth.is_some(),
                "--enable-bootstrap-token-auth",
            ),
            (self.allow_privileged.is_some(), "--allow-privileged"),
            (
                self.kubelet_client_certificate.is_some(),
                "--kubelet-client-certificate",
            ),
            (self.kubelet_client_key.is_some(), "--kubelet-client-key"),
            (
                self.kubelet_certificate_authority.is_some(),
                "--kubelet-certificate-authority",
            ),
            (
                !self.kubelet_preferred_address_types.is_empty(),
                "--kubelet-preferred-address-types",
            ),
            (
                self.proxy_client_cert_file.is_some(),
                "--proxy-client-cert-file",
            ),
            (
                self.proxy_client_key_file.is_some(),
                "--proxy-client-key-file",
            ),
            (self.etcd_cafile.is_some(), "--etcd-cafile"),
            (self.etcd_certfile.is_some(), "--etcd-certfile"),
            (self.etcd_keyfile.is_some(), "--etcd-keyfile"),
            (self.profiling == Some(true), "--profiling"),
            (
                self.contention_profiling == Some(true),
                "--contention-profiling",
            ),
            (self.runtime_config.is_some(), "--runtime-config"),
            (self.cloud_provider.is_some(), "--cloud-provider"),
        ];
        let mut out: Vec<String> = given
            .into_iter()
            .filter(|(g, _)| *g)
            .map(|(_, n)| {
                format!(
                    "{n} is accepted for upstream argv compatibility and ignored (not implemented)"
                )
            })
            .collect();
        // Ignoring these leaves the server less strict than asked.
        if self.anonymous_auth == Some(false) {
            out.push(
                "SECURITY: --anonymous-auth=false is accepted but NOT enforced; \
                 unauthenticated requests are handled as before"
                    .into(),
            );
        }
        if let Some(v) = &self.tls_min_version {
            out.push(format!(
                "SECURITY: --tls-min-version={v} is accepted but NOT enforced"
            ));
        }
        if !self.tls_cipher_suites.is_empty() {
            out.push("SECURITY: --tls-cipher-suites is accepted but NOT enforced".into());
        }
        out
    }
}

/// Resolve `--bind-address` and `--secure-port` into the `host:port` socket
/// address the server binds. Upstream `--bind-address` is an IP
/// (serving.go:155) and the port comes from `--secure-port`; the native flag
/// is a `host:port` socket address, which is still accepted. A `--secure-port`
/// always overrides the port; with neither, the port is 6443.
pub fn resolve_bind_address(bind: &str, secure_port: Option<u16>) -> Result<String> {
    let (ip, port): (IpAddr, Option<u16>) = if let Ok(sa) = bind.parse::<std::net::SocketAddr>() {
        (sa.ip(), Some(sa.port()))
    } else if let Ok(ip) = bind.parse::<IpAddr>() {
        (ip, None)
    } else {
        bail!("invalid --bind-address {bind:?}: expected an IP address (or host:port)");
    };
    let port = secure_port.or(port).unwrap_or(DEFAULT_SECURE_PORT);
    Ok(std::net::SocketAddr::new(ip, port).to_string())
}

/// Upstream serves TLS whenever certificates are configured; the native
/// `--tls` switch is opt-in. A serving cert/key pair given without `--tls`
/// (kubeadm and Talos never pass `--tls`) must not leave the port plaintext.
pub fn implies_tls(cert: &Option<String>, key: &Option<String>) -> bool {
    cert.is_some() && key.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Args;
    use clap::Parser;

    fn argv(text: &str) -> Vec<String> {
        text.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(String::from)
            .collect()
    }

    #[test]
    fn kubeadm_kube_apiserver_argv_parses() {
        let a = Args::try_parse_from(argv(include_str!(
            "../tests/fixtures/kubeadm-kube-apiserver.args"
        )))
        .expect("kubeadm argv must parse");
        a.upstream.validate().unwrap();
        assert_eq!(a.upstream.secure_port, Some(6443));
        assert_eq!(a.etcd_servers, "https://127.0.0.1:2379");
        assert_eq!(a.service_cluster_ip_range, "10.96.0.0/12");
        assert_eq!(
            a.tls_key_file.as_deref(),
            Some("/etc/kubernetes/pki/apiserver.key")
        );
        assert!(implies_tls(&a.tls_cert_file, &a.tls_key_file));
        assert_eq!(
            resolve_bind_address(&a.bind_address, a.upstream.secure_port).unwrap(),
            "0.0.0.0:6443"
        );
        // Accepted-but-inert flags are reported, never silent.
        assert_eq!(a.upstream.ignored_flags().len(), 10);
    }

    #[test]
    fn talos_kube_apiserver_argv_parses() {
        let a = Args::try_parse_from(argv(include_str!(
            "../tests/fixtures/talos-kube-apiserver.args"
        )))
        .expect("talos argv must parse");
        a.upstream.validate().unwrap();
        // `--bind-address=0.0.0.0` is a bare IP upstream.
        assert_eq!(
            resolve_bind_address(&a.bind_address, a.upstream.secure_port).unwrap(),
            "0.0.0.0:6443"
        );
        let ignored = a.upstream.ignored_flags().join("\n");
        assert!(ignored.contains("SECURITY: --anonymous-auth=false"));
        assert!(ignored.contains("SECURITY: --tls-min-version=VersionTLS13"));
        // --profiling=false is a no-op, not a warning.
        assert!(!ignored.contains("--profiling"));
    }

    #[test]
    fn encryption_provider_config_is_a_startup_error() {
        let a = Args::try_parse_from([
            "kube-apiserver",
            "--encryption-provider-config=/etc/kubernetes/enc.yaml",
        ])
        .unwrap();
        let e = a.upstream.validate().unwrap_err().to_string();
        assert!(e.contains("encryption at rest is not implemented"), "{e}");
    }

    #[test]
    fn secure_port_zero_rejected() {
        let a = Args::try_parse_from(["kube-apiserver", "--secure-port=0"]).unwrap();
        assert!(a.upstream.validate().is_err());
    }

    #[test]
    fn bind_address_forms() {
        let r = resolve_bind_address;
        assert_eq!(r("0.0.0.0:6443", None).unwrap(), "0.0.0.0:6443");
        assert_eq!(r("127.0.0.1", None).unwrap(), "127.0.0.1:6443");
        assert_eq!(r("127.0.0.1", Some(8443)).unwrap(), "127.0.0.1:8443");
        assert_eq!(r("0.0.0.0:6443", Some(443)).unwrap(), "0.0.0.0:443");
        assert_eq!(r("::", Some(6443)).unwrap(), "[::]:6443");
        assert!(r("not-an-ip", None).is_err());
    }

    #[test]
    fn tls_private_key_file_is_the_native_key_flag() {
        let a = Args::try_parse_from([
            "kube-apiserver",
            "--tls-cert-file=/c",
            "--tls-private-key-file=/k",
        ])
        .unwrap();
        assert_eq!(a.tls_key_file.as_deref(), Some("/k"));
    }
}
