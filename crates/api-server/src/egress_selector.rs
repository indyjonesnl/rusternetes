//! `--egress-selector-config-file`: konnectivity egress selection (#3416).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/egressselector/`
//! (`config.go` `ReadEgressSelectorConfiguration` :48 / `ValidateEgressSelectorConfiguration`
//! :73, `egress_selector.go` `NewEgressSelector` :373 / `Lookup` :407,
//! `metrics/metrics.go`), the internal types in
//! `staging/src/k8s.io/apiserver/pkg/apis/apiserver/types.go` :55-149, the
//! `master` -> `controlplane` conversion in `apis/apiserver/v1beta1/conversion.go`
//! :24 (and v1alpha1) and the flag in `pkg/server/options/egress_selector.go`.

use rusternetes_common::validation::field::{BadValue, Error as FieldError, ErrorList, Path};
use std::collections::BTreeSet;
use std::sync::Arc;

pub const PROTOCOL_HTTP_CONNECT: &str = "HTTPConnect";
pub const PROTOCOL_GRPC: &str = "GRPC";
pub const PROTOCOL_DIRECT: &str = "Direct";

/// `apiserver.EgressSelectorConfiguration` (internal type). `egress_selections`
/// is `None` for a nil Go slice (key absent), which `NewEgressSelector` treats
/// as "no configuration".
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EgressSelectorConfiguration {
    pub egress_selections: Option<Vec<EgressSelection>>,
}

/// `apiserver.EgressSelection`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EgressSelection {
    pub name: String,
    pub connection: Connection,
}

/// `apiserver.Connection`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Connection {
    pub proxy_protocol: String,
    pub transport: Option<Transport>,
}

/// `apiserver.Transport`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Transport {
    pub tcp: Option<TcpTransport>,
    pub uds: Option<UdsTransport>,
}

/// `apiserver.TCPTransport`. The internal type has no json tags, so a bad
/// value of this type marshals with the Go field names.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize)]
pub struct TcpTransport {
    #[serde(rename = "URL")]
    pub url: String,
    #[serde(rename = "TLSConfig")]
    pub tls_config: Option<TlsConfig>,
}

/// `apiserver.UDSTransport`.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize)]
pub struct UdsTransport {
    #[serde(rename = "UDSName")]
    pub uds_name: String,
}

/// `apiserver.TLSConfig`.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize)]
pub struct TlsConfig {
    #[serde(rename = "CABundle")]
    pub ca_bundle: String,
    #[serde(rename = "ClientKey")]
    pub client_key: String,
    #[serde(rename = "ClientCert")]
    pub client_cert: String,
}

// ---------------------------------------------------------------------------
// config.go: loading
// ---------------------------------------------------------------------------

/// `validEgressSelectorNames` (config.go:36), sorted as `sets.List()` does.
const VALID_EGRESS_SELECTOR_NAMES: [&str; 3] = ["cluster", "controlplane", "etcd"];

/// `cfgScheme = runtime.NewScheme()` (config.go:33), as `GetNameFromCallsite`
/// prints it in kube-apiserver builds.
const SCHEME_NAME: &str = "k8s.io/apiserver/pkg/server/egressselector/config.go:33";
const KIND: &str = "EgressSelectorConfiguration";
/// The kind is registered only in the legacy `apiserver.k8s.io` group, at
/// v1alpha1 (v1alpha1/register.go) and v1beta1 (v1beta1/register.go
/// `addKnownTypes`); `apiserver.config.k8s.io/v1` does not carry it.
const API_VERSIONS: [&str; 2] = ["apiserver.k8s.io/v1alpha1", "apiserver.k8s.io/v1beta1"];

/// The versioned wire shape (v1alpha1/v1beta1 `types.go`; the two are
/// field-for-field identical).
#[derive(Debug, Default, serde::Deserialize)]
struct ExtConfiguration {
    #[serde(default, rename = "egressSelections")]
    egress_selections: Option<Vec<ExtEgressSelection>>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtEgressSelection {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    connection: Option<ExtConnection>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtConnection {
    #[serde(default, rename = "proxyProtocol")]
    proxy_protocol: Option<String>,
    #[serde(default)]
    transport: Option<ExtTransport>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtTransport {
    #[serde(default)]
    tcp: Option<ExtTcp>,
    #[serde(default)]
    uds: Option<ExtUds>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtTcp {
    #[serde(default)]
    url: Option<String>,
    #[serde(default, rename = "tlsConfig")]
    tls_config: Option<ExtTls>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtUds {
    #[serde(default, rename = "udsName")]
    uds_name: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExtTls {
    #[serde(default, rename = "caBundle")]
    ca_bundle: Option<String>,
    #[serde(default, rename = "clientKey")]
    client_key: Option<String>,
    #[serde(default, rename = "clientCert")]
    client_cert: Option<String>,
}

const TOP_KEYS: [&str; 3] = ["apiVersion", "kind", "egressSelections"];
const SELECTION_KEYS: [&str; 2] = ["name", "connection"];
const CONNECTION_KEYS: [&str; 2] = ["proxyProtocol", "transport"];
const TRANSPORT_KEYS: [&str; 2] = ["tcp", "uds"];
const TCP_KEYS: [&str; 2] = ["url", "tlsConfig"];
const UDS_KEYS: [&str; 1] = ["udsName"];
const TLS_KEYS: [&str; 3] = ["caBundle", "clientKey", "clientCert"];

/// `unknown field "<path>"` for every key not in the json tags above; the
/// strict decoder (`serializer.EnableStrict`, config.go:60) is case-sensitive.
fn strict_errors(doc: &serde_yaml::Value) -> Vec<String> {
    use crate::authorization_config::{each_item, unknown_fields};
    let mut out = Vec::new();
    unknown_fields(doc, "", &TOP_KEYS, &mut out, |key, val, full, out| {
        if key != "egressSelections" {
            return;
        }
        each_item(val, full, out, |s, p, out| {
            unknown_fields(s, p, &SELECTION_KEYS, out, |key, val, full, out| {
                if key != "connection" {
                    return;
                }
                unknown_fields(val, full, &CONNECTION_KEYS, out, |key, val, full, out| {
                    if key != "transport" {
                        return;
                    }
                    unknown_fields(
                        val,
                        full,
                        &TRANSPORT_KEYS,
                        out,
                        |key, val, full, out| match key {
                            "tcp" => {
                                unknown_fields(val, full, &TCP_KEYS, out, |key, val, full, out| {
                                    if key == "tlsConfig" {
                                        unknown_fields(val, full, &TLS_KEYS, out, |_, _, _, _| {});
                                    }
                                })
                            }
                            "uds" => unknown_fields(val, full, &UDS_KEYS, out, |_, _, _, _| {}),
                            _ => {}
                        },
                    );
                });
            });
        });
    });
    out
}

/// The generated external -> internal conversions plus the hand-written
/// `Convert_v1{alpha1,beta1}_EgressSelection_To_apiserver_EgressSelection`
/// (v1beta1/conversion.go:24-33), which renames the deprecated `master` to
/// `controlplane`.
fn convert(ext: ExtConfiguration) -> EgressSelectorConfiguration {
    EgressSelectorConfiguration {
        egress_selections: ext.egress_selections.map(|v| {
            v.into_iter()
                .map(|s| {
                    let c = s.connection.unwrap_or_default();
                    let mut name = s.name.unwrap_or_default();
                    if name == "master" {
                        name = "controlplane".to_string();
                    }
                    EgressSelection {
                        name,
                        connection: Connection {
                            proxy_protocol: c.proxy_protocol.unwrap_or_default(),
                            transport: c.transport.map(|t| Transport {
                                tcp: t.tcp.map(|t| TcpTransport {
                                    url: t.url.unwrap_or_default(),
                                    tls_config: t.tls_config.map(|c| TlsConfig {
                                        ca_bundle: c.ca_bundle.unwrap_or_default(),
                                        client_key: c.client_key.unwrap_or_default(),
                                        client_cert: c.client_cert.unwrap_or_default(),
                                    }),
                                }),
                                uds: t.uds.map(|u| UdsTransport {
                                    uds_name: u.uds_name.unwrap_or_default(),
                                }),
                            }),
                        },
                    }
                })
                .collect()
        }),
    }
}

/// `serializer.NewCodecFactory(cfgScheme, serializer.EnableStrict).UniversalDecoder().Decode`
/// (config.go:59-72). The yaml serializer converts YAML to JSON first, so an
/// empty document reads as `null`; kind and apiVersion are required (json.go
/// `Interpret`, kind first), the (group, version, kind) must be registered, and
/// unknown or duplicate fields are a `strict decoding error`.
fn decode(data: &[u8]) -> Result<EgressSelectorConfiguration, String> {
    let text = String::from_utf8_lossy(data);
    let doc: serde_yaml::Value = match serde_yaml::from_str(&text) {
        Ok(d) => d,
        Err(e) => {
            // `yaml.YAMLToJSONStrict` (json.go:280) reports a repeated key as
            // `key "<k>" already set in map` inside a strictDecodingError.
            let msg = e.to_string();
            if msg.contains("duplicate entry") {
                let key = msg.split('"').nth(1).unwrap_or_default().to_string();
                // yaml.v2 names the line of the repeated key; serde_yaml names
                // the enclosing mapping, so look for the second occurrence.
                let start = e.location().map(|l| l.line()).unwrap_or(1);
                let needle = format!("{key}:");
                let line = text
                    .lines()
                    .enumerate()
                    .skip(start.saturating_sub(1))
                    .filter(|(_, l)| {
                        let t = l.trim_start().trim_start_matches("- ");
                        t.starts_with(&needle)
                    })
                    .nth(1)
                    .map(|(i, _)| i + 1)
                    .unwrap_or(start);
                return Err(format!(
                    "strict decoding error: error converting YAML to JSON: yaml: unmarshal errors:\n  line {line}: key \"{key}\" already set in map"
                ));
            }
            return Err(format!("couldn't get version/kind; json parse error: {e}"));
        }
    };
    let (kind, api_version) = if doc.is_null() {
        ("".to_string(), "".to_string())
    } else {
        let field = |name: &str| {
            doc.get(name)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        (field("kind"), field("apiVersion"))
    };
    let shown = if doc.is_null() {
        "null".to_string()
    } else {
        text.to_string()
    };
    if kind.is_empty() {
        return Err(format!("Object 'Kind' is missing in '{shown}'"));
    }
    if api_version.is_empty() {
        return Err(format!("Object 'apiVersion' is missing in '{shown}'"));
    }
    if kind != KIND || !API_VERSIONS.contains(&api_version.as_str()) {
        return Err(format!(
            "no kind {kind:?} is registered for version {api_version:?} in scheme {SCHEME_NAME:?}"
        ));
    }
    let unknown = strict_errors(&doc);
    if !unknown.is_empty() {
        return Err(format!("strict decoding error: {}", unknown.join(", ")));
    }
    let ext: ExtConfiguration = serde_yaml::from_value(doc)
        .map_err(|e| format!("couldn't get version/kind; json parse error: {e}"))?;
    Ok(convert(ext))
}

/// `ReadEgressSelectorConfiguration` (config.go:48). An empty path is the
/// nil (no-op) configuration; a file that cannot be read or decoded is an
/// error.
pub fn read_egress_selector_configuration(
    config_file_path: &str,
) -> Result<Option<EgressSelectorConfiguration>, String> {
    if config_file_path.is_empty() {
        return Ok(None);
    }
    let data = std::fs::read(config_file_path).map_err(|e| {
        format!(
            "unable to read egress selector configuration from {config_file_path:?} [open {config_file_path}: {}]",
            crate::authorization_config::go_io_error(&e)
        )
    })?;
    decode(&data).map(Some)
}

// ---------------------------------------------------------------------------
// config.go: validation
// ---------------------------------------------------------------------------

/// `path.Exists(path.CheckFollowSymlink, p)`: a stat that follows symlinks;
/// any failure means "does not exist" for the callers below.
fn path_exists(p: &str) -> bool {
    std::fs::metadata(p).is_ok()
}

/// `ValidateEgressSelectorConfiguration` (config.go:73). A nil configuration
/// is valid.
pub fn validate_egress_selector_configuration(
    config: Option<&EgressSelectorConfiguration>,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(config) = config else {
        return errs;
    };
    let selections = config.egress_selections.as_deref().unwrap_or_default();
    for service in selections {
        let fld_path = Path::new("service").child("connection");
        let conn = &service.connection;
        match conn.proxy_protocol.as_str() {
            PROTOCOL_DIRECT => errs.extend(validate_direct_connection(conn, &fld_path)),
            PROTOCOL_HTTP_CONNECT => errs.extend(validate_http_connect_transport(
                conn.transport.as_ref(),
                &fld_path,
            )),
            PROTOCOL_GRPC => {
                errs.extend(validate_grpc_transport(conn.transport.as_ref(), &fld_path))
            }
            other => errs.push(FieldError::not_supported(
                &fld_path.child("protocol"),
                other,
                &[PROTOCOL_DIRECT, PROTOCOL_HTTP_CONNECT, PROTOCOL_GRPC],
            )),
        }
    }

    let mut seen = BTreeSet::new();
    for (i, service) in selections.iter().enumerate() {
        let canonical_name = service.name.to_lowercase();
        let fld_path = Path::new("service").child("connection");
        if seen.contains(&canonical_name) {
            errs.push(FieldError::duplicate(
                &fld_path.index(i),
                canonical_name.as_str(),
            ));
            continue;
        }
        seen.insert(canonical_name.clone());

        if !VALID_EGRESS_SELECTOR_NAMES.contains(&canonical_name.as_str()) {
            errs.push(FieldError::not_supported(
                &fld_path,
                canonical_name.as_str(),
                &VALID_EGRESS_SELECTOR_NAMES,
            ));
            continue;
        }
    }
    errs
}

fn validate_http_connect_transport(transport: Option<&Transport>, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(transport) = transport else {
        errs.push(FieldError::required(
            &fld_path.child("transport"),
            "transport must be set for HTTPConnect",
        ));
        return errs;
    };
    match (&transport.tcp, &transport.uds) {
        (Some(tcp), Some(_)) => errs.push(FieldError::invalid(
            &fld_path.child("tcp"),
            BadValue::marshal(tcp),
            "TCP and UDS cannot both be set",
        )),
        (None, None) => errs.push(FieldError::required(
            &fld_path.child("tcp"),
            "One of TCP or UDS must be set",
        )),
        (Some(tcp), None) => errs.extend(validate_tcp_connection(tcp, fld_path)),
        (None, Some(uds)) => errs.extend(validate_uds_connection(uds, fld_path)),
    }
    errs
}

fn validate_grpc_transport(transport: Option<&Transport>, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(transport) = transport else {
        errs.push(FieldError::required(
            &fld_path.child("transport"),
            "transport must be set for GRPC",
        ));
        return errs;
    };
    match &transport.uds {
        Some(uds) => errs.extend(validate_uds_connection(uds, fld_path)),
        None => errs.push(FieldError::required(
            &fld_path.child("uds"),
            "UDS must be set with GRPC",
        )),
    }
    errs
}

fn validate_direct_connection(connection: &Connection, fld_path: &Path) -> ErrorList {
    if connection.transport.is_some() {
        return vec![FieldError::invalid(
            &fld_path.child("transport"),
            "direct",
            "config must be absent for direct connect",
        )];
    }
    ErrorList::new()
}

fn validate_uds_connection(uds: &UdsTransport, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if uds.uds_name.is_empty() {
        errs.push(FieldError::invalid(
            &fld_path.child("udsName"),
            "nil",
            "must be present for UDS connections",
        ));
    }
    errs
}

fn validate_tcp_connection(tcp: &TcpTransport, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if tcp.url.starts_with("http://") {
        if tcp.tls_config.is_some() {
            errs.push(FieldError::invalid(
                &fld_path.child("tlsConfig"),
                "nil",
                "config must not be present when using HTTP",
            ));
        }
    } else if tcp.url.starts_with("https://") {
        return validate_tls_config(tcp.tls_config.as_ref(), fld_path);
    } else {
        errs.push(FieldError::invalid(
            &fld_path.child("url"),
            tcp.url.as_str(),
            "supported connection protocols are http:// and https://",
        ));
    }
    errs
}

fn validate_tls_config(tls: Option<&TlsConfig>, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(tls) = tls else {
        errs.push(FieldError::required(
            &fld_path.child("tlsConfig"),
            "TLSConfig must be present when using HTTPS",
        ));
        return errs;
    };
    let tls_path = fld_path.child("tlsConfig");
    if !tls.ca_bundle.is_empty() && !path_exists(&tls.ca_bundle) {
        errs.push(FieldError::invalid(
            &tls_path.child("caBundle"),
            tls.ca_bundle.as_str(),
            "TLS config ca bundle does not exist",
        ));
    }
    if tls.client_cert.is_empty() {
        errs.push(FieldError::invalid(
            &tls_path.child("clientCert"),
            "nil",
            "Using TLS requires clientCert",
        ));
    } else if !path_exists(&tls.client_cert) {
        errs.push(FieldError::invalid(
            &tls_path.child("clientCert"),
            tls.client_cert.as_str(),
            "TLS client cert does not exist",
        ));
    }
    if tls.client_key.is_empty() {
        errs.push(FieldError::invalid(
            &tls_path.child("clientKey"),
            "nil",
            "Using TLS requires requires clientKey",
        ));
    } else if !path_exists(&tls.client_key) {
        errs.push(FieldError::invalid(
            &tls_path.child("clientKey"),
            tls.client_key.as_str(),
            "TLS client key does not exist",
        ));
    }
    errs
}

// ---------------------------------------------------------------------------
// metrics/metrics.go
// ---------------------------------------------------------------------------

mod metrics {
    //! `egressselector/metrics/metrics.go` (`DialMetrics`).
    use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};
    use std::sync::{LazyLock, Once};

    const NAMESPACE: &str = "apiserver";
    const SUBSYSTEM: &str = "egress_dialer";

    pub const PROTOCOL_HTTP_CONNECT: &str = "http_connect";
    pub const PROTOCOL_GRPC: &str = "grpc";
    pub const TRANSPORT_TCP: &str = "tcp";
    pub const TRANSPORT_UDS: &str = "uds";
    pub const STAGE_CONNECT: &str = "connect";
    pub const STAGE_PROXY: &str = "proxy";

    /// "Use buckets ranging from 5 ms to 12.5 seconds." (metrics.go:48)
    const LATENCY_BUCKETS: [f64; 6] = [0.005, 0.025, 0.1, 0.5, 2.5, 12.5];

    static STARTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new(
                "dial_start_total",
                "[ALPHA] Dial starts, labeled by the protocol (http-connect or grpc) and transport (tcp or uds).",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["protocol", "transport"],
        )
        .expect("valid metric")
    });
    static LATENCIES: LazyLock<HistogramVec> = LazyLock::new(|| {
        HistogramVec::new(
            HistogramOpts::new(
                "dial_duration_seconds",
                "[ALPHA] Dial latency histogram in seconds, labeled by the protocol (http-connect or grpc), transport (tcp or uds)",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM)
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["protocol", "transport"],
        )
        .expect("valid metric")
    });
    static FAILURES: LazyLock<IntCounterVec> = LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new(
                "dial_failure_count",
                "[ALPHA] Dial failure count, labeled by the protocol (http-connect or grpc), transport (tcp or uds), and stage (connect or proxy). The stage indicates at which stage the dial failed",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["protocol", "transport", "stage"],
        )
        .expect("valid metric")
    });

    static REGISTER: Once = Once::new();

    /// `legacyregistry.MustRegister` (metrics.go:96-98), done once.
    pub fn register() {
        REGISTER.call_once(|| {
            let _ = prometheus::register(Box::new(STARTS.clone()));
            let _ = prometheus::register(Box::new(LATENCIES.clone()));
            let _ = prometheus::register(Box::new(FAILURES.clone()));
        });
    }

    pub fn observe_dial_start(protocol: &str, transport: &str) {
        register();
        STARTS.with_label_values(&[protocol, transport]).inc();
    }

    pub fn observe_dial_latency(elapsed: std::time::Duration, protocol: &str, transport: &str) {
        register();
        LATENCIES
            .with_label_values(&[protocol, transport])
            .observe(elapsed.as_secs_f64());
    }

    pub fn observe_dial_failure(protocol: &str, transport: &str, stage: &str) {
        register();
        FAILURES
            .with_label_values(&[protocol, transport, stage])
            .inc();
    }
}

// ---------------------------------------------------------------------------
// egress_selector.go
// ---------------------------------------------------------------------------

/// `egressselector.EgressType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EgressType {
    /// Traffic intended to go to the control plane.
    ControlPlane,
    /// Traffic intended to go to Kubernetes persistence store.
    Etcd,
    /// Traffic intended to go to the system being managed by Kubernetes.
    Cluster,
}

/// `EgressType.String` (egress_selector.go:82).
impl std::fmt::Display for EgressType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            EgressType::ControlPlane => "controlplane",
            EgressType::Etcd => "etcd",
            EgressType::Cluster => "cluster",
        })
    }
}

/// `lookupServiceName` (egress_selector.go:100).
fn lookup_service_name(name: &str) -> Result<EgressType, String> {
    match name.to_lowercase().as_str() {
        "controlplane" => Ok(EgressType::ControlPlane),
        "etcd" => Ok(EgressType::Etcd),
        "cluster" => Ok(EgressType::Cluster),
        _ => Err(format!("unrecognized service name {name}")),
    }
}

/// A connection to a destination, direct or through the proxy server.
pub trait AsyncConn: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncConn for T {}
pub type Conn = Box<dyn AsyncConn>;

/// `proxier` (egress_selector.go:146).
#[async_trait::async_trait]
pub trait Proxier: Send + Sync {
    /// Returns a connection to addr.
    async fn proxy(&self, addr: &str) -> Result<Conn, String>;
}

/// `proxyServerConnector` (egress_selector.go:172).
#[async_trait::async_trait]
pub trait ProxyServerConnector: Send + Sync {
    /// Establishes connection to the proxy server, and returns a proxier based
    /// on the connection.
    async fn connect(&self) -> Result<Box<dyn Proxier>, String>;
}

/// Go's `http.DefaultMaxHeaderBytes` (1 << 20), the limit `tunnelHTTPConnect`
/// applies to the proxy's response headers (egress_selector.go:120).
const DEFAULT_MAX_HEADER_BYTES: usize = 1 << 20;

/// `tunnelHTTPConnect` (egress_selector.go:112).
async fn tunnel_http_connect(
    mut proxy_conn: Conn,
    proxy_address: &str,
    addr: &str,
) -> Result<Conn, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // fmt.Fprintf's write error is ignored upstream; the read below reports it.
    let _ = proxy_conn
        .write_all(format!("CONNECT {addr} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").as_bytes())
        .await;

    let read_failed = |e: String| {
        format!(
            "reading HTTP response from CONNECT to {addr} via proxy {proxy_address} failed: {e}"
        )
    };
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
        if buf.len() >= DEFAULT_MAX_HEADER_BYTES {
            return Err(read_failed(
                "http: server response headers exceeded 1048576 bytes".into(),
            ));
        }
        let want = chunk.len().min(DEFAULT_MAX_HEADER_BYTES - buf.len());
        match proxy_conn.read(&mut chunk[..want]).await {
            Ok(0) => return Err(read_failed("unexpected EOF".into())),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => return Err(read_failed(e.to_string())),
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let status_line = head.lines().next().unwrap_or_default();
    // http.ReadResponse: "HTTP/x.y NNN[ text]"; Status is everything after
    // the protocol and its space ("503 Service Unavailable").
    let (status, code) = status_line
        .split_once(' ')
        .and_then(|(proto, rest)| {
            let code: u16 = rest.get(..3)?.parse().ok()?;
            proto
                .starts_with("HTTP/")
                .then(|| (rest.trim().to_string(), code))
        })
        .ok_or_else(|| read_failed(format!("malformed HTTP response {status_line:?}")))?;
    if code != 200 {
        return Err(format!(
            "proxy error from {proxy_address} while dialing {addr}, code {code}: {status}"
        ));
    }

    // "It's safe to discard the bufio.Reader here ... But we can
    // double-check." (egress_selector.go:134-142)
    let buffered = buf.len() - header_end;
    if buffered > 0 {
        return Err(format!(
            "unexpected {buffered} bytes of buffered data from CONNECT proxy {proxy_address:?}"
        ));
    }
    Ok(proxy_conn)
}

/// `httpConnectProxier` (egress_selector.go:153). The connection is consumed
/// by its single `proxy` call (the proxier is built per dial).
struct HttpConnectProxier {
    conn: tokio::sync::Mutex<Option<Conn>>,
    proxy_address: String,
}

#[async_trait::async_trait]
impl Proxier for HttpConnectProxier {
    async fn proxy(&self, addr: &str) -> Result<Conn, String> {
        let conn = self
            .conn
            .lock()
            .await
            .take()
            .ok_or_else(|| "proxy connection already used".to_string())?;
        tunnel_http_connect(conn, &self.proxy_address, addr).await
    }
}

/// `tcpHTTPConnectConnector` (egress_selector.go:182).
struct TcpHttpConnectConnector {
    proxy_address: String,
    tls: Arc<rustls::ClientConfig>,
}

#[async_trait::async_trait]
impl ProxyServerConnector for TcpHttpConnectConnector {
    async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
        // tls.Dialer.DialContext infers ServerName from the address host.
        let host = self
            .proxy_address
            .rsplit_once(':')
            .map(|(h, _)| h)
            .ok_or_else(|| {
                format!(
                    "dial tcp: address {}: missing port in address",
                    self.proxy_address
                )
            })?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let server_name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|e| format!("tls: invalid server name: {e}"))?;
        let tcp = tokio::net::TcpStream::connect(&self.proxy_address)
            .await
            .map_err(|e| e.to_string())?;
        let tls = tokio_rustls::TlsConnector::from(self.tls.clone())
            .connect(server_name, tcp)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Box::new(HttpConnectProxier {
            conn: tokio::sync::Mutex::new(Some(Box::new(tls))),
            proxy_address: self.proxy_address.clone(),
        }))
    }
}

/// `udsHTTPConnectConnector` (egress_selector.go:198).
struct UdsHttpConnectConnector {
    uds_name: String,
}

#[async_trait::async_trait]
impl ProxyServerConnector for UdsHttpConnectConnector {
    async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
        let conn = tokio::net::UnixStream::connect(&self.uds_name)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Box::new(HttpConnectProxier {
            conn: tokio::sync::Mutex::new(Some(Box::new(conn))),
            proxy_address: self.uds_name.clone(),
        }))
    }
}

/// `udsGRPCConnector` (egress_selector.go:211). Upstream hands the socket to
/// the konnectivity-client `CreateSingleUseGrpcTunnel`; that client library
/// (apiserver-network-proxy's `ProxyService` bidi stream) is not ported yet, so
/// a GRPC egress selection connects and then fails every dial. Deliberate gap,
/// tracked in #3463.
struct UdsGrpcConnector {
    uds_name: String,
}

#[async_trait::async_trait]
impl ProxyServerConnector for UdsGrpcConnector {
    async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
        Err(format!(
            "konnectivity gRPC tunnel to {} is not implemented (#3463)",
            self.uds_name
        ))
    }
}

/// `metricsOptions` (egress_selector.go:249).
#[derive(Debug, Clone, Default)]
pub struct MetricsOptions {
    pub transport: String,
    pub protocol: String,
}

/// `dialerCreator` (egress_selector.go:243).
pub struct DialerCreator {
    pub connector: Option<Arc<dyn ProxyServerConnector>>,
    pub direct: bool,
    pub options: MetricsOptions,
}

/// `utilnet.DialFunc` selected for an egress type: `directDialer`
/// (`http.DefaultTransport`'s `DialContext`) or a dial through the proxy
/// server.
#[derive(Clone)]
pub enum Dialer {
    Direct,
    Proxy(Arc<DialerCreator>),
}

/// `http.DefaultTransport`'s dialer timeout (net.Dialer{Timeout: 30s}).
const DIRECT_DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl Dialer {
    /// Dials `addr`. The proxy dialer ignores `network`, as upstream's
    /// closure (egress_selector.go:258) does: it always asks the proxy for
    /// `tcp`.
    pub async fn dial(&self, network: &str, addr: &str) -> Result<Conn, String> {
        match self {
            Dialer::Direct => match network {
                "unix" => tokio::net::UnixStream::connect(addr)
                    .await
                    .map(|c| Box::new(c) as Conn)
                    .map_err(|e| e.to_string()),
                _ => {
                    tokio::time::timeout(DIRECT_DIAL_TIMEOUT, tokio::net::TcpStream::connect(addr))
                        .await
                        .map_err(|_| format!("dial {network} {addr}: i/o timeout"))?
                        .map(|c| Box::new(c) as Conn)
                        .map_err(|e| e.to_string())
                }
            },
            Dialer::Proxy(d) => d.dial(addr).await,
        }
    }
}

impl DialerCreator {
    /// The body of the closure `createDialer` returns (egress_selector.go:258-275).
    /// Upstream also opens an OpenTelemetry span ("Proxy via ... protocol over
    /// ... transport", reported when longer than 500ms); tracing is tracked
    /// separately.
    async fn dial(&self, addr: &str) -> Result<Conn, String> {
        let (protocol, transport) = (&self.options.protocol, &self.options.transport);
        let start = std::time::Instant::now();
        metrics::observe_dial_start(protocol, transport);
        let connector = self
            .connector
            .as_ref()
            .ok_or_else(|| "no proxy server connector".to_string())?;
        let proxier = match connector.connect().await {
            Ok(p) => p,
            Err(e) => {
                metrics::observe_dial_failure(protocol, transport, metrics::STAGE_CONNECT);
                return Err(e);
            }
        };
        let conn = match proxier.proxy(addr).await {
            Ok(c) => c,
            Err(e) => {
                metrics::observe_dial_failure(protocol, transport, metrics::STAGE_PROXY);
                return Err(e);
            }
        };
        metrics::observe_dial_latency(start.elapsed(), protocol, transport);
        Ok(conn)
    }

    /// `createDialer` (egress_selector.go:254).
    pub fn create_dialer(self) -> Dialer {
        metrics::register();
        if self.direct {
            return Dialer::Direct;
        }
        Dialer::Proxy(Arc::new(self))
    }
}

/// `getTLSConfig` (egress_selector.go:278). A nil `*TLSConfig` is a nil
/// pointer dereference upstream (validation allows an `http://` URL without
/// `tlsConfig`, see config.go:162, but `connectionToDialerCreator` still calls
/// this unconditionally), which panics the apiserver at startup; replicated.
fn get_tls_config(t: Option<&TlsConfig>) -> Result<Arc<rustls::ClientConfig>, String> {
    let t = t.unwrap_or_else(|| {
        panic!("runtime error: invalid memory address or nil pointer dereference")
    });
    let key_pair_err = |e: String| {
        format!(
            "failed to read key pair {} & {}, got {e}",
            t.client_cert, t.client_key
        )
    };
    let cert_pem = std::fs::read(&t.client_cert).map_err(|e| key_pair_err(e.to_string()))?;
    let key_pem = std::fs::read(&t.client_key).map_err(|e| key_pair_err(e.to_string()))?;
    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| key_pair_err(e.to_string()))?;
    if certs.is_empty() {
        return Err(key_pair_err(
            "tls: failed to find any PEM data in certificate input".into(),
        ));
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|e| key_pair_err(e.to_string()))?
        .ok_or_else(|| key_pair_err("tls: failed to find any PEM data in key input".into()))?;

    let mut roots = rustls::RootCertStore::empty();
    if !t.ca_bundle.is_empty() {
        let bytes = std::fs::read(&t.ca_bundle)
            .map_err(|e| format!("failed to read cert file {}, got {e}", t.ca_bundle))?;
        let cas = rustls_pemfile::certs(&mut bytes.as_slice())
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        let (added, _) = roots.add_parsable_certificates(cas);
        if added == 0 {
            return Err("failed to append CA cert to the cert pool".to_string());
        }
    } else {
        // "Use host's root CA set instead of providing our own"
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| key_pair_err(e.to_string()))?;
    Ok(Arc::new(config))
}

/// `getProxyAddress` (egress_selector.go:306): `url.Parse(s).Host`.
fn get_proxy_address(url_string: &str) -> Result<String, String> {
    url::Url::parse(url_string)
        .map_err(|e| format!("invalid proxy server url {url_string:?}: {e}"))?;
    let rest = url_string.split_once("://").map(|(_, r)| r).unwrap_or("");
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    Ok(authority.rsplit('@').next().unwrap_or("").to_string())
}

/// `connectionToDialerCreator` (egress_selector.go:314).
fn connection_to_dialer_creator(c: &Connection) -> Result<DialerCreator, String> {
    match c.proxy_protocol.as_str() {
        PROTOCOL_HTTP_CONNECT => {
            // `c.Transport` is dereferenced unconditionally upstream; the
            // validation that runs first guarantees it is set.
            let transport = c.transport.as_ref().unwrap_or_else(|| {
                panic!("runtime error: invalid memory address or nil pointer dereference")
            });
            if let Some(uds) = &transport.uds {
                Ok(DialerCreator {
                    connector: Some(Arc::new(UdsHttpConnectConnector {
                        uds_name: uds.uds_name.clone(),
                    })),
                    direct: false,
                    options: MetricsOptions {
                        transport: metrics::TRANSPORT_UDS.into(),
                        protocol: metrics::PROTOCOL_HTTP_CONNECT.into(),
                    },
                })
            } else if let Some(tcp) = &transport.tcp {
                let tls = get_tls_config(tcp.tls_config.as_ref())?;
                let proxy_address = get_proxy_address(&tcp.url)?;
                Ok(DialerCreator {
                    connector: Some(Arc::new(TcpHttpConnectConnector { tls, proxy_address })),
                    direct: false,
                    options: MetricsOptions {
                        transport: metrics::TRANSPORT_TCP.into(),
                        protocol: metrics::PROTOCOL_HTTP_CONNECT.into(),
                    },
                })
            } else {
                Err("Either a TCP or UDS transport must be specified".to_string())
            }
        }
        PROTOCOL_GRPC => {
            let transport = c.transport.as_ref().unwrap_or_else(|| {
                panic!("runtime error: invalid memory address or nil pointer dereference")
            });
            if let Some(uds) = &transport.uds {
                return Ok(DialerCreator {
                    connector: Some(Arc::new(UdsGrpcConnector {
                        uds_name: uds.uds_name.clone(),
                    })),
                    direct: false,
                    options: MetricsOptions {
                        transport: metrics::TRANSPORT_UDS.into(),
                        protocol: metrics::PROTOCOL_GRPC.into(),
                    },
                });
            }
            Err("UDS transport must be specified for GRPC".to_string())
        }
        PROTOCOL_DIRECT => Ok(DialerCreator {
            connector: None,
            direct: true,
            options: MetricsOptions::default(),
        }),
        other => Err(format!(
            "unrecognized service connection protocol {other:?}"
        )),
    }
}

/// `egressselector.EgressSelector`: the map of network context type to
/// dialer.
pub struct EgressSelector {
    egress_to_dialer: std::collections::HashMap<EgressType, Dialer>,
}

impl EgressSelector {
    /// `NewEgressSelector` (egress_selector.go:373). A nil configuration (or
    /// one with a nil `EgressSelections`) yields no selector, so every caller
    /// dials directly.
    pub fn new(config: Option<&EgressSelectorConfiguration>) -> Result<Option<Self>, String> {
        let Some(selections) = config.and_then(|c| c.egress_selections.as_ref()) else {
            return Ok(None);
        };
        let mut cs = EgressSelector {
            egress_to_dialer: std::collections::HashMap::new(),
        };
        for service in selections {
            let name = lookup_service_name(&service.name)?;
            let creator = connection_to_dialer_creator(&service.connection).map_err(|e| {
                format!("failed to create dialer for egressSelection \"{name}\": {e}")
            })?;
            cs.egress_to_dialer.insert(name, creator.create_dialer());
        }
        Ok(Some(cs))
    }

    /// `NewEgressSelectorWithMap` (egress_selector.go:396).
    pub fn with_map(m: std::collections::HashMap<EgressType, Dialer>) -> Self {
        EgressSelector {
            egress_to_dialer: m,
        }
    }

    /// `Lookup` (egress_selector.go:407): the dialer configured for the
    /// network context, or `None` when that egress type has no selection
    /// (callers then use their default dialer).
    pub fn lookup(&self, network_context: EgressType) -> Option<Dialer> {
        self.egress_to_dialer.get(&network_context).cloned()
    }
}

// ---------------------------------------------------------------------------
// options/egress_selector.go
// ---------------------------------------------------------------------------

/// `EgressSelectorOptions.AddFlags` (options/egress_selector.go:42); shared by
/// the `api-server` and all-in-one binaries.
#[derive(clap::Args, Debug, Clone, Default)]
pub struct EgressSelectorArgs {
    /// File with apiserver egress selector configuration.
    #[arg(long = "egress-selector-config-file", default_value = "")]
    pub config_file: String,
}

/// `EgressSelectorOptions.Validate` (options/egress_selector.go:80).
pub fn validate_options(config_file: &str) -> Vec<String> {
    if config_file.is_empty() {
        return Vec::new();
    }
    if !path_exists(config_file) {
        return vec![format!(
            "egress-selector-config-file {config_file} does not exist"
        )];
    }
    Vec::new()
}

/// `EgressSelectorOptions.ApplyTo` (options/egress_selector.go:55): read,
/// validate and build the selector. `Ok(None)` is the no-op configuration.
pub fn apply_to(config_file: &str) -> Result<Option<EgressSelector>, String> {
    let np_config = read_egress_selector_configuration(config_file)
        .map_err(|e| format!("failed to read egress selector config: {e}"))?;
    let errs = validate_egress_selector_configuration(np_config.as_ref());
    if !errs.is_empty() {
        return Err(format!(
            "failed to validate egress selector configuration: {}",
            crate::authorization_config::aggregate_error(
                errs.iter().map(|e| e.to_string()).collect()
            )
        ));
    }
    EgressSelector::new(np_config.as_ref())
        .map_err(|e| format!("failed to setup egress selector with config {np_config:?}: {e}"))
}

/// `server.Config.EgressSelector`: upstream carries the selector on the server
/// config handed to every consumer; the Rust handlers get it from this
/// process-wide slot until it is threaded through `AppState`.
static EGRESS_SELECTOR: std::sync::OnceLock<Option<Arc<EgressSelector>>> =
    std::sync::OnceLock::new();

/// Installs the selector (`c.EgressSelector = cs`). First call wins.
pub fn set_egress_selector(selector: Option<EgressSelector>) {
    let _ = EGRESS_SELECTOR.set(selector.map(Arc::new));
}

/// The configured selector, `None` when no configuration was supplied.
pub fn egress_selector() -> Option<Arc<EgressSelector>> {
    EGRESS_SELECTOR.get().cloned().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::validation::field::ErrorType;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn tmp(name: &str, contents: &str) -> String {
        let p = std::env::temp_dir().join(format!(
            "egress-selector-{}-{}-{name}",
            std::process::id(),
            contents.len()
        ));
        std::fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn sel(name: &str, protocol: &str, transport: Option<Transport>) -> EgressSelection {
        EgressSelection {
            name: name.into(),
            connection: Connection {
                proxy_protocol: protocol.into(),
                transport,
            },
        }
    }

    fn cfg(selections: Vec<EgressSelection>) -> EgressSelectorConfiguration {
        EgressSelectorConfiguration {
            egress_selections: Some(selections),
        }
    }

    fn tls(dir: &str) -> TlsConfig {
        TlsConfig {
            ca_bundle: format!("{dir}/ca.crt"),
            client_key: format!("{dir}/client.key"),
            client_cert: format!("{dir}/client.crt"),
        }
    }

    fn tcp(url: &str, tls: Option<TlsConfig>) -> Transport {
        Transport {
            tcp: Some(TcpTransport {
                url: url.into(),
                tls_config: tls,
            }),
            uds: None,
        }
    }

    fn uds(name: &str) -> Transport {
        Transport {
            tcp: None,
            uds: Some(UdsTransport {
                uds_name: name.into(),
            }),
        }
    }

    // ---- config_test.go TestReadEgressSelectorConfiguration -------------

    const V1BETA1: &str = r#"
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: "cluster"
  connection:
    proxyProtocol: "HTTPConnect"
    transport:
      tcp:
        url: "https://127.0.0.1:8131"
        tlsConfig:
          caBundle: "/etc/srv/kubernetes/pki/konnectivity-server/ca.crt"
          clientKey: "/etc/srv/kubernetes/pki/konnectivity-server/client.key"
          clientCert: "/etc/srv/kubernetes/pki/konnectivity-server/client.crt"
- name: "controlplane"
  connection:
    proxyProtocol: "HTTPConnect"
    transport:
      tcp:
        url: "https://127.0.0.1:8132"
        tlsConfig:
          caBundle: "/etc/srv/kubernetes/pki/konnectivity-server-master/ca.crt"
          clientKey: "/etc/srv/kubernetes/pki/konnectivity-server-master/client.key"
          clientCert: "/etc/srv/kubernetes/pki/konnectivity-server-master/client.crt"
- name: "etcd"
  connection:
    proxyProtocol: "Direct"
"#;

    fn expected_v1beta1() -> EgressSelectorConfiguration {
        cfg(vec![
            sel(
                "cluster",
                "HTTPConnect",
                Some(tcp(
                    "https://127.0.0.1:8131",
                    Some(tls("/etc/srv/kubernetes/pki/konnectivity-server")),
                )),
            ),
            sel(
                "controlplane",
                "HTTPConnect",
                Some(tcp(
                    "https://127.0.0.1:8132",
                    Some(tls("/etc/srv/kubernetes/pki/konnectivity-server-master")),
                )),
            ),
            sel("etcd", "Direct", None),
        ])
    }

    #[test]
    fn read_empty_path_is_nil_config() {
        assert_eq!(read_egress_selector_configuration(""), Ok(None));
    }

    #[test]
    fn read_empty_file_errors() {
        let p = tmp("empty", "");
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(e, "Object 'Kind' is missing in 'null'");
    }

    #[test]
    fn read_absent_file_errors() {
        let e = read_egress_selector_configuration("/nonexistent/egress.yaml").unwrap_err();
        assert_eq!(
            e,
            "unable to read egress selector configuration from \"/nonexistent/egress.yaml\" [open /nonexistent/egress.yaml: no such file or directory]"
        );
    }

    #[test]
    fn read_unknown_field_is_strict_decoding_error() {
        let p = tmp(
            "unknown",
            r#"
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: "etcd"
  connection:
    proxyProtocol: "Direct"
  foo:
    bar: "baz"
"#,
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(
            e,
            "strict decoding error: unknown field \"egressSelections[0].foo\""
        );
    }

    #[test]
    fn read_duplicate_field_is_strict_decoding_error() {
        let p = tmp(
            "dup",
            r#"
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: "etcd"
  connection:
    proxyProtocol: "Direct"
  connection:
    proxyProtocol: "Indirect"
"#,
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(
            e,
            "strict decoding error: error converting YAML to JSON: yaml: unmarshal errors:\n  line 8: key \"connection\" already set in map"
        );
    }

    #[test]
    fn read_v1beta1() {
        let p = tmp("v1beta1", V1BETA1);
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
    }

    #[test]
    fn read_v1alpha1_same_shape() {
        let p = tmp(
            "v1alpha1",
            &V1BETA1.replace("apiserver.k8s.io/v1beta1", "apiserver.k8s.io/v1alpha1"),
        );
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
    }

    #[test]
    fn read_master_is_converted_to_controlplane() {
        let p = tmp(
            "master",
            &V1BETA1.replace("name: \"controlplane\"", "name: \"master\""),
        );
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
        let p = tmp(
            "master-a1",
            &V1BETA1
                .replace("name: \"controlplane\"", "name: \"master\"")
                .replace("apiserver.k8s.io/v1beta1", "apiserver.k8s.io/v1alpha1"),
        );
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
    }

    #[test]
    fn read_wrong_kind_errors() {
        let p = tmp(
            "wrong",
            "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: proxy-agent\n",
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(
            e,
            "no kind \"DaemonSet\" is registered for version \"apps/v1\" in scheme \"k8s.io/apiserver/pkg/server/egressselector/config.go:33\""
        );
    }

    #[test]
    fn read_v1_group_has_no_egress_selector_kind() {
        // The apiserver.config.k8s.io/v1 group does not register the kind
        // (v1/register.go), only the legacy apiserver.k8s.io group does.
        let p = tmp(
            "v1",
            &V1BETA1.replace("apiserver.k8s.io/v1beta1", "apiserver.config.k8s.io/v1"),
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert!(e.starts_with("no kind \"EgressSelectorConfiguration\" is registered for version \"apiserver.config.k8s.io/v1\""), "{e}");
    }

    // ---- config_test.go TestValidateEgressSelectorConfiguration ---------

    fn errs_of(c: &EgressSelectorConfiguration) -> Vec<String> {
        validate_egress_selector_configuration(Some(c))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn validate_nil_is_valid() {
        assert!(validate_egress_selector_configuration(None).is_empty());
    }

    #[test]
    fn validate_direct_valid() {
        assert!(errs_of(&cfg(vec![sel("controlplane", "Direct", None)])).is_empty());
    }

    #[test]
    fn validate_direct_invalid_transport() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "controlplane",
                "Direct",
                Some(Transport::default())
            )])),
            vec![
                "service.connection.transport: Invalid value: \"direct\": config must be absent for direct connect"
            ]
        );
    }

    #[test]
    fn validate_httpconnect_no_https() {
        assert!(errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp("http://127.0.0.1:8131", None))
        )]))
        .is_empty());
    }

    #[test]
    fn validate_http_with_tls_config_invalid() {
        let e = errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp("http://127.0.0.1:8131", Some(TlsConfig::default()))),
        )]));
        assert_eq!(
            e,
            vec!["service.connection.tlsConfig: Invalid value: \"nil\": config must not be present when using HTTP"]
        );
    }

    #[test]
    fn validate_httpconnect_https_no_cert_error() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "HTTPConnect",
                Some(tcp("https://127.0.0.1:8131", None))
            )])),
            vec!["service.connection.tlsConfig: Required value: TLSConfig must be present when using HTTPS"]
        );
    }

    #[test]
    fn validate_tls_files() {
        let e = errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    ca_bundle: "/nonexistent/ca".into(),
                    client_key: String::new(),
                    client_cert: "/nonexistent/cert".into(),
                }),
            )),
        )]));
        assert_eq!(
            e,
            vec![
                "service.connection.tlsConfig.caBundle: Invalid value: \"/nonexistent/ca\": TLS config ca bundle does not exist",
                "service.connection.tlsConfig.clientCert: Invalid value: \"/nonexistent/cert\": TLS client cert does not exist",
                "service.connection.tlsConfig.clientKey: Invalid value: \"nil\": Using TLS requires requires clientKey",
            ]
        );
        let e = errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    client_key: "/nonexistent/key".into(),
                    ..Default::default()
                }),
            )),
        )]));
        assert_eq!(
            e,
            vec![
                "service.connection.tlsConfig.clientCert: Invalid value: \"nil\": Using TLS requires clientCert",
                "service.connection.tlsConfig.clientKey: Invalid value: \"/nonexistent/key\": TLS client key does not exist",
            ]
        );
    }

    #[test]
    fn validate_tls_existing_files_ok() {
        let ca = tmp("ca", "x");
        let key = tmp("key", "xx");
        let crt = tmp("crt", "xxx");
        assert!(errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    ca_bundle: ca,
                    client_key: key,
                    client_cert: crt
                })
            ))
        )]))
        .is_empty());
    }

    #[test]
    fn validate_unsupported_url_scheme() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "HTTPConnect",
                Some(tcp("ftp://x", None))
            )])),
            vec!["service.connection.url: Invalid value: \"ftp://x\": supported connection protocols are http:// and https://"]
        );
    }

    #[test]
    fn validate_httpconnect_tcp_uds_both_set() {
        let t = Transport {
            tcp: Some(TcpTransport {
                url: "http://127.0.0.1:8131".into(),
                tls_config: None,
            }),
            uds: Some(UdsTransport {
                uds_name: "/s.socket".into(),
            }),
        };
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "HTTPConnect", Some(t))])),
            vec![
                "service.connection.tcp: Invalid value: {\"URL\":\"http://127.0.0.1:8131\",\"TLSConfig\":null}: TCP and UDS cannot both be set"
            ]
        );
    }

    #[test]
    fn validate_httpconnect_neither_tcp_nor_uds() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "HTTPConnect",
                Some(Transport::default())
            )])),
            vec!["service.connection.tcp: Required value: One of TCP or UDS must be set"]
        );
    }

    #[test]
    fn validate_httpconnect_and_grpc_require_transport() {
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "HTTPConnect", None)])),
            vec!["service.connection.transport: Required value: transport must be set for HTTPConnect"]
        );
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "GRPC", None)])),
            vec!["service.connection.transport: Required value: transport must be set for GRPC"]
        );
    }

    #[test]
    fn validate_httpconnect_uds() {
        assert!(errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(uds("/s.socket"))
        )]))
        .is_empty());
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "HTTPConnect", Some(uds("")))])),
            vec!["service.connection.udsName: Invalid value: \"nil\": must be present for UDS connections"]
        );
    }

    #[test]
    fn validate_grpc() {
        assert!(errs_of(&cfg(vec![sel("cluster", "GRPC", Some(uds("/s.socket")))])).is_empty());
        // grpc-https-invalid: TCP transport is not supported with GRPC.
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "GRPC",
                Some(tcp("http://127.0.0.1:8131", Some(TlsConfig::default())))
            )])),
            vec!["service.connection.uds: Required value: UDS must be set with GRPC"]
        );
    }

    #[test]
    fn validate_unknown_protocol() {
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "Indirect", None)])),
            vec!["service.connection.protocol: Unsupported value: \"Indirect\": supported values: \"Direct\", \"HTTPConnect\", \"GRPC\""]
        );
    }

    #[test]
    fn validate_invalid_name() {
        assert_eq!(
            errs_of(&cfg(vec![sel("invalid", "Direct", None)])),
            vec!["service.connection: Unsupported value: \"invalid\": supported values: \"cluster\", \"controlplane\", \"etcd\""]
        );
    }

    #[test]
    fn validate_names_are_case_insensitive_and_duplicates_rejected() {
        assert!(errs_of(&cfg(vec![sel("ControlPlane", "Direct", None)])).is_empty());
        let e = validate_egress_selector_configuration(Some(&cfg(vec![
            sel("controlplane", "Direct", None),
            sel("ControlPlane", "Direct", None),
        ])));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].error_type, ErrorType::Duplicate);
        assert_eq!(
            e[0].to_string(),
            "service.connection[1]: Duplicate value: \"controlplane\""
        );
    }

    // ---- egress_selector_test.go TestEgressSelector ---------------------

    #[test]
    fn selector_direct_for_all_types() {
        let s = EgressSelector::new(Some(&cfg(vec![
            sel("cluster", "Direct", None),
            sel("controlplane", "Direct", None),
            sel("etcd", "Direct", None),
        ])))
        .unwrap()
        .unwrap();
        for t in [
            EgressType::Cluster,
            EgressType::ControlPlane,
            EgressType::Etcd,
        ] {
            assert!(matches!(s.lookup(t), Some(Dialer::Direct)));
        }
    }

    #[test]
    fn selector_nil_config_is_no_selector() {
        assert!(EgressSelector::new(None).unwrap().is_none());
        assert!(
            EgressSelector::new(Some(&EgressSelectorConfiguration::default()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn selector_empty_selections_gives_selector_with_no_dialers() {
        let s = EgressSelector::new(Some(&cfg(vec![]))).unwrap().unwrap();
        assert!(s.lookup(EgressType::Cluster).is_none());
    }

    #[test]
    fn selector_unrecognized_name() {
        let e = EgressSelector::new(Some(&cfg(vec![sel("bogus", "Direct", None)])))
            .err()
            .unwrap();
        assert_eq!(e, "unrecognized service name bogus");
    }

    #[test]
    fn selector_unconfigured_type_has_no_dialer() {
        let s = EgressSelector::new(Some(&cfg(vec![sel("etcd", "Direct", None)])))
            .unwrap()
            .unwrap();
        assert!(s.lookup(EgressType::Cluster).is_none());
        assert!(s.lookup(EgressType::Etcd).is_some());
    }

    #[test]
    fn selector_unknown_protocol_errors() {
        let e = EgressSelector::new(Some(&cfg(vec![sel("etcd", "Bogus", None)])))
            .err()
            .unwrap();
        assert_eq!(
            e,
            "failed to create dialer for egressSelection \"etcd\": unrecognized service connection protocol \"Bogus\""
        );
    }

    #[test]
    fn selector_with_map() {
        let mut m = HashMap::new();
        m.insert(EgressType::Etcd, Dialer::Direct);
        let s = EgressSelector::with_map(m);
        assert!(s.lookup(EgressType::Etcd).is_some());
        assert!(s.lookup(EgressType::Cluster).is_none());
    }

    #[test]
    fn egress_type_names() {
        assert_eq!(EgressType::ControlPlane.to_string(), "controlplane");
        assert_eq!(EgressType::Etcd.to_string(), "etcd");
        assert_eq!(EgressType::Cluster.to_string(), "cluster");
    }

    // ---- egress_selector_test.go TestMetrics ----------------------------

    struct FakeConnector {
        connector_err: bool,
        proxier_err: bool,
    }
    struct FakeProxier {
        err: bool,
    }
    #[async_trait::async_trait]
    impl ProxyServerConnector for FakeConnector {
        async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
            if self.connector_err {
                return Err("fake error".into());
            }
            Ok(Box::new(FakeProxier {
                err: self.proxier_err,
            }))
        }
    }
    #[async_trait::async_trait]
    impl Proxier for FakeProxier {
        async fn proxy(&self, _addr: &str) -> Result<Conn, String> {
            if self.err {
                return Err("fake error".into());
            }
            let (a, _b) = tokio::io::duplex(8);
            Ok(Box::new(a))
        }
    }

    fn fake_dialer(connector_err: bool, proxier_err: bool, tag: &str) -> Dialer {
        DialerCreator {
            connector: Some(Arc::new(FakeConnector {
                connector_err,
                proxier_err,
            })),
            direct: false,
            options: MetricsOptions {
                transport: tag.into(),
                protocol: "fake_protocol".into(),
            },
        }
        .create_dialer()
    }

    fn counter(name: &str, labels: &[(&str, &str)]) -> f64 {
        for mf in prometheus::gather() {
            if mf.name() != name {
                continue;
            }
            for m in mf.get_metric() {
                let ok = labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                });
                if ok {
                    let n = m.get_histogram().get_sample_count();
                    return if n > 0 {
                        n as f64
                    } else {
                        m.get_counter().value()
                    };
                }
            }
        }
        0.0
    }

    #[tokio::test]
    async fn metrics_connect_failure() {
        let d = fake_dialer(true, true, "t_connect");
        assert!(d.dial("", "").await.is_err());
        let l = [("protocol", "fake_protocol"), ("transport", "t_connect")];
        assert_eq!(counter("apiserver_egress_dialer_dial_start_total", &l), 1.0);
        assert_eq!(
            counter(
                "apiserver_egress_dialer_dial_failure_count",
                &[l[0], l[1], ("stage", "connect")]
            ),
            1.0
        );
    }

    #[tokio::test]
    async fn metrics_proxy_failure() {
        let d = fake_dialer(false, true, "t_proxy");
        assert!(d.dial("", "").await.is_err());
        assert_eq!(
            counter(
                "apiserver_egress_dialer_dial_failure_count",
                &[
                    ("protocol", "fake_protocol"),
                    ("transport", "t_proxy"),
                    ("stage", "proxy")
                ]
            ),
            1.0
        );
    }

    #[tokio::test]
    async fn metrics_success_observes_latency() {
        let d = fake_dialer(false, false, "t_ok");
        assert!(d.dial("", "").await.is_ok());
        assert_eq!(
            counter(
                "apiserver_egress_dialer_dial_duration_seconds",
                &[("protocol", "fake_protocol"), ("transport", "t_ok")]
            ),
            1.0
        );
    }

    // ---- dialing ---------------------------------------------------------

    #[tokio::test]
    async fn direct_dialer_connects() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let accept = tokio::spawn(async move { l.accept().await.unwrap().0 });
        let mut c = Dialer::Direct.dial("tcp", &addr).await.unwrap();
        let mut s = accept.await.unwrap();
        c.write_all(b"hi").await.unwrap();
        let mut b = [0u8; 2];
        s.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"hi");
    }

    /// A proxy that answers one CONNECT with `status_line` and then echoes.
    async fn fake_connect_proxy(sock: &str, status_line: &'static str, extra: &'static [u8]) {
        let l = tokio::net::UnixListener::bind(sock).unwrap();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut b = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut b).await.unwrap();
                buf.push(b[0]);
            }
            let mut resp = format!("{status_line}\r\n\r\n").into_bytes();
            resp.extend_from_slice(extra);
            s.write_all(&resp).await.unwrap();
            let mut e = [0u8; 4];
            if s.read_exact(&mut e).await.is_ok() {
                let _ = s.write_all(&e).await;
            }
        });
    }

    fn uds_dialer(sock: &str) -> Dialer {
        EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(uds(sock)),
        )])))
        .unwrap()
        .unwrap()
        .lookup(EgressType::Cluster)
        .unwrap()
    }

    fn sock_path(n: &str) -> String {
        let p = std::env::temp_dir().join(format!("egress-{}-{n}.sock", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn http_connect_over_uds_tunnels() {
        let sock = sock_path("ok");
        fake_connect_proxy(&sock, "HTTP/1.1 200 Connection established", b"").await;
        let mut c = uds_dialer(&sock)
            .dial("tcp", "10.0.0.1:10250")
            .await
            .unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut e = [0u8; 4];
        c.read_exact(&mut e).await.unwrap();
        assert_eq!(&e, b"ping");
    }

    #[tokio::test]
    async fn http_connect_non_200_is_proxy_error() {
        let sock = sock_path("503");
        fake_connect_proxy(&sock, "HTTP/1.1 503 Service Unavailable", b"").await;
        let e = uds_dialer(&sock)
            .dial("tcp", "10.0.0.1:10250")
            .await
            .err()
            .unwrap();
        assert_eq!(
            e,
            format!(
                "proxy error from {sock} while dialing 10.0.0.1:10250, code 503: 503 Service Unavailable"
            )
        );
    }

    #[tokio::test]
    async fn http_connect_unexpected_buffered_data_is_error() {
        let sock = sock_path("buf");
        fake_connect_proxy(&sock, "HTTP/1.1 200 OK", b"junk").await;
        let e = uds_dialer(&sock)
            .dial("tcp", "10.0.0.1:10250")
            .await
            .err()
            .unwrap();
        assert_eq!(
            e,
            format!("unexpected 4 bytes of buffered data from CONNECT proxy \"{sock}\"")
        );
    }

    #[tokio::test]
    async fn http_connect_uds_missing_socket_is_connect_error() {
        let sock = sock_path("missing");
        assert!(uds_dialer(&sock).dial("tcp", "1.2.3.4:5").await.is_err());
    }

    #[test]
    #[should_panic(expected = "nil pointer dereference")]
    fn http_scheme_without_tls_config_panics_like_upstream() {
        // egress_selector.go:329 calls getTLSConfig(c.Transport.TCP.TLSConfig)
        // unconditionally while validation (config.go:162) allows http:// with
        // no tlsConfig: a nil dereference in Go, replicated here.
        let _ = EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp("http://127.0.0.1:8131", None)),
        )])));
    }

    #[test]
    fn bad_key_pair_is_error() {
        let ca = tmp("kp-ca", "x");
        let e = EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    ca_bundle: ca,
                    client_key: "/nonexistent/k".into(),
                    client_cert: "/nonexistent/c".into(),
                }),
            )),
        )])))
        .err()
        .unwrap();
        assert!(
            e.starts_with(
                "failed to create dialer for egressSelection \"cluster\": failed to read key pair /nonexistent/c & /nonexistent/k, got "
            ),
            "{e}"
        );
    }

    // ---- options/egress_selector.go -------------------------------------

    #[test]
    fn validate_options_checks_file_exists() {
        assert!(validate_options("").is_empty());
        assert_eq!(
            validate_options("/nonexistent/egress.yaml"),
            vec!["egress-selector-config-file /nonexistent/egress.yaml does not exist"]
        );
        assert!(validate_options(&tmp("opt", "x")).is_empty());
    }

    #[test]
    fn apply_to_no_file_is_noop() {
        assert!(apply_to("").unwrap().is_none());
    }

    #[test]
    fn apply_to_builds_selector_from_file() {
        let p = tmp(
            "apply",
            "apiVersion: apiserver.k8s.io/v1beta1\nkind: EgressSelectorConfiguration\negressSelections:\n- name: etcd\n  connection:\n    proxyProtocol: Direct\n",
        );
        let s = apply_to(&p).unwrap().unwrap();
        assert!(matches!(s.lookup(EgressType::Etcd), Some(Dialer::Direct)));
    }

    #[test]
    fn apply_to_reports_read_and_validation_errors() {
        let e = apply_to("/nonexistent/egress.yaml").err().unwrap();
        assert!(
            e.starts_with("failed to read egress selector config: unable to read"),
            "{e}"
        );
        let p = tmp(
            "apply-bad",
            "apiVersion: apiserver.k8s.io/v1beta1\nkind: EgressSelectorConfiguration\negressSelections:\n- name: bogus\n  connection:\n    proxyProtocol: Direct\n",
        );
        assert_eq!(
            apply_to(&p).err().unwrap(),
            "failed to validate egress selector configuration: service.connection: Unsupported value: \"bogus\": supported values: \"cluster\", \"controlplane\", \"etcd\""
        );
    }

    #[test]
    fn flag_is_parsed() {
        use clap::Parser;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            e: EgressSelectorArgs,
        }
        let c = Cli::parse_from(["x", "--egress-selector-config-file", "/etc/egress.yaml"]);
        assert_eq!(c.e.config_file, "/etc/egress.yaml");
        assert_eq!(Cli::parse_from(["x"]).e.config_file, "");
    }

    // ---- HTTP CONNECT over TLS (tcp transport) --------------------------

    #[tokio::test]
    async fn http_connect_over_tls_tcp_tunnels() {
        use std::sync::Arc;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let dir = std::env::temp_dir().join(format!("egress-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (crt, k) = (dir.join("client.crt"), dir.join("client.key"));
        std::fs::write(&crt, cert.pem()).unwrap();
        std::fs::write(&k, key.serialize_pem()).unwrap();

        let server_cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let mut s = acceptor.accept(tcp).await.unwrap();
            let mut buf = Vec::new();
            let mut b = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut b).await.unwrap();
                buf.push(b[0]);
            }
            assert_eq!(
                String::from_utf8(buf).unwrap(),
                "CONNECT 10.0.0.1:10250 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
            );
            s.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            let mut e = [0u8; 4];
            s.read_exact(&mut e).await.unwrap();
            s.write_all(&e).await.unwrap();
            s.flush().await.unwrap();
        });

        let d = EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                &format!("https://127.0.0.1:{port}"),
                Some(TlsConfig {
                    ca_bundle: crt.to_string_lossy().into_owned(),
                    client_key: k.to_string_lossy().into_owned(),
                    client_cert: crt.to_string_lossy().into_owned(),
                }),
            )),
        )])))
        .unwrap()
        .unwrap()
        .lookup(EgressType::Cluster)
        .unwrap();
        let mut c = d.dial("tcp", "10.0.0.1:10250").await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut e = [0u8; 4];
        c.read_exact(&mut e).await.unwrap();
        assert_eq!(&e, b"ping");
    }

    #[test]
    fn proxy_address_is_url_host() {
        assert_eq!(
            get_proxy_address("https://127.0.0.1:8131").unwrap(),
            "127.0.0.1:8131"
        );
        assert_eq!(
            get_proxy_address("https://u@proxy.example/x?y").unwrap(),
            "proxy.example"
        );
        assert!(get_proxy_address("://bad").is_err());
    }
}
