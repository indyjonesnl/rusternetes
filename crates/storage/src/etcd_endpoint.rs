//! etcd endpoint parsing, including `unix:` / `unixs:` socket endpoints.
//!
//! Ported from `translateEndpoint`
//! (`vendor/go.etcd.io/etcd/client/v3/internal/endpoint/endpoint.go`, as
//! vendored by kubernetes release-1.35), which is how the upstream apiserver's
//! `newETCD3Client` (`staging/src/k8s.io/apiserver/pkg/storage/storagebackend/
//! factory/etcd3.go`, `Endpoints: c.ServerList`) reaches kine/k3s/k0s over a
//! unix socket.
//!
//! DELIBERATE, FORCED DEVIATION in *transport only*: `clientv3` hands the
//! translated address to grpc-go, whose resolver dials unix sockets. The
//! Rust `etcd-client` 0.14.1 exposes no connector hook (its `Client`
//! constructors are `pub(crate)` and `build_endpoint` always yields a TCP
//! tonic `Endpoint`), so [`UnixBridge`] terminates a loopback TCP listener and
//! splices each connection onto the socket. Parsing is the upstream one.

use rusternetes_common::{Error, Result};
use std::path::PathBuf;
use tokio::net::{TcpListener, UnixStream};

/// What an endpoint string resolves to.
#[derive(Debug, PartialEq, Eq)]
pub enum ParsedEndpoint {
    /// Anything that is not `unix:`/`unixs:`: passed to etcd-client untouched.
    Passthrough(String),
    /// A unix socket path. `tls` is true for the `unixs` scheme
    /// (`CredsRequire` upstream).
    Unix { path: PathBuf, tls: bool },
}

/// Port of `translateEndpoint` for the `unix:`/`unixs:` branch. The three
/// accepted spellings and their results:
///
/// * `unix:///abs/path`  -> `/abs/path`   ("absolute path case")
/// * `unix://rel/path`   -> `rel/path`    ("legacy etcd local path")
/// * `unix:path`         -> `path`        (grpc canonical form)
///
/// The path is taken verbatim: k0s's `unix:///run/k0s/kine/kine.sock:2379`
/// names a socket file literally called `kine.sock:2379`; upstream only uses
/// the last segment as the TLS server name and never strips `:port`.
pub fn parse_endpoint(ep: &str) -> ParsedEndpoint {
    let (scheme, rest) = if let Some(r) = ep.strip_prefix("unixs:") {
        ("unixs", r)
    } else if let Some(r) = ep.strip_prefix("unix:") {
        ("unix", r)
    } else {
        return ParsedEndpoint::Passthrough(ep.to_string());
    };
    let path = if rest.starts_with("///") {
        // `unix:///abs` -> "/abs"
        &rest[2..]
    } else if let Some(local) = rest.strip_prefix("//") {
        local
    } else {
        rest
    };
    ParsedEndpoint::Unix {
        path: PathBuf::from(path),
        tls: scheme == "unixs",
    }
}

/// Loopback TCP -> unix socket splice. The accept loop is aborted on drop.
pub struct UnixBridge {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for UnixBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl UnixBridge {
    /// Bind `127.0.0.1:0`; returns the bridge and the `host:port` to give
    /// etcd-client.
    pub(crate) async fn start(path: PathBuf) -> Result<(Self, String)> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage(format!("unix etcd bridge bind failed: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| Error::Storage(format!("unix etcd bridge addr failed: {e}")))?;
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut tcp, _)) = listener.accept().await else {
                    continue;
                };
                let path = path.clone();
                tokio::spawn(async move {
                    if let Ok(mut unix) = UnixStream::connect(&path).await {
                        let _ = tokio::io::copy_bidirectional(&mut tcp, &mut unix).await;
                    }
                });
            }
        });
        Ok((Self { task }, addr.to_string()))
    }
}

/// Resolve each endpoint: unix endpoints get a bridge and become `host:port`.
pub async fn resolve_endpoints(endpoints: Vec<String>) -> Result<(Vec<String>, Vec<UnixBridge>)> {
    let mut out = Vec::with_capacity(endpoints.len());
    let mut bridges = Vec::new();
    for ep in endpoints {
        match parse_endpoint(&ep) {
            ParsedEndpoint::Passthrough(s) => out.push(s),
            ParsedEndpoint::Unix { tls: true, .. } => {
                return Err(Error::Storage(format!(
                    "etcd endpoint {ep}: the unixs scheme requires TLS, which is not supported"
                )));
            }
            ParsedEndpoint::Unix { path, .. } => {
                let (bridge, addr) = UnixBridge::start(path).await?;
                out.push(addr);
                bridges.push(bridge);
            }
        }
    }
    Ok((out, bridges))
}
