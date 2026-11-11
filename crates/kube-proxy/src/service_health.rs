//! Port of pkg/proxy/healthcheck/service_health.go and the listener/HTTP
//! server seams of pkg/proxy/healthcheck/common.go (release-1.35).
//!
//! `ServiceHealthServer` serves one HTTP endpoint per Service that has a
//! `healthCheckNodePort` (type=LoadBalancer, externalTrafficPolicy=Local).
//! Load balancers poll it to learn whether this node has local ready
//! endpoints: 503 when there are none (or kube-proxy itself is unhealthy),
//! 200 otherwise.
//!
//! Deviations, each because the Rust stack has no equivalent of the Go one:
//! - `newServiceHealthServer` takes the already-resolved `node_ips`;
//!   `proxyutil.NodePortAddresses` (`--nodeport-addresses`) is not ported yet,
//!   so the caller passes `[0.0.0.0]`, upstream's default when
//!   `nodePortAddresses.MatchAll()`.
//! - `events.EventRecorder` is the small [`EventRecorder`] trait below.
//! - `http.Server` is an axum server on tokio; `Serve` runs on a spawned task
//!   (the goroutine upstream) and `Close` interrupts it.

use crate::serve_metrics::{ProxyHealth, ProxyHealthServer};
use axum::{
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use futures::future::BoxFuture;
use std::{
    any::Any,
    collections::HashMap,
    fmt, io,
    net::{IpAddr, SocketAddr, TcpListener},
    sync::{Arc, RwLock},
};
use tokio::sync::Notify;
use tracing::{debug, error, info};

/// `types.NamespacedName`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamespacedName {
    pub namespace: String,
    pub name: String,
}

impl NamespacedName {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl fmt::Display for NamespacedName {
    /// `NamespacedName.String()`: `namespace/name`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// `healthcheck.ServiceHealthServer` (service_health.go:41-55).
pub trait ServiceHealthServer: Send + Sync {
    /// Make the new set of services be active. Services that were open before
    /// will be closed. Services that are new will be opened. Service that
    /// existed and are in the new set will be left alone. The value of the map
    /// is the healthcheck-port to listen on.
    fn sync_services(&self, new_services: &HashMap<NamespacedName, u16>) -> io::Result<()>;
    /// Make the new set of endpoints be active. Endpoints for services that do
    /// not exist will be dropped. The value of the map is the number of
    /// endpoints the service has on this node.
    fn sync_endpoints(&self, new_endpoints: &HashMap<NamespacedName, usize>) -> io::Result<()>;
}

/// `proxyHealthChecker` (service_health.go:57-60).
pub trait ProxyHealthChecker: Send + Sync {
    /// Health returns the proxy's health state and last updated time.
    fn health(&self) -> ProxyHealth;
}

impl ProxyHealthChecker for ProxyHealthServer {
    fn health(&self) -> ProxyHealth {
        ProxyHealthServer::health(self)
    }
}

/// The subset of `events.EventRecorder.Eventf` used by SyncServices.
pub trait EventRecorder: Send + Sync {
    /// A Warning event regarding the v1 Service `namespace/name`.
    fn warning(&self, namespace: &str, name: &str, reason: &str, action: &str, note: &str);
}

/// `healthcheck.listener` (common.go:28-33). `Listen` is fixed to "tcp".
pub trait Listener: Send + Sync {
    fn listen(&self, addr: &str) -> io::Result<Box<dyn NetListener>>;
}

/// `net.Listener`.
pub trait NetListener: Send {
    fn addr(&self) -> String;
    /// The real socket, for [`StdHttpServer`]; fakes return `None`.
    fn into_tcp(self: Box<Self>) -> Option<TcpListener>;
}

/// `healthcheck.httpServerFactory` (common.go:35-40).
pub trait HttpServerFactory: Send + Sync {
    fn new_server(&self, handler: Router) -> Arc<dyn HttpServer>;
}

/// `healthcheck.httpServer` (common.go:42-47).
pub trait HttpServer: Send + Sync {
    /// Runs until the server is closed; a closed server returns `Ok(())`
    /// (`http.ErrServerClosed` is not an error to the caller).
    fn serve(&self, listener: Box<dyn NetListener>) -> BoxFuture<'static, io::Result<()>>;
    fn close(&self) -> io::Result<()>;
    fn as_any(&self) -> &dyn Any;
}

/// `stdNetListener` (common.go:49-56).
pub struct StdNetListener;

struct StdListener(TcpListener);

impl NetListener for StdListener {
    fn addr(&self) -> String {
        self.0
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default()
    }
    fn into_tcp(self: Box<Self>) -> Option<TcpListener> {
        Some(self.0)
    }
}

impl Listener for StdNetListener {
    fn listen(&self, addr: &str) -> io::Result<Box<dyn NetListener>> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Box::new(StdListener(listener)))
    }
}

/// `stdHTTPServerFactory` (common.go:58-66).
pub struct StdHttpServerFactory;

impl HttpServerFactory for StdHttpServerFactory {
    fn new_server(&self, handler: Router) -> Arc<dyn HttpServer> {
        Arc::new(StdHttpServer {
            handler,
            closed: Arc::new(Notify::new()),
        })
    }
}

/// `http.Server` stand-in.
pub struct StdHttpServer {
    handler: Router,
    closed: Arc<Notify>,
}

impl HttpServer for StdHttpServer {
    fn serve(&self, listener: Box<dyn NetListener>) -> BoxFuture<'static, io::Result<()>> {
        let handler = self.handler.clone();
        let closed = Arc::clone(&self.closed);
        Box::pin(async move {
            let Some(tcp) = listener.into_tcp() else {
                return Ok(());
            };
            let tcp = tokio::net::TcpListener::from_std(tcp)?;
            tokio::select! {
                result = axum::serve(tcp, handler) => result,
                // notify_one stores a permit, so a Close that races ahead of
                // this task still stops it.
                _ = closed.notified() => Ok(()),
            }
        })
    }

    fn close(&self) -> io::Result<()> {
        self.closed.notify_one();
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
struct HcInstance {
    port: u16,
    http_servers: Vec<Arc<dyn HttpServer>>,
    /// number of local endpoints for a service
    endpoints: usize,
}

type Services = HashMap<NamespacedName, HcInstance>;

impl HcInstance {
    /// `closeAll`: errors are logged and aggregated.
    fn close_all(&self, nsn: &NamespacedName) -> io::Result<()> {
        let mut errors = Vec::new();
        for server in &self.http_servers {
            if let Err(err) = server.close() {
                error!(error = %err, service = %nsn, "Error closing server for health check service");
                errors.push(err.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(errors.join("; ")))
        }
    }
}

struct Shared {
    healthz_server: Arc<dyn ProxyHealthChecker>,
    services: RwLock<Services>,
}

/// `healthcheck.server` (service_health.go:99-109).
pub struct Server {
    node_name: String,
    /// node addresses where health check port will listen on
    node_ips: Vec<IpAddr>,
    recorder: Option<Arc<dyn EventRecorder>>,
    listener: Arc<dyn Listener>,
    http_factory: Arc<dyn HttpServerFactory>,
    shared: Arc<Shared>,
}

/// `newServiceHealthServer` (service_health.go:62-85), with `node_ips`
/// already resolved (see the module docs).
pub fn new_service_health_server(
    node_name: &str,
    recorder: Option<Arc<dyn EventRecorder>>,
    listener: Arc<dyn Listener>,
    factory: Arc<dyn HttpServerFactory>,
    node_ips: Vec<IpAddr>,
    healthz_server: Arc<dyn ProxyHealthChecker>,
) -> Server {
    Server {
        node_name: node_name.to_owned(),
        node_ips,
        recorder,
        listener,
        http_factory: factory,
        shared: Arc::new(Shared {
            healthz_server,
            services: RwLock::new(HashMap::new()),
        }),
    }
}

/// `NewServiceHealthServer` (service_health.go:87-90). `node_ips` is
/// `[0.0.0.0]` unless `--nodeport-addresses` narrows it (service_health.go:63-74).
pub fn new_std_service_health_server(
    node_name: &str,
    recorder: Option<Arc<dyn EventRecorder>>,
    node_ips: Vec<IpAddr>,
    healthz_server: Arc<dyn ProxyHealthChecker>,
) -> Server {
    new_service_health_server(
        node_name,
        recorder,
        Arc::new(StdNetListener),
        Arc::new(StdHttpServerFactory),
        node_ips,
        healthz_server,
    )
}

/// `net.JoinHostPort(ip.String(), port)`.
fn join_host_port(ip: &IpAddr, port: u16) -> String {
    SocketAddr::new(*ip, port).to_string()
}

impl Server {
    /// `hcInstance.listenAndServeAll` (service_health.go:154-195).
    fn listen_and_serve_all(&self, nsn: &NamespacedName, hci: &mut HcInstance) -> io::Result<()> {
        hci.http_servers = Vec::with_capacity(self.node_ips.len());
        // for each of the node addresses start listening and serving
        for ip in &self.node_ips {
            let addr = join_host_port(ip, hci.port);
            // create http server
            let http_srv = self
                .http_factory
                .new_server(handler_router(nsn.clone(), Arc::clone(&self.shared)));
            // start listener
            let listener = match self.listener.listen(&addr) {
                Ok(l) => l,
                Err(err) => {
                    // must close whatever have been previously opened
                    // to allow a retry/or port ownership change as needed
                    let _ = hci.close_all(nsn);
                    return Err(err);
                }
            };
            // start serving
            let nsn = nsn.clone();
            debug!(service = %nsn, address = %listener.addr(), "Starting goroutine for healthcheck");
            let serving = http_srv.serve(listener);
            tokio::spawn(async move {
                // Serve() will exit and return ErrServerClosed when the http
                // server is closed.
                match serving.await {
                    Err(err) => error!(error = %err, service = %nsn, "Healthcheck closed"),
                    Ok(()) => debug!(service = %nsn, "Healthcheck closed"),
                }
            });
            hci.http_servers.push(http_srv);
        }
        Ok(())
    }
}

impl ServiceHealthServer for Server {
    /// service_health.go:111-152.
    fn sync_services(&self, new_services: &HashMap<NamespacedName, u16>) -> io::Result<()> {
        let mut services = self.shared.services.write().expect("healthcheck lock");

        // Remove any that are not needed any more.
        services.retain(|nsn, svc| {
            if new_services.get(nsn).is_some_and(|port| *port == svc.port) {
                return true;
            }
            info!(service = %nsn, port = svc.port, "Closing healthcheck");
            // errors are logged in close_all()
            let _ = svc.close_all(nsn);
            false
        });

        // Add any that are needed.
        for (nsn, port) in new_services {
            if services.contains_key(nsn) {
                debug!(service = %nsn, port, "Existing healthcheck");
                continue;
            }

            info!(service = %nsn, port, "Opening healthcheck");

            let mut svc = HcInstance {
                port: *port,
                ..Default::default()
            };
            if let Err(err) = self.listen_and_serve_all(nsn, &mut svc) {
                let msg = format!(
                    "node {} failed to start healthcheck {:?} on port {}: {}",
                    self.node_name,
                    nsn.to_string(),
                    port,
                    err
                );
                if let Some(recorder) = &self.recorder {
                    recorder.warning(
                        &nsn.namespace,
                        &nsn.name,
                        "FailedToStartServiceHealthcheck",
                        "Listen",
                        &msg,
                    );
                }
                error!(error = %err, node = %self.node_name, service = %nsn, port, "Failed to start healthcheck");
                continue;
            }
            services.insert(nsn.clone(), svc);
        }
        Ok(())
    }

    /// service_health.go:259-276.
    fn sync_endpoints(&self, new_endpoints: &HashMap<NamespacedName, usize>) -> io::Result<()> {
        let mut services = self.shared.services.write().expect("healthcheck lock");

        for (nsn, count) in new_endpoints {
            let Some(svc) = services.get_mut(nsn) else {
                continue;
            };
            debug!(endpoint_count = count, service = %nsn, "Reporting endpoints for healthcheck");
            svc.endpoints = *count;
        }
        for (nsn, hci) in services.iter_mut() {
            if !new_endpoints.contains_key(nsn) {
                hci.endpoints = 0;
            }
        }
        Ok(())
    }
}

/// `hcHandler.ServeHTTP` (service_health.go:197-257): every path and method
/// reaches the same handler.
fn handler_router(name: NamespacedName, shared: Arc<Shared>) -> Router {
    Router::new().fallback(move || {
        let (name, shared) = (name.clone(), Arc::clone(&shared));
        async move { serve_http(&name, &shared) }
    })
}

fn serve_http(name: &NamespacedName, shared: &Shared) -> Response {
    let count = {
        let services = shared.services.read().expect("healthcheck lock");
        match services.get(name) {
            Some(svc) => svc.endpoints,
            None => {
                // Go returns without writing: an empty 200.
                error!(service = %name, "Received request for closed healthcheck");
                return StatusCode::OK.into_response();
            }
        }
    };
    let kube_proxy_healthy = shared.healthz_server.health().healthy;

    let status = if count != 0 && kube_proxy_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    // dedent + Trim "\n" of the Go raw string.
    let body = format!(
        "{{\n\t\"service\": {{\n\t\t\"namespace\": {},\n\t\t\"name\": {}\n\t}},\n\t\"localEndpoints\": {},\n\t\"serviceProxyHealthy\": {}\n}}",
        serde_json::Value::from(name.namespace.as_str()),
        serde_json::Value::from(name.name.as_str()),
        count,
        kube_proxy_healthy
    );
    let mut resp = (status, body).into_response();
    let headers = resp.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "X-Load-Balancing-Endpoint-Weight",
        HeaderValue::from_str(&count.to_string()).expect("digits"),
    );
    resp
}

/// `FakeServiceHealthServer` (service_health.go:278-291).
pub struct FakeServiceHealthServer;

impl ServiceHealthServer for FakeServiceHealthServer {
    fn sync_services(&self, _: &HashMap<NamespacedName, u16>) -> io::Result<()> {
        Ok(())
    }
    fn sync_endpoints(&self, _: &HashMap<NamespacedName, usize>) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Port of TestServer and TestServerWithSelectiveListeningAddress
    //! (pkg/proxy/healthcheck/healthcheck_test.go:148-432, 958-1006). They
    //! live in-module because upstream's tests inspect `hcs.services`.
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct FakeListener {
        open_ports: Arc<Mutex<HashSet<String>>>,
    }

    struct FakeNetListener;

    impl NetListener for FakeNetListener {
        fn addr(&self) -> String {
            "<test>".into()
        }
        fn into_tcp(self: Box<Self>) -> Option<TcpListener> {
            None
        }
    }

    impl Listener for FakeListener {
        fn listen(&self, addr: &str) -> io::Result<Box<dyn NetListener>> {
            self.open_ports.lock().unwrap().insert(addr.to_owned());
            Ok(Box::new(FakeNetListener))
        }
    }

    struct FakeHttpServer {
        handler: Router,
    }

    impl HttpServer for FakeHttpServer {
        fn serve(&self, _: Box<dyn NetListener>) -> BoxFuture<'static, io::Result<()>> {
            Box::pin(async { Ok(()) }) // Cause the goroutine to return
        }
        fn close(&self) -> io::Result<()> {
            Ok(())
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct FakeHttpServerFactory;

    impl HttpServerFactory for FakeHttpServerFactory {
        fn new_server(&self, handler: Router) -> Arc<dyn HttpServer> {
            Arc::new(FakeHttpServer { handler })
        }
    }

    struct FakeProxyHealthChecker(Mutex<bool>);

    impl ProxyHealthChecker for FakeProxyHealthChecker {
        fn health(&self) -> ProxyHealth {
            // we only need "healthy" field for testing service healthchecks.
            ProxyHealth::for_test(*self.0.lock().unwrap())
        }
    }

    fn mknsn(ns: &str, name: &str) -> NamespacedName {
        NamespacedName::new(ns, name)
    }

    fn setup(node_ips: Vec<IpAddr>) -> (Server, Arc<FakeListener>, Arc<FakeProxyHealthChecker>) {
        let listener = Arc::new(FakeListener::default());
        let checker = Arc::new(FakeProxyHealthChecker(Mutex::new(true)));
        let hcs = new_service_health_server(
            "hostname",
            None,
            listener.clone(),
            Arc::new(FakeHttpServerFactory),
            node_ips,
            checker.clone(),
        );
        (hcs, listener, checker)
    }

    fn nservices(hcs: &Server) -> usize {
        hcs.shared.services.read().unwrap().len()
    }

    fn endpoints(hcs: &Server, nsn: &NamespacedName) -> usize {
        hcs.shared.services.read().unwrap()[nsn].endpoints
    }

    fn open_ports(l: &FakeListener) -> usize {
        l.open_ports.lock().unwrap().len()
    }

    async fn t_handler(
        hcs: &Server,
        nsn: &NamespacedName,
        status: u16,
        endpoints: usize,
        kube_proxy_healthy: bool,
    ) {
        let handlers: Vec<Router> = hcs.shared.services.read().unwrap()[nsn]
            .http_servers
            .iter()
            .map(|h| {
                h.as_any()
                    .downcast_ref::<FakeHttpServer>()
                    .unwrap()
                    .handler
                    .clone()
            })
            .collect();
        for handler in handlers {
            let req = axum::http::Request::get("/healthz")
                .body(axum::body::Body::empty())
                .unwrap();
            let resp = handler.oneshot(req).await.unwrap();
            assert_eq!(resp.status().as_u16(), status);
            let headers = resp.headers().clone();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(payload["service"]["name"], nsn.name.as_str());
            assert_eq!(payload["service"]["namespace"], nsn.namespace.as_str());
            assert_eq!(payload["localEndpoints"], endpoints);
            assert_eq!(payload["serviceProxyHealthy"], kube_proxy_healthy);
            assert_eq!(headers.get_all("Content-Type").iter().count(), 1);
            assert_eq!(headers["Content-Type"], "application/json");
            assert_eq!(headers["X-Content-Type-Options"], "nosniff");
            assert_eq!(
                headers["X-Load-Balancing-Endpoint-Weight"],
                endpoints.to_string().as_str()
            );
        }
    }

    async fn test_handler(hcs: &Server, nsn: &NamespacedName, status: u16, endpoints: usize) {
        t_handler(hcs, nsn, status, endpoints, true).await;
    }

    fn svcs(v: &[(&NamespacedName, u16)]) -> HashMap<NamespacedName, u16> {
        v.iter().map(|(n, p)| ((*n).clone(), *p)).collect()
    }

    fn eps(v: &[(&NamespacedName, usize)]) -> HashMap<NamespacedName, usize> {
        v.iter().map(|(n, p)| ((*n).clone(), *p)).collect()
    }

    #[tokio::test]
    async fn test_server() {
        let (hcs, listener, checker) = setup(vec![IpAddr::from([0, 0, 0, 0])]);
        assert_eq!(nservices(&hcs), 0);

        // sync nothing
        hcs.sync_services(&HashMap::new()).unwrap();
        assert_eq!(nservices(&hcs), 0);
        hcs.sync_endpoints(&HashMap::new()).unwrap();
        assert_eq!(nservices(&hcs), 0);

        // sync unknown endpoints, should be dropped
        hcs.sync_endpoints(&eps(&[(&mknsn("a", "b"), 93)])).unwrap();
        assert_eq!(nservices(&hcs), 0);

        // sync a real service
        let nsn = mknsn("a", "b");
        hcs.sync_services(&svcs(&[(&nsn, 9376)])).unwrap();
        assert_eq!(nservices(&hcs), 1);
        assert_eq!(endpoints(&hcs, &nsn), 0);
        assert_eq!(open_ports(&listener), 1);
        assert!(listener.open_ports.lock().unwrap().contains("0.0.0.0:9376"));
        test_handler(&hcs, &nsn, 503, 0).await;

        // sync an endpoint
        hcs.sync_endpoints(&eps(&[(&nsn, 18)])).unwrap();
        assert_eq!(nservices(&hcs), 1);
        assert_eq!(endpoints(&hcs, &nsn), 18);
        test_handler(&hcs, &nsn, 200, 18).await;

        // sync zero endpoints
        hcs.sync_endpoints(&eps(&[(&nsn, 0)])).unwrap();
        assert_eq!(endpoints(&hcs, &nsn), 0);
        test_handler(&hcs, &nsn, 503, 0).await;

        // put the endpoint back
        hcs.sync_endpoints(&eps(&[(&nsn, 11)])).unwrap();
        assert_eq!(endpoints(&hcs, &nsn), 11);
        // sync nil endpoints
        hcs.sync_endpoints(&HashMap::new()).unwrap();
        assert_eq!(nservices(&hcs), 1);
        assert_eq!(endpoints(&hcs, &nsn), 0);
        test_handler(&hcs, &nsn, 503, 0).await;

        // put the endpoint back
        hcs.sync_endpoints(&eps(&[(&nsn, 18)])).unwrap();
        assert_eq!(endpoints(&hcs, &nsn), 18);
        // delete the service
        hcs.sync_services(&HashMap::new()).unwrap();
        assert_eq!(nservices(&hcs), 0);

        // sync multiple services
        let nsn1 = mknsn("a", "b");
        let nsn2 = mknsn("c", "d");
        let nsn3 = mknsn("e", "f");
        let nsn4 = mknsn("g", "h");
        hcs.sync_services(&svcs(&[(&nsn1, 9376), (&nsn2, 12909), (&nsn3, 11113)]))
            .unwrap();
        assert_eq!(nservices(&hcs), 3);
        for n in [&nsn1, &nsn2, &nsn3] {
            assert_eq!(endpoints(&hcs, n), 0);
        }
        // fakeHTTPServer.Close does not close the fake listener, so the
        // 0.0.0.0:9376 entry from the first service is the same string as
        // nsn1's; upstream asserts the same count.
        assert_eq!(open_ports(&listener), 3);
        for n in [&nsn1, &nsn2, &nsn3] {
            test_handler(&hcs, n, 503, 0).await;
        }

        // sync endpoints
        hcs.sync_endpoints(&eps(&[(&nsn1, 9), (&nsn2, 3), (&nsn3, 7)]))
            .unwrap();
        assert_eq!(nservices(&hcs), 3);
        assert_eq!(endpoints(&hcs, &nsn1), 9);
        assert_eq!(endpoints(&hcs, &nsn2), 3);
        assert_eq!(endpoints(&hcs, &nsn3), 7);
        test_handler(&hcs, &nsn1, 200, 9).await;
        test_handler(&hcs, &nsn2, 200, 3).await;
        test_handler(&hcs, &nsn3, 200, 7).await;

        // sync new services
        hcs.sync_services(&svcs(&[
            // (&nsn1, 9376), // remove it
            (&nsn2, 12909), // leave it
            (&nsn3, 11114), // change it
            (&nsn4, 11878), // add it
        ]))
        .unwrap();
        assert_eq!(nservices(&hcs), 3);
        assert_eq!(endpoints(&hcs, &nsn2), 3);
        assert_eq!(endpoints(&hcs, &nsn3), 0);
        assert_eq!(endpoints(&hcs, &nsn4), 0);
        test_handler(&hcs, &nsn2, 200, 3).await;
        test_handler(&hcs, &nsn3, 503, 0).await;
        test_handler(&hcs, &nsn4, 503, 0).await;

        // sync endpoints
        hcs.sync_endpoints(&eps(&[(&nsn1, 9), (&nsn2, 3), (&nsn3, 7), (&nsn4, 6)]))
            .unwrap();
        assert_eq!(nservices(&hcs), 3);
        assert_eq!(endpoints(&hcs, &nsn2), 3);
        assert_eq!(endpoints(&hcs, &nsn3), 7);
        assert_eq!(endpoints(&hcs, &nsn4), 6);
        test_handler(&hcs, &nsn2, 200, 3).await;
        test_handler(&hcs, &nsn3, 200, 7).await;
        test_handler(&hcs, &nsn4, 200, 6).await;

        // sync endpoints, missing nsn2
        hcs.sync_endpoints(&eps(&[(&nsn3, 7), (&nsn4, 6)])).unwrap();
        assert_eq!(nservices(&hcs), 3);
        assert_eq!(endpoints(&hcs, &nsn2), 0);
        assert_eq!(endpoints(&hcs, &nsn3), 7);
        assert_eq!(endpoints(&hcs, &nsn4), 6);
        test_handler(&hcs, &nsn2, 503, 0).await;
        test_handler(&hcs, &nsn3, 200, 7).await;
        test_handler(&hcs, &nsn4, 200, 6).await;

        // fake a temporary unhealthy proxy
        *checker.0.lock().unwrap() = false;
        t_handler(&hcs, &nsn2, 503, 0, false).await;
        t_handler(&hcs, &nsn3, 503, 7, false).await;
        t_handler(&hcs, &nsn4, 503, 6, false).await;

        // fake a healthy proxy
        *checker.0.lock().unwrap() = true;
        t_handler(&hcs, &nsn2, 503, 0, true).await;
        t_handler(&hcs, &nsn3, 200, 7, true).await;
        t_handler(&hcs, &nsn4, 200, 6, true).await;
    }

    #[tokio::test]
    async fn test_server_with_selective_listening_address() {
        // limiting addresses to loop back (upstream resolves 127.0.0.0/8 to
        // 127.0.0.1 through NodePortAddresses.GetNodeIPs).
        let (hcs, listener, _) = setup(vec![IpAddr::from([127, 0, 0, 1])]);
        assert_eq!(nservices(&hcs), 0);

        hcs.sync_services(&HashMap::new()).unwrap();
        assert_eq!(nservices(&hcs), 0);
        hcs.sync_endpoints(&HashMap::new()).unwrap();
        assert_eq!(nservices(&hcs), 0);

        // sync unknown endpoints, should be dropped
        hcs.sync_endpoints(&eps(&[(&mknsn("a", "b"), 93)])).unwrap();
        assert_eq!(nservices(&hcs), 0);

        // sync a real service
        let nsn = mknsn("a", "b");
        hcs.sync_services(&svcs(&[(&nsn, 9376)])).unwrap();
        assert_eq!(nservices(&hcs), 1);
        assert_eq!(endpoints(&hcs, &nsn), 0);
        assert_eq!(open_ports(&listener), 1);
        assert!(listener
            .open_ports
            .lock()
            .unwrap()
            .contains("127.0.0.1:9376"));
        test_handler(&hcs, &nsn, 503, 0).await;
    }
}
