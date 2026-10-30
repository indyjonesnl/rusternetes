use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

use http_body_util::{BodyExt, Full};

/// Configuration for the kubectl proxy server
pub struct ProxyConfig {
    /// Address to bind to (default: 127.0.0.1)
    pub address: String,
    /// Port to bind to (default: 8001, 0 = random)
    pub port: u16,
    /// API server URL to proxy to
    pub api_server: String,
    /// Bearer token for authentication
    pub token: Option<String>,
    /// Skip TLS verification when connecting to API server
    pub skip_tls_verify: bool,
}

struct ProxyState {
    api_server: String,
    token: Option<String>,
    client: reqwest::Client,
}

/// Execute the proxy command: bind, print the address, and serve forever.
pub async fn execute(config: ProxyConfig) -> Result<()> {
    let listener = bind_listener(&config).await?;
    let addr = listener.local_addr()?;
    println!("Starting to serve on {}", addr);

    serve(listener, config).await
}

/// Bind a TCP listener according to the config. Exposed for testing.
pub async fn bind_listener(config: &ProxyConfig) -> Result<TcpListener> {
    let addr: SocketAddr = format!("{}:{}", config.address, config.port)
        .parse()
        .context("Invalid bind address")?;
    TcpListener::bind(addr)
        .await
        .context("Failed to bind proxy listener")
}

/// Accept connections on the listener and proxy them to the API server.
async fn serve(listener: TcpListener, config: ProxyConfig) -> Result<()> {
    let client = if config.skip_tls_verify {
        reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .context("Failed to build HTTP client")?
    } else {
        reqwest::Client::new()
    };

    let state = Arc::new(ProxyState {
        api_server: config.api_server.trim_end_matches('/').to_string(),
        token: config.token,
        client,
    });

    loop {
        let (stream, _remote_addr) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle_request(req, &state).await }
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("proxy connection error: {}", err);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: &ProxyState,
) -> Result<Response<Full<bytes::Bytes>>, hyper::Error> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

    let target_url = format!("{}{}", state.api_server, path_and_query);

    // Build the outgoing request
    let mut builder = state.client.request(method, &target_url);

    // Copy headers from the incoming request (skip host)
    for (key, value) in req.headers() {
        if key != hyper::header::HOST {
            builder = builder.header(key.clone(), value.clone());
        }
    }

    // Add authentication
    if let Some(ref token) = state.token {
        builder = builder.header("Authorization", format!("Bearer {}", token));
    }

    // Read the request body
    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            let error_body = format!("Failed to read request body: {}", e);
            let resp = Response::builder()
                .status(502)
                .body(Full::new(bytes::Bytes::from(error_body)))
                .unwrap();
            return Ok(resp);
        }
    };

    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes.to_vec());
    }

    // Send the request to the API server
    let upstream_response = match builder.send().await {
        Ok(resp) => resp,
        Err(e) => {
            let error_body = format!("Failed to proxy request to {}: {}", target_url, e);
            let resp = Response::builder()
                .status(502)
                .body(Full::new(bytes::Bytes::from(error_body)))
                .unwrap();
            return Ok(resp);
        }
    };

    // Build the response back to the client
    let status = upstream_response.status();
    let headers = upstream_response.headers().clone();
    let response_bytes = match upstream_response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            let error_body = format!("Failed to read upstream response: {}", e);
            let resp = Response::builder()
                .status(502)
                .body(Full::new(bytes::Bytes::from(error_body)))
                .unwrap();
            return Ok(resp);
        }
    };

    let mut response_builder = Response::builder().status(status);
    for (key, value) in headers.iter() {
        response_builder = response_builder.header(key, value);
    }

    let response = response_builder.body(Full::new(response_bytes)).unwrap();

    Ok(response)
}
