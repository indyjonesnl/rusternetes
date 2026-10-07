use crate::ratelimit::RateLimiter;
use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// List-level metadata (`metadata` on a `*List` envelope).
///
/// `resourceVersion` is what a reflector resumes its watch from after the
/// initial list.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMeta {
    pub resource_version: Option<String>,
    #[serde(rename = "continue")]
    pub continue_token: Option<String>,
}

/// Generic Kubernetes List wrapper
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesList<T> {
    #[allow(dead_code)]
    pub api_version: String,
    #[allow(dead_code)]
    pub kind: String,
    pub metadata: Option<ListMeta>,
    pub items: Vec<T>,
}

pub struct ApiClient {
    base_url: String,
    /// Client for normal CRUD requests. Has a total-request timeout so a
    /// stalled connection can't hang a caller forever (#1165: a controller's
    /// single worker would block on a hung GET/LIST/UPDATE and stop reconciling
    /// until process restart).
    client: Client,
    /// Client for long-lived watch streams (`get_stream`). No total timeout —
    /// a watch is meant to stay open indefinitely — but still bounded by
    /// connect-timeout + TCP keepalive so a dead connection is detected.
    stream_client: Client,
    token: Option<String>,
    /// Throttles every request EXCEPT watches (client-go `rest/request.go:763-764`).
    /// Unlimited unless the component opts in via [`Self::with_rate_limit`]. A
    /// limiter belongs to ONE client: [`Self::for_controller`] gives a clone its
    /// own fresh bucket rather than sharing this one (#1863).
    limiter: Arc<RateLimiter>,
}

#[derive(Debug)]
pub enum GetError {
    NotFound,
    Other(anyhow::Error),
}

impl std::fmt::Display for GetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetError::NotFound => write!(f, "Resource not found"),
            GetError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for GetError {}

/// Format a non-2xx HTTP response into a readable error.
///
/// The Kubernetes API server returns a `Status` object on failures, e.g.
/// `{"kind":"Status","status":"Failure","reason":"AlreadyExists","message":"...","code":409}`.
/// When the body parses as such, surface a clean upstream-`kubectl`-style
/// message: `Error from server (AlreadyExists): <message>`. Otherwise fall
/// back to the raw status + body text.
fn format_status_error(status: reqwest::StatusCode, body: &str) -> anyhow::Error {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        if v.get("kind").and_then(|k| k.as_str()) == Some("Status") {
            let reason = v.get("reason").and_then(|r| r.as_str()).unwrap_or("");
            let message = v.get("message").and_then(|m| m.as_str()).unwrap_or(body);
            if !reason.is_empty() {
                return anyhow::anyhow!("Error from server ({reason}): {message}");
            }
            return anyhow::anyhow!("Error from server: {message}");
        }
    }
    anyhow::anyhow!("request failed with status {status}: {body}")
}

impl ApiClient {
    /// Ergonomic constructor without a CA (system trust store / insecure).
    /// Used widely by tests; production wiring uses [`Self::with_tls`].
    #[allow(dead_code)]
    pub fn new(
        base_url: &str,
        insecure_skip_tls_verify: bool,
        token: Option<String>,
    ) -> Result<Self> {
        Self::with_tls(base_url, insecure_skip_tls_verify, None, None, None, token)
    }

    /// Build a client from a resolved [`crate::config::ClientConfig`]
    /// (kubeconfig- or in-cluster-sourced), reusing the [`Self::with_tls`]
    /// CA/token path.
    pub fn from_config(config: &crate::config::ClientConfig) -> Result<Self> {
        Self::with_tls(
            &config.base_url,
            config.insecure_skip_tls_verify,
            config.ca_pem.as_ref().map(|pem| pem.as_bytes().to_vec()),
            config
                .client_cert_pem
                .as_ref()
                .map(|pem| pem.as_bytes().to_vec()),
            config
                .client_key_pem
                .as_ref()
                .map(|pem| pem.as_bytes().to_vec()),
            config.token.clone(),
        )
    }

    /// Build a client, optionally trusting a kubeconfig-supplied CA certificate
    /// and presenting a client certificate for mTLS auth (#1578).
    ///
    /// TLS precedence mirrors upstream kubectl:
    ///   1. `insecure_skip_tls_verify` — accept any cert (skips CA entirely).
    ///   2. `ca_pem` present — verify against that CA (added to the default
    ///      roots), which is how we trust the api-server's self-signed cert.
    ///   3. neither — verify against the system roots only.
    ///
    /// When both `client_cert_pem` and `client_key_pem` are present, they are
    /// concatenated (cert then key) and installed as a rustls `Identity` so the
    /// client authenticates via mTLS. This mirrors upstream client-go, which
    /// builds the identity from the cert+key DATA pair
    /// (`transport.go`: `tls.X509KeyPair(c.TLS.CertData, c.TLS.KeyData)`).
    /// Supplying exactly one of the two is a misconfiguration and errors, per
    /// upstream's pair guard (`config.go` `HasCertAuth`: both cert AND key).
    pub fn with_tls(
        base_url: &str,
        insecure_skip_tls_verify: bool,
        ca_pem: Option<Vec<u8>>,
        client_cert_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        token: Option<String>,
    ) -> Result<Self> {
        // Resolve the optional client identity up front so a cert/key mismatch
        // fails fast (and only once, not per build_client call). A client cert
        // and its key must be supplied together.
        let identity = match (client_cert_pem.as_ref(), client_key_pem.as_ref()) {
            (Some(cert), Some(key)) => {
                // reqwest/rustls wants the cert PEM followed by the key PEM in a
                // single buffer (equivalent to Go's X509KeyPair(cert, key)).
                let mut combined = cert.clone();
                if !combined.ends_with(b"\n") {
                    combined.push(b'\n');
                }
                combined.extend_from_slice(key);
                let id = reqwest::Identity::from_pem(&combined)
                    .context("building client identity from client cert + key PEM (mTLS)")?;
                Some(id)
            }
            (Some(_), None) => {
                anyhow::bail!("client certificate supplied without a client key (mTLS misconfig)")
            }
            (None, Some(_)) => {
                anyhow::bail!("client key supplied without a client certificate (mTLS misconfig)")
            }
            (None, None) => None,
        };

        // Build a reqwest client with the shared TLS config. `total_timeout`
        // is applied only to the CRUD client — the stream client must stay open
        // for long-lived watches. Both get a connect-timeout and TCP keepalive
        // so a dead connection surfaces as an error instead of hanging forever
        // (#1165).
        let build_client = |total_timeout: Option<std::time::Duration>| -> Result<Client> {
            let mut builder = Client::builder()
                .connect_timeout(std::time::Duration::from_secs(15))
                .tcp_keepalive(std::time::Duration::from_secs(60));
            if insecure_skip_tls_verify {
                builder = builder.danger_accept_invalid_certs(true);
            } else if let Some(ref ca) = ca_pem {
                let cert = reqwest::Certificate::from_pem(ca)
                    .context("parsing kubeconfig CA certificate (certificate-authority-data)")?;
                builder = builder.add_root_certificate(cert);
            }
            if let Some(ref id) = identity {
                builder = builder.identity(id.clone());
            }
            if let Some(t) = total_timeout {
                builder = builder.timeout(t);
            }
            builder.build().context("Failed to build HTTP client")
        };

        let client = build_client(Some(std::time::Duration::from_secs(60)))?;
        let stream_client = build_client(None)?;

        Ok(Self {
            base_url: base_url.to_string(),
            client,
            stream_client,
            token,
            limiter: Arc::new(RateLimiter::unlimited()),
        })
    }

    /// Set this client's sustained request rate and burst, as a component's
    /// `--kube-api-qps` / `--kube-api-burst` do upstream. A non-positive `qps`
    /// disables throttling (`rest/config.go:370-381`: `if qps > 0`).
    ///
    /// Watches are never throttled regardless of this setting.
    pub fn with_rate_limit(mut self, qps: f64, burst: f64) -> Self {
        self.limiter = Arc::new(RateLimiter::new(qps, burst));
        self
    }

    /// This client's configured sustained QPS (`<= 0` means unthrottled).
    pub fn qps(&self) -> f64 {
        self.limiter.qps()
    }

    /// True when both clients draw from the same token bucket. Lets callers
    /// (and tests) assert the per-controller independence #1863 depends on.
    pub fn shares_limiter_with(&self, other: &ApiClient) -> bool {
        Arc::ptr_eq(&self.limiter, &other.limiter)
    }

    /// A client for one controller, cloned from this skeleton.
    ///
    /// Ported from upstream's `SimpleControllerClientBuilder.Config`
    /// (`staging/src/k8s.io/controller-manager/pkg/clientbuilder/client_builder.go:40-47`):
    /// `clientConfig := *b.ClientConfig` copies the skeleton, and
    /// `RESTClientFor` then builds a FRESH token bucket for every client that
    /// does not carry its own limiter (`client-go/rest/config.go:370-381`). So
    /// "QPS = 20" means 20 per controller, never 20 shared by all of them
    /// (#1856 shared one and was reverted in #1862).
    ///
    /// The HTTP connection pools (`reqwest::Client` is an `Arc` inside) are
    /// shared, as upstream shares the transport cache; only the limiter is new,
    /// with this skeleton's QPS/burst.
    pub fn for_controller(&self, _name: &str) -> Self {
        Self {
            base_url: self.base_url.clone(),
            client: self.client.clone(),
            stream_client: self.stream_client.clone(),
            token: self.token.clone(),
            limiter: Arc::new(RateLimiter::new(self.limiter.qps(), self.limiter.burst())),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, GetError> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self.client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| GetError::Other(anyhow::anyhow!("Failed to send GET request: {}", e)))?;

        let status = response.status();

        if status == StatusCode::NOT_FOUND {
            return Err(GetError::NotFound);
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GetError::Other(format_status_error(status, &body)));
        }

        response
            .json()
            .await
            .map_err(|e| GetError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))
    }

    /// Get a list of resources, automatically unwrapping the Kubernetes List wrapper
    #[allow(dead_code)]
    pub async fn get_list<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>, GetError> {
        let list: KubernetesList<T> = self.get(path).await?;
        Ok(list.items)
    }

    /// Get a streaming response for watch mode
    pub async fn get_stream(&self, path: &str) -> Result<reqwest::Response, GetError> {
        let url = format!("{}{}", self.base_url, path);
        // Use the stream client (no total-request timeout) — a watch is a
        // long-lived stream and must not be killed by the CRUD timeout (#1165).
        let mut request = self.stream_client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| GetError::Other(anyhow::anyhow!("Failed to send GET request: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GetError::Other(anyhow::anyhow!(
                "Watch request failed with status {}: {}",
                status,
                body
            )));
        }

        Ok(response)
    }

    pub async fn post<T: Serialize, R: DeserializeOwned>(&self, path: &str, body: &T) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self.client.post(&url).json(body);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .context("Failed to send POST request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        response.json().await.context("Failed to parse response")
    }

    pub async fn put<T: Serialize, R: DeserializeOwned>(&self, path: &str, body: &T) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self.client.put(&url).json(body);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send PUT request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        response.json().await.context("Failed to parse response")
    }

    /// DELETE with query parameters and optional JSON body (for DeleteOptions).
    /// Returns the response status code (useful for checking 404 on wait-polling).
    pub async fn delete_with_options(
        &self,
        path: &str,
        query_params: &[(String, String)],
        body: Option<&serde_json::Value>,
    ) -> Result<StatusCode> {
        let mut url = format!("{}{}", self.base_url, path);

        if !query_params.is_empty() {
            let qs: Vec<String> = query_params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            let separator = if url.contains('?') { "&" } else { "?" };
            url.push_str(&format!("{}{}", separator, qs.join("&")));
        }

        self.limiter.acquire().await;
        let mut request = self.client.delete(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        if let Some(b) = body {
            request = request.header("Content-Type", "application/json").json(b);
        }

        let response = request
            .send()
            .await
            .context("Failed to send DELETE request")?;

        let status = response.status();

        if !status.is_success() && status != StatusCode::NOT_FOUND {
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        Ok(status)
    }

    /// Check if a resource exists (GET returns 200). Returns false on 404.
    pub async fn resource_exists(&self, path: &str) -> Result<bool> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self.client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send GET request")?;

        Ok(response.status().is_success())
    }

    pub async fn patch<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
        content_type: &str,
    ) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self
            .client
            .patch(&url)
            .header("Content-Type", content_type)
            .json(body);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .context("Failed to send PATCH request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        response.json().await.context("Failed to parse response")
    }

    /// Get a resource as plain text (for logs, etc.)
    pub async fn get_text(&self, path: &str) -> Result<String> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self.client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send GET request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Request failed with status {}: {}", status, body);
        }

        response
            .text()
            .await
            .context("Failed to read response text")
    }

    /// Convert HTTP(S) base URL to WebSocket URL for streaming endpoints
    pub fn get_ws_url(&self, path: &str) -> Result<String> {
        let ws_base = if self.base_url.starts_with("https://") {
            self.base_url.replace("https://", "wss://")
        } else if self.base_url.starts_with("http://") {
            self.base_url.replace("http://", "ws://")
        } else {
            anyhow::bail!("Invalid base URL: {}", self.base_url);
        };

        let mut url = format!("{}{}", ws_base, path);

        // Add token as query parameter if present (WebSocket doesn't support headers)
        if let Some(ref token) = self.token {
            let separator = if url.contains('?') { "&" } else { "?" };
            url.push_str(&format!("{}token={}", separator, token));
        }

        Ok(url)
    }

    pub fn get_base_url(&self) -> &str {
        &self.base_url
    }

    pub fn get_token(&self) -> Option<&String> {
        self.token.as_ref()
    }

    /// GET a path with a custom `Accept` header, returning the parsed JSON.
    /// Used for aggregated API discovery (`APIGroupDiscoveryList`).
    pub async fn get_raw_with_accept(
        &self,
        path: &str,
        accept: &str,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        self.limiter.acquire().await;
        let mut request = self.client.get(&url).header("Accept", accept);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send GET request")?;

        let status = response.status();
        let text = response
            .text()
            .await
            .context("Failed to read response body")?;

        if !status.is_success() {
            anyhow::bail!("discovery request to {url} failed: {status}: {text}");
        }

        serde_json::from_str(&text)
            .with_context(|| format!("failed to parse discovery response from {url} as JSON"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_status_error_already_exists() {
        let body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","message":"configmaps \"f5-cm\" already exists","code":409}"#;
        let err = format_status_error(StatusCode::CONFLICT, body);
        assert_eq!(
            err.to_string(),
            "Error from server (AlreadyExists): configmaps \"f5-cm\" already exists"
        );
    }

    #[test]
    fn test_format_status_error_not_found() {
        let body = r#"{"kind":"Status","status":"Failure","reason":"NotFound","message":"pods \"x\" not found","code":404}"#;
        let err = format_status_error(StatusCode::NOT_FOUND, body);
        assert_eq!(
            err.to_string(),
            "Error from server (NotFound): pods \"x\" not found"
        );
    }

    #[test]
    fn test_format_status_error_no_reason() {
        // A Status object without a reason field still surfaces the message.
        let body = r#"{"kind":"Status","status":"Failure","message":"something bad","code":500}"#;
        let err = format_status_error(StatusCode::INTERNAL_SERVER_ERROR, body);
        assert_eq!(err.to_string(), "Error from server: something bad");
    }

    #[test]
    fn test_format_status_error_non_status_body_falls_back() {
        let body = "plain text gateway error";
        let err = format_status_error(StatusCode::BAD_GATEWAY, body);
        assert_eq!(
            err.to_string(),
            "request failed with status 502 Bad Gateway: plain text gateway error"
        );
    }

    #[test]
    fn test_format_status_error_non_json_body_falls_back() {
        let body = "<html>503</html>";
        let err = format_status_error(StatusCode::SERVICE_UNAVAILABLE, body);
        assert_eq!(
            err.to_string(),
            "request failed with status 503 Service Unavailable: <html>503</html>"
        );
    }

    // Throwaway self-signed client cert + key (RSA-2048, CN=test-client),
    // generated once with `openssl req -x509 -newkey rsa:2048 -nodes` for the
    // mTLS identity test (#1578). Not used by any live endpoint.
    const TEST_CLIENT_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDDTCCAfWgAwIBAgIUbg0wEZz4PWxhG6150TKdtY3RToUwDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLdGVzdC1jbGllbnQwHhcNMjYwNzA2MjIxNzU5WhcNMzYw
NzAzMjIxNzU5WjAWMRQwEgYDVQQDDAt0ZXN0LWNsaWVudDCCASIwDQYJKoZIhvcN
AQEBBQADggEPADCCAQoCggEBALx2Re8hobRJCZXb7KE5QWGTezVNKWz2Fc2dQhl5
ep/8KwjxpRTCLFulTyKdRompRs4PJwsszG4yxujW2ORdiXENmn4Hzph5r3BIU4kc
N3yOd7ATSe2wlsQPIf2QYVR4gSOvlDRkAugUt4QnUqVVn2gkRLa4pYfqJvcusEtP
T9xqxk6bJExwkU2n2lo/AWq5zl6WVPOUORh7/eXJjWtyzln5cGVulu0w56VZHDID
DKdSA6S9D04/jBEsL7W20AYYxi+rEttb6+TiJH0GrxEuZAK+Blwid5x8XcRyQ6Tz
TsOOBouR/jshSSVIXfVWEPqkD2+EWrFqtnUBgV5mblpSH1kCAwEAAaNTMFEwHQYD
VR0OBBYEFJlz3rKIHbaw0y3IrRu0jFbrku0YMB8GA1UdIwQYMBaAFJlz3rKIHbaw
0y3IrRu0jFbrku0YMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB
AJoTqX4QPacZo4DE5eLEvM5RTYfXfk7PHGNveeCOUPT/EGMWXYD/6UrIZCALkhzx
2IHmrHPhAZnGGzY6/erbztorscz1Wew94+dKgQBJH5T6oaIkli777AfK7Lpq+kiw
XTsO4gVnylljtfBvudQAXZ6dsg3zRrYO++H2YgcsWLimi7cCKP6HZHnKCKD1etx/
4MVDaXxd6gaS+O3g9rYPIcbGLsF9p4hRWFjqjKJM7kmRp4WXzdt7C/aXI+wYVhCF
NoU2uWEbSHUCaOYCC+3NNmWCXS8JKT2zvfijPiW/eLgWM9gVYVX5YQtkZaq1fTBC
Ue8KW7NRFZVYv2Rw1Pg1g0g=
-----END CERTIFICATE-----
";
    const TEST_CLIENT_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC8dkXvIaG0SQmV
2+yhOUFhk3s1TSls9hXNnUIZeXqf/CsI8aUUwixbpU8inUaJqUbODycLLMxuMsbo
1tjkXYlxDZp+B86Yea9wSFOJHDd8jnewE0ntsJbEDyH9kGFUeIEjr5Q0ZALoFLeE
J1KlVZ9oJES2uKWH6ib3LrBLT0/casZOmyRMcJFNp9paPwFquc5ellTzlDkYe/3l
yY1rcs5Z+XBlbpbtMOelWRwyAwynUgOkvQ9OP4wRLC+1ttAGGMYvqxLbW+vk4iR9
Bq8RLmQCvgZcInecfF3EckOk807DjgaLkf47IUklSF31VhD6pA9vhFqxarZ1AYFe
Zm5aUh9ZAgMBAAECggEAK4ILgZoMTIRrCdV4krzW1vm3ATZj2KOUI4CJTLvCfzI2
Vi2BLKJqHqsycn2IFgpGDhap7xbDyDIBQSoubsQYUYjwMGXJgGJhSeTsohPpTGBQ
ic3eLJkuqSsMMA9XpOpf99bWOmUXVbBIsKHqXsB+WUq8MUm97ztzjO+SpAQ2nd4D
ZaV7s77gNQtiTnERTyeML50UblxTbkm3LDxxcFFlFqn5wXWHxNJWrfyN12Du5Lpc
wzD8KkJsjV597bIPMuyLTBZhZGM3n+/SI9B18sZdHViUaoxQMXfFmrkKHlkjjD3I
IJBUz6c6jnplWPi7qFP4nc9iL2QKVEFVmk9fodAtXQKBgQD4RPlAlJGWfA32gP0H
KfSvo9yq+J1WUVO/MgHXVGa8nikAmMIDEbTeZpbjJbwyCakG/qtuiROePUEvbvRM
HaQkKxMKajEivdwdvhcLmVdLq4rHEGak5mPix5sKvkKHUVvX44as4RXVKwVaK9gj
+QeemIxRJSMh3KgGNSJSIWBjhQKBgQDCVI65wZlPqr8P7ujNpKHANs00yGgKKYrF
xPkyLMaP7skZURMtrq+qB3JMZ+qvOJP+QmvF+3aPKHA5/hzZr5C/F+Az4WKODBVG
REDy6MG/bNNFVY984NhqKRbPvCCcg0jAfsJlyZmCFes34OQtYOmcat10oLHCloT1
0XrMMuyCxQKBgQDC2A7uOitQeSfUMENkne7k8as7m0aP+d/KDAsZ3amLmmz/hOOu
2PSkHsuIlZLvillXngMZCweUhupjuaaNHi42HIAjClhptavMw+T+O2ghgQ23UQ3d
mNsHnjP16H/6B0YXVv/ZKgWieNMIg6RsBwON2pc1D/pUlwJfbM/0uTEWqQKBgQDC
CKX96cWHm2hso1LGSlTLVKyuwE/JndMXR2a+Z6DXhEg9RAuPOHXjos3IZpYY4Lg8
TtvHch7eMDVmYkkyPi+b7l4Jz0iVppDzeSEUqb0Swrls6FJ+EQ9laKODRkeVnyxc
L/UwpwvkrLgRMjcC7Fo1uSpn0i/LqHkX7VLcYxhuNQKBgQDsVFcFNZVKSQIkR0p9
lq2scfEWl+7k3MBexUAC3xDPw2ItVOeJR1PXpJqqRUICTp8jc4MBRjzPR27ZKfBb
o2Wx1kwClW9YuBRA6HmLA3xFZqtKWCLy+LLDEMjlnGxuANyz6OeYCo9VPIiDCBHk
pYMXass1aOZuRtmE5ibX9iPpBQ==
-----END PRIVATE KEY-----
";

    // #1578: a client cert + key pair yields a usable client (reqwest parses the
    // combined cert+key PEM into a rustls Identity). Mirrors upstream client-go
    // `tls.X509KeyPair(c.TLS.CertData, c.TLS.KeyData)` (transport.go:137).
    #[test]
    fn test_with_tls_client_identity_ok() {
        let client = ApiClient::with_tls(
            "https://localhost:6443",
            false,
            None,
            Some(TEST_CLIENT_CERT_PEM.as_bytes().to_vec()),
            Some(TEST_CLIENT_KEY_PEM.as_bytes().to_vec()),
            None,
        );
        assert!(
            client.is_ok(),
            "cert+key pair must build a client: {:?}",
            client.err()
        );
    }

    // #1578: cert without key (or vice-versa) is a misconfig. Mirrors upstream's
    // pair guard (config.go `HasCertAuth`: both cert AND key must be present).
    #[test]
    fn test_with_tls_cert_without_key_errs() {
        let cert_only = ApiClient::with_tls(
            "https://localhost:6443",
            false,
            None,
            Some(TEST_CLIENT_CERT_PEM.as_bytes().to_vec()),
            None,
            None,
        );
        assert!(cert_only.is_err(), "cert without key must error");

        let key_only = ApiClient::with_tls(
            "https://localhost:6443",
            false,
            None,
            None,
            Some(TEST_CLIENT_KEY_PEM.as_bytes().to_vec()),
            None,
        );
        assert!(key_only.is_err(), "key without cert must error");
    }

    // ---- per-controller rate limiting (#1863) -------------------------------

    /// Minimal HTTP/1.1 server answering every request with `{}`; returns its
    /// base URL. Real time: the limiter's sleeps are tokio timers.
    async fn spawn_ok_server() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {
                                let resp = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}";
                                if sock.write_all(resp.as_bytes()).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    /// The whole point of #1863: two clients cloned from one skeleton must not
    /// share tokens. Upstream clones the config per controller
    /// (controller-manager clientbuilder/client_builder.go:40-47) and
    /// `RESTClientFor` builds a fresh limiter for each (rest/config.go:370-381).
    #[tokio::test(start_paused = true)]
    async fn for_controller_clients_do_not_share_tokens() {
        let skeleton = ApiClient::new("http://127.0.0.1:1", true, None)
            .unwrap()
            .with_rate_limit(10.0, 1.0);
        let a = skeleton.for_controller("a");
        let b = skeleton.for_controller("b");

        assert!(!Arc::ptr_eq(&a.limiter, &b.limiter));
        assert!(!Arc::ptr_eq(&a.limiter, &skeleton.limiter));
        assert_eq!(a.limiter.qps(), 10.0, "clone keeps the skeleton's QPS");
        assert_eq!(a.limiter.burst(), 1.0, "clone keeps the skeleton's burst");

        // Exhaust A's burst and queue a lot more behind it.
        for _ in 0..50 {
            a.limiter.acquire().await;
        }
        // B still has its full burst: no time passes.
        let start = tokio::time::Instant::now();
        b.limiter.acquire().await;
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);
    }

    /// An `ApiClient` that never opted in must behave exactly as before the
    /// limiter existed, so kubelet/scheduler/kubectl are untouched.
    #[tokio::test(start_paused = true)]
    async fn default_client_is_unthrottled() {
        let c = ApiClient::new("http://127.0.0.1:1", true, None).unwrap();
        let start = tokio::time::Instant::now();
        for _ in 0..1000 {
            c.limiter.acquire().await;
        }
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);
        let d = c.for_controller("x");
        for _ in 0..1000 {
            d.limiter.acquire().await;
        }
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);
    }

    /// End to end over real HTTP: the limiter is wired into the request path,
    /// one client's wait does not delay its sibling, and watches are exempt
    /// (client-go rest/request.go:763-764).
    #[tokio::test]
    async fn requests_are_throttled_per_client_and_watches_are_exempt() {
        use std::time::{Duration, Instant};
        let base = spawn_ok_server().await;
        // 2 QPS, burst 1: the second request on one client waits ~500ms.
        let skeleton = ApiClient::new(&base, true, None)
            .unwrap()
            .with_rate_limit(2.0, 1.0);
        let a = skeleton.for_controller("a");
        let b = skeleton.for_controller("b");

        let _: serde_json::Value = a.get("/x").await.unwrap(); // spends A's burst

        let t = Instant::now();
        let _: serde_json::Value = b.get("/x").await.unwrap(); // B's own burst
        assert!(
            t.elapsed() < Duration::from_millis(300),
            "B must not wait on A's exhausted bucket: {:?}",
            t.elapsed()
        );

        // Watches take no token even from the exhausted client A.
        let t = Instant::now();
        for _ in 0..3 {
            a.get_stream("/x").await.unwrap();
        }
        assert!(
            t.elapsed() < Duration::from_millis(300),
            "watch streams must not be throttled: {:?}",
            t.elapsed()
        );

        // A's next CRUD request has to wait for a token.
        let t = Instant::now();
        let _: serde_json::Value = a.get("/x").await.unwrap();
        assert!(
            t.elapsed() >= Duration::from_millis(400),
            "A's second request must be throttled: {:?}",
            t.elapsed()
        );
    }
}
