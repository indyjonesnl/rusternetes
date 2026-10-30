//! Static pod manifest-URL source (`staticPodURL`).
//!
//! Port of `sourceURL` in `pkg/kubelet/config/http.go`: `NewSourceURL` polls
//! the URL every `httpCheckFrequency` (`wait.Until(..., period, ...)`,
//! kubelet.go:390), `extractFromURL` (http.go:80-141) fetches it with a 10s
//! client timeout, short-circuits unchanged bodies and decodes a single Pod or
//! a PodList; `run` (http.go:63-78) rate-limits the failure log.

use crate::static_pods::{parse_http_manifest, MAX_CONFIG_LENGTH};
use anyhow::{anyhow, Result};
use rusternetes_common::resources::Pod;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, info};

/// `sourceURL` state.
pub struct UrlSource {
    url: String,
    headers: reqwest::header::HeaderMap,
    node_name: String,
    client: reqwest::Client,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// `sourceURL.data`: the last body read, for the unchanged short-circuit.
    data: Vec<u8>,
    /// `sourceURL.failureLogs`.
    failure_logs: u32,
}

/// What one `extractFromURL` pass produced. Upstream sends a
/// `kubetypes.PodUpdate{Op: SET}` on `updates` and returns an error
/// separately; both can happen in one pass (zero-length body).
#[derive(Debug, Default)]
pub struct PollResult {
    /// `Some(pods)` = a SET update for the HTTP source; `None` = nothing sent.
    pub update: Option<Vec<Pod>>,
    pub error: Option<anyhow::Error>,
}

impl UrlSource {
    /// `NewSourceURL` (minus the goroutine; the caller owns the period).
    /// `header` mirrors `makePodSourceConfig`'s `manifestURLHeader`
    /// (kubelet.go:366-373): every value of every key is `Add`ed.
    pub fn new(
        url: &str,
        header: &std::collections::BTreeMap<String, Vec<String>>,
        node_name: &str,
    ) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (k, vs) in header {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| anyhow!("staticPodURLHeader key {k:?}: {e}"))?;
            for v in vs {
                let value = reqwest::header::HeaderValue::from_str(v)
                    .map_err(|e| anyhow!("staticPodURLHeader {k:?} value: {e}"))?;
                headers.append(name.clone(), value);
            }
        }
        Ok(Self {
            url: url.to_string(),
            headers,
            node_name: node_name.to_string(),
            // "Timing out requests leads to retries." (http.go:49-51)
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            state: Mutex::new(State::default()),
        })
    }

    /// `sourceURL.run` (http.go:63-78): one fetch, with the failure-log
    /// bookkeeping. Returns the update to publish, if any.
    pub async fn run(&self) -> Option<Vec<Pod>> {
        let PollResult { update, error } = self.extract_from_url().await;
        let mut st = self.state.lock().unwrap();
        match error {
            Some(err) => {
                // "Don't log this multiple times per minute."
                if st.failure_logs < 3 {
                    info!(error = %format!("{err:#}"), "Failed to read pods from URL");
                } else if st.failure_logs == 3 {
                    info!(error = %format!("{err:#}"), "Failed to read pods from URL. Dropping verbosity of this message to debug");
                } else {
                    debug!(error = %format!("{err:#}"), "Failed to read pods from URL");
                }
                st.failure_logs += 1;
            }
            None => {
                if st.failure_logs > 0 {
                    info!("Successfully read pods from URL");
                    st.failure_logs = 0;
                }
            }
        }
        update
    }

    /// `sourceURL.extractFromURL` (http.go:80-141).
    pub async fn extract_from_url(&self) -> PollResult {
        let resp = match self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return PollResult {
                    update: None,
                    error: Some(e.into()),
                }
            }
        };
        let status = resp.status();
        // `utilio.ReadAtMost(resp.Body, maxConfigLength)`: error when the body
        // is larger than the limit.
        let data = match read_at_most(resp, MAX_CONFIG_LENGTH).await {
            Ok(d) => d,
            Err(e) => {
                return PollResult {
                    update: None,
                    error: Some(e),
                }
            }
        };
        if !status.is_success() || status.as_u16() != 200 {
            return PollResult {
                update: None,
                error: Some(anyhow!("{}: {}", self.url, status)),
            };
        }
        if data.is_empty() {
            // "Emit an update with an empty PodList to allow HTTPSource to be
            // marked as seen"
            return PollResult {
                update: Some(Vec::new()),
                error: Some(anyhow!("zero-length data received from {}", self.url)),
            };
        }
        {
            // "Short circuit if the data has not changed since the last time
            // it was read."
            let mut st = self.state.lock().unwrap();
            if st.data == data {
                return PollResult::default();
            }
            st.data = data.clone();
        }
        match parse_http_manifest(&data, &self.url, &self.node_name) {
            Ok(pods) => PollResult {
                update: Some(pods),
                error: None,
            },
            Err(e) => PollResult {
                update: None,
                error: Some(e),
            },
        }
    }
}

async fn read_at_most(mut resp: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        out.extend_from_slice(&chunk);
        if out.len() > limit {
            return Err(anyhow!("limit reached on read"));
        }
    }
    Ok(out)
}
