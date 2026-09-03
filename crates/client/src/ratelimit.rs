//! Client-side request throttling.
//!
//! Ported from client-go, where every Kubernetes client is rate limited by
//! construction: `rest.Config` defaults to `DefaultQPS = 5.0` /
//! `DefaultBurst = 10` when unset
//! (`staging/src/k8s.io/client-go/rest/config.go:47-48, 374-378`), and each
//! component raises it — controller-manager 20/30, kubelet 50/100,
//! kube-proxy 5/10.
//!
//! This limiter is the governor that upstream's concurrent controller paths
//! are written against, not a standalone safety net. Two examples of the
//! pairing, both of which we port:
//!
//! - `slowStartBatch` with `burstReplicas = 500` (`replica_set.go`) means "at
//!   most 500 pods per sync". Without a limiter underneath, that becomes "up
//!   to 500 requests as fast as the runtime can issue them".
//! - `orphanDependents` (`garbagecollector.go:673-696`) starts **one goroutine
//!   per dependent**, unbounded, and `wg.Wait()`s. The e2e test that exercises
//!   it says so explicitly
//!   (`test/e2e/apimachinery/garbage_collector.go:410-416`):
//!
//! ```text
//! // Orphaning the 100 pods takes 100 PATCH operations. The default qps of
//! // a client is 5. If the qps is saturated, it will take 20s to orphan
//! // the pods.
//! ```
//!
//! That "20s" is the limiter doing its job: the fan-out is unbounded and the
//! limiter is the only thing metering it. Port one without the other and you
//! get either a serial controller that misses its deadline, or a burst that
//! buries the api-server.
//!
//! Watches are deliberately NOT throttled, matching client-go:
//!
//! ```text
//! // We specifically don't want to rate limit watches, so we
//! // don't use r.rateLimiter here.
//! ```
//!
//! (`client-go/rest/request.go:763-764`). A watch is one long-lived request;
//! throttling it would delay establishing the stream a controller depends on,
//! and it costs nothing per event afterwards.

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// client-go's `DefaultQPS` for a client whose QPS is unset
/// (`rest/config.go:47`).
pub const DEFAULT_QPS: f64 = 5.0;

/// client-go's `DefaultBurst` (`rest/config.go:48`).
pub const DEFAULT_BURST: f64 = 10.0;

/// Upstream kube-controller-manager's `ClientConnection.QPS` / `.Burst`
/// (`pkg/controller/apis/config/v1alpha1/defaults.go:59, 62`).
pub const CONTROLLER_MANAGER_QPS: f64 = 20.0;
pub const CONTROLLER_MANAGER_BURST: f64 = 30.0;

/// Upstream kubelet's `KubeAPIQPS` / `KubeAPIBurst`
/// (`pkg/kubelet/apis/config/v1beta1/defaults.go:221, 224`).
pub const KUBELET_QPS: f64 = 50.0;
pub const KUBELET_BURST: f64 = 100.0;

/// Upstream kube-proxy's `ClientConnection.QPS` / `.Burst`
/// (`pkg/proxy/apis/config/v1alpha1/defaults.go:130, 133`).
pub const KUBE_PROXY_QPS: f64 = 5.0;
pub const KUBE_PROXY_BURST: f64 = 10.0;

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// A token bucket: `qps` tokens accrue per second, capped at `burst`.
///
/// Same shape as client-go's `flowcontrol.NewTokenBucketRateLimiter`, which
/// wraps `golang.org/x/time/rate`. A burst of saved-up tokens is spent
/// immediately; past that, callers wait for the next token.
pub struct RateLimiter {
    qps: f64,
    burst: f64,
    bucket: Mutex<Bucket>,
}

impl RateLimiter {
    /// A limiter allowing `qps` sustained requests per second with `burst`
    /// available immediately. A non-positive `qps` disables throttling.
    pub fn new(qps: f64, burst: f64) -> Self {
        Self {
            qps,
            burst: burst.max(1.0),
            bucket: Mutex::new(Bucket {
                // Start full, as a token bucket does: a client that has been
                // idle may spend its burst at once.
                tokens: burst.max(1.0),
                last_refill: Instant::now(),
            }),
        }
    }

    /// Take one token, waiting if none is available.
    ///
    /// The wait is computed under the lock and slept outside it, so a queue of
    /// callers does not serialise on the mutex while sleeping.
    pub async fn acquire(&self) {
        if self.qps <= 0.0 {
            return;
        }

        let wait = {
            let mut bucket = self.bucket.lock().unwrap();
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(bucket.last_refill);
            bucket.tokens = (bucket.tokens + elapsed.as_secs_f64() * self.qps).min(self.burst);
            bucket.last_refill = now;

            if bucket.tokens >= 1.0 {
                bucket.tokens -= 1.0;
                Duration::ZERO
            } else {
                // Reserve the token now and wait for it to accrue, so
                // concurrent callers queue in order instead of all sleeping
                // for the same slot and thundering.
                let deficit = 1.0 - bucket.tokens;
                bucket.tokens -= 1.0;
                Duration::from_secs_f64(deficit / self.qps)
            }
        };

        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Burst is spendable immediately — an idle client must not be punished
    /// for its first few requests.
    #[tokio::test(start_paused = true)]
    async fn burst_is_available_without_waiting() {
        let limiter = RateLimiter::new(10.0, 5.0);
        let start = Instant::now();

        for _ in 0..5 {
            limiter.acquire().await;
        }

        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the full burst must be spendable immediately"
        );
    }

    /// Past the burst, callers wait at the sustained rate.
    #[tokio::test(start_paused = true)]
    async fn requests_past_the_burst_wait_for_tokens() {
        let limiter = RateLimiter::new(10.0, 1.0);
        let start = Instant::now();

        // 1 from burst, then 4 at 10/s = 100ms each.
        for _ in 0..5 {
            limiter.acquire().await;
        }

        assert_eq!(
            start.elapsed(),
            Duration::from_millis(400),
            "four requests past a burst of 1 must take 4 x 100ms at 10 QPS"
        );
    }

    /// Tokens accrue while idle, back up to the burst ceiling and no further —
    /// otherwise a long-idle client could later flood.
    #[tokio::test(start_paused = true)]
    async fn tokens_refill_while_idle_but_are_capped_at_burst() {
        let limiter = RateLimiter::new(10.0, 3.0);

        // Spend the burst.
        for _ in 0..3 {
            limiter.acquire().await;
        }

        // Idle for far longer than it takes to refill 3 tokens.
        tokio::time::sleep(Duration::from_secs(10)).await;

        let start = Instant::now();
        for _ in 0..3 {
            limiter.acquire().await;
        }
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "an idle client should have refilled its burst"
        );

        // The fourth must wait: the bucket refilled to `burst`, not beyond.
        limiter.acquire().await;
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(100),
            "refill must cap at burst rather than accumulating 10s of tokens"
        );
    }

    /// A non-positive QPS disables throttling, so a caller can opt out
    /// explicitly rather than by passing an enormous number.
    #[tokio::test(start_paused = true)]
    async fn non_positive_qps_disables_throttling() {
        let limiter = RateLimiter::new(0.0, 0.0);
        let start = Instant::now();

        for _ in 0..1000 {
            limiter.acquire().await;
        }

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    /// Concurrent callers must not all sleep for the same slot: 10 requests at
    /// 10 QPS with a burst of 1 take 900ms in total, not 100ms.
    #[tokio::test(start_paused = true)]
    async fn concurrent_callers_queue_rather_than_thunder() {
        let limiter = std::sync::Arc::new(RateLimiter::new(10.0, 1.0));
        let start = Instant::now();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let limiter = std::sync::Arc::clone(&limiter);
            handles.push(tokio::spawn(async move { limiter.acquire().await }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(
            start.elapsed(),
            Duration::from_millis(900),
            "9 requests past the burst must serialise at 10 QPS"
        );
    }
}
