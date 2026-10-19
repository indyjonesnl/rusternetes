//! `k8s.io/apimachinery/pkg/util/wait` backoff, shared by the webhook-style
//! delegating clients (api-server authorization webhook, kubelet BuildAuth).

use std::time::Duration;

/// `wait.Backoff`.
#[derive(Debug, Clone)]
pub struct Backoff {
    pub duration: Duration,
    pub factor: f64,
    pub jitter: f64,
    pub steps: u32,
}

impl Backoff {
    /// `DefaultAuthWebhookRetryBackoff`
    /// (`apiserver/pkg/server/options/authentication.go:46`) ==
    /// `DefaultRetryBackoffWithInitialDelay(500ms)`.
    pub fn default_auth_webhook() -> Self {
        Self {
            duration: Duration::from_millis(500),
            factor: 1.5,
            jitter: 0.2,
            steps: 5,
        }
    }

    /// `Backoff.Step`: the next sleep, then grow.
    pub fn step(&mut self) -> Duration {
        self.steps = self.steps.saturating_sub(1);
        let mut d = self.duration;
        if self.factor != 0.0 {
            self.duration = self.duration.mul_f64(self.factor);
        }
        if self.jitter > 0.0 {
            // wait.Jitter: d + rand*jitter*d
            d += d.mul_f64(rand::random::<f64>() * self.jitter);
        }
        d
    }
}
