//! The webhook transport the api-server installs: egress-selector aware, as
//! upstream's webhook client is.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/util/webhook/authentication.go`
//! (`NewDefaultAuthenticationInfoResolverWrapper`): with an egress selector the
//! dial of a `clientConfig.service` is `Lookup(Cluster)` and of a
//! `clientConfig.url` is `Lookup(ControlPlane)`; with none, the default dial.
//! The TLS side follows `rest.Config`: the `caBundle` alone is the root pool
//! (`rootCertPool`), the server name of a service is `<name>.<namespace>.svc`,
//! and the connection is HTTP/1.1 (client.go:182-184, issue 75791).

use crate::egress_http::{tls_client_config, EgressHttpClient};
use crate::egress_selector::{Dialer, EgressSelector, EgressType};
use rusternetes_admission_webhook::transport::{
    Dial, TransportFuture, WebhookHttpError, WebhookHttpRequest, WebhookHttpResponse,
    WebhookTransport,
};

/// The `egressSelector` the wrapper is built with
/// (`NewDefaultAuthenticationInfoResolverWrapper`'s second argument).
pub struct EgressWebhookTransport {
    egress_selector: Option<std::sync::Arc<EgressSelector>>,
}

impl EgressWebhookTransport {
    pub fn new(egress_selector: Option<std::sync::Arc<EgressSelector>>) -> Self {
        Self { egress_selector }
    }

    /// `Lookup(Cluster)` for a service, `Lookup(ControlPlane)` for a URL; no
    /// selector or selection leaves the default dial.
    fn dialer(&self, service: bool) -> Dialer {
        let context = if service {
            EgressType::Cluster
        } else {
            EgressType::ControlPlane
        };
        self.egress_selector
            .as_ref()
            .and_then(|s| s.lookup(context))
            .unwrap_or(Dialer::Direct)
    }
}

impl WebhookTransport for EgressWebhookTransport {
    fn post_json<'a>(&'a self, req: WebhookHttpRequest<'a>) -> TransportFuture<'a> {
        Box::pin(async move {
            let target = req.target;
            let dial_override = match (&target.dial, &target.service) {
                (Dial::Failed(e), _) => {
                    return Err(WebhookHttpError {
                        message: format!("Post \"{}\": {e}", target.url),
                        connect: true,
                        ..Default::default()
                    });
                }
                (Dial::Resolved(to), Some(svc)) => Some((svc.host_port(), to.clone())),
                _ => None,
            };
            let tls =
                tls_client_config(req.ca_bundle, false, None).map_err(|e| WebhookHttpError {
                    message: format!("Post \"{}\": {e}", target.url),
                    ..Default::default()
                })?;
            let client = EgressHttpClient::with_dial_override(
                self.dialer(target.service.is_some()),
                tls,
                req.timeout,
                dial_override,
            );
            match client.post_json(&target.url, &[], req.body).await {
                Ok(r) => Ok(WebhookHttpResponse {
                    status: r.status,
                    body: r.body.to_vec(),
                }),
                Err(e) if e.timeout => Err(WebhookHttpError {
                    message: format!("Post \"{}\"", target.url),
                    timeout: true,
                    ..Default::default()
                }),
                Err(e) => Err(WebhookHttpError {
                    message: e.message,
                    connect: true,
                    ..Default::default()
                }),
            }
        })
    }
}
