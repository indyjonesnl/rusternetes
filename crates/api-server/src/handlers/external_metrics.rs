use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    Extension, Json,
};
use chrono::Utc;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{ExternalMetricValue, ExternalMetricValueList},
    Result,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// TypeMeta for the external-metrics group; see the custom-metrics twin (#1916).
fn external_metric_type_meta(kind: &str) -> rusternetes_common::types::TypeMeta {
    rusternetes_common::types::TypeMeta {
        api_version: "external.metrics.k8s.io/v1beta1".to_string(),
        kind: kind.to_string(),
    }
}
use tracing::{info, warn};

/// GET /apis/external.metrics.k8s.io/v1beta1/namespaces/{namespace}/{metric}
///
/// Lists values for a global (external) metric. The HPA controller's `External`
/// metric path queries this. Values come from Prometheus when configured,
/// otherwise a fallback mock value keeps the wiring exercised in dev clusters.
pub async fn list_external_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, metric_name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ExternalMetricValueList>> {
    info!(
        "Listing external metric {} in namespace {}",
        metric_name, namespace
    );

    // External metrics are authorized as a resource named by the metric, in the
    // external.metrics.k8s.io group, scoped to the requesting namespace.
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        crate::request_scope::list_or_watch_verb(),
        &metric_name,
    )
    .with_api_group("external.metrics.k8s.io")
    .with_namespace(&namespace);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    crate::handlers::validate_synthesized_list_options(&params)?;

    // external_metrics/reststorage.go:73-77: `options.LabelSelector` (decoded
    // by ListResource; a parse failure is a 400) is the metric selector.
    let metric_selector = crate::handlers::metric_selectors::decode_label_selector(&params)?;

    // `GetExternalMetric` (provider.go:312-327): one value per series whose
    // labels satisfy the selector, each carrying that series' labels. The
    // built-in series carries no labels. With Prometheus the selector becomes
    // series matchers (the Rusternetes-only mapping of
    // `prometheus_client::promql_matchers`) and every matching series is
    // returned with its own labels.
    let series: Vec<(Option<BTreeMap<String, String>>, String)> =
        if let Some(ref prometheus_client) = state.prometheus_client {
            let matchers = crate::prometheus_client::promql_matchers(&metric_selector)
                .map_err(|e| crate::handlers::metric_selectors::unknown_error(e.to_string()))?;
            match prometheus_client
                .query_external_series(&metric_name, &namespace, &matchers)
                .await
            {
                Ok(found) => found
                    .into_iter()
                    .map(|s| (Some(s.labels), s.value))
                    .collect(),
                Err(e) => {
                    warn!(
                    "Failed to query Prometheus for external metric {}: {}. Using fallback value.",
                    metric_name, e
                );
                    vec![(None, "0".to_string())]
                }
            }
        } else if metric_selector.matches(&HashMap::new()) {
            // Fallback mock value when Prometheus is not configured.
            vec![(None, "100".to_string())]
        } else {
            Vec::new()
        };

    let items: Vec<ExternalMetricValue> = series
        .into_iter()
        .map(|(metric_labels, value)| ExternalMetricValue {
            type_meta: external_metric_type_meta("ExternalMetricValue"),
            metric_name: metric_name.clone(),
            metric_labels,
            timestamp: Utc::now(),
            window_seconds: None,
            value,
        })
        .collect();

    let list = ExternalMetricValueList {
        type_meta: external_metric_type_meta("ExternalMetricValueList"),
        // `metav1.ListMeta`; the bespoke predecessor carried only the
        // long-deprecated `selfLink` (#1916).
        metadata: rusternetes_common::types::ListMeta::default(),
        items,
    };

    Ok(crate::audit::json_response(list))
}
