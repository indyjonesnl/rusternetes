use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    Extension, Json,
};
use chrono::Utc;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    label_selector::LabelOperator,
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
    let attrs = RequestAttributes::new(auth_ctx.user, "list", &metric_name)
        .with_api_group("external.metrics.k8s.io")
        .with_namespace(&namespace);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    crate::handlers::validate_synthesized_list_options(&params)?;

    // external_metrics/reststorage.go:73-77: `options.LabelSelector` (decoded
    // by ListResource; a parse failure is a 400) is the metric selector.
    let metric_selector = crate::handlers::metric_selectors::decode_label_selector(&params)?;

    // `GetExternalMetric` (provider.go:312-327): a series whose labels do not
    // satisfy the selector is not returned. The built-in series carries no
    // labels. With Prometheus the selector becomes series matchers (the
    // Rusternetes-only mapping of `prometheus_client::promql_matchers`), and
    // the labels echoed are the ones the selector pins by equality.
    let (value, metric_labels) = if let Some(ref prometheus_client) = state.prometheus_client {
        let matchers = crate::prometheus_client::promql_matchers(&metric_selector)
            .map_err(|e| crate::handlers::metric_selectors::unknown_error(e.to_string()))?;
        let value = match prometheus_client
            .query_external_metric(&metric_name, &namespace, &matchers)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed to query Prometheus for external metric {}: {}. Using fallback value.",
                    metric_name, e
                );
                "0".to_string()
            }
        };
        let pinned: BTreeMap<String, String> = metric_selector
            .requirements()
            .iter()
            .filter(|r| {
                matches!(
                    r.operator(),
                    LabelOperator::Equals | LabelOperator::DoubleEquals
                )
            })
            .filter_map(|r| Some((r.key().to_string(), r.values()?.first()?.clone())))
            .collect();
        (Some(value), (!pinned.is_empty()).then_some(pinned))
    } else if metric_selector.matches(&HashMap::new()) {
        // Fallback mock value when Prometheus is not configured.
        (Some("100".to_string()), None)
    } else {
        (None, None)
    };

    let items: Vec<ExternalMetricValue> = value
        .into_iter()
        .map(|value| ExternalMetricValue {
            type_meta: external_metric_type_meta("ExternalMetricValue"),
            metric_name: metric_name.clone(),
            metric_labels: metric_labels.clone(),
            timestamp: Utc::now(),
            window: Some("60s".to_string()),
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

    Ok(Json(list))
}
