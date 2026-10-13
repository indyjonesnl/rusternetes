//! Option decoding shared by the custom.metrics.k8s.io and
//! external.metrics.k8s.io handlers.
//!
//! Upstream: kubernetes-sigs/custom-metrics-apiserver @
//! ed62571de2338833370b3cf22e6ad91e323186c4,
//! `pkg/apiserver/endpoints/handlers/get.go:84-126` (`ListOptions` decode,
//! field-label transform, name -> `metadata.name` term) and
//! `pkg/registry/custom_metrics/reststorage.go:72-101` (`labelSelector` =
//! object selector, `metricLabelSelector` parsed with `labels.Parse` and its
//! error returned as-is, `metadata.name` `RequiresExactMatch`).

use rusternetes_common::field_selector::{FieldOperator, FieldSelector};
use rusternetes_common::label_selector::{LabelOperator, LabelSelector};
use rusternetes_common::resources::custom_metrics::MetricSelector;
use rusternetes_common::types::{LabelSelectorRequirement, Status};
use rusternetes_common::Error;
use std::collections::{BTreeMap, HashMap};

/// What `ListResource` hands to `REST.List` for the custom metrics API.
pub struct DecodedListOptions {
    /// `options.LabelSelector` (defaults to `labels.Everything()`).
    pub object_selector: LabelSelector,
    /// `RequiresExactMatch("metadata.name")` of `options.FieldSelector`.
    pub name: Option<String>,
}

/// A non-APIStatus error escaping a REST storage: `ErrorToAPIStatus`
/// (`apiserver/pkg/endpoints/handlers/responsewriters/status.go:34-93`)
/// answers 500 with reason `Unknown` and `Message: err.Error()`.
pub fn unknown_error(message: impl Into<String>) -> Error {
    Error::Status(Box::new(Status::failure(message, "Unknown", 500)))
}

/// `Status{Code: 404, Reason: NotFound, Message}` without details, as the
/// provider's `NewMetricNotFound*Error` (`pkg/provider/errors.go:32-61`).
pub fn metric_not_found(message: impl Into<String>) -> Error {
    Error::Status(Box::new(Status::failure(message, "NotFound", 404)))
}

/// `labelSelector` decode (`labels.Parse`; failure is
/// `errors.NewBadRequest(err.Error())`, get.go:85-89).
pub fn decode_label_selector(params: &HashMap<String, String>) -> Result<LabelSelector, Error> {
    LabelSelector::parse(
        params
            .get("labelSelector")
            .map(String::as_str)
            .unwrap_or(""),
    )
    .map_err(|e| Error::BadRequest(e.to_string()))
}

/// get.go:84-126. `route_name` is the object named by the path (the
/// individual-GET routes), which "is treated the same as a field selector
/// entry".
pub fn decode_list_options(
    params: &HashMap<String, String>,
    route_name: Option<&str>,
) -> Result<DecodedListOptions, Error> {
    let object_selector = decode_label_selector(params)?;

    let field_selector = match params.get("fieldSelector") {
        Some(s) => Some(FieldSelector::parse(s).map_err(|e| Error::BadRequest(e.to_string()))?),
        None => None,
    };
    // `opts.FieldSelector.Transform(ConvertFieldLabel)`: the scheme has no
    // field-label conversion for the metrics types, so apimachinery
    // `DefaultMetaV1FieldSelectorConversion` (runtime/conversion.go:33-42)
    // applies.
    if let Some(fs) = &field_selector {
        for r in fs.requirements() {
            if r.field() != "metadata.name" && r.field() != "metadata.namespace" {
                return Err(Error::BadRequest(format!(
                    "{:?} is not a known field selector: only \"metadata.name\", \"metadata.namespace\"",
                    r.field()
                )));
            }
        }
    }
    // `RequiresExactMatch("metadata.name")`: an `=`/`==` term.
    let selected_name = field_selector.as_ref().and_then(|fs| {
        fs.requirements()
            .iter()
            .find(|r| r.operator() == FieldOperator::Equals && r.field() == "metadata.name")
            .map(|r| r.value().to_string())
    });

    let name = match route_name {
        Some(name) => {
            if field_selector.as_ref().is_some_and(|fs| !fs.is_empty())
                && selected_name.as_deref() != Some(name)
            {
                return Err(Error::BadRequest(
                    "fieldSelector metadata.name doesn't match requested name".to_string(),
                ));
            }
            Some(name.to_string())
        }
        None => selected_name,
    };
    Ok(DecodedListOptions {
        object_selector,
        name,
    })
}

/// `labels.Parse(metricOptions.MetricLabelSelector)` when non-empty
/// (reststorage.go:84-91): the error is returned unwrapped.
pub fn decode_metric_label_selector(
    params: &HashMap<String, String>,
) -> Result<LabelSelector, Error> {
    let raw = params
        .get("metricLabelSelector")
        .map(String::as_str)
        .unwrap_or("");
    LabelSelector::parse(raw).map_err(|e| unknown_error(e.to_string()))
}

/// `metav1.ParseToLabelSelector(metricSelector.String())` for a non-empty
/// selector (test-adapter/provider/provider.go:253-259), the `Metric.Selector`
/// echoed on each value. Errors as `ParseToLabelSelector`
/// (apimachinery/pkg/apis/meta/v1/helpers.go:107-152) returns them.
pub fn metric_selector_echo(selector: &LabelSelector) -> Result<Option<MetricSelector>, Error> {
    if selector.is_empty() {
        return Ok(None);
    }
    let mut match_labels = BTreeMap::new();
    let mut exprs = Vec::new();
    for r in selector.requirements() {
        let vals = r.values().unwrap_or(&[]);
        let op = match r.operator() {
            LabelOperator::Equals | LabelOperator::DoubleEquals => {
                if vals.len() != 1 {
                    return Err(unknown_error("equals operator must have exactly one value"));
                }
                match_labels.insert(r.key().to_string(), vals[0].clone());
                continue;
            }
            LabelOperator::In => "In",
            LabelOperator::NotIn => "NotIn",
            LabelOperator::Exists => "Exists",
            LabelOperator::DoesNotExist => "DoesNotExist",
            op @ (LabelOperator::GreaterThan | LabelOperator::LessThan) => {
                return Err(unknown_error(format!(
                    "\"{op}\" isn't supported in label selectors"
                )))
            }
            op => {
                return Err(unknown_error(format!(
                    "\"{op}\" is not a valid label selector operator"
                )))
            }
        };
        let mut values = vals.to_vec();
        values.sort();
        exprs.push(LabelSelectorRequirement {
            key: r.key().to_string(),
            operator: op.to_string(),
            values: Some(values),
        });
    }
    Ok(Some(MetricSelector {
        match_labels: (!match_labels.is_empty()).then_some(match_labels),
        match_expressions: (!exprs.is_empty()).then_some(exprs),
    }))
}
