//! autoscaling/v1 <-> autoscaling/v2 HorizontalPodAutoscaler conversion (#2100).
//!
//! A port of `pkg/apis/autoscaling/v1/conversion.go` (release-1.35) and the
//! round-trip annotation helpers in `pkg/apis/autoscaling/annotations.go` /
//! `helpers.go`. The stored (hub) shape is the v2 one our
//! [`HorizontalPodAutoscaler`](super::HorizontalPodAutoscaler) type models.
//! `autoscaling/v1` carries only `targetCPUUtilizationPercentage` and
//! `currentCPUUtilizationPercentage`; everything else v2 has -- the other
//! metrics, current metrics, the behavior and the conditions -- travels in
//! `autoscaling.alpha.kubernetes.io/*` annotations so a v1 client that reads and
//! writes the object back loses nothing.
//!
//! The conversion works on JSON values, the way the Event conversion does: the
//! patch machinery needs to move an object into the request's version and back
//! without a typed round trip.
//!
//! Deliberate deviations: none in mechanism. The behavior annotation holds
//! Go's *internal* `HorizontalPodAutoscalerBehavior` marshalled without json
//! tags (`conversion.go:253-263` carries a TODO about it), so its keys are the
//! Go field names (`ScaleUp`, `StabilizationWindowSeconds`, ...) and are read
//! back case-insensitively as `encoding/json` does.

use serde_json::{json, Map, Value};

/// `autoscaling.MetricSpecsAnnotation` (annotations.go:20).
pub const METRIC_SPECS_ANNOTATION: &str = "autoscaling.alpha.kubernetes.io/metrics";
/// `autoscaling.MetricStatusesAnnotation` (annotations.go:24).
pub const METRIC_STATUSES_ANNOTATION: &str = "autoscaling.alpha.kubernetes.io/current-metrics";
/// `autoscaling.HorizontalPodAutoscalerConditionsAnnotation` (annotations.go:28).
pub const CONDITIONS_ANNOTATION: &str = "autoscaling.alpha.kubernetes.io/conditions";
/// `autoscaling.BehaviorSpecsAnnotation` (annotations.go:37).
pub const BEHAVIOR_SPECS_ANNOTATION: &str = "autoscaling.alpha.kubernetes.io/behavior";
/// `autoscaling.ToleranceScaleDownAnnotation` (annotations.go:42).
pub const TOLERANCE_SCALE_DOWN_ANNOTATION: &str =
    "autoscaling.alpha.kubernetes.io/scale-down-tolerance";
/// `autoscaling.ToleranceScaleUpAnnotation` (annotations.go:46).
pub const TOLERANCE_SCALE_UP_ANNOTATION: &str =
    "autoscaling.alpha.kubernetes.io/scale-up-tolerance";
/// `autoscaling.DefaultCPUUtilization` (annotations.go:34).
pub const DEFAULT_CPU_UTILIZATION: i64 = 80;

const ROUND_TRIP_ANNOTATIONS: [&str; 6] = [
    METRIC_SPECS_ANNOTATION,
    BEHAVIOR_SPECS_ANNOTATION,
    TOLERANCE_SCALE_DOWN_ANNOTATION,
    TOLERANCE_SCALE_UP_ANNOTATION,
    METRIC_STATUSES_ANNOTATION,
    CONDITIONS_ANNOTATION,
];

fn present<'a>(obj: &'a Value, key: &str) -> Option<&'a Value> {
    obj.get(key).filter(|v| !v.is_null())
}

/// A quantity field that Go declares as a non-pointer `resource.Quantity`: the
/// zero quantity marshals as `"0"`.
fn quantity_or_zero(obj: &Value, key: &str) -> Value {
    present(obj, key).cloned().unwrap_or_else(|| json!("0"))
}

fn set_if_present(out: &mut Map<String, Value>, out_key: &str, obj: &Value, key: &str) {
    if let Some(v) = present(obj, key) {
        out.insert(out_key.to_string(), v.clone());
    }
}

/// `DropRoundTripHorizontalPodAutoscalerAnnotations` (helpers.go:36-54), applied
/// to `metadata.annotations` of `obj`. An annotations map emptied by it is
/// removed (Go's `omitempty`).
fn drop_round_trip_annotations(obj: &mut Value) {
    let Some(annotations) = obj
        .get_mut("metadata")
        .and_then(|m| m.get_mut("annotations"))
        .and_then(|a| a.as_object_mut())
    else {
        return;
    };
    if !ROUND_TRIP_ANNOTATIONS
        .iter()
        .any(|k| annotations.contains_key(*k))
    {
        return;
    }
    for k in ROUND_TRIP_ANNOTATIONS {
        annotations.remove(k);
    }
    if annotations.is_empty() {
        if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.remove("annotations");
        }
    }
}

fn set_annotation(obj: &mut Value, key: &str, value: String) {
    if obj.get("metadata").is_none_or(|m| !m.is_object()) {
        obj["metadata"] = json!({});
    }
    let meta = obj["metadata"]
        .as_object_mut()
        .expect("metadata is an object");
    let annotations = meta.entry("annotations").or_insert_with(|| json!({}));
    if let Some(map) = annotations.as_object_mut() {
        map.insert(key.to_string(), Value::String(value));
    }
}

fn annotation(obj: &Value, key: &str) -> Option<String> {
    obj.get("metadata")?
        .get("annotations")?
        .get(key)?
        .as_str()
        .map(str::to_string)
}

fn is_cpu_utilization_metric(metric: &Value) -> bool {
    metric.get("type").and_then(|t| t.as_str()) == Some("Resource")
        && metric
            .get("resource")
            .is_some_and(|r| r.get("name").and_then(|n| n.as_str()) == Some("cpu"))
        && metric
            .get("resource")
            .and_then(|r| r.get("target"))
            .and_then(|t| present(t, "averageUtilization"))
            .is_some()
}

fn ref_v2_to_v1(r: &Value) -> Value {
    let mut out = Map::new();
    out.insert("kind".into(), r.get("kind").cloned().unwrap_or(json!("")));
    out.insert("name".into(), r.get("name").cloned().unwrap_or(json!("")));
    set_if_present(&mut out, "apiVersion", r, "apiVersion");
    Value::Object(out)
}

fn metric_ident(src: &Value) -> (Value, Option<&Value>) {
    let metric = src.get("metric").cloned().unwrap_or(json!({}));
    let name = metric.get("name").cloned().unwrap_or(json!(""));
    let selector = src.get("metric").and_then(|m| present(m, "selector"));
    (name, selector)
}

/// `Convert_autoscaling_MetricSpec_To_v1_MetricSpec` and the per-source
/// functions (conversion.go:36-150, :270-279, :405-410).
fn metric_spec_v2_to_v1(m: &Value) -> Value {
    let mut out = Map::new();
    out.insert("type".into(), m.get("type").cloned().unwrap_or(json!("")));
    if let Some(o) = present(m, "object") {
        let (name, selector) = metric_ident(o);
        let target = o.get("target").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert(
            "target".into(),
            ref_v2_to_v1(o.get("describedObject").unwrap_or(&json!({}))),
        );
        v.insert("metricName".into(), name);
        v.insert("targetValue".into(), quantity_or_zero(&target, "value"));
        if let Some(s) = selector {
            v.insert("selector".into(), s.clone());
        }
        set_if_present(&mut v, "averageValue", &target, "averageValue");
        out.insert("object".into(), Value::Object(v));
    }
    if let Some(p) = present(m, "pods") {
        let (name, selector) = metric_ident(p);
        let target = p.get("target").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert("metricName".into(), name);
        v.insert(
            "targetAverageValue".into(),
            quantity_or_zero(&target, "averageValue"),
        );
        if let Some(s) = selector {
            v.insert("selector".into(), s.clone());
        }
        out.insert("pods".into(), Value::Object(v));
    }
    if let Some(r) = present(m, "resource") {
        let target = r.get("target").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert("name".into(), r.get("name").cloned().unwrap_or(json!("")));
        set_if_present(
            &mut v,
            "targetAverageUtilization",
            &target,
            "averageUtilization",
        );
        set_if_present(&mut v, "targetAverageValue", &target, "averageValue");
        out.insert("resource".into(), Value::Object(v));
    }
    if let Some(r) = present(m, "containerResource") {
        let target = r.get("target").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert("name".into(), r.get("name").cloned().unwrap_or(json!("")));
        set_if_present(
            &mut v,
            "targetAverageUtilization",
            &target,
            "averageUtilization",
        );
        set_if_present(&mut v, "targetAverageValue", &target, "averageValue");
        v.insert(
            "container".into(),
            r.get("container").cloned().unwrap_or(json!("")),
        );
        out.insert("containerResource".into(), Value::Object(v));
    }
    if let Some(e) = present(m, "external") {
        let (name, selector) = metric_ident(e);
        let target = e.get("target").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert("metricName".into(), name);
        if let Some(s) = selector {
            v.insert("metricSelector".into(), s.clone());
        }
        set_if_present(&mut v, "targetValue", &target, "value");
        set_if_present(&mut v, "targetAverageValue", &target, "averageValue");
        out.insert("external".into(), Value::Object(v));
    }
    Value::Object(out)
}

/// `Convert_v1_MetricSpec_To_autoscaling_MetricSpec` and the per-source
/// functions (conversion.go:51-130, :196-234, :281-291).
fn metric_spec_v1_to_v2(m: &Value) -> Value {
    let mut out = Map::new();
    out.insert("type".into(), m.get("type").cloned().unwrap_or(json!("")));
    let identifier = |src: &Value, name_key: &str, selector_key: &str| {
        let mut id = Map::new();
        id.insert(
            "name".into(),
            src.get(name_key).cloned().unwrap_or(json!("")),
        );
        set_if_present(&mut id, "selector", src, selector_key);
        Value::Object(id)
    };
    if let Some(o) = present(m, "object") {
        let target_type = if present(o, "averageValue").is_none() {
            "Value"
        } else {
            "AverageValue"
        };
        let mut target = Map::new();
        target.insert("type".into(), json!(target_type));
        // `Value: &in.TargetValue`: always set.
        target.insert("value".into(), quantity_or_zero(o, "targetValue"));
        set_if_present(&mut target, "averageValue", o, "averageValue");
        out.insert(
            "object".into(),
            json!({
                "describedObject": ref_v2_to_v1(o.get("target").unwrap_or(&json!({}))),
                "metric": identifier(o, "metricName", "selector"),
                "target": Value::Object(target),
            }),
        );
    }
    if let Some(p) = present(m, "pods") {
        out.insert(
            "pods".into(),
            json!({
                "metric": identifier(p, "metricName", "selector"),
                "target": {"type": "AverageValue",
                           "averageValue": quantity_or_zero(p, "targetAverageValue")},
            }),
        );
    }
    let resource_like = |src: &Value, container: bool| {
        let utilization = present(src, "targetAverageUtilization");
        let mut target = Map::new();
        target.insert(
            "type".into(),
            json!(if utilization.is_none() {
                "AverageValue"
            } else {
                "Utilization"
            }),
        );
        set_if_present(&mut target, "averageValue", src, "targetAverageValue");
        set_if_present(
            &mut target,
            "averageUtilization",
            src,
            "targetAverageUtilization",
        );
        let mut v = Map::new();
        v.insert("name".into(), src.get("name").cloned().unwrap_or(json!("")));
        if container {
            v.insert(
                "container".into(),
                src.get("container").cloned().unwrap_or(json!("")),
            );
        }
        v.insert("target".into(), Value::Object(target));
        Value::Object(v)
    };
    if let Some(r) = present(m, "resource") {
        out.insert("resource".into(), resource_like(r, false));
    }
    if let Some(r) = present(m, "containerResource") {
        out.insert("containerResource".into(), resource_like(r, true));
    }
    if let Some(e) = present(m, "external") {
        let value = present(e, "targetValue");
        let mut target = Map::new();
        target.insert(
            "type".into(),
            json!(if value.is_none() {
                "AverageValue"
            } else {
                "Value"
            }),
        );
        set_if_present(&mut target, "value", e, "targetValue");
        set_if_present(&mut target, "averageValue", e, "targetAverageValue");
        out.insert(
            "external".into(),
            json!({
                "metric": identifier(e, "metricName", "metricSelector"),
                "target": Value::Object(target),
            }),
        );
    }
    Value::Object(out)
}

/// `Convert_autoscaling_MetricStatus_To_v1_MetricStatus` and the per-source
/// functions (conversion.go:152-268 reversed, :221-262).
fn metric_status_v2_to_v1(m: &Value) -> Value {
    let mut out = Map::new();
    out.insert("type".into(), m.get("type").cloned().unwrap_or(json!("")));
    if let Some(o) = present(m, "object") {
        let (name, selector) = metric_ident(o);
        let current = o.get("current").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert(
            "target".into(),
            ref_v2_to_v1(o.get("describedObject").unwrap_or(&json!({}))),
        );
        v.insert("metricName".into(), name);
        v.insert("currentValue".into(), quantity_or_zero(&current, "value"));
        if let Some(s) = selector {
            v.insert("selector".into(), s.clone());
        }
        set_if_present(&mut v, "averageValue", &current, "averageValue");
        out.insert("object".into(), Value::Object(v));
    }
    if let Some(p) = present(m, "pods") {
        let (name, selector) = metric_ident(p);
        let current = p.get("current").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert("metricName".into(), name);
        v.insert(
            "currentAverageValue".into(),
            quantity_or_zero(&current, "averageValue"),
        );
        if let Some(s) = selector {
            v.insert("selector".into(), s.clone());
        }
        out.insert("pods".into(), Value::Object(v));
    }
    for (key, container) in [("resource", false), ("containerResource", true)] {
        if let Some(r) = present(m, key) {
            let current = r.get("current").cloned().unwrap_or(json!({}));
            let mut v = Map::new();
            v.insert("name".into(), r.get("name").cloned().unwrap_or(json!("")));
            set_if_present(
                &mut v,
                "currentAverageUtilization",
                &current,
                "averageUtilization",
            );
            v.insert(
                "currentAverageValue".into(),
                quantity_or_zero(&current, "averageValue"),
            );
            if container {
                v.insert(
                    "container".into(),
                    r.get("container").cloned().unwrap_or(json!("")),
                );
            }
            out.insert(key.into(), Value::Object(v));
        }
    }
    if let Some(e) = present(m, "external") {
        let (name, selector) = metric_ident(e);
        let current = e.get("current").cloned().unwrap_or(json!({}));
        let mut v = Map::new();
        v.insert("metricName".into(), name);
        if let Some(s) = selector {
            v.insert("metricSelector".into(), s.clone());
        }
        v.insert("currentValue".into(), quantity_or_zero(&current, "value"));
        set_if_present(&mut v, "currentAverageValue", &current, "averageValue");
        out.insert("external".into(), Value::Object(v));
    }
    Value::Object(out)
}

/// `Convert_v1_MetricStatus_To_autoscaling_MetricStatus` and the per-source
/// functions (conversion.go:164-268).
fn metric_status_v1_to_v2(m: &Value) -> Value {
    let mut out = Map::new();
    out.insert("type".into(), m.get("type").cloned().unwrap_or(json!("")));
    let identifier = |src: &Value, selector_key: &str| {
        let mut id = Map::new();
        id.insert(
            "name".into(),
            src.get("metricName").cloned().unwrap_or(json!("")),
        );
        set_if_present(&mut id, "selector", src, selector_key);
        Value::Object(id)
    };
    if let Some(o) = present(m, "object") {
        let mut current = Map::new();
        // `Value: &in.CurrentValue`: always set.
        current.insert("value".into(), quantity_or_zero(o, "currentValue"));
        set_if_present(&mut current, "averageValue", o, "averageValue");
        out.insert(
            "object".into(),
            json!({
                "metric": identifier(o, "selector"),
                "current": Value::Object(current),
                "describedObject": ref_v2_to_v1(o.get("target").unwrap_or(&json!({}))),
            }),
        );
    }
    if let Some(p) = present(m, "pods") {
        out.insert(
            "pods".into(),
            json!({
                "metric": identifier(p, "selector"),
                "current": {"averageValue": quantity_or_zero(p, "currentAverageValue")},
            }),
        );
    }
    for (key, container) in [("resource", false), ("containerResource", true)] {
        if let Some(r) = present(m, key) {
            let mut current = Map::new();
            // `AverageValue: &in.CurrentAverageValue`: always set.
            current.insert(
                "averageValue".into(),
                quantity_or_zero(r, "currentAverageValue"),
            );
            set_if_present(
                &mut current,
                "averageUtilization",
                r,
                "currentAverageUtilization",
            );
            let mut v = Map::new();
            v.insert("name".into(), r.get("name").cloned().unwrap_or(json!("")));
            if container {
                v.insert(
                    "container".into(),
                    r.get("container").cloned().unwrap_or(json!("")),
                );
            }
            v.insert("current".into(), Value::Object(current));
            out.insert(key.into(), Value::Object(v));
        }
    }
    if let Some(e) = present(m, "external") {
        let mut current = Map::new();
        current.insert("value".into(), quantity_or_zero(e, "currentValue"));
        set_if_present(&mut current, "averageValue", e, "currentAverageValue");
        out.insert(
            "external".into(),
            json!({
                "metric": identifier(e, "metricSelector"),
                "current": Value::Object(current),
            }),
        );
    }
    Value::Object(out)
}

/// A JSON field looked up the way `encoding/json` matches keys: exactly, else
/// ignoring case.
fn get_ci<'a>(obj: &'a Value, name: &str) -> Option<&'a Value> {
    let map = obj.as_object()?;
    map.get(name).or_else(|| {
        map.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })
}

/// The v2 behavior as `json.Marshal` of the internal
/// `autoscaling.HorizontalPodAutoscalerBehavior` writes it: no json tags, so Go
/// field names, and nil pointers/slices as `null`.
fn behavior_to_go_json(behavior: &Value) -> Value {
    let rules = |r: Option<&Value>| match r.filter(|r| !r.is_null()) {
        None => Value::Null,
        Some(r) => {
            let policies = match present(r, "policies").and_then(|p| p.as_array()) {
                Some(p) => Value::Array(
                    p.iter()
                        .map(|p| {
                            json!({
                                "Type": p.get("type").cloned().unwrap_or(json!("")),
                                "Value": p.get("value").cloned().unwrap_or(json!(0)),
                                "PeriodSeconds": p.get("periodSeconds").cloned().unwrap_or(json!(0)),
                            })
                        })
                        .collect(),
                ),
                None => Value::Null,
            };
            json!({
                "StabilizationWindowSeconds": r.get("stabilizationWindowSeconds").cloned().unwrap_or(Value::Null),
                "SelectPolicy": r.get("selectPolicy").cloned().unwrap_or(Value::Null),
                "Policies": policies,
                "Tolerance": r.get("tolerance").cloned().unwrap_or(Value::Null),
            })
        }
    };
    json!({
        "ScaleUp": rules(behavior.get("scaleUp")),
        "ScaleDown": rules(behavior.get("scaleDown")),
    })
}

/// The inverse of [`behavior_to_go_json`]. `None` when the annotation does not
/// decode or decodes to the zero behavior (conversion.go:400).
fn behavior_from_go_json(raw: &str) -> Option<Value> {
    let v: Value = serde_json::from_str(raw).ok()?;
    if !v.is_object() {
        return None;
    }
    let rules = |r: Option<&Value>| -> Option<Value> {
        let r = r.filter(|r| r.is_object())?;
        let mut out = Map::new();
        if let Some(x) = get_ci(r, "StabilizationWindowSeconds").filter(|x| !x.is_null()) {
            out.insert("stabilizationWindowSeconds".into(), x.clone());
        }
        if let Some(x) = get_ci(r, "SelectPolicy").filter(|x| !x.is_null()) {
            out.insert("selectPolicy".into(), x.clone());
        }
        if let Some(p) = get_ci(r, "Policies").and_then(|p| p.as_array()) {
            out.insert(
                "policies".into(),
                Value::Array(
                    p.iter()
                        .map(|p| {
                            json!({
                                "type": get_ci(p, "Type").cloned().unwrap_or(json!("")),
                                "value": get_ci(p, "Value").cloned().unwrap_or(json!(0)),
                                "periodSeconds": get_ci(p, "PeriodSeconds").cloned().unwrap_or(json!(0)),
                            })
                        })
                        .collect(),
                ),
            );
        }
        if let Some(x) = get_ci(r, "Tolerance").filter(|x| !x.is_null()) {
            out.insert("tolerance".into(), x.clone());
        }
        Some(Value::Object(out))
    };
    let mut out = Map::new();
    if let Some(r) = rules(get_ci(&v, "ScaleUp")) {
        out.insert("scaleUp".into(), r);
    }
    if let Some(r) = rules(get_ci(&v, "ScaleDown")) {
        out.insert("scaleDown".into(), r);
    }
    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

/// `Convert_autoscaling_HorizontalPodAutoscaler_To_v1_HorizontalPodAutoscaler`
/// (conversion.go:280-373) with the spec and status functions at :455-472 /
/// :501-518. `v2` is a stored `autoscaling/v2` object as JSON; the result is its
/// `autoscaling/v1` representation.
pub fn hpa_v2_to_v1(v2: &Value) -> Value {
    let mut out = v2.clone();
    out["apiVersion"] = json!("autoscaling/v1");
    drop_round_trip_annotations(&mut out);

    let in_spec = v2.get("spec").cloned().unwrap_or(json!({}));
    let in_status = v2.get("status").cloned().unwrap_or(json!({}));
    let metrics: Vec<Value> = in_spec
        .get("metrics")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();

    // Convert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_...
    let mut spec = Map::new();
    spec.insert(
        "scaleTargetRef".into(),
        ref_v2_to_v1(in_spec.get("scaleTargetRef").unwrap_or(&json!({}))),
    );
    set_if_present(&mut spec, "minReplicas", &in_spec, "minReplicas");
    spec.insert(
        "maxReplicas".into(),
        in_spec.get("maxReplicas").cloned().unwrap_or(json!(0)),
    );
    if let Some(m) = metrics.iter().find(|m| is_cpu_utilization_metric(m)) {
        spec.insert(
            "targetCPUUtilizationPercentage".into(),
            m["resource"]["target"]["averageUtilization"].clone(),
        );
    }
    out["spec"] = Value::Object(spec);

    // Convert_autoscaling_HorizontalPodAutoscalerStatus_To_v1_...: the status
    // is a struct upstream, so it is always present.
    let mut status = Map::new();
    set_if_present(
        &mut status,
        "observedGeneration",
        &in_status,
        "observedGeneration",
    );
    set_if_present(&mut status, "lastScaleTime", &in_status, "lastScaleTime");
    status.insert(
        "currentReplicas".into(),
        in_status
            .get("currentReplicas")
            .cloned()
            .unwrap_or(json!(0)),
    );
    status.insert(
        "desiredReplicas".into(),
        in_status
            .get("desiredReplicas")
            .cloned()
            .unwrap_or(json!(0)),
    );
    let current_metrics: Vec<Value> = in_status
        .get("currentMetrics")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();
    // The loop has no `break`: the last CPU metric with a utilization wins.
    for m in &current_metrics {
        if m.get("type").and_then(|t| t.as_str()) == Some("Resource")
            && m["resource"]["name"].as_str() == Some("cpu")
        {
            if let Some(u) = present(&m["resource"]["current"], "averageUtilization") {
                status.insert("currentCPUUtilizationPercentage".into(), u.clone());
            }
        }
    }
    out["status"] = Value::Object(status);

    // Metrics v1 cannot express, the raw statuses, the behavior and the
    // conditions go to annotations (conversion.go:289-372).
    let other_metrics: Vec<Value> = metrics
        .iter()
        .filter(|m| !is_cpu_utilization_metric(m))
        .map(metric_spec_v2_to_v1)
        .collect();
    if !other_metrics.is_empty() {
        set_annotation(
            &mut out,
            METRIC_SPECS_ANNOTATION,
            Value::Array(other_metrics).to_string(),
        );
    }
    if !current_metrics.is_empty() {
        let statuses: Vec<Value> = current_metrics.iter().map(metric_status_v2_to_v1).collect();
        set_annotation(
            &mut out,
            METRIC_STATUSES_ANNOTATION,
            Value::Array(statuses).to_string(),
        );
    }
    if let Some(behavior) = present(&in_spec, "behavior") {
        set_annotation(
            &mut out,
            BEHAVIOR_SPECS_ANNOTATION,
            behavior_to_go_json(behavior).to_string(),
        );
    }
    if let Some(conditions) = in_status
        .get("conditions")
        .and_then(|c| c.as_array())
        .filter(|c| !c.is_empty())
    {
        set_annotation(
            &mut out,
            CONDITIONS_ANNOTATION,
            Value::Array(conditions.clone()).to_string(),
        );
    }
    out
}

/// `Convert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler`
/// (conversion.go:375-453) with the spec and status functions at :474-499 /
/// :520-538, and `SetDefaults_HorizontalPodAutoscaler` (v1/defaults.go:29-34).
/// `v1` is an `autoscaling/v1` object as JSON; the result is the `autoscaling/v2`
/// object it stands for. `apiVersion` is left as received: the caller stamps the
/// stored version where it needs to.
pub fn hpa_v1_to_v2(v1: &Value) -> Value {
    let mut out = v1.clone();
    let in_spec = v1.get("spec").cloned().unwrap_or(json!({}));
    let in_status = v1.get("status").cloned().unwrap_or(json!({}));

    // Convert_v1_HorizontalPodAutoscalerSpec_To_autoscaling_...
    let mut spec = Map::new();
    spec.insert(
        "scaleTargetRef".into(),
        ref_v2_to_v1(in_spec.get("scaleTargetRef").unwrap_or(&json!({}))),
    );
    set_if_present(&mut spec, "minReplicas", &in_spec, "minReplicas");
    spec.insert(
        "maxReplicas".into(),
        in_spec.get("maxReplicas").cloned().unwrap_or(json!(0)),
    );
    let mut metrics: Vec<Value> = Vec::new();
    if let Some(pct) = present(&in_spec, "targetCPUUtilizationPercentage") {
        metrics.push(json!({
            "type": "Resource",
            "resource": {"name": "cpu",
                         "target": {"type": "Utilization", "averageUtilization": pct}},
        }));
    }
    // SetDefaults_HorizontalPodAutoscaler
    if present(&in_spec, "minReplicas").is_none() {
        spec.insert("minReplicas".into(), json!(1));
    }

    // Convert_v1_HorizontalPodAutoscalerStatus_To_autoscaling_...
    let mut status = Map::new();
    set_if_present(
        &mut status,
        "observedGeneration",
        &in_status,
        "observedGeneration",
    );
    set_if_present(&mut status, "lastScaleTime", &in_status, "lastScaleTime");
    status.insert(
        "currentReplicas".into(),
        in_status
            .get("currentReplicas")
            .cloned()
            .unwrap_or(json!(0)),
    );
    status.insert(
        "desiredReplicas".into(),
        in_status
            .get("desiredReplicas")
            .cloned()
            .unwrap_or(json!(0)),
    );
    if let Some(pct) = present(&in_status, "currentCPUUtilizationPercentage") {
        status.insert(
            "currentMetrics".into(),
            json!([{"type": "Resource",
                    "resource": {"name": "cpu", "current": {"averageUtilization": pct}}}]),
        );
    }

    // The annotations written by the reverse conversion (conversion.go:377-453).
    if let Some(Value::Array(other)) = annotation(v1, METRIC_SPECS_ANNOTATION)
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
    {
        let mut all: Vec<Value> = other.iter().map(metric_spec_v1_to_v2).collect();
        // The normal spec conversion could have produced one metric: it goes last.
        all.append(&mut metrics);
        metrics = all;
    }
    if let Some(behavior) =
        annotation(v1, BEHAVIOR_SPECS_ANNOTATION).and_then(|raw| behavior_from_go_json(&raw))
    {
        spec.insert("behavior".into(), behavior);
    }
    if let Some(Value::Array(current)) = annotation(v1, METRIC_STATUSES_ANNOTATION)
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
    {
        // "ignore any existing status values -- the ones here have more information"
        status.insert(
            "currentMetrics".into(),
            Value::Array(current.iter().map(metric_status_v1_to_v2).collect()),
        );
    }
    // The implicit default v1 formerly applied in the controller (:420-433).
    if metrics.is_empty() {
        metrics.push(json!({
            "type": "Resource",
            "resource": {"name": "cpu",
                         "target": {"type": "Utilization",
                                    "averageUtilization": DEFAULT_CPU_UTILIZATION}},
        }));
    }
    spec.insert("metrics".into(), Value::Array(metrics));
    if let Some(Value::Array(conditions)) = annotation(v1, CONDITIONS_ANNOTATION)
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
    {
        status.insert("conditions".into(), Value::Array(conditions));
    }

    out["spec"] = Value::Object(spec);
    out["status"] = Value::Object(status);
    // "drop round-tripping annotations after converting to internal"
    drop_round_trip_annotations(&mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// conversion_test.go:32-: the CPU utilization metric is the percentage.
    #[test]
    fn the_cpu_metric_is_the_percentage() {
        let v2 = json!({"spec": {"maxReplicas": 3, "metrics": [
            {"type": "Resource", "resource": {"name": "cpu",
             "target": {"type": "Utilization", "averageUtilization": 55}}}]}});
        let v1 = hpa_v2_to_v1(&v2);
        assert_eq!(v1["spec"]["targetCPUUtilizationPercentage"], 55);
        assert!(v1["spec"].get("metrics").is_none());
        assert!(v1["metadata"].get("annotations").is_none());
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["metrics"], v2["spec"]["metrics"]);
    }

    #[test]
    fn a_memory_utilization_metric_goes_to_the_annotation_and_back() {
        let v2 = json!({"spec": {"maxReplicas": 3, "metrics": [
            {"type": "Resource", "resource": {"name": "memory",
             "target": {"type": "Utilization", "averageUtilization": 55}}}]}});
        let v1 = hpa_v2_to_v1(&v2);
        assert!(v1["spec"].get("targetCPUUtilizationPercentage").is_none());
        assert!(v1["metadata"]["annotations"][METRIC_SPECS_ANNOTATION].is_string());
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["metrics"], v2["spec"]["metrics"]);
        assert!(back["metadata"].get("annotations").is_none());
    }

    #[test]
    fn object_and_external_metrics_round_trip() {
        let v2 = json!({"spec": {"maxReplicas": 3, "metrics": [
            {"type": "Object", "object": {
                "describedObject": {"kind": "Ingress", "name": "i", "apiVersion": "networking.k8s.io/v1"},
                "metric": {"name": "rps"},
                "target": {"type": "Value", "value": "10"}}},
            {"type": "External", "external": {
                "metric": {"name": "q", "selector": {"matchLabels": {"a": "b"}}},
                "target": {"type": "AverageValue", "averageValue": "5"}}}]}});
        let back = hpa_v1_to_v2(&hpa_v2_to_v1(&v2));
        assert_eq!(back["spec"]["metrics"], v2["spec"]["metrics"]);
    }

    #[test]
    fn behavior_and_conditions_round_trip() {
        let v2 = json!({"spec": {"maxReplicas": 3,
            "behavior": {"scaleUp": {"stabilizationWindowSeconds": 5, "selectPolicy": "Max",
                "policies": [{"type": "Pods", "value": 4, "periodSeconds": 15}], "tolerance": "100m"}}},
          "status": {"currentReplicas": 1, "desiredReplicas": 2,
            "conditions": [{"type": "AbleToScale", "status": "True"}]}});
        let v1 = hpa_v2_to_v1(&v2);
        assert_eq!(
            serde_json::from_str::<Value>(
                v1["metadata"]["annotations"][BEHAVIOR_SPECS_ANNOTATION]
                    .as_str()
                    .unwrap()
            )
            .unwrap()["ScaleUp"]["Policies"][0]["PeriodSeconds"],
            15
        );
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["behavior"], v2["spec"]["behavior"]);
        assert_eq!(back["status"]["conditions"], v2["status"]["conditions"]);
    }

    /// conversion.go:420-433.
    #[test]
    fn an_empty_v1_spec_gets_the_default_cpu_metric() {
        let back = hpa_v1_to_v2(&json!({"spec": {"maxReplicas": 3}}));
        assert_eq!(
            back["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"],
            80
        );
        assert_eq!(back["spec"]["minReplicas"], 1);
    }

    /// An undecodable annotation is ignored (`err == nil` guards).
    #[test]
    fn a_bad_annotation_is_ignored() {
        let v1 = json!({"metadata": {"annotations": {METRIC_SPECS_ANNOTATION: "nope"}},
                        "spec": {"maxReplicas": 3, "targetCPUUtilizationPercentage": 40}});
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["metrics"].as_array().unwrap().len(), 1);
        assert!(back["metadata"].get("annotations").is_none());
    }
}
