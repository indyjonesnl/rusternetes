use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Create a HorizontalPodAutoscaler for a deployment, replica set, stateful set,
/// or replication controller.
///
/// Equivalent to `kubectl autoscale deployment foo --min=2 --max=10 --cpu-percent=80`
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    min: Option<i32>,
    max: i32,
    cpu_percent: Option<i32>,
    hpa_name: Option<&str>,
) -> Result<()> {
    if max < 1 {
        anyhow::bail!(
            "--max=MAXPODS is required and must be at least 1, max: {}",
            max
        );
    }
    if let Some(min_val) = min {
        if max < min_val {
            anyhow::bail!(
                "--max=MAXPODS must be larger or equal to --min=MINPODS, max: {}, min: {}",
                max,
                min_val
            );
        }
    }

    // Resolve the target resource API group and kind
    let (api_version, kind) = match resource_type {
        "deployment" | "deployments" | "deploy" => ("apps/v1", "Deployment"),
        "replicaset" | "replicasets" | "rs" => ("apps/v1", "ReplicaSet"),
        "statefulset" | "statefulsets" | "sts" => ("apps/v1", "StatefulSet"),
        "replicationcontroller" | "replicationcontrollers" | "rc" => {
            ("v1", "ReplicationController")
        }
        _ => anyhow::bail!(
            "cannot autoscale a {}: resource type not supported for autoscaling",
            resource_type
        ),
    };

    // Verify the target resource exists
    let resource_path = match resource_type {
        "deployment" | "deployments" | "deploy" => {
            format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                namespace, name
            )
        }
        "replicaset" | "replicasets" | "rs" => {
            format!(
                "/apis/apps/v1/namespaces/{}/replicasets/{}",
                namespace, name
            )
        }
        "statefulset" | "statefulsets" | "sts" => {
            format!(
                "/apis/apps/v1/namespaces/{}/statefulsets/{}",
                namespace, name
            )
        }
        "replicationcontroller" | "replicationcontrollers" | "rc" => {
            format!(
                "/api/v1/namespaces/{}/replicationcontrollers/{}",
                namespace, name
            )
        }
        _ => unreachable!(),
    };

    let _target: Value = client
        .get(&resource_path)
        .await
        .map_err(|e| anyhow::anyhow!("{} \"{}\" not found: {}", kind, name, e))?;

    let hpa_name = hpa_name.unwrap_or(name);

    // Build the HPA spec - try autoscaling/v2 first
    let mut hpa = json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": hpa_name,
            "namespace": namespace,
        },
        "spec": {
            "scaleTargetRef": {
                "apiVersion": api_version,
                "kind": kind,
                "name": name,
            },
            "maxReplicas": max,
        }
    });

    if let Some(min_val) = min {
        if min_val > 0 {
            hpa["spec"]["minReplicas"] = json!(min_val);
        }
    }

    // Add CPU metric if specified
    if let Some(cpu) = cpu_percent {
        if cpu > 0 {
            hpa["spec"]["metrics"] = json!([{
                "type": "Resource",
                "resource": {
                    "name": "cpu",
                    "target": {
                        "type": "Utilization",
                        "averageUtilization": cpu,
                    }
                }
            }]);
        }
    }

    let hpa_path = format!(
        "/apis/autoscaling/v2/namespaces/{}/horizontalpodautoscalers",
        namespace
    );

    let result: Result<Value> = client.post(&hpa_path, &hpa).await;
    match result {
        Ok(_) => {
            println!(
                "horizontalpodautoscaler.autoscaling/{} autoscaled",
                hpa_name
            );
        }
        Err(e) => {
            // Fall back to autoscaling/v1
            let hpa_v1 = build_hpa_v1(
                hpa_name,
                namespace,
                api_version,
                kind,
                name,
                min,
                max,
                cpu_percent,
            );
            let hpa_v1_path = format!(
                "/apis/autoscaling/v1/namespaces/{}/horizontalpodautoscalers",
                namespace
            );
            let _result: Value = client.post(&hpa_v1_path, &hpa_v1).await.with_context(|| {
                format!("failed to create HPA (v2 error: {}, v1 also failed)", e)
            })?;
            println!(
                "horizontalpodautoscaler.autoscaling/{} autoscaled",
                hpa_name
            );
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn build_hpa_v1(
    hpa_name: &str,
    namespace: &str,
    api_version: &str,
    kind: &str,
    name: &str,
    min: Option<i32>,
    max: i32,
    cpu_percent: Option<i32>,
) -> Value {
    let mut hpa = json!({
        "apiVersion": "autoscaling/v1",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": hpa_name,
            "namespace": namespace,
        },
        "spec": {
            "scaleTargetRef": {
                "apiVersion": api_version,
                "kind": kind,
                "name": name,
            },
            "maxReplicas": max,
        }
    });

    if let Some(min_val) = min {
        if min_val > 0 {
            hpa["spec"]["minReplicas"] = json!(min_val);
        }
    }

    if let Some(cpu) = cpu_percent {
        if cpu > 0 {
            hpa["spec"]["targetCPUUtilizationPercentage"] = json!(cpu);
        }
    }

    hpa
}
