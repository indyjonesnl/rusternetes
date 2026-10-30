use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde::Deserialize;
use std::fs;
use std::io::{self, Read};

/// Compute the API path for a given resource kind, apiVersion, namespace, and name.
/// Returns the full path including the resource name (for PUT requests).
pub fn resource_path(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<String> {
    let (base, resource) = match kind {
        // Core v1 namespaced resources
        "Pod" => ("/api/v1", "pods"),
        "Service" => ("/api/v1", "services"),
        "ConfigMap" => ("/api/v1", "configmaps"),
        "Secret" => ("/api/v1", "secrets"),
        "ServiceAccount" => ("/api/v1", "serviceaccounts"),
        "Endpoints" => ("/api/v1", "endpoints"),
        "PersistentVolumeClaim" => ("/api/v1", "persistentvolumeclaims"),
        "ResourceQuota" => ("/api/v1", "resourcequotas"),
        "LimitRange" => ("/api/v1", "limitranges"),
        "ReplicationController" => ("/api/v1", "replicationcontrollers"),
        "Event" => ("/api/v1", "events"),

        // Core v1 cluster-scoped resources
        "Namespace" => {
            return Ok(format!("/api/v1/namespaces/{}", name));
        }
        "Node" => {
            return Ok(format!("/api/v1/nodes/{}", name));
        }
        "PersistentVolume" => {
            return Ok(format!("/api/v1/persistentvolumes/{}", name));
        }

        // apps/v1
        "Deployment" => ("/apis/apps/v1", "deployments"),
        "StatefulSet" => ("/apis/apps/v1", "statefulsets"),
        "DaemonSet" => ("/apis/apps/v1", "daemonsets"),
        "ReplicaSet" => ("/apis/apps/v1", "replicasets"),

        // batch/v1
        "Job" => ("/apis/batch/v1", "jobs"),
        "CronJob" => ("/apis/batch/v1", "cronjobs"),

        // networking.k8s.io/v1
        "Ingress" => ("/apis/networking.k8s.io/v1", "ingresses"),
        "NetworkPolicy" => ("/apis/networking.k8s.io/v1", "networkpolicies"),

        // rbac.authorization.k8s.io/v1 namespaced
        "Role" => ("/apis/rbac.authorization.k8s.io/v1", "roles"),
        "RoleBinding" => ("/apis/rbac.authorization.k8s.io/v1", "rolebindings"),

        // rbac.authorization.k8s.io/v1 cluster-scoped
        "ClusterRole" => {
            return Ok(format!(
                "/apis/rbac.authorization.k8s.io/v1/clusterroles/{}",
                name
            ));
        }
        "ClusterRoleBinding" => {
            return Ok(format!(
                "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
                name
            ));
        }

        // storage.k8s.io/v1
        "StorageClass" => {
            return Ok(format!("/apis/storage.k8s.io/v1/storageclasses/{}", name));
        }

        // scheduling.k8s.io/v1
        "PriorityClass" => {
            return Ok(format!(
                "/apis/scheduling.k8s.io/v1/priorityclasses/{}",
                name
            ));
        }

        // apiextensions.k8s.io/v1
        "CustomResourceDefinition" => {
            return Ok(format!(
                "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{}",
                name
            ));
        }

        // snapshot.storage.k8s.io/v1
        "VolumeSnapshot" => ("/apis/snapshot.storage.k8s.io/v1", "volumesnapshots"),
        "VolumeSnapshotClass" => {
            return Ok(format!(
                "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/{}",
                name
            ));
        }

        // autoscaling/v1 or v2
        "HorizontalPodAutoscaler" => {
            let base = format!("/apis/{}", api_version);
            let ns = namespace.unwrap_or("default");
            return Ok(format!(
                "{}/namespaces/{}/horizontalpodautoscalers/{}",
                base, ns, name
            ));
        }

        // policy/v1
        "PodDisruptionBudget" => ("/apis/policy/v1", "poddisruptionbudgets"),

        // For unknown kinds, try to construct the path from apiVersion
        _ => {
            // If apiVersion contains a slash (e.g., "apps/v1"), it's an API group
            if api_version.contains('/') {
                let base = format!("/apis/{}", api_version);
                let plural = kind_to_plural(kind);
                if let Some(ns) = namespace {
                    return Ok(format!("{}/namespaces/{}/{}/{}", base, ns, plural, name));
                } else {
                    return Ok(format!("{}/{}/{}", base, plural, name));
                }
            } else {
                let plural = kind_to_plural(kind);
                if let Some(ns) = namespace {
                    return Ok(format!("/api/v1/namespaces/{}/{}/{}", ns, plural, name));
                } else {
                    return Ok(format!("/api/v1/{}/{}", plural, name));
                }
            }
        }
    };

    // Namespaced resource
    let ns = namespace.unwrap_or("default");
    Ok(format!("{}/namespaces/{}/{}/{}", base, ns, resource, name))
}

/// Simple heuristic to pluralize a Kind name.
pub fn kind_to_plural(kind: &str) -> String {
    let lower = kind.to_lowercase();
    if lower.ends_with("ss")
        || lower.ends_with("ch")
        || lower.ends_with("sh")
        || lower.ends_with('x')
    {
        format!("{}es", lower)
    } else if lower.ends_with('s') {
        lower
    } else if lower.ends_with('y')
        && !lower.ends_with("ey")
        && !lower.ends_with("ay")
        && !lower.ends_with("oy")
    {
        format!("{}ies", &lower[..lower.len() - 1])
    } else {
        format!("{}s", lower)
    }
}

/// Kind to lowercase resource name for output (e.g., "Deployment" -> "deployment.apps")
pub fn kind_to_output_name(kind: &str, api_version: &str) -> String {
    let lower = kind.to_lowercase();
    if api_version == "v1" || api_version.is_empty() {
        lower
    } else if let Some(group) = api_version.split('/').next() {
        if group == "v1" || group == api_version {
            lower
        } else {
            format!("{}.{}", lower, group)
        }
    } else {
        lower
    }
}

pub async fn execute(client: &ApiClient, file: &str) -> Result<()> {
    let contents = if file == "-" {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .context("Failed to read from stdin")?;
        buffer
    } else {
        fs::read_to_string(file).context("Failed to read file")?
    };

    // Support multi-document YAML files
    for document in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(document)?;

        if value.is_null() {
            continue;
        }

        replace_resource(client, &value).await?;
    }

    Ok(())
}

async fn replace_resource(client: &ApiClient, value: &serde_yaml::Value) -> Result<()> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field in resource")?;

    let api_version = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .context("Missing 'apiVersion' field in resource")?;

    let metadata = value
        .get("metadata")
        .context("Missing 'metadata' field in resource")?;

    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .context("Missing 'metadata.name' field in resource")?;

    let namespace = metadata.get("namespace").and_then(|n| n.as_str());

    let path = resource_path(api_version, kind, namespace, name)?;

    // Convert to JSON for the PUT request
    let json_value: serde_json::Value = serde_yaml::from_value(value.clone())?;

    let _result: serde_json::Value = client.put(&path, &json_value).await?;

    let output_name = kind_to_output_name(kind, api_version);
    println!("{}/{} replaced", output_name, name);

    Ok(())
}
