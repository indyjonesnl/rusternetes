use crate::types::TopCommands;
use anyhow::Result;
use rusternetes_client::http::ApiClient;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct NodeMetrics {
    pub metadata: MetricsMetadata,
    pub usage: ResourceUsage,
}

#[derive(Deserialize)]
pub struct PodMetrics {
    pub metadata: MetricsMetadata,
    pub containers: Vec<ContainerMetrics>,
}

#[derive(Deserialize)]
pub struct MetricsMetadata {
    pub name: String,
}

#[derive(Deserialize)]
pub struct ResourceUsage {
    pub cpu: String,
    pub memory: String,
}

#[derive(Deserialize)]
pub struct ContainerMetrics {
    pub name: String,
    pub usage: ResourceUsage,
}

/// Execute top commands for resource usage
pub async fn execute(
    client: &ApiClient,
    command: TopCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        TopCommands::Node { name, selector: _ } => {
            // Try to fetch node metrics from metrics server API
            let path = if let Some(n) = name {
                format!("/apis/metrics.k8s.io/v1beta1/nodes/{}", n)
            } else {
                "/apis/metrics.k8s.io/v1beta1/nodes".to_string()
            };

            match client.get::<serde_json::Value>(&path).await {
                Ok(response) => {
                    if let Some(items) = response.get("items").and_then(|i| i.as_array()) {
                        println!(
                            "{:<20} {:<15} {:<15}",
                            "NAME", "CPU(cores)", "MEMORY(bytes)"
                        );
                        for item in items {
                            if let Ok(metrics) = serde_json::from_value::<NodeMetrics>(item.clone())
                            {
                                println!(
                                    "{:<20} {:<15} {:<15}",
                                    metrics.metadata.name, metrics.usage.cpu, metrics.usage.memory
                                );
                            }
                        }
                    } else if let Ok(metrics) = serde_json::from_value::<NodeMetrics>(response) {
                        println!(
                            "{:<20} {:<15} {:<15}",
                            "NAME", "CPU(cores)", "MEMORY(bytes)"
                        );
                        println!(
                            "{:<20} {:<15} {:<15}",
                            metrics.metadata.name, metrics.usage.cpu, metrics.usage.memory
                        );
                    }
                }
                Err(_) => {
                    println!(
                        "Error: Metrics API not available. Metrics Server might not be installed."
                    );
                    println!("Install metrics-server: kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml");
                }
            }
        }
        TopCommands::Pod {
            name,
            namespace,
            all_namespaces,
            selector: _,
            containers,
        } => {
            let ns = if all_namespaces {
                None
            } else {
                Some(namespace.as_deref().unwrap_or(default_namespace))
            };

            let path = if let Some(n) = name {
                format!(
                    "/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods/{}",
                    ns.unwrap_or("default"),
                    n
                )
            } else if let Some(namespace) = ns {
                format!("/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods", namespace)
            } else {
                "/apis/metrics.k8s.io/v1beta1/pods".to_string()
            };

            match client.get::<serde_json::Value>(&path).await {
                Ok(response) => {
                    if let Some(items) = response.get("items").and_then(|i| i.as_array()) {
                        if containers {
                            println!(
                                "{:<30} {:<20} {:<15} {:<15}",
                                "POD", "CONTAINER", "CPU(cores)", "MEMORY(bytes)"
                            );
                        } else {
                            println!("{:<30} {:<15} {:<15}", "POD", "CPU(cores)", "MEMORY(bytes)");
                        }

                        for item in items {
                            if let Ok(metrics) = serde_json::from_value::<PodMetrics>(item.clone())
                            {
                                if containers {
                                    for container in &metrics.containers {
                                        println!(
                                            "{:<30} {:<20} {:<15} {:<15}",
                                            metrics.metadata.name,
                                            container.name,
                                            container.usage.cpu,
                                            container.usage.memory
                                        );
                                    }
                                } else {
                                    let total_cpu: String = metrics
                                        .containers
                                        .first()
                                        .map(|c| c.usage.cpu.clone())
                                        .unwrap_or_default();
                                    let total_mem: String = metrics
                                        .containers
                                        .first()
                                        .map(|c| c.usage.memory.clone())
                                        .unwrap_or_default();
                                    println!(
                                        "{:<30} {:<15} {:<15}",
                                        metrics.metadata.name, total_cpu, total_mem
                                    );
                                }
                            }
                        }
                    } else if let Ok(metrics) = serde_json::from_value::<PodMetrics>(response) {
                        if containers {
                            println!(
                                "{:<30} {:<20} {:<15} {:<15}",
                                "POD", "CONTAINER", "CPU(cores)", "MEMORY(bytes)"
                            );
                            for container in &metrics.containers {
                                println!(
                                    "{:<30} {:<20} {:<15} {:<15}",
                                    metrics.metadata.name,
                                    container.name,
                                    container.usage.cpu,
                                    container.usage.memory
                                );
                            }
                        } else {
                            println!("{:<30} {:<15} {:<15}", "POD", "CPU(cores)", "MEMORY(bytes)");
                            let total_cpu: String = metrics
                                .containers
                                .first()
                                .map(|c| c.usage.cpu.clone())
                                .unwrap_or_default();
                            let total_mem: String = metrics
                                .containers
                                .first()
                                .map(|c| c.usage.memory.clone())
                                .unwrap_or_default();
                            println!(
                                "{:<30} {:<15} {:<15}",
                                metrics.metadata.name, total_cpu, total_mem
                            );
                        }
                    }
                }
                Err(_) => {
                    println!(
                        "Error: Metrics API not available. Metrics Server might not be installed."
                    );
                    println!("Install metrics-server: kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml");
                }
            }
        }
    }

    Ok(())
}
