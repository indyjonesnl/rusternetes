use anyhow::Result;
use std::collections::HashMap;

/// Explain Kubernetes resource documentation
pub async fn execute(resource: &str, api_version: Option<&str>, recursive: bool) -> Result<()> {
    // Parse resource path (e.g., "pod", "pod.spec", "pod.spec.containers")
    let parts: Vec<&str> = resource.split('.').collect();
    let resource_type = parts[0];

    // Hardcoded resource documentation
    let docs = get_resource_docs();

    if let Some(doc) = docs.get(resource_type) {
        println!("KIND:     {}", doc.kind);
        println!("VERSION:  {}", api_version.unwrap_or(&doc.version));
        println!();
        println!("DESCRIPTION:");
        println!("{}", doc.description);
        println!();

        if parts.len() == 1 {
            // Show top-level fields
            println!("FIELDS:");
            for (field_name, field_doc) in &doc.fields {
                println!("   {}   <{}>", field_name, field_doc.type_info);
                if !field_doc.required {
                    println!("      (optional)");
                }
                println!("      {}", field_doc.description);
                println!();
            }
        } else {
            // Show nested field
            let field_path = parts[1..].join(".");
            if let Some(field_doc) = doc.fields.get(field_path.as_str()) {
                println!("FIELD:    {}", field_path);
                println!("TYPE:     {}", field_doc.type_info);
                println!();
                println!("DESCRIPTION:");
                println!("{}", field_doc.description);
            } else {
                anyhow::bail!("Field '{}' not found in {}", field_path, resource_type);
            }
        }

        if recursive && parts.len() == 1 {
            println!(
                "\nNote: Recursive display is limited. Use '{}.<field>' to explore nested fields.",
                resource_type
            );
        }
    } else {
        anyhow::bail!(
            "Resource type '{}' not found. Common types: pod, service, deployment, node",
            resource_type
        );
    }

    Ok(())
}

pub struct ResourceDoc {
    pub kind: String,
    pub version: String,
    pub description: String,
    pub fields: HashMap<String, FieldDoc>,
}

pub struct FieldDoc {
    pub type_info: String,
    pub description: String,
    pub required: bool,
}

pub fn get_resource_docs() -> HashMap<&'static str, ResourceDoc> {
    let mut docs = HashMap::new();

    // Pod documentation
    let mut pod_fields = HashMap::new();
    pod_fields.insert(
        "apiVersion".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "APIVersion defines the versioned schema of this representation of an object."
                    .to_string(),
            required: false,
        },
    );
    pod_fields.insert(
        "kind".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "Kind is a string value representing the REST resource this object represents."
                    .to_string(),
            required: false,
        },
    );
    pod_fields.insert("metadata".to_string(), FieldDoc {
        type_info: "ObjectMeta".to_string(),
        description: "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".to_string(),
        required: false,
    });
    pod_fields.insert("spec".to_string(), FieldDoc {
        type_info: "PodSpec".to_string(),
        description: "Specification of the desired behavior of the pod. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status".to_string(),
        required: false,
    });
    pod_fields.insert("status".to_string(), FieldDoc {
        type_info: "PodStatus".to_string(),
        description: "Most recently observed status of the pod. This data may not be up to date. Populated by the system. Read-only.".to_string(),
        required: false,
    });

    docs.insert("pod", ResourceDoc {
        kind: "Pod".to_string(),
        version: "v1".to_string(),
        description: "Pod is a collection of containers that can run on a host. This resource is created by clients and scheduled onto hosts.".to_string(),
        fields: pod_fields,
    });

    // Service documentation
    let mut service_fields = HashMap::new();
    service_fields.insert(
        "apiVersion".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "APIVersion defines the versioned schema of this representation of an object."
                    .to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "kind".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "Kind is a string value representing the REST resource this object represents."
                    .to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "metadata".to_string(),
        FieldDoc {
            type_info: "ObjectMeta".to_string(),
            description: "Standard object's metadata.".to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "spec".to_string(),
        FieldDoc {
            type_info: "ServiceSpec".to_string(),
            description: "Spec defines the behavior of a service.".to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "status".to_string(),
        FieldDoc {
            type_info: "ServiceStatus".to_string(),
            description: "Most recently observed status of the service.".to_string(),
            required: false,
        },
    );

    docs.insert("service", ResourceDoc {
        kind: "Service".to_string(),
        version: "v1".to_string(),
        description: "Service is a named abstraction of software service (for example, mysql) consisting of local port (for example 3306) that the proxy listens on, and the selector that determines which pods will answer requests sent through the proxy.".to_string(),
        fields: service_fields,
    });

    // Deployment documentation
    let mut deployment_fields = HashMap::new();
    deployment_fields.insert(
        "apiVersion".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "APIVersion defines the versioned schema of this representation of an object."
                    .to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "kind".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "Kind is a string value representing the REST resource this object represents."
                    .to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "metadata".to_string(),
        FieldDoc {
            type_info: "ObjectMeta".to_string(),
            description: "Standard object metadata.".to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "spec".to_string(),
        FieldDoc {
            type_info: "DeploymentSpec".to_string(),
            description: "Specification of the desired behavior of the Deployment.".to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "status".to_string(),
        FieldDoc {
            type_info: "DeploymentStatus".to_string(),
            description: "Most recently observed status of the Deployment.".to_string(),
            required: false,
        },
    );

    docs.insert(
        "deployment",
        ResourceDoc {
            kind: "Deployment".to_string(),
            version: "apps/v1".to_string(),
            description: "Deployment enables declarative updates for Pods and ReplicaSets."
                .to_string(),
            fields: deployment_fields,
        },
    );

    docs
}
